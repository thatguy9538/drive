#!/usr/bin/perl
# Sodium Slide LAN server.
# Serves the game to every computer on your network and relays the cars between them.
# Written in Perl because every Mac already has it, so the host doesn't need to install anything.
# It only uses modules that come with Perl.
#
#   perl drive-server.pl [port] [--auto-stop] [--url-file=PATH] [--open]
#
# --auto-stop    stop a minute after the last player leaves (two minutes if nobody ever joins)
# --url-file     write the address friends should type to this file once the server is up
# --open         open the game in the browser once the server is up
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET SO_KEEPALIVE);
use Digest::SHA qw(sha1_base64);
use JSON::PP;
use Time::HiRes qw(time);
use File::Basename qw(dirname basename);
use Cwd qw(abs_path);
use POSIX qw(strftime);

$SIG{PIPE} = 'IGNORE';
$| = 1;
binmode STDOUT, ':encoding(UTF-8)';

my ($port_arg) = grep { /^\d+$/ } @ARGV;
my $START_PORT = $port_arg || $ENV{PORT} || 8080;
my $AUTO = grep { $_ eq '--auto-stop' } @ARGV;
my $OPEN = grep { $_ eq '--open' } @ARGV;
my ($URL_FILE) = map { /^--url-file=(.*)$/ ? $1 : () } @ARGV;
my $DIR = dirname(abs_path($0));
my $JSON = JSON::PP->new->utf8->canonical(0);

sub stamp { strftime('%H:%M', localtime) }
sub logmsg { print "  [" . stamp() . "] $_[0]\n" }

# ---------- find the game ----------
sub find_game {
  opendir(my $dh, $DIR) or return;
  my @files = sort grep { /\.html?$/i } readdir $dh;
  closedir $dh;
  my ($pick) = grep { /sodium|drive|slide/i } @files;
  $pick //= $files[0];
  return defined $pick ? "$DIR/$pick" : undef;
}

# ---------- addresses on this network ----------
sub lan_ips {
  my @ips;
  # the address of the network card that leads to the rest of the network (no packets are sent)
  my $u = IO::Socket::INET->new(PeerAddr => '10.255.255.255', PeerPort => 9, Proto => 'udp');
  if ($u) { my $ip = $u->sockhost; push @ips, $ip if $ip && $ip !~ /^(0\.|127\.)/; close $u; }
  # plus anything else ifconfig lists
  my $out = `/sbin/ifconfig 2>/dev/null` || `ifconfig 2>/dev/null` || `ip -4 addr 2>/dev/null` || '';
  for my $ip ($out =~ /inet (?:addr:)?(\d+\.\d+\.\d+\.\d+)/g) {
    next if $ip =~ /^(127\.|169\.254\.|0\.)/;
    push @ips, $ip unless grep { $_ eq $ip } @ips;
  }
  # home and school networks use these ranges; list them first
  return sort { (($b =~ /^(192\.168|10)\./) <=> ($a =~ /^(192\.168|10)\./)) } @ips;
}

# ---------- connections ----------
# each connection: { sock, in, out, ws, id, name, ready, alive, addr, frag, fragop }
my %conns;          # fileno -> conn
my %players;        # id -> conn (WebSocket players)
my $next_id = 1;
my $SHARE = '';
my $sel = IO::Select->new;
my $listener;
my $stop_at;        # auto-stop deadline

sub clean_name {
  my $n = defined $_[0] ? "$_[0]" : '';
  $n =~ s/[\x00-\x1f]//g; $n =~ s/^\s+|\s+$//g;
  $n = substr($n, 0, 16);
  return length $n ? $n : 'Driver';
}
sub ready_count { scalar grep { $_->{ready} } values %players }

sub queue { my ($c, $bytes) = @_; return if $c->{dead}; $c->{out} .= $bytes; flush($c); }
sub flush {
  my $c = shift;
  while (length $c->{out}) {
    my $n = syswrite($c->{sock}, $c->{out}, 65536);
    if (!defined $n) { return if $!{EAGAIN} || $!{EWOULDBLOCK} || $!{EINTR}; drop($c); return; }
    substr($c->{out}, 0, $n) = '';
  }
  if ($c->{closing} && !length $c->{out}) { drop($c); }
}
sub ws_frame {
  my ($op, $data) = @_;
  my $len = length $data;
  my $head = $len < 126 ? pack('CC', 0x80 | $op, $len)
           : $len < 65536 ? pack('CCn', 0x80 | $op, 126, $len)
           : pack('CCNN', 0x80 | $op, 127, 0, $len);
  return $head . $data;
}
sub ws_send { my ($c, $text) = @_; queue($c, ws_frame(1, $text)); }
sub send_obj { my ($c, $obj) = @_; ws_send($c, $JSON->encode($obj)); }
sub broadcast_raw {
  my ($text, $except) = @_;
  my $frame = ws_frame(1, $text);
  for my $p (values %players) { next if !$p->{ready} || (defined $except && $p->{id} == $except); queue($p, $frame); }
}
sub broadcast_obj { my ($obj, $except) = @_; broadcast_raw($JSON->encode($obj), $except); }

sub drop {
  my $c = shift;
  return if $c->{dead};
  $c->{dead} = 1;
  $sel->remove($c->{sock});
  delete $conns{$c->{fd}};
  close $c->{sock};
  if ($c->{ws} && delete $players{$c->{id}}) {
    if ($c->{ready}) {
      broadcast_obj({ t => 'leave', id => $c->{id} });
      logmsg("$c->{name} left  (" . ready_count() . " connected)");
    }
    auto_stop();
  }
}
sub auto_stop {
  my $first = shift;
  return unless $AUTO;
  $stop_at = %players ? undef : time + ($first ? 120 : 60);
}

# ---------- game messages ----------
sub on_message {
  my ($c, $raw) = @_;
  my ($t) = $raw =~ /^\s*\{\s*"t"\s*:\s*"(\w+)"/;
  if (!defined $t) { my $m = eval { $JSON->decode($raw) }; return unless ref $m eq 'HASH' && defined $m->{t}; $t = $m->{t}; }
  if ($t eq 'hello') {
    return if $c->{ready};
    my $m = eval { $JSON->decode($raw) } || {};
    $c->{name} = clean_name($m->{n}); $c->{ready} = 1;
    send_obj($c, { t => 'welcome', id => $c->{id}, share => $SHARE,
                   players => [ map { { id => $_->{id}, n => $_->{name} } } sort { $a->{id} <=> $b->{id} } grep { $_->{ready} } values %players ] });
    broadcast_obj({ t => 'join', id => $c->{id}, n => $c->{name} }, $c->{id});
    logmsg("$c->{name} joined from $c->{addr}  (" . ready_count() . " connected)");
    return;
  }
  return unless $c->{ready};
  if ($t eq 'name') {
    my $m = eval { $JSON->decode($raw) } || {};
    my $old = $c->{name}; $c->{name} = clean_name($m->{n});
    broadcast_obj({ t => 'name', id => $c->{id}, n => $c->{name} }, $c->{id});
    logmsg("$old is now $c->{name}") if $old ne $c->{name};
    return;
  }
  if ($t eq 'start') { my ($mode) = $raw =~ /"mode"\s*:\s*"(\w+)"/; logmsg("$c->{name} started " . ($mode // 'a run')); }
  # everything else is passed on to the other players, tagged with who sent it
  # (added last, so it wins over any "id" already in the message)
  return unless $raw =~ s/\}\s*\z/,"id":$c->{id}}/ && $raw !~ /^\s*\{\s*,/;
  broadcast_raw($raw, $c->{id});
}

# ---------- WebSocket frames ----------
sub ws_read {
  my $c = shift;
  while (length($c->{in}) >= 2) {
    my ($b0, $b1) = unpack('CC', $c->{in});
    my $fin = $b0 & 0x80; my $op = $b0 & 0x0f; my $masked = $b1 & 0x80; my $len = $b1 & 0x7f; my $off = 2;
    if ($len == 126) { return if length($c->{in}) < 4; $len = unpack('n', substr($c->{in}, 2, 2)); $off = 4; }
    elsif ($len == 127) { return if length($c->{in}) < 10; my ($hi, $lo) = unpack('NN', substr($c->{in}, 2, 8)); $len = $hi * 4294967296 + $lo; $off = 10; }
    if ($len > 4 * 1024 * 1024) { drop($c); return; }   # nothing the game sends is this big
    my $need = $off + ($masked ? 4 : 0) + $len;
    return if length($c->{in}) < $need;
    my $payload = substr($c->{in}, $off + ($masked ? 4 : 0), $len);
    if ($masked) {
      my $mask = substr($c->{in}, $off, 4);
      $payload ^= substr($mask x (int($len / 4) + 1), 0, $len);
    }
    substr($c->{in}, 0, $need) = '';
    $c->{alive} = 1;
    if ($op == 8) { queue($c, ws_frame(8, '')); $c->{closing} = 1; flush($c); return; }
    if ($op == 9) { queue($c, ws_frame(10, $payload)); next; }
    next if $op == 10;
    if ($op == 1 || $op == 2) { $c->{fragop} = $op; $c->{frag} = $payload; }
    elsif ($op == 0) { $c->{frag} .= $payload; }
    else { next; }
    if ($fin) { on_message($c, $c->{frag}) if $c->{fragop} == 1; $c->{frag} = ''; }
    return if $c->{dead};
  }
}

# ---------- plain web requests ----------
sub http_read {
  my $c = shift;
  my $end = index($c->{in}, "\r\n\r\n");
  if ($end < 0) { drop($c) if length $c->{in} > 65536; return; }
  my $head = substr($c->{in}, 0, $end); substr($c->{in}, 0, $end + 4) = '';
  my ($line, @lines) = split /\r\n/, $head;
  my ($method, $path) = split / /, $line // '';
  $path //= '/'; $path =~ s/\?.*//;
  my %h; for (@lines) { $h{lc $1} = $2 if /^([^:]+):\s*(.*)$/ }

  if ($path eq '/ws' && ($h{upgrade} // '') =~ /websocket/i && $h{'sec-websocket-key'}) {
    my $accept = sha1_base64($h{'sec-websocket-key'} . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11') . '=';
    queue($c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\n\r\n");
    setsockopt($c->{sock}, IPPROTO_TCP, TCP_NODELAY, 1);
    setsockopt($c->{sock}, SOL_SOCKET, SO_KEEPALIVE, 1);
    @$c{qw(ws id name ready alive frag fragop)} = (1, $next_id++, 'Driver', 0, 1, '', 0);
    $players{$c->{id}} = $c;
    $stop_at = undef;
    ws_read($c) if length $c->{in};
    return;
  }
  my $resp;
  if ($path eq '/' || $path =~ /\.html?$/i) {
    my $game = find_game();
    if ($game && open(my $fh, '<:raw', $game)) {
      local $/; my $body = <$fh>; close $fh;
      $resp = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nContent-Length: " . length($body) . "\r\nConnection: close\r\n\r\n" . $body;
    } else {
      my $msg = 'Put the Sodium Slide .html file in the same folder as the server.';
      $resp = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: " . length($msg) . "\r\nConnection: close\r\n\r\n$msg";
    }
  } elsif ($path eq '/favicon.ico') {
    $resp = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
  } else {
    $resp = "HTTP/1.1 302 Found\r\nLocation: /\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
  }
  $c->{closing} = 1;
  queue($c, $method && $method eq 'HEAD' ? substr($resp, 0, index($resp, "\r\n\r\n") + 4) : $resp);
}

# ---------- start up ----------
my $port;
for my $p ($START_PORT .. $START_PORT + 10) {
  $listener = IO::Socket::INET->new(LocalAddr => '0.0.0.0', LocalPort => $p, Proto => 'tcp', Listen => 64, ReuseAddr => 1);
  if ($listener) { $port = $p; last; }
}
die "\n  Could not start the server: $!\n\n" unless $listener;
$listener->blocking(0);
$sel->add($listener);

my @ips = lan_ips();
my $game = find_game();
my $main = @ips ? "http://$ips[0]:$port" : "http://localhost:$port";
($SHARE = $main) =~ s{^http://}{};
my $rule = '  ' . ('-' x 58);
print "\n$rule\n   SODIUM SLIDE  -  LAN server is running\n$rule\n";
print $game ? "\n  Game file: " . basename($game) . "\n" : "\n  !! No .html game file found next to this server.\n";
print "\n  Friends on the same Wi-Fi open a browser and go to:\n\n";
print "      http://$_:$port\n" for @ips;
print "      (no network found, are you on Wi-Fi?)\n" unless @ips;
print "\n  On this computer:  http://localhost:$port\n$rule\n\n";
if ($URL_FILE && open(my $fh, '>', $URL_FILE)) { print $fh "$main\n"; close $fh; }
system('open', "http://localhost:$port") if $OPEN && $^O eq 'darwin';
auto_stop(1);

# ---------- main loop ----------
my $next_ping = time + 8;
while (1) {
  for my $s ($sel->can_read(0.5)) {
    if ($s == $listener) {
      while (my $cs = $listener->accept) {
        $cs->blocking(0);
        my $c = { sock => $cs, fd => fileno($cs), in => '', out => '', addr => ($cs->peerhost // '?') };
        $c->{addr} =~ s/^::ffff://;
        $conns{$c->{fd}} = $c; $sel->add($cs);
      }
      next;
    }
    my $c = $conns{fileno($s)} or next;
    my $n = sysread($s, my $buf, 65536);
    if (!defined $n) { next if $!{EAGAIN} || $!{EWOULDBLOCK} || $!{EINTR}; drop($c); next; }
    if ($n == 0) { drop($c); next; }
    $c->{in} .= $buf;
    $c->{ws} ? ws_read($c) : http_read($c);
  }
  # finish writes that didn't fit in one go
  for my $c (values %conns) {
    flush($c) if length $c->{out};
    drop($c) if !$c->{dead} && length($c->{out}) > 8 * 1024 * 1024;   # a stuck player shouldn't hold everyone up
  }
  my $now = time;
  # drop players whose computer went to sleep or lost Wi-Fi without saying goodbye
  if ($now >= $next_ping) {
    $next_ping = $now + 8;
    for my $p (values %players) {
      if (!$p->{alive}) { drop($p); next; }
      $p->{alive} = 0; queue($p, ws_frame(9, ''));
    }
  }
  if (defined $stop_at && $now >= $stop_at) { logmsg('Nobody is playing, so the server stopped.'); exit 0; }
}
