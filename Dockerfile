FROM perl:5.38-slim
WORKDIR /app
COPY drive-server.pl "Sodium Slide.html" ./
CMD ["perl", "drive-server.pl"]
