FROM perl:5.38-slim
WORKDIR /app
COPY . .
CMD ["perl", "drive-server.pl"]
