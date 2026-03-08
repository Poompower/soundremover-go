FROM golang:1.22-alpine AS build

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o app .

FROM alpine:3.20
WORKDIR /app
ENV PROCESSING_ROLE=worker
RUN apk add --no-cache docker-cli ca-certificates ffmpeg curl python3 && \
    curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp
COPY --from=build /app/app /app/app
EXPOSE 8083
CMD ["/app/app"]
