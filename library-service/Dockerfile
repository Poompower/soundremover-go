FROM golang:1.22-alpine AS build

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o app .

FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=build /app/app /app/app
EXPOSE 8084
CMD ["/app/app"]

