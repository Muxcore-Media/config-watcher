FROM golang:1.26-alpine AS builder
COPY core/ /build/core/
COPY config-watcher/ /build/config-watcher/
WORKDIR /build/config-watcher
RUN go mod download && CGO_ENABLED=0 go build -o /config-watcher ./cmd/module
FROM alpine:3.21
RUN adduser -D -h /data app
USER app
WORKDIR /app
COPY --from=builder /config-watcher .
ENTRYPOINT ["./config-watcher"]
