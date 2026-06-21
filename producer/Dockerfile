FROM golang:1.21-alpine AS builder
WORKDIR /app

# Copy everything (including vendor)
COPY . .

# Build using vendored dependencies (no network)
RUN go build -mod=vendor -o producer .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/producer .
CMD ["./producer"]