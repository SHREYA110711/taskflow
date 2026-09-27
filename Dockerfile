# Multi-stage Dockerfile for TaskFlow services (API and Worker)
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build argument to specify which binary to build: "api" or "worker"
ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-w -s" -o /app/bin/service ./cmd/${SERVICE}

# Final minimal runtime image
FROM alpine:3.19

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/bin/service /app/service
COPY --from=builder /app/internal/handler/dashboard.html /app/internal/handler/dashboard.html

EXPOSE 8080

ENTRYPOINT ["/app/service"]
