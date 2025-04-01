# Use golang alpine as builder
FROM golang:1.23-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application (with CGO disabled)
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# Final stage
FROM alpine:latest

WORKDIR /app

# Create necessary directory for limits.conf
RUN mkdir -p /etc/security

# Copy binary from builder
COPY --from=builder /app/main .

# Increase file descriptor limits
RUN echo "websocket-server soft nofile 1000000" >> /etc/security/limits.conf && \
    echo "websocket-server hard nofile 1000000" >> /etc/security/limits.conf

# Expose port 8080
EXPOSE 8080

# Run the application
CMD ["./main"]


# sudo docker build -t websocket-server .
# docker run --ulimit nofile=1000000:1000000 websocket-server