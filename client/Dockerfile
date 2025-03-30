FROM golang:1.22.2 as builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod tidy
COPY . .
RUN go build -o client main.go

FROM debian:latest
WORKDIR /app
COPY --from=builder /app/client .
CMD ["/app/client"]

# sudo docker build -t ws-client .