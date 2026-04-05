FROM golang:1.23.7 AS builder

WORKDIR /src

# Copy the client module files and the local server module metadata first so
# the replace directive can resolve `fastsocket` from the repo, not the network.
COPY client/go.mod client/go.sum ./client/
COPY server/go.mod ./server/go.mod
COPY server/grpc/trackingpb ./server/grpc/trackingpb

WORKDIR /src/client
RUN go mod download

COPY client/ ./
COPY server/grpc/trackingpb ../server/grpc/trackingpb

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/client ./main.go

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /out/client /app/client

CMD ["/app/client"]

# sudo docker build -t ws-client .