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

RUN mkdir -p /out && CGO_ENABLED=0 GOOS=linux go build -o /out/client ./main.go

FROM debian:bookworm-slim

RUN groupadd -r appgroup && useradd -r -g appgroup appuser
WORKDIR /app
RUN chown -R appuser:appgroup /app
COPY --from=builder --chown=appuser:appgroup /out/client /app/client

USER appuser
CMD ["/app/client"]

# sudo docker build -f Dockerfile -t ws-client ..