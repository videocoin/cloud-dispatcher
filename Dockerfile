FROM golang:1.14 as builder
WORKDIR /go/src/github.com/videocoin/cloud-dispatcher
COPY . .
RUN make build

FROM bitnami/minideb:jessie

COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/bin/dispatcher /dispatcher
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/tools/linux_amd64/goose /goose
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/migrations /migrations
RUN install_packages curl && GRPC_HEALTH_PROBE_VERSION=v0.3.0 && \
   curl -L -k https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 --output /bin/grpc_health_probe && chmod +x /bin/grpc_health_probe
CMD ["/dispatcher"]
