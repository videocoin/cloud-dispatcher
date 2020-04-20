FROM golang:1.14 as builder
WORKDIR /go/src/github.com/videocoin/cloud-dispatcher
COPY . .
RUN make build

FROM bitnami/minideb:jessie

COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/bin/dispatcher /dispatcher
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/tools/linux_amd64/goose /goose
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/migrations /migrations
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/tools/linux_amd64/grpc_health_probe /bin/grpc_health_probe
CMD ["/dispatcher"]
