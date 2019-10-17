FROM golang:1.12.4 as builder
WORKDIR /go/src/github.com/videocoin/cloud-dispatcher
COPY . .
RUN make build

FROM bitnami/minideb:jessie

COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/bin/dispatcher /dispatcher
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/tools/linux_amd64/goose /goose
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/migrations /migrations

CMD ["/dispatcher"]
