FROM golang:1.12.4 as builder
WORKDIR /go/src/github.com/videocoin/cloud-dispatcher
COPY . .
RUN make build

FROM bitnami/minideb:jessie
COPY --from=builder /go/src/github.com/videocoin/cloud-dispatcher/bin/dispatcher /opt/videocoin/bin/dispatcher
CMD ["/opt/videocoin/bin/dispatcher"]
