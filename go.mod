module github.com/videocoin/cloud-dispatcher

go 1.14

require (
	github.com/AlekSi/pointer v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/grafov/m3u8 v0.11.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jinzhu/copier v0.0.0-20190924061706-b57f9002281a
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/labstack/echo v3.3.10+incompatible
	github.com/mailru/dbr v3.0.0+incompatible
	github.com/opentracing/opentracing-go v1.1.0
	github.com/prometheus/client_golang v1.4.1
	github.com/sirupsen/logrus v1.4.2
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/videocoin/cloud-api v0.2.15
	github.com/videocoin/cloud-pkg v0.0.6
	golang.org/x/mod v0.1.1-0.20191105210325-c90efee705ee
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	google.golang.org/grpc v1.28.1
)

replace github.com/videocoin/cloud-api => ../cloud-api

replace github.com/videocoin/cloud-pkg => ../cloud-pkg
