module github.com/videocoin/cloud-dispatcher

go 1.13

require (
	github.com/AlekSi/pointer v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/grafov/m3u8 v0.11.1
	github.com/jinzhu/copier v0.0.0-20190924061706-b57f9002281a
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/labstack/echo v3.3.10+incompatible
	github.com/mailru/dbr v3.0.0+incompatible
	github.com/opentracing/opentracing-go v1.1.0
	github.com/prometheus/client_golang v0.9.4
	github.com/sirupsen/logrus v1.4.2
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/videocoin/cloud-api v0.2.15
	github.com/videocoin/cloud-pkg v0.0.6
	golang.org/x/net v0.0.0-20200222125558-5a598a2470a0
	google.golang.org/grpc v1.27.1
)

replace github.com/videocoin/cloud-api => ../cloud-api

replace github.com/videocoin/cloud-pkg => ../cloud-pkg
