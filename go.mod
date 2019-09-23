module github.com/videocoin/cloud-dispatcher

go 1.12

require (
	github.com/go-playground/locales v0.12.1
	github.com/go-playground/universal-translator v0.16.0
	github.com/gogo/protobuf v1.3.0
	github.com/jinzhu/copier v0.0.0-20190625015134-976e0346caa8
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/leodido/go-urn v1.1.0 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/sirupsen/logrus v1.4.2
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/videocoin/cloud-api v0.2.7
	github.com/videocoin/cloud-pkg v0.0.5
	github.com/videocoin/cloud-streams v0.0.0-20190909150823-8d31a6477edb
	google.golang.org/grpc v1.23.0
	gopkg.in/go-playground/validator.v9 v9.29.1
)

replace github.com/videocoin/cloud-api => ../cloud-api

replace github.com/videocoin/cloud-pkg => ../cloud-pkg
