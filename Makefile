GOOS?=linux
GOARCH?=amd64

GCP_PROJECT?=videocoin-network
ENV?=dev

NAME=dispatcher
VERSION?=$$(git describe --abbrev=0)-$$(git rev-parse --abbrev-ref HEAD)-$$(git rev-parse --short HEAD)

.PHONY: deploy

default: build

version:
	@echo ${VERSION}

lint: docker-lint

docker-lint:
	docker build -f Dockerfile.lint .

build:
	GOOS=${GOOS} GOARCH=${GOARCH} \
		go build \
			-mod vendor \
			-ldflags="-w -s -X main.Version=${VERSION}" \
			-o bin/${NAME} \
			./cmd/main.go

deps:
	GO111MODULE=on go mod vendor

docker-build:
	docker build -t gcr.io/${GCP_PROJECT}/${NAME}:${VERSION} -f Dockerfile .

docker-push:
	docker push gcr.io/${GCP_PROJECT}/${NAME}:${VERSION}

release: docker-build docker-push

deploy:
	ENV=${ENV} GCP_PROJECT=${GCP_PROJECT} deploy/deploy.sh