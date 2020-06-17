GOOS?=linux
GOARCH?=amd64
ENV?=dev

NAME=dispatcher
VERSION?=$$(git describe --abbrev=0)-$$(git rev-parse --abbrev-ref HEAD)-$$(git rev-parse --short HEAD)

REGISTRY_SERVER?=registry.videocoin.net
REGISTRY_PROJECT?=cloud

.PHONY: deploy

default: build

version:
	@echo ${VERSION}

lint: docker-lint

build:
	GOOS=${GOOS} GOARCH=${GOARCH} \
		go build \
			-mod vendor \
			-ldflags="-w -s -X main.Version=${VERSION}" \
			-o bin/${NAME} \
			./cmd/main.go

deps:
	GO111MODULE=on go mod vendor

release: docker-build docker-push

docker-lint:
	docker build -f Dockerfile.lint .

docker-build:
	docker build -t ${REGISTRY_SERVER}/${REGISTRY_PROJECT}/${NAME}:${VERSION} -f Dockerfile .

docker-push:
	docker push ${REGISTRY_SERVER}/${REGISTRY_PROJECT}/${NAME}:${VERSION}

deploy:
	cd deploy && helm upgrade -i --wait --set image.tag="${VERSION}" -n console dispatcher ./helm
