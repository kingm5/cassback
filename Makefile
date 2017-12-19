VERSION = $(shell git describe --tags)
DOCKER_REGISTRY ?=
DOCKER_TAG ?= $(VERSION)

check:
	flake8 --max-line-length=120 cassback/

docker-build:
	docker build --pull -f Dockerfile -t $(DOCKER_REGISTRY)cassback:$(DOCKER_TAG) .

docker-push:
	docker push $(DOCKER_REGISTRY)cassback:$(DOCKER_TAG)

test:
	python -m unittest discover -v

version:
	@echo $(VERSION)

.PHONY: check docker-build docker-push test version
