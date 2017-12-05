DOCKER_TAG ?= latest

check:
	flake8 --max-line-length=120 cassback/

docker-build:
	docker build --pull -f Dockerfile -t cassback:$(DOCKER_TAG) .

test:
	python -m unittest discover -v

.PHONY: check
