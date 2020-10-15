IMAGE := amundsendev/ybloader
VERSION:= $(shell grep -m 1 '__version__' setup.py | cut -d '=' -f 2 | tr -d "'" | tr -d '[:space:]')

.PHONY: clean
clean:
	find . -name \*.pyc -delete
	find . -name __pycache__ -delete
	rm -rf dist/

.PHONY: build
build:
	pip install -r requirements.txt

.PHONY: test_unit
test_unit:
	pip install -r test-requirements.txt
	python3 -bb -m pytest tests

lint:
	flake8 .

.PHONY: mypy
mypy:
	mypy .

.PHONY: test
test: build test_unit lint mypy

.PHONY: image
image:
	docker build -f ybloader.Dockerfile -t ${IMAGE}:${VERSION} .
	docker tag ${IMAGE}:${VERSION} ${IMAGE}:latest

