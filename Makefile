YB_IMAGE := amundsen-loader-yugabyte
MINIO_IMAGE := amundsen-loader-minio
MINIO_STATS_IMAGE := amundsen-loader-minio-stats
VERSION:= $(shell git describe --tags --dirty)

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

.PHONY: yb-image
yb-image:
	docker build --build-arg scriptpath="rcpai/yugabyte_sql_loader.py" -f docker/base.Dockerfile -t ${YB_IMAGE}:${VERSION} .

.PHONY: minio-image
minio-image:
	docker build --build-arg scriptpath="rcpai/minio_loader.py" -f docker/spark.Dockerfile -t ${MINIO_IMAGE}:${VERSION} .

.PHONY: minio-stats-image
minio-stats-image:
	docker build --build-arg scriptpath="rcpai/minio_stats_loader.py" -f docker/spark.Dockerfile -t ${MINIO_STATS_IMAGE}:${VERSION} .

.PHONY: image
image: minio-image minio-stats-image yb-image
