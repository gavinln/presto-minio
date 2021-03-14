ifeq ($(OS),Windows_NT)
    SHELL='c:/Program Files/Git/usr/bin/sh.exe'
endif

SCRIPT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.DEFAULT_GOAL=help
.PHONY: help
help:  ## help for this Makefile
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: list
list:  ## list all targets
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

connect:  ## connect to gavinsvr 
	ssh gavinsvr

mypy:  ## mypy presto-api.py
	@mypy --follow-imports=skip --ignore-missing-imports $(SCRIPT_DIR)/python/presto-api.py

pytest:  ## pytest presto-api.py
	@pytest $(SCRIPT_DIR)/python/presto-api.py

.PHONY: pytype
pytype:  ## pytype type checker
	@pytype $(SCRIPT_DIR)/python

.PHONY: pyright
pyright:  ## pyright type checker
	PYTHONPATH=python:${PYTHONPATH} pyright $(SCRIPT_DIR)/python

yapf:  ## yapf -i presto-api.py
	@yapf -i $(SCRIPT_DIR)/python/presto-api.py

flake8:  ## flake8 presto-api.py
	@flake8 $(SCRIPT_DIR)/python/presto-api.py

presto-start:  ## start Presto
	@docker-compose -f $(SCRIPT_DIR)/presto-minio/docker-compose.yml up -d
	@echo "IP address: "
	@hostname -I

presto-stop:  ## stop Presto
	docker-compose -f $(SCRIPT_DIR)/presto-minio/docker-compose.yml stop

presto-ps:  ## list Presto docker containers
	docker-compose -f $(SCRIPT_DIR)/presto-minio/docker-compose.yml ps

postgres-start:  ## Start Postges in container
	docker-compose -f $(SCRIPT_DIR)/postgres/docker-compose.yml up -d

postgres-stop:  ## Start Postges in container
	docker-compose -f $(SCRIPT_DIR)/postgres/docker-compose.yml stop

.PHONY: presto-hive
presto-hive:  ## run presto-hive db util
	pipenv run python python/presto-hive.py

.PHONY: frank
frank:  ## run frank
	PYTHONPATH=./python:$PYTHONPATH typer python/frank.py run --help


.PHONY: clean
clean:  ## remove targets and intermediate files
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} \;
	find . -type d -name ".pytype" -exec rm -rf {} \;
	find . -type f -name "$(PROJECT_LIB_NAME).egg-info" -exec rm -rf {} \;
