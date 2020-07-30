ifeq ($(OS),Windows_NT)
    SHELL='c:/Program Files/Git/usr/bin/sh.exe'
endif

SCRIPT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.DEFAULT_GOAL=help
.PHONY: help
help:  ## help for this Makefile
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

connect:  ## connect to gavinsvr 
	ssh gavinsvr

run:  ## run presto-api.py
	@pipenv run python3 $(SCRIPT_DIR)/python/presto-api.py

mypy:  ## mypy presto-api.py
	@mypy --follow-imports=skip --ignore-missing-imports $(SCRIPT_DIR)/python/presto-api.py

pytest:  ## pytest presto-api.py
	@pytest $(SCRIPT_DIR)/python/presto-api.py

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
