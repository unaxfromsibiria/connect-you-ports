HOME_DIR := $(HOME)

example_server:
	cp example/server/docker-compose.yml docker-compose.yml
	@echo "Redis password option: $(shell cat /dev/urandom | tr -dc 'A-Za-z0-9' | head -c40)"
	@echo "Key option: $(shell openssl rand -hex 32)"
	@echo "Edit lines in docker-compose.yml:"
	cat docker-compose.yml | grep EDIT

example_client:
	cp example/client/docker-compose.yml docker-compose.yml
	@echo "Redis password option: $(shell cat /dev/urandom | tr -dc 'A-Za-z0-9' | head -c40)"
	@echo "Key option: $(shell openssl rand -hex 32)"
	@echo "Edit lines in docker-compose.yml:"
	cat docker-compose.yml | grep -i EDIT

todev:
	source "$(HOME_DIR)/.cargo/env"
