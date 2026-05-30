HOME_DIR := $(HOME)
DOCKER_COMPOSE_CMD := $(shell docker help 2>/dev/null | grep -q compose && echo "docker compose" || echo "docker-compose")

example_server:
	cat example/server/docker-compose.yml > docker-compose.yml
	sed -i "s|CRYPTO_KEY: \"<SETUP>\"|CRYPTO_KEY: \"$(shell openssl rand -hex 32)\"|" docker-compose.yml
	@echo "Key for other side: $(shell cat docker-compose.yml | grep CRYPTO_KEY | cut -d ':' -f2)"

example_client:
	cat example/client/docker-compose.yml > docker-compose.yml
	sed -i "s|CRYPTO_KEY: \"<SETUP>\"|CRYPTO_KEY: \"$(shell openssl rand -hex 32)\"|" docker-compose.yml
	@echo "Key for other side: $(shell cat docker-compose.yml | grep CRYPTO_KEY | cut -d ':' -f2)"
	@echo "Edit lines in docker-compose.yml:"
	cat docker-compose.yml | grep -i EDIT

todev:
	source "$(HOME_DIR)/.cargo/env"
