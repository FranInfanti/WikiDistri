
FILTERS ?= 1
CLIENTS ?= 1
WORD_READERS ?= 4
SUM_COUNTS ?= 4

COMPOSE = docker compose -f docker-compose.yaml

.PHONY: generate up down logs ps clean

generate:
	@{ \
		echo "services:"; \
		printf '  rabbitmq:\n'; \
		printf '    build:\n      context: ./src/services/rabbitmq\n      dockerfile: Dockerfile\n'; \
		printf '    container_name: rabbitmq\n'; \
		printf '    environment:\n      RABBITMQ_LOG_LEVELS: error\n'; \
		printf '    healthcheck:\n      interval: 5s\n      retries: 10\n      start_period: 50s\n      test: rabbitmq-diagnostics check_port_connectivity\n      timeout: 3s\n'; \
		printf '    ports:\n      - "5672:5672"\n      - "15672:15672"\n'; \
		printf '  gateway:\n'; \
		printf '    build:\n      context: ./src/\n      dockerfile: services/gateway/Dockerfile\n'; \
		printf '    container_name: gateway\n'; \
		printf '    environment:\n      PYTHONUNBUFFERED: "1"\n      HOST: gateway\n      PORT: "5000"\n      JOBS: 5\n      BATCH_SIZE: "50"\n      MOM_HOST: rabbitmq\n      GATEWAY_PREFIX: gateway\n      FILTER_OUTPUT_QUEUE: filter_queue\n      WORD_READER_OUTPUT_QUEUE: word_reader_queue\n'; \
		printf '    ports:\n      - "5000:5000"\n'; \
		printf '    depends_on:\n      rabbitmq:\n        condition: service_healthy\n'; \
		for i in $$(seq 0 $$(( $(CLIENTS) - 1 ))); do \
			printf '  client_%s:\n' "$$i"; \
			printf '    build:\n      context: .\n      dockerfile: src/services/client/Dockerfile\n'; \
			printf '    container_name: client_%s\n' "$$i"; \
			printf '    environment:\n      PYTHONUNBUFFERED: "1"\n      INPUT_FILE: /app/input/articles.csv\n      SERVER_HOST: gateway\n      SERVER_PORT: "5000"\n      BATCH_SIZE: "100"\n'; \
			printf '    volumes:\n      - ./input:/app/input:ro\n      - ./output:/app/output:ro\n'; \
			printf '    depends_on:\n      gateway:\n        condition: service_started\n'; \
		done; \
		for i in $$(seq 0 $$(( $(FILTERS) - 1 ))); do \
			printf '  filter_%s:\n' "$$i"; \
			printf '    build:\n      context: ./src/\n      dockerfile: services/filter/Dockerfile\n'; \
			printf '    container_name: filter_%s\n' "$$i"; \
			printf '    environment:\n      PYTHONUNBUFFERED: "1"\n'; \
			printf '      ID: "%s"\n' "$$i"; \
			printf '      FILTER_AMOUNT: "$(FILTERS)"\n      MOM_HOST: rabbitmq\n      FILTER_PREFIX: filter\n      GATEWAY_PREFIX: gateway\n      FILTER_INPUT_QUEUE: filter_queue\n      IMAGE_DOWNLOADER_OUTPUT_QUEUE: image_downloader_queue\n'; \
			printf '    depends_on:\n      rabbitmq:\n        condition: service_healthy\n'; \
		done; \
		for i in $$(seq 0 $$(( $(WORD_READERS) - 1 ))); do \
			printf '  word_reader_%s:\n' "$$i"; \
			printf '    build:\n      context: ./src/\n      dockerfile: services/reader/Dockerfile\n'; \
			printf '    container_name: word_reader_%s\n' "$$i"; \
			printf '    environment:\n      MOM_HOST: rabbitmq\n      WORD_READER_OUTPUT_QUEUE: word_reader_queue\n      WORD_READER_PREFIX: word_reader\n'; \
			printf '      WORD_READER_AMOUNT: "$(WORD_READERS)"\n      ID: "%s"\n' "$$i"; \
			printf '      SUM_WORDS_QUEUE_PREFIX: sum_words_queue\n      SUM_WORDS_COUNT: "$(SUM_COUNTS)"\n      JOIN_QUEUE: join_queue\n      SUM_AMOUNT_QUEUE: sum_amount_queue\n'; \
			printf '    depends_on:\n      rabbitmq:\n        condition: service_healthy\n'; \
		done; \
		for i in $$(seq 0 $$(( $(SUM_COUNTS) - 1 ))); do \
			printf '  sum_%s:\n' "$$i"; \
			printf '    build:\n      context: ./src/\n      dockerfile: services/sum_count/Dockerfile\n'; \
			printf '    container_name: sum_%s\n' "$$i"; \
			printf '    environment:\n      MOM_HOST: rabbitmq\n      SUM_WORDS_QUEUE_PREFIX: sum_words_queue\n'; \
			printf '      SUM_WORDS_COUNT: "$(SUM_COUNTS)"\n      SUM_COUNT_PREFIX: sum_count\n      COORDINATOR_ID: "0"\n      GATEWAY_PREFIX: gateway\n      CHUNK_SIZE: "1000"\n'; \
			printf '      ID: "%s"\n' "$$i"; \
			printf '    depends_on:\n      rabbitmq:\n        condition: service_healthy\n'; \
		done; \
		printf '  download:\n'; \
		printf '    build:\n      context: .\n      dockerfile: src/services/client/Dockerfile\n'; \
		printf '    container_name: download\n'; \
		printf '    profiles: ["data-setup"]\n'; \
		printf '    entrypoint: ["python", "scripts/download_dataset.py"]\n'; \
		printf '    environment:\n      FILES_PER_LANGUAGE: $${FILES_PER_LANGUAGE:-5}\n      WIKI_LANGUAGES: $${WIKI_LANGUAGES:-en,fr}\n      MAX_ROWS: $${MAX_ROWS:-0}\n'; \
		printf '    volumes:\n      - ./input:/app/input\n      - kaggle-cache:/cache/kagglehub\n'; \
		printf 'volumes:\n  kaggle-cache:\n'; \
	} > docker-compose.yaml
	@echo "Configuración generada en docker-compose.yaml"

up: generate
	$(COMPOSE) up --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down
