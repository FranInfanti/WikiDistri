import yaml
import argparse

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Must be an integer") from exc
    
    if number < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    
    return number

def build_compose(
    filters: int,
    image_downloaders: int,
    clients: int,
    word_readers: int,
    sum_counts: int,
    join_tops: int,
) -> str:
    counts = {
        "filters": filters,
        "image_downloaders": image_downloaders,
        "clients": clients,
        "word_readers": word_readers,
        "sum_counts": sum_counts,
        "join_tops": join_tops,
    }

    for name, count in counts.items():
        if count < 1:
            raise ValueError(f"{name} must be greater than zero")

    services = {
        "dataset-downloader": {
            "build": {
                "context": ".",
                "dockerfile": "./src/services/client/Dockerfile",
            },
            "profiles": ["data-setup"],
            "container_name": "dataset-downloader",
            "entrypoint": ["python", "scripts/download_dataset.py"],
            "environment": {
                "FILES_PER_LANGUAGE": "${FILES_PER_LANGUAGE:-5}",
                "WIKI_LANGUAGES": "${WIKI_LANGUAGES:-en,fr}",
                "MAX_ROWS": "${MAX_ROWS:-0}",
            },
            "volumes": [
                "./input:/app/input",
                "kaggle-cache:/cache/kagglehub",
            ],
        },
        "rabbitmq": {
            "build": {
                "context": "./src/services/rabbitmq",
                "dockerfile": "Dockerfile",
            },
            "container_name": "rabbitmq",
            "environment": {
                "RABBITMQ_LOG_LEVELS": "error",
            },
            "healthcheck": {
                "interval": "5s",
                "retries": 10,
                "start_period": "50s",
                "test": "rabbitmq-diagnostics check_port_connectivity",
                "timeout": "3s",
            },
            "ports": ["5672:5672", "15672:15672"],
        },
        "gateway": {
            "build": {
                "context": "./src/",
                "dockerfile": "services/gateway/Dockerfile",
            },
            "container_name": "gateway",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "HOST": "gateway",
                "PORT": "5000",
                "JOBS": "4",
                "BATCH_SIZE": "50",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "FILTER_QUEUE": "filter_queue",
                "WORD_READER_QUEUE": "word_reader_queue",
            },
            "ports": ["5000:5000"],
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        },
        "download": {
            "build": {
                "context": ".",
                "dockerfile": "src/services/client/Dockerfile",
            },
            "container_name": "download",
            "profiles": ["data-setup"],
            "entrypoint": ["python", "scripts/download_dataset.py"],
            "environment": {
                "FILES_PER_LANGUAGE": "${FILES_PER_LANGUAGE:-5}",
                "WIKI_LANGUAGES": "${WIKI_LANGUAGES:-en,fr}",
                "MAX_ROWS": "${MAX_ROWS:-0}",
            },
            "volumes": [
                "./input:/app/input",
                "kaggle-cache:/cache/kagglehub",
            ],
        },
    }

    for filter_id in range(filters):
        service_name = f"filter_{filter_id}"
        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/filter/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "ID": str(filter_id),
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "FILTER_AMOUNT": str(filters),
                "FILTER_PREFIX": "filter",
                "FILTER_QUEUE": "filter_queue",
                "IMAGE_DOWNLOADER_QUEUE": "image_downloader_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for downloader_id in range(image_downloaders):
        service_name = f"image_downloader_{downloader_id}"
        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/downloader/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "ID": str(downloader_id),
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "IMAGE_DOWNLOADER_AMOUNT": str(image_downloaders),
                "IMAGE_DOWNLOADER_PREFIX": "image_downloader",
                "IMAGE_DOWNLOADER_QUEUE": "image_downloader_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for client_id in range(clients):
        service_name = f"client_{client_id}"
        services[service_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/services/client/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "INPUT_FILE": "/app/input/articles.csv",
                "SERVER_HOST": "gateway",
                "SERVER_PORT": "5000",
                "BATCH_SIZE": "100",
            },
            "volumes": [
                "./input:/app/input:ro",
                "./output:/app/output",
            ],
            "depends_on": {
                "gateway": {"condition": "service_started"},
            },
        }

    for reader_id in range(word_readers):
        service_name = f"word_reader_{reader_id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/reader/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "MOM_HOST": "rabbitmq",
                "WORD_READER_OUTPUT_QUEUE": "word_reader_queue",
                "WORD_READER_PREFIX": "word_reader",
                "WORD_READER_AMOUNT": str(word_readers),
                "ID": str(reader_id),
                "SUM_WORDS_QUEUE_PREFIX": "sum_words_queue",
                "SUM_WORDS_COUNT": str(sum_counts),
                "JOIN_QUEUE": "join_queue",
                "SUM_AMOUNT_QUEUE": "sum_amount_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for sum_id in range(sum_counts):
        service_name = f"sum_{sum_id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/sum_count/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "MOM_HOST": "rabbitmq",
                "SUM_WORDS_QUEUE_PREFIX": "sum_words_queue",
                "SUM_WORDS_COUNT": str(sum_counts),
                "SUM_COUNT_PREFIX": "sum_count",
                "COORDINATOR_ID": "0",
                "GATEWAY_PREFIX": "gateway",
                "CHUNK_SIZE": "1000",
                "JOIN_TOP_QUEUE": "join_top",
                "JOIN_TOP_COUNT": "1",
                "ID": str(sum_id),
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for join_top_id in range(join_tops):
        service_name = f"join_top_{join_top_id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/join_top/Dockerfile",
            },
            "container_name": service_name,
            "environment": {
                "MOM_HOST": "rabbitmq",
                "JOIN_TOP_QUEUE": "join_top",
                "SUM_WORDS_COUNT": str(sum_counts),
                "GATEWAY_PREFIX": "gateway",
                "ID": str(join_top_id),
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    compose = {
        "services": services,
        "volumes": {
            "kaggle-cache": None,
        },
    }

    return yaml.safe_dump(
        compose,
        sort_keys=False,
        default_flow_style=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a docker-compose.yml file"
    )

    parser.add_argument("--filters", type=positive_int, default=3, metavar="N")
    parser.add_argument(
        "--image-downloaders",
        type=positive_int,
        default=1,
        metavar="N",
    )
    parser.add_argument("--clients", type=positive_int, default=1, metavar="N")
    parser.add_argument(
        "--word-readers",
        type=positive_int,
        default=4,
        metavar="N",
    )
    parser.add_argument(
        "--sum-counts",
        type=positive_int,
        default=4,
        metavar="N",
    )
    parser.add_argument("--jobs", type=positive_int, default=4, metavar="N")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT_DIR / "docker-compose.yml",
        help="Output file path",
    )

    parser.add_argument(
        "--join-tops",
        type=positive_int,
        default=1,
        metavar="N",
    )

    args = parser.parse_args(argv)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        build_compose(
            filters=args.filters,
            image_downloaders=args.image_downloaders,
            clients=args.clients,
            word_readers=args.word_readers,
            sum_counts=args.sum_counts,
            join_tops=args.join_tops,
        ),
        encoding="utf-8",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
