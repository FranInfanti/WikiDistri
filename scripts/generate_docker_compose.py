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
    word_readers: int,
    sum_counts: int,
    join_tops: int,
    sum_amounts: int,
    averages: int,
    join_averages: int,
    clients: int,
    debug: bool
) -> str:
    instances = {
        "filters": filters,
        "image_downloaders": image_downloaders,
        "word_readers": word_readers,
        "sum_counts": sum_counts,
        "join_averages": join_averages,
        "sum_amounts": sum_amounts,
        "join_tops": join_tops,
        "averages": averages,
        "clients": clients,
    }

    log_level = "INFO" if not debug else "DEBUG"

    for name, count in instances.items():
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
                "FILES_PER_LANGUAGE": "${FILES_PER_LANGUAGE:-1}",
                "WIKI_LANGUAGES": "${WIKI_LANGUAGES:-en,fr}",
                "DOWNLOAD_DIR": "/app/input/data",
            },
            "volumes": [
                "./input:/app/input",
            ],
        },
        "notebook": {
            "build": {
                "context": ".",
                "dockerfile": "./src/notebook/Dockerfile",
            },
            "profiles": ["notebook"],
            "container_name": "notebook",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "PER_LANGUAGE_SAMPLE_AMOUNT": "1",
                "INPUT_DIR": "/app/input",
                "OUTPUT_DIR": "/app/output",
            },
            "volumes": [
                "./input:/app/input",
                "./output:/app/output",
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
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "HOST": "gateway",
                "PORT": "5000",
                "JOBS": "5",
                "BATCH_SIZE": "1000",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "FILTER_QUEUE": "filter_queue",
                "WORD_READER_QUEUE": "word_reader_queue",
            },
            "ports": ["5000:5000"],
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }
    }

    for id in range(filters):
        service_name = f"filter_{id}"
        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/filter/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "BATCH_SIZE": "1000",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "FILTER_INSTANCES": str(filters),
                "FILTER_PREFIX": "filter",
                "FILTER_QUEUE": "filter_queue",
                "IMAGE_DOWNLOADER_QUEUE": "image_downloader_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(image_downloaders):
        service_name = f"image_downloader_{id}"
        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/downloader/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "IMAGE_DOWNLOADER_INSTANCES": str(image_downloaders),
                "IMAGE_DOWNLOADER_PREFIX": "image_downloader",
                "IMAGE_DOWNLOADER_QUEUE": "image_downloader_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(word_readers):
        service_name = f"word_reader_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/reader/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "MOM_HOST": "rabbitmq",
                "WORD_READER_INSTANCES": str(word_readers),
                "WORD_READER_PREFIX": "word_reader",
                "WORD_READER_QUEUE": "word_reader_queue",
                "SUM_COUNT_INSTANCES": str(sum_counts),
                "SUM_COUNT_QUEUE": "sum_count_queue",
                "SUM_AMOUNT_QUEUE": "sum_amount_queue",
                "JOIN_AVERAGE_INSTANCES": str(join_averages),
                "JOIN_AVERAGE_QUEUE": "join_average_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(sum_counts):
        service_name = f"sum_count_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/sum_count/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "COORDINATOR_ID": "0",
                "TOP_SIZE": "20",
                "CHUNK_SIZE": "1000",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "SUM_COUNT_INSTANCES": str(sum_counts),
                "SUM_COUNT_PREFIX": "sum_count",
                "SUM_COUNT_QUEUE": "sum_count_queue",
                "JOIN_TOP_INSTANCES": "1",
                "JOIN_TOP_QUEUE": "join_top_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(join_tops):
        service_name = f"join_top_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/join_top/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "TOP_SIZE": "20",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "JOIN_TOP_QUEUE": "join_top_queue",
                "SUM_COUNT_INSTANCES": str(sum_counts),
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(sum_amounts):
        service_name = f"sum_amount_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/sum_amount/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "MOM_HOST": "rabbitmq",
                "SUM_AMOUNT_INSTANCES": str(sum_amounts),
                "SUM_AMOUNT_PREFIX": "sum_amount",
                "SUM_AMOUNT_QUEUE": "sum_amount_queue",
                "AVERAGE_INSTANCES": str(averages),
                "AVERAGE_QUEUE": "average_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }
    
    for id in range(averages):
        service_name = f"average_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/average/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "MOM_HOST": "rabbitmq",
                "AVERAGE_QUEUE": "average_queue",
                "SUM_AMOUNT_INSTANCES": str(sum_amounts),
                "JOIN_AVERAGE_INSTANCES": str(join_averages),
                "JOIN_AVERAGE_QUEUE": "join_average_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }
    
    for id in range(join_averages):
        service_name = f"join_average_{id}"

        services[service_name] = {
            "build": {
                "context": "./src/",
                "dockerfile": "services/join_average/Dockerfile",
            },
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "LOG_LEVEL": log_level,
                "ID": str(id),
                "COORDINATOR_ID": "0",
                "CHUNK_SIZE": "1000",
                "MOM_HOST": "rabbitmq",
                "GATEWAY_PREFIX": "gateway",
                "JOIN_AVERAGE_INSTANCES": str(join_averages),
                "JOIN_AVERAGE_PREFIX": "join_average",
                "JOIN_AVERAGE_QUEUE": "join_average_queue",
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
            },
        }

    for id in range(clients):
        service_name = f"client_{id}"
        services[service_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/services/client/Dockerfile",
            },
            "profiles": ["client"],
            "container_name": service_name,
            "stop_grace_period": "30s",
            "environment": {
                "PYTHONUNBUFFERED": "1",
                "CLIENT_ID": f"client_{id}",
                "INPUT_DIR": "/app/input/data",
                "OUTPUT_DIR": "/app/output",
                "FILES_PER_LANGUAGE": "1",
                "WIKI_LANGUAGES": "en,fr",
                "MAX_ROWS": "0",
                "SERVER_HOST": "gateway",
                "SERVER_PORT": "5000",
                "BATCH_SIZE": "1000",
                "PARQUET_BATCH_SIZE": "2000"
            },
            "volumes": [
                "./input:/app/input",
                "./output:/app/output",
            ],
            "depends_on": {
                "gateway": {"condition": "service_started"},
            },
        }

    compose = {
        "services": services,
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
    parser.add_argument("--word-readers", type=positive_int, default=4, metavar="N")
    parser.add_argument("--image-downloaders", type=positive_int, default=3, metavar="N")
    parser.add_argument("--sum-counts", type=positive_int, default=4, metavar="N")
    parser.add_argument("--join-tops", type=positive_int, default=3, metavar="N")
    parser.add_argument("--sum-amounts", type=positive_int, default=3, metavar="N")
    parser.add_argument("--averages", type=positive_int, default=3, metavar="N")
    parser.add_argument("--join-averages", type=positive_int, default=3, metavar="N")
    parser.add_argument("--clients", type=positive_int, default=1, metavar="N")
    parser.add_argument("--jobs", type=positive_int, default=5, metavar="N")
    parser.add_argument(
        "--debug",
        type=bool,
        default=False
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT_DIR / "docker-compose.yml",
        help="Output file path",
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
            sum_amounts=args.sum_amounts,
            averages=args.averages,
            join_averages=args.join_averages,
            debug=args.debug
        ),
        encoding="utf-8",
    )

    return 0

if __name__ == "__main__":
    SystemExit(main())
