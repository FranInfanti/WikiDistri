# WikiDistri

To download a specific amount of files per language use:

```bash
docker compose run --build --rm -e FILES_PER_LANGUAGE=1 dataset-downloader
```

To run the client use:

```bash
docker compose up --build client_0
```

To run the whole system, including the clients, use:

```bash
docker compose up --build
```
