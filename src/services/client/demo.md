# Ejecutar el cliente

Abrí Docker Desktop. Ejecutá los siguientes pasos desde la raíz del repositorio,
en la misma terminal.

## 1. Entrar al proyecto

```sh
cd /Users/lautarotorraca/Documents/WikiDistri
```

Si necesitás regenerar el Compose, hacelo antes de continuar:

```sh
.venv/bin/python scripts/generate_docker_compose.py
```

Esto es opcional; el Compose incluido ya está listo para usar.

## 2. Construir las imágenes

Solo la primera vez o después de modificar código:

```sh
docker compose build
```

## 3. Levantar los servicios

```sh
docker compose up -d --wait \
  rabbitmq gateway \
  filter_0 filter_1 filter_2 \
  image_downloader_0 image_downloader_1 image_downloader_2 \
  word_reader_0 word_reader_1 word_reader_2 word_reader_3 \
  sum_count_0 sum_count_1 sum_count_2 sum_count_3 join_top_0 \
  sum_amount_0 sum_amount_1 sum_amount_2 \
  average_0 average_1 average_2 \
  join_average_0 join_average_1 join_average_2
```

Comprobá que RabbitMQ esté `healthy` y los demás servicios estén `Up`:

```sh
docker compose ps -a
```

## 4. Ejecutar la muestra de 42 artículos

```sh
docker compose run --rm --no-deps \
  -v "$PWD/examples/demo:/app/demo:ro" \
  -e INPUT_PARQUET=/app/demo/articles.parquet \
  -e MAX_ROWS=0 \
  -e BATCH_SIZE=7 \
  -e CLIENT_ID=demo \
  client_0
echo $?
```

Esperá a que termine: el último número debe ser **0**.
`INPUT_PARQUET` selecciona el archivo local y evita descargar otros datasets.
Para usar otro Parquet de `input/`, reemplazá esa variable por
`-e INPUT_PARQUET=/app/input/tu_archivo.parquet`.

## Puertos y errores de inicio

El Compose y su generador usan estos puertos de la computadora:

| Servicio | Puerto publicado | Puerto dentro de Docker |
| --- | --- | --- |
| Gateway | 5001 | 5000 |
| RabbitMQ | 5673 | 5672 |
| Administración RabbitMQ | 15673 | 15672 |

El puerto 5000 suele estar ocupado por Control Center de macOS; el gateway usa
5001 para evitar ese conflicto. Los clientes de Docker se conectan a
`gateway:5000`. La administración de RabbitMQ está en
[localhost:15673](http://localhost:15673).

Si tenías contenedores creados con los puertos anteriores, aplicá la configuración:

```sh
docker compose up -d --wait rabbitmq gateway
```

Para revisar un error:

```sh
docker compose logs --tail=60 rabbitmq gateway
```
