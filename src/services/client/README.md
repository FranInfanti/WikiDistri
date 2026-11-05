# Cliente: datasets de Kaggle

El cliente procesa los Parquet del dataset de Kaggle
`wikimedia-foundation/wikipedia-structured-contents`. Se usan los artículos de
Wikipedia en **inglés (`en`) y francés (`fr`)**.

En una misma ejecución descarga cada Parquet que falte en `input/`, envía sus
artículos al gateway por batches y continúa con el siguiente archivo. Los
Parquet ya descargados se reutilizan.

Ejecutá los comandos desde la raíz del repositorio, con Docker Desktop iniciado
y conexión a Internet. Para ejecutar la muestra de 42 artículos, seguí
[demo.md](demo.md).

## 1. Construir las imágenes

```sh
docker compose build
```

Repetí este paso si modificás el código o las dependencias.

## 2. Levantar los servicios de procesamiento

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

Verificá el estado con `docker compose ps -a`: RabbitMQ debe estar `healthy`
y las demás etapas, en ejecución.

## 3. Descargar y enviar con el cliente

Con los servicios levantados, este único comando descarga y envía todos los
artículos de cinco archivos en inglés y cinco en francés (diez en total):

```sh
docker compose run --rm --no-deps \
  -e WIKI_LANGUAGES=en,fr \
  -e FILES_PER_LANGUAGE=5 \
  -e MAX_ROWS=0 \
  -e BATCH_SIZE=100 \
  -e PARQUET_BATCH_SIZE=50 \
  client_0
```



## Resultados y cierre

Los resultados quedan en `output/client_0/run_*/`: cinco archivos
`query_1.csv` a `query_5.csv` y las imágenes descargadas en `images/`.
Los CSV completos no tienen el sufijo `.partial`.

```sh
docker compose down
```

Los archivos de `input/` y los resultados de `output/` se conservan.
Para revisar errores, ejecutá `docker compose logs --tail=60 rabbitmq gateway`.
