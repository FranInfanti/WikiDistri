# Cliente

Ejecutá los comandos desde la raíz del proyecto, con Docker Desktop abierto.

## Descargar todos los datos

```sh
docker compose build download client
FILES_PER_LANGUAGE=all docker compose run --rm download
```

`all` descarga el dataset completo y genera `input/articles.csv` con los artículos de inglés y francés. Necesitás espacio para los Parquet en la caché de Docker y para el CSV generado.

## Descargar 1 o N Parquet

```sh
FILES_PER_LANGUAGE=1 docker compose run --rm download
FILES_PER_LANGUAGE=3 docker compose run --rm download
```

La cantidad se aplica **por idioma**: los ejemplos descargan 2 y 6 Parquet, respectivamente. Para descargar uno solo, elegí también el idioma:

```sh
WIKI_LANGUAGES=en FILES_PER_LANGUAGE=1 docker compose run --rm download
```

Para generar un CSV de solo cinco filas:

```sh
MAX_ROWS=5 docker compose run --rm download
```

El límite cuenta filas totales. Se descarga al menos un Parquet completo, aunque el CSV tenga solo cinco filas. El archivo `input/articles.csv` tiene siete columnas de artículos, sin encabezado ni índice numérico.

## Ejecutar el cliente

```sh
docker compose run --rm client
```

