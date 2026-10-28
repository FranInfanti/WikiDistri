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

## Ejecutar el Job 1

El Job 1 devuelve la URL y la fecha de última modificación de los artículos en inglés modificados en 2020 o antes. Ejecutá estos pasos desde la raíz del proyecto, con Docker Desktop abierto.

### 1. Construir las imágenes

```sh
docker compose build download client rabbitmq gateway filter_0
```

### 2. Descargar una muestra de Wikipedia

```sh
MAX_ROWS=1000 FILES_PER_LANGUAGE=1 docker compose run --rm download
```

Esto genera `input/articles.csv` con hasta 1000 artículos reales. Se descarga al menos un Parquet completo para obtener esas filas. Para procesar el dataset completo, reemplazá ese comando por:

```sh
FILES_PER_LANGUAGE=all docker compose run --rm download
```

### 3. Levantar los servicios

Si no usás macOS y el puerto 5000 está libre, seguí este paso sin modificar ni crear archivos YAML:

```sh
docker compose up -d --wait rabbitmq gateway filter_0
docker compose logs --tail 20 gateway
```

En macOS, si el puerto 5000 está ocupado por `ControlCenter`, prepará una configuración local que habilite escritura en `output` y evite publicar ese puerto en la Mac. El cliente se conecta a `gateway:5000` dentro de Docker. El YAML original no se modifica:

```sh
cat > /tmp/wikidistri-local.yaml <<'YAML'
services:
  gateway:
    ports: !reset []
  client:
    volumes: !override
      - ./input:/app/input:ro
      - ./output:/app/output:rw
    environment:
      OUTPUT_DIR: /app/output
YAML

docker compose -f docker-compose.yaml -f /tmp/wikidistri-local.yaml \
  up -d --wait rabbitmq gateway filter_0
docker compose logs --tail 20 gateway
```

El Gateway debe mostrar `Listening to connections` antes de ejecutar el cliente. Si elegiste la alternativa para macOS, usá ambos archivos `-f` al levantar los servicios y ejecutar el cliente. Si se elimina el archivo temporal, volvé a crearlo con el comando anterior.

### 4. Ejecutar el cliente

Si seguiste los pasos sin crear un YAML local:

```sh
docker compose run --rm --no-deps \
  --volume "$PWD/output:/app/results:rw" \
  --env OUTPUT_DIR=/app/results \
  client
```

Se monta la salida en `/app/results` para evitar el montaje de solo lectura que el Compose original tiene en `/app/output`. Los archivos quedan igualmente en `output/` en tu máquina. Este permiso de escritura es necesario en cualquier sistema operativo.

Si creaste la configuración local para macOS:

```sh
docker compose -f docker-compose.yaml -f /tmp/wikidistri-local.yaml \
  run --rm --no-deps client
```

`!override` reemplaza los montajes del cliente para que `output` tenga permiso de escritura; pasar solamente `--volume` a `run` no quitó el modo de solo lectura en la versión de Compose utilizada. El cliente envía los CSV de `input/` por batches mientras otro thread recibe y guarda las respuestas del Gateway.

### 5. Revisar los resultados

```sh
cat output/query_1.csv
docker compose logs gateway filter_0
```

Cada fila de `query_1.csv` contiene `URL,fecha_de_modificación`. El archivo puede quedar vacío si ningún artículo cumple la condición. En los logs del Gateway, `articles_sent` indica cuántos artículos recibió y `All jobs finished` confirma el fin de los Jobs configurados.

La configuración actual usa `JOBS: 1`: sólo ejecuta el Job 1 y los archivos de consultas 2 a 5 quedan vacíos. Los archivos conservan `.partial` si la ejecución falla; al finalizar correctamente se guardan como `query_1.csv` a `query_5.csv`. Cada ejecución reemplaza los resultados anteriores.

### 6. Detener los servicios

```sh
docker compose stop
```

Para procesar nuevamente el CSV descargado, repetí desde el paso 3.

## Probar el recorrido completo del Job 1

Esta prueba ejecuta el cliente, el Gateway, RabbitMQ y el filtro reales. Envía cuatro artículos desde dos CSV: dos cumplen la condición del Job 1 (inglés y año de modificación menor o igual a 2020), uno está en francés y otro es de 2021. El resultado esperado son dos filas.

Los archivos de esta prueba ya están preparados en `output/job1-demo/`:

- `input/a.csv` e `input/b.csv`: artículos de entrada, sin encabezado.
- `expected.csv`: las dos filas esperadas.
- `compose.json`: configuración del entorno de prueba, con `JOBS=1` y salida con permiso de escritura.
- `run_client.py`: ejecuta el cliente y registra el inicio de ambos threads. Pausa el envío después del primer batch hasta recibir un resultado real, para comprobar la recepción durante la carga.

Estos archivos son locales: `output/` está excluido de Git. Los comandos siguientes suponen que conservás esa carpeta en la ubicación donde se preparó la prueba.

### 1. Construir las imágenes

Con Docker Desktop abierto, desde la raíz del repositorio:

```sh
docker compose -f output/job1-demo/compose.json build
```

### 2. Levantar RabbitMQ, el Gateway y el filtro

```sh
docker compose -f output/job1-demo/compose.json up -d --wait rabbitmq gateway filter_0
docker compose -f output/job1-demo/compose.json ps
docker compose -f output/job1-demo/compose.json logs --tail 20 gateway
```

RabbitMQ debe figurar como `healthy` y el Gateway debe mostrar `Listening to connections` antes de ejecutar el cliente.

### 3. Ejecutar el cliente

```sh
docker compose -f output/job1-demo/compose.json run -T --rm client
```

La salida debe incluir estos mensajes; los horarios y números de thread pueden variar:

```text
Thread-1 (send_batches): Sender thread started
Thread-2 (receive_results): Receiver thread started
Thread-2 (receive_results): Received real Job 1 result; upload_finished=False
Thread-2 (receive_results): Received Gateway EOF
MainThread: PASS: 4 articles sent from 2 CSVs, both threads started, real Job 1 received during upload, 2 expected rows saved
```

`upload_finished=False` comprueba que llegó un resultado mientras el envío todavía estaba abierto. El mensaje `PASS` confirma que el cliente terminó correctamente y que el archivo guardado coincide con el resultado esperado. La pausa y estos registros pertenecen al ejecutor de la prueba; el cliente de producción no se modifica.

### 4. Revisar los resultados y los logs

```sh
cat output/job1-demo/results/query_1.csv
diff -u -w output/job1-demo/expected.csv output/job1-demo/results/query_1.csv
docker compose -f output/job1-demo/compose.json logs gateway filter_0
```

El archivo debe contener:

```csv
https://example.org/en-2019,2019-01-02 03:04:05
https://example.org/en-2020,2020-12-31 23:59:59
```

`diff` no debe mostrar diferencias; `-w` ignora las diferencias de espacios y finales de línea entre ambos CSV. En los logs, el Gateway debe informar `articles_sent=4` y `All jobs finished`; el filtro debe enviar dos resultados. Los archivos de consultas 2 a 5 quedan vacíos porque esta prueba sólo ejecuta el Job 1. Si el cliente falla, los archivos conservan el sufijo `.partial`.

### 5. Detener el entorno de prueba

```sh
docker compose -f output/job1-demo/compose.json stop
```

Para repetir la prueba, volvé al paso 2. El cliente reemplaza los archivos de resultados de la prueba anterior. El Compose original del proyecto permanece igual; este entorno usa su propia configuración y red.
