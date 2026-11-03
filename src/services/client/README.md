# Cliente

El cliente descarga los Parquet necesarios desde Kaggle y los lee por lotes con PyArrow. No genera ni necesita
un CSV intermedio. Un thread lee, normaliza y envía artículos, otro recibe y guarda
resultados sobre la misma conexión TCP.

## Ejecutar un flujo completo

Ejecutá los comandos desde la raíz del repositorio, con Docker Desktop iniciado.
El flujo usa los Parquet disponibles en `input/`. Si no hay archivos `.parquet`,
los descarga desde Kaggle y los conserva en esa carpeta. Un CSV existente en
`input/` no sirve como entrada del cliente.

### 1. Construir las imágenes

```sh
docker compose -f docker-compose.yml build
```

Hay que reconstruir cliente, gateway, filtros y lectores después de modificar el
protocolo para que todos usen el mismo formato.

### 2. Levantar todas las etapas de procesamiento

La configuración actual tiene tres filtros, cuatro lectores, cuatro sumadores,
un descargador de imágenes y un join de top. Levantá todas sus réplicas antes de
ejecutar el cliente:

Si estás en macOS y el puerto 5000 está ocupado, o Docker informa un conflicto de
puertos al iniciar RabbitMQ, aplicá los comandos de la sección 5 y volvé a
ejecutar este paso.

```sh
docker compose -f docker-compose.yml up -d --wait \
  rabbitmq gateway \
  filter_0 filter_1 filter_2 \
  image_downloader_0 \
  word_reader_0 word_reader_1 word_reader_2 word_reader_3 \
  sum_0 sum_1 sum_2 sum_3 \
  join_top_0
```

No se inicia todavía `client_0` ni hace falta ejecutar `download` o
`dataset-downloader`. El propio cliente obtiene los Parquet si los necesita.

Es importante levantar las tres réplicas de filtro. Si solo se ejecuta
`filter_0` pero `FILTER_AMOUNT=3`, las etapas pueden quedarse esperando la
coordinación del EOF. Lo mismo aplica a los lectores y sumadores. Si cambiás las
cantidades en Compose, adaptá la lista de servicios.

Comprobá el estado y los logs antes de enviar datos:

```sh
docker compose -f docker-compose.yml ps -a
docker compose -f docker-compose.yml logs --tail=60 \
  gateway filter_0 filter_1 filter_2 \
  image_downloader_0 \
  word_reader_0 word_reader_1 word_reader_2 word_reader_3 \
  sum_0 sum_1 sum_2 sum_3 join_top_0
```

RabbitMQ debe estar `healthy` y los demás servicios deben estar `Up`. Si alguno
aparece como `Exited`, revisá su log antes de continuar. `--wait` comprueba el
estado de los contenedores, pero no garantiza que la aplicación no haya fallado
después de arrancar.

### 3. Ejecutar el cliente con un Parquet por idioma

```sh
docker compose -f docker-compose.yml run --rm --no-deps \
  -e BATCH_SIZE=50 -e FILES_PER_LANGUAGE=1 client_0
echo $?
```

El resultado de `echo $?` debe ser `0`. `FILES_PER_LANGUAGE=1` limita la entrada a
un Parquet por idioma. Con `WIKI_LANGUAGES=en,fr`, el cliente procesa primero un
Parquet inglés, luego uno francés y continúa alternándolos. Procesa todas las filas
de esos archivos. Después envía un único EOF y espera los resultados y el EOF
global del gateway. No interrumpas el cliente al terminar la subida, todavía puede
estar recibiendo resultados o imágenes.

Si necesitás elegir un Parquet concreto que ya está en `input/`:

```sh
docker compose -f docker-compose.yml run --rm --no-deps \
  -e BATCH_SIZE=50 -e INPUT_PARQUET=/app/input/articles.parquet client_0
```

Reemplazá `articles.parquet` por la ruta real dentro de `input/`. El archivo se
lee desde disco por lotes. Para limitar filas en una prueba, podés pasar
`-e MAX_ROWS=200`; el valor predeterminado `0` procesa todas las filas de los
Parquet seleccionados.

### 4. Revisar los resultados de las consultas

El log del cliente informa la carpeta exacta, por ejemplo:

```text
Client client_0 output: /app/output/client_0/run_<id>
```

En tu computadora corresponde a `output/client_0/run_<id>/`. Listá las ejecuciones
para ubicar la más reciente:

```sh
ls -lt output/client_0
```

Dentro de la carpeta de esa ejecución vas a encontrar:

| Archivo | Resultado |
| --- | --- |
| `query_1.csv` | URL y fecha de artículos en inglés modificados hasta 2020 inclusive. |
| `query_2.csv` | Nombre, URL y ruta local de imagen para artículos anteriores a 2020 con sección de biografía. La ruta queda vacía si no se obtuvo una imagen. |
| `query_3.csv` | Palabras presentes en artículos de ambos idiomas, después del filtrado de stopwords. |
| `query_4.csv` | Top de palabras por cantidad de artículos en los que aparecen, sumando ambos idiomas. |
| `query_5.csv` | Vacío en la configuración actual. |
| `images/` | Imágenes guardadas cuando la consulta 2 devuelve contenido descargado. |

La ejecución finalizó correctamente cuando el cliente sale con código `0` y los
archivos `query_1.csv` a `query_5.csv` ya no tienen la extensión `.partial`.
Un archivo vacío no indica necesariamente una falla: puede no haber artículos
que cumplan el filtro en los Parquet seleccionados.

El gateway está configurado con `JOBS=4`: espera las consultas 1 a 4. La consulta
5 no tiene un consumidor de resultados implementado en este pipeline. Cambiar
solamente `JOBS` a 5 dejaría al cliente esperando un EOF que no se produce.

Los Parquet se procesan alternando idiomas, así que con `WIKI_LANGUAGES=en,fr`
se leen ambos idiomas cuando hay archivos disponibles para ambos. `query_3.csv`
puede quedar vacío si los artículos seleccionados no tienen palabras en común
después de filtrar stopwords; el flujo igual debe terminar correctamente.

### 5. Diagnosticar una ejecución que no termina

Desde otra terminal:

```sh
docker compose -f docker-compose.yml ps -a
docker compose -f docker-compose.yml logs -f --tail=100
```

Revisá servicios detenidos, errores de conexión, excepciones y cantidades de
réplicas que no coincidan con los servicios levantados. El gateway debe registrar
`Upload completed` al recibir todos los artículos y `All jobs finished` cuando
terminan las cuatro consultas. Los `.partial` conservan lo recibido si la
ejecución falla.

#### macOS: puerto 5000 ocupado

Si aparece `Ports are not available` o `bind: address already in use` para el
puerto 5000, ejecutá desde la raíz del repositorio:

```sh
sed -i '' 's/5000:5000/5001:5000/g' docker-compose.yml
```

Este comando usa la sintaxis de `sed` de macOS. Cambia el puerto publicado en tu
Mac a 5001; los clientes dentro de Docker siguen conectándose a `gateway:5000`.
Después volvé a ejecutar el comando del paso 2 para levantar las etapas.

#### RabbitMQ: puertos ocupados u otro error de inicio

Si Docker informa `Ports are not available` o `bind: address already in use`
para los puertos 5672 o 15672 de RabbitMQ, en macOS ejecutá:

```sh
sed -i '' -e 's/5672:5672/5673:5672/g' -e 's/15672:15672/15673:15672/g' docker-compose.yml
```

En Linux, usá el mismo comando quitando `''` después de `-i`. Los servicios
dentro de Docker siguen usando `rabbitmq:5672`. La interfaz de administración
pasa a estar disponible en `http://localhost:15673`. Después repetí el paso 2.

Si RabbitMQ aparece como `unhealthy` o `Exited` y el error no es de puertos,
consultá el motivo antes de continuar:

```sh
docker compose -f docker-compose.yml logs --tail=100 rabbitmq
```

Los cambios de puertos se aplican al `docker-compose.yml` actual. Si lo
regenerás con `scripts/generate_docker_compose.py`, volvé a aplicar los comandos
necesarios antes de levantar los servicios.

### 6. Detener el sistema

```sh
docker compose -f docker-compose.yml down
```

Los Parquet de `input/` y los resultados de `output/` se conservan en la
computadora. Para repetir el flujo podés dejar las etapas levantadas y ejecutar
otra vez el comando del cliente. Cada ejecución tiene su propia carpeta.

## Configuración del cliente

Variables del cliente:

| Variable | Uso |
| --- | --- |
| `MAX_ROWS` | Límite opcional de filas procesadas por cliente; `0` (predeterminado) procesa todas las filas de los Parquet seleccionados. |
| `WIKI_LANGUAGES` | Idiomas a descargar: `en,fr` por defecto, también acepta `en` o `fr`. |
| `FILES_PER_LANGUAGE` | Máximo de Parquet por idioma, `5` por defecto. Los archivos se alternan entre idiomas y `all` procesa todos los disponibles. |
| `INPUT_PARQUET` | Archivo Parquet local específico, tiene prioridad sobre `INPUT_DIR`. |
| `INPUT_DIR` | Directorio con Parquet locales, buscados recursivamente, al indicarlo se omite Kaggle. |
| `PARQUET_BATCH_SIZE` | Filas por lectura de PyArrow. Por defecto usa `BATCH_SIZE` y nunca lo supera. |
| `BATCH_SIZE` | Artículos por mensaje TCP, entre `1` y `65535`, `50` por defecto. También se respeta el límite de bytes del protocolo. |
| `BATCH_BYTES` | Límite de bytes del batch de artículos completos, 8 MiB por defecto. También respeta los límites de TCP y RabbitMQ. Un artículo que lo excede produce un error. |
| `DOWNLOAD_DIR` | Directorio de descarga, `input` por defecto y `/app/input` en Docker. |
| `CLIENT_ID` | Identificador para separar salidas. Compose asigna `client_0`, `client_1`, etc. Sin valor se genera uno único. |
| `OUTPUT_DIR` | Raíz de resultados, `/app/output` en Compose. |

Opcionalmente, para reutilizar un Parquet local específico de `input/` sin descargar:

```sh
docker compose run --rm --no-deps -e INPUT_PARQUET=/app/input/articles.parquet client_0
```

## Dos clientes en paralelo

Generá la configuración con la cantidad de clientes y levantá todos los servicios:

```sh
python3 -m pip install PyYAML
python3 scripts/generate_docker_compose.py --clients 2
docker compose up --build
```

El generador necesita PyYAML instalado en el Python local.
Cada cliente procesa independientemente el conjunto configurado, los datos no se
reparten entre clientes.

Los resultados se guardan por cliente y por ejecución:

```text
output/
  client_0/run_<id>/query_1.csv ... query_5.csv
  client_0/run_<id>/images/
  client_1/run_<id>/query_1.csv ... query_5.csv
  client_1/run_<id>/images/
```

Cada ejecución mantiene su carpeta independiente, como se describe en el flujo
completo. La configuración actual espera cuatro consultas.

## Protocolo de subida

El cliente prepara un único batch de artículos completos y lo envía una sola vez.
Se mantiene el encabezado TCP de siete bytes: opcode de 1 byte, cantidad de
artículos de 2 bytes y longitud del payload de 4 bytes, en big endian.

```text
payload DATA = [longitud artículo: uint32][artículo] ...
artículo = [longitud columna: uint32][columna UTF-8] ... (7 columnas)
```

Orden fijo: `abstract`, `name`, `url`, `date_modified`, `image`, `in_language`,
`sections`. Las longitudes cuentan bytes UTF-8. `0` representa texto vacío,
`0xffffffff` representa `None` y no lleva contenido.

El cliente extrae `image.content_url` e `in_language.identifier`, formatea la fecha
como `YYYY-MM-DD HH:MM:SS` y serializa las secciones como JSON. El gateway valida
el sobre y agrega el contexto de solicitud, sin decodificar campos ni generar
JSON con sus datos. Al publicar, recorre las longitudes y copia los bloques
binarios de columnas que necesita cada rama. Recalcula el prefijo del artículo
proyectado sin modificar los prefijos ni el contenido de las columnas.

El mensaje DATA en RabbitMQ lleva `[WDB2: 4 bytes][req_id: uint64]
[cantidad: uint16][batch]`, en big endian. Este formato se define en `internal.py`.
El marcador `WDB2` identifica DATA con artículos proyectados, reemplaza la versión
que enviaba artículos completos a ambos workers.
Los mensajes internos de control, resultados y demás enlaces conservan JSON.

Filtro y lector desarman su batch al consumirlo. Cada uno recibe y decodifica
solamente sus columnas, en este orden:

- Filtro: `name`, `url`, `date_modified`, `image`, `in_language`, `sections`.
- Lector: `abstract`, `url`, `date_modified`, `in_language`.

El cliente envía un único EOF de subida, con cantidad cero y payload vacío.
El gateway mantiene `Client.articles_sent`, lo incrementa una sola vez después de
publicar cada batch en ambas ramas y envía ese total en el EOF de cada cola.
Una entrada vacía publica total cero en ambas ramas. Las respuestas al cliente
conservan el protocolo anterior, incluido el EOF global vacío.

El formato reemplaza la variante de artículos completos en ambas colas: hay que reconstruir las
imágenes de cliente, gateway, filtro y lector juntas.

## CSV para debug

El exportador original sigue disponible de forma opcional:

```sh
MAX_ROWS=5 FILES_PER_LANGUAGE=1 docker compose run --rm dataset-downloader
```

Este script conserva la exportación original y genera `input/articles.csv` con
las columnas originales del Parquet, sin encabezado ni índice. La descarga se hace
con KaggleHub directamente a `input/`. El CSV es opcional para inspección. Sigue siendo una herramienta de inspección y debug.
El cliente descarga y procesa los Parquet directamente, o usa los locales si
se indica `INPUT_DIR` o `INPUT_PARQUET`, el CSV exportado no es su entrada.
