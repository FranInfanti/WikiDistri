"""Columnas UTF-8 codificadas por el cliente y decodificadas por los workers."""

from . import constants
from .batching import unbatch_bytes, unbatch_views

# El orden coincide con WikipediaArticle.to_list(). UINT32_MAX representa un nulo.
COLUMNS = ("abstract", "name", "url", "date_modified", "image", "in_language", "sections")
FILTER_COLUMNS = ("name", "url", "date_modified", "image", "in_language", "sections")
READER_COLUMNS = ("abstract", "url", "date_modified", "in_language")
NULL = constants.NULL_FIELD_LENGTH
COLUMN_PREFIX_SIZE = constants.MAX_PREFIX_LENGTH


def encode_columns(values):
    """Codifica los siete campos con un prefijo que indica su longitud en bytes."""

    if len(values) != len(COLUMNS):
        raise ValueError(f"Expected {len(COLUMNS)} article columns")

    parts = []

    for value in values:
        if value is None:
            parts.append(NULL.to_bytes(COLUMN_PREFIX_SIZE, constants.ENDIANNESS))
        else:
            data = value.encode(constants.ENCODING)

            if len(data) >= NULL:
                raise ValueError("Article column too large")

            parts.extend((len(data).to_bytes(COLUMN_PREFIX_SIZE, constants.ENDIANNESS), data))

    return b"".join(parts)


def _column_blocks(payload, columns):
    """Ubica los bloques de campos sin interpretar ni copiar su contenido."""

    view = memoryview(payload)
    blocks = {}
    offset = 0

    for column in columns:
        start = offset

        if offset + COLUMN_PREFIX_SIZE > len(payload):
            raise ValueError("Truncated article column length")

        # La vista incluye el prefijo para reutilizarlo al proyectar la columna.
        size = int.from_bytes(view[offset:offset + COLUMN_PREFIX_SIZE], constants.ENDIANNESS)
        offset += COLUMN_PREFIX_SIZE

        # Un campo nulo contiene solamente el prefijo, sin bytes de contenido.
        if size != NULL:
            if offset + size > len(payload):
                raise ValueError("Truncated article column")

            offset += size

        blocks[column] = view[start:offset]

    if offset != len(payload):
        raise ValueError("Unexpected bytes after article columns")

    return blocks


def decode_columns(payload, columns=COLUMNS, *, source_columns=COLUMNS):
    """Decodifica en el worker los campos de un artículo completo o proyectado."""

    if not set(columns).issubset(source_columns):
        raise ValueError("Unknown article columns")

    blocks = _column_blocks(payload, source_columns)
    values = {}

    for column in columns:
        block = blocks[column]
        size = int.from_bytes(block[:COLUMN_PREFIX_SIZE], constants.ENDIANNESS)
        values[column] = (
            None
            if size == NULL
            else bytes(block[COLUMN_PREFIX_SIZE:]).decode(constants.ENCODING)
        )

    return [values[column] for column in columns]


def decode_batch(payload, batches, columns=COLUMNS):
    """Valida y decodifica el batch completo antes de ejecutar las consultas."""

    return [
        decode_columns(record, columns, source_columns=columns)
        for record in unbatch_bytes(payload, batches)
    ]


def project_batches(payload, batches, projections=(FILTER_COLUMNS, READER_COLUMNS)):
    """Construye los batches de cada rama copiando bloques binarios de columnas."""

    if any(not columns or not set(columns).issubset(COLUMNS) for columns in projections):
        raise ValueError("Invalid article projection")

    outputs = [bytearray() for _ in projections]

    # Se recorren los prefijos una sola vez por artículo, sin decodificar texto.
    for record in unbatch_views(payload, batches):
        blocks = _column_blocks(record, COLUMNS)

        for output, columns in zip(outputs, projections):
            # Cambia el tamaño del artículo proyectado, pero no el de sus columnas.
            size = sum(len(blocks[column]) for column in columns)
            output.extend(size.to_bytes(COLUMN_PREFIX_SIZE, constants.ENDIANNESS))

            for column in columns:
                output.extend(blocks[column])

    # Ambas proyecciones se devuelven después de validar todos los artículos.
    # Así, un registro truncado impide que el gateway publique una sola rama.
    return tuple(bytes(output) for output in outputs)
