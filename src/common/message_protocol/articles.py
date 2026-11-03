from common import message_protocol

COLUMNS = ("abstract", "name", "url", "date_modified", "image", "in_language", "sections")

_FILTER_COLUMNS = ("name", "url", "date_modified", "image", "in_language", "sections")
_READER_COLUMNS = ("abstract", "url", "date_modified", "in_language")

_COLUMN_PREFIX_LENGTH = 4
_NULL = message_protocol.MAX_PAYLOAD_SIZE

def encode_record(record: list):
    """Codifica cada campo UTF-8 precedido por su longitud hexadecimal en ASCII."""

    if len(record) != len(COLUMNS):
        raise ValueError(f"Expected {len(COLUMNS)} article columns")

    parts = []
    for value in record:
        if value is None:
            parts.append(_NULL.to_bytes(_COLUMN_PREFIX_LENGTH, message_protocol.ENDIANNESS))
        else:
            data = value.encode(message_protocol.ENCODING)
            if len(data) >= _NULL:
                raise ValueError("Article column too large")

            parts.extend((len(data).to_bytes(_COLUMN_PREFIX_LENGTH, message_protocol.ENDIANNESS), data))

    return b"".join(parts)

def decode_record(payload: bytes, columns: tuple = COLUMNS, *, source_columns: tuple = COLUMNS):
    """Decodifica en el worker los campos de un artículo completo o proyectado."""

    if not set(columns).issubset(source_columns):
        raise ValueError("Unknown article columns")

    blocks = _column_blocks(payload, source_columns)
    values = {}

    for column in columns:
        block = blocks[column]
        size = int.from_bytes(block[:_COLUMN_PREFIX_LENGTH], message_protocol.ENDIANNESS)
        values[column] = (
            None
            if size == _NULL
            else bytes(block[_COLUMN_PREFIX_LENGTH:]).decode(message_protocol.ENCODING)
        )

    return [values[column] for column in columns]

def project_batches(
    payload: bytes,
    batches: int,
    projections: tuple = (_FILTER_COLUMNS, _READER_COLUMNS),
) -> list[list[list[str | None]]]:
    """Returns each requested projection as records containing decoded string columns."""

    if any(not columns or not set(columns).issubset(COLUMNS) for columns in projections):
        raise ValueError("Invalid article projection")

    outputs = [[] for _ in projections]

    for record in message_protocol.batching.unbatch_messages(payload, batches):
        for output, columns in zip(outputs, projections):
            output.append(decode_record(record, columns))

    return outputs

def _column_blocks(payload, columns):
    """Ubica los bloques de campos sin interpretar ni copiar su contenido."""

    view = memoryview(payload)
    blocks = {}
    offset = 0

    for column in columns:
        start = offset

        if offset + _COLUMN_PREFIX_LENGTH > len(payload):
            raise ValueError("Truncated article column length")

        # La vista incluye el prefijo para reutilizarlo al proyectar la columna.
        size = int.from_bytes(view[offset:offset + _COLUMN_PREFIX_LENGTH], message_protocol.ENDIANNESS)
        offset += _COLUMN_PREFIX_LENGTH

        # Un campo nulo contiene solamente el prefijo, sin bytes de contenido.
        if size != _NULL:
            if offset + size > len(payload):
                raise ValueError("Truncated article column")

            offset += size

        blocks[column] = view[start:offset]

    if offset != len(payload):
        raise ValueError("Unexpected bytes after article columns")

    return blocks
