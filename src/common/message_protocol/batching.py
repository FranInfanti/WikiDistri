from . import constants


def batch_messages(messages: list[str | bytes]):
    """Agrupa mensajes de texto o bytes y agrega la longitud de cada registro."""

    block = []

    for message in messages:
        line = (
            message.encode(encoding=constants.ENCODING)
            if isinstance(message, str)
            else message
        )
        length = len(line).to_bytes(
            constants.MAX_PREFIX_LENGTH, byteorder=constants.ENDIANNESS
        )

        block.append(length + line)

    return b"".join(block)


def batch_payload_size(messages: list[str | bytes]) -> int:
    """Calcula los bytes del batch, incluidos los prefijos de sus registros."""

    return sum(
        len(
            message.encode(encoding=constants.ENCODING)
            if isinstance(message, str)
            else message
        ) + constants.MAX_PREFIX_LENGTH
        for message in messages
    )


def unbatch_views(payload: bytes, batches: int):
    """Valida los límites del batch y devuelve vistas de sus registros sin copiarlos."""

    if not 0 <= batches <= constants.MAX_BATCH_RECORDS:
        raise ValueError("Invalid record count")

    records = []
    pointer = 0

    # Se validan todos los límites antes de entregar la primera vista.
    for _ in range(batches):
        end = pointer + constants.MAX_PREFIX_LENGTH

        if end > len(payload):
            raise ValueError("Truncated record length")

        length = int.from_bytes(payload[pointer:end], constants.ENDIANNESS)
        pointer = end
        end = pointer + length

        if end > len(payload):
            raise ValueError("Truncated record")

        records.append((pointer, end))
        pointer = end

    if pointer != len(payload):
        raise ValueError("Unexpected bytes after batch")

    view = memoryview(payload)
    for start, end in records:
        yield view[start:end]


def unbatch_bytes(payload: bytes, batches: int):
    """Entrega cada registro como bytes después de validar el batch."""

    for record in unbatch_views(payload, batches):
        yield bytes(record)


def unbatch_messages(payload: bytes, batches: int):
    """Separa un batch de mensajes de texto y decodifica cada registro en UTF-8."""

    pointer = 0

    for _ in range(batches):
        length = int.from_bytes(
            payload[pointer : pointer + constants.MAX_PREFIX_LENGTH],
            byteorder=constants.ENDIANNESS
        )
        pointer += constants.MAX_PREFIX_LENGTH

        message = payload[pointer : pointer + length]
        pointer += length

        yield message.decode(encoding=constants.ENCODING)
