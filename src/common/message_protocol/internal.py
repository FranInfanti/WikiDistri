import json
import os
import struct

from . import constants

# Cabecera DATA binaria: versión, identificador de solicitud y cantidad de artículos.
_BATCH_MAGIC = constants.BINARY_BATCH_MARKER
_BATCH_HEADER = struct.Struct(constants.BINARY_BATCH_HEADER_FORMAT)
BATCH_HEADER_SIZE = _BATCH_HEADER.size


def batch_payload_limit():
    """Calcula el máximo del payload descontando la cabecera interna de RabbitMQ."""

    configured_limit = int(os.environ.get("BATCH_BYTES", constants.DEFAULT_BATCH_BYTES))

    if configured_limit < 1:
        raise ValueError("BATCH_BYTES must be greater than zero")

    return min(
        configured_limit,
        constants.MAX_PAYLOAD_SIZE,
        constants.MAX_RABBITMQ_MESSAGE_SIZE - BATCH_HEADER_SIZE,
    )


def serialize_batch(req_id: int, batches: int, payload: bytes):
    """Agrega el contexto de la solicitud sin volver a serializar sus artículos."""

    if (
        not constants.MIN_BATCH_RECORDS <= batches <= constants.MAX_BATCH_RECORDS
        or not payload
    ):
        raise ValueError("Invalid binary DATA count or payload")

    if len(payload) > batch_payload_limit():
        raise ValueError("Binary batch exceeds transport limit")

    return _BATCH_HEADER.pack(_BATCH_MAGIC, req_id, batches) + payload


def serialize(opcode: int, req_id: int, batches: int = 0, message=()):
    """Serializa en JSON el opcode, la solicitud, la cantidad y el contenido."""

    return json.dumps(
        [opcode, req_id, batches, message]
    ).encode(constants.ENCODING)


def deserialize(message: bytes):
    """Lee el sobre binario de DATA o el formato JSON de los demás mensajes."""

    # El payload binario se devuelve intacto para que lo interprete el worker.
    if message.startswith(_BATCH_MAGIC):
        if len(message) < BATCH_HEADER_SIZE:
            raise ValueError("Truncated binary DATA header")

        _, req_id, batches = _BATCH_HEADER.unpack_from(message)
        payload = message[BATCH_HEADER_SIZE:]

        if batches == 0 or not payload or len(payload) > batch_payload_limit():
            raise ValueError("Invalid binary DATA count or payload")

        return constants.OPCODE_DATA, req_id, batches, payload

    opcode, req_id, batches, payload = json.loads(
        message.decode(constants.ENCODING)
    )

    return opcode, req_id, batches, payload
