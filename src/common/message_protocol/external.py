import socket

from common import safe_socket
from . import constants

_OPCODE_SIZE = constants.OPCODE_SIZE
_BATCHES_SIZE = constants.BATCH_COUNT_SIZE
_LENGTH_SIZE = constants.MAX_PREFIX_LENGTH

_HEADER_SIZE = _OPCODE_SIZE + _BATCHES_SIZE + _LENGTH_SIZE


def recv_message(client_socket: socket.socket, max_payload_size=None):
    """Lee la cabecera y rechaza tamaños excesivos antes de recibir el payload."""

    header = safe_socket.recv_all(client_socket, _HEADER_SIZE)

    opcode = header[0]
    batch = int.from_bytes(header[_OPCODE_SIZE :_OPCODE_SIZE + _BATCHES_SIZE], byteorder=constants.ENDIANNESS)
    length = int.from_bytes(
        header[_OPCODE_SIZE + _BATCHES_SIZE: _OPCODE_SIZE + _BATCHES_SIZE + _LENGTH_SIZE], byteorder=constants.ENDIANNESS
    )
    limit = constants.MAX_PAYLOAD_SIZE if max_payload_size is None else max_payload_size

    if length > limit:
        raise ValueError(f"Message length exceeds maximum payload size of {limit} bytes")

    payload = safe_socket.recv_all(client_socket, length)

    return (opcode, batch, payload)


def send_message(client_socket: socket.socket, message: str | bytes, opcode: int, batches: int = 1):
    """Envía un mensaje con opcode, cantidad de registros y longitud del payload."""

    if opcode not in [constants.OPCODE_EOF, constants.OPCODE_DATA, constants.OPCODE_ACK]:
        raise ValueError(f"Invalid message type: {opcode}")

    opcode_bytes = opcode.to_bytes(_OPCODE_SIZE, byteorder=constants.ENDIANNESS)
    batches_bytes = batches.to_bytes(_BATCHES_SIZE, byteorder=constants.ENDIANNESS)

    payload = message.encode(encoding=constants.ENCODING) if isinstance(message, str) else message
    if len(payload) > constants.MAX_PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {constants.MAX_PAYLOAD_SIZE} bytes")

    length_bytes = len(payload).to_bytes(_LENGTH_SIZE, byteorder=constants.ENDIANNESS)

    safe_socket.send_all(client_socket, opcode_bytes + batches_bytes + length_bytes + payload)
