import socket

from common import safe_socket
from common import message_protocol

OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2

_HEADER_SIZE = 5
_MAX_PAYLOAD_SIZE = 65535

def recv_message(client_socket: socket):
    header = safe_socket.recv_all(client_socket, _HEADER_SIZE)
    if len(header) < _HEADER_SIZE:
        raise ValueError("Incomplete header received")

    opcode = header[0]
    batch = int.from_bytes(header[1:3], byteorder=message_protocol.ENDIANNESS)

    length = int.from_bytes(header[3:5], byteorder=message_protocol.ENDIANNESS)
    if length > _MAX_PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {_MAX_PAYLOAD_SIZE} bytes")

    payload = safe_socket.recv_all(client_socket, length)
    if len(payload) < length:
        raise ValueError("Incomplete payload received")

    return (opcode, batch, payload)

def send_message(client_socket: socket, message: str | bytes, message_type: int, batches: int):
    if message_type not in [OPCODE_EOF, OPCODE_DATA, OPCODE_ACK]:
        raise ValueError(f"Invalid message type: {message_type}")

    opcode = message_type.to_bytes(1, byteorder=message_protocol.ENDIANNESS)
    batch = batches.to_bytes(2, byteorder=message_protocol.ENDIANNESS)

    payload = message.encode("utf-8") if isinstance(message, str) else message
    if len(payload) > _MAX_PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {_MAX_PAYLOAD_SIZE} bytes")

    length = len(payload).to_bytes(2, byteorder=message_protocol.ENDIANNESS)

    safe_socket.send_all(client_socket, opcode + batch + length + payload)
