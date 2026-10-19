import socket

from common import safe_socket

OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2

_HEADER_SIZE = 5
_PAYLOAD_SIZE = 65535

_ENDIANNESS = "big"

def recv_message(client_socket: socket):
    header = safe_socket.recv_all(client_socket, _HEADER_SIZE)
    if len(header) < _HEADER_SIZE:
        raise ValueError("Incomplete header received")

    opcode = header[0]
    batch = int.from_bytes(header[1:3], byteorder=_ENDIANNESS)

    length = int.from_bytes(header[3:5], byteorder=_ENDIANNESS)
    if length > _PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {_PAYLOAD_SIZE} bytes")

    payload = safe_socket.recv_all(client_socket, length)
    if len(payload) < length:
        raise ValueError("Incomplete payload received")

    message = payload.decode("utf-8")
    return (opcode, batch, message)

def send_message(client_socket: socket, message: str, message_type: int, batches: int):
    if message_type not in [OPCODE_EOF, OPCODE_DATA, OPCODE_ACK]:
        raise ValueError(f"Invalid message type: {message_type}")

    opcode = message_type.to_bytes(1, byteorder=_ENDIANNESS)
    batch = batches.to_bytes(2, byteorder=_ENDIANNESS)

    length = len(message).to_bytes(2, byteorder=_ENDIANNESS)
    if len(message) > _PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {_PAYLOAD_SIZE} bytes")
    
    payload = message.encode("utf-8")

    safe_socket.send_all(client_socket, opcode + batch + length + payload)
