import socket

from common import safe_socket
from common import message_protocol

_OPCODE_SIZE = 1
_BATCHES_SIZE = 2
_LENGTH_SIZE = 4

_HEADER_SIZE = _OPCODE_SIZE + _BATCHES_SIZE + _LENGTH_SIZE

def recv_message(client_socket: socket.socket):
    """
    Returns the opcode, number of batches and payload recevied in the packet.
    
    client_socket
        The socket from which the message is received.
    """
    header = safe_socket.recv_all(client_socket, _HEADER_SIZE)

    opcode = header[0]
    batch = int.from_bytes(header[_OPCODE_SIZE :_OPCODE_SIZE + _BATCHES_SIZE], byteorder=message_protocol.ENDIANNESS)
    length = int.from_bytes(
        header[_OPCODE_SIZE + _BATCHES_SIZE: _OPCODE_SIZE + _BATCHES_SIZE + _LENGTH_SIZE], byteorder=message_protocol.ENDIANNESS
    )
    if length > message_protocol.MAX_PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {message_protocol.MAX_PAYLOAD_SIZE} bytes")

    payload = safe_socket.recv_all(client_socket, length)

    return (opcode, batch, payload)

def send_message(client_socket: socket.socket, message: str | bytes, opcode: int, batches: int = 1):
    """
    Sends a message via a TCP/IP connection. The message can be a simple string or a entire
    batch; in the latter case, the number of batches must be specified.

    client_socket
        The socket from which the message will be send.

    message
        The message being send.

    opcode
        The type of message being send.

    batches
        The number of messages contained in the batch.
    """
    if opcode not in [message_protocol.OPCODE_EOF, message_protocol.OPCODE_DATA, message_protocol.OPCODE_ACK]:
        raise ValueError(f"Invalid message type: {opcode}")

    opcode_bytes = opcode.to_bytes(_OPCODE_SIZE, byteorder=message_protocol.ENDIANNESS)
    batches_bytes = batches.to_bytes(_BATCHES_SIZE, byteorder=message_protocol.ENDIANNESS)

    payload = message.encode(encoding=message_protocol.ENCODING) if isinstance(message, str) else message
    if len(payload) > message_protocol.MAX_PAYLOAD_SIZE:
        raise ValueError(f"Message length exceeds maximum payload size of {message_protocol.MAX_PAYLOAD_SIZE} bytes")

    length_bytes = len(payload).to_bytes(_LENGTH_SIZE, byteorder=message_protocol.ENDIANNESS)

    safe_socket.send_all(client_socket, opcode_bytes + batches_bytes + length_bytes + payload)
