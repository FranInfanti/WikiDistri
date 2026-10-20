from common import message_protocol

_LENGTH_SIZE = 2

def batch_messages(messages: list[str]):
    block = []

    for message in messages:
        line = message.encode(encoding=message_protocol.ENCODING)
        length = len(line).to_bytes(2, byteorder=message_protocol.ENDIANNESS)

        block.append(length + line)

    return b"".join(block)

def unbatch_messages(payload: bytes, batches: int):
    pointer = 0

    for _ in range(batches):
        length = int.from_bytes(
            payload[pointer : pointer + _LENGTH_SIZE], 
            byteorder=message_protocol.ENDIANNESS
        )
        pointer += _LENGTH_SIZE

        message = payload[pointer : pointer + length]
        pointer += length

        yield message.decode(encoding=message_protocol.ENCODING)