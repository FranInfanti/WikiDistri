from common import message_protocol

def batch_messages(messages: list[str]):
    """
    Returns a byte stream containing the list of messages, 
    separated using the Length-Prefix protocol.
    
    messages
        A list of messages to be packed into a single batch.
    """
    block = []

    for message in messages:
        line = message.encode(encoding=message_protocol.ENCODING)
        length = len(line).to_bytes(
            message_protocol.MAX_PREFIX_LENGTH, byteorder=message_protocol.ENDIANNESS
        )

        block.append(length + line)

    return b"".join(block)

def batch_payload_size(messages: list[str]) -> int:
    """Returns the size in bytes of the payload produced by ``batch_messages``."""
    return sum(
        len(message.encode(encoding=message_protocol.ENCODING)) + message_protocol.MAX_PREFIX_LENGTH
        for message in messages
    )

def unbatch_messages(payload: bytes, batches: int):
    """
    Yields a message each time it's invoked, until no more messages remain.
    Each message represent a single CSV line.

    payload
        The stream of bytes received in the packet.
    
    batches
        The number of batches contained in the payload.
    """
    pointer = 0

    for _ in range(batches):
        length = int.from_bytes(
            payload[pointer : pointer + message_protocol.MAX_PREFIX_LENGTH], 
            byteorder=message_protocol.ENDIANNESS
        )
        pointer += message_protocol.MAX_PREFIX_LENGTH

        message = payload[pointer : pointer + length]
        pointer += length

        yield message.decode(encoding=message_protocol.ENCODING)