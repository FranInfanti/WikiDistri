import json

from common import message_protocol

def serialize(opcode: int, req_id: int, batches: int = 0, message = []):
    """
    Returns 

    opcode
        The type of message.
    req_id
        The client identifier.
    batches
        The number of messages contained in the batch.
    message
        The message being send.
    """
    return json.dumps(
        [opcode, req_id, batches, message]
    ).encode(message_protocol.ENCODING)

def deserialize(message: bytes):
    """
    Returns the opcode, the req_id, number of batches and the payload
    contained in the message.
    """
    opcode, req_id, batches, payload = json.loads(
        message.decode(message_protocol.ENCODING)
    )
    
    return opcode, req_id, batches, payload
