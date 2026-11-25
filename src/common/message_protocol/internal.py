import json

from common import message_protocol

def serialize(opcode: int, message_id:  int | str = 0, req_id: int = 0, batches: int = 0, message = []):
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
        [opcode, message_id, req_id, batches, message]
    ).encode(message_protocol.ENCODING)

def deserialize(message: bytes):
    """
    Returns the opcode, the req_id, number of batches and the payload
    contained in the message.
    """
    opcode, message_id, req_id, batches, payload = json.loads(
        message.decode(message_protocol.ENCODING)
    )
    
    return opcode, message_id, req_id, batches, payload