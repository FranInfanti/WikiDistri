import json

def serialize(message: list):
    return json.dumps(message).encode("utf-8")

def deserialize(message: bytes):
    return json.loads(message.decode("utf-8"))