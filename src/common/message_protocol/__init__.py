from .external import send_message, recv_message
from .internal import serialize, deserialize
from .batching import batch_messages, unbatch_messages

ENDIANNESS = "big"
ENCODING = "utf-8"

OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2
OPCODE_PREPARE = 3
OPCODE_OK = 4
OPCODE_COMMIT = 5