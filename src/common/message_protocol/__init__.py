from .external import send_message, recv_message
from .internal import serialize, deserialize
from .batching import batch_messages, batch_payload_size, unbatch_messages

ENDIANNESS = "big"
ENCODING = "utf-8"
MAX_PAYLOAD_SIZE = (1 << 32) - 1
MAX_PREFIX_LENGTH = 4

OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2
OPCODE_PREPARE = 3
OPCODE_OK = 4
OPCODE_COMMIT = 5