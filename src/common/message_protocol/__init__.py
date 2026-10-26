from .internal import serialize, deserialize
from .external import send_message, recv_message
from .batching import batch_messages, batch_payload_size, unbatch_messages

ENCODING = "utf-8"
ENDIANNESS = "big"
MAX_PREFIX_LENGTH = 4
MAX_PAYLOAD_SIZE = (1 << 32) - 1
MAX_RABBITMQ_MESSAGE_SIZE = 134217728

OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2
OPCODE_PREPARE = 3
OPCODE_OK = 4
OPCODE_COMMIT = 5