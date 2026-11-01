ENCODING = "utf-8"
ENDIANNESS = "big"

# Batching fields
MAX_BATCH_PREFIX_LENGTH = 4

# Message protocol fields
OPCODE_SIZE = 1
BATCHES_SIZE = 2
LENGTH_SIZE = 4
MAX_PAYLOAD_SIZE = (1 << 32) - 1

MAX_RABBITMQ_MESSAGE_SIZE = 134217728

MAX_BATCH_RECORDS = (1 << (8 * BATCHES_SIZE)) - 1
MIN_BATCH_RECORDS = 1

# Opcode types
OPCODE_EOF = 0
OPCODE_DATA = 1
OPCODE_ACK = 2
OPCODE_PREPARE = 3
OPCODE_OK = 4
OPCODE_COMMIT = 5
OPCODE_DONE = 6

from .internal import serialize, deserialize
from .external import send_message, recv_message
from .batching import batch_messages, batch_payload_size, unbatch_messages
from .articles import encode_record
