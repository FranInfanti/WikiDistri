import os
import json
import time
import signal
import logging
import threading
import collections

from datetime import datetime
from common import entity, message_protocol, middleware, rabbitmq

ID = int(os.environ["ID"])
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
FILTER_INSTANCES = int(os.environ.get("FILTER_INSTANCES", 1))
FILTER_PREFIX = os.environ.get("FILTER_PREFIX", "filter")
FILTER_QUEUE = os.environ.get("FILTER_QUEUE", "filter_queue")
IMAGE_DOWNLOADER_INSTANCES = int(os.environ.get("IMAGE_DOWNLOADER_INSTANCES", 1))
IMAGE_DOWNLOADER_PREFIX = os.environ.get("IMAGE_DOWNLOADER_PREFIX", "image_downloader_queue")

FILTER_STATE_PATH = os.environ.get("FILTER_STATE_PATH", f"{FILTER_PREFIX}_{ID}_data_state.json")

RECEIVING_PHASE = "RECEIVING"

def section_contains(sections: str | None, *words: str):
    import json

    if not sections:
        return False

    parsed_sections = json.loads(sections)

    return any(
        section.get("name", "").casefold() == word.casefold()
        for section in parsed_sections
        for word in words
    )

class Filter:
    _TIMEOUT = 10.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_1"
    _MAX_GATEWAY_MESSAGE_SIZE = message_protocol.MAX_RABBITMQ_MESSAGE_SIZE

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True

        self.image_downloader_queue_names = [f"{IMAGE_DOWNLOADER_PREFIX}_{i}" for i in range(IMAGE_DOWNLOADER_INSTANCES)]
        self.publisher_downloader = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.image_downloader_queue_names],
        )
        self.publisher_gateway = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE],
        )

        # Estado propio del proceso. El coordinator lo lee con get_tracker.
        self._state = rabbitmq.PhaseManager(FILTER_STATE_PATH)
        
        self._trackers = {}
        self._trackers_lock = threading.RLock()
        
        self.recv_seq_num = collections.defaultdict(int)  # proximo id de entrada esperado
        self.sent_seq_num = collections.defaultdict(int)  # proximo id de salida a usar
        
        self.__post_init__()

        # EOF recibidos cuyo ack espera a que termine el protocolo
        self._pending_eof = {}
        self._eof_lock = threading.Lock()

        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=FILTER_PREFIX,
            instances=FILTER_INSTANCES,
            on_all_recv=self._on_recv_all,
            get_tracker=self._get_tracker,
        )
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{FILTER_QUEUE}_{ID}"
        )

    def __post_init__(self):
        """Reconstruye desde disco los trackers y las secuencias de cada req_id."""
        for req_id in self._state.pending(RECEIVING_PHASE):
            rec = self._state.get(req_id)

            tracker = self._new_tracker()
            tracker.record_recv(rec["recv"])
            for key, n in rec["sent"].items():
                tracker.record_sent(key, n)

            self._trackers[req_id] = tracker
            self.recv_seq_num[req_id] = rec["recv_seq"]
            self.sent_seq_num[req_id] = rec["sent_seq"]

    def _new_tracker(self):
        tracker = entity.message_tracker.MessageTracker()
        tracker.record_sent(IMAGE_DOWNLOADER_PREFIX, 0)
        return tracker

    def _get_tracker(self, req_id: int):
        with self._trackers_lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = self._new_tracker()
            return tracker

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, proceed to shutdown the service...")
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to stop")

        timed_out = []
        components = (
            ("coordinator", self.coordinator.stop),
            ("downloader publisher", self.publisher_downloader.stop),
            ("gateway publisher", self.publisher_gateway.stop),
        )
        for name, stop in components:
            if stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append(name)

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")


    def _publish_batch(self, req_id: int, dest: str, batch: list, message_id: str):
        message_id = f"{FILTER_PREFIX}_{ID}_{seqnum}"

        if dest == GATEWAY_PREFIX:
            publisher = self.publisher_gateway
            queue = self._GATEWAY_QUEUE
        elif dest == IMAGE_DOWNLOADER_PREFIX:
            publisher = self.publisher_downloader
            queue = self.image_downloader_queue_names[message_id % IMAGE_DOWNLOADER_INSTANCES]
        else:
            raise ValueError(f"Unknown batch destination: {dest}")

        logging.debug(
            "Sending OPCODE_DATA batch of %s messages to %s for req_id=%s",
            len(batch),
            dest,
            req_id,
        )

        publisher.publish_queue(
            queue=queue,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                message_id=message_id,
                req_id=req_id,
                batches=len(batch),
                message=batch,
            )
        )

    def _flush_batches(self, req_id: int, out: dict, message_id: str):
        for dest, batch in out.items():
            self._publish_batch(
                req_id=req_id, 
                dest=dest, 
                batch=batch,
                seqnum=seqnum
            )

    def _filter_message(self, message_id: int, req_id: int, _: int, messages: list[list]):
        logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")

        if self.recv_seq_num[req_id] > message_id:
            logging.warning(f"OPCODE_DATA message received for req_id={req_id} is dup; ignoring...")
            return

        out = {
            GATEWAY_PREFIX: [],
            IMAGE_DOWNLOADER_PREFIX: []
        }

        recv = sent = 0
        for message in messages:
            name, url, date_modified, image, in_language, sections = message
            recv += 1

            date_modified = datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%S") if date_modified else None
            if date_modified is None:
                logging.warning(f"Received a message with an invalid date_modified value for req_id={req_id}: {message}")
                continue

            if in_language == "en" and date_modified.year <= 2020:
                out[GATEWAY_PREFIX].append([url, date_modified.strftime("%Y-%m-%d %H:%M:%S")])

            if date_modified.year <= 2020 and section_contains(sections, "biography", "biographie"):
                out[IMAGE_DOWNLOADER_PREFIX].append([name, url, image])
                sent += 1

        seqnum = 0 # De algun lado tengo que sacar el seqnum siguiente, que tiene que ser el mismo
                   # que usaria si me caigo antes de confirmar con `ack()
        self._flush_batches(req_id=req_id, out=out, message_id=f"{FILTER_PREFIX}_{ID}_{message_id}_{seqnum}")

        # 50 | 51

        # top_{ID}_{message_id_count}_0

    def _on_recv_all(self, req_id: int, sent_messages: list):
        # Se valida antes de publicar: si falla, no queda un EOF enviado a medias
        if len(sent_messages) == 0:
            key, sent = IMAGE_DOWNLOADER_PREFIX, 0
        else:
            key, sent = sent_messages[0]

        if key != IMAGE_DOWNLOADER_PREFIX:
            raise ValueError(f"Unknown key; expected: {IMAGE_DOWNLOADER_PREFIX}, received: {key}")

        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        message_id = self.recv_seq_num[req_id] + 1

        self.publisher_gateway.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=message_id,
                req_id=req_id
            )
        )

        # No se consume la secuencia: despues del EOF no entran batches nuevos, y asi
        # si on_all_recv se repite (caida) el EOF sale con el mismo id.
        seq = self.sent_seq_num[req_id]
        logging.debug(f"Sending OPCODE_EOF message to {IMAGE_DOWNLOADER_PREFIX} for req_id={req_id}; expected messages: {sent}")
        self.publisher_downloader.publish_queue(
            queue=self.image_downloader_queue_names[seq % IMAGE_DOWNLOADER_INSTANCES],
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=f"{FILTER_PREFIX}_{ID}_{seq}",
                req_id=req_id,
                batches=1,
                message=sent
            )
        )

        # El protocolo termino: recien ahora se confirma el EOF a RabbitMQ
        self._ack_eof(req_id)

    def _ack_eof(self, req_id: int):
        with self._eof_lock:
            ack = self._pending_eof.pop(req_id, None)
        if ack:
            ack()

    def _handle_eof(self, req_id: int, expected_messages: int, ack):
        # Se registra ANTES de llamar a on_eof: con una sola instancia el protocolo
        # termina dentro de on_eof y _on_recv_all necesita encontrar el ack.
        with self._eof_lock:
            self._pending_eof[req_id] = ack

        try:
            already_finished = self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)
        except Exception:
            with self._eof_lock:
                self._pending_eof.pop(req_id, None)
            raise

        # EOF repetido de un protocolo que ya habia terminado: nadie lo va a confirmar
        if already_finished:
            self._ack_eof(req_id)

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        try:
            if opcode == message_protocol.OPCODE_DATA:
                self._filter_message(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                _message_id, req_id, _, expected_messages = args
                self._handle_eof(req_id, expected_messages, ack)
                return  # el ack lo hace _on_recv_all (o _handle_eof si ya habia terminado)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
            # save(recv, sent)
            # save(seqnum)
            # aca
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        try:
            logging.info("Waiting to receive messages...")
            self.coordinator.start()
            self.input_queue.start_consuming(self._process_message)
        except:
            if self.is_running:
                logging.critical("An unexpected error occurred while running the service", exc_info=True)
                raise
            logging.debug("Interrupted by the SIGTERM signal")
        finally:
            self._close()

def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    logging.getLogger("pika").propagate = False
    try:
        Filter().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())