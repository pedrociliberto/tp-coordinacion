import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_ROUTING_KEY = "SUM_ROUTING_KEY"

TIMEOUT_JOIN = 2.0

DATA_MESSAGE_FIELDS = 3
CONTROL_START_EOF_FIELDS = 1
CONTROL_FRUIT_REPORT_FIELDS = 2
INITIAL_AMOUNT = 0
INITIAL_COUNT = 0
EOF_ENDING = -1

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_ROUTING_KEY]
        )
        self.control_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_ROUTING_KEY]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client_and_fruit = {}
        self.client_processed_counts = {}
        self.client_expected_totals = {}
        self.pending_client_ids = set()

        self.state_lock = threading.Lock()
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info(f"[Sum {ID}] Received SIGTERM. Shutting down...")
        self.closed = True
        try:
            if self.control_receiver:
                self.control_receiver.stop_consuming_threadsafe()
        except Exception as e:
            logging.error(f"[Sum {ID}] Error while stopping consumers: {e}")

        try:
            if self.input_queue:
                self.input_queue.stop_consuming_threadsafe()
        except Exception as e:
            logging.error(f"[Sum {ID}] Error while stopping consumers: {e}")

        if self._prev_sigterm_handler and callable(self._prev_sigterm_handler):
            self._prev_sigterm_handler(signum, frame)

    def _aggregation_index(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _flush_client_data(self, client_id):
        client_dict = self.amount_by_client_and_fruit.pop(client_id, {})
        for final_fruit_item in client_dict.values():
            target_agg_index = self._aggregation_index(final_fruit_item.fruit)
            data_output_exchange = self.data_output_exchanges[target_agg_index]
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        messages_count = self.client_processed_counts.pop(client_id, INITIAL_COUNT)
        self.control_sender.send(
            message_protocol.internal.serialize([client_id, messages_count])
        )
        
    def _process_data(self, client_id, fruit, amount):
        client_dict = self.amount_by_client_and_fruit.setdefault(client_id, {})
        new_item = fruit_item.FruitItem(fruit, int(amount))
        client_dict[fruit] = client_dict.get(fruit, fruit_item.FruitItem(fruit, INITIAL_AMOUNT)) + new_item
        self.client_processed_counts[client_id] = self.client_processed_counts.get(client_id, INITIAL_COUNT) + 1
        if client_id in self.pending_client_ids:
            self._flush_client_data(client_id)

    def _handle_gateway_eof(self, client_id, total_messages):
        logging.info(f"[Sum {ID}] Received EOF from Gateway (Total: {total_messages}). Broadcasting...")
        with self.state_lock:
            self.client_expected_totals[client_id] = (int(total_messages), INITIAL_COUNT)
        self.control_sender.send(
            message_protocol.internal.serialize([client_id])
        )

    def _start_client_report(self, client_id):
        with self.state_lock:
            self.pending_client_ids.add(client_id)
            self._flush_client_data(client_id)

    def _complete_client_eof(self, client_id):
        logging.info(f"[Sum {ID}] Processing EOF completion for client: {client_id}")
        with self.state_lock:
            self.pending_client_ids.discard(client_id)
        eof_msg = message_protocol.internal.serialize([client_id])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_msg)

    def _update_client_expected_total(self, client_id, amount_to_add):
        with self.state_lock:
            if client_id in self.client_expected_totals:
                total_expected, current = self.client_expected_totals[client_id]
                new_count = current + amount_to_add
                self.client_expected_totals[client_id] = (total_expected, new_count)
                if new_count >= total_expected:
                    self.client_expected_totals.pop(client_id, None)
                    self.control_sender.send(
                        message_protocol.internal.serialize([client_id, EOF_ENDING])
                    )

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == DATA_MESSAGE_FIELDS:
            with self.state_lock:
                self._process_data(*fields)
        else:
            self._handle_gateway_eof(fields[0], fields[1])
        ack()

    def process_control_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == CONTROL_START_EOF_FIELDS:
            self._start_client_report(fields[0])
        elif len(fields) == CONTROL_FRUIT_REPORT_FIELDS:
            client_id, second_field = fields[0], fields[1]
            if second_field == EOF_ENDING:
                self._complete_client_eof(client_id)
            else:
                amount_to_add = int(second_field)
                self._update_client_expected_total(client_id, amount_to_add)

        ack()

    def _start_control_consumer(self):
        try:
            self.control_receiver.start_consuming(self.process_control_messsage)
        except Exception as e:
            if not self.closed:
                logging.error(f"[Sum {ID}] Error in control thread: {e}")

    def _close_middleware_connection(self, middleware_connection):
        try:
            if middleware_connection:
                middleware_connection.close()
        except Exception as e:
            logging.error(f"[Sum {ID}] Error while closing middleware connection: {e}")

    def close(self):
        logging.info(f"[Sum {ID}] Closing connections...")

        self._close_middleware_connection(self.control_receiver)
        self._close_middleware_connection(self.control_sender)
        self._close_middleware_connection(self.input_queue)
        for idx, exchange in enumerate(self.data_output_exchanges):
            self._close_middleware_connection(exchange)

    def start(self):
        control_thread = threading.Thread(
            target=self._start_control_consumer,
            daemon=True
        )
        control_thread.start()

        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        except Exception as e:
            if not self.closed:
                logging.error(f"[Sum {ID}] Error in data consumer: {e}")
        finally:
            control_thread.join(timeout=TIMEOUT_JOIN)
            if control_thread.is_alive():
                logging.warning(f"[Sum {ID}] Control thread did not finish in time.")
            self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()