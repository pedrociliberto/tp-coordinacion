import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

DATA_MESSAGE_FIELDS = 3
EOF_MESSAGE_FIELDS = 1

INITIAL_COUNT = 0
INITIAL_AMOUNT = 0

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client_and_fruit = {}
        self.client_eof_counts = {}
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info(f"[Aggregation {ID}] Received SIGTERM. Shutting down...")
        self.closed = True
        try:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"[Aggregation {ID}] Error while stopping consumers: {e}")

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_dict = self.amount_by_client_and_fruit.setdefault(client_id, {})
        client_dict[fruit] = client_dict.get(
            fruit, fruit_item.FruitItem(fruit, INITIAL_AMOUNT)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _calculate_top_fruits(self, client_dict):
        fruit_items = list(client_dict.values())
        fruit_items.sort(key=lambda x: x.amount)
        fruit_chunk = list(fruit_items[-TOP_SIZE:])
        fruit_chunk.reverse()
        return list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

    def _process_eof(self, client_id):
        self.client_eof_counts[client_id] = self.client_eof_counts.get(client_id, 0) + 1
        logging.info(
            f"[Aggregation {ID}] Received SUM_EOF for client: {client_id} "
            f"({self.client_eof_counts[client_id]}/{SUM_AMOUNT})"
        )
        if self.client_eof_counts[client_id] < SUM_AMOUNT:
            return
        
        logging.info(f"[Aggregation {ID}] Received all SUM_EOF for client: {client_id}. Sending top...")
        client_dict = self.amount_by_client_and_fruit.get(client_id, {})
        fruit_top = self._calculate_top_fruits(client_dict)
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        self.client_eof_counts.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == DATA_MESSAGE_FIELDS:
            self._process_data(*fields)
        elif len(fields) == EOF_MESSAGE_FIELDS:
            self._process_eof(fields[0])
        ack()

    def _close_middleware_connections(self, middleware_connection):
        try:
            if middleware_connection:
                middleware_connection.close()
        except Exception as e:
            logging.error(f"[Aggregation {ID}] Error while closing middleware connection: {e}")

    def close(self):
        logging.info(f"[Aggregation {ID}] Closing connections...")
        self._close_middleware_connections(self.input_exchange)
        self._close_middleware_connections(self.output_queue)

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception as e:
            if not self.closed:
                logging.error(f"[Aggregation {ID}] Error while consuming messages: {e}")
        finally:
            self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
