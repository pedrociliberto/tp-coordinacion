import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

PARTIAL_TOP_MESSAGE_FIELDS = 2
INITIAL_COUNT = 0

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_client = {}
        self.client_aggregation_counts = {}
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("[Join] Received SIGTERM. Shutting down...")
        self.closed = True
        try:
            if self.input_queue:
                self.input_queue.stop_consuming()
                self.input_queue.close()
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.error(f"[Join] Error while stopping consumer: {e}")

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _calculate_global_top(self, all_partial_tops):
        fruit_objects = [
            fruit_item.FruitItem(fruit, amount)
            for partial_top in all_partial_tops
            for fruit, amount in partial_top
        ]
        fruit_objects.sort(key=lambda x: x.amount)
        top_fruits = list(fruit_objects[-TOP_SIZE:])
        top_fruits.reverse()
        return [(item.fruit, item.amount) for item in top_fruits]

    def _process_global_top(self, client_id):
        logging.info(f"[Join] All partial tops received for client: {client_id}")
        all_partial_tops = self.partial_tops_by_client.pop(client_id, [])
        final_top = self._calculate_global_top(all_partial_tops)
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )
        self.client_aggregation_counts.pop(client_id, None)

    def _process_partial_top(self, client_id, fruit_top):
        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = []
            self.client_aggregation_counts[client_id] = INITIAL_COUNT
        self.partial_tops_by_client[client_id].append(fruit_top)
        self.client_aggregation_counts[client_id] += 1

        count = self.client_aggregation_counts[client_id]
        logging.info(
            f"[Join] Received partial top for client: {client_id} "
            f"({count}/{AGGREGATION_AMOUNT})"
        )

        if count >= AGGREGATION_AMOUNT:
            self._process_global_top(client_id)

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == PARTIAL_TOP_MESSAGE_FIELDS:
            client_id, fruit_top = fields
            self._process_partial_top(client_id, fruit_top)
        ack()

    def _close_middleware_connections(self, middleware_connection):
        try:
            if middleware_connection:
                middleware_connection.close()
        except Exception as e:
            logging.error(f"[Join] Error while closing middleware connection: {e}")

    def close(self):
        logging.info("[Join] Closing connections...")
        self._close_middleware_connections(self.input_queue)
        self._close_middleware_connections(self.output_queue)

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        except Exception as e:
            if not self.closed:
                logging.error(f"[Join] Error while consuming messages: {e}")
        finally:
            self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
