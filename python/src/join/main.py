import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


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

    def _process_global_top(self, client_id):
        logging.info(f"[Join] All partial tops received for client: {client_id}")

        all_items = self.partial_tops_by_client[client_id]
        fruit_objects = [
            fruit_item.FruitItem(fruit, amount)
            for partial_top in all_items
            for fruit, amount in partial_top
        ]
        fruit_objects.sort(key=lambda x: x.amount)
        top_fruits = list(fruit_objects[-TOP_SIZE:])
        top_fruits.reverse()

        final_top = [
            (fruit_item.fruit, fruit_item.amount) for fruit_item in top_fruits
        ]
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )

        del self.partial_tops_by_client[client_id]
        del self.client_aggregation_counts[client_id]

    def _process_partial_top(self, client_id, fruit_top):
        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = []
            self.client_aggregation_counts[client_id] = 0
        self.partial_tops_by_client[client_id].append(fruit_top)
        self.client_aggregation_counts[client_id] += 1

        count = self.client_aggregation_counts[client_id]
        logging.info(
            f"[Join] Received partial top for client: {client_id} "
            f"({count}/{AGGREGATION_AMOUNT})"
        )

        if count < AGGREGATION_AMOUNT:
            return
        self._process_global_top(client_id)

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            client_id, fruit_top = fields
            self._process_partial_top(client_id, fruit_top)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
