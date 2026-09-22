import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


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

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        if client_id not in self.amount_by_client_and_fruit:
            self.amount_by_client_and_fruit[client_id] = {}
        client_dict = self.amount_by_client_and_fruit[client_id]
        client_dict[fruit] = client_dict.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))


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

        fruit_items = list(client_dict.values())
        fruit_items.sort(key=lambda x: x.amount)
        fruit_chunk = list(fruit_items[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        if client_id in self.amount_by_client_and_fruit:
            del self.amount_by_client_and_fruit[client_id]
        del self.client_eof_counts[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            client_id = fields[0]
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
