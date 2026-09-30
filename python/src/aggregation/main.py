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
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}

    def _process_data(self, client_name, fruit, amount):
        logging.info(f"Processing data message from {client_name}")
        fruit_top = self.fruit_top_by_client.setdefault(
            client_name,
            [],
        )
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_item = (
                    fruit_top[i]
                    + fruit_item.FruitItem(
                        fruit,
                        int(amount),
                    )
                )
                fruit_top.pop(i)
                bisect.insort(fruit_top, updated_item)
                return

        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, int(amount)))

    def _process_eof(self, client_name):
        logging.info(f"Received EOF from {client_name}")
        eof_count = self.eof_count_by_client.get(client_name, 0) + 1
        self.eof_count_by_client[client_name] = eof_count
        if eof_count < SUM_AMOUNT:
            return
        self.eof_count_by_client.pop(client_name, None)
        fruit_top = self.fruit_top_by_client.pop(client_name,[],)
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        client_fruit_top = [(item.fruit, item.amount) for item in fruit_chunk]
        self.output_queue.send(
            message_protocol.internal.serialize([client_name, client_fruit_top])
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)
        else:
            raise ValueError(f"invalid message")
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
