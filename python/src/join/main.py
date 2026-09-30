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


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_client = {}
        self.received_by_client = {}

    def handle_sigterm(self, signum, frame):
        try:
            self.input_queue.stop_consuming()
        except Exception as error:
            logging.error(f"handling SIGTERM error: {error}")

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_name, fruit_top = message_protocol.internal.deserialize(message)
        partial_top = self.partial_tops_by_client.setdefault(client_name,{})
        for fruit, amount in fruit_top:
            current_item = partial_top.get(fruit,fruit_item.FruitItem(fruit, 0))
            partial_top[fruit] = (current_item + fruit_item.FruitItem(fruit, int(amount)))
        received = self.received_by_client.get(client_name, 0) + 1
        self.received_by_client[client_name] = received
        if received == AGGREGATION_AMOUNT:
            final_items = sorted(partial_top.values(),reverse=True)[:TOP_SIZE]
            final_top = [(item.fruit, item.amount) for item in final_items]
            self.output_queue.send(
                message_protocol.internal.serialize([client_name, final_top])
            )
            self.partial_tops_by_client.pop(client_name)
            self.received_by_client.pop(client_name)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, join_filter.handle_sigterm)
    try:
        join_filter.start()
    finally:
        join_filter.close()

    return 0


if __name__ == "__main__":
    main()
