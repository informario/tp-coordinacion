import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_QUEUE_PREFIX = "sum_control"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.control_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST,f"{SUM_CONTROL_QUEUE_PREFIX}_{ID}")
        self.control_output_queues = [middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST,f"{SUM_CONTROL_QUEUE_PREFIX}_{i}")
            for i in range(SUM_AMOUNT)
        ]
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}

    def _process_data(self, client_name, fruit, amount):
        logging.info(f"Process data")
        amount_by_fruit = self.amount_by_client.setdefault(client_name, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(fruit,fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit,int(amount))

    def _process_eof(self, client_name):
        logging.info(f"broadcasting data messages from {client_name}")
        amount_by_fruit = self.amount_by_client.pop(client_name, {})
        for final_fruit_item in amount_by_fruit.values():
            aggregation_index = self._aggregation_index(client_name,final_fruit_item.fruit)
            message = message_protocol.internal.serialize([client_name,final_fruit_item.fruit,final_fruit_item.amount])
            #reparto los cosos en funcion del "hash"
            self.data_output_exchanges[aggregation_index].send(message)
        eof_message = message_protocol.internal.serialize([client_name])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)

    def _broadcast_eof(self, client_name):
        eof_message = message_protocol.internal.serialize([client_name])

        for control_output_queue in self.control_output_queues:
            control_output_queue.send(eof_message)

    def _aggregation_index(self, client_name, fruit):
        key = f"{client_name}\0{fruit}"
        return sum(ord(character) for character in key) % AGGREGATION_AMOUNT

    #este es el callback de input queue
    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._broadcast_eof(*fields)
        else:
            raise ValueError("mensaje desconocido")
        ack()
    #este es el calback de control input queue
    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 1:
            raise ValueError("mensaje de control desconocido")
        self._process_eof(*fields)
        ack()

    def start(self):
        control_thread = threading.Thread(
            target=self.control_input_queue.start_consuming,
            args=(self.process_control_message,),
            daemon=True,
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_message)



def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
