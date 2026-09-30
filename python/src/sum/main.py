import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
EOF_TOKEN = "EOF_TOKEN"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
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

    def _send_eof_token(self, client_name, processed_sum_ids):
        token = message_protocol.internal.serialize({"type": EOF_TOKEN,"client_name": client_name,"processed_sum_ids": processed_sum_ids})
        self.input_queue.send(token)

    def _process_eof_token(self, client_name, processed_sum_ids):
        if ID in processed_sum_ids:
            if len(processed_sum_ids) < SUM_AMOUNT:
                self._send_eof_token(client_name, processed_sum_ids)
            return
        self._process_eof(client_name)
        processed_sum_ids = processed_sum_ids + [ID]
        if len(processed_sum_ids) < SUM_AMOUNT:
            self._send_eof_token(client_name, processed_sum_ids)

    def _aggregation_index(self, client_name, fruit):
        key = f"{client_name}\0{fruit}"
        return sum(ord(character) for character in key) % AGGREGATION_AMOUNT

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if isinstance(fields, dict) and fields.get("type") == EOF_TOKEN:
            self._process_eof_token(
                fields["client_name"],
                fields["processed_sum_ids"],
            )
        elif len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof_token(fields[0], [])
        else:
            raise ValueError("mensaje desconocido")

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
