from common import message_protocol
import secrets

class MessageHandler:

    def __init__(self):
        self.client_name = secrets.token_hex(32)
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_name, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_name])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 2:
            return []
        client_name, fruit_top = fields
        if client_name != self.client_name:
            return []
        return fruit_top
