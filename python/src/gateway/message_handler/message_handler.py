import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        fruit, amount = message[0], message[1]
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        msg_client_id, fruit_top = fields[0], fields[1]
        if msg_client_id != self.client_id:
            return None
        return fruit_top
