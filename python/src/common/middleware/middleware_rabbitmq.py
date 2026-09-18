import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)

DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.StreamLostError,
)
DISCONNECTION_MSG = "Error connecting to RabbitMQ: {}"
ALREADY_CONSUMING_MSG = "Already consuming messages. Stop consuming before starting again."

class _MessageMiddlewareRabbitMQBase:
    ATTR_CONNECTION = 'connection'
    ATTR_CHANNEL = 'channel'

    def __init__(self, host):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def _wrap_callback(self, on_message_callback):
        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return _internal_callback

    def _consume(self, queue_name, on_message_callback):
        if self.is_consuming:
            raise MessageMiddlewareMessageError(ALREADY_CONSUMING_MSG)
        try:
            callback = self._wrap_callback(on_message_callback)
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            self.is_consuming = True
            self.channel.start_consuming()
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e
        finally:
            self.is_consuming = False

    def stop_consuming(self):
        try:
            if self.is_consuming:
                self.channel.stop_consuming()
                self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def close(self):
        try:
            if hasattr(self, self.ATTR_CHANNEL) and self.channel and self.channel.is_open:
                self.channel.close()
            if hasattr(self, self.ATTR_CONNECTION) and self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e) from e
        
class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queue_name = queue_name
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def start_consuming(self, on_message_callback):
        self._consume(self.queue_name, on_message_callback)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e
    
class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        try:
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)
            self._consume(queue_name, on_message_callback)
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e