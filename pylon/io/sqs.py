"""AWS SQS as a Pylon input or output."""

from typing import Iterable

from ..aws import sqs
from ..interfaces.messaging import MessageConsumer, MessageProducer
from ..models.messages import BaseMessage
from ._common import encode_message, decode_message

class QueueMessageProducerConsumer(MessageConsumer, MessageProducer):
    """A message consumer which uses AWS SQS to send and receive messages."""

    def __init__(
            self, queue_name: str,  max_messages_receive: int = 10, receive_batch_size: int = 10,
            send_batch_size: int = 10
    ):
        self._queue = sqs.Queue(queue_name)
        self._max_messages_receive = max_messages_receive
        self.receive_batch_size = receive_batch_size
        self.send_batch_size = send_batch_size

    def get_messages(self) -> Iterable[BaseMessage]:
        messages = self._queue.get_messages(
            max_messages=self._max_messages_receive, batch_size=self.receive_batch_size
        )
        for sqs_message in messages:
            message = decode_message(body=sqs_message.body, attributes=sqs_message.attributes)
            yield message

    def send_messages(self, messages: Iterable[BaseMessage]) -> None:
        encoded_messages = (encode_message(message) for message in messages)
        sqs_messages = (
            sqs.Message(
                body=encoded_message['body'],
                attributes=encoded_message['attributes']
            )
            for encoded_message in encoded_messages
        )
        self._queue.send_messages(sqs_messages, batch_size=self.send_batch_size)
