"""AWS SQS as a Pylon input or output."""
import contextlib
from typing import ContextManager

from ..aws import sqs
from ..interfaces.messaging import MessageConsumer, MessageProducer
from ..models.messages import BaseMessage
from ._common import encode_message, decode_message

class QueueMessageProducerConsumer(MessageConsumer, MessageProducer):
    """A message consumer which uses AWS SQS to send and receive messages."""

    def __init__(self, queue_name: str):
        self._queue = sqs.Queue(queue_name)

    @contextlib.contextmanager
    def getMessage(self) -> ContextManager[BaseMessage]:
        with self._queue.get_message() as sqs_message:
            message = decode_message(body=sqs_message.body, attributes=sqs_message.attributes)
            yield message

    def sendMessage(self, message: BaseMessage) -> None:
        encoded_message = encode_message(message)
        sqs_message = sqs.Message(
            body=encoded_message['body'],
            attributes=encoded_message['attributes']
        )
        self._queue.send_message(sqs_message)
