"""AWS SQS as a Pylon input or output."""

import contextlib

from ..aws import sqs
from ..interfaces.messaging import MessageConsumer, MessageProducer
from ..models.messages import BaseMessage

class QueueMessageProducerConsumer(MessageConsumer, MessageProducer):
    """A message consumer which uses AWS SQS to send and receive messages."""

    def __init__(self, queue_name: str):
        self._queue = sqs.Queue(queue_name)

    @contextlib.contextmanager
    def getMessage(self) -> BaseMessage:
        with self._queue.getMessage() as message:
            yield message

    def sendMessage(self, message: BaseMessage) -> None:
        self._queue.sendMessage(message)
