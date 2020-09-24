import boto3
import typing
import uuid
import contextlib
import functools

from ._bases import BaseMixin
from ..interfaces.messaging import MessageProducer, MessageConsumer, NoMessagesAvailable
from ..models.messages import BaseMessage, ObjectType, MessageAttribute
from ..models.data import DataAsset
from ..utils import chunked
from ._common import encodeMessage, decodeMessage
from ..utils import logging

@functools.lru_cache(maxsize=16)
class Queue(BaseMixin, MessageConsumer, MessageProducer):

    def __init__(self, queueName: str):
        super().__init__(queueName)
        self.resource = boto3.resource('sqs')
        self.client = boto3.client('sqs')
        self.queue = self.resource.get_queue_by_name(QueueName=queueName)

    def sendMessage(self, message: BaseMessage) -> None:
        """Posts a notification to SQS"""
        encoded = self._encode(message)

        logging.info(
            'Sending {msg} of {size} bytes to {dest}'
            .format(
                msg=str(message),
                size=message.getApproxSize(),
                dest=str(self)
            )
        )
        self.queue.send_message(**encoded)

    def sendMessages(self, messages: typing.Iterable[BaseMessage]) -> None:
        """Override the basic `sendMessages` functionality and send messages in batches."""
        for i, chunk in enumerate(chunked(messages, chunkSize=10)):
            logging.info(f'Sending chunk {i} to {self}')
            encoded = [self._encode(message) for message in chunk]
            for message in encoded:
                message['Id'] = str(uuid.uuid1())

            logging.info(
                'Sending {n} messages totalling {size} bytes to {dest}'
                .format(
                    n=len(encoded),
                    size=sum([message.getApproxSize() for message in chunk]),
                    dest=str(self)
                )
            )

            self.queue.send_messages(
                Entries=encoded
            )

    @contextlib.contextmanager
    def getMessage(self) -> typing.Generator[BaseMessage, None, None]:
        """
        Fetches a message from the SQS Queue.

        Example usage:

            queue = SQSQueue('my_awesome_queue')
            try:
                with queue.getMessage() as message:
                    doSomething(message)
            except NoMessagesAvailable:
                pass

        """
        logging.debug(f'Receiving SQS message')

        messages = self.queue.receive_messages(
            MessageAttributeNames=['*'],
            MaxNumberOfMessages=1
        )

        if len(messages) == 0:
            logging.info(f'No messages available from {self}')
            raise NoMessagesAvailable

        rawMessage = messages[0]
        message = self._decode(rawMessage)
        logging.info(f'Received {message} from {self}')
        yield message

        logging.info(f'Deleting {message} from {self}')
        rawMessage.delete()

    @contextlib.contextmanager
    def getMessages(self, maxMessages) -> typing.Generator[typing.Iterator, None, None]:
        """
        Fetches a message from the SQS Queue.

        Example usage:

            queue = SQSQueue('my_awesome_queue')
            try:
                with queue.getMessages(10) as messages:
                    doSomething(messages)
            except NoMessagesAvailable:
                pass

        """
        logging.debug(f'Receiving SQS messages')

        rawMessages = []
        remaining = maxMessages
        while remaining > 0:
            batchSize = min(10, remaining)
            fetched = self.queue.receive_messages(
                MessageAttributeNames=['*'],
                MaxNumberOfMessages=batchSize
            )
            if len(fetched) > 0:
                rawMessages = rawMessages + fetched
                remaining -= len(fetched)
            else:
                break

        if len(rawMessages) == 0:
            logging.info(f'No messages available from {self}')
            raise NoMessagesAvailable

        messages = [self._decode(m) for m in rawMessages]
        logging.info(f'Received {len(messages)} messages from {self}')

        yield messages

        logging.info(f'Deleting {len(messages)} messages from {self}')
        for rawMessage in rawMessages:
            rawMessage.delete()

    def __len__(self):
        attributes = self.client.get_queue_attributes(
            QueueUrl=self.queue.url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        numMessages = int(attributes['Attributes']['ApproximateNumberOfMessages'])
        return numMessages

    def _encode(self, message: BaseMessage) -> dict:
        genericEncoded = encodeMessage(message)
        return {
            'MessageBody': genericEncoded['body'],
            'MessageAttributes': genericEncoded['attributes']
        }

    def _decode(self, rawMessage) -> BaseMessage:
        return decodeMessage(
            body=rawMessage.body,
            attributes=rawMessage.message_attributes
        )
