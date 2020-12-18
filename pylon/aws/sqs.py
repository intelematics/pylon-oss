"""Provides an interface for interacting with AWS SQS Queues."""
from __future__ import annotations

import contextlib
import functools
from typing import ContextManager, Iterable, Optional
import uuid

import boto3

from ._bases import BaseMixin
from ._common import Message
from ..utils import chunked
from ..utils import logging


class NoMessagesAvailable(Exception):
    """Raised when there are no messages to get from an SQS Queue."""


@functools.lru_cache(maxsize=16)
class Queue(BaseMixin):
    """A class for sending and receiving messages from an AWS SQS Queue."""
    def __init__(self, queueName: str):
        super().__init__(queueName)
        self.resource = boto3.resource('sqs')
        self.client = boto3.client('sqs')
        self.queue = self.resource.get_queue_by_name(QueueName=queueName)

    def send_message(self, message: Message) -> None:
        """Sends a message to an SQS queue.

        Args:
            message (Messages): The message to be sent.
        """
        # Send the message
        logging.debug('Sending message to %s.', self.name)
        self.queue.send_message(**message._encode())  # pylint: disable=protected-access

    def send_messages(self, messages: Iterable[Message], chunk_size: Optional[int] = 10) -> None:
        """Send messages to SQS in a batch.

        Args:
            messages (Iterable[Message]): The messages to be sent.
            chunk_size (int, optional): The number of messages to send to SQS in one go.
                Defaults to 10.
        """
        for i, chunk in enumerate(chunked(messages, chunkSize=chunk_size)):
            logging.debug('Preparing to send chunk %s to %s', i, self.name)
            # Encode each message in this chunk
            entries = [message._encode() for message in chunk]  # pylint: disable=protected-access

            # Update each message in this chunk with an ID to make it unique within this chunk.
            for message in entries:
                message['Id'] = str(uuid.uuid1())

            # Send this chunk to the queue.
            logging.debug('Sending %s messages to %s from chunk %s', len(entries), self.name, i)
            self.queue.send_messages(Entries=entries)
            # TODO: Check for messages which failed to send
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_messages

    @contextlib.contextmanager
    def get_message(self) -> ContextManager[Message]:
        """Gets a message from the SQS Queue. Deletes the messsage from the queue once the context
            is successfully existed.

        Raises:
            NoMessagesAvailable: Raised when there is not message availabe in the queue to get.

        Returns:
            Message: The message retrieved from the queue.
        """
        logging.debug('Receiving SQS message from %s', self.name)
        # Retrieve the message
        messages = self.queue.receive_messages(
            MessageAttributeNames=['*'],
            MaxNumberOfMessages=1
        )

        # If there were no messages to retrieve, raise an exception
        if len(messages) == 0:
            logging.debug('No messages available from %s', self.name)
            raise NoMessagesAvailable

        # Decode and yield the message received
        raw_message = messages[0]
        logging.debug('Received message with id "%s" from %s', raw_message.message_id, self.name)
        message = Message._decode(raw_message)  # pylint: disable=protected-access
        yield message

        # Delete the message once the context has been exited successfully
        logging.debug('Deleting message with id "%s" from %s', raw_message.message_id, self.name)
        raw_message.delete()

    def get_messages(
            self, max_messages: Optional[int] = 10, batch_size: Optional[int] = 10
    ) -> Iterable[Message]:
        """Gets up to the number of messages specified from the queue.

        Messages are returned as a generator and each message is deleted from the queue when the
            next message in the generator is requested.

        Args:
            max_messages (Optional[int], optional): The maximum number of messages to return.
                Defaults to 10.
            batch_size (Optional[int], optional): The number of messages to ask SQS for in any one
                request. Defaults to 10.

        Returns:
            Iterable[Message]: A generator of messages.
        """
        logging.debug(
            'Receiving up to %d messages from %s in batches of %d', max_messages, self.name,
            batch_size
        )

        remaining = max_messages
        batch_n = 0
        while remaining > 0:
            logging.debug('Fetching batch number %d from %s', batch_n, self.name)
            fetched = self.queue.receive_messages(
                MessageAttributeNames=['*'],
                MaxNumberOfMessages=min(batch_size, remaining)
            )

            for raw_message in fetched:
                # Perhaps this isn't the best strategy. If the caller stops iterating, the last
                # message yielded won't be deleted.
                logging.debug('Yielding message with id "%s"', raw_message.message_id)
                message = Message._decode(raw_message)  # pylint: disable=protected-access
                yield message
                raw_message.delete()
                logging.debug(
                    'Deleting message with id "%s" from %s', raw_message.message_id, self.name
                )

            remaining -= len(fetched)
            batch_n += 1

    def __len__(self):
        attributes = self.client.get_queue_attributes(
            QueueUrl=self.queue.url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(attributes['Attributes']['ApproximateNumberOfMessages'])
        return num_messages
