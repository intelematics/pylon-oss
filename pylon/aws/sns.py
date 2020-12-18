"""Provides an interface for interacting with AWS SNS Topics."""
import functools

import boto3

from ._bases import BaseMixin
from ._common import Message
from ..utils import logging


class MessageTooLarge(Exception):
    """Raised when trying to send a message to an SNS topic but the message is too large."""


@functools.lru_cache(maxsize=16)
class Topic(BaseMixin):
    """A class for publishing messages to an AWS SNS Topic."""
    def __init__(self, topic_arn: str):
        super().__init__(topic_arn)
        self.topic = boto3.resource('sns').Topic(topic_arn)

    def publish_message(self, message: Message) -> None:
        """Publishes a message to an SNS Topic.

        Args:
            message (Message): The message to publish to SNS.

        Raises:
            MessageTooLarge: Raised when the message being published is too large to do so.
        """
        logging.debug('Publishing message to %s', self.name)

        encoded_message = message._encode()  # pylint: disable=protected-access
        try:
            self.topic.publish(**encoded_message)
        except boto3.client('sns').exceptions.InvalidParameterException as _ex:
            raise MessageTooLarge(str(_ex)) from _ex
