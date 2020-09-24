import boto3
import typing
import functools

from ._bases import BaseMixin
from ..interfaces.messaging import MessageConsumer, MessageTooLarge
from ..models.messages import BaseMessage
from ._common import encodeMessage
from ..utils import logging


@functools.lru_cache(maxsize=16)
class Topic(BaseMixin, MessageConsumer):

    def __init__(self, topicArn: str):
        super().__init__(topicArn)
        self.topic = boto3.resource('sns').Topic(topicArn)

    def sendMessage(self, message: BaseMessage) -> None:
        """Posts a notification to SNS"""
        encoded = self._encode(message)

        logging.info(
            'Sending {msg} of {size} bytes to {dest}'
            .format(
                msg=str(message),
                size=message.getApproxSize(),
                dest=str(self)
            )
        )

        try:
            self.topic.publish(**encoded)
        except boto3.client('sns').exceptions.InvalidParameterException as _ex:
            raise MessageTooLarge(str(_ex))

    def _encode(self, message: BaseMessage) -> dict:
        genericEncoded = encodeMessage(message)
        return {
            'Message': genericEncoded['body'],
            'MessageAttributes': genericEncoded['attributes']
        }
