"""AWS SNS as a Pylon output."""

from typing import Iterable

from ..aws import sns
from ..interfaces.messaging import MessageConsumer
from ..models.messages import BaseMessage
from._common import encode_message

class TopicMessageConsumer(MessageConsumer):
    """A message consumer which uses AWS SNS to send messages."""

    def __init__(self, topic_arn: str):
        self._topic = sns.Topic(topic_arn)

    def send_messages(self, messages: Iterable[BaseMessage]) -> None:
        for message in messages:
            encoded_message = encode_message(message)
            sns_message = sns.Message(
                body=encoded_message['body'],
                attributes=encoded_message['attributes']
            )
            self._topic.publish_message(sns_message)
