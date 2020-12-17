"""AWS SNS as a Pylon output."""

from typing import Iterable

from ..aws import sns
from ..interfaces.messaging import MessageConsumer
from ..models.messages import BaseMessage

class TopicMessageConsumer(MessageConsumer):
    """A message consumer which uses AWS SNS to send messages."""

    def __init__(self, topic_arn: str):
        self._topic = sns.Topic(topic_arn)

    def sendMessage(self, message: BaseMessage) -> None:
        self._topic.sendMessage(message)

    def sendMessages(self, messages: Iterable[BaseMessage]) -> None:
        self._topic.sendMessages(messages)
