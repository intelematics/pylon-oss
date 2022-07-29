import typing
import abc

from .. import models


class MessageTooLarge(Exception):
    """Raised when a message fails to send beacuse it is too large"""


class MessageProducer(abc.ABC):

    @abc.abstractmethod
    def get_messages(self) -> typing.Iterable[models.messages.BaseMessage]:
        raise NotImplementedError


class MessageConsumer(abc.ABC):

    @abc.abstractmethod
    def sendMessages(self, messages: typing.Iterable[models.messages.BaseMessage]) -> None:
        raise NotImplementedError


class MessageStore(abc.ABC):

    @abc.abstractmethod
    def check_message_in(self, message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Stores the message body to be retrieved later

        Args:
            message (models.messages.BaseMessage): The message to be stored

        Returns:
            models.messages.BaseMessage: The message with the payload pointing to the store
        """
        raise NotImplementedError

    @abc.abstractmethod
    def check_out_message(self, message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Retrieves the message body from the message store

        Args:
            message (models.messages.BaseMessage): The message to be loaded

        Returns:
            models.messages.BaseMessage: The loaded message
        """
        raise NotImplementedError
