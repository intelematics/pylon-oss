import typing
import abc
import contextlib

from .. import models


class NoMessagesAvailable(Exception):
    """Raised when a message queue does not have any available messages"""
    pass


class MessageTooLarge(Exception):
    """Raised when a message fails to send beacuse it is too large"""


class MessageProducer(abc.ABC):

    @abc.abstractmethod
    @contextlib.contextmanager
    def getMessage(self) -> typing.Generator[models.messages.BaseMessage, None, None]:
        raise NotImplementedError


class MessageConsumer(abc.ABC):

    @abc.abstractmethod
    def sendMessage(self, message: models.messages.BaseMessage) -> None:
        raise NotImplementedError

    def sendMessages(self, messages: typing.Iterable[models.messages.BaseMessage]) -> None:
        """
        Default implementation for sendMessages. If a more efficient method of batching
        messages exists then subclasses should implement it. However some subclasses
        may not have a better way than this.
        """
        for message in messages:
            self.sendMessage(message)


class MessageStore(abc.ABC):

    @abc.abstractmethod
    def checkInPayload(self, message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Stores the message body to be retrieved later

        Args:
            message (models.messages.BaseMessage): The message to be stored

        Returns:
            models.messages.BaseMessage: The message with the payload pointing to the store
        """
        raise NotImplementedError

    @abc.abstractmethod
    def checkOutPayload(self, message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Retrieves the message body from the message store

        Args:
            message (models.messages.BaseMessage): The message to be loaded

        Returns:
            models.messages.BaseMessage: The loaded message
        """
        raise NotImplementedError
