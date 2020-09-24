import contextlib
import json
import os
import typing
import uuid

from ..interfaces.messaging import MessageConsumer, MessageProducer, NoMessagesAvailable
from .. import interfaces
from .. import models
from .. import utils

class FolderMessageProducerConsumer(MessageProducer, MessageConsumer):
    def __init__(self, path: str):
        self.path = path
        os.makedirs(path, exist_ok=True)

    @contextlib.contextmanager
    def getMessage(self) -> models.messages.BaseMessage:
        filepaths = _listFilepaths(self.path)

        try:
            filepath = next(filepaths)
        except StopIteration:
            raise NoMessagesAvailable(f'No messages available under path {self.path}')

        raw_message = _readFile(filepath)

        message = _decode(raw_message)
        yield message
        os.remove(filepath)

    def sendMessage(self, message: models.messages.BaseMessage):
        raw_message = _encode(message)
        filename = f'{uuid.uuid4()}.json'
        filepath = os.path.join(self.path, filename)
        _writeFile(raw_message, filepath)


def _decode(raw_message: str) -> models.messages.BaseMessage:
    message_dict = json.loads(raw_message)
    message = models.messages.BaseMessage()
    for attr in utils.getClassAttributes(models.messages.BaseMessage):
        setattr(message, attr, message_dict[attr])

    if message.objectType in (
            models.messages.ObjectType.INGESTION_STEP, models.messages.ObjectType.DATA_ASSET
    ):
        message.body = interfaces.serializing.JsonSerializable.fromJSON(message.body)

    return message


def _encode(message: models.messages.BaseMessage) -> str:
    message_dict = {
        attr: getattr(message, attr)
        for attr in utils.getClassAttributes(models.messages.BaseMessage)
    }

    if isinstance(message.body, interfaces.serializing.JsonSerializable):
        message.body = message.body.toJSON()

    raw_message = json.dumps(message_dict)
    return raw_message


def _listFilepaths(path: str) -> typing.Iterable[str]:
    return (
        filepath
        for filename in os.listdir(path)
        if os.path.isfile((filepath := os.path.join(path, filename)))
        if not filename.startswith('.')
    )


def _writeFile(inp: str, filepath: str):
    with open(filepath, 'w') as _fd:
        _fd.write(inp)


def _readFile(filepath: str) -> bytes:
    with open(filepath, 'r') as _fd:
        out = _fd.read()
    return out
