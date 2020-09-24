import pytest

from pylon.models.messages import BaseMessage


@pytest.fixture
def testMessage_inline():
    """Provides a inline message"""
    message = BaseMessage()
    message.body = 'hello'
    message.payloadMimeType = 'text'
    message.objectType = 'rawContent'

    yield message

@pytest.fixture
def testMessage_s3():
    """Provides an s3 message"""
    message = BaseMessage()
    message.body = 'really big string'
    message.payloadMimeType = 'text'
    message.objectType = 'rawContent'
    message.payloadStoreKey = 's3://pylon-special/test.txt'

    yield message
