from unittest import mock
import uuid

import pytest

from pylon.aws import s3


@pytest.fixture
def s3MessageStore():
    return s3.MessageStore('s3://test-bucket/test.prefix/')


def test_MessageStore_checkInPayload(monkeypatch, s3MessageStore, testMessage_inline):
    mockPutObject = mock.MagicMock()
    monkeypatch.setattr(s3, 'putObject', mockPutObject)
    monkeypatch.setattr(uuid, 'uuid4', lambda: 'test-uuid')

    message = s3MessageStore.checkInPayload(testMessage_inline)

    mockPutObject.assert_called_once_with(
        's3://test-bucket/test.prefix/test-uuid', 'hello', encoding='utf-8'
    )
    assert message.payloadStoreKey == 's3://test-bucket/test.prefix/test-uuid'
    assert message.body == 's3://test-bucket/test.prefix/test-uuid'


def test_MessageStore_checkOutPayload(monkeypatch, testMessage_s3):
    mockGetObject = mock.MagicMock(return_value=('really big string'.encode('utf-8'), {}))
    monkeypatch.setattr(s3, 'getObject', mockGetObject)
    testMessage_s3.body = 'remove actual body'

    message = s3.MessageStore.checkOutPayload(testMessage_s3)

    mockGetObject.assert_called_once_with('s3://pylon-special/test.txt')
    assert message.payloadStoreKey is None
    assert message.body == 'really big string'
