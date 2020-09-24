import pytest
import unittest
from unittest import mock

import boto3

from pylon.aws import sns, sqs, s3, dynamodb
from pylon.models.messages import BaseMessage


@pytest.fixture
def mockAWS(monkeypatch):
    """mock AWS endpoints"""
    monkeypatch.setattr(boto3, 'resource', mock.MagicMock())
    monkeypatch.setattr(boto3, 'client', mock.MagicMock())

@pytest.fixture
def testBucket(mockAWS):
    """Provides a bucket we can use"""
    s3.Bucket.s3Client = boto3.client('s3')
    yield s3.Bucket('PylonBucketBestBucket')

@pytest.fixture
def testTopic(mockAWS):
    """Provides a topic we can use"""
    yield sns.Topic('PylonTopicBestTopic')


@pytest.fixture
def testQueue(mockAWS):
    """Provides a topic we can use"""
    yield sqs.Queue('PylonQueueBestQueue')

@pytest.fixture
def testDynamoDBTable(mockAWS):
    """Provides a dynamoDB Table we can use"""
    yield dynamodb.Table('PylonTableBestTable')


@pytest.fixture
def testMessage_inline():
    """Provides a inline message"""
    message = BaseMessage()
    message.body = 'hello'
    message.payloadMimeType = 'text'
    message.objectType = 'rawContent'
    message.artifactName = 'test_artifact'
    message.artifactVersion = '0.0.0'

    yield message

@pytest.fixture
def rawMessage_inline():
    """Provides a mock of a raw SQS message"""
    message = mock.MagicMock()
    message.body = 'hello'
    message.message_attributes = {
        'payloadMimeType': {'StringValue': 'text', 'DataType': 'String'},
        'objectType': {'StringValue': 'rawContent', 'DataType': 'String'},
        'ingestionId': {'StringValue': 'None', 'DataType': 'String'},
        'artifactName': {'StringValue': 'test_artifact', 'DataType': 'String'},
        'artifactVersion': {'StringValue': '0.0.0', 'DataType': 'String'},
    }
    yield message

@pytest.fixture
def testMessage_s3():
    """Provides an s3 message"""
    message = BaseMessage()
    message.body = 'really big string'
    message.payloadMimeType = 'text'
    message.objectType = 'rawContent'
    message.payloadStoreKey = 's3://pylon-special/test.txt'
    message.artifactName = 'test_artifact'
    message.artifactVersion = '0.0.0'

    yield message

@pytest.fixture
def rawMessage_s3(monkeypatch):
    """Provides a mock of a raw SQS message"""
    s3Contents = {
        's3://pylon-special/test.txt': ('really big string', {})
    }

    origS3GetObject = s3.getObject
    def fakeS3GetObject(s3Path, encoding=None):
        if s3Path in s3Contents:
            return s3Contents[s3Path]
        else:
            return origS3GetObject(s3Path, encoding=encoding)

    monkeypatch.setattr(s3, 'getObject', fakeS3GetObject)

    message = mock.MagicMock()
    message.body = 'really big string'
    message.message_attributes = {
        'payloadMimeType': {'StringValue': 'text', 'DataType': 'String'},
        'objectType': {'StringValue': 'rawContent', 'DataType': 'String'},
        'ingestionId': {'StringValue': 'None', 'DataType': 'String'},
        'payloadStoreKey': {'StringValue': 's3://pylon-special/test.txt', 'DataType': 'String'},
        'artifactName': {'StringValue': 'test_artifact', 'DataType': 'String'},
        'artifactVersion': {'StringValue': '0.0.0', 'DataType': 'String'},
    }
    yield message

