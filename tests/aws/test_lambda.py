import pytest
from unittest import mock

from pylon.aws import lambda_
from pylon.models.messages import LambdaEvent, BaseMessage


@pytest.fixture
def mock_lambda_invoke(monkeypatch):
    mock_invoke_sync = mock.MagicMock()
    mock_invoke_async = mock.MagicMock()
    monkeypatch.setattr(lambda_, 'invoke_sync', mock_invoke_sync)
    monkeypatch.setattr(lambda_, 'invoke_async', mock_invoke_async)

    yield mock_invoke_sync, mock_invoke_async


def test_function_call_sync(mock_lambda_invoke):
    mock_invoke_sync, mock_invoke_async = mock_lambda_invoke

    f = lambda_.Function('Pylon', asynchronous=False)
    event = {'foo': 'bar'}
    f(event)
    expected_event = {'foo': 'bar'}
    mock_invoke_sync.assert_called_once_with('Pylon', expected_event)

    g = lambda_.Function('Pylon', asynchronous=True)
    event = LambdaEvent({'foo': 'bar'})
    g(event)
    expected_event = {
        "body": {
            "foo": "bar"
        },
        "attributes": {
            "payloadMimeType": {
                "StringValue": "text/json",
                "DataType": "String"
            },
            "objectType": {
                "StringValue": "lambdaEvent",
                "DataType": "String"
            },
            "ingestionId": {
                "StringValue": "None",
                "DataType": "String"
            },
            "artifactName": {
                "StringValue": "None",
                "DataType": "String"
            },
            "artifactVersion": {
                "StringValue": "None",
                "DataType": "String"
            },
        }
    }
    mock_invoke_async.assert_called_once_with('Pylon', expected_event)


@pytest.fixture
def lambdaEventSimple():
    yield {
        "body": {"partition": "a/b/c"},
        "attributes": {
            "payloadMimeType": {"StringValue": "text/json"},
            "objectType": {"StringValue": "lambdaEvent"},
            "ingestionId": {"StringValue": "None"},
            "artifactName": {"StringValue": "test_artifact"},
            "artifactVersion": {"StringValue": "0.0.0"},
        }
    }

def test_pseudo_queue_simple(lambdaEventSimple):
    q = lambda_.PseudoQueue(lambdaEventSimple)

    assert len(q) == 1

    with q.getMessage() as m:
        assert isinstance(m, BaseMessage)
        assert m.body == {'partition': 'a/b/c'}
        assert m.ingestionId is None
        assert m.payloadMimeType == 'text/json'
        assert m.objectType == 'lambdaEvent'

    assert len(q) == 0

    with pytest.raises(IndexError):
        # getting a message from an empty queue raises IndexError
        with q.getMessage() as m:
            assert True


@pytest.fixture
def lambdaEventSQS():
    yield {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "test",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {
                    "payloadMimeType": {
                        "StringValue": "text",
                        "DataType": "String"
                    },
                    "objectType": {
                        "StringValue": "rawContent",
                        "DataType": "String"
                    },
                    "ingestionId": {
                        "StringValue": "aaaa",
                        "DataType": "String"
                    },
                    "artifactName": {
                        "StringValue": "test_artifact",
                        "DataType": "String"
                    },
                    "artifactVersion": {
                        "StringValue": "0.0.0",
                        "DataType": "String"
                    },
                },
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            },
            {
                "messageId": "2e1424d4-f796-459a-8184-9c92662be6da",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "test",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082650636",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082650649"
                },
                "messageAttributes": {
                    "payloadMimeType": {
                        "StringValue": "text",
                        "DataType": "String"
                    },
                    "objectType": {
                        "StringValue": "rawContent",
                        "DataType": "String"
                    },
                    "ingestionId": {
                        "StringValue": "bbbb",
                        "DataType": "String"
                    },
                    "artifactName": {
                        "StringValue": "test_artifact",
                        "DataType": "String"
                    },
                    "artifactVersion": {
                        "StringValue": "0.0.0",
                        "DataType": "String"
                    },
                },
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            }
        ]
    }

def test_pseudo_queue_sqs(lambdaEventSQS):
    q = lambda_.PseudoQueue(lambdaEventSQS)

    assert len(q) == 2

    with q.getMessage() as m:
        assert isinstance(m, BaseMessage)
        assert m.body == 'test'
        assert m.ingestionId == 'aaaa'
        assert m.payloadMimeType == 'text'
        assert m.objectType == 'rawContent'
        assert m.artifactName == 'test_artifact'
        assert m.artifactVersion == '0.0.0'

    assert len(q) == 1

    with q.getMessage() as m:
        assert isinstance(m, BaseMessage)
        assert m.body == 'test'
        assert m.ingestionId == 'bbbb'
        assert m.payloadMimeType == 'text'
        assert m.objectType == 'rawContent'
        assert m.artifactName == 'test_artifact'
        assert m.artifactVersion == '0.0.0'

    assert len(q) == 0

    with pytest.raises(IndexError):
        # getting a message from an empty queue raises IndexError
        with q.getMessage() as m:
            assert True


def test_pseudo_queue_raises():
    event = {'foo': 'bar'}

    # when the event doesn't appear to be from pylon or SQS, it gives up
    with pytest.raises(ValueError):
        lambda_.PseudoQueue(event)
