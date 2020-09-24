"""
This is `lambda_` because `lambda` is a reserved word in python...
"""

import json
import functools
import contextlib
import typing

import boto3

from ..models.messages import BaseMessage, ObjectType, LambdaEvent
from ..interfaces.messaging import MessageProducer
from ._common import decodeMessage, encodeMessage


@functools.lru_cache(maxsize=1)
def lambdaClient():
    return boto3.client('lambda')


def invoke_sync(funcname: str, event: dict=None) -> dict:
    if event is None:
        event = {}

    payload = json.dumps(event).encode('utf-8')
    response = lambdaClient().invoke(
        FunctionName=funcname,
        InvocationType='RequestResponse',
        Payload=payload
    )

    response_payload = response.get('Payload')
    if response_payload:
        return response_payload.read().decode('utf-8')


def invoke_async(funcname: str, event: dict=None):
    if event is None:
        event = {}

    payload = json.dumps(event).encode('utf-8')
    lambdaClient().invoke(
        FunctionName=funcname,
        InvocationType='Event',
        Payload=payload
    )


class Function:
    """
    Convenience class for Lambda function

    Args:
        funcname (str): name of the lambda function
        asynchronous (bool): whether to invoke the function synchronously or
            asynchronously, defaults to False.

    Usage:
    ```
    f = Function('my-function-name', asynchronous=True)

    f({'foo': 'bar'}) # call function with
    ```
    """
    def __init__(self, funcname, asynchronous=False):
        if asynchronous:
            self.function = functools.partial(invoke_async, funcname)
        else:
            self.function = functools.partial(invoke_sync, funcname)

    def __call__(self, event):
        if isinstance(event, BaseMessage):
            event = encodeMessage(event)

        return self.function(event)


class PseudoQueue(MessageProducer):
    def __new__(cls, event: dict):
        # determine which pseudo-queue is more appropriate
        if (
            "Records" in event
            and event["Records"][0]["eventSource"] == "aws:sqs"
        ):
            new_cls = PseudoQueueForSQSEvent
        elif (
            "body" in event
            and "attributes" in event
            and event["attributes"]["objectType"]["StringValue"] == ObjectType.LAMBDA_EVENT
        ):
            new_cls = PseudoQueueSimpleEvent
        else:
            raise ValueError('Unable to determine the event type')
        # create a new object instance with the desired class
        instance = object.__new__(new_cls)
        # pass the object instance to run `__init__`
        return instance


class PseudoQueueSimpleEvent(PseudoQueue):
    """
    Example event:
    ```json
    {
        "body": {"partition": "a/b/c"},
        "attributes": {
            "payloadMimeType": {"StringValue": "text/json"},
            "objectType": {"StringValue": "lambdaEvent"},
            "ingestionId": {"StringValue": "None"}
        }
    }
    ```
    """
    def __init__(self, event: dict):
        self.event = event
        self.queue = [event]
        self.deleted = []

    @contextlib.contextmanager
    def getMessage(self):
        rawMessage = self.queue.pop(0)
        yield self._decode(rawMessage)
        self.deleted.append(rawMessage)

    def _decode(self, rawMessage: dict):
        return decodeMessage(rawMessage['body'], rawMessage['attributes'])

    def __len__(self):
        return len(self.queue)


class PseudoQueueForSQSEvent(PseudoQueue):
    """
    Example event:
    ```json
    {
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
                "messageAttributes": {},
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
                "messageAttributes": {},
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            }
        ]
    }
    ```
    """
    def __init__(self, event):
        self.event = event
        # take a copy of the records list as the queue
        self.queue = event["Records"][:]
        self.deleted = []

    @contextlib.contextmanager
    def getMessage(self):
        rawMessage = self.queue.pop(0)
        yield self._decode(rawMessage)
        self.deleted.append(rawMessage)

    def _decode(self, rawMessage: dict):
        return decodeMessage(rawMessage["body"], rawMessage["messageAttributes"])

    def __len__(self):
        return len(self.queue)
