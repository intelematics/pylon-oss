import copy
from unittest import mock

import pytest

from pylon.aws import sqs

@pytest.mark.parametrize(
    'useFixtures',
    [
        ('testMessage_inline', 'rawMessage_inline'),
        ('testMessage_s3', 'rawMessage_s3')
    ],
    indirect=['useFixtures']
)
def test_Queue_encode_decode(mockAWS, testQueue, useFixtures):
    testMessage, rawMessage = useFixtures

    encoded = testQueue._encode(testMessage)
    assert encoded['MessageBody'] == rawMessage.body
    assert encoded['MessageAttributes'] == rawMessage.message_attributes

    decoded = testQueue._decode(rawMessage)
    assert testMessage == decoded


@pytest.fixture
def mockData(testQueue, monkeypatch, rawMessage_inline):
    queue = [
        copy.deepcopy(rawMessage_inline)
    ]

    def receive_messages(**kwargs):
        """SQS returns a list of messages, each with a delete callback"""
        MaxNumberOfMessages = kwargs['MaxNumberOfMessages']
        messages = []

        index = 0
        while index < len(queue) and len(messages) < MaxNumberOfMessages:
            message = queue[index]
            message.delete = lambda: queue.pop(0)
            messages.append(message)
            index += 1

        return messages

    def send_message(MessageBody, MessageAttributes, **kwargs):
        message = mock.MagicMock()
        message.body = MessageBody
        message.message_attributes = MessageAttributes
        # set any additional attributes onto the mock object
        for key, value in kwargs.items():
            setattr(message, key, value)

        queue.append(message)

    def send_messages(Entries):
        for message in Entries:
            send_message(**message)

    monkeypatch.setattr(testQueue.queue, 'receive_messages', receive_messages)
    monkeypatch.setattr(testQueue.queue, 'send_message', send_message)
    monkeypatch.setattr(testQueue.queue, 'send_messages', send_messages)

    yield queue


def test_Queue_send(caplog, mockAWS, testQueue, mockData, testMessage_s3, rawMessage_s3):
    assert len(mockData) == 1

    testQueue.sendMessage(testMessage_s3)

    assert len(mockData) == 2

    assert (
        'Sending <BaseMessage body="really big..."> of 180 bytes to '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) in caplog.text

    assert mockData[1].message_attributes == rawMessage_s3.message_attributes


def test_Queue_send_many(caplog, mockAWS, testQueue, mockData, testMessage_s3, rawMessage_s3):
    assert len(mockData) == 1

    testQueue.sendMessages([testMessage_s3, testMessage_s3])

    assert len(mockData) == 3

    assert (
        'Sending 2 messages totalling 360 bytes to '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) in caplog.text

    assert mockData[1].message_attributes == rawMessage_s3.message_attributes
    assert mockData[2].message_attributes == rawMessage_s3.message_attributes

    assert mockData[1].Id != mockData[2].Id


def test_Queue_receive(caplog, mockAWS, testQueue, mockData, testMessage_inline):
    # retrieves a message from the queue. check that it's encoded correctly,
    # and it is popped from the queue at the end
    with testQueue.getMessage() as message:

        assert (
            'Received <BaseMessage body="hello"> from '
            '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
        ) in caplog.text

        assert message == testMessage_inline
        assert len(mockData) == 1

    assert (
        'Deleting <BaseMessage body="hello"> from '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) in caplog.text

    assert len(mockData) == 0


def test_Queue_receive_many(caplog, mockAWS, testQueue, mockData, testMessage_inline, testMessage_s3, rawMessage_s3):
    assert len(mockData) == 1

    testQueue.sendMessages([testMessage_s3, testMessage_s3])

    assert len(mockData) == 3

    with testQueue.getMessages(2) as messages:

        assert (
            'Received 2 messages from '
            '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
        ) in caplog.text

        assert len(messages) == 2
        assert len(mockData) == 3
        assert messages[0] == testMessage_inline
        assert messages[1] == testMessage_s3

    assert (
        'Deleting 2 messages from '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) in caplog.text

    assert len(mockData) == 1


def test_Queue_receive_empty(caplog, mockAWS, testQueue, mockData):
    with testQueue.getMessage():
        # throwaway the message in the queue
        pass

    assert len(mockData) == 0

    # retrieving a message from an empty queue triggers NoMessagesAvailable
    with pytest.raises(sqs.NoMessagesAvailable):
        with testQueue.getMessage():
            pass # pragma: no cover

    assert (
        'No messages available from '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) in caplog.text


def test_Queue_receive_error(caplog, mockAWS, testQueue, mockData, testMessage_inline):
    class SkipProcessing(Exception):
        """Custom exception to mimic a processing error"""
        pass

    try:
        with testQueue.getMessage() as message:
            assert (
                'Received <BaseMessage body="hello"> from '
                '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
            ) in caplog.text

            assert message == testMessage_inline
            assert len(mockData) == 1
            raise SkipProcessing
    except SkipProcessing:
        pass

    assert (
        'Deleting <BaseMessage body="hello"> from '
        '<pylon.aws.sqs.Queue PylonQueueBestQueue>'
    ) not in caplog.text

    # check that if an error is raised during processing, the message is not popped
    assert len(mockData) == 1

