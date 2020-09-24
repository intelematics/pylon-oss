import pytest
from unittest import mock


@pytest.mark.parametrize(
    'useFixtures',
    [
        ('testMessage_inline', 'rawMessage_inline'),
        ('testMessage_s3', 'rawMessage_s3')
    ],
    indirect=['useFixtures']
)
def test_Topic_encode(mockAWS, testTopic, useFixtures):
    testMessage, rawMessage = useFixtures

    encoded = testTopic._encode(testMessage)
    assert encoded['Message'] == rawMessage.body
    assert encoded['MessageAttributes'] == rawMessage.message_attributes


def test_Topic_sendMessage(
    monkeypatch, caplog, mockAWS, testTopic,
    testMessage_inline, rawMessage_inline
):
    monkeypatch.setattr(testTopic.topic, 'publish', mock.MagicMock())

    testTopic.sendMessage(testMessage_inline)
    expected = {
        'Message': rawMessage_inline.body,
        'MessageAttributes': rawMessage_inline.message_attributes
    }

    # ensure that the `topic.publish` has been called with the expected args
    testTopic.topic.publish.assert_called_once_with(**expected)

    assert (
        'Sending <BaseMessage body="hello"> of 145 bytes to '
        '<pylon.aws.sns.Topic PylonTopicBestTopic>'
    ) in caplog.text
