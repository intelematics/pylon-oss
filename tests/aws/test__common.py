import pytest

from pylon.aws import _common


@pytest.mark.parametrize(
    'useFixtures',
    [
        ('testMessage_inline', 'rawMessage_inline'),
        ('testMessage_s3', 'rawMessage_s3')
    ],
    indirect=['useFixtures']
)
def test_encodeMessage(useFixtures):
    message, rawMessage = useFixtures
    encoded = _common.encodeMessage(message)

    assert encoded['body'] == rawMessage.body
    assert encoded['attributes'] == rawMessage.message_attributes


@pytest.mark.parametrize(
    'useFixtures',
    [
        ('testMessage_inline', 'rawMessage_inline'),
        ('testMessage_s3', 'rawMessage_s3')
    ],
    indirect=['useFixtures']
)
def test_decodeMessage(useFixtures):
    message, rawMessage = useFixtures
    decoded = _common.decodeMessage(rawMessage.body, rawMessage.message_attributes)

    assert decoded.body == message.body
    assert decoded.payloadMimeType == message.payloadMimeType
    assert decoded.objectType == message.objectType
    assert decoded.ingestionId == message.ingestionId
