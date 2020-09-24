import copy

import pytest

from pylon.models import messages


@pytest.mark.parametrize(
    'useFixture,expected',
    [
        ('testMessage_inline', 'hello'),
        ('testMessage_s3', 'really big string')
    ],
    indirect=['useFixture']
)
def test_getBody(useFixture, expected):
    testMessage = useFixture
    assert testMessage.getBody() == expected


@pytest.mark.parametrize(
    'useFixture,expected',
    [
        ('testMessage_inline', 135),
        ('testMessage_s3', 170)
    ],
    indirect=['useFixture']
)
def test_getApproxSize(useFixture, expected):
    testMessage = useFixture
    assert testMessage.getApproxSize() == expected


@pytest.mark.parametrize(
    'useFixture,expected',
    [
        ('testMessage_inline', '<BaseMessage body="hello">'),
        ('testMessage_s3', '<BaseMessage body="really big...">')
    ],
    indirect=['useFixture']
)
def test_str(useFixture, expected):
    testMessage = useFixture
    assert str(testMessage) == expected


def test_eq(testMessage_inline, testMessage_s3):
    assert testMessage_inline != testMessage_s3

    testMessage_s3_copy = copy.copy(testMessage_s3)
    assert testMessage_s3 == testMessage_s3_copy

    testMessage_s3_copy.body = 'i can see clearly now the rain is gone'
    assert testMessage_s3 != testMessage_s3_copy
