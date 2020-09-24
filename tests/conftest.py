import pytest
import pylon


@pytest.fixture
def useFixture(request):
    """Uses a fixture by name"""
    return request.getfixturevalue(request.param)


@pytest.fixture
def useFixtures(request):
    """Uses multiple fixtures as specified by name in a list"""
    return [
        request.getfixturevalue(item)
        for item in request.param
    ]


@pytest.fixture(autouse=True)
def reset_logging():
    pylon.logging.updateLogger(logLevel='info')
