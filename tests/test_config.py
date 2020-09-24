import copy
import json
from unittest import mock

import pytest

import pylon

TEST_CONFIG = {'test_key': 'test_value'}
TEST_CONFIG_STR = json.dumps(TEST_CONFIG)


@pytest.fixture
def mockConfigStores(monkeypatch):
    mockS3GetObject = mock.MagicMock(return_value=(TEST_CONFIG_STR, {}))
    mockSSMParameterStore = mock.MagicMock(return_value=TEST_CONFIG_STR)
    mock_readFile = mock.MagicMock(return_value=TEST_CONFIG_STR)

    monkeypatch.setattr(pylon.aws.s3, 'getObject', mockS3GetObject)
    monkeypatch.setattr(pylon.aws.ssm.ParameterStore, 'get', mockSSMParameterStore)
    monkeypatch.setattr(pylon.config, '_readFile', mock_readFile) # This is gross...

    yield {
        's3': mockS3GetObject,
        'ssm': mockSSMParameterStore,
        'file': mock_readFile,
    }

def test_getConfig(monkeypatch):
    # Set up the config
    PYLON_CONFIG = json.dumps({
        'MY_VAR': 'yeet',
        'PYLON_LOOP_SLEEP_SECONDS': 60
    })
    # Patch the config environment variable
    monkeypatch.setenv('PYLON_CONFIG', PYLON_CONFIG)
    actual = pylon.config.getConfig()

    # Assert that defaults appear in config and are overridden by explicitly set values
    expected = {
        'MY_VAR': 'yeet',
        'PYLON_LOOP_SLEEP_SECONDS': 60,
        'PYLON_LOG_FORMAT': 'txt',
        'PYLON_LOG_LEVEL': 'warning',
        'PYLON_STORE_MIN_MESSAGE_BYTES': 250 * 1024,
    }
    assert actual == expected


@pytest.mark.parametrize(
    'config_path, exp_store, exp_call',
    [
        ('s3://test-bucket/test_object.json', 's3', 's3://test-bucket/test_object.json'),
        ('ssm://test_config', 'ssm', '/test_config'),
        ('file:///test-folder/test_file.json', 'file', '/test-folder/test_file.json'),
        ('file://test-folder/test_file.json', 'file', 'test-folder/test_file.json'),
    ]
)
def test__getConfigString(config_path, exp_store, exp_call, mockConfigStores):
    exp_store = mockConfigStores[exp_store]
    out = pylon.config._getConfigString(config_path)

    assert out == TEST_CONFIG_STR
    exp_store.assert_called_once_with(exp_call)


@pytest.mark.parametrize(
    'inp, exp',
    [
        (
            {'MY_VAR': 'value'},
            {
                'MY_VAR': 'value',
                'PYLON_LOOP_SLEEP_SECONDS': 60,
                'PYLON_LOG_FORMAT': 'txt',
                'PYLON_LOG_LEVEL': 'warning',
                'PYLON_STORE_MIN_MESSAGE_BYTES': 250 * 1024,
            }
        ),
        (
            {
                'PYLON_LOOP_SLEEP_SECONDS': 1,
                'PYLON_LOG_FORMAT': 'json',
                'PYLON_LOG_LEVEL': 'info',
                'PYLON_STORE_MIN_MESSAGE_BYTES': 250,
                'PYLON_STORE_DESTINATION': 's3://bucket/prefix',
            },
            {
                'PYLON_LOOP_SLEEP_SECONDS': 1,
                'PYLON_LOG_FORMAT': 'json',
                'PYLON_LOG_LEVEL': 'info',
                'PYLON_STORE_MIN_MESSAGE_BYTES': 250,
                'PYLON_STORE_DESTINATION': 's3://bucket/prefix',
            },
        ),
    ]
)
def test__addDefaults(inp, exp):
    original_inp = copy.deepcopy(inp)

    out = pylon.config._addDefaults(inp)

    assert inp == original_inp
    assert out == exp


@pytest.mark.parametrize(
    'inp, exp',
    [
        (
            {
                'PYLON_LOOP_SLEEP_SECONDS': '0',
                'PYLON_STORE_MIN_MESSAGE_BYTES': '250',
            },
            {
                'PYLON_LOOP_SLEEP_SECONDS': 0,
                'PYLON_STORE_MIN_MESSAGE_BYTES': 250,
            }
        ),
        ({}, {}),
    ]
)
def test__enforceTypes(inp, exp):
    original_inp = copy.deepcopy(inp)

    out = pylon.config._enforceTypes(inp)

    assert inp == original_inp
    assert out == exp


@pytest.mark.parametrize(
    'inp, exp',
    [
        (
            {
                'INPUT_QUEUE_NAME': 'test_queue',
                'OUTPUT_TOPIC_ARN': 'test_topic_arn',
                'PYLON_PLAIN_LOGGING': True
            },
            {
                'PYLON_LOG_FORMAT': 'txt',
                'PYLON_INPUT': 'sqs://test_queue',
                'PYLON_PLAIN_LOGGING': True,
                'PYLON_OUTPUT': 'sns://test_topic_arn',
                'INPUT_QUEUE_NAME': 'test_queue',
                'OUTPUT_TOPIC_ARN': 'test_topic_arn'
            }
        ),
        (
            {
                'INPUT_QUEUE_NAME': 'test_queue',
            },
            {
                'PYLON_INPUT': 'sqs://test_queue',
                'INPUT_QUEUE_NAME': 'test_queue',
            }
        ),
        (
            {
                'OUTPUT_TOPIC_ARN': 'test_topic_arn',
            },
            {
                'OUTPUT_TOPIC_ARN': 'test_topic_arn',
                'PYLON_OUTPUT': 'sns://test_topic_arn',
            }
        ),
        (
            {
                'PYLON_PLAIN_LOGGING': True,
            },
            {
                'PYLON_LOG_FORMAT': 'txt',
                'PYLON_PLAIN_LOGGING': True,
            }
        ),
        (
            {
                'PYLON_PLAIN_LOGGING': False,
            },
            {
                'PYLON_LOG_FORMAT': 'json',
                'PYLON_PLAIN_LOGGING': False,
            }
        ),
    ]
)
def test__checkDeprecation(inp, exp):
    original_inp = copy.deepcopy(inp)

    out = pylon.config._checkDeprecation(inp)

    assert inp == original_inp
    assert out == exp
