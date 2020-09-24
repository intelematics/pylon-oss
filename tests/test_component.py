import importlib
import json
import logging
import os
from unittest import mock
import time

import pytest

import pylon
from pylon import component, utils



class T:
    def __init__(self, name, sleep=0.1):
        self.name = name
        self.sleep = sleep
        self.objectType = 'test message pls ignore'
        self.payloadStoreKey = None

    def __eq__(self, other):
        return self.name == other.name

    def isCheckedIn(self):
        return self.payloadStoreKey is not None

    def getApproxSize(self):
        return 10


@pytest.fixture
def mockMessageConsumer():
    class MockMessageConsumer(pylon.interfaces.messaging.MessageConsumer):
        def sendMessage(self, message):
            del message
        def _encode(self, message):
            del message

    out = MockMessageConsumer()
    out.sendMessage = mock.MagicMock()
    yield out


@pytest.fixture
def config(monkeypatch):
    config = json.dumps({
        'PYLON_INPUT': 'sqs://ignore',
        'PYLON_OUTPUT': 'sns://ignore',
        'PYLON_LOG_FORMAT': 'txt',
        'PYLON_LOG_LEVEL': 'info',
    })
    monkeypatch.setenv('PYLON_CONFIG', config)
    monkeypatch.setattr(pylon.aws.sqs, 'Queue', mock.MagicMock())


@pytest.fixture
def pylon_component(request, config, mockMessageConsumer):
    if request.param == 'pipeline':
        @component.PipelineComponent
        def coreFunction(message, _config):
            time.sleep(message.sleep)
            return T('out')

        coreFunction.inputMessageProducer = mock.MagicMock()
        coreFunction.outputMessageConsumer = mockMessageConsumer
    elif request.param == 'source':
        @component.SourceComponent
        def coreFunction(message, _config):
            time.sleep(0.1)
            return T('out')

        coreFunction.outputMessageConsumer = mockMessageConsumer
    elif request.param == 'sink':
        @component.SinkComponent
        def coreFunction(message, _config):
            time.sleep(message.sleep)
            return T('out')

        coreFunction.inputMessageProducer = mock.MagicMock()
    else:
        assert False, f'unkown component type {request.param}'

    yield coreFunction


@pytest.mark.parametrize(
    'results,expected,pylon_component',
    [
        ([T('a')], [T('a')], 'source'),
        ([T('a'), T('b')], [T('a'), T('b')], 'source'),
        ([T('a'), None, T('b'), None], [T('a'), T('b')], 'source'),
        ((T(case) for case in 'ab'), [T('a'), T('b')], 'source'),
        (None, None, 'source'),
        ([None, None], None, 'source'),
        ([None, None, None, None, None, None], None, 'source'),
    ],
    indirect=['pylon_component']
)
def test__sendMessages(results, expected, pylon_component):
    pylon_component._sendMessages(results)
    if expected is None:
        pylon_component.outputMessageConsumer.sendMessage.assert_not_called()
    else:
        expected = [mock.call(el) for el in expected]
        assert pylon_component.outputMessageConsumer.sendMessage.call_args_list == expected


@pytest.mark.parametrize(
    ('pylon_component', 'sleep', 'useInput', 'expectOutput'),
    [
        ('pipeline', 0.5, True, True),
        ('source', 0.1, False, True),
        ('sink', 0, True, False),
    ],
    indirect=['pylon_component']
)
def test_Component_runOnce(pylon_component, sleep, useInput, expectOutput, caplog):
    if useInput:
        inputMessage = T('inp', sleep)
        inputMessage.ingestionId = 'parent'
        pylon_component.inputMessageProducer.getMessage.return_value.__enter__.return_value = inputMessage

    pylon_component.runOnce()

    # Extact ingestion step from logs.
    ingestionStepMarker = 'INGESTION_STEP: '
    for record in caplog.records:
        message = record.getMessage()
        print()
        print(message)
        print()
        if message.startswith(ingestionStepMarker):
            break
    else:
        assert False, 'ingestion step not in log'
    ingestionStepJSON = message[len(ingestionStepMarker):]
    outIngestionStep = pylon.models.ingestion.IngestionStep.fromJSON(ingestionStepJSON)

    if expectOutput:
        pylon_component.outputMessageConsumer.sendMessage.assert_called_once_with(T('out'))
    else:
        assert (
            'outputMessageConsumer' not in dir(pylon_component) or
            pylon_component.outputMessageConsumer.sendMessage.assert_not_called()
        )
    assert outIngestionStep.parentIngestionId == ('parent' if useInput else None)
    assert outIngestionStep.metadata['durationSeconds'] == pytest.approx(sleep, rel=0.1)


@pytest.mark.parametrize('pylon_component', ['pipeline'], indirect=['pylon_component'])
def test_Component_runOnce_w_message_store(pylon_component, monkeypatch):
    pylon_component.config.update({
        'PYLON_STORE_MIN_MESSAGE_BYTES': 1,
        'PYLON_STORE_DESTINATION': 's3://test',
    })
    mock_S3MessageStore = mock.MagicMock()
    monkeypatch.setattr(pylon.aws.s3, 'MessageStore', mock_S3MessageStore)

    inputMessage = T('inp', 0)
    inputMessage.ingestionId = 'parent'
    inputMessage.payloadStoreKey = 's3://test/inMessage'
    pylon_component.inputMessageProducer.getMessage.return_value.__enter__.return_value = inputMessage

    pylon_component.runOnce()

    mock_S3MessageStore.checkOutPayload.assert_called_once_with(inputMessage)
    mock_S3MessageStore.return_value.checkInPayload.assert_called_once_with(T('out'))


@pytest.mark.parametrize(
    'pylon_input, pylon_component, exp_module_name, exp_class_name, exp_constructor_call',
    [('sqs://queue_name', 'sink', 'pylon.aws.sqs', 'Queue', 'queue_name')],
    indirect=['pylon_component']
)
def test_Component__getInputFromConfig(
        pylon_input, pylon_component, exp_module_name, exp_class_name, exp_constructor_call,
        monkeypatch
):
    module = importlib.import_module(exp_module_name)
    mock_class = mock.MagicMock()
    monkeypatch.setattr(module, exp_class_name, mock_class)
    pylon_component.config['PYLON_INPUT'] = pylon_input

    pylon_component._getInputFromConfig()

    mock_class.assert_called_once_with(exp_constructor_call)


@pytest.mark.parametrize(
    'pylon_output, pylon_component, exp_module_name, exp_class_name, exp_constructor_call',
    [
        ('sqs://queue_name', 'source', 'pylon.aws.sqs', 'Queue', 'queue_name'),
        ('sns://topic_arn', 'source', 'pylon.aws.sns', 'Topic', 'topic_arn'),
    ],
    indirect=['pylon_component']
)
def test_Component__getOutputFromConfig(
        pylon_output, pylon_component, exp_module_name, exp_class_name, exp_constructor_call,
        monkeypatch
):
    module = importlib.import_module(exp_module_name)
    mock_class = mock.MagicMock()
    monkeypatch.setattr(module, exp_class_name, mock_class)
    pylon_component.config['PYLON_OUTPUT'] = pylon_output

    pylon_component._getOutputFromConfig()

    mock_class.assert_called_once_with(exp_constructor_call)
