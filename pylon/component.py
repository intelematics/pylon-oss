"""
Defines components.

- PipelineComponent: receive message from a queue, publishes to a topic
- SourceComponent: generates data somehow (e.g. external fetch), publishes to a topic
- SinkComponent: receive messages, do not publish anything
- NullComponent: does not receive or publish anything
"""

import datetime
from typing import Any, Callable, Dict, Iterable

from . import aws

from .models.data import DataAsset
from .models.ingestion import IngestionStep
from .models.messages import BaseMessage, ObjectType
from .interfaces.messaging import MessageProducer, MessageConsumer, MessageTooLarge, MessageStore
from .interfaces.entrypoint import Entrypoint
from .io import FolderMessageProducerConsumer, TopicMessageConsumer, QueueMessageProducerConsumer
from .utils import logging
from .utils import catchAllExceptionsToLog


class Component(Entrypoint):
    def __init__(self, core_function: Callable):
        self.core_function: Callable = core_function

        self.input: MessageProducer = self._get_input()  # TODO: Move this function to io
        self.output: MessageConsumer = self._get_output()  # TODO: Move this function to io

        self.message_store: MessageStore = self._get_message_store()  # TODO: Move this function to io

    @catchAllExceptionsToLog  # TODO: Find a better way to do this, maybe just change decorator
    def run_once(self) -> None:
        logging.info('heartbeat: run_once')
        messages = self._get_messages
        for message in messages:
            context: Dict[str, Any] = dict()
            self.setup(context)
            message = self.prepare_in_message(message, context)

            context['start_datetime'] = _utcnow()
            out_messages = self.core_function(message, self.config)
            context['end_datetime'] = _utcnow()

            if self.has_output:
                out_messages = self.prepare_out_messages(out_messages, context)
                self._send_messages(out_messages)
            elif out_messages is not None:
                logging.warning('Component produced an output, but no output has been specified.')
            self.teardown(context)

    def setup(self, context: Dict[str, Any]) -> None:
        # Create ingestion step
        ingestion_step = IngestionStep()
        ingestion_step.populate(self.config)

        # Logging
        logging.updateLogger(
            ingestionId=ingestion_step.ingestionId, logFormat=self.config['PYLON_LOG_FORMAT'],
            logLevel=self.config['PYLON_LOG_LEVEL']
        )
        logging.info('heartbeat: start_core_process')

        context['ingestion_step'] = ingestion_step

    def prepare_in_message(self, message: BaseMessage, context: Dict[str, Any]) -> BaseMessage:
        ingestion_step = context['ingestion_step']
        ingestion_step.parentIngestionId = message.parentIngestionId

        logging.info({
            'message': 'Preparing message.',
            'parentIngestionId': str(ingestion_step.parentIngestionId)
        })

        # Retrieve message if it is checked in to a message store
        if message is not None and message.isCheckedIn():
            message = _retrieveMessageBody(message)

        return message

    def prepare_out_messages(
            self, messages: Iterable[BaseMessage], context: Dict[str, Any]
    ) -> Iterable[BaseMessage]:
        # If a message was received instead of an iterable of messages, make an iterable of messages
        if not isinstance(messages, Iterable):  # pylint: disable=isinstance-second-argument-not-valid-type
            messages = [messages]

        ingestion_step = context['ingestion_step']

        n_messages = 0
        for message in messages:
            # Remove message which are `None`
            if message is None:
                continue

            n_messages += 1

            # set ingestionId and some relevant information from it to all messages
            message.ingestionId = ingestion_step.ingestionId
            message.artifactName = ingestion_step.artifactName
            message.artifactVersion = ingestion_step.artifactVersion

            # double happiness if data asset 囍
            if message.objectType == ObjectType.DATA_ASSET:
                message.body.ingestionId = ingestion_step.ingestionId
                for row in message.body.data:
                    row['ingestion_id'] = ingestion_step.ingestionId

            # Store message in message store if appropriate
            message = _storeMessageBody(message, self.config)

            yield message

        logging.info({'n_out_messages': n_messages})
        if not n_messages:
            logging.warning('Component produced no output messages.')

    def teardown(self, context: Dict[str, Any]) -> None:
        ingestion_step = context['ingestion_step']

        duration_seconds = (context['start_datetime'] - context['end_datetime']).total_seconds()
        ingestion_step.updateMetadata({'duration_seconds': duration_seconds})

        logging.info({'INGESTION_STEP': ingestion_step.to_json()})

        logging.tearDownLogging(
            logFormat=self.config['PYLON_LOG_FORMAT'], logLevel=self.config['PYLON_LOG_LEVEL']
        )

    def _get_messages(self) -> Iterable[BaseMessage]:
        return self.input.get_messages(max_messages=self.config['PYLON_MAX_MESSAGES'])  # TODO: Better name for config variable

    def _send_messages(self, messages: Iterable[BaseMessage]):
        try:
            self.output.sendMessages(messages)
        except MessageTooLarge as _ex:
            raise MessageTooLarge(
                'Failed to send message because it is too large. Try using '
                'PYLON_STORE_DESTINATION and PYLON_STORE_MIN_MESSAGE_BYTES to store the body '
                'of large messages before sending them.'
            ) from _ex

    def _get_input(self) -> MessageProducer:
        inp = self.config.get('PYLON_INPUT')

        if inp is None:
            return None
        if inp.startswith('sqs://'):
            return QueueMessageProducerConsumer(inp[6:])
        if inp.startswith('folder://'):
            return FolderMessageProducerConsumer(inp[9:])

        raise NotImplementedError(f'Unsupported input "{inp}"')

    def _get_output(self) -> MessageConsumer:
        output = self.config['PYLON_OUTPUT']

        if output is None:
            return None
        if output.startswith('sns://'):
            return TopicMessageConsumer(output[6:])
        if output.startswith('sqs://'):
            return QueueMessageProducerConsumer(output[6:])
        if output.startswith('folder://'):
            return FolderMessageProducerConsumer(output[9:])

        raise NotImplementedError(f'Unsupported output "{output}"')

    def _get_message_store(self) -> MessageStore:
        return _getStoreFromConfig(self.config)

    @property
    def _has_input(self):
        return self.input is not None

    @property
    def has_output(self):
        return self.output is not None


def _getStoreFromConfig(config: dict) -> MessageStore:
    storeDestination = config.get('PYLON_STORE_DESTINATION')
    if storeDestination is None:
        return None

    if storeDestination.startswith('s3://'):
        return aws.s3.MessageStore(storeDestination)

    raise NotImplementedError(f"Unsupported message store {config['PYLON_STORE_DESTINATION']}")


def _storeMessageBody(message: BaseMessage, config: dict) -> BaseMessage:
    if message.isCheckedIn():
        logging.debug('Message is already checked in, not checking in again.')
        return message

    store = _getStoreFromConfig(config)
    min_body_size_bytes = config.get('PYLON_STORE_MIN_MESSAGE_BYTES')

    if (
        store is not None and
        min_body_size_bytes is not None and
        message.getApproxSize() >= min_body_size_bytes
    ):
        logging.info('Message too large, checking message in.')
        return store.checkInPayload(message)

    return message


def _retrieveMessageBody(message: BaseMessage) -> BaseMessage:
    if not message.isCheckedIn():
        raise ValueError('message is not checked in anywhere???')

    if message.payloadStoreKey.startswith('s3://'):
        message = aws.s3.MessageStore.checkOutPayload(message)
    else:
        # how did they make the message????!?!
        raise NotImplementedError(f"Unsupported message store for {message.payloadStoreKey}")

    # Load message body from json if applicable
    # This usually happens in decode but didn't because the message body only available now
    if message.objectType == ObjectType.DATA_ASSET:
        message.body = DataAsset.fromJSON(message.body)
    if message.objectType == ObjectType.INGESTION_STEP:
        message.body = IngestionStep.fromJSON(message.body)

    return message


def _utcnow():
    return datetime.datetime.now(datetime.timezone.utc)
