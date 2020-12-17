"""
Defines components.

- PipelineComponent: receive message from a queue, publishes to a topic
- SourceComponent: generates data somehow (e.g. external fetch), publishes to a topic
- SinkComponent: receive messages, do not publish anything
- NullComponent: does not receive or publish anything
"""

import typing
import contextlib
import copy
import itertools
import functools

from . import aws
from .models.messages import BaseMessage, ObjectType, IngestionMessage
from .models.ingestion import IngestionStep
from .models.data import DataAsset
from .interfaces.messaging import MessageProducer, MessageConsumer, NoMessagesAvailable, MessageTooLarge, MessageStore
from .interfaces.entrypoint import Entrypoint
from .io import FolderMessageProducerConsumer, TopicMessageConsumer, QueueMessageProducerConsumer
from .utils import logging
from .utils import timed, catchAllExceptionsToLog


class Component(Entrypoint):
    @timed('total')
    @catchAllExceptionsToLog
    def runOnce(self):
        """
        Process a single message
        """
        logging.info('heartbeat: run_once')
        if self._hasInput:
            try:
                with self.inputMessageProducer.getMessage() as message:
                    self._runOnce(message)
            except NoMessagesAvailable:
                pass
        else:
            self._runOnce(None)

    def lambda_handler(self, event, context):
        logging.info('heartbeat: lambda_handler')
        if self._hasInput:
            self.inputMessageProducer = aws.lambda_.PseudoQueue(event)

            while len(self.inputMessageProducer) > 0:
                self.runOnce(PYLON_ALLOW_EXCEPTIONS=True)
        else:
            self.runOnce(PYLON_ALLOW_EXCEPTIONS=True)

    def _runOnce(self, message: BaseMessage):
        ingestionStep = self._getIngestionStep(message)

        logging.updateLogger(
            ingestionId=ingestionStep.ingestionId, logFormat=self.config['PYLON_LOG_FORMAT'],
            logLevel=self.config['PYLON_LOG_LEVEL']
        )
        logging.info({
            'message': 'heartbeat: core_process',
            'parentIngestionId': str(ingestionStep.parentIngestionId)
        })

        results = self._processInput(message=message)
        if self._hasOutput:
            if results is not None:
                self._processOutput(results, ingestionStep)
            else:
                logging.warning(f'Component did not produce any output')

        # add duration time to ingestionStep
        ingestionStep.updateMetadata({'durationSeconds': self.coreFunction.durationSeconds})
        logging.info(f'INGESTION_STEP: {ingestionStep.toJSON()}')

        logging.tearDownLogging(
            logFormat=self.config['PYLON_LOG_FORMAT'], logLevel=self.config['PYLON_LOG_LEVEL']
        )

    def _processInput(self, message):
        if message is not None and message.isCheckedIn():
            message = _retrieveMessageBody(message)

        results = self.coreFunction(message, self.config)
        return results

    def _processOutput(self, results, ingestionStep):
        if not isinstance(results, typing.Iterable):
            results = [results]

        results = (
            _storeMessageBody(_populateMessageAttributes(msg, ingestionStep), self.config)
            for msg in results
            if msg is not None
        )

        self._sendMessages(results)

    def _getIngestionStep(self, message: BaseMessage):
        ingestionStep = IngestionStep()
        parentIngestionId = message.ingestionId if message is not None else None
        ingestionStep.populate(config=self.config, parentIngestionId=parentIngestionId)
        return ingestionStep

    def _sendMessages(self, messages: typing.Iterable[BaseMessage]):
        if messages is not None:
            messages = (message for message in messages if message is not None)
            try:
                self.outputMessageConsumer.sendMessages(messages)
            except MessageTooLarge:
                raise MessageTooLarge(
                    'Failed to send message because it is too large. Try using '
                    'PYLON_STORE_DESTINATION and PYLON_STORE_MIN_MESSAGE_BYTES to store the body'
                    'of large messages before sending them.'
                )

    def _getInputFromConfig(self) -> MessageProducer:
        inp = self.config['PYLON_INPUT']

        if inp.startswith('sqs://'):
            return QueueMessageProducerConsumer(inp[6:])
        if inp.startswith('folder://'):
            return FolderMessageProducerConsumer(inp[9:])

        raise NotImplementedError(f"Unsupported input {self.config['PYLON_INPUT']}")

    def _getOutputFromConfig(self) -> MessageConsumer:
        output = self.config['PYLON_OUTPUT']

        if output.startswith('sns://'):
            return TopicMessageConsumer(output[6:])
        if output.startswith('sqs://'):
            return QueueMessageProducerConsumer(output[6:])
        if output.startswith('folder://'):
            return FolderMessageProducerConsumer(output[9:])

        raise NotImplementedError(f"Unsupported output {self.config['PYLON_OUTPUT']}")

    @property
    def _hasInput(self):
        try:
            return self.inputMessageProducer is not None
        except AttributeError:
            return False

    @property
    def _hasOutput(self):
        try:
            return self.outputMessageConsumer is not None
        except AttributeError:
            return False


class PipelineComponent(Component):
    """
    A component that receives an input message, and generates one or more output
    messages. This is typically a step in a data pipeline, such as transforming
    data.

    Use this class to decorate a function to provide plumbing to input queues,
    output topics and ingestion boilerplate.

    Usage:

    ```
    @PipelineComponent
    def myWorkflow(
       message: BaseMessage,
       config: dict
    ) -> typing.Union[BaseMessage, typing.Iterable[BaseMessage]]:
       # do something with the message
       # add something interesting to the ingestion step
       # return some results
       pass

    if __name__ == '__main__':
        myWorkflow.runForever()
    ```
    """

    def makeAdapters(self):
        self.inputMessageProducer = self._getInputFromConfig()
        self.outputMessageConsumer = self._getOutputFromConfig()


class SourceComponent(Component):
    """
    A component that generates output messages, but does not consume any input
    messages. This is the "source" of a pipeline, typically a fetcher that
    downloads data from a supplier triggered by an external scheduler. Often used in combination
    with the config variable `PYLON_LOOP_SLEEP_SECONDS` which defines the number of seconds between
    calls to the function decorated by `@SourceComponent`.

    Use this class to decorate a function to provide plumbing to output topics
    and ingestion boilerplate.

    Usage:

    ```
    @SourceComponent
    def myWorkflow(
        message: BaseMessage
        config: dict
    ) -> typing.Union[BaseMessage, typing.Iterable[BaseMessage]]:
        # do something with the message
        # add something interesting to the ingestion step
        # return some results
        pass

    if __name__ == '__main__':
        myWorkflow.runOnce()
    ```
    """
    def makeAdapters(self):
        self.outputMessageConsumer = self._getOutputFromConfig()


class SinkComponent(Component):
    """
    A component that receives an input message, but does not produce any output
    messages.

    Use this class to decorate a function to provide plumbing to input queues.

    Usage:

    ```
    @SinkComponent
    def myWorkflow(
       message: BaseMessage,
       config: dict
    ) -> None:
       # do something with the message
       # add something interesting to the ingestion step
       pass

    if __name__ == '__main__':
        myWorkflow.runForever()
    ```
    """

    def makeAdapters(self):
        self.inputMessageProducer = self._getInputFromConfig()


class NullComponent(Component):
    """
    A component that generates no output messages, and does not consume any input
    messages. This is a weird component useful for running things on a timer in ECS. Often used in
    combination with the config variable `PYLON_LOOP_SLEEP_SECONDS` which defines the number of
    seconds between calls to the function decorated by `@SourceComponent`.

    Usage:

    ```
    @NullComponent
    def myWorkflow(
       message: BaseMessage,
       config: dict,
    ) -> None:
       del message
       # do something interesting
       pass

    if __name__ == '__main__':
        myWorkflow.runForever()
    ```
    """
    pass


def _populateMessageAttributes(
    message: BaseMessage,
    ingestionStep: IngestionStep
):
    message = copy.deepcopy(message)

    # set ingestionId and some relevant information from it to all messages
    message.ingestionId = ingestionStep.ingestionId
    message.artifactName = ingestionStep.artifactName
    message.artifactVersion = ingestionStep.artifactVersion

    # double happiness if data asset
    # 囍
    if message.objectType == ObjectType.DATA_ASSET:
        message.body.ingestionId = ingestionStep.ingestionId
        for row in message.body.data:
            row['ingestion_id'] = ingestionStep.ingestionId


    return message


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
