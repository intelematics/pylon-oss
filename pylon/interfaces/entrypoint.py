import abc
import typing

from ..config import getConfig
from ..utils import timed, GracefulLooper
from ..utils import logging

class Entrypoint(abc.ABC):

    def __init__(self, coreFunction: typing.Callable):
        self.coreFunction = timed('core_process', coreFunction)
        self.config = getConfig()

        # Setup logging type with blank ingestionId
        logging.updateLogger(
            logFormat=self.config['PYLON_LOG_FORMAT'], logLevel=self.config['PYLON_LOG_LEVEL']
        )

        self.makeAdapters()

    @abc.abstractclassmethod
    def makeAdapters(self):
        raise NotImplementedError

    @abc.abstractmethod
    def runOnce(self):
        """
        Runs the component execution step once
        """
        raise NotImplementedError

    def runForever(self):
        """
        Runs the component execution step forever
        """
        logging.info('heartbeat: run_forever')
        sleep = self.config.get('PYLON_LOOP_SLEEP_SECONDS', 0)
        looper = GracefulLooper(sleep=sleep)
        looper.runForever(self.runOnce)

    @abc.abstractmethod
    def lambda_handler(self, event, context):
        raise NotImplementedError
