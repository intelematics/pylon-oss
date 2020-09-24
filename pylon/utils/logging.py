"""
Creates a logging object to be used within pylon.

In main function import ROOT_LOGGER as show below

`from pylon import logging`

Can configure logging to print in JSON by setting the PYLON_CONFIG variable
'PYLON_LOG_FORMAT' to "json". This is default behaviour.

As soon as an ingestionId is generated a filter will be added to the logger
to inject the value into all log outputs.

Additionally, if using the JSON logging format additional JSON fields can be
added/used like so:

`logging.info({"special": "value", "run": 12})`

this is nice.

"""

import logging
import sys
import json

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}


class Filter(logging.Filter):
    """
    This is a filter which injects the ingestion ID into the log.
    """
    def __init__(self, ingestionId):
        self.ingestionId = ingestionId or '-'

    def filter(self, record):
        record.ingestionId = self.ingestionId
        return True


def tearDownLogging(logFormat='txt', logLevel='warning'):
    """
    Tear down logging (strip ingestionId)
    """
    updateLogger(ingestionId=None, logFormat=logFormat, logLevel=logLevel)


def updateLogger(ingestionId=None, logFormat='txt', logLevel='warning'):
    """
    Update the logger with the ingestion id (or strip ingestion id if it is `None`)
    """
    isJSON = (logFormat == 'json')
    hasIngestionId = ingestionId is not None

    formatter = getFormatter(isJSON=isJSON, hasIngestionId=hasIngestionId)
    filter = Filter(ingestionId)
    updateRootLogger(formatter, filter, logLevel=logLevel)


class JsonFormatter(logging.Formatter):
    _default_fields = ['levelname', 'asctime', 'name', 'module', 'funcName']

    def __init__(self, fields=None):
        """
        Constructs a json-ready format string
        e.g. JsonFormatter(['levelname', 'asctime', 'name']) creates a simple
        Formatter with the format string (ignore spaces):
        {
            "levelname": "%(levelname)s",
            "asctime": "%(asctime)s",
            "funcName": "%(funcName)s",
            %(message)
        }
        """
        if fields is None:
            fields = self._default_fields

        fmt = json.dumps({
            field: f'%({field})s'
            for field in fields
        })
        fmt = fmt[:-1] + ', %(message)s' + fmt[-1]

        super().__init__(fmt)


    def format(self, record):
        """Formats a log record, adds special handling for records which are dict"""
        if not isinstance(record.msg, dict):
            record.msg = {'message': record.msg}
        # record.msg = {'foo': 'bar', 'boo': 'baz'}

        record.msg = json.dumps(record.msg, default=str)
        # record.msg = '{"foo": "bar", "boo": "baz"}'

        record.msg = record.msg[1:-1] # strip the first '{' and last '}'
        # record.msg = '"foo": "bar", "boo": "baz"'

        # record.msg is always a str
        return super().format(record)


def getFormatter(isJSON=False, hasIngestionId=False):
    """
    Get the formatter (either JSON or plain)
    """
    components = ['levelname', 'asctime', 'name', 'module', 'funcName']
    if hasIngestionId:
        components.append('ingestionId')

    if isJSON:
        # do something
        return JsonFormatter(components)

    else:
        return logging.Formatter(
            ' '.join(f'[%({elem})s]' for elem in components) + ' %(message)s'
        )


def updateRootLogger(formatter, filter, logLevel):
    """
    Set logger level, handler and formatter
    """
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVELS.get(logLevel, logging.WARNING))
    logger.propagate = False

    if not logger.handlers:
        logHandler = logging.StreamHandler(sys.stdout)
        logger.addHandler(logHandler)

    handler = logger.handlers[0]

    handler.setFormatter(formatter)
    # reset filters with the supplied filter
    handler.filters = [filter]


updateLogger()
ROOT_LOGGER = logging.getLogger('pylon')

debug = ROOT_LOGGER.debug
info = ROOT_LOGGER.info
warn = ROOT_LOGGER.warn
warning = ROOT_LOGGER.warning
error = ROOT_LOGGER.error
exception = ROOT_LOGGER.exception
fatal = ROOT_LOGGER.fatal
critical = ROOT_LOGGER.critical
log = ROOT_LOGGER.log
