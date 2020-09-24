import copy
import dataclasses
import functools
import json
import logging
import os
import typing

from . import aws

# To avoid importing logging in case of circular logic
logger = logging.getLogger()


@dataclasses.dataclass
class _ConfigVariableDefinition:
    name: str
    python_type: type
    default: typing.Any = None


CONFIG_ENV_VAR_NAME = 'PYLON_CONFIG'
CONFIG_VARS = [
    _ConfigVariableDefinition('PYLON_INPUT', str),
    _ConfigVariableDefinition('PYLON_OUTPUT', str),
    _ConfigVariableDefinition('PYLON_LOG_LEVEL', str, 'warning'),
    _ConfigVariableDefinition('PYLON_LOG_FORMAT', str, 'txt'),
    _ConfigVariableDefinition('PYLON_LOOP_SLEEP_SECONDS', int, 60),
    _ConfigVariableDefinition('PYLON_STORE_MIN_MESSAGE_BYTES', int, 250 * 1024),
    _ConfigVariableDefinition('PYLON_STORE_DESTINATION', str),
]


def getConfig() -> dict:
    """
    Retrieves configuration from an environment variable.

    If the value of the environment variable is a string pointing to an SSM
    parameter following the format "ssm://path/to/variable", then the value of
    the SSM parameter will be retrieved, and will then be treated as the value
    of the environment variable.

    Returns:
        dict - keys are config variables, values are the corresponding values for those variables.
    """
    value = os.environ[CONFIG_ENV_VAR_NAME]

    if not value.startswith('{'):
        # value doesn't look like a JSON string, try fetch it from somewhere
        value = _getConfigString(value)

    config = json.loads(value)
    config = _addDefaults(config)
    config = _enforceTypes(config)
    config = _checkDeprecation(config)
    _checkUnrecognisedVars(config)

    return config


def _getConfigString(config_path: str) -> typing.Union[str, bytes]:
    if config_path.startswith('ssm://'):
        key = config_path[5:]  # should we trim more characters?
        return aws.ssm.ParameterStore.get(key)

    if config_path.startswith('s3://'):
        contents, _ = aws.s3.getObject(config_path)
        return contents

    if config_path.startswith('file://'):
        path = config_path[7:]  # should we trim less characters?
        return _readFile(path)

    raise ValueError(f'"{config_path}" not recognised as a valid path to a config.')


def _addDefaults(config: dict) -> dict:
    defaults = _getDefaultConfig()
    defaults.update(config)
    return defaults


#@functools.lru_cache(maxsize=1)
def _getDefaultConfig() -> dict:
    """Contains default values for all pylon config options.

    Returns:
        dict: The default config.
    """
    return {
        config_var.name: config_var.default
        for config_var in CONFIG_VARS
        if config_var.default is not None
    }


def _enforceTypes(config: dict) -> dict:
    config = copy.deepcopy(config)
    for config_var in CONFIG_VARS:
        if config_var.name in config:
            config[config_var.name] = config_var.python_type(config[config_var.name])
    return config


def _checkDeprecation(config: dict) -> dict:
    config = copy.deepcopy(config)
    if 'INPUT_QUEUE_NAME' in config:
        logger.warning(
            '"INPUT_QUEUE_NAME" is deprecated, use "PYLON_INPUT" and prefix the value with "sqs://"'
            ' instead'
        )
        config['PYLON_INPUT'] = 'sqs://' + config['INPUT_QUEUE_NAME']

    if 'OUTPUT_TOPIC_ARN' in config:
        logger.warning(
            '"OUTPUT_TOPIC_ARN" is deprecated, use "PYLON_OUTPUT" and prefix the value with '
            '"sns://" instead'
        )
        config['PYLON_OUTPUT'] = 'sns://' + config['OUTPUT_TOPIC_ARN']

    if 'PYLON_PLAIN_LOGGING' in config:
        logger.warning(
            '"PYLON_PLAIN_LOGGING" is deprecated, use "PYLON_LOG_FORMAT" with the value either '
            '"txt" or "json" instead'
        )
        config['PYLON_LOG_FORMAT'] = 'txt' if config['PYLON_PLAIN_LOGGING'] else 'json'

    return config


def _checkUnrecognisedVars(config: dict):
    pylon_config_vars = {
        config_var.name
        for config_var in CONFIG_VARS
    }
    for config_var in config.keys():
        if config_var.startswith('PYLON') and config_var not in pylon_config_vars:
            logger.warning('Using unrecognised config variable %s', config_var)


def _readFile(path: str) -> bytes:
    with open(path, 'r') as fd:
        contents = fd.read()
    return contents
