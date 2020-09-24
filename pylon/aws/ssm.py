import boto3
import typing
import functools

from ._bases import BaseMixin
from ..utils import logging


@functools.lru_cache(maxsize=1)
def ssmClient():
    return boto3.client('ssm')

class ParameterStore(BaseMixin):

    def __init__(self):
        super().__init__(name='')

    @classmethod
    def get(cls, key: str) -> bytes:
        logging.debug(f'Fetching SSM Parameter {key}')
        response = ssmClient().get_parameter(Name=key, WithDecryption=True)
        return response['Parameter']['Value']
