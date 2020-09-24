import boto3
import typing
import functools

from ._bases import BaseMixin
from ..utils import logging

@functools.lru_cache(maxsize=16)
class Table(BaseMixin):

    def __init__(self, tableName: str):
        super().__init__(tableName)
        self.table = boto3.resource('dynamodb').Table(tableName)

    def get(self, key: dict) -> dict:
        """
        Retrieves an item as a dict from dynamoDB table

        'key' is a mapping of columns and values which uniquely
        identifies a record in the dynamo table.

        Example:
        >>> Table('my-dynamodb-table').get({'Year': 2019, 'Name': 'John'})
        {
            'Year': 2019,
            'Name': 'John',
            'other': 'stuff'
        }
        """
        logging.info(f'Fetching dynamodb key {key}')
        response = self.table.get_item(
            Key=key,
            ConsistentRead=True
        )
        try:
            item = response['Item']
        except KeyError:
            raise KeyError(key)

        return item

    def put(self, item: dict) -> None:
        """Puts an item as a dict to DynamoDB"""
        logging.info(f'Putting dynamodb item {item}')
        self.table.put_item(Item=item)

    def delete(self, key: dict) -> None:
        """Deletes an item as a dict from DynamoDB"""
        logging.info(f'Deleting dynamodb item with key {key}')
        self.table.delete_item(Key=key)

    def fullScan(self) -> typing.Iterable:
        response = self.table.scan()
        items = response.get('Items', [])
        lastEvaluatedKey = response.get('LastEvaluatedKey', None)

        while lastEvaluatedKey is not None:
            response = self.table.scan(ExclusiveStartKey=lastEvaluatedKey)
            items = items + response.get('Items', [])
            lastEvaluatedKey = response.get('LastEvaluatedKey', None)

        return items
