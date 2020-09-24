"""
A collection of functions for working with AWS Glue
"""
import functools
import itertools
import typing
import json

import boto3
from botocore.exceptions import ClientError

from ..utils import logging


@functools.lru_cache(maxsize=1)
def getGlueClient():
    return boto3.client('glue')


@functools.lru_cache(maxsize=16)
def getGlueTableInfo(databaseName: str, tableName: str) -> dict:
    glueClient = getGlueClient()
    return glueClient.get_table(DatabaseName=databaseName, Name=tableName)


def permutePartitions(**partitionPermutations: typing.List[str]):
    """
    Permutes through all possible combinations for partitions.

    Args:
        key-value pairs, where keys are the partition keys, and
        values are lists of possible partition values

    Yields:
        dict of partition keys and partition values

    Example:
    ```
    for p in permutePartitions(foo=['a', 'b'], bar=['c', 'd']):
        print(p)
    # {'foo': 'a', 'bar': 'c'}
    # {'foo': 'a', 'bar': 'd'}
    # {'foo': 'b', 'bar': 'c'}
    # {'foo': 'b', 'bar': 'd'}
    """
    keys = list(partitionPermutations.keys())
    value_lists = list(partitionPermutations.values())

    for values in itertools.product(*value_lists):
        yield {
            k: v
            for k, v in zip(keys, values)
        }


def getPartitionInfo(databaseName: str, tableName: str, s3Prefix: str, partition: dict) -> dict:
    partition_path_elem = [
        f'{k}={v}'
        for k, v in partition.items()
    ]

    s3_location = s3Prefix.rstrip('/') + '/' + '/'.join(partition_path_elem)

    return {
        'databaseName': databaseName,
        'tableName': tableName,
        's3Location': s3_location,
        'partition': partition
    }



def upsertPartition(databaseName: str, tableName: str, s3Location: str, partition: dict):
    """Create a partition in glue, or if one already exists update it.

    Args:
        databaseName (str): The name of the database in Glue in which the table lives.
        tableName (str): The name of the table to add the partition to.
        s3Location (str): The s3 path prefix to the data within this partition. e.g.
            s3://data-asset-bucket/key1=val1/key2=val2/
        partition (dict): A dictionary containing the partition keys for this table and the values
            for them in this partition. e.g. `{"key1": "val1", "key2": "val2"}`.
    """
    glueClient = getGlueClient()
    tableInfo = getGlueTableInfo(databaseName, tableName)

    storageDescriptor = tableInfo['Table']['StorageDescriptor']
    storageDescriptor['Location'] = s3Location
    partitionValues = [
        partition[partitionKey["Name"]]
        for partitionKey
        in tableInfo['Table']['PartitionKeys']
    ]

    createdPartition = False
    updatedPartition = False
    try:
        glueClient.create_partition(
            DatabaseName=databaseName,
            TableName=tableName,
            PartitionInput={
                'Values': partitionValues,
                'StorageDescriptor': storageDescriptor
            }
        )
        createdPartition = True
    except glueClient.exceptions.AlreadyExistsException as e:
        glueClient.update_partition(
            DatabaseName=databaseName,
            TableName=tableName,
            PartitionValueList=partitionValues,
            PartitionInput={
                'Values': partitionValues,
                'StorageDescriptor': storageDescriptor
            }
        )
        updatedPartition = True
    finally:
        logging.debug({
            "createdPartition": createdPartition,
            "updatedPartition": updatedPartition,
            "partition": partition
        })
