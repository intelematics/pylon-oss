import pytest

import boto3

from pylon.aws import s3


def test_delete_small(testBucket, caplog):
    keys = list([str(i) for i in range(100)])

    testBucket.delete(keys)

    assert 'Deleting 100 objects from S3' in caplog.text

    endpoint = boto3.client('s3').delete_objects
    assert endpoint.call_count == 1

    args, kwargs = endpoint.call_args_list[0]

    assert kwargs['Delete'] == {
        'Objects': [
            {'Key': str(i)}
            for i in range(100)
        ]
    }


def test_delete_large(testBucket, caplog):
    keys = (str(i) for i in range(1200)) # this is a generator

    testBucket.delete(keys)

    assert 'Deleting 1000 objects from S3' in caplog.text
    assert 'Deleting 200 objects from S3' in caplog.text

    endpoint = boto3.client('s3').delete_objects
    assert endpoint.call_count == 2

    args, kwargs = endpoint.call_args_list[0]

    assert kwargs['Delete'] == {
        'Objects': [
            {'Key': str(i)}
            for i in range(1000)
        ]
    }

    args, kwargs = endpoint.call_args_list[1]

    assert kwargs['Delete'] == {
        'Objects': [
            {'Key': str(i)}
            for i in range(1000, 1200)
        ]
    }





