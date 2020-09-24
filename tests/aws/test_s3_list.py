import datetime

import pytest

import boto3

from pylon.aws import s3


@pytest.fixture
def testBucketList(testBucket, monkeypatch):

    s3_client = boto3.client('s3')

    _objects = [
        'foo.png',
        'foo/bar.txt',
        'foo/baz/boo.txt'
    ]

    def mock_list_objects_v2(Bucket, Prefix='', Delimiter=None):
        """
        A very simple mock implementation of AWS's list_objects_v2:
        - discard objects that don't match a prefix
        - if there is a delimiter, look for the delimiter in the matched object
            - chop it at the first instance of the delimiter
            - return unique
        - otherwise return the object
        """
        out = {
            'Contents': [],
            'CommonPrefixes': [],
            'IsTruncated': False
        }
        found_objs = []
        found_prefixes = []
        for obj in _objects:
            if not obj.startswith(Prefix):
                continue

            trimmed = obj[len(Prefix):] # remove the prefix

            if Delimiter is not None and Delimiter in trimmed:
                found_prefixes.append(Prefix + trimmed.split(Delimiter)[0] + Delimiter)
            else:
                found_objs.append(obj)

        out = {
            'IsTruncated': False
        }
        if len(found_objs) > 0:
            out['Contents'] = [
                {
                    'Key': key,
                    'LastModified': datetime.datetime(1970, 1, 1, 0, 0, 0)
                }
                for key in found_objs
            ]
        if len(found_prefixes) > 0:
            out['CommonPrefixes'] = [
                {'Prefix': p}
                for p in set(found_prefixes)
            ]
        return out

    monkeypatch.setattr(s3_client, 'list_objects_v2', mock_list_objects_v2)

    yield testBucket


@pytest.mark.parametrize(
    ('prefix', 'expected'),
    [
        (None, ['foo.png', 'foo/bar.txt', 'foo/baz/boo.txt']),
        ('foo', ['foo.png', 'foo/bar.txt', 'foo/baz/boo.txt']),
        ('foo/', ['foo/bar.txt', 'foo/baz/boo.txt']),
        ('does_not_exist', [])
    ]
)
def test_bucket_list_default(testBucketList, prefix, expected):

    out = testBucketList.list(prefix=prefix)
    out = list(out)

    keys = [o['Key'] for o in out]

    assert sorted(keys) == sorted(expected)


@pytest.mark.parametrize(
    ('prefix', 'expected_keys', 'expected_prefixes'),
    [
        (None, ['foo.png'], ['foo/']),
        ('foo', ['foo/bar.txt'], ['foo/baz/']),
        ('foo/', ['foo/bar.txt'], ['foo/baz/']),
        ('does_not_exist', [], [])
    ]
)
def test_bucket_list_no_recurse(testBucketList, prefix, expected_keys, expected_prefixes):

    out = testBucketList.list(prefix=prefix, recursive=False)
    out = list(out)

    keys = [o['Key'] for o in out if 'Key' in o]
    prefixes = [o['Prefix'] for o in out if 'Prefix' in o]

    assert sorted(keys) == sorted(expected_keys)
    assert sorted(prefixes) == sorted(expected_prefixes)
