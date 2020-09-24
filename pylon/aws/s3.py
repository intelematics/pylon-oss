import copy
import typing
import functools
import uuid

import boto3

from ._bases import BaseMixin
from .. import interfaces
from .. import models
from ..utils import logging


_STR_OR_BYTES = typing.Union[str, bytes]


def getObject(s3Path, encoding=None):
    """
    Get an object with a fully qualified s3 path
    """
    bucketName, key = splitPath(s3Path)
    bucket = Bucket(bucketName)
    return bucket.get(key, encoding=encoding)


def putObject(
    s3Path, content, encoding=None, storageClass: str='INTELLIGENT_TIERING',
    metadata: dict=None, kmsKeyID: str=None
) -> str:
    bucketName, key = splitPath(s3Path)
    bucket = Bucket(bucketName)
    return bucket.put(key, content, encoding=encoding, storageClass=storageClass,
                        metadata=metadata, kmsKeyID=kmsKeyID)


def getPath(bucketName, key):
    """
    Args:
        bucketName: the name of the s3 bucket
        key: S3 object key within a bucket, i.e. the path

    Returns:
        The path of the s3 object
    """
    return f's3://{bucketName}/{key}'


def splitPath(s3Path: str) -> typing.Tuple[str, str]:
    """Split a full S3 path into its bucket and key components"""
    if not s3Path.startswith('s3://'):
        raise ValueError('s3Path must begin with "s3://"')
    s3Path = s3Path[5:]

    bucketName, key = s3Path.split('/', 1)
    return bucketName, key

def copy_file_in_s3(sourcePath: str, destinationPath: str, delete: bool=False) -> None:
    """
    Copy a file within S3 from one location to another

    Args:
        sourcePath: full S3 path for source file
        destinationPath: full S3 path for source file
        delete: Whether or not to delete the source file
    """
    s3 = boto3.resource('s3')
    sourceBucket, sourceKey = splitPath(sourcePath)
    destinationBucket, destinationKey = splitPath(destinationPath)

    source = {
        'Bucket': sourceBucket,
        'Key': sourceKey
    }

    logging.debug(f'Copying {sourcePath} to {destinationPath}')
    destination = s3.Bucket(destinationBucket).Object(destinationKey)
    destination.copy(source)

    logging.debug(f'Copy complete.')
    if delete:
        logging.debug(f'Deleting file {sourcePath}')
        bucketSource = Bucket(sourceBucket)
        bucketSource.delete(sourceKey)

@functools.lru_cache(maxsize=16)
class Bucket(BaseMixin):
    def __init__(self, bucketName: str):
        super().__init__(bucketName)
        self.bucket = boto3.resource('s3').Bucket(bucketName)

    def put(
        self, key: str, content: _STR_OR_BYTES, encoding: str=None,
        storageClass: str='INTELLIGENT_TIERING', metadata: dict=None,
        kmsKeyID:str = None
    ) -> str:
        """
        Saves a file to an S3 destination.

        Args:
            key: S3 object key within a bucket, i.e. the path
            content: content to store
            encoding: if the content is string, provide an
                encoding to convert the string content to bytes, e.g. 'utf-8'
            storageClass: The storage class to put the object in. See boto3 docs for valid values.
            metadata: simple key-value pairs, cannot include
                nested lists/dicts. All non-string values are converted to
                strings.
            kmsKeyID: a KMS key ID (UUID-style) or alias ("alias/<name>").
                If not supplied, the bucket defaults for server-side encryption is used.
                Examples:
                    >>> put(...) # uses bucket default KMS settings
                    >>> put(..., kmsKeyID="alias/aws/s3") # uses SSE:S3 with an account-default KMS key
                    >>> put(..., kmsKeyID="alias/sharing") # uses SSE:CMK with the sharing key
        """
        logging.debug(f'Putting S3 object {key}')
        # encode content into bytes if required
        if encoding is not None:
            content = content.encode(encoding)

        if metadata is None:
            metadata = {}
        # S3 metadata is always string, convert everything!!
        metadata = {str(k): str(v) for k, v in metadata.items()}

        kwargs = {}
        if kmsKeyID is not None:
            kwargs['ServerSideEncryption'] = 'aws:kms'
            kwargs['SSEKMSKeyId'] = kmsKeyID

        logging.info(f'Writing {len(content):,} bytes to {key}')
        logging.debug(f'Metadata: {metadata}')

        self.bucket.put_object(
            Body=content,
            Key=key,
            Metadata=metadata,
            StorageClass=storageClass,
            **kwargs
        )

        return getPath(self.bucket.name, key)

    def get(self, key, encoding=None) -> typing.Tuple[_STR_OR_BYTES, dict]:
        """Retrieves a file from S3, returns the content and metadata"""
        logging.debug(f'Retrieving S3 object {key}')

        response = self.bucket.Object(key).get()
        content = response['Body'].read()
        metadata = response['Metadata']

        logging.info(f'Read {len(content):,} bytes from {key}')
        logging.debug(f'Metadata: {metadata}')

        if encoding is not None:
            content = content.decode(encoding)

        return content, metadata

    def getStreamingBody(self, key):
        return self.bucket.Object(key).get()['Body']

    def get_signed_url(self, key, expirySeconds=86_400):
        """Retrieves a presigned url for an object, by default expires in 1 day"""
        url = boto3.client('s3').generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket.name,
                'Key': key
            },
            ExpiresIn=expirySeconds
        )
        return url

    def list(self, prefix: str=None, recursive: bool=True) -> typing.Iterable[dict]:
        """
        Retrieves a list of all files in a prefix.

        Args:
            prefix (str): The prefix to list objects from, defaults to the root of the bucket
            recursive (bool): If True, lists all objects within the prefix. If False, behaves like
                a filesystem, and lists objects and folders within the prefix. Defaults to True

        Returns:
            a generator that yields dictionaries

        Yields:
            for objects, yields dictionaries with the following fields:
            ```
            {
              "Key": "foo/bar.txt",
              "LastModified": datetime.datetime(...),
              "ETag": "\"<hash>\"",
              "Size": 100,
              "StorageClass": "INTELLIGENT_TIERING",
              "Owner": {
                "DisplayName": "aws-data-and-content-prod",
                "ID": "<aws_canonical_id>"
              }
            }
            ```

            for prefixes (called with `recursive=False`), yields dictionaries with the following fields:
            ```
            {
              "Prefix": "foo/bar/"
            }
            ```

        Examples:
        ```
        >>> bucket = Bucket('my-bucket')
        >>> bucket.list()
        <generator object>
        >>> for o in bucket.list():
        ...     print(o['Key'])
        foo.png
        foo/bar.txt
        foo/baz/boo.txt
        >>> for elem in bucket.list(recursive=False):
        ...     if 'Key' in elem:
        ...         print('object: {elem["Key"]}')
        ...     else:
        ...         print('prefix: {elem["Prefix"]}')
        object: foo.png
        prefix: foo/
        >>> for elem in bucket.list(prefix="foo", recursive=False):
        ...     if 'Key' in elem:
        ...         print('object: {elem["Key"]}')
        ...     else:
        ...         print('prefix: {elem["Prefix"]}')
        object: foo/bar.txt
        prefix: foo/baz/
        ```
        """
        logging.info(f'Retrieving directory listing for {prefix}')

        # We must keep track of what arguments we plan to call `list_objects_v2` with.
        # The reason for this is list_objects_v2 refuses to accept `None` as a parameter because
        # for paramters "Prefix" and "ContinutationToken" as it expects strings. If we would like
        # `list_objects_v2` to behave as if we didn't pass in one of the parameters, we cannot
        # pass in `None` but instead must literally not pass in that parameter.
        list_kwargs = {
            'Bucket': self.bucket.name,
        }
        if prefix is not None:
            list_kwargs['Prefix'] = prefix
        if recursive is False:
            # list_objects_v2 implements folder listing by using the 'Delimiter' argument, very intuitive...
            list_kwargs['Delimiter'] = '/'
            # if a prefix is given as 'foo/bar', and 'foo/bar/' is a folder, then list_objects_v2 very
            # unhelpfully returns 'foo/bar/' as the result ('foo/bar/' = 'foo/bar' + delimiter)
            # ensure the prefix ends with '/'
            if prefix is not None and prefix != '' and not prefix.endswith('/'):
                list_kwargs['Prefix'] = prefix + '/'

        while True:
            out = boto3.client('s3').list_objects_v2(**list_kwargs)

            objects = out.pop('Contents', [])
            logging.info(f'fetched {len(objects)} objects...')
            yield from objects

            if recursive is False:
                prefixes = out.pop('CommonPrefixes', [])
                logging.info(f'fetched {len(prefixes)} prefixes...')
                yield from prefixes

            if not out['IsTruncated']:
                break
            list_kwargs['ContinuationToken'] = out['NextContinuationToken']


    def delete(self, keys: typing.Iterable[str]) -> None:
        """Deletes a number of keys from S3"""
        # convenience handler when only 1 object is being deleted
        if isinstance(keys, str):
            keys = [keys]

        # s3 delete_objects takes at most 1000 objects in one api call
        # if delete_objects is called with more than 1000 objects,
        # a MalformedXML error is raised. Very helpful...
        # we will batch the the objects to chunks of 1000 at a time.
        def chunk_keys(keys):
            chunk = []
            for key in keys:
                chunk.append(key)

                if len(chunk) == 1000:
                    yield chunk
                    chunk = []
            # yield the last chunk, which may have less than 1000 keys
            if len(chunk) > 0:
                yield chunk

        def delete_chunk(chunk):
            logging.info('Deleting {n} objects from S3'.format(n=len(chunk)))
            deleteRequest = {
                'Objects': [
                    {'Key': key}
                    for key in chunk
                ]
            }

            response = boto3.client('s3').delete_objects(
                Bucket=self.bucket.name,
                Delete=deleteRequest
            )

            logging.debug(response)

        for chunk in chunk_keys(keys):
            delete_chunk(chunk)


class MessageStore(interfaces.messaging.MessageStore):

    def __init__(self, prefix):
        self.prefix = prefix.rstrip('/')

    def checkInPayload(self, message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Stores the message body to be retrieved later

        Args:
            message (models.messages.BaseMessage): The message to be stored

        Returns:
            models.messages.BaseMessage: The message with the payload pointing to the store
        """
        message = copy.deepcopy(message)

        message.serializeBody()
        key = self._getPath()
        putObject(key, message.body, encoding='utf-8')

        message.payloadStoreKey = key
        message.body = key
        return message

    @staticmethod
    def checkOutPayload(message: models.messages.BaseMessage) -> models.messages.BaseMessage:
        """Retrieves the message body from the message store

        Args:
            message (models.messages.BaseMessage): The message to be loaded

        Returns:
            models.messages.BaseMessage: The loaded message
        """
        message = copy.deepcopy(message)

        message.body, _ = getObject(message.payloadStoreKey)
        try:
            message.body = message.body.decode('utf-8')
        except UnicodeDecodeError:
            pass

        message.deserializeBody()
        message.payloadStoreKey = None
        return message

    def _getPath(self):
        return '/'.join([self.prefix, str(uuid.uuid4())])
