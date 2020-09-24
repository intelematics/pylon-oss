import boto3
import botocore
import pytest

from pylon.aws import glue

GLUE_DATABASE = {
    'test_db': {
        'test_table': {
            'partitionKeys': ['first', 'third'],
            'partitions': {
                (1, 1): 's3://data-asset-bucket/first=1/third=2/',
                (1, 2): 's3://data-asset-bucket/first=1/third=1/',
                (1, 3): 's3://data-asset-bucket/first=1/third=3/',
                (2, 1): 's3://data-asset-bucket/first=2/third=1/',
                (2, 3): 's3://data-asset-bucket/first=2/third=3/',
                (3, 1): 's3://data-asset-bucket/first=3/third=1/',
            }
        }
    }
}

class MockGlueClient:
    class exceptions:
        class AlreadyExistsException(Exception):
            pass
        class EntityNotFoundException(Exception):
            pass
        class ClientError(Exception):
            pass

    def __init__(self, databases):
        self.databases = databases
        self.badMode = False

    def get_table(self, **kwargs):
        database = kwargs['DatabaseName']
        table = kwargs['Name']
        try:
            partitionKeys = self.databases[database][table]['partitionKeys']
        except KeyError:
            raise self.exceptions.EntityNotFoundException()

        return {
            'Table': {
                'StorageDescriptor': {},
                'PartitionKeys': [{'Name': key} for key in partitionKeys]
            }
        }

    def get_partition(self, **kwargs):
        database = kwargs['DatabaseName']
        table = kwargs['TableName']
        partitionValues = tuple(kwargs['PartitionValues'])

        try:
            location = self.databases[database][table]['partitions'][partitionValues]
        except KeyError:
            raise self.exceptions.EntityNotFoundException()

        return {
            'Partition': {
                'StorageDescriptor': {'Location': location},
                'Values': list(partitionValues)
            }
        }

    def create_partition(self, **kwargs):
        if self.badMode:
            raise self.exceptions.ClientError()

        database = kwargs['DatabaseName']
        table = kwargs['TableName']
        partitionInput = kwargs['PartitionInput']

        partitionValues = tuple(partitionInput['Values'])
        location = partitionInput['StorageDescriptor']['Location']

        if partitionValues in self.databases[database][table]['partitions']:
            raise self.exceptions.AlreadyExistsException()

        self.databases[database][table]['partitions'][partitionValues] = location

        return dict()

    def update_partition(self, **kwargs):
        if self.badMode:
            raise self.exceptions.ClientError()

        database = kwargs['DatabaseName']
        table = kwargs['TableName']
        partitionValues = kwargs['PartitionValueList']
        partitionInput = kwargs['PartitionInput']

        partitionValues = tuple(partitionInput['Values'])
        location = partitionInput['StorageDescriptor']['Location']

        if partitionValues not in self.databases[database][table]['partitions']:
            raise self.exceptions.EntityNotFoundException()

        self.databases[database][table]['partitions'][partitionValues] = location

        return dict()


MOCK_GLUE_CLIENT = MockGlueClient(GLUE_DATABASE)


@pytest.fixture()
def mock_boto3_client(monkeypatch):
    clients = {'glue': MOCK_GLUE_CLIENT}
    try:
        monkeypatch.setattr(boto3, 'client', lambda x: clients[x])
    except KeyError:
        raise botocore.exceptions.UnknownServiceError


@pytest.fixture
def mock_boto3_client_bad(mock_boto3_client):
    glue_client = boto3.client('glue')
    glue_client.badMode = True
    yield
    glue_client.badMode = False


@pytest.mark.parametrize(
    ('databaseName', 'tableName', 's3Location', 'partition', 'exp'),
    [
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=3/third=2/',
            {'first': 3, 'third': 2}, [3, 2]
        ),
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=3/third=3/',
            {'first': 3, 'third': 3}, [3, 3]
        ),
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=3/third=2/',
            {'third': 2, 'first': 3}, [3, 2]
        ),
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=1/third=2/',
            {'first': 1, 'third': 2}, [1, 2]
        ),
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=1/third=1/',
            {'first': 1, 'third': 1}, [1, 1]
        ),
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=1/third=2/',
            {'third': 2, 'first': 1}, [1, 2]
        ),
    ]
)
def test_upsertPartition(mock_boto3_client, databaseName, tableName, s3Location, partition, exp):
    glue.upsertPartition(databaseName, tableName, s3Location, partition)

    glueClient = boto3.client('glue')
    outPartition = glueClient.get_partition(
        DatabaseName=databaseName,
        TableName=tableName,
        PartitionValues=exp
    )['Partition']
    assert outPartition['Values'] == exp
    assert outPartition['StorageDescriptor']['Location'] == s3Location


@pytest.mark.parametrize(
    ('databaseName', 'tableName', 's3Location', 'partition'),
    [
        (
            'test_db', 'test_table', 's3://data-asset-bucket/first=3/third=2/',
            {'first': 3, 'third': 2}
        ),
    ]
)
def test_upsertPartition_raises(mock_boto3_client_bad, databaseName, tableName, s3Location, partition):
    with pytest.raises(Exception):
        glue.upsertPartition(databaseName, tableName, s3Location, partition)
