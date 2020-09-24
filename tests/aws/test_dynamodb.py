import pytest


@pytest.fixture
def mockData(testDynamoDBTable, monkeypatch):
    data = [
        {'key': 'foo', 'bar': 'baz'},
        {'key': 'bar', 'bar': 'bap'}
    ]

    def get_item(Key: dict, ConsistentRead=True):
        for row in data:
            if all(k in row and row[k] == v for k, v in Key.items()):
                return {'Item': row}

        # dynamodb returns empty response if there is no matching keys
        return {}

    def put_item(Item: dict):
        updated = False
        for row in data:
            if row['key'] == Item['key']:
                row.update(Item)
                updated = True

        if not updated:
            data.append(Item)

    def delete_item(Key: dict):
        for row in data:
            if all(k in row and row[k] == v for k, v in Key.items()):
                data.remove(row)
                return
        

    def scan(ExclusiveStartKey=0):
        LastEvaluatedKey = ExclusiveStartKey + 1

        if ExclusiveStartKey < len(data):
            return {
                'Items': data[ExclusiveStartKey:LastEvaluatedKey],
                'LastEvaluatedKey': LastEvaluatedKey
            }

        return {
            'Items': []
        }

    monkeypatch.setattr(testDynamoDBTable.table, 'get_item', get_item)
    monkeypatch.setattr(testDynamoDBTable.table, 'put_item', put_item)
    monkeypatch.setattr(testDynamoDBTable.table, 'delete_item', delete_item)
    monkeypatch.setattr(testDynamoDBTable.table, 'scan', scan)

    yield data


def test_Table(testDynamoDBTable, mockData):

    assert str(testDynamoDBTable) == '<pylon.aws.dynamodb.Table PylonTableBestTable>'
    assert testDynamoDBTable.get({'key': 'foo'}) == {'key': 'foo', 'bar': 'baz'}
    with pytest.raises(KeyError):
        testDynamoDBTable.get({'key': 'this is not the key youre looking for'})

    testDynamoDBTable.put({'key': 'bar', 'bar': 'wow'})
    assert testDynamoDBTable.get({'key': 'bar'}) == {'key': 'bar', 'bar': 'wow'}

    testDynamoDBTable.delete({'key': 'bar'})
    with pytest.raises(KeyError):
        testDynamoDBTable.get({'key': 'bar'})

def test_Full_Scan(testDynamoDBTable, mockData):
    assert len(testDynamoDBTable.fullScan()) == len(mockData)
