import pytest
import json

from pylon.models import data


@pytest.fixture
def sampleDataAsset():
    d = data.DataAsset()
    d.dataAssetName = 'yeet'
    d.dataAssetCountry = 'yeetopia'
    d.dataAssetVersion = '1'
    d.dataAssetPartitionKeys = ['yeetYear', 'yeetType']
    d.dataAssetUniqueKeys = ['yeetID', 'yeetVersion']
    d.data = [
        {'yeetID': 'yeet1', 'yeetVersion': 0, 'yeetYear': 2019, 'yeetType': 'yeet'},
        {'yeetID': 'yeet1', 'yeetVersion': 1, 'yeetYear': 2019, 'yeetType': 'yought'},
        {'yeetID': 'yeet2', 'yeetVersion': 0, 'yeetYear': 2020, 'yeetType': 'yeeting'}
    ]
    d.ingestionId = 'yeet-yeeeet-yeet-yeeet'

    yield d


def test_toJSON(sampleDataAsset):
    out = sampleDataAsset.toJSON()

    assert isinstance(out, str)

    out_dict = json.loads(out)

    expected_keys = [
        'dataAssetName',
        'dataAssetCountry',
        'dataAssetVersion',
        'dataAssetPartitionKeys',
        'dataAssetUniqueKeys',
        'data',
        'ingestionId'
    ]

    assert set(out_dict.keys()) == set(expected_keys)

    for key in expected_keys:
        assert out_dict[key] == getattr(sampleDataAsset, key)


def test___repr__(sampleDataAsset):
    out = sampleDataAsset.toJSON()
    exp = json.dumps({
        'data': [
            {'yeetID': 'yeet1', 'yeetVersion': 0, 'yeetYear': 2019, 'yeetType': 'yeet'},
            {'yeetID': 'yeet1', 'yeetVersion': 1, 'yeetYear': 2019, 'yeetType': 'yought'},
            {'yeetID': 'yeet2', 'yeetVersion': 0, 'yeetYear': 2020, 'yeetType': 'yeeting'}
        ],
        'dataAssetCountry': 'yeetopia',
        'dataAssetName': 'yeet',
        'dataAssetPartitionKeys': ['yeetYear', 'yeetType'],
        'dataAssetUniqueKeys': ['yeetID', 'yeetVersion'],
        'dataAssetVersion': '1',
        'ingestionId': 'yeet-yeeeet-yeet-yeeet'
    })
    assert out == exp
