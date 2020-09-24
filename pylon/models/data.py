import typing
import json

from ..interfaces.serializing import JsonSerializable
from ..utils import logging

class DataAsset(JsonSerializable):
    """
    Usage:

    ```
    # from a dataframe
    my_data = pd.DataFrame({
        'foo': [1, 2, 3, 4],
        'bar': [2, 3, 4, 5],
        'baz': [3, 4, 5, 6]
    })
    my_data_asset = DataAsset.from_dataframe(
        my_data,
        name='foobar',
        version=1,
        country='AU',
        partition_keys=['foo'],
        unique_keys=['bar']
    )
    my_msg = pylon.models.messages.DataAssetMessage(my_data_asset)

    # or even simpler
    my_msg = pylon.models.messages.DataAssetMessage.from_dataframe(
        my_data,
        name='foobar',
        version=1,
        country='AU',
        partition_keys=['foo'],
        unique_keys=['bar']
    )

    # or craft it by hand...
    my_data_asset = DataAsset()
    # need to set properties defined in .__init__()
    ```
    """

    def __init__(self):
        self.dataAssetName          = ''
        self.dataAssetCountry       = ''
        self.dataAssetVersion       = ''
        # Ordered list of data asset fields by which the data is partitioned
        self.dataAssetPartitionKeys = []
        # Unordered list of data asset fields used for deduplication
        # Two data asset records are considered identical if their values for each of these keys match
        self.dataAssetUniqueKeys    = []
        # List of dictionaries mapping the data asset fields to their values
        self.data                   = []
        # The id of the ingestion step that produced this record
        self.ingestionId            = None

    @classmethod
    def from_dataframe(
        cls,
        df,
        name: str,
        version: str,
        country: str,
        partition_keys: typing.List[str],
        unique_keys: typing.List[str],
        quiet: bool=False
    ):
        this = cls()
        this.dataAssetName = name
        this.dataAssetVersion = version
        this.dataAssetCountry = country
        this.dataAssetPartitionKeys = partition_keys
        this.dataAssetUniqueKeys = unique_keys

        if not quiet:
            logging.info(f"Creating {cls.__name__} message with {len(df)} records")

        this.data = (
            df
            .sort_index(axis=1)     # impose sorting of dataframe columns
            .to_dict('records')
        )

        return this

