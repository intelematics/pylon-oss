import uuid
import typing

from ..utils import getClassAttributes
from .data import DataAsset
from .ingestion import IngestionStep


class ObjectType: # pragma: no cover
    DATA_ASSET          = 'dataAsset'
    INGESTION_STEP      = 'ingestionStep'
    URL_RESOURCE        = 'urlResource'
    UPDATE_NOTIFICATION = 'updateNotification'
    RAW_CONTENT         = 'rawContent'
    NULL                = 'null'
    LAMBDA_EVENT        = 'lambdaEvent'


class MessageAttribute: # pragma: no cover
    OBJECT_TYPE        = 'objectType'
    INGESTION_ID       = 'ingestionId'
    PAYLOAD_MIME_TYPE  = 'payloadMimeType'
    PAYLOAD_STORE_KEY  = 'payloadStoreKey'
    ARTIFACT_NAME      = 'artifactName'
    ARTIFACT_VERSION   = 'artifactVersion'
    CUSTOM_ATTRIBUTES  = 'customAttributes'


class BaseMessage:
    __slots__ = ['body'] + list(getClassAttributes(MessageAttribute).values())

    def __init__(self):
        self.body             = None
        self.payloadMimeType  = None
        self.objectType       = None

        # add your own attributes to the message
        # all values should be of type str
        self.customAttributes = dict()

        # pylon will populate
        self.payloadStoreKey  = None
        self.ingestionId      = None
        self.artifactName     = None
        self.artifactVersion  = None

    def isCheckedIn(self):
        return (self.payloadStoreKey is not None)

    def getBody(self):
        return self.body

    def getApproxSize(self):
        """
        Computes the approximate size of the message. Most messaging queues
        and topics have a size limit. Payloads which are too big should be
        cached to another location first before transmitting.
        """
        size = 0

        for key, value in self.items():
            size += len(str(key))
            size += len(str(value))

        return size

    def serializeBody(self):
        pass

    def deserializeBody(self):
        pass

    def items(self):
        for attr in self.__slots__:
            yield attr, getattr(self, attr)

    def __eq__(self, other):
        return list(self.items()) == list(other.items())

    def __str__(self):
        clsName = self.__class__.__name__
        bodySnippet = str(self.body)[:10]
        if len(str(self.body)) > 10:
            bodySnippet += '...'
        return f'<{clsName} body="{bodySnippet}">'

    def __repr__(self):
        return str(self)


class NullMessage(BaseMessage): # pragma: no cover

    def __init__(self):
        super().__init__()
        self.body               = ''
        self.payloadMimeType    = 'text'
        self.objectType         = ObjectType.NULL

class IngestionMessage(BaseMessage): # pragma: no cover

    def __init__(self, body):
        super().__init__()
        self.body               = body
        self.payloadMimeType    = 'text/json'
        self.objectType         = ObjectType.INGESTION_STEP

    def serializeBody(self):
        self.body = self.body.toJSON()

    def deserializeBody(self):
        self.body = IngestionStep.fromJSON(self.body)

class URLResourceMessage(BaseMessage): # pragma: no cover

    def __init__(self, body):
        super().__init__()
        self.body               = body
        self.payloadMimeType    = 'text/json'
        self.objectType         = ObjectType.URL_RESOURCE

class RawContentMessage(BaseMessage): # pragma: no cover

    def __init__(self, payloadMimeType):
        super().__init__()
        self.body               = ''
        self.payloadMimeType    = payloadMimeType
        self.objectType         = ObjectType.RAW_CONTENT

class UpdateNotificationMessage(BaseMessage): # pragma: no cover

    def __init__(self, body):
        super().__init__()
        self.body               = body
        self.payloadMimeType    = 'text/json'
        self.objectType         = ObjectType.UPDATE_NOTIFICATION

class DataAssetMessage(BaseMessage): # pragma: no cover

    def __init__(self, dataAsset: DataAsset):
        super().__init__()
        self.body               = dataAsset
        self.payloadMimeType    = 'text/json'
        self.objectType         = ObjectType.DATA_ASSET

    def serializeBody(self):
        self.body = self.body.toJSON()

    def deserializeBody(self):
        self.body = DataAsset.fromJSON(self.body)

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
        dataAsset = DataAsset.from_dataframe(
            df,
            name=name, version=version, country=country,
            partition_keys=partition_keys, unique_keys=unique_keys,
            quiet=quiet
        )
        return cls(dataAsset)

class LambdaEvent(BaseMessage): # pgragma: no cover

    def __init__(self, event: dict):
        super().__init__()
        self.body            = event
        self.payloadMimeType = 'text/json'
        self.objectType      = ObjectType.LAMBDA_EVENT
        self.ingestionId     = None
