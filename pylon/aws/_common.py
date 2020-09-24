from ..models.messages import BaseMessage, ObjectType, MessageAttribute
from ..models.data import DataAsset
from ..models.ingestion import IngestionStep
from ..interfaces.serializing import JsonSerializable
from . import s3

TRUE_STRING  = 'true'
FALSE_STRING = 'false'


def encodeMessage(message: BaseMessage) -> dict:
    """
    Encode the given message object into a format suitable for
    posting to a queue or topic.
    """
    body = message.getBody()
    attributes = {
        MessageAttribute.PAYLOAD_MIME_TYPE: message.payloadMimeType,
        MessageAttribute.OBJECT_TYPE:       message.objectType,
        MessageAttribute.INGESTION_ID:      message.ingestionId,
        MessageAttribute.ARTIFACT_NAME:     message.artifactName,
        MessageAttribute.ARTIFACT_VERSION:  message.artifactVersion
    }
    attributes.update(message.customAttributes)

    if isinstance(body, JsonSerializable):
        body = body.toJSON()

    if not body:
        body = 'filling message body with a string so it is not empty'

    if message.objectType == ObjectType.INGESTION_STEP:
        attributes[MessageAttribute.INGESTION_ID] = message.ingestionId

    if message.payloadStoreKey is not None:
        attributes[MessageAttribute.PAYLOAD_STORE_KEY] = message.payloadStoreKey

    attributes = {
        str(k): {
            'StringValue': str(v),
            'DataType': 'String'
        }
        for k, v in attributes.items()
    }

    return {'body': body, 'attributes': attributes}


def decodeMessage(body: str, attributes: dict) -> BaseMessage:
    """
    Decodes a message from the body and attributes into pylon message models
    """
    message = BaseMessage()
    message.body            = body
    message.payloadMimeType = attributes.pop(MessageAttribute.PAYLOAD_MIME_TYPE)['StringValue']
    message.objectType      = attributes.pop(MessageAttribute.OBJECT_TYPE)['StringValue']
    message.ingestionId     = attributes.pop(MessageAttribute.INGESTION_ID)['StringValue']
    message.artifactName    = attributes.pop(MessageAttribute.ARTIFACT_NAME)['StringValue']
    message.artifactVersion = attributes.pop(MessageAttribute.ARTIFACT_VERSION)['StringValue']

    payloadStoreKey = attributes.pop(MessageAttribute.PAYLOAD_STORE_KEY, None)
    if payloadStoreKey is not None:
        message.payloadStoreKey = payloadStoreKey['StringValue']

    if message.ingestionId == 'None':
        message.ingestionId = None
    if message.artifactName == 'None':
        message.artifactName = None
    if message.artifactVersion == 'None':
        message.artifactVersion = None
    if message.payloadStoreKey == 'None':
        message.payloadStoreKey = None

    if not message.isCheckedIn():
        if message.objectType == ObjectType.DATA_ASSET:
            message.body = DataAsset.fromJSON(message.body)

        if message.objectType == ObjectType.INGESTION_STEP:
            message.body = IngestionStep.fromJSON(message.body)

    message.customAttributes = {
        attribute: attributes[attribute]['StringValue']
        for attribute in attributes
    }

    return message
