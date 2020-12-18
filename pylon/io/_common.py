"""For code common to multiple IO related features.

This can probaby be done a lot better, but should be saved for when touching up the message models.
"""
from typing import Dict

from ..interfaces.serializing import JsonSerializable
from ..models.data import DataAsset
from ..models.ingestion import IngestionStep
from ..models.messages import BaseMessage, MessageAttribute, ObjectType


def encode_message(message: BaseMessage) -> Dict:
    """Encodes a BaseMessage as a dict.

    Args:
        message (BaseMessage): The message to be encoded.

    Returns:
        Dict: A dict with two keys: "body" which has the body of the message as a string, and
            "attributes" which is another dict of key/value pairs for the attributes of the message.
    """
    # TODO: Consider if this function or similar should be a part of the BaseMessage class
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

    return {'body': body, 'attributes': attributes}


def decode_message(body: str, attributes: Dict) -> BaseMessage:
    """Decodes the body and attributes of a message into a BaseMessage.

    Args:
        body (str): The body of the message being decoded.
        attributes (Dict): The attributes of the message being decoded.

    Returns:
        BaseMessage: A message with the provided body and attributes.
    """
    # TODO: Consider if this function or similar should be a part of the BaseMessage class
    # pylint: disable=bad-whitespace
    message = BaseMessage()
    message.body            = body
    message.payloadMimeType = attributes.pop(MessageAttribute.PAYLOAD_MIME_TYPE)
    message.objectType      = attributes.pop(MessageAttribute.OBJECT_TYPE)
    message.ingestionId     = attributes.pop(MessageAttribute.INGESTION_ID)
    message.artifactName    = attributes.pop(MessageAttribute.ARTIFACT_NAME)
    message.artifactVersion = attributes.pop(MessageAttribute.ARTIFACT_VERSION)

    message.payloadStoreKey = attributes.pop(MessageAttribute.PAYLOAD_STORE_KEY, None)

    if not message.isCheckedIn():
        if message.objectType == ObjectType.DATA_ASSET:
            message.body = DataAsset.fromJSON(message.body)

        if message.objectType == ObjectType.INGESTION_STEP:
            message.body = IngestionStep.fromJSON(message.body)

    # Everything remaining in attributes is a custom attribute
    message.customAttributes = attributes.copy()

    return message
