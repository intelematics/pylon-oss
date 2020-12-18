"""A place for code common to multiple AWS wrappers"""
from __future__ import annotations

import dataclasses
from typing import Dict

from ..utils import logging

# For encoding common non-string values into strings.
# Hopefully they are obscure enough no one would actually try and use them for another purpose.
_TRUE_STRING = r'pylon//~|~True~|~\\pylon'
_FALSE_STRING = r'pylon//~|~False~|~\\pylon'
_NONE_STRING = r'pylon//~|~None~|~\\pylon'

@dataclasses.dataclass
class Message:
    """Represents a message from an SQS Queue."""
    body: str
    attributes: Dict = dataclasses.field(default_factory=dict)

    def _encode(self):
        # Ensure body is a string
        body = self.body
        if not isinstance(str, body):
            body = str(body)
            logging.debug('Message body "%s" is not a string, casting to string.', body)

        # Ensure attribute keys and values are strings
        attributes = dict()
        for key, value in self.attributes.items():
            if not isinstance(key, str):
                key = str(key)
                logging.debug('Attribute key "%s" is not a string, casting to string.', key)
            if isinstance(value, str):
                pass
            elif value is True:
                value = _TRUE_STRING
            elif value is False:
                value = _FALSE_STRING
            elif value is None:
                value = _NONE_STRING
            else:
                value = str(value)
                logging.debug(
                    'Attribute value "%s" with key "%s" is not a string, casting to string.', value,
                    key
                )

            attributes[key] = {
                'StringValue': str(value),
                'DataType': 'String',
            }

        encoded = {
            'MessageBody': body,
            'MessageAttributes': attributes,
        }
        return encoded

    @classmethod
    def _decode(cls, encoded_message: Dict) -> Message:
        body = encoded_message['MessageBody']
        attributes = {
            key: _recover_value(value['StringValue'])
            for key, value
            in encoded_message['MessageAttributes'].items()
        }
        return cls(body=body, attributes=attributes)


def _recover_value(value: str):
    """Recover a value that has been encoded into a string"""
    if value == _TRUE_STRING:
        return True
    if value == _FALSE_STRING:
        return False
    if value == _NONE_STRING:
        return None
    return value
