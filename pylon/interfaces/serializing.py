"""Classes for serializing/deserialising objects"""
from __future__ import annotations

import abc
import dataclasses
import json

_JSON_SERIALIZABLE_SUBCLASSES = dict()


class NotJsonSerializable(Exception):
    """Raised when a dictionary cannot be deserialized in as it is not a serialized
        JsonSerializable.
    """


class SubclassNotDataclass(Exception):
    """Raised when subclassing a class and the base class is required to be a dataclass, but it is
        not.
    """


class NoMatchingSubclass(Exception):
    """Raised when deserialising an object and the target class cannot be found."""


class JsonSerializable(abc.ABC):
    """A super class for dataclasses which can be serialized and then deserialized to/from JSON."""
    def to_json(self) -> str:
        """Serliazes this object as JSON.

        Returns:
            str: A JSON string representing this object.
        """
        _dict = dataclasses.asdict(self)
        _dict['__classname__'] = self.__class__.__name__
        out = json.dumps(_dict)
        return out

    @staticmethod
    def from_json(json_str: str) -> JsonSerializable:
        """Deserializes JSON as an instance of the appropriate subclass.

        Args:
            json_str (str): The JSON string to be deserialzed.

        Raises:
            NotJsonSerializable: Raised when the incoming JSON does not match what is expected from
                the JSON output by `to_json`.
            NoMatchingSubclass: Raised when no subclass which matches the incoming JSON can be
                found.

        Returns:
            JsonSerializable: The deserializeed JSON string.
        """
        # Parse JSON
        _dict = json.loads(json_str)

        # Find the name of the class to instantiate (target class)
        try:
            target_class_name = _dict.pop('__classname__')
        except KeyError as _ex:
            raise NotJsonSerializable() from _ex

        # Find candidates for the target class by looking for subclasses
        try:
            target_classes = _JSON_SERIALIZABLE_SUBCLASSES[target_class_name]
        except KeyError as _ex:
            raise NoMatchingSubclass(f'No class found with name {target_class_name}.') from None

        # From the potential target classes, find a target class which has fields matching the
        # fields provided in the JSON.
        source_field_names = set(_dict.keys())
        for target_class in target_classes:
            target_fields = {field.name: field for field in dataclasses.fields(target_class)}
            target_field_names = set(target_fields.keys())
            # If fields match break out to finish processing
            if source_field_names == target_field_names:
                break
        # If the loop was not broken out of no match was found and so we raise an exception
        else:
            raise NoMatchingSubclass(
                f'No class found with name {target_class_name} and matchinf fields.'
            )

        # Create a new instance of the target class.
        # We need to distinguish between fields which can be given during the init, and which should
        # be set afterwards.
        init_kwargs = {name: value for name, value in _dict.items() if target_fields[name].init}
        out = target_class(**init_kwargs)  # pylint: disable=undefined-loop-variable
        for name, value in _dict.items():
            setattr(out, name, value)

        return out


    @classmethod
    def __init__subclass__(cls, **kwargs):
        if dataclasses.is_dataclass(cls):
            raise SubclassNotDataclass(f'{cls.__name__} must be a dataclass.')

        _JSON_SERIALIZABLE_SUBCLASSES.setdefault(cls.__name__, set()).add(cls)

        super().__init_subclass__(**kwargs)

    def __repr__(self):
        return self.to_json()
