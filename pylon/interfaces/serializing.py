import json
import typing

class JsonSerializable:

    def toJSON(self) -> str:
        jsonObj = {
            k: getattr(self, k)
            for k in self.jsonKeys()
        }
        return json.dumps(jsonObj)

    @classmethod
    def fromJSON(cls, jsonStr: str):
        jsonObj = json.loads(jsonStr)
        deserialized = cls()
        for k in deserialized.jsonKeys():
            v = jsonObj.get(k, None)
            setattr(deserialized, k, v)
        return deserialized

    def jsonKeys(self) -> typing.Iterable:
        """
        Return the list of keys to be serialized

        To only serialize specific keys override this
        method in subclasses
        """
        keys = []
        for attr in dir(self):
            if attr.startswith('__') and attr.endswith('__'):
                continue
            if isinstance(getattr(self, attr), typing.Callable):
                continue
            keys.append(attr)
        return keys

    def __repr__(self):
        return self.toJSON()
