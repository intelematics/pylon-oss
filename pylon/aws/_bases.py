"""Some base classes for AWS"""

class BaseMixin:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return '<{module}.{cls} {name}>'.format(
            module=self.__class__.__module__,
            cls=self.__class__.__name__,
            name=self.name
        )
