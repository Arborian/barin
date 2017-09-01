from collections import defaultdict
from .collection import Collection, CollectionRef


class Metadata(object):

    def __init__(self, db=None):
        self.collections = []
        self._classes_full = {}
        self._classes_short = defaultdict(list)
        self.db = db

    def __getitem__(self, index):
        try:
            return self._classes_full[index]
        except KeyError:
            pass
        classes = self._classes_short.get(index, [])
        if len(classes) == 1:
            return classes[0]
        elif len(classes) > 1:
            options = map(repr, classes)
            raise ValueError(
                'Ambiguous classname, could be any of: [%s]',
                ', '.join(options))
        else:
            raise KeyError(index)

    def register(self, cls):
        if issubclass(cls, Collection):
            self.collections.append(cls)
        k = cls.__module__ + '.' + cls.__name__
        self._classes_full[k] = cls
        self._classes_short[cls.__name__].append(cls)

    def bind(self, db):
        self.db = db

    def cref(self, name):
        return CollectionRef(self, name)
