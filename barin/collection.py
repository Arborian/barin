from collections import defaultdict

from . import base
from . import manager
from . import field
from . import index
from . import errors


class Collection(base.Document):
    pass


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


class CollectionRef(object):

    def __init__(self, metadata, name):
        self._metadata = metadata
        self._name = name

    def __getattr__(self, name):
        actual = self._metadata[self._name]
        return getattr(actual, name)


def collection(metadata, cname, *args, **options):
    fields = []
    indexes = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            indexes.append(arg)
        else:
            raise errors.SchemaError('Unknown argument type {}'.format(arg))
    fields = field.FieldCollection(fields)
    fields.bind_metadata(metadata)
    mgr = manager.CollectionManager(
        metadata, cname, fields, indexes, **options)
    dct = dict(m=mgr, __barin__=mgr, **fields)
    res = type(cname, (Collection,), dct)
    metadata.register(res)
    return res


def subdocument(metadata, name, *args, **options):
    fields = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        else:
            raise errors.SchemaError('Unknown argument type {}'.format(arg))
    fields = field.FieldCollection(fields)
    fields.bind_metadata(metadata)
    mgr = manager.Manager(metadata, name, fields, **options)
    dct = dict(m=mgr, __barin__=mgr, **fields)
    res = type(name, (base.Document,), dct)
    metadata.register(res)
    return res
