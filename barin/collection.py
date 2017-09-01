from itertools import chain
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
        metadata, cname, indexes, **options)
    dct = dict(m=mgr, __barin__=mgr, **fields)
    cls = type(cname, (Collection,), dct)
    mgr.registry.register(cls, fields)
    metadata.register(cls)
    return cls


def subdocument(metadata, name, *args, **options):
    fields = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            raise errors.SchemaError('Indexes must only occur on base collections')
        else:
            raise errors.SchemaError('Unknown argument type {}'.format(arg))
    fields = field.FieldCollection(fields)
    fields.bind_metadata(metadata)
    mgr = manager.Manager(metadata, name, **options)
    dct = dict(m=mgr, __barin__=mgr, **fields)
    cls = type(name, (base.Document,), dct)
    mgr.registry.register(cls, fields)
    metadata.register(cls)
    return cls


def derived(parent, discriminator, *args, **options):
    mgr = parent.m.manager
    fields = [
        field.Field(
            mgr.registry.polymorphic_discriminator,
            str, default=discriminator)]
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            raise errors.SchemaError(
                'Indexes must only occur on base collections')
        else:
            raise errors.SchemaError('Unknown argument type {}'.format(arg))
    base_fields = mgr.registry.default.fields.values()
    fields = field.FieldCollection(chain(base_fields, fields))
    fields.bind_metadata(mgr.metadata)
    dct = dict(m=mgr, __barin__=mgr, **fields)
    name = '{}[{}={}]'.format(
        mgr.name, mgr.registry.polymorphic_discriminator, discriminator)
    cls = type(name,  (base.Document,), dct)
    mgr.registry.register(cls, fields, discriminator)
    return cls


