from itertools import chain

from . import base
from . import manager
from . import field
from . import index
from . import errors


class Collection(base.Document):
    pass


class CollectionRef(object):

    def __init__(self, metadata, name):
        self._metadata = metadata
        self._name = name

    def __getattr__(self, name):
        actual = self._metadata[self._name]
        return getattr(actual, name)

    def __dir__(self):
        return dir(self._metadata[self._name]) + list(self.__dict__.keys())


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
            raise errors.SchemaError(
                'Indexes must only occur on base collections')
        else:
            raise errors.SchemaError('Unknown argument type {}'.format(arg))
    fields = field.FieldCollection(fields)
    fields.bind_metadata(metadata)
    mgr = manager.BaseManager(metadata, name, **options)
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
    cls = type(name,  (parent,), dct)
    mgr.registry.register(cls, fields, discriminator)
    return cls


def cmap(collection):
    '''decorator that marks a class as providing behavior for a collection'''
    def decorator(cls):
        mapped_cls = type(
            cls.__name__, (cls, collection), {})
        collection.m.registry.register_override(collection, mapped_cls)
        return mapped_cls
    return decorator
