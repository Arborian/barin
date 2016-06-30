from . import base
from . import manager
from . import field
from . import index
from . import errors


class Metadata(object):

    def __init__(self):
        self.collections = []

    def __getitem__(self, index):
        for c in self.collections:
            if c.m.name == index:
                return c
        raise KeyError(index)

    def register(self, collection):
        self.collections.append(collection)

    def bind(self, db):
        for c in self.collections:
            c.m.bind(db)


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
    res = type(cname, (base.Document,), dct)
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
    return res
