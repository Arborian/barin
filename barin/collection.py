from . import base
from . import manager
from . import field
from . import index


class Metadata(object):

    def __init__(self):
        self.collections = []

    def register(self, collection):
        self.collections.append(collection)

    def bind(self, db):
        for c in self.collections:
            c.m.bind(db)


class CollectionDocument(base.Document):
    pass


def collection(metadata, cname, *args, **options):
    fields = []
    indexes = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            indexes.append(arg)
    fields = field.FieldCollection(
        (f.name, f) for f in fields)
    mgr = manager.Manager(cname, fields, indexes, **options)
    dct = dict(m=mgr, **fields)
    res = type(cname, (CollectionDocument,), dct)
    metadata.register(res)
    return res
