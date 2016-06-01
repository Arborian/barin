from . import base
from . import manager
from . import query
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


class Collection(base.Document):
    pass


def collection(metadata, cname, *args, **options):
    fields = []
    indexes = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            indexes.append(arg)
    field_collection = field.FieldCollection(
        (f.name, f) for f in fields)
    schema = field_collection.make_schema()
    mgr = manager.Manager(cname, schema)
    mgr.indexes = indexes
    dct = dict(m=mgr, c=field_collection, q=query.Query(mgr))
    res = type(cname, (Collection,), dct)
    metadata.register(res)
    return res
