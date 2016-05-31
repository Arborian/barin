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


class Document(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class Collection(Document):
    pass


class FieldCollection(Document):

    def make_schema(self):
        return dict((name, f.schema) for name, f in self.items())


def collection(metadata, cname, *args, **options):
    fields = []
    indexes = []
    for arg in args:
        if isinstance(arg, field.Field):
            fields.append(arg)
        elif isinstance(arg, index.Index):
            indexes.append(arg)
    field_collection = FieldCollection((f.name, f) for f in fields)
    schema = field_collection.make_schema()
    mgr = manager.Manager(cname, schema)
    mgr.indexes = indexes
    dct = dict(m=mgr, c=field_collection)
    res = type(cname, (Collection,), dct)
    metadata.register(res)
    return res
