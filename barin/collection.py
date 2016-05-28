from . import manager
from . import schema as S


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


def collection(metadata, cname, *args, **options):
    fields = []
    indexes = []
    for arg in args:
        if isinstance(arg, Field):
            fields.append(arg)
        elif isinstance(arg, Index):
            indexes.append(arg)
    schema = dict((f.name, f.schema) for f in fields)
    mgr = manager.Manager(cname, schema)
    mgr.indexes = indexes
    dct = dict((f.name, f) for f in fields)
    dct['m'] = mgr
    res = type(cname, (Collection,), dct)
    metadata.register(res)
    return res


class Index(object):

    def __init__(self, arg, **options):
        if isinstance(arg, basestring):
            arg = [(arg, 1)]
        self.arg = []
        for a in arg:
            if isinstance(a, basestring):
                self.arg.append((a, 1))
            else:
                self.arg.append(a)
        self.options = options


class Field(object):

    def __init__(self, name, schema, **options):
        self.name = name
        self.schema = S.compile_schema(schema, **options)
        self.options = options

    def __get__(self, obj, cls=None):
        if obj is None:
            return ClassField(self, cls)
        else:
            return InstanceField(self, obj)


class ClassField(object):

    def __init__(self, field, cls):
        self.field = field
        self.cls = cls

    def __getattr__(self, name):
        return getattr(self.cls, name)


class InstanceField(object):

    def __init__(self, field, obj):
        self.field = field
        self.obj = obj

    def __getattr__(self, name):
        return getattr(self.obj, name)
