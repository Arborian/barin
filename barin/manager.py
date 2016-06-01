from . import schema as S
from . import cursor
from . import query


class Manager(object):

    def __init__(self, cname, field_collection, indexes, **options):
        # schema=S.Missing, db=None):
        self._cname = cname
        self.f = field_collection
        self.indexes = indexes
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        self._schema = field_collection.make_schema(**options)
        self._db = None

    @property
    def collection(self):
        return getattr(self._db, self._cname)

    def bind(self, db):
        self._db = db

    def validate(self, value, state=None):
        if self._schema is not S.Missing:
            value = self._schema.validate(value, state)
        return value

    def __dir__(self):
        return dir(self.collection) + self.__dict__.keys()

    def __getattr__(self, name):
        return getattr(self.collection, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            return ClassManager(self, cls)
        else:
            return InstanceManager(self, obj)


class ClassManager(object):

    def __init__(self, manager, cls):
        self._manager = manager
        self._cls = cls
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')

    def __dir__(self):
        return dir(self._manager) + self.__dict__.keys()

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def validate(self, value, state=None):
        value = self._manager.validate(value, state)
        return self._cls(value)

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    def _wrap_cursor(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self.collection, name)
            res = orig(*args, **kwargs)
            return cursor.Cursor(self, res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper

    def _wrap_single(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self.collection, name)
            res = orig(*args, **kwargs)
            if res is None:
                return res
            return self._cls(res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper


class InstanceManager(Manager):

    def __init__(self, manager, obj):
        self._manager = manager
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def insert(self):
        return self.collection.insert(self._obj)

    def delete(self):
        return self.collection.delete(self._obj._id)
