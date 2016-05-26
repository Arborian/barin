from . import schema as S


class Manager(object):
    _all_managers = []

    def __init__(self, cname, schema=S.Missing, db=None):
        self._cname = cname
        self._schema = S.compile_schema(schema)
        self._db = db
        self._all_managers.append(self)

    @property
    def collection(self):
        return getattr(self._db, self._cname)

    @classmethod
    def bind_all(cls, db):
        for m in cls._all_managers:
            m.bind(db)

    def bind(self, db):
        self._db = db

    def to_py(self, value, state=None):
        if self._schema is not S.Missing:
            value = self._schema.to_py(value, state)
        return value

    def to_db(self, value, state=None):
        if self._schema is not S.Missing:
            value = self._schema.to_db(value, state)
        return value

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

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def to_py(self, value, state=None):
        value = self._manager.to_py(value, state)
        return self._cls(value)

    def to_db(self, value, state=None):
        value = self._manager.to_db(value, state)
        return value

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    def _wrap_cursor(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self.collection, name)
            res = orig(*args, **kwargs)
            return Cursor(self, res)
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


class Cursor(object):

    def __init__(self, manager, pymongo_cursor):
        self._manager = manager
        self.pymongo_cursor = pymongo_cursor
        self._wrap_cursor('sort')
        self._wrap_cursor('skip')
        self._wrap_cursor('limit')

    def __getattr__(self, name):
        return getattr(self.pymongo_cursor, name)

    def __iter__(self):
        for obj in self.pymongo_cursor:
            yield self._manager.to_py(obj)

    def all(self):
        return list(self)

    def first(self):
        return iter(self).next()

    def one(self):
        it = iter(self)
        res = it.next()
        try:
            it.next()
        except StopIteration:
            return res
        raise ValueError('More than one result returned for one()')

    def _wrap_cursor(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self.pymongo_cursor, name)
            res = orig(*args, **kwargs)
            return Cursor(self._manager, res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper
