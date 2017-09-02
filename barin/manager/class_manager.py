from barin import cursor
from barin import query
from barin.adapter import adapter


class ClassManager(object):

    def __init__(self, reg, cls):
        self._reg = reg
        self.cls = cls
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')
        self.adapter = adapter(self)
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        if reg.spec:
            self.query = self.query.match(reg.spec)
            self.aggregate = self.aggregate.match(reg.spec)

    def __repr__(self):
        return '<{} {}>'.format(
            self.__class__.__name__,
            self.cls.__name__)

    def __getattr__(self, name):
        return getattr(self._reg, name)

    def __getitem__(self, name):
        return self._reg[name]

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        vobj = self.schema.validate(dict(*args, **kwargs))
        return self.cls(vobj)

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    def insert_one(self, obj):
        return self.collection.insert_one(
            self.adapter(obj))

    def insert_many(self, objs):
        return self.collection.insert_many(map(self.adapter, objs))

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
            return self.adapter(res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper


