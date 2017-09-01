from barin import cursor
from barin import query


class ClassManager(object):

    def __init__(self, reg, cls):
        self._reg = reg
        self._cls = cls
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        if reg.spec:
            self.query = self.query.match(reg.spec)
            self.aggregate = self.aggregate.match(reg.spec)

    def __repr__(self):
        return '<ClassManager for {}>'.format(self._cls.__name__)

    def __getattr__(self, name):
        return getattr(self._reg, name)

    def __getitem__(self, name):
        return self._reg[name]

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        val = dict(*args, **kwargs)
        return self.validate(val)

    def validate(self, value, state=None):
        value = self.schema.validate(value, state)
        return self._cls(value)

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    def insert_one(self, obj):
        return self.collection.insert_one(obj)

    def insert_many(self, objs):
        return self.collection.insert_many(objs)

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
            return self.validate(res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper


