from barin import cursor
from barin import query
from barin import event
from barin.adapter import adapter


class ClassManager(object):

    def __init__(self, reg, cls):
        self._reg = reg
        self.cls = cls
        self.adapter = adapter(self)

    def __repr__(self):
        return '<{} {}>'.format(
            self.__class__.__name__,
            self.cls.__name__)

    def __getattr__(self, name):
        return getattr(self._reg, name)

    def __dir__(self):
        return dir(self._reg) + list(self.__dict__.keys())

    def __getitem__(self, name):
        return self._reg[name]

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        vobj = self.schema.validate(dict(*args, **kwargs))
        return self.cls(vobj)


class CollectionClassManager(ClassManager):

    def __init__(self, reg, cls, collection_manager):
        super(CollectionClassManager, self).__init__(reg, cls)
        self.collection_manager = collection_manager
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        if reg.spec:
            self.query = self.query.match(reg.spec)
            self.aggregate = self.aggregate.match(reg.spec)

    def filtered_query(self, spec):
        result = dict(spec)
        result.update(self._reg.spec)
        return result

    def _wrap_cursor(name):
        def wrapper(self, *args, **kwargs):
            orig = getattr(self.collection_manager.collection, name)
            res = orig(*args, **kwargs)
            return cursor.Cursor(self, res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        return wrapper

    def _wrap_single(name):
        def wrapper(self, *args, **kwargs):
            orig = getattr(self.collection_manager.collection, name)
            res = orig(*args, **kwargs)
            if res is None:
                return res
            return self.adapter(res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        return wrapper

    find = _wrap_cursor('find')
    find_one = _wrap_single('find_one')
    find_one_and_update = _wrap_single('find_one_and_update')
    find_one_and_replace = _wrap_single('find_one_and_replace')
    find_one_and_delete = _wrap_single('find_one_and_delete')

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    @event.with_hooks()
    def insert_one(self, obj):
        return self.collection_manager.insert_one(
            self.adapter(obj))

    @event.with_hooks()
    def insert_many(self, objs):
        return self.collection_manager.insert_many(map(self.adapter, objs))
