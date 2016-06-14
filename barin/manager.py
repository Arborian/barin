from . import schema as S
from . import cursor
from . import query


class Manager(object):

    def __init__(self, name, fields, **options):
        self._name = name
        self.fields = fields
        self.options = options
        self.schema = fields.make_schema(**options)

    def __get__(self, obj, cls=None):
        class_manager = ClassManager(self, cls)
        if obj is None:
            return class_manager
        else:
            return InstanceManager(class_manager, obj)

    def validate(self, value, state=None):
        if self.schema is not S.Missing:
            value = self.schema.validate(value, state)
        return value

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        val = dict(*args, **kwargs)
        return self.validate(val)


class ClassManager(object):

    def __init__(self, manager, cls):
        self._manager = manager
        self._cls = cls
        options = dict(manager.options)
        options.setdefault('as_class', cls)
        self.schema = manager.fields.make_schema(**options)

    def __dir__(self):
        return dir(self._manager) + self.__dict__.keys()

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def validate(self, value, state=None):
        if self.schema is not S.Missing:
            value = self.schema.validate(value, state)
        return value


class InstanceManager(object):

    def __init__(self, class_manager, obj):
        self._class_manager = class_manager
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._class_manager, name)


class CollectionManager(Manager):

    def __init__(self, cname, fields, indexes, **options):
        super(CollectionManager, self).__init__(cname, fields, **options)
        self.indexes = indexes
        self._db = None

    def __get__(self, obj, cls=None):
        class_manager = ClassCollectionManager(self, cls)
        if obj is None:
            return class_manager
        else:
            return InstanceCollectionManager(class_manager, obj)

    @property
    def collection(self):
        if self._db is None:
            return None
        return getattr(self._db, self._name)

    def bind(self, db):
        self._db = db

    def __dir__(self):
        return dir(self.collection) + self.__dict__.keys()

    def __getattr__(self, name):
        coll = self.collection
        if coll is None:
            raise AttributeError(name)
        return getattr(coll, name)


class ClassCollectionManager(ClassManager):

    def __init__(self, manager, cls):
        super(ClassCollectionManager, self).__init__(manager, cls)
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')

    def get(self, **kwargs):
        return self.find_one(kwargs)

    def find_by(self, **kwargs):
        return self.find(kwargs)

    def insert_one(self, obj):
        return self.collection.insert_one(obj)

    def insert_many(self, objs):
        return self.collection.insert_many(objs)

    def remove_one(self, obj):
        return self.collection.remove_one(obj._id)

    def replace_one(self, obj, **kwargs):
        return self.collection.replace_one(
            {'_id': self._obj._id},
            self, **kwargs)

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


class InstanceCollectionManager(InstanceManager):

    def insert(self):
        return self._class_manager.insert_one(self._obj)

    def remove(self):
        return self._class_manager.remove_one({'_id': self._obj._id})

    def replace(self, **kwargs):
        return self._class_manager.replace_one(
            {'_id': self._obj._id}, self._obj, **kwargs)

    def update(self, update_spec, **kwargs):
        return self._class_manager.update_one(
            {'_id': self._obj._id},
            update_spec, **kwargs)


############################
# Obsolete
############################


class _Manager(object):

    def __init__(self, cname, fields, indexes, **options):
        # schema=S.Missing, db=None):
        self._cname = cname
        self.fields = fields
        self.indexes = indexes
        self.options = options
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        self.schema = fields.make_schema(**options)
        self._db = None

    @property
    def collection(self):
        if self._db is None:
            return None
        return getattr(self._db, self._cname)

    def bind(self, db):
        self._db = db

    def validate(self, value, state=None):
        if self.schema is not S.Missing:
            value = self.schema.validate(value, state)
        return value

    def __dir__(self):
        return dir(self.collection) + self.__dict__.keys()

    def __getattr__(self, name):
        coll = self.collection
        if coll is None:
            raise AttributeError(name)
        return getattr(coll, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            return ClassManager(self, cls)
        else:
            return InstanceManager(self, obj)


class _ClassManager(object):

    def __init__(self, manager, cls):
        self._manager = manager
        self._cls = cls
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')
        self.schema = manager.fields.make_schema(
            as_class=cls, **manager.options)

    def __dir__(self):
        return dir(self._manager) + self.__dict__.keys()

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def validate(self, value, state=None):
        value = self._manager.validate(value, state)
        return self._cls(value)

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        val = dict(*args, **kwargs)
        return self.validate(val)

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
            return self.validate(res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper


class _InstanceManager(object):

    def __init__(self, manager, obj):
        self._manager = manager
        self._class_manager = ClassManager(manager, obj.__class__)
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._class_manager, name)

    def insert(self):
        return self.collection.insert(self._obj)

    def delete(self):
        return self.collection.delete(self._obj._id)

    def replace(self, **kwargs):
        return self.collection.replace_one({'_id': self._obj._id}, self._obj, **kwargs)

    def update(self, update_spec, **kwargs):
        return self.collection.update_one({'_id': self._obj._id}, update_spec, **kwargs)
