import pymongo

from . import schema as S
from . import cursor
from . import query
from .util import reify

# TODO: test backref logic


class Manager(object):

    def __init__(self, metadata, name, fields, **options):
        self.metadata = metadata
        self.name = name
        self.fields = fields
        self.options = options
        self._class_managers = {}

    def __get__(self, obj, cls=None):
        if cls is None:
            cls = obj.__class__
        cm = self._class_managers.get(cls)
        if cm is None:
            cm = ClassManager(self, cls)
        if obj is None:
            return cm
        else:
            return InstanceManager(cm, obj)

    def __getitem__(self, name):
        return self.fields[name]

    @reify
    def schema(self):
        return self.fields.make_schema(self.metadata, **self.options)

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

    @reify
    def schema(self):
        options = dict(self._manager.options)
        options.setdefault('as_class', self._cls)
        return self.fields.make_schema(
            self._manager.metadata, **options)

    def __dir__(self):
        return dir(self._manager) + self.__dict__.keys()

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def __getitem__(self, name):
        return self._manager[name]

    def validate(self, value, state=None):
        if self.schema is not S.Missing:
            value = self.schema.validate(value, state)
        return value

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        val = dict(*args, **kwargs)
        return self.validate(val)


class InstanceManager(object):

    def __init__(self, class_manager, obj):
        self._class_manager = class_manager
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._class_manager, name)

    def synchronize(self, isdel=False):
        '''Sync all backrefs'''
        _id = self._obj['_id']
        for fname, f in self.fields.items():
            if f.backref:
                v = f.__get__(self._obj)
                other_cls = self.metadata[f.backref.cname]
                other_fld = other_cls.m.fields[f.backref.fname]
                if isinstance(f._schema, S.Array):
                    if isinstance(other_fld._schema, S.Array):
                        self._sync_m2m(_id, f, v, other_cls, other_fld, isdel)
                    else:
                        self._sync_o2m(_id, f, v, other_cls, other_fld, isdel)
                else:
                    if isinstance(other_fld._schema, S.Array):
                        self._sync_m2o(_id, f, v, other_cls, other_fld, isdel)
                    else:
                        self._sync_o2o(_id, f, v, other_cls, other_fld, isdel)

    def _sync_m2m(self, this_id, this_fld, this_val, other_cls, other_fld, isdel):
        "this is an array, other is an array"
        q = (other_cls.m.query
            .match(other_fld == this_id)
            .match(other_cls._id.nin(this_val)))
        q.update_many(other_fld.pull(this_id))
        if isdel:
            return
        q = (other_cls.m.query
            .match(other_cls._id.in_(this_val)))
        q.update_many(other_fld.add_to_set(this_id))

    def _sync_o2m(self, this_id, this_fld, this_val, other_cls, other_fld, isdel):
        "this is an array, other is a scalar"
        q = (other_cls.m.query
            .match(other_fld == this_id)
            .match(other_cls._id.nin(this_val)))
        q.update_many(other_fld.set(None))
        if isdel:
            return
        q = (other_cls.m.query
            .match(other_cls._id.in_(this_val)))
        q.update_many(other_fld.set(this_id))

    def _sync_m2o(self, this_id, this_fld, this_val, other_cls, other_fld, isdel):
        "this is a scalar, other is an array"
        q = (other_cls.m.query
            .match(other_fld == this_id)
            .match(other_cls._id != this_val))
        q.update_many(other_fld.pull(this_id))
        if isdel:
            return
        q = (other_cls.m.query
            .match(other_cls._id == this_val))
        q.update_one(other_fld.add_to_set(this_id))

    def _sync_o2o(self, this_id, this_fld, this_val, other_cls, other_fld, isdel):
        "this is a scalar, other is a scalar"
        q = (other_cls.m.query
            .match(other_fld == this_id)
            .match(other_cls._id != this_val))
        q.update_many(other_fld.set(None))
        if isdel:
            return
        q = (other_cls.m.query
            .match(other_cls._id == this_val))
        q.update_one(other_fld.set(this_id))


class CollectionManager(Manager):

    def __init__(self, metadata, cname, fields, indexes, **options):
        super(CollectionManager, self).__init__(
            metadata, cname, fields, **options)
        self.indexes = indexes

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
        return getattr(self._db, self.name)

    @property
    def _db(self):
        return self.metadata.db

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
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)

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


class InstanceCollectionManager(InstanceManager):

    def insert(self):
        return self._class_manager.insert_one(self._obj)

    def delete(self):
        return self._class_manager.delete_one(
            {'_id': self._obj._id})

    def replace(self, **kwargs):
        return self._class_manager.replace_one(
            {'_id': self._obj._id}, self._obj, **kwargs)

    def update(self, update_spec, **kwargs):
        refresh = kwargs.pop('refresh', False)
        if refresh:
            obj = self._class_manager.find_one_and_update(
                {'_id': self._obj._id},
                update_spec,
                return_document=pymongo.ReturnDocument.AFTER,
                **kwargs)
            if obj:
                self._obj.clear()
                self._obj.update(obj)
            else:
                # Object has been deleted
                return None
        else:
            return self._class_manager.update_one(
                {'_id': self._obj._id},
                update_spec, **kwargs)
