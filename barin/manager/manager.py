import logging
import pymongo

from . import schema as S
from . import cursor
from . import query
from . import polymorphism as poly
from .util import reify, NoDefault


log = logging.getLogger(__name__)

# TODO: test backref logic



class Manager(object):

    def __init__(self, metadata, name, **options):
        self.metadata = metadata
        self.name = name
        self.options = options
        self.registry = poly.Registry(
            self, options.pop('polymorphic_discriminator', None))

    def __get__(self, obj, cls=None):
        if cls is None:
            cls = type(obj)
        reg = self.registry.by_class(cls)
        if obj is None:
            return ClassManager(reg, cls)
        else:
            return InstanceManager(reg, obj)

    def __getitem__(self, name):
        return self.fields[name]


class CollectionManager(Manager):

    def __init__(self, metadata, cname, indexes, **options):
        super(CollectionManager, self).__init__(
            metadata, cname, **options)
        self.indexes = indexes

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


class ClassManager(object):

    def __init__(self, reg, cls):
        self._reg = reg
        self._cls = cls
        self._wrap_cursor('find')
        self._wrap_single('find_one')
        self._wrap_single('find_one_and_update')
        self._wrap_single('find_one_and_replace')
        self._wrap_single('find_one_and_delete')

    def __repr__(self):
        return '<ClassManager for {}>'.format(self._cls.__name__)

    def __getattr__(self, name):
        return getattr(self._reg, name)

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


class InstanceManager(object):

    def __init__(self, manager, obj):
        self._manager = manager
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._manager, name)

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

    def insert(self):
        return self._manager.insert_one(self._obj)

    def delete(self):
        return self._manager.delete_one(
            {'_id': self._obj._id})

    def replace(self, **kwargs):
        return self._manager.replace_one(
            {'_id': self._obj._id}, self._obj, **kwargs)

    def update(self, update_spec, **kwargs):
        refresh = kwargs.pop('refresh', False)
        if refresh:
            obj = self._manager.find_one_and_update(
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
            return self._manager.update_one(
                {'_id': self._obj._id},
                update_spec, **kwargs)

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

