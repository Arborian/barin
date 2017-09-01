import logging
from collections import defaultdict

from . import polymorphism as poly
from .class_manager import ClassManager, CollectionClassManager
from .instance_manager import InstanceManager


log = logging.getLogger(__name__)


class BaseManager(object):

    def __init__(self, metadata, name, **options):
        self.metadata = metadata
        self.name = name
        self.options = options
        self.registry = poly.Registry(
            self, options.pop('polymorphic_discriminator', None))
        self.hooks = defaultdict(list)

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)

    def class_manager(self, cls):
        reg = self.registry.by_class(cls)
        return ClassManager(reg, cls)

    def __get__(self, obj, cls=None):
        if cls is None:
            cls = type(obj)
        cm = self.class_manager(cls)
        if obj is None:
            return cm
        else:
            return InstanceManager(cm, obj)

    def __getitem__(self, name):
        return self.fields[name]


class CollectionManager(BaseManager):

    def __init__(self, metadata, cname, indexes, **options):
        super(CollectionManager, self).__init__(
            metadata, cname, **options)
        self.indexes = indexes

    def class_manager(self, cls):
        reg = self.registry.by_class(cls)
        return CollectionClassManager(reg, cls, self)

    @property
    def collection(self):
        if self._db is None:
            return None
        return getattr(self._db, self.name)

    @property
    def _db(self):
        return self.metadata.db

    def __dir__(self):
        return dir(self.collection) + list(self.__dict__.keys())

    def __getattr__(self, name):
        coll = self.collection
        if coll is None:
            raise AttributeError(name)
        return getattr(coll, name)

