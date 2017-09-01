from barin.util import reify, NoDefault
from . import query


class Registry(object):

    def __init__(self, manager, polymorphic_discriminator):
        self.manager = manager
        self.polymorphic_discriminator = polymorphic_discriminator
        self._by_disc = {}
        self._by_cls = {}
        self.default = None

    def register(self, cls, fields, discriminator=NoDefault):
        reg = Registration(self, cls, fields, discriminator)
        self._by_disc[discriminator] = self._by_cls[cls] = reg
        if discriminator is NoDefault:
            self.default = reg

    def by_disc(self, discriminator):
        return self._by_disc.get(discriminator, self.default)

    def by_class(self, cls):
        return self._by_cls.get(cls, self.default)

    def by_value(self, value):
        disc = value.get(self.polymorphic_discriminator)
        return self.by_disc(disc)

    def __getattr__(self, name):
        return getattr(self.manager, name)


class Registration(object):

    def __init__(self, registry, cls, fields, discriminator):
        self.registry = registry
        self.cls = cls
        self.fields = fields
        self.discriminator = discriminator
        self.query = query.Query(self)
        self.aggregate = query.Aggregate(self)
        if discriminator is NoDefault:
            self.spec = {}
        else:
            self.spec = {registry.polymorphic_discriminator: discriminator}
            self.query = self.query.match(self.spec)
            self.aggregate = self.aggregate.match(self.spec)

    def __repr__(self):
        return '<Reg {}>'.format(self._cls.__name__)

    def __getattr__(self, name):
        return getattr(self.registry, name)

    def create(self, *args, **kwargs):
        """Create a (validated) document."""
        val = dict(*args, **kwargs)
        return self.validate(val)

    @reify
    def schema(self):
        return self.fields.make_schema(self.metadata)

