import re
from barin.util import reify, NoDefault


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

    def register_override(self, collection, cls):
        reg = self.by_class(collection)
        reg.cls = cls

    def by_disc(self, discriminator):
        return self._by_disc.get(discriminator, self.default)

    def by_class(self, cls):
        for cur in cls.mro():
            reg = self._by_cls.get(cur, None)
            if reg is not None:
                return reg
        return self.default

    def by_value(self, value):
        disc = value.get(self.polymorphic_discriminator)
        return self.by_disc(disc)

    def __getattr__(self, name):
        return getattr(self.manager, name)

    def __dir__(self):
        return dir(self.manager) + list(self.__dict__.keys())


class Registration(object):

    def __init__(self, registry, cls, fields, discriminator):
        self.registry = registry
        self.cls = cls
        self.fields = fields
        self.discriminator = discriminator
        if discriminator is NoDefault:
            self.spec = {}
        else:
            re_disc = re.compile(f'^{re.escape(discriminator)}')
            self.spec = {
                registry.polymorphic_discriminator: re_disc}

    def __repr__(self):
        return '<Reg {}>'.format(self.cls.__name__)

    def __getattr__(self, name):
        return getattr(self.registry, name)

    def __dir__(self):
        return dir(self.registry) + list(self.__dict__.keys())

    def __getitem__(self, name):
        return self.fields[name]

    def make_schema(self, **options):
        '''This is called for subdocuments'''
        options.setdefault('allow_extra', True)
        options.setdefault('strip_extra', True)
        return self.fields.make_schema(
            self.metadata,
            as_class=self.cls,
            **options)

    @reify
    def schema(self):
        '''This is called documents'''
        return self.fields.make_schema(
            self.metadata,
            as_class=self.cls,
            allow_extra=True,
            strip_extra=True)
