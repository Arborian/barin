from barin.util import reify


def adapter(manager):
    if manager.polymorphic_discriminator:
        return PolymorphicAdapter(manager)
    else:
        return StaticAdapter(manager)


class StaticAdapter(object):
    '''Validate and adapt instances of a (non-polymorphic) managed class'''

    def __init__(self, manager):
        self._manager = manager

    @reify
    def cls(self):
        return self._manager.cls

    @reify
    def schema(self):
        return self._manager.schema

    def __call__(self, obj, state=None):
        vobj = self.schema.validate(obj, state)
        if not isinstance(vobj, self.cls):
            vobj = self.cls(vobj)
        return vobj


class PolymorphicAdapter(object):
    '''Validate and adapt instances of a polymorphic managed class'''

    def __init__(self, manager):
        self._manager = manager

    @reify
    def cls(self):
        return self._manager.cls

    @reify
    def registry(self):
        return self._manager.registry

    def __call__(self, obj, state=None):
        reg = self.registry.by_value(obj)
        vobj = reg.schema.validate(obj, state)
        return vobj