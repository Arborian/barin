from functools import partial


class Document(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return '{}: {}'.format(
            self.__class__.__name__,
            super(Document, self).__repr__())


class _PartialMethod(partial):

    def __get__(self, obj, cls):
        if obj is None:
            return self
        return partial(
            self.func, obj,
            *(self.args or ()),
            **(self.keywords or {}))


partialmethod = _PartialMethod
