from functools import partial


class Document(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class _PartialMethod(partial):

    def __get__(self, obj, cls):
        if obj is None:
            return self
        return partial(
            self.func, obj,
            *(self.args or ()),
            **(self.keywords or {}))


partialmethod = _PartialMethod
