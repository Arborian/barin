from .adapter import adapter


class Cursor(object):

    def __init__(self, manager, pymongo_cursor):
        self._manager = manager
        self.pymongo_cursor = pymongo_cursor
        self._wrap_cursor('sort')
        self._wrap_cursor('skip')
        self._wrap_cursor('limit')
        self.adapter = adapter(manager)

    def __getattr__(self, name):
        return getattr(self.pymongo_cursor, name)

    def __dir__(self):
        return dir(self.pymongo_cursor) + list(self.__dict__.keys())

    def __iter__(self):
        return self

    def __next__(self):
        obj = next(self.pymongo_cursor)
        return self.adapter(obj)
    next = __next__

    def all(self):
        return list(self)

    def first(self):
        return next(iter(self))

    def one(self):
        it = iter(self)
        res = next(it)
        try:
            next(it)
        except StopIteration:
            return res
        raise ValueError('More than one result returned for one()')

    def _wrap_cursor(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self.pymongo_cursor, name)
            res = orig(*args, **kwargs)
            return Cursor(self._manager, res)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper
