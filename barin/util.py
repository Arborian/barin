from functools import update_wrapper


class _NoDefault(object):
    pass

NoDefault = _NoDefault()


def reify(func):
    result = _Reified(func)
    update_wrapper(result, func)
    return result


class _Reified(object):

    def __init__(self, func, name=None):
        self._func = func
        if name is None:
            name = func.__name__
        self._name = name

    def __get__(self, obj, cls=None):
        if obj is None:
            return None
        result = obj.__dict__[self._name] = self._func(obj)
        return result
