from functools import update_wrapper


def reify(func):
    result = _Reified(func)
    update_wrapper(result, func)
    return result


class _Reified(object):

    def __init__(self, func):
        self._func = func

    def __get__(self, obj, cls=None):
        if obj is None:
            return None
        result = obj.__dict__[self.__name__] = self._func(obj)
        return result
