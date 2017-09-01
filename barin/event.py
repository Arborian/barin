from functools import wraps
from contextlib import contextmanager

from barin.util import _Reified


def listen(obj, hook, callback):
    obj.__barin__.hooks[hook].append(callback)


def listens_for(obj, hook=None):
    def decorator(func):
        if hook is None:
            hook_name = func.__name__
        else:
            hook_name = hook
        listen(obj, hook_name, func)
        return func
    return decorator


def with_hooks(hook=None, before=True, after=True):
    def decorator(func):
        if hook is None:
            hook_name = func.__name__
        else:
            hook_name = hook
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with hook_context(self.hooks, hook_name, self, args, kwargs):
                return func(self, *args, **kwargs)
        return wrapper
    return decorator


@contextmanager
def hook_context(hooks, hook, self, args, kwargs, before=True, after=True):
    if before:
        _call_hooks(hooks.get('before_' + hook, []), self, args, kwargs)
    yield
    if after:
        _call_hooks(hooks.get('after_' + hook, []), self, args, kwargs)


def _call_hooks(funcs, self, args, kwargs):
    for func in funcs:
        func(self, *args, **kwargs)
