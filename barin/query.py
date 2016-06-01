from itertools import chain

from .base import partialmethod
from .cursor import Cursor
from . import mql


def _append_simple(self, op, value):
    stage = {op: value}
    return Query(self._mgr, self._pipeline + [stage])


def _append_pipeline(self, op, value):
    stage = {op: value}
    return Aggregation(self._mgr, self._pipeline + [stage])


class Query(object):

    def __init__(self, mgr, pipeline=None):
        self._mgr = mgr
        if pipeline is None:
            pipeline = []
        self._pipeline = pipeline
        self._wrap_mgr('find')
        self._wrap_mgr('update_one')
        self._wrap_mgr('update_many')
        self._wrap_mgr('replace_one')
        self._wrap_mgr('remove_one')
        self._wrap_mgr('remove_many')
        self._wrap_mgr('find_one_and_update')
        self._wrap_mgr('find_one_and_replace')
        self._wrap_mgr('find_one_and_delete')

    match = partialmethod(_append_simple, '$match')
    limit = partialmethod(_append_simple, '$limit')
    skip = partialmethod(_append_simple, '$skip')
    sort = partialmethod(_append_simple, '$sort')
    geo_near = partialmethod(_append_simple, '$geoNear')

    project = partialmethod(_append_pipeline, '$project')
    redact = partialmethod(_append_pipeline, '$redact')
    unwind = partialmethod(_append_pipeline, '$unwind')
    group = partialmethod(_append_pipeline, '$group')
    sample = partialmethod(_append_pipeline, '$sample')
    lookup = partialmethod(_append_pipeline, '$lookup')
    index_stats = partialmethod(_append_pipeline, '$indexStats')

    def __iter__(self):
        return iter(self._mgr.find(**self._compile_query()))

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, basestring):
            sval = [(key_or_list, direction)]
        else:
            sval = key_or_list
        stage = {'$sort': sval}
        return Query(self._mgr, self._pipeline + [stage])

    def all(self):
        return list(self)

    def first(self):
        return iter(self).next()

    def one(self):
        it = iter(self)
        res = it.next()
        try:
            it.next()
        except StopIteration:
            return res
        raise ValueError('More than one result returned for one()')

    def _wrap_mgr(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self._mgr, name)
            return orig(self._compile_query(), *args, **kwargs)
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper

    def _compile_query(self):
        filters = []
        limits = []
        skips = []
        sorts = []
        for stage in self._pipeline:
            op, value = stage.items()[0]
            if op == '$match':
                filters.append(value)
            elif op == '$limit':
                limits.append(value)
            elif op == '$skip':
                skips.append(value)
            elif op == '$sort':
                sorts.append(value)
        result = dict(
            filter=mql.and_(*filters))
        if limits:
            result['limit'] = max(limits)
        if skips:
            result['skip'] = sum(skips)
        if sorts:
            result['sort'] = list(chain(*sorts))
        return result


class Aggregation(Query):

    project = partialmethod(_append_pipeline, '$project')
    match = partialmethod(_append_pipeline, '$match')
    limit = partialmethod(_append_pipeline, '$limit')
    skip = partialmethod(_append_pipeline, '$skip')
    sort = partialmethod(_append_pipeline, '$sort')
    geo_near = partialmethod(_append_pipeline, '$geoNear')

    def __iter__(self):
        cursor = self._mgr.collection.aggregate(
            self._pipeline)
        return iter(Cursor(self._mgr, cursor))
