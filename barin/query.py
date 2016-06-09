from itertools import chain

from .base import partialmethod
from .cursor import Cursor
from . import mql


class _CursorSource(object):

    def get_cursor(self):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.get_cursor())

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


class Query(_CursorSource):

    def __init__(self, mgr, pipeline=None):
        self._mgr = mgr
        if pipeline is None:
            pipeline = []
        self.pipeline = pipeline
        self._wrap_mgr('find')
        self._wrap_mgr('update_one')
        self._wrap_mgr('update_many')
        self._wrap_mgr('replace_one')
        self._wrap_mgr('remove_one')
        self._wrap_mgr('remove_many')
        self._wrap_mgr('find_one_and_update')
        self._wrap_mgr('find_one_and_replace')
        self._wrap_mgr('find_one_and_delete')

    def _append(self, op, value):
        stage = {op: value}
        return Query(self._mgr, self.pipeline + [stage])

    match = partialmethod(_append, '$match')
    limit = partialmethod(_append, '$limit')
    skip = partialmethod(_append, '$skip')
    sort = partialmethod(_append, '$sort')
    geo_near = partialmethod(_append, '$geoNear')

    def get_cursor(self):
        return self._mgr.find(**self._compile_query())

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, basestring):
            sval = [(key_or_list, direction)]
        else:
            sval = key_or_list
        stage = {'$sort': sval}
        return Query(self._mgr, self.pipeline + [stage])

    def _wrap_mgr(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self._mgr, name)
            qres = self._compile_query()
            res = orig(qres['filter'], *args, **kwargs)
            if qres['limit']:
                res = res.limit(qres['limit'])
            if qres['skip']:
                res = res.skip(qres['skip'])
            if qres['sort']:
                res = res.sort(qres['sort'])
            return res
        wrapper.__name__ = 'wrapped_{}'.format(name)
        setattr(self, name, wrapper)
        return wrapper

    def _compile_query(self):
        filters = []
        limits = []
        skips = []
        sorts = []
        for stage in self.pipeline:
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


class Aggregate(_CursorSource):
    OPS_MODIFYING_DOC_STRUCTURE = set([
        '$project', '$redact', '$unwind', '$group',
        '$lookup', '$indexStats'])

    def __init__(self, mgr, pipeline=None):
        self._mgr = mgr
        if pipeline is None:
            pipeline = []
        self.pipeline = pipeline

    def _append(self, op, value):
        stage = {op: value}
        return Aggregate(self._mgr, self.pipeline + [stage])

    project = partialmethod(_append, '$project')
    match = partialmethod(_append, '$match')
    limit = partialmethod(_append, '$limit')
    skip = partialmethod(_append, '$skip')
    sort = partialmethod(_append, '$sort')
    geo_near = partialmethod(_append, '$geoNear')
    redact = partialmethod(_append, '$redact')
    unwind = partialmethod(_append, '$unwind')
    group = partialmethod(_append, '$group')
    sample = partialmethod(_append, '$sample')
    lookup = partialmethod(_append, '$lookup')
    index_stats = partialmethod(_append, '$indexStats')

    def out(self, collection_name):
        pipeline = self.pipeline + [{'$out': collection_name}]
        cursor = self._mgr.collection.aggregate(pipeline)
        return iter(Cursor(self._mgr, cursor))

    def get_cursor(self):
        pymongo_cursor = self._mgr.collection.aggregate(self.pipeline)

        # Wrap it in a barin cursor if the document structure is unmodified
        for stage in self.pipeline:
            if stage.keys()[0] in self.OPS_MODIFYING_DOC_STRUCTURE:
                return pymongo_cursor
        return Cursor(self._mgr, pymongo_cursor)
