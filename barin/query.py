from itertools import chain

import six

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
        try:
            return iter(self).next()
        except StopIteration:
            return None

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
        self._wrap_mgr('delete_one')
        self._wrap_mgr('delete_many')
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
        if isinstance(key_or_list, six.string_types):
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
            if 'limit' in qres:
                res = res.limit(qres['limit'])
            if 'skip' in qres:
                res = res.skip(qres['skip'])
            if 'sort' in qres:
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
            op, value = list(stage.items())[0]
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

    @property
    def current(self):
        return _AggCurrent()

    c = current

    project = partialmethod(_append, '$project')
    match = partialmethod(_append, '$match')
    limit = partialmethod(_append, '$limit')
    skip = partialmethod(_append, '$skip')
    geo_near = partialmethod(_append, '$geoNear')
    redact = partialmethod(_append, '$redact')
    unwind = partialmethod(_append, '$unwind')
    group = partialmethod(_append, '$group')
    sample = partialmethod(_append, '$sample')
    lookup = partialmethod(_append, '$lookup')
    index_stats = partialmethod(_append, '$indexStats')

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, six.string_types):
            sval = [(key_or_list, direction)]
        else:
            sval = key_or_list
        stage = {'$sort': sval}
        return Aggregate(self._mgr, self.pipeline + [stage])

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


class _AggCurrent(object):

    def __getitem__(self, name):
        return _AggName('$' + name)

    def __getattr__(self, name):
        return self[name]


class _AggName(six.text_type):

    def __init__(self, name):
        super(_AggName, self).__init__(name)

    def __getitem__(self, name):
        return _AggName(six.text_type(self) + '.' + name)

    def __getattr__(self, name):
        return self[name]

    def _unop(self, op):
        return _AggOp(op, self)

    def _binop(self, op, other):
        return _AggOp(op, self, other)

    def _rbinop(self, op, other):
        return _AggOp(op, other, self)

    __abs__ = partialmethod(_unop, '$abs')
    __add__ = partialmethod(_binop, '$add')
    __sub__ = partialmethod(_binop, '$subtract')
    __mul__ = partialmethod(_binop, '$multiply')
    __div__ = partialmethod(_binop, '$divide')
    __mod__ = partialmethod(_binop, '$mod')
    __pow__ = partialmethod(_binop, '$pow')
    __radd__ = partialmethod(_rbinop, '$add')
    __rsub__ = partialmethod(_rbinop, '$subtract')
    __rmul__ = partialmethod(_rbinop, '$multiply')
    __rdiv__ = partialmethod(_rbinop, '$divide')
    __rmod__ = partialmethod(_rbinop, '$mod')
    __rpow__ = partialmethod(_rbinop, '$pow')

    ceil = partialmethod(_unop, '$ceil')
    exp = partialmethod(_unop, '$exp')
    floor = partialmethod(_unop, '$floor')
    ln = partialmethod(_unop, '$ln')
    log = partialmethod(_binop, '$ln')
    log10 = partialmethod(_unop, '$log10')
    sqrt = partialmethod(_unop, '$sqrt')
    trunc = partialmethod(_unop, '$trunc')

    concat = partialmethod(_binop, '$concat')
    lower = partialmethod(_unop, '$toLower')
    upper = partialmethod(_unop, '$toUpper')
    strcasecmp = partialmethod(_binop, '$strcasecmp')

    def substr(self, start, length):
        return _AggOp('$substr', self, start, length)


class _AggOp(dict):

    def __init__(self, op, *args):
        super(_AggOp, self).__init__({op: list(args)})
