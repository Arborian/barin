import six
from bson.son import SON

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
            return next(iter(self))
        except StopIteration:
            return None

    def one(self):
        it = iter(self)
        res = next(it)
        try:
            next(it)
        except StopIteration:
            return res
        raise ValueError("More than one result returned for one()")

    def count(self):
        filter_args = self._compile_query().get("filter", {})
        return self.get_cursor().collection.count_documents(filter=filter_args)

    def count_documents(self):
        return self.count()


class Query(_CursorSource):
    def __init__(self, mgr, pipeline=None):
        self._mgr = mgr
        if pipeline is None:
            pipeline = []
        self.pipeline = pipeline
        self._wrap_mgr("find")
        self._wrap_mgr("update_one")
        self._wrap_mgr("update_many")
        self._wrap_mgr("replace_one")
        self._wrap_mgr("delete_one")
        self._wrap_mgr("delete_many")
        self._wrap_mgr("find_one_and_update")
        self._wrap_mgr("find_one_and_replace")
        self._wrap_mgr("find_one_and_delete")

    def _append(self, op, value):
        stage = {op: value}
        return Query(self._mgr, self.pipeline + [stage])

    match = partialmethod(_append, "$match")
    limit = partialmethod(_append, "$limit")
    skip = partialmethod(_append, "$skip")
    geo_near = partialmethod(_append, "$geoNear")

    def get_cursor(self):
        return self._mgr.find(**self._compile_query())

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, six.string_types):
            sval = (key_or_list, direction)
        else:
            sval = key_or_list
        stage = {"$sort": sval}
        return Query(self._mgr, self.pipeline + [stage])

    def _wrap_mgr(self, name):
        def wrapper(*args, **kwargs):
            orig = getattr(self._mgr, name)
            qres = self._compile_query()
            if "sort" in qres:
                res = orig(qres["filter"], sort=qres["sort"], *args, **kwargs)
            else:
                res = orig(qres["filter"], *args, **kwargs)
            if "limit" in qres:
                res = res.limit(qres["limit"])
            if "skip" in qres:
                res = res.skip(qres["skip"])
            return res

        wrapper.__name__ = "wrapped_{}".format(name)
        setattr(self, name, wrapper)
        return wrapper

    def _compile_query(self):
        filters = []
        limits = []
        skips = []
        sorts = []
        for stage in self.pipeline:
            op, value = list(stage.items())[0]
            if op == "$match":
                filters.append(value)
            elif op == "$limit":
                limits.append(value)
            elif op == "$skip":
                skips.append(value)
            elif op == "$sort":
                sorts.append(value)
        result = dict(filter=mql.and_(*filters))
        if limits:
            result["limit"] = min(limits)
        if skips:
            result["skip"] = sum(skips)
        if sorts:
            result["sort"] = list(sorts)
        return result


class Aggregate(_CursorSource):
    def __init__(
        self, mgr, pipeline=None, raw=False, hint=None, collection=None
    ):
        self._mgr = mgr
        if pipeline is None:
            pipeline = []
        self.pipeline = pipeline
        self.raw = raw
        self._hint = hint
        if collection is None:
            collection = self._mgr.collection
        self.collection = collection

    def clone(self, **overrides):
        kwargs = dict(
            mgr=self._mgr,
            pipeline=self.pipeline,
            raw=self.raw,
            hint=self._hint,
            collection=self.collection,
        )
        kwargs.update(overrides)
        return Aggregate(**kwargs)

    def _append(self, op, value, raw=None):
        if raw is None:
            raw = self.raw
        stage = {op: value}
        return self.clone(pipeline=self.pipeline + [stage], raw=raw)

    def hint(self, index_name):
        return self.clone(hint=index_name)

    @property
    def m(self):
        return self._mgr

    @property
    def current(self):
        return _AggCurrent()

    c = current

    project = partialmethod(_append, "$project", raw=True)
    add_fields = partialmethod(_append, "$addFields", raw=True)
    match = partialmethod(_append, "$match")
    limit = partialmethod(_append, "$limit")
    skip = partialmethod(_append, "$skip")
    geo_near = partialmethod(_append, "$geoNear")
    redact = partialmethod(_append, "$redact", raw=True)
    unwind = partialmethod(_append, "$unwind", raw=True)
    group = partialmethod(_append, "$group", raw=True)
    sample = partialmethod(_append, "$sample")
    lookup = partialmethod(_append, "$lookup", raw=True)
    index_stats = partialmethod(_append, "$indexStats", raw=True)
    count = partialmethod(_append, "$count", raw=True)
    replace_root = partialmethod(_append, "$replaceRoot", raw=True)

    def text(self, search, **kwargs):
        """$text must always be part of the initial $match pipeline stage"""
        new_match = {"$text": {"$search": search, **kwargs}}
        for i, stage in enumerate(self.pipeline):
            if "$match" in stage:
                return self.clone(
                    pipeline=self.pipeline[:i]
                    + [{"$match": dict(stage["$match"], **new_match)}]
                    + self.pipeline[i + 1 :],
                )
        else:
            return self.match(new_match)

    def graph_lookup(
        self,
        startWith,
        connectFromField,
        connectToField,
        as_,
        from_=None,
        maxDepth=None,
        depthField=None,
        restrictSearchWithMatch=None,
    ):
        if from_ is None:
            from_ = self.collection.name
        args = {
            "from": from_,
            "startWith": startWith,
            "as": as_,
            "connectFromField": connectFromField,
            "connectToField": connectToField,
        }
        if maxDepth is not None:
            args["maxDepth"] = maxDepth
        if depthField is not None:
            args["depthField"] = depthField
        if restrictSearchWithMatch is not None:
            args["restrictSearchWithMatch"] = restrictSearchWithMatch
        stage = {"$graphLookup": args}
        return self.clone(pipeline=self.pipeline + [stage], raw=True)

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, six.string_types):
            sval = [(key_or_list, direction)]
        else:
            sval = key_or_list
        stage = {"$sort": SON(sval)}
        return self.clone(pipeline=self.pipeline + [stage])

    def out(self, collection_name):
        pipeline = self.pipeline + [{"$out": collection_name}]
        if self._hint:
            cursor = self.collection.aggregate(pipeline, hint=self._hint)
        else:
            cursor = self.collection.aggregate(pipeline)
        return iter(Cursor(self._mgr, cursor))

    def explain(self):
        kwargs = dict(
            pipeline=self.pipeline,
            explain=True,
        )
        if self._hint:
            kwargs["hint"] = self._hint
        return self._mgr.database.command(
            "aggregate", self.collection.name, **kwargs
        )

    def get_cursor(self):
        if self._hint:
            pymongo_cursor = self.collection.aggregate(
                self.pipeline, hint=self._hint
            )
        else:
            pymongo_cursor = self.collection.aggregate(self.pipeline)
        if self.raw:
            return pymongo_cursor
        else:
            return Cursor(self._mgr, pymongo_cursor)

    def conform(self, mgr):
        return self.clone(mgr=mgr, raw=False)

    def lookup_to_one(self, lookup_spec):
        self = self.lookup(lookup_spec)
        return self.unwind(
            {
                "path": "$" + lookup_spec["as"],
                "preserveNullAndEmptyArrays": True,
            }
        )

    def real_class(self):
        """Handles barin.cmap(...) classes"""
        return self._mgr.registry.by_class(self._mgr.cls).cls

    def join(self, name, *args, **kwargs):
        cls = self.real_class()
        attr = getattr(cls, name)
        return attr.join(self, *args, **kwargs)


class _AggCurrent(object):
    def __getitem__(self, name):
        return _AggName("$" + name)

    def __getattr__(self, name):
        return self[name]


class _AggName(six.text_type):
    def __getitem__(self, name):
        return _AggName(six.text_type(self) + "." + name)

    def __getattr__(self, name):
        return self[name]

    def _unop(self, op):
        return _AggOp(op, self)

    def _binop(self, op, other):
        return _AggOp(op, self, other)

    def _rbinop(self, op, other):
        return _AggOp(op, other, self)

    __abs__ = partialmethod(_unop, "$abs")
    __add__ = partialmethod(_binop, "$add")
    __sub__ = partialmethod(_binop, "$subtract")
    __mul__ = partialmethod(_binop, "$multiply")
    __div__ = partialmethod(_binop, "$divide")
    __mod__ = partialmethod(_binop, "$mod")
    __pow__ = partialmethod(_binop, "$pow")
    __radd__ = partialmethod(_rbinop, "$add")
    __rsub__ = partialmethod(_rbinop, "$subtract")
    __rmul__ = partialmethod(_rbinop, "$multiply")
    __rdiv__ = partialmethod(_rbinop, "$divide")
    __rmod__ = partialmethod(_rbinop, "$mod")
    __rpow__ = partialmethod(_rbinop, "$pow")

    ceil = partialmethod(_unop, "$ceil")
    exp = partialmethod(_unop, "$exp")
    floor = partialmethod(_unop, "$floor")
    ln = partialmethod(_unop, "$ln")
    log = partialmethod(_binop, "$ln")
    log10 = partialmethod(_unop, "$log10")
    sqrt = partialmethod(_unop, "$sqrt")
    trunc = partialmethod(_unop, "$trunc")

    concat = partialmethod(_binop, "$concat")
    lower = partialmethod(_unop, "$toLower")
    upper = partialmethod(_unop, "$toUpper")
    strcasecmp = partialmethod(_binop, "$strcasecmp")

    def substr(self, start, length):
        return _AggOp("$substr", self, start, length)


class _AggOp(dict):
    def __init__(self, op, *args):
        super(_AggOp, self).__init__({op: list(args)})
