from barin import query
from barin.adapter import adapter


class joined_property:
    """Descriptor that works with $lookup

    the __get__ method will return the
    "joined" object[s] if they are present
    in the dictionary, or call the getter
    method if they are not.
    """

    MISSING = object()

    def __init__(self, cname, many=False):
        self._cname = cname
        self._many = many
        self._owner = self._name = None
        self._fget = None

    def __set_name__(self, owner, name):
        self._owner = owner
        self._name = name

    def __call__(self, getter):
        self._fget = getter
        return self

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        value = obj.get(self._name, self.MISSING)
        cref = cls.m.metadata.cref(self._cname)
        if value is self.MISSING:
            value = self._fget(obj, cref)
        if value is None:
            return None
        if isinstance(value, query.Aggregate):
            return value
        adapt = adapter(cref.m)
        if self._many:
            return list(map(adapt, value))
        else:
            return adapt(value)


class relationship(joined_property):
    """joined_property that works with $lookups"""

    MISSING = object()

    def __init__(self, cname, many=False, **lookup_spec):
        super().__init__(cname, many)
        self._lookup_spec = lookup_spec
        self._joiner = None

    @classmethod
    def eq(cls, cname, local_field, foreign_field="_id", many=False, **kwargs):
        spec = {
            "localField": local_field,
            "foreignField": foreign_field,
            **kwargs,
        }
        self = cls(cname, many, **spec)
        if not kwargs:
            self._fget = self.simple_load
        return self

    def join(self, agg, *args, **kwargs):
        if self._joiner:
            return self._joiner(self, agg, *args, **kwargs)
        return self._join(agg, *args, **kwargs)

    def joiner(self, func):
        self._joiner = func
        return self

    def simple_load(self, obj, cref):
        local_field = self._lookup_spec["localField"]
        foreign_field = self._lookup_spec["foreignField"]
        local_value = dotted_getattr(obj, local_field)
        q = cref.m.aggregate.match({foreign_field: local_value})
        if self._many:
            return q
        else:
            return q.first()

    def _join(self, agg, **kwargs):
        """kwargs can override any part of the lookup spec"""
        many = kwargs.pop("many", self._many)
        spec = {
            "from": self._cname,
            "as": self._name,
            **self._lookup_spec,
            **kwargs,
        }
        spec = {k: v for k, v in spec.items() if v is not None}
        agg = agg.lookup(spec)
        if not many:
            agg = agg.unwind(
                {
                    "path": "$" + self._name,
                    "preserveNullAndEmptyArrays": True,
                }
            )
            agg = agg.add_fields(
                {self._name: {"$ifNull": ["$" + self._name, None]}}
            )
        return agg


def dotted_getattr(obj, path):
    for attr in path.split("."):
        obj = getattr(obj, attr)
    return obj
