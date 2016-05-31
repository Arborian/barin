from functools import partial

from . import schema as S
from .mql import Clause


class Field(object):

    def __init__(self, name, schema, **options):
        self.name = name
        self.schema = S.compile_schema(schema, **options)
        self.options = options

    def __get__(self, obj, cls=None):
        if obj is None:
            return ClassField(self, cls)
        else:
            return InstanceField(self, obj)


class ClassField(object):

    def __init__(self, field, cls):
        self.field = field
        self.cls = cls

    def __getattr__(self, name):
        return getattr(self.cls, name)

    # Query operators
    def _op(self, op, value):
        return Clause({op: value})

    __eq__ = partial(_op, '$eq')
    __ne__ = partial(_op, '$ne')
    __gt__ = partial(_op, '$gt')
    __ge__ = partial(_op, '$gte')
    __lt__ = partial(_op, '$lt')
    __le__ = partial(_op, '$lte')
    in_ = partial(_op, '$in')
    nin = partial(_op, '$nin')
    exists = partial(_op, '$exists')
    type = partial(_op, '$type')
    where = partial(_op, '$where')
    geo_within = partial(_op, '$geoWithin')
    geo_intersects = partial(_op, '$geoIntersects')
    all = partial(_op, '$all')
    elem_match = partial(_op, '$elemMatch')
    size = partial(_op, '$size')
    bits_all_set = partial(_op, '$bitsAllSet')
    bits_any_set = partial(_op, '$bitsAnySet')
    bits_all_clear = partial(_op, '$bitsAllClear')
    bits_any_clear = partial(_op, '$bitsAnyClear')

    def mod(self, divisor, remainder):
        return Clause({'$mod': [divisor, remainder]})

    def regex(self, regex, options=None):
        spec = {'$regex': regex}
        if options:
            spec['$options'] = options
        return Clause(spec)

    def text(
            self, search,
            language=None, case_sensitive=False, diacritic_sensitive=False):
        spec = {'$search': search}
        if language:
            spec['$language'] = language
        if case_sensitive:
            spec['$caseSensitive'] = case_sensitive
        if diacritic_sensitive:
            spec['$diacriticSensitive'] = diacritic_sensitive
        return Clause(spec)

    def near(self, value, min_distance=None, max_distance=None):
        spec = {'$near': value}
        if min_distance is not None:
            spec['$minDistance'] = min_distance
        if max_distance is not None:
            spec['$max_distance'] = max_distance
        return Clause(spec)

    def near_sphere(self, value, min_distance=None, max_distance=None):
        spec = {'$nearSphere': value}
        if min_distance is not None:
            spec['$minDistance'] = min_distance
        if max_distance is not None:
            spec['$max_distance'] = max_distance
        return Clause(spec)


class InstanceField(object):

    def __init__(self, field, obj):
        self.field = field
        self.obj = obj

    def __getattr__(self, name):
        return getattr(self.obj, name)
