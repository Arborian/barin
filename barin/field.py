from . import mql
from . import schema as S
from .base import Document, partialmethod


class FieldCollection(Document):

    def make_schema(self, **options):
        d_schema = dict((name, f.schema) for name, f in self.items())
        s_schema = S.compile_schema(d_schema, **options)
        return s_schema

    def __dir__(self):
        return self.keys()


class Field(object):

    def __init__(self, name, schema, **options):
        self.name = name
        if isinstance(schema, type) and issubclass(schema, S.Validator):
            self.schema = schema(**options)
        else:
            self.schema = S.compile_schema(schema, **options)
        self.options = options

    def __repr__(self):
        return '<Field {}: {}>'.format(self.name, self.schema)

    # Query operators
    def _op(self, op, value):
        return mql.Clause({self.name: {op: value}})

    __eq__ = partialmethod(_op, '$eq')
    __ne__ = partialmethod(_op, '$ne')
    __gt__ = partialmethod(_op, '$gt')
    __ge__ = partialmethod(_op, '$gte')
    __lt__ = partialmethod(_op, '$lt')
    __le__ = partialmethod(_op, '$lte')
    in_ = partialmethod(_op, '$in')
    nin = partialmethod(_op, '$nin')
    exists = partialmethod(_op, '$exists')
    type = partialmethod(_op, '$type')
    where = partialmethod(_op, '$where')
    geo_within = partialmethod(_op, '$geoWithin')
    geo_intersects = partialmethod(_op, '$geoIntersects')
    all = partialmethod(_op, '$all')
    elem_match = partialmethod(_op, '$elemMatch')
    size = partialmethod(_op, '$size')
    bits_all_set = partialmethod(_op, '$bitsAllSet')
    bits_any_set = partialmethod(_op, '$bitsAnySet')
    bits_all_clear = partialmethod(_op, '$bitsAllClear')
    bits_any_clear = partialmethod(_op, '$bitsAnyClear')

    def mod(self, divisor, remainder):
        return mql.Clause(
            {self.name: {'$mod': [divisor, remainder]}})

    def regex(self, regex, options=None):
        spec = {'$regex': regex}
        if options is not None:
            spec['$options'] = options
        return mql.Clause({self.name: spec})

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
        return mql.Clause({self.name: spec})

    def near(self, value, min_distance=None, max_distance=None):
        spec = {'$near': value}
        if min_distance is not None:
            spec['$minDistance'] = min_distance
        if max_distance is not None:
            spec['$maxDistance'] = max_distance
        return mql.Clause({self.name: spec})

    def near_sphere(self, value, min_distance=None, max_distance=None):
        spec = {'$nearSphere': value}
        if min_distance is not None:
            spec['$minDistance'] = min_distance
        if max_distance is not None:
            spec['$maxDistance'] = max_distance
        return mql.Clause({self.name: spec})
