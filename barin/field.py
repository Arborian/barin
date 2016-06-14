from . import mql
from . import schema as S
from .base import Document, partialmethod
from .util import reify


class FieldCollection(Document):

    def make_schema(self, metadata, **options):
        d_schema = dict((name, f.schema) for name, f in self.items())
        s_schema = S.compile_schema(metadata, d_schema, **options)
        return s_schema

    def bind_metadata(self, metadata):
        for fld in self.values():
            fld.bind_metadata(metadata)

    def __dir__(self):
        return self.keys()


class Field(object):

    def __init__(self, name, schema, **options):
        self.name = name
        self._schema = schema
        self.options = options
        self.metadata = None

    def bind_metadata(self, metadata):
        self.metadata = metadata

    @reify
    def schema(self):
        if isinstance(self._schema, type):
            if issubclass(self._schema, S.Validator):
                return self._schema(**self.options)
        return S.compile_schema(self.metadata, self._schema, **self.options)

    def __repr__(self):
        return '<Field {}: {}>'.format(self.name, self.schema)

    def __get__(self, inst, cls=None):
        if inst is None:
            return self
        return inst[self.name]

    def __set__(self, inst, value):
        inst[self.name] = self.schema.validate(value)

    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        subname = '{}.{}'.format(self.name, name)
        subfield = self.schema[name]
        if isinstance(subfield, Field):
            return Field(subname, subfield.schema)
        elif isinstance(subfield, S.Validator):
            return Field(subname, subfield)
        else:
            raise KeyError(name)

    # Update operators
    def _uop(self, op, value):
        return mql.Clause({op: {self.name: value}})

    inc = partialmethod(_uop, '$inc')
    mul = partialmethod(_uop, '$mul')
    rename = partialmethod(_uop, '$rename')
    set_on_insert = partialmethod(_uop, '$setOnInsert')
    set = partialmethod(_uop, '$set')
    unset = partialmethod(_uop, '$unset')
    min = partialmethod(_uop, '$min')
    max = partialmethod(_uop, '$max')
    current_date = partialmethod(_uop, '$currentDate')
    add_to_set = partialmethod(_uop, '$addToSet')
    pop = partialmethod(_uop, '$pop')
    pull_all = partialmethod(_uop, '$pullAll')
    pull = partialmethod(_uop, '$pull')
    push_all = partialmethod(_uop, '$pushAll')
    push = partialmethod(_uop, '$push')
    bit = partialmethod(_uop, '$bit')

    # Query operators
    def _qop(self, op, value):
        return mql.Clause({self.name: {op: value}})

    __eq__ = partialmethod(_qop, '$eq')
    __ne__ = partialmethod(_qop, '$ne')
    __gt__ = partialmethod(_qop, '$gt')
    __ge__ = partialmethod(_qop, '$gte')
    __lt__ = partialmethod(_qop, '$lt')
    __le__ = partialmethod(_qop, '$lte')
    in_ = partialmethod(_qop, '$in')
    nin = partialmethod(_qop, '$nin')
    exists = partialmethod(_qop, '$exists')
    type = partialmethod(_qop, '$type')
    where = partialmethod(_qop, '$where')
    geo_within = partialmethod(_qop, '$geoWithin')
    geo_intersects = partialmethod(_qop, '$geoIntersects')
    all = partialmethod(_qop, '$all')
    elem_match = partialmethod(_qop, '$elemMatch')
    size = partialmethod(_qop, '$size')
    bits_all_set = partialmethod(_qop, '$bitsAllSet')
    bits_any_set = partialmethod(_qop, '$bitsAnySet')
    bits_all_clear = partialmethod(_qop, '$bitsAllClear')
    bits_any_clear = partialmethod(_qop, '$bitsAnyClear')

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
