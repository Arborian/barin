import bson
import struct
from collections import defaultdict
from marisa_trie import Trie
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


class PathTrie(object):

    def __init__(self, paths=None):
        self._next = defaultdict(PathTrie)
        self._terminals = set()
        if paths:
            for p in paths:
                self.add(p)

    def add(self, path):
        if len(path) == 1:
            self._terminals.add(path[0])
        else:
            next = self._next[path[0]]
            next.add(path[1:])

    def get(self, part):
        return self._next.get(part, None)

    def __contains__(self, part):
        return part in self._terminals

class _Sentry(object): pass
Unknown = _Sentry()
Missing = _Sentry()


class RawDoc(object):  # bson.raw_bson.RawBSONDocument):
    _type_marker = bson.raw_bson._RAW_BSON_DOCUMENT_MARKER

    def __init__(
            self,
            bson_bytes,
            codec_options=bson.codec_options.DEFAULT_CODEC_OPTIONS):
        self._raw = bson_bytes
        self._codec_options = codec_options
        self._cache = {}
        self._pos = 4

    @reify
    def _obj_end(self):
        obj_size = bson._UNPACK_INT(self._raw[:4])[0]
        return obj_size - 1

    def __repr__(self):
        if self._cache:
            return '<RawDoc {} ({}% decoded)>'.format(
                self._cache, 100 * self._pos / self._obj_end)
        else:
            return '<RawDoc {}>'.format(repr(self._raw))

    def get(self, key, default=None):
        'D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.'
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, name):
        # Try to lookup in cache
        value = self._cache.get(name, Unknown)
        if value is Missing:
            raise KeyError(name)
        elif value is not Unknown:
            return value
        opts = self._codec_options
        e2d = bson._element_to_dict
        raw = self._raw
        obj_end = self._obj_end
        while self._pos < obj_end:
            ename, value, self._pos = e2d(raw, self._pos, obj_end, opts)
            self._cache[ename] = value
            if ename == name:
                return value
        self._cache[name] = Missing
        raise KeyError(name)


def getval(data, opts, name):
    obj_size = bson._UNPACK_INT(data[:4])[0]
    obj_end = obj_size - 1
    pos = 4
    while pos < obj_size - 1:
        ename, value, pos = bson._element_to_dict(data, pos, obj_end, opts)
        if ename == name:
            return value
    raise KeyError(name)


def iter_records(curs, *names):
    opts = bson.CodecOptions(document_class=bson.raw_bson.RawBSONDocument)
    trie = PathTrie([n.split('.') for n in names])
    template = dict((n, None) for n in names)
    for doc in curs:
        rec = dict(template)
        for k, v in get_fields(doc.raw, opts, trie):
            rec['.'.join(k)] = v
        yield tuple(rec[n] for n in names)


def get_fields(data, opts, trie):
    """Return list of (name, data) tuples where name is in the trie."""
    obj_size = bson._UNPACK_INT(data[:4])[0]
    obj_end = obj_size - 1
    pos = 4
    result = []
    while pos < obj_size - 1:
        etype = data[pos]
        pos += 1
        ename, pos = _get_c_string(data, pos)
        value, pos = _ELEMENT_GETTER[etype](
            data, pos, obj_end, opts, ename)
        if ename in trie:
            result.append(([ename], value))
            continue
        new_trie = trie.get(ename)
        if new_trie:
            result += [
                ([ename] + key, v)
                for key, v in get_fields(value, opts, new_trie)
            ]
    return result


def getvals(data, opts):
    import ipdb; ipdb.set_trace();
    obj_size = bson._UNPACK_INT(data[:4])[0]
    obj_end = obj_size - 1
    pos = 4
    while pos < obj_size - 1:
        ename, value, pos = bson._element_to_dict(data, pos, obj_end, opts)
        yield ename, value



def iter_elements(data, recurse=True):
    opts = bson.CodecOptions(document_class=bson.raw_bson.RawBSONDocument)
    for kpath, val in _iter_elements(data, opts, recurse):
        yield '.'.join(kpath), val


class Recurser(object):

    def __init__(self, trie, prefix=''):
        self._trie = trie
        self._prefix = prefix

    def __contains__(self, name):
        path = '.'.join([self._prefix, name, ''])
        return self._trie.has_keys_with_prefix(path)

    def __call__(self, name):
        return Trie(self._trie, self._prefix + name)

    @classmethod
    def from_names(cls, names):
        trie = Trie([n + '.' for n in names])
        return cls(trie)


def _iter_elements(data, opts, recurser=None):
    obj_size = bson._UNPACK_INT(data[:4])[0]
    obj_end = obj_size - 1
    pos = 4
    while pos < obj_size - 1:
        etype = data[pos]
        pos += 1
        ename, pos = _get_c_string(data, pos)
        value, pos = _ELEMENT_GETTER[etype](
            data, pos, obj_end, opts, ename)
        if recurser and etype == bson.BSONOBJ:
            if ename in recurser:
                for k, v in _iter_elements(value, opts, recurser(ename)):
                    yield [ename] + k, v
            else:
                yield [ename], value
        else:
            yield [ename], value


def _get_c_string(data, position):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return data[position:end].decode('utf8'), end + 1


def _get_string(data, position, obj_end, opts, dummy):
    """Decode a BSON string to python unicode string."""
    length = bson._UNPACK_INT(data[position:position + 4])[0]
    position += 4
    if length < 1 or obj_end - position < length:
        raise bson.InvalidBSON("invalid string length")
    end = position + length - 1
    if data[end:end + 1] != b"\x00":
        raise bson.InvalidBSON("invalid end of string")
    return data[position:end].decode('utf8'), end + 1


def _get_object(data, position, obj_end, opts, dummy):
    """Decode a BSON subdocument to opts.document_class or bson.dbref.DBRef."""
    obj_size = bson._UNPACK_INT(data[position:position + 4])[0]
    end = position + obj_size - 1
    if data[end:position + obj_size] != b"\x00":
        raise bson.InvalidBSON("bad eoo")
    if end >= obj_end:
        raise bson.InvalidBSON("invalid object length")
    return data[position:end + 1], end + 1


_ELEMENT_GETTER = dict(bson._ELEMENT_GETTER)
_ELEMENT_GETTER[bson.BSONOBJ] = _get_object
_ELEMENT_GETTER[bson.BSONSTR] = _get_string
_ELEMENT_GETTER[bson.BSONSYM] = _get_string



def _element_to_dict(data, position, obj_end, opts):
    """Decode a single key, value pair."""
    element_type = data[position:position + 1]
    position += 1
    element_name, position = bson._get_c_string(data, position, opts)
    try:
        value, position = bson._ELEMENT_GETTER[element_type](
            data, position, obj_end, opts, element_name)
    except KeyError:
        bson._raise_unknown_type(element_type, element_name)
    return element_name, value, position


def _bson_to_dict(data, opts):
    """Decode a BSON string to document_class."""
    try:
        obj_size = _UNPACK_INT(data[:4])[0]
    except struct.error as exc:
        raise InvalidBSON(str(exc))
    if obj_size != len(data):
        raise InvalidBSON("invalid object size")
    if data[obj_size - 1:obj_size] != b"\x00":
        raise InvalidBSON("bad eoo")
    try:
        if _raw_document_class(opts.document_class):
            return opts.document_class(data, opts)
        return _elements_to_dict(data, 4, obj_size - 1, opts)
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)



