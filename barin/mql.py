from . import errors


class Clause(dict):

    def __and__(self, other):
        return and_(self, other)

    def __or__(self, other):
        return or_(self, other)

    def __invert__(self):
        return not_(self)


def not_(value):
    return Clause({'$not': value})


def comment(value):
    return Clause({'$comment': value})


def _logical_nary_op(op):
    def inner(*parts):
        cur = None
        for part in parts:
            if not isinstance(part, dict):
                raise errors.QueryError(
                    'Illegal {} clause: {}'.format(op, part))
            if cur is None:
                cur = dict(part)
                continue
            cur = {op: [cur, part]}
        return cur
    inner.__name__ = op
    return inner


def and_(*parts):
    """Try to build a compound document without using $and."""
    result = None
    for part in parts:
        if not isinstance(part, dict):
            raise errors.QueryError(
                'Illegal $and clause: {}'.format(part))
        if result is None:
            result = part
            continue
        for k, v in part.items():
            r_v = result.setdefault(k, {})
            if set(v.keys()).intersection(r_v):
                raise errors.ConflictError(
                    'Conflict for {}: {} and {}'.format(
                        k, r_v, v))
            r_v.update(v)
    return result

# and_ = _logical_nary_op('$and')
or_ = _logical_nary_op('$or')
nor_ = _logical_nary_op('$nor')
