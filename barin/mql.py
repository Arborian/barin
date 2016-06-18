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
        for part in parts:
            if not isinstance(part, dict):
                raise errors.QueryError(
                    'Illegal {} clause: {}'.format(op, part))
        return {op: list(parts)}
    inner.__name__ = op
    return inner


def and_(*parts):
    """Try to build a compound document without using $and."""
    result = {}
    for part in parts:
        if not isinstance(part, dict):
            raise errors.QueryError(
                'Illegal $and clause: {}'.format(part))
        for k, v in part.items():
            if isinstance(v, dict):
                r_v = result.setdefault(k, {})
                if set(v.keys()).intersection(r_v):
                    raise errors.ConflictError(
                        'Conflict for {}: {} and {}'.format(
                            k, r_v, v))
                r_v.update(v)
            elif k in result:
                raise errors.ConflictError(
                    'Conflict for {}: {} and {}'.format(
                        k, result[k], v))
            else:
                result[k] = v
    return result

# and_ = _logical_nary_op('$and')
or_ = _logical_nary_op('$or')
nor_ = _logical_nary_op('$nor')
