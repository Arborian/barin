from . import errors


class Clause(dict):

    def __and__(self, other):
        return and_(self, other)

    def __or__(self, other):
        return or_(self, other)

    def __invert__(self):
        return not_(self)


def not_(value):
    result = {}
    for k, v in value.items():
        if not isinstance(v, dict):
            v = {'$eq': v}
        result[k] = {'$not': v}
    return result


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
    result = Clause()
    or_clauses = []
    for part in parts:
        if not isinstance(part, dict):
            raise errors.QueryError(
                'Illegal $and clause: {}'.format(part))
        for k, v in part.items():
            if k == '$or':
                or_clauses.append({k: v})
            elif isinstance(v, dict):
                r_v = result.setdefault(k, {})
                if not isinstance(r_v, dict):
                    r_v = result[k] = {'$eq': r_v}
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
    if or_clauses:
        result = {'$and': [result] + or_clauses}
    return result

# and_ = _logical_nary_op('$and')
or_ = _logical_nary_op('$or')
nor_ = _logical_nary_op('$nor')
