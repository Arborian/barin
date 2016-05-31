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
            assert isinstance(part, dict), 'Illegal clause {}'.format(part)
            if cur is None:
                cur = dict(part)
                continue
            cur = {op: [cur, part]}
        return cur
    inner.__name__ = op
    return inner


and_ = _logical_nary_op('$and')
or_ = _logical_nary_op('$or')
nor_ = _logical_nary_op('$nor')
