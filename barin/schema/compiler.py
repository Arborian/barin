"""Shorthand compiler for schemas."""
from datetime import datetime


def compile_schema(s):
    from barin import schema as S
    if isinstance(s, S.Validator):
        return s
    elif isinstance(s, list):
        if len(s) == 1:
            schema = compile_schema(s[0])
            return S.Array(validator=schema)
        elif len(s) == 0:
            return S.Array()
        else:
            raise S.Invalid('Invalid schema {}'.format(s))
    elif isinstance(s, dict):
        fields = dict(
            (name, compile_schema(value))
            for name, value in s.items())
        extra_validator = fields.pop(str, S.Missing)
        return S.Document(fields=fields, extra_validator=extra_validator)
    elif issubclass(s, (int, long)):
        return S.Integer()
    elif issubclass(s, datetime):
        return S.DateTime()
    elif issubclass(s, float):
        return S.Float()
    elif issubclass(s, basestring):
        return S.Unicode()
    else:
        raise S.Invalid('Invalid schema {}'.format(s))
