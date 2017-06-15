"""Shorthand compiler for schemas."""
from datetime import datetime
import six


def compile_schema(metadata, s, **options):
    from barin import schema as S
    from barin import manager
    if s is None:
        return S.Anything()
    if isinstance(s, S.Validator):
        return s
    elif isinstance(s, six.string_types):
        return metadata[s].__barin__
    elif hasattr(s, '__barin__'):
        return s.__barin__
    elif isinstance(s, manager.ClassManager):
        return s
    elif isinstance(s, list):
        if len(s) == 1:
            schema = compile_schema(metadata, s[0])
            return S.Array(validator=schema, **options)
        elif len(s) == 0:
            return S.Array(**options)
        else:
            raise S.Invalid('Invalid schema {}'.format(s))
    elif isinstance(s, dict):
        fields = dict(
            (name, compile_schema(metadata, value))
            for name, value in s.items())
        extra_validator = fields.pop(str, S.Missing)
        return S.Document(
            fields=fields,
            extra_validator=extra_validator,
            **options)
    elif not isinstance(s, type):
        raise S.Invalid('Invalid schema {}'.format(s), s)
    elif issubclass(s, S.Validator):
        return s(**options)
    elif issubclass(s, six.integer_types):
        return S.Integer(**options)
    elif issubclass(s, datetime):
        return S.DateTime(**options)
    elif issubclass(s, float):
        return S.Float(**options)
    elif issubclass(s, six.string_types):
        return S.Unicode(**options)
    else:
        raise S.Invalid('Invalid schema {}'.format(s), s)
