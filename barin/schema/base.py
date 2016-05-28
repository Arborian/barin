"""Base classes for schemas."""


class _Missing(tuple):
    pass

Missing = _Missing()


class Invalid(Exception):

    def __init__(self, msg, value, array=None, document=None):
        self.msg = msg
        self.value = value
        self.array = array
        self.document = document

    def __repr__(self):
        parts = [
            'Invalid({}, {}'.format(
                repr(self.msg), repr(self.value))]
        if self.array is not None:
            part = ', '.join(map(repr, self.array))
            parts.append('array=[{}]'.format(part))
        if self.document is not None:
            part = ', '.join(
                '{}={}'.format(k, repr(v))
                for k, v in self.document.items())
            parts.append('document={' + part + '}')
        return ', '.join(parts) + ')'


class Validator(object):
    _msgs = dict(
        missing='Value cannot be missing',
        none='Value cannot be None')

    def __init__(
            self,
            allow_none=Missing,
            default=Missing,
            required=True):
        self.default = default
        self.required = required
        if allow_none is Missing:
            if self.default is None:
                self.allow_none = True
            else:
                self.allow_none = False
        else:
            self.allow_none = allow_none

    def validate(self, value, state=None):
        if value is Missing:
            value = self._get_default()
        if value is None:
            if self.allow_none:
                return value
            else:
                raise Invalid(self._msgs['none'], value)
        if value is Missing and self.required:
            raise Invalid(self._msgs['missing'], value)
        return self._validate(value, state)

    def _validate(self, value, state=None):
        return value

    def _get_default(self):
        if callable(self.default):
            return self.default()
        return self.default


class Anything(Validator):
    pass
