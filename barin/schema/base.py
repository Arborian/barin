"""Base classes for schemas."""


class _Missing(tuple):
    pass

Missing = _Missing()


class Invalid(Exception):

    def __init__(self, msg, value, array=None, object=None):
        self.msg = msg
        self.value = value
        self.array = array
        self.object = object


class Schema(object):

    def __init__(
            self,
            default=Missing,
            required=Missing,
            required_py=Missing,
            required_db=Missing):
        self.default = default
        if required is Missing:
            self.required_py = self.required_db = False
        else:
            self.required_py = self.required_db = required
        if required_py is not Missing:
            self.required_py = required_py
        if required_db is not Missing:
            self.required_db = required_db

    def to_py(self, value):
        if value is Missing:
            value = self._get_default()
        if value is Missing and self.required_db:
            raise Invalid('Missing', value)
        return value

    def to_db(self, value):
        if value is Missing:
            value = self._get_default()
        if value is Missing and self.required_py:
            raise Invalid('Missing', value)
        return value

    def _get_default(self):
        if callable(self.default):
            return self.default()
        return self.default
