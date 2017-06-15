"""Schemas for compound types (Documents and Arrays)."""
import six

from barin.base import Document as BaseDocument
from barin.schema.base import Validator, Invalid, Missing


class Document(Validator):
    _msgs = dict(
        Validator._msgs,
        not_doc='Value must be a document',
        extra='Field has no validator')

    def __init__(
            self,
            fields=Missing,
            allow_extra=False,
            extra_validator=Missing,
            as_class=BaseDocument,
            **kwargs):
        if not kwargs.setdefault('required', False):
            kwargs.setdefault('default', lambda: {})
        super(Document, self).__init__(**kwargs)
        if fields is Missing:
            fields = {}
        if extra_validator is not Missing:
            allow_extra = True
        self.fields = dict(fields)
        self.allow_extra = allow_extra
        self.extra_validator = extra_validator
        self.as_class = as_class

    def __repr__(self):
        parts = [self.__class__.__name__]
        if self.required:
            parts.append('required')
        if self.allow_none:
            parts.append('nullable')
        if self.default is not Missing:
            parts.append('default={}'.format(self.default))
        if self.as_class is not Missing:
            parts.append('as_class={}'.format(self.as_class))
        return '<{}>'.format(' '.join(parts))

    def __getitem__(self, name):
        return self.fields[name]

    def _validate(self, value, state=None):
        if not isinstance(value, dict):
            raise Invalid(self._msgs['not_doc'], value)
        validated = {}
        errors = {}

        # Validate by explicitly specified fields
        for name, validator in self.fields.items():
            r_val = value.get(name, Missing)
            try:
                v_val = validator.validate(r_val, state)
                validated[name] = v_val
            except Invalid as err:
                errors[name] = err

        # Validate unknown keys ('extra fields')
        for name, r_val in value.items():
            if name in self.fields:
                continue
            if not self.allow_extra:
                errors[name] = Invalid(self._msgs['extra'], r_val)
            elif self.extra_validator:
                try:
                    v_val = self.extra_validator.validate(r_val, state)
                    validated[name] = v_val
                except Invalid as err:
                    errors[name] = err
            else:
                validated[name] = r_val

        if errors:
            raise Invalid('', value, document=errors)
        return self.as_class(validated)


class Array(Validator):
    _msgs = dict(
        Validator._msgs,
        not_arr='Value must be an array')

    def __init__(
            self,
            validator=Missing,
            only_validate=Missing,
            **kwargs):
        kwargs.setdefault('default', list)
        super(Array, self).__init__(**kwargs)
        self.validator = validator
        if isinstance(only_validate, slice):
            only_validate = [only_validate]
        elif only_validate is Missing:
            only_validate = [slice(None)]
        self.only_validate = only_validate

    def __getitem__(self, name):
        return self.validator

    def _validate_indices(self, length):
        seen = set()
        for sl in self.only_validate:
            indices = six.moves.range(*sl.indices(length))
            for ix in indices:
                if ix in seen:
                    continue
                yield ix
                seen.add(ix)

    def _validate(self, value, state=None):
        if not isinstance(value, list):
            raise Invalid(self._msgs['not_arr'], value)
        if self.validator is Missing:
            return value
        validated = list(value)
        errors = [None] * len(value)
        has_errors = False

        for ix in self._validate_indices(len(value)):
            r_val = value[ix]
            try:
                v_val = self.validator.validate(r_val, state)
                validated[ix] = v_val
            except Invalid as err:
                errors[ix] = err
                has_errors = True

        if has_errors:
            raise Invalid('', value, array=errors)

        return validated
