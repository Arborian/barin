"""Schemas for simple BSON types."""
import pytz
from datetime import datetime

import bson

from .base import Schema, Invalid


class Scalar(Schema):
    _msgs = dict(
        none='Value cannot be None')

    def __init__(self, allow_none=False, **kwargs):
        super(Scalar, self).__init__(**kwargs)
        self.allow_none = allow_none

    def to_py(self, value):
        res = super(Scalar, self).to_py(value)
        if res is None:
            if self.allow_none:
                return res
            else:
                raise Invalid(self._msgs['none'], value)
        return self._validate(res)

    def to_db(self, value):
        res = super(Scalar, self).to_py(value)
        if res is None:
            if self.allow_none:
                return res
            else:
                raise Invalid(self._msgs['none'], value)
        return self._validate(res)

    def _validate(self, value):
        return value


class ObjectId(Scalar):
    _msgs = dict(
        Scalar._msgs,
        not_oid='Value must be an ObjectId')

    def _validate(self, value):
        if not isinstance(value, bson.ObjectId):
            raise Invalid(self._msgs['not_oid'], value)
        return value


class Number(Scalar):
    _msgs = dict(
        Scalar._msgs,
        not_num='Value must be an number')

    def _validate(self, value):
        if not isinstance(value, (int, long, float)):
            raise Invalid(self._msgs['not_num'], value)
        return value


class Integer(Number):
    _msgs = dict(
        Number._msgs,
        not_int='Value must be an integer')

    def _validate(self, value):
        value = super(Integer, self)._validate(value)
        if not isinstance(value, (int, long)):
            raise Invalid(self._msgs['not_int'], value)
        return value


class Float(Number):
    _msgs = dict(
        Number._msgs,
        not_float='Value must be a floating-point number')

    def _validate(self, value):
        value = super(Float, self)._validate(value)
        return float(value)


class Unicode(Scalar):
    _msgs = dict(
        Scalar._msgs,
        not_unicode='Value must be a Unicode string')

    def _validate(self, value):
        if not isinstance(value, unicode):
            raise Invalid(self._msgs['not_unicode'], value)
        return value


class DateTime(Scalar):
    _msgs = dict(
        Scalar._msgs,
        not_dt='Value must be a datetime object')

    def __init__(self, default_timezone=None, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        self.default_timezone = default_timezone

    def to_py(self, value):
        """Store only naive (assume UTC) datetimes."""
        res = super(DateTime, self).to_py(value)
        res = self._validate(res)
        if isinstance(res, datetime):
            tz = self._get_default_timezone()
            if tz is not None:
                res = res.replace(tzinfo=pytz.utc)
                res = res.astimezone(tz)
        return res

    def to_db(self, value):
        """Convert to a default datetime on egress from the DB."""
        res = super(DateTime, self).to_py(value)
        res = self._validate(res)
        if isinstance(res, datetime) and res.tzinfo:
            res = res.astimezone(pytz.utc)
            res = res.replace(tzinfo=None)
        return res

    def _validate(self, value):
        if value is None and self.allow_none:
            return value
        if not isinstance(value, datetime):
            raise Invalid(self._msgs['not_dt'], value)
        return value

    def _get_default_timezone(self):
        if callable(self.default):
            return self.default()
        return self.default
