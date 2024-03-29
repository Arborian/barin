"""Schemas for simple BSON types."""
import uuid
import logging
from datetime import datetime, date, time

import six
import pytz
import bson

from .base import Validator, Invalid

log = logging.getLogger(__name__)


class Scalar(Validator):
    _msgs = dict(Validator._msgs)


class ObjectId(Scalar):
    _msgs = dict(Scalar._msgs, not_oid="Value must be an ObjectId")

    def _validate(self, value, state=None):
        if not isinstance(value, bson.ObjectId):
            raise Invalid(self._msgs["not_oid"], value)
        return value


class DBRef(Scalar):
    _msgs = dict(Scalar._msgs, not_dbref="Value must be an DBRef")

    def _validate(self, value, state=None):
        if not isinstance(value, bson.dbref.DBRef):
            raise Invalid(self._msgs["not_dbref"], value)
        return value


class Number(Scalar):
    _msgs = dict(Scalar._msgs, not_num="Value must be an number")

    def _validate(self, value, state=None):
        if not isinstance(value, (six.integer_types, float)):
            raise Invalid(self._msgs["not_num"], value)
        return value


class Integer(Number):
    _msgs = dict(Number._msgs, not_int="Value must be an integer")

    def _validate(self, value, state=None):
        value = super(Integer, self)._validate(value, state)
        if not isinstance(value, six.integer_types):
            raise Invalid(self._msgs["not_int"], value)
        return value


class Float(Number):
    _msgs = dict(
        Number._msgs, not_float="Value must be a floating-point number"
    )

    def _validate(self, value, state=None):
        value = super(Float, self)._validate(value, state)
        return float(value)


class Unicode(Scalar):
    _msgs = dict(Scalar._msgs, not_unicode="Value must be a Unicode string")

    def _validate(self, value, state=None):
        if not isinstance(value, six.string_types):
            raise Invalid(self._msgs["not_unicode"], value)
        return value


class Binary(Scalar):
    _msgs = dict(
        Scalar._msgs, not_binary="Value must be a bson.Binary or bytes object"
    )

    def _validate(self, value, state=None):
        try:
            return bson.Binary(value)
        except TypeError:
            raise Invalid(self._msgs["not_binary"], value)
        return value


class DateTime(Scalar):
    _msgs = dict(Scalar._msgs, not_dt="Value must be a datetime object")

    def _validate(self, value, state=None):
        """Convert to a default datetime on egress from the DB."""
        res = super(DateTime, self)._validate(value, state)
        if not isinstance(res, datetime) and isinstance(res, date):
            res = datetime.combine(res, time.min)
        if not isinstance(res, datetime):
            raise Invalid(self._msgs["not_dt"], value)
        if res.tzinfo:
            res = res.astimezone(pytz.utc)
            res = res.replace(tzinfo=None)
        return res


class UUID(Scalar):
    _msgs = dict(Scalar._msgs, not_uuid="Value is not a valid UUID")

    def _validate(self, value, state=None):
        res = super()._validate(value, state)
        # Handle legacy non-uuid storage maybe?
        if isinstance(res, bson.Binary):
            try:
                res = uuid.UUID(bytes=res)
            except Exception as err:
                log.exception(f"Error converting bson.Binary to UUID: {err}")
                raise Invalid(self._msgs["not_uuid"], value)
        if not isinstance(res, uuid.UUID):
            raise Invalid(self._msgs["not_uuid"], value)
        return res
