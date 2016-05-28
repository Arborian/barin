import bson
from unittest import TestCase

from barin import schema as S


class TestSchema(TestCase):

    def test_required_is_present(self):
        s = S.Validator(required=True)
        v = s.validate(5)
        self.assertEqual(v, 5)

    def test_required_is_missing(self):
        s = S.Validator(required=True)
        with self.assertRaises(S.Invalid):
            s.validate(S.Missing)

    def test_default_present(self):
        s = S.Validator(default=5)
        v = s.validate(10)
        self.assertEqual(v, 10)

    def test_default_callable_present(self):
        s = S.Validator(default=lambda: 5)
        v = s.validate(10)
        self.assertEqual(v, 10)

    def test_default_missing(self):
        s = S.Validator(default=5)
        v = s.validate(S.Missing)
        self.assertEqual(v, 5)

    def test_default_callable_missing(self):
        s = S.Validator(default=lambda: 5)
        v = s.validate(S.Missing)
        self.assertEqual(v, 5)


class TestScalar(TestCase):

    def test_scalar_ok(self):
        s = S.Scalar()
        v = s.validate(5)
        self.assertEqual(v, 5)

    def test_scalar_none_ok(self):
        s = S.Scalar(allow_none=True)
        v = s.validate(None)
        self.assertEqual(v, None)

    def test_scalar_none_fail_implicit(self):
        s = S.Scalar()
        with self.assertRaises(S.Invalid):
            s.validate(None)

    def test_scalar_none_fail_explicit(self):
        s = S.Scalar(allow_none=False)
        with self.assertRaises(S.Invalid):
            s.validate(None)


class TestObjectId(TestCase):

    def test_oid_ok(self):
        s = S.ObjectId()
        oid = bson.ObjectId()
        v = s.validate(oid)
        self.assertEqual(v, oid)

    def test_int_fail(self):
        s = S.ObjectId()
        with self.assertRaises(S.Invalid):
            s.validate('5')


class TestNumber(TestCase):

    def test_int_ok_int(self):
        s = S.Number()
        v = s.validate(5)
        self.assertEqual(v, 5)

    def test_num_ok_float(self):
        s = S.Number()
        v = s.validate(5.5)
        self.assertEqual(v, 5.5)

    def test_num_fail(self):
        s = S.Number()
        with self.assertRaises(S.Invalid):
            s.validate('5')


class TestInt(TestCase):

    def test_int_ok(self):
        s = S.Integer()
        v = s.validate(5)
        self.assertEqual(v, 5)

    def test_int_fail(self):
        s = S.Integer()
        with self.assertRaises(S.Invalid):
            s.validate('5')


class TestFloat(TestCase):

    def test_float_ok_int(self):
        s = S.Float()
        v = s.validate(5)
        self.assertEqual(v, 5)
        self.assertIsInstance(v, float)

    def test_float_ok_float(self):
        s = S.Float()
        v = s.validate(5)
        self.assertEqual(v, 5)

    def test_float_fail(self):
        s = S.Float()
        with self.assertRaises(S.Invalid):
            s.validate('5')


class TestUnicode(TestCase):

    def test_uni_ok(self):
        s = S.Unicode()
        uni = u'foo'
        v = s.validate(uni)
        self.assertEqual(v, uni)

    def test_uni_fail(self):
        s = S.Unicode()
        with self.assertRaises(S.Invalid):
            s.validate('5')


class TestDocument(TestCase):

    def test_doc_ok(self):
        s = S.Document(fields=dict(x=S.Integer()))
        val = {'x': 5}
        res = s.validate(val)
        self.assertEqual(res, val)

    def test_doc_notdoc_fail(self):
        s = S.Document(fields=dict(x=S.Integer()))
        val = 5
        with self.assertRaises(S.Invalid):
            s.validate(val)

    def test_doc_missing_fail(self):
        s = S.Document(fields=dict(x=S.Integer()))
        val = {}
        with self.assertRaises(S.Invalid) as err:
            s.validate(val)
        self.assertIsInstance(err.exception.document['x'], S.Invalid)

    def test_doc_missing_default(self):
        s = S.Document(fields=dict(x=S.Integer(default=None)))
        val = {}
        res = s.validate(val)
        self.assertEqual(res, {'x': None})

    def test_doc_partial(self):
        s = S.Document(fields=dict(x=S.Integer(), y=S.Integer()))
        val = {'x': 1, 'y': 'foo'}
        with self.assertRaises(S.Invalid) as err:
            s.validate(val)
        self.assertEqual(err.exception.document.keys(), ['y'])


class TestArray(TestCase):

    def test_arr_ok(self):
        s = S.Array(validator=S.Integer())
        val = [5]
        res = s.validate(val)
        self.assertEqual(res, val)

    def test_arr_notarr_fail(self):
        s = S.Array(validator=S.Integer())
        val = '5'
        with self.assertRaises(S.Invalid):
            s.validate(val)

    def test_arr_partial(self):
        s = S.Array(validator=S.Integer())
        val = [1, 2, 3, 'foo']
        with self.assertRaises(S.Invalid) as err:
            s.validate(val)
        self.assertEqual(len(err.exception.array), len(val))
        self.assertEqual(err.exception.array[:3], [None] * 3)

    def test_arr_only_begin(self):
        s = S.Array(validator=S.Integer(), only_validate=slice(None, 2))
        val = [1, 2, 3, 'foo']
        res = s.validate(val)
        self.assertEqual(val, res)
