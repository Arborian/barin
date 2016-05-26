import bson
from unittest import TestCase

from barin import schema as S


class TestSchema(TestCase):

    def test_required_is_present_to_py(self):
        s = S.Schema(required=True)
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_required_is_present_to_db(self):
        s = S.Schema(required=True)
        v = s.to_db(5)
        self.assertEqual(v, 5)

    def test_required_is_missing_to_py(self):
        s = S.Schema(required=True)
        with self.assertRaises(S.Invalid):
            s.to_py(S.Missing)

    def test_required_is_missing_to_db(self):
        s = S.Schema(required=True)
        with self.assertRaises(S.Invalid):
            s.to_py(S.Missing)

    def test_required_py_is_present_to_py(self):
        s = S.Schema(required_py=True)
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_required_py_is_present_to_db(self):
        s = S.Schema(required_py=True)
        v = s.to_db(5)
        self.assertEqual(v, 5)

    def test_required_py_is_missing_to_py(self):
        s = S.Schema(required_py=True)
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_required_py_is_missing_to_db(self):
        s = S.Schema(required_py=True)
        with self.assertRaises(S.Invalid):
            s.to_db(S.Missing)

    def test_required_db_is_present_to_py(self):
        s = S.Schema(required_db=True)
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_required_db_is_present_to_db(self):
        s = S.Schema(required_db=True)
        v = s.to_db(5)
        self.assertEqual(v, 5)

    def test_required_db_is_missing_to_py(self):
        s = S.Schema(required_db=True)
        with self.assertRaises(S.Invalid):
            s.to_py(S.Missing)

    def test_required_db_is_missing_to_db(self):
        s = S.Schema(required_db=True)
        v = s.to_db(5)
        self.assertEqual(v, 5)

    def test_default_present_to_py(self):
        s = S.Schema(default=5)
        v = s.to_py(10)
        self.assertEqual(v, 10)

    def test_default_callable_present_to_py(self):
        s = S.Schema(default=lambda: 5)
        v = s.to_py(10)
        self.assertEqual(v, 10)

    def test_default_present_to_db(self):
        s = S.Schema(default=5)
        v = s.to_db(10)
        self.assertEqual(v, 10)

    def test_default_callable_present_to_db(self):
        s = S.Schema(default=lambda: 5)
        v = s.to_db(10)
        self.assertEqual(v, 10)

    def test_default_missing_to_py(self):
        s = S.Schema(default=5)
        v = s.to_py(S.Missing)
        self.assertEqual(v, 5)

    def test_default_callable_missing_to_py(self):
        s = S.Schema(default=lambda: 5)
        v = s.to_py(S.Missing)
        self.assertEqual(v, 5)

    def test_default_missing_to_db(self):
        s = S.Schema(default=5)
        v = s.to_db(S.Missing)
        self.assertEqual(v, 5)

    def test_default_callable_missing_to_db(self):
        s = S.Schema(default=lambda: 5)
        v = s.to_db(S.Missing)
        self.assertEqual(v, 5)

    def test_default_missing_required_to_db(self):
        s = S.Schema(default=5, required=True)
        v = s.to_db(S.Missing)
        self.assertEqual(v, 5)


class TestScalar(TestCase):

    def test_scalar_to_py_ok(self):
        s = S.Scalar()
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_scalar_to_py_none_ok(self):
        s = S.Scalar(allow_none=True)
        v = s.to_py(None)
        self.assertEqual(v, None)

    def test_scalar_to_py_none_fail_implicit(self):
        s = S.Scalar()
        with self.assertRaises(S.Invalid):
            s.to_py(None)

    def test_scalar_to_py_none_fail_explicit(self):
        s = S.Scalar(allow_none=False)
        with self.assertRaises(S.Invalid):
            s.to_py(None)


class TestObjectId(TestCase):

    def test_oid_to_py_ok(self):
        s = S.ObjectId()
        oid = bson.ObjectId()
        v = s.to_py(oid)
        self.assertEqual(v, oid)

    def test_int_to_py_fail(self):
        s = S.ObjectId()
        with self.assertRaises(S.Invalid):
            s.to_py('5')


class TestNumber(TestCase):

    def test_int_to_py_ok_int(self):
        s = S.Number()
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_num_to_py_ok_float(self):
        s = S.Number()
        v = s.to_py(5.5)
        self.assertEqual(v, 5.5)

    def test_num_to_py_fail(self):
        s = S.Number()
        with self.assertRaises(S.Invalid):
            s.to_py('5')


class TestInt(TestCase):

    def test_int_to_py_ok(self):
        s = S.Integer()
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_int_to_py_fail(self):
        s = S.Integer()
        with self.assertRaises(S.Invalid):
            s.to_py('5')


class TestFloat(TestCase):

    def test_float_to_py_ok_int(self):
        s = S.Float()
        v = s.to_py(5)
        self.assertEqual(v, 5)
        self.assertIsInstance(v, float)

    def test_float_to_py_ok_float(self):
        s = S.Float()
        v = s.to_py(5)
        self.assertEqual(v, 5)

    def test_float_to_py_fail(self):
        s = S.Float()
        with self.assertRaises(S.Invalid):
            s.to_py('5')


class TestUnicode(TestCase):

    def test_uni_to_py_ok(self):
        s = S.Unicode()
        uni = u'foo'
        v = s.to_py(uni)
        self.assertEqual(v, uni)

    def test_uni_to_py_fail(self):
        s = S.Unicode()
        with self.assertRaises(S.Invalid):
            s.to_py('5')

