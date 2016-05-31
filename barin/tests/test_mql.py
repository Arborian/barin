from unittest import TestCase

from barin import collection, Metadata, Field
from barin import mql


class TestMQL(TestCase):

    def setUp(self):
        self.metadata = Metadata()
        self.MyDoc = collection(
            self.metadata, 'mydoc',
            Field('x', None))

    def test_eq(self):
        self.assertEqual(
            {'x': {'$eq': 5}},
            self.MyDoc.c.x == 5)

    def test_mod(self):
        self.assertEqual(
            {'x': {'$mod': [5, 0]}},
            self.MyDoc.c.x.mod(5, 0))

    def test_regex(self):
        self.assertEqual(
            {'x': {'$regex': '.*'}},
            self.MyDoc.c.x.regex('.*'))

    def test_regex_options(self):
        self.assertEqual(
            {'x': {'$regex': '.*', '$options': 0}},
            self.MyDoc.c.x.regex('.*', 0))

    def test_text(self):
        self.assertEqual(
            {'x': {'$search': 'foo'}},
            self.MyDoc.c.x.text('foo'))

    def test_text_language(self):
        self.assertEqual(
            {'x': {'$search': 'foo', '$language': 'en-gb'}},
            self.MyDoc.c.x.text('foo', language='en-gb'))

    def test_text_case_sensitive(self):
        self.assertEqual(
            {'x': {'$search': 'foo', '$caseSensitive': True}},
            self.MyDoc.c.x.text('foo', case_sensitive=True))

    def test_text_dia_sensitive(self):
        self.assertEqual(
            {'x': {'$search': 'foo', '$diacriticSensitive': True}},
            self.MyDoc.c.x.text('foo', diacritic_sensitive=True))

    def test_near(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$near': 'foo'}},
            self.MyDoc.c.x.near('foo'))

    def test_near_min(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$near': 'foo', '$minDistance': 5}},
            self.MyDoc.c.x.near('foo', min_distance=5))

    def test_near_max(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$near': 'foo', '$maxDistance': 5}},
            self.MyDoc.c.x.near('foo', max_distance=5))

    def test_near_sphere(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$nearSphere': 'foo'}},
            self.MyDoc.c.x.near_sphere('foo'))

    def test_near_sphere_min(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$nearSphere': 'foo', '$minDistance': 5}},
            self.MyDoc.c.x.near_sphere('foo', min_distance=5))

    def test_near_sphere_max(self):
        # This is a poor example for actual usage
        self.assertEqual(
            {'x': {'$nearSphere': 'foo', '$maxDistance': 5}},
            self.MyDoc.c.x.near_sphere('foo', max_distance=5))

    def test_and(self):
        self.assertEqual(
            {'$and': [
                {'x': {'$gt': 5}},
                {'x': {'$lt': 15}}]},
            mql.and_(self.MyDoc.c.x > 5, self.MyDoc.c.x < 15))

    def test_and_op(self):
        self.assertEqual(
            {'$and': [
                {'x': {'$gt': 5}},
                {'x': {'$lt': 15}}]},
            (self.MyDoc.c.x > 5) & (self.MyDoc.c.x < 15))

    def test_or(self):
        self.assertEqual(
            {'$or': [
                {'x': {'$gt': 5}},
                {'x': {'$lt': 15}}]},
            mql.or_(self.MyDoc.c.x > 5, self.MyDoc.c.x < 15))

    def test_or_op(self):
        self.assertEqual(
            {'$or': [
                {'x': {'$gt': 5}},
                {'x': {'$lt': 15}}]},
            (self.MyDoc.c.x > 5) | (self.MyDoc.c.x < 15))

    def test_not(self):
        self.assertEqual(
            {'$not': {'x': {'$gt': 5}}},
            mql.not_(self.MyDoc.c.x > 5))

    def test_not_op(self):
        self.assertEqual(
            {'$not': {'x': {'$gt': 5}}},
            ~(self.MyDoc.c.x > 5))

    def test_comment(self):
        self.assertEqual(
            {'$comment': 'foo'},
            mql.comment('foo'))

