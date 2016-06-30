from unittest import TestCase

from mock import Mock

import pymongo

from barin import collection, Metadata, Field, Index
from barin import schema as S


class TestField(TestCase):

    def test_field_schema(self):
        fld = Field('x', int)
        self.assertIsInstance(fld._schema, S.Integer)
        self.assertEqual(fld._schema.required, True)
        self.assertEqual(fld._schema.default, S.Missing)

    def test_field_schema_options(self):
        fld = Field('x', int, default=0)
        self.assertEqual(fld._schema.default, 0)


class TestCollection(TestCase):

    def setUp(self):
        self.db = Mock()
        self.metadata = Metadata()

        self.MyDoc = collection(
            self.metadata, 'mydoc',
            Field('_id', int),
            Field('x', int),
            Index('x'))
        self.metadata.bind(self.db)
        self.db.mydoc.with_options.return_value = self.db.mydoc

    def test_can_find(self):
        self.db.mydoc.find.return_value = iter([
            self.MyDoc({'_id': 0, 'x': 5})])
        spec = {'a': {'$exists': True}}
        curs = self.MyDoc.m.find(spec)
        self.db.mydoc.find.assert_called_with(spec)
        doc = curs.first()
        self.assertIsInstance(doc, self.MyDoc)

    def test_can_insert(self):
        doc = self.MyDoc(x=5)
        doc.m.insert()
        self.db.mydoc.insert_one.assert_called_with({'x': 5})

    def test_can_update_with_refresh(self):
        doc = self.MyDoc(_id=1, x=5)
        self.db.mydoc.find_one_and_update.return_value = dict(
            _id=1, x=6)
        doc.m.update({'$inc': {'x': 1}}, refresh=True)
        self.db.mydoc.find_one_and_update.assert_called_with(
            {'_id': 1},
            {'$inc': {'x': 1}},
            return_document=pymongo.ReturnDocument.AFTER)
        self.assertEqual(doc.x, 6)

    def test_can_update_without_refresh(self):
        doc = self.MyDoc(_id=1, x=5)
        doc.m.update({'$inc': {'x': 1}})
        self.db.mydoc.update_one.assert_called_with(
            {'_id': 1},
            {'$inc': {'x': 1}})
        self.assertEqual(doc.x, 5)


class TestSubdoc(TestCase):

        def setUp(self):
            metadata = Metadata()
            subdoc = S.compile_schema(metadata, {'x': int})
            self.MyDoc = collection(
                metadata, 'mydoc',
                Field('x', subdoc),
                Field('y', [subdoc]))
            self.doc = self.MyDoc.m.create(x=dict(x=5), y=[])

        def test_can_access(self):
            self.assertEqual(5, self.doc.x.x)

        def test_dotted_document(self):
            self.assertEqual(
                {'x.x': 5},
                self.MyDoc.x.x == 5)

        def test_dotted_array(self):
            self.assertEqual(
                {'y.0': 5},
                self.MyDoc.y[0] == 5)
