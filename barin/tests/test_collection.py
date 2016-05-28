from unittest import TestCase

from mock import Mock

from barin.collection import collection, Metadata, Field, Index


class TestManager(TestCase):

    def setUp(self):
        self.db = Mock()
        self.metadata = Metadata()

        self.MyDoc = collection(
            self.metadata, 'mydoc',
            Field('x', int),
            Index('x'))
        self.metadata.bind(self.db)

    def test_can_find(self):
        self.db.mydoc.find.return_value = [{'x': 5}]
        spec = {'a': {'$exists': True}}
        curs = self.MyDoc.m.find(spec)
        self.db.mydoc.find.assert_called_with(spec)
        doc = curs.first()
        self.assertIsInstance(doc, self.MyDoc)

    def test_can_insert(self):
        doc = self.MyDoc(x=5)
        doc.m.insert()
        self.db.mydoc.insert.assert_called_with({'x': 5})
