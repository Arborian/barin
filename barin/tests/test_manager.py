from unittest import TestCase

from mock import Mock

from barin.manager import Manager


class TestManager(TestCase):

    def setUp(self):
        self.db = Mock()

        class Cls(dict):
            m = Manager('mydoc', db=self.db)
        self.Cls = Cls

    def test_can_find(self):
        self.db.mydoc.find.return_value = [{'a': 5}]
        spec = {'a': {'$exists': True}}
        curs = self.Cls.m.find(spec)
        self.db.mydoc.find.assert_called_with(spec)
        doc = curs.first()
        self.assertIsInstance(doc, self.Cls)

    def test_can_insert(self):
        doc = self.Cls()
        doc.m.insert()
        self.db.mydoc.insert.assert_called_with({})
