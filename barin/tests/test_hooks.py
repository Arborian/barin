from unittest import TestCase, mock

from mock import Mock

from barin import collection, Metadata, Field
from barin import event



class TestCollection(TestCase):

    def setUp(self):
        self.db = Mock()
        self.metadata = Metadata()

        self.MyDoc = collection(
            self.metadata, 'mydoc',
            Field('_id', int, default=0),
            Field('x', int, default=0))
        self.metadata.bind(self.db)
        self.db.mydoc.with_options.return_value = self.db.mydoc

    def test_before_insert(self):
        before_insert_one = mock.Mock()
        event.listen(self.MyDoc, 'before_insert_one', before_insert_one)
        doc = self.MyDoc.m.create()
        doc.m.insert()
        before_insert_one.assert_called()
        args, kwargs = before_insert_one.call_args
        self.assertEqual(args[1], doc)

    def test_before_replace(self):
        before_replace = mock.Mock()
        event.listen(self.MyDoc, 'before_replace', before_replace)
        doc = self.MyDoc.m.create()
        doc.m.replace()
        before_replace.assert_called()
        args, kwargs = before_replace.call_args
        self.assertEqual(args[0].instance, doc)

    def test_before_delete(self):
        before_delete = mock.Mock()
        event.listen(self.MyDoc, 'before_delete', before_delete)
        doc = self.MyDoc.m.create()
        doc.m.delete()
        before_delete.assert_called()
        args, kwargs = before_delete.call_args
        self.assertEqual(args[0].instance, doc)
