from unittest import TestCase

from barin import field, subdocument, metadata


class TestSubdocumentManager(TestCase):

    def setUp(self):
        self.metadata = metadata.Metadata()
        self.Doc = subdocument(
            self.metadata, 'Doc',
            field.Field('x', int))

    def test_has_no_find(self):
        self.assertFalse(hasattr(self.Doc.m, 'find'))


