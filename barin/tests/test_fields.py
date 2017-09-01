from unittest import TestCase

from barin import field, metadata


class TestFields(TestCase):

    def setUp(self):
        self.fields = field.FieldCollection([
            field.Field('x', int)])
        self.metadata = metadata.Metadata()

    def test_make_schema(self):
        self.fields.make_schema(self.metadata)
