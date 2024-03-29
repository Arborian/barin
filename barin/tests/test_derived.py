import re
from unittest import TestCase

from unittest.mock import Mock

from barin import collection, derived, Metadata, Field


class TestDerived(TestCase):
    def setUp(self):
        self.db = Mock()
        self.metadata = Metadata()

        self.Base = collection(
            self.metadata,
            "mydoc",
            Field("_id", int),
            Field("disc", str, default="base"),
            polymorphic_discriminator="disc",
        )

        self.Derived = derived(
            self.Base,
            "derived",
            Field("x", int, default=10),
        )

        self.Derived2 = derived(
            self.Base,
            "derived2",
            Field("x", int, default=10),
            allow_extra=True,
            strip_extra=False,
        )

        self.metadata.bind(self.db)
        self.db.mydoc.with_options.return_value = self.db.mydoc

    def test_query_base(self):
        self.assertEqual(self.Base.m.query._compile_query(), {"filter": {}})

    def test_query_derived(self):
        self.assertEqual(
            self.Derived.m.query._compile_query(),
            {"filter": {"disc": re.compile("^derived")}},
        )

    def test_poly_query(self):
        self.db.mydoc.find.return_value = iter([self.Derived.m.create(_id=1)])
        res = self.Base.m.query.one()
        self.assertEqual(type(res), self.Derived)

    def test_derived_with_extra(self):
        self.assertEqual(self.Derived.m.schema.allow_extra, False)
        self.assertEqual(self.Derived.m.schema.strip_extra, False)
        self.assertEqual(self.Derived2.m.schema.allow_extra, True)
        self.assertEqual(self.Derived2.m.schema.strip_extra, False)
