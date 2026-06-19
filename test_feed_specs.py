# Regression tests for feed_specs availability detection.
# Run: python3 -m unittest test_feed_specs -v
import unittest

try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

import feed_specs as fs

COMPARI = "Compari / Árukereső / Pazaruvaj (case-insensitive)"

# Minimal ProductsUp "Blank Export" item, the shape served at
# transport.productsup.io/.../ro_favi.xml. {avail} is the only field varied.
COMPARI_TMPL = """<?xml version="1.0" encoding="UTF-8"?>
<products>
  <product>
    <Name>X</Name>
    <Category>C</Category>
    <Price>10,00</Price>
    <Image_url>http://example/i.jpg</Image_url>
    <Description>d</Description>
    <Identifier>1</Identifier>
    <Product_url>http://example/p</Product_url>
    {avail}
  </product>
</products>"""


class AvailabilityCasingTest(unittest.TestCase):
    def _first_item(self, xml):
        root = ET.fromstring(xml)
        spec = fs.detect_spec(root)
        items = fs.get_item_nodes(root, spec)
        self.assertTrue(items, "expected at least one item")
        return spec, items[0]

    def test_productsup_delivery_time_casing(self):
        # The real ro_favi.xml uses <Delivery_Time> (capital D and T), which the
        # spec's case-sensitive ./Delivery_time XPath never matched -> the bug.
        spec, item = self._first_item(
            COMPARI_TMPL.format(avail="<Delivery_Time>3</Delivery_Time>")
        )
        self.assertEqual(spec, COMPARI)
        self.assertEqual(fs.read_availability(item, spec), "3")

    def test_delivery_time_all_casings(self):
        for tag in ("Delivery_Time", "DELIVERY_TIME", "delivery_time",
                    "DeliveryTime", "deliverytime"):
            spec, item = self._first_item(
                COMPARI_TMPL.format(avail=f"<{tag}>2</{tag}>")
            )
            self.assertEqual(
                fs.read_availability(item, spec), "2",
                f"<{tag}> should be read as availability",
            )

    def test_explicit_availability_still_wins(self):
        # Exact-path match must take priority over the alias fallback.
        spec, item = self._first_item(COMPARI_TMPL.format(
            avail="<availability>in stock</availability>"
                  "<Delivery_Time>3</Delivery_Time>"
        ))
        self.assertEqual(fs.read_availability(item, spec), "in stock")

    def test_missing_availability_stays_missing(self):
        # No availability/stock/delivery field at all -> genuinely missing.
        # Guards the fallback's additive property (no false positives).
        spec, item = self._first_item(COMPARI_TMPL.format(avail=""))
        self.assertEqual(fs.read_availability(item, spec), "")

    def test_attribute_based_case_insensitive(self):
        # Ceneo spec reads @avail; a feed drifting to @AVAIL must still resolve.
        xml = ('<offers><o id="1" url="http://example" AVAIL="1">'
               '<name>n</name><price>5</price><cat>c</cat><desc>d</desc>'
               '<imgs><main url="http://example/i"/></imgs></o></offers>')
        root = ET.fromstring(xml)
        spec = fs.detect_spec(root)
        items = fs.get_item_nodes(root, spec)
        self.assertEqual(spec, "Ceneo strict")
        self.assertEqual(fs.read_availability(items[0], spec), "1")


class FieldCasingMatrixTest(unittest.TestCase):
    """Every core reader must tolerate tag-casing / namespace-prefix drift, not
    just availability — guards against the case-sensitive-XPath class of false
    'missing' reports across id / link / price / image."""

    def _first_item(self, xml):
        root = ET.fromstring(xml)
        spec = fs.detect_spec(root)
        items = fs.get_item_nodes(root, spec)
        self.assertTrue(items, "expected at least one item")
        return spec, items[0]

    def test_compari_all_fields_uppercased(self):
        # Whole item upper-cased — every reader must still resolve its value.
        xml = ("<products><product>"
               "<NAME>X</NAME><CATEGORY>C</CATEGORY><DESCRIPTION>d</DESCRIPTION>"
               "<IDENTIFIER>SKU-1</IDENTIFIER><PRODUCT_URL>http://example/p</PRODUCT_URL>"
               "<IMAGE_URL>http://example/i.jpg</IMAGE_URL><PRICE>10,00</PRICE>"
               "<DELIVERY_TIME>3</DELIVERY_TIME>"
               "</product></products>")
        spec, item = self._first_item(xml)
        self.assertEqual(spec, COMPARI)
        self.assertEqual(fs.read_id(item, spec), "SKU-1")
        self.assertEqual(fs.read_link(item, spec), "http://example/p")
        self.assertEqual(fs.gather_primary_image(item, spec), "http://example/i.jpg")
        amt, raw = fs.read_price(item, spec)
        self.assertEqual(raw, "10,00")
        self.assertEqual(amt, 10.0)
        self.assertEqual(fs.read_availability(item, spec), "3")

    def test_ceneo_attribute_casing(self):
        # Attribute-based spec with upper-cased attributes (@ID/@URL/@PRICE/@AVAIL).
        xml = ('<offers><o ID="OID-1" URL="http://example/p" PRICE="5,00" AVAIL="1">'
               '<name>n</name><cat>c</cat><desc>d</desc>'
               '<imgs><main url="http://example/i.jpg"/></imgs></o></offers>')
        spec, item = self._first_item(xml)
        self.assertEqual(spec, "Ceneo strict")
        self.assertEqual(fs.read_id(item, spec), "OID-1")
        self.assertEqual(fs.read_link(item, spec), "http://example/p")
        self.assertEqual(fs.read_price(item, spec)[1], "5,00")
        self.assertEqual(fs.read_availability(item, spec), "1")

    def test_google_namespace_prefix_agnostic(self):
        # Same Google namespace URI bound to a non-'g' prefix must still resolve.
        xml = ('<rss xmlns:gm="http://base.google.com/ns/1.0"><channel><item>'
               '<gm:id>1</gm:id><title>t</title><description>d</description>'
               '<link>http://example/p</link>'
               '<gm:image_link>http://example/i.jpg</gm:image_link>'
               '<gm:price>9,00</gm:price>'
               '<gm:availability>in stock</gm:availability>'
               '</item></channel></rss>')
        spec, item = self._first_item(xml)
        self.assertTrue(spec.startswith("Google Merchant"))
        self.assertEqual(fs.read_id(item, spec), "1")
        self.assertEqual(fs.read_link(item, spec), "http://example/p")
        self.assertEqual(fs.gather_primary_image(item, spec), "http://example/i.jpg")
        self.assertEqual(fs.read_availability(item, spec), "in stock")

    def test_no_false_positive_when_genuinely_absent(self):
        # No id / url / image / price children -> readers must report empty,
        # i.e. the alias fallback never invents a value.
        xml = ("<products><product><Name>X</Name><Category>C</Category>"
               "<Description>d</Description></product></products>")
        spec, item = self._first_item(xml)
        self.assertEqual(spec, COMPARI)
        self.assertEqual(fs.read_id(item, spec), "")
        self.assertEqual(fs.read_link(item, spec), "")
        self.assertEqual(fs.gather_primary_image(item, spec), "")
        self.assertEqual(fs.read_price(item, spec), (None, ""))


if __name__ == "__main__":
    unittest.main()
