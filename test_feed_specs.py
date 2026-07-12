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
        # i.e. the alias fallback never invents a value. Note: detection now
        # requires a Compari-distinctive tag (Identifier/Product_url/Image_url),
        # so this degenerate feed is UNKNOWN — we pin the spec to exercise the
        # readers directly.
        xml = ("<products><product><Name>X</Name><Category>C</Category>"
               "<Description>d</Description></product></products>")
        root = ET.fromstring(xml)
        self.assertEqual(fs.detect_spec(root), "UNKNOWN")
        item = fs.get_item_nodes(root, COMPARI)[0]
        self.assertEqual(fs.read_id(item, COMPARI), "")
        self.assertEqual(fs.read_link(item, COMPARI), "")
        self.assertEqual(fs.gather_primary_image(item, COMPARI), "")
        self.assertEqual(fs.read_price(item, COMPARI), (None, ""))


class PriceAnalysisTest(unittest.TestCase):
    """analyze_price_text drives BOTH the parser and the GUI format validator,
    so these cases pin amount + format verdict together (they can never
    disagree). Expected tuples: (amount, format_valid, overprecision)."""

    CASES = {
        "8000":       (8000.0, True, False),
        "8,000":      (8000.0, True, False),   # FAVI-documented comma thousands
        "8 000":      (8000.0, True, False),
        "8000,70":    (8000.7, True, False),
        "8000.70":    (8000.7, True, False),
        "1 234,50":   (1234.5, True, False),
        "1,234.50":   (1234.5, True, False),   # comma groups + dot decimals: allowed
        "1,234,567":  (1234567.0, True, False),
        "8.000":      (8000.0, False, False),  # dot must not group thousands
        "1.234,50":   (1234.5, False, False),
        "1.234.567":  (1234567.0, False, False),
        "8000,505":   (8000.505, True, True),  # comma decimal, NOT thousands ×1000
        "1,2345":     (1.2345, True, True),    # no silent token truncation
        "1234 567":   (None, False, False),    # malformed grouping, not "1234"
        "8000 CZK":   (8000.0, True, False),
        "8 000": (8000.0, True, False),   # narrow NBSP (CLDR cs/fr/pl)
        "0":          (0.0, True, False),
        "-5":         (-5.0, True, False),
    }

    def test_matrix(self):
        for raw, (amount, valid, overprec) in self.CASES.items():
            amt, v, op, _reason = fs.analyze_price_text(raw)
            self.assertEqual((amt, v, op), (amount, valid, overprec), f"case {raw!r}")

    def test_parse_price_text_same_amounts(self):
        for raw, (amount, _v, _op) in self.CASES.items():
            self.assertEqual(fs.parse_price_text(raw), amount, f"case {raw!r}")


class DetectionRegressionTest(unittest.TestCase):
    def test_skroutz_mywebstore_root(self):
        xml = ("<mywebstore><created_at>t</created_at><products><product>"
               "<id>1</id><name>n</name><link>https://x.gr/p</link>"
               "<image>https://x.gr/i.jpg</image><price_with_vat>9.99</price_with_vat>"
               "</product></products></mywebstore>")
        self.assertEqual(fs.detect_spec(ET.fromstring(xml)), "Skroutz strict")

    def test_generic_product_feed_not_compari(self):
        # name/price/category/description alone must NOT classify as Compari —
        # its path table would then read <link>/<image> as empty and mass-flag
        # every product as missing URL/image.
        xml = ("<products><product><id>1</id><name>N</name><link>https://x/p</link>"
               "<image>https://x/i.jpg</image><price>10.00</price>"
               "<category>Sofas</category><description>d</description>"
               "</product></products>")
        self.assertEqual(fs.detect_spec(ET.fromstring(xml)), "UNKNOWN")

    def test_heureka_itemgroup_is_not_an_id(self):
        xml = ("<SHOP><SHOPITEM><ITEMGROUP_ID>G1</ITEMGROUP_ID>"
               "<PRODUCTNAME>A</PRODUCTNAME><URL>https://x/a</URL>"
               "</SHOPITEM></SHOP>")
        root = ET.fromstring(xml)
        spec = fs.detect_spec(root)
        self.assertEqual(spec, "Heureka strict")
        self.assertEqual(fs.read_id(fs.get_item_nodes(root, spec)[0], spec), "")

    def test_heureka_net_price_does_not_mask_missing_price_vat(self):
        xml = ("<SHOP><SHOPITEM><ITEM_ID>1</ITEM_ID><PRICE>100</PRICE>"
               "</SHOPITEM></SHOP>")
        item = fs.get_item_nodes(ET.fromstring(xml), "Heureka strict")[0]
        self.assertEqual(fs.read_price(item, "Heureka strict"), (None, ""))


class UrlAndGtinTest(unittest.TestCase):
    def test_percent_encode_idempotent(self):
        u = "https://shop.cz/st%C5%AFl?ref=a%20b"
        self.assertEqual(fs.percent_encode_url(u), u)
        self.assertEqual(fs.percent_encode_url("https://x/a b.jpg"), "https://x/a%20b.jpg")

    def test_percent_encode_never_raises(self):
        self.assertEqual(fs.percent_encode_url("http://[broken/p"), "http://[broken/p")

    def test_gtin(self):
        self.assertTrue(fs.is_valid_gtin("6417182041488"))
        self.assertFalse(fs.is_valid_gtin("6417182041489"))   # bad check digit
        self.assertFalse(fs.is_valid_gtin("1234567"))         # bad length
        self.assertFalse(fs.is_valid_gtin("INTERNAL-1"))


class NestedParamsTest(unittest.TestCase):
    def test_ceneo_attrs_brand_and_ean_visible(self):
        xml = ('<offers><o id="1" url="https://x/p" price="100" avail="1">'
               '<cat>Meble</cat><name>Sofa</name><desc>d</desc>'
               '<imgs><main url="https://x/i.jpg"/><i url="https://x/2.jpg"/></imgs>'
               '<attrs><a name="Producent">BRW</a><a name="EAN">6417182041488</a></attrs>'
               '</o></offers>')
        root = ET.fromstring(xml)
        item = fs.get_item_nodes(root, fs.detect_spec(root))[0]
        present = fs.present_recommended_fields(item)
        self.assertIn("brand", present)
        self.assertIn("gtin", present)
        self.assertEqual(fs.read_recommended_value(item, "gtin"), "6417182041488")
        # gallery path fixed: Ceneo uses <i url=...>, not <img>
        self.assertEqual(fs.gather_gallery(item, "Ceneo strict"), ["https://x/2.jpg"])

    def test_atom_summary_counts_as_description(self):
        xml = ('<feed xmlns="http://www.w3.org/2005/Atom" '
               'xmlns:g="http://base.google.com/ns/1.0"><entry>'
               '<g:id>1</g:id><title>TV</title><summary>Nice.</summary>'
               '</entry></feed>')
        item = ET.fromstring(xml).find("{http://www.w3.org/2005/Atom}entry")
        self.assertIn("description", fs.present_recommended_fields(item))


class GoogleNoNamespaceRootTest(unittest.TestCase):
    """Google Shopping feeds without the g: namespace must be detected as
    Google regardless of the wrapper root — including the bare <items> root
    used by Channable exports (vidaXL.cz), which used to be mislabeled as
    Jeftinije because the Jeftinije spec also claims the <items> root."""

    GOOGLE = "Google Merchant (no-namespace) RSS"
    JEFTINIJE = "Jeftinije / Ceneje (element-based)"

    def _spec(self, xml):
        return fs.detect_spec(ET.fromstring(xml))

    def test_channable_items_root_detects_google(self):
        # Real shape served by files.channable.com for vidaXL.cz: <items> root,
        # lowercase <item>, Google field names, no g: namespace.
        xml = ("<items><item>"
               "<id>143046</id>"
               "<title>vidaXL Ram</title>"
               "<description>d</description>"
               "<link>https://www.vidaxl.cz/e/p/8718475623502.html</link>"
               "<image_link>https://vdxl.im/x.jpg</image_link>"
               "<price>5426.00</price>"
               "<availability>in stock</availability>"
               "<brand>vidaXL</brand>"
               "<gtin>8718475623502</gtin>"
               "<google_product_category>Home &amp; Garden</google_product_category>"
               "</item></items>")
        self.assertEqual(self._spec(xml), self.GOOGLE)

    def test_canonical_rss_root_still_detects_google(self):
        # The original <rss><channel> no-namespace wrapper must keep working.
        xml = ('<rss version="2.0"><channel><item>'
               "<id>1</id><title>t</title><description>d</description>"
               "<link>http://e/p</link><image_link>http://e/i.jpg</image_link>"
               "<price>9.00</price><availability>in stock</availability>"
               "</item></channel></rss>")
        self.assertEqual(self._spec(xml), self.GOOGLE)

    def test_jeftinije_items_root_still_detects_jeftinije(self):
        # Regression guard: a genuine Jeftinije/Ceneje element-based feed also
        # uses an <items> root, but with <Item> children and <image> (not the
        # Google-specific <image_link>). It must NOT be stolen by the loosened
        # Google detector.
        xml = ("<items><Item>"
               "<id>1</id><name>n</name>"
               "<link>http://e/p</link><image>http://e/i.jpg</image>"
               "<price>10</price><availability>in stock</availability>"
               "</Item></items>")
        self.assertEqual(self._spec(xml), self.JEFTINIJE)


if __name__ == "__main__":
    unittest.main()
