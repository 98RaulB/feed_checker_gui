from __future__ import annotations

import csv
import gzip
import io
import json
import os
import tempfile
import unittest
from unittest import mock

try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

import feed_filter as ff
import feed_specs as fs


GOOGLE_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:g="http://base.google.com/ns/1.0">
  <channel>
    <item>
      <g:id>SKU-1</g:id>
      <title>Sofa</title>
      <description>Large sofa</description>
      <link>https://example.com/p/1</link>
      <g:image_link>https://example.com/i/1.jpg</g:image_link>
      <g:additional_image_link>https://example.com/i/1.jpg</g:additional_image_link>
      <g:additional_image_link>https://example.com/i/2.jpg</g:additional_image_link>
      <g:price>499.00 EUR</g:price>
      <g:availability>in stock</g:availability>
      <g:brand>Brand</g:brand>
      <g:product_type>Living Room &gt; Sofas</g:product_type>
    </item>
  </channel>
</rss>
"""


def _table() -> ff.FeedTable:
    table = ff.FeedTable("test")
    table.columns["id"] = ["1", "2", "3"]
    table.columns["title"] = ["Alpha", "Beta", ""]
    table.columns["price"] = [10.0, 20.0, None]
    table.columns["has_image"] = [True, False, False]
    table.columns["description_length"] = [10, 0, 5]
    table.columns["availability"] = ["in stock", "out of stock", ""]
    table.columns["category"] = ["Living room", "Dining room", ""]
    table.columns["brand"] = ["Acme", "Acme", ""]
    table.columns["url"] = [
        "https://example.com/alpha",
        "https://example.com/beta",
        "",
    ]
    table.total_seen = 3
    return table


def _browser_table() -> ff.FeedTable:
    table = ff.FeedTable("test")
    table.columns["id"] = ["SKU-1", "SKU-2", "SKU-3"]
    table.columns["title"] = ["Red Sofa [sale]", "Blue Chair", "Red Lamp"]
    table.columns["price"] = [100.0, 50.0, 25.0]
    table.columns["availability"] = ["in stock", "in stock", "preorder"]
    table.columns["brand"] = ["Acme", "Acme", "Bright"]
    table.columns["category"] = [
        "Living, Sofas",
        "Dining > Chairs",
        "Lighting",
    ]
    table.columns["url"] = [
        "https://example.com/sofa",
        "https://example.com/chair",
        "https://example.com/lamp",
    ]
    table.total_seen = 3
    return table


class RuleSafetyTests(unittest.TestCase):
    def test_no_rules_are_identity_even_in_remove_mode(self):
        result = ff.apply_rules(_table(), [], mode="remove")
        self.assertEqual(result["matched"], 0)
        self.assertEqual(result["kept"], 3)
        self.assertEqual(result["removed"], 0)
        self.assertEqual(result["keep_mask"], [True, True, True])
        self.assertIn("all products kept", ff.describe([], "AND", "remove", result))

    def test_incomplete_rules_are_ignored(self):
        rules = [{"field": "price", "op": "<", "value": "", "value2": ""}]
        result = ff.apply_rules(_table(), rules, mode="keep")
        self.assertEqual(result["active_rule_count"], 0)
        self.assertEqual(result["incomplete_rule_count"], 1)
        self.assertEqual(result["kept"], 3)

    def test_blank_text_rule_never_matches_everything(self):
        rule = {"field": "title", "op": "contains", "value": ""}
        self.assertEqual(ff.rule_mask(_table(), rule), [False, False, False])

    def test_invalid_bool_operator_matches_nothing(self):
        rule = {"field": "has_image", "op": "not-a-real-op"}
        self.assertEqual(ff.rule_mask(_table(), rule), [False, False, False])

    def test_invalid_combine_and_mode_fail_closed(self):
        with self.assertRaises(ValueError):
            ff.apply_rules(_table(), [], combine="or")
        with self.assertRaises(ValueError):
            ff.apply_rules(_table(), [], mode="KEEP")

    def test_zero_length_is_empty_for_derived_numeric_fields(self):
        rule = {"field": "description_length", "op": "is empty"}
        self.assertEqual(ff.rule_mask(_table(), rule), [False, True, False])

    def test_machine_spec_omits_incomplete_rules(self):
        rules = [
            {"field": "price", "op": "<", "value": ""},
            {"field": "title", "op": "contains", "value": "sofa"},
        ]
        spec = ff.to_spec(rules, "AND", "keep")
        self.assertNotIn("version", spec)
        self.assertEqual(len(spec["rules"]), 1)
        self.assertEqual(spec["rules"][0]["field"], "title")

    def test_non_finite_numbers_are_rejected(self):
        for value in ("nan", "inf", "-inf", "1e309"):
            rule = {"field": "price", "op": "!=", "value": value}
            self.assertIsNotNone(ff.rule_error(rule))
            self.assertEqual(ff.rule_mask(_table(), rule), [False, False, False])

    def test_reversed_between_bounds_are_rejected(self):
        rule = {
            "field": "price",
            "op": "between",
            "value": "200",
            "value2": "100",
        }
        self.assertEqual(
            ff.rule_error(rule),
            "Minimum cannot be greater than maximum.",
        )
        self.assertEqual(ff.rule_mask(_table(), rule), [False, False, False])

    def test_null_rule_values_are_incomplete(self):
        rule = {"field": "title", "op": "contains", "value": None}
        self.assertIsNotNone(ff.rule_error(rule))
        self.assertEqual(ff.rule_mask(_table(), rule), [False, False, False])

    def test_handoff_uses_the_same_table_validation_as_evaluation(self):
        table = _table()
        rule = {
            "field": "param",
            "op": "contains",
            "value": "red",
            "value2": "color",
        }
        result = ff.apply_rules(table, [rule])
        self.assertEqual(result["active_rule_count"], 0)
        self.assertIn(
            "no filters",
            ff.describe([rule], "AND", "keep", result, table=table),
        )
        self.assertEqual(
            ff.to_spec([rule], "AND", "keep", table=table)["rules"],
            [],
        )

    def test_machine_spec_rejects_invalid_enums(self):
        with self.assertRaises(ValueError):
            ff.to_spec([], "or", "keep")
        with self.assertRaises(ValueError):
            ff.to_spec([], "AND", "KEEP")

    def test_parameter_multi_values_use_any_value_semantics(self):
        table = ff.FeedTable("test", index_params=True)
        table.columns["id"] = ["1", "2"]
        table.columns["param"] = [
            {"color": "Red | Blue"},
            {"color": "Green"},
        ]
        table.total_seen = 2
        equals = {
            "field": "param",
            "op": "equals",
            "value": "red",
            "value2": "color",
        }
        in_list = {
            "field": "param",
            "op": "in list",
            "value": "blue, yellow",
            "value2": "color",
        }
        self.assertEqual(ff.rule_mask(table, equals), [True, False])
        self.assertEqual(ff.rule_mask(table, in_list), [True, False])


class CategorySelectionTests(unittest.TestCase):
    def test_category_operators_are_streamlined_but_legacy_ops_still_work(self):
        self.assertEqual(
            ff.operators_for_field("category"),
            ["one of", "not one of", "is empty", "is not empty"],
        )
        self.assertIn("contains", ff.operators_for_field("title"))
        table = _browser_table()
        legacy = {
            "field": "category",
            "op": "in list",
            "value": "Lighting",
        }
        self.assertIsNone(ff.rule_error(legacy, table))
        self.assertEqual(ff.rule_mask(table, legacy), [False, False, True])

    def test_native_selection_preserves_a_comma_inside_category_name(self):
        table = _browser_table()
        selected = {
            "field": "category",
            "op": "one of",
            "value": ["Living, Sofas"],
        }
        self.assertIsNone(ff.rule_error(selected, table))
        self.assertEqual(ff.rule_mask(table, selected), [True, False, False])

        # A string retains the v1 comma-list interpretation for compatibility.
        legacy_text = {
            "field": "category",
            "op": "in list",
            "value": "Living, Sofas",
        }
        self.assertEqual(
            ff.rule_mask(table, legacy_text),
            [False, False, False],
        )

    def test_not_one_of_is_exact_and_blank_selection_is_incomplete(self):
        table = _browser_table()
        rule = {
            "field": "category",
            "op": "not one of",
            "value": ["lighting"],
        }
        self.assertEqual(ff.rule_mask(table, rule), [True, True, False])

        blank = {
            "field": "category",
            "op": "not one of",
            "value": [],
        }
        self.assertEqual(
            ff.rule_error(blank, table),
            "Select at least one category.",
        )
        self.assertEqual(ff.rule_mask(table, blank), [False, False, False])

    def test_category_facets_count_case_variants_and_can_include_missing(self):
        table = ff.FeedTable("test")
        table.columns["id"] = ["1", "2", "3", "4"]
        table.columns["category"] = ["Sofas", "sofas", "Lighting", ""]
        table.total_seen = 4
        self.assertEqual(
            ff.category_facets(table),
            [
                {"value": "Sofas", "count": 2, "label": "Sofas (2)"},
                {"value": "Lighting", "count": 1, "label": "Lighting (1)"},
            ],
        )
        with_empty = ff.category_facets(table, include_empty=True)
        missing = next(facet for facet in with_empty if facet["value"] == "")
        self.assertEqual(missing["count"], 1)
        self.assertEqual(missing["label"], "(Missing category) (1)")


class RuleGroupTests(unittest.TestCase):
    def _groups(self):
        return [
            {
                "combine": "OR",
                "rules": [
                    {"field": "title", "op": "equals", "value": "Alpha"},
                    {"field": "title", "op": "equals", "value": "Beta"},
                ],
            },
            {
                "combine": "OR",
                "rules": [
                    {"field": "price", "op": "<", "value": "15"},
                    {"field": "id", "op": "equals", "value": "3"},
                ],
            },
        ]

    def test_parenthesised_group_truth_table_and_global_or(self):
        groups = self._groups()
        both = ff.apply_rule_groups(_table(), groups, "AND", "keep")
        either = ff.apply_rule_groups(_table(), groups, "OR", "keep")
        self.assertEqual(both["keep_mask"], [True, False, False])
        self.assertEqual(either["keep_mask"], [True, True, True])
        self.assertEqual(both["active_group_count"], 2)
        self.assertEqual(
            [
                (entry["group_index"], entry["combine"], entry["matched"])
                for entry in both["per_group"]
            ],
            [(0, "OR", 2), (1, "OR", 2)],
        )

    def test_remove_mode_is_exact_inversion_when_rules_are_active(self):
        keep = ff.apply_rule_groups(_table(), self._groups(), "AND", "keep")
        remove = ff.apply_rule_groups(
            _table(), self._groups(), "AND", "remove"
        )
        self.assertEqual(
            remove["keep_mask"],
            [not value for value in keep["keep_mask"]],
        )
        self.assertEqual(remove["kept"], 2)
        self.assertEqual(remove["removed"], 1)

    def test_empty_and_incomplete_groups_are_ignored_in_preview(self):
        groups = [
            {"combine": "AND", "rules": []},
            {
                "combine": "OR",
                "rules": [{"field": "price", "op": "<", "value": ""}],
            },
            {
                "combine": "AND",
                "rules": [
                    {"field": "title", "op": "equals", "value": "Alpha"}
                ],
            },
        ]
        result = ff.apply_rule_groups(_table(), groups, "AND", "keep")
        self.assertEqual(result["keep_mask"], [True, False, False])
        self.assertEqual(result["active_group_count"], 1)
        self.assertEqual(result["active_rule_count"], 1)
        self.assertEqual(result["incomplete_rule_count"], 1)
        self.assertEqual([g["group_index"] for g in result["per_group"]], [2])
        self.assertEqual(len(result["group_diagnostics"]), 3)
        self.assertFalse(result["group_diagnostics"][0]["active"])
        self.assertIsNotNone(result["rule_errors"][0])

    def test_no_active_group_is_identity_even_in_remove_mode(self):
        groups = [
            {"combine": "AND", "rules": []},
            {
                "combine": "OR",
                "rules": [{"field": "price", "op": "<", "value": ""}],
            },
        ]
        result = ff.apply_rule_groups(_table(), groups, "OR", "remove")
        self.assertEqual(result["matched"], 0)
        self.assertEqual(result["keep_mask"], [True, True, True])
        self.assertEqual(result["kept"], 3)
        self.assertIn(
            "no filters",
            ff.describe_rule_groups(groups, "OR", "remove", result, _table()),
        )

    def test_group_enums_are_strict(self):
        with self.assertRaises(ValueError):
            ff.apply_rule_groups(_table(), [], "or", "keep")
        with self.assertRaises(ValueError):
            ff.apply_rule_groups(_table(), [], "AND", "KEEP")
        with self.assertRaises(ValueError):
            ff.apply_rule_groups(
                _table(),
                [{"combine": "any", "rules": []}],
                "AND",
                "keep",
            )
        with self.assertRaises(ValueError):
            ff.to_group_spec([], "and", "keep")

    def test_description_preserves_parentheses_and_group_connectors(self):
        text = ff.describe_rule_groups(
            self._groups(),
            "AND",
            "keep",
            table=_table(),
        )
        self.assertIn(
            "(Product name equals Alpha OR Product name equals Beta)",
            text,
        )
        self.assertIn(
            "AND (Price (amount) < 15 OR Item ID equals 3)",
            text,
        )

    def test_v2_spec_preserves_group_shape_and_native_category_list(self):
        groups = [
            {
                "combine": "OR",
                "rules": [
                    {
                        "field": "category",
                        "op": "one of",
                        "value": ["Living, Sofas", "Lighting"],
                    },
                    {"field": "price", "op": "<", "value": ""},
                ],
            },
            {"combine": "AND", "rules": []},
        ]
        spec = ff.to_group_spec(groups, "AND", "remove", _browser_table())
        self.assertEqual(spec["version"], 2)
        self.assertEqual(spec["groupCombine"], "AND")
        self.assertEqual(spec["mode"], "remove")
        self.assertEqual(
            spec["groups"],
            [{
                "combine": "OR",
                "rules": [{
                    "field": "category",
                    "op": "one of",
                    "value": ["Living, Sofas", "Lighting"],
                }],
            }],
        )

    def test_collection_values_are_json_safe_in_v1_and_v2_specs(self):
        table = _browser_table()
        for collection in (
            ("Lighting",),
            {"Lighting"},
            frozenset({"Lighting"}),
        ):
            with self.subTest(collection_type=type(collection).__name__):
                rule = {
                    "field": "category",
                    "op": "one of",
                    "value": collection,
                }
                v1 = ff.to_spec([rule], "AND", "keep", table)
                v2 = ff.to_group_spec(
                    [{"combine": "AND", "rules": [rule]}],
                    "AND",
                    "keep",
                    table,
                )
                v1_round_trip = json.loads(json.dumps(v1))
                v2_round_trip = json.loads(json.dumps(v2))
                self.assertEqual(v1_round_trip["rules"][0]["value"], ["Lighting"])
                self.assertEqual(
                    v2_round_trip["groups"][0]["rules"][0]["value"],
                    ["Lighting"],
                )
                self.assertEqual(
                    ff.rule_mask(table, v2_round_trip["groups"][0]["rules"][0]),
                    [False, False, True],
                )

    def test_single_group_matches_v1_and_v1_spec_is_unchanged(self):
        rules = [
            {"field": "title", "op": "contains", "value": "a"},
            {"field": "price", "op": "<", "value": "20"},
        ]
        flat = ff.apply_rules(_table(), rules, "AND", "keep")
        grouped = ff.apply_rule_groups(
            _table(),
            [{"combine": "AND", "rules": rules}],
            "OR",
            "keep",
        )
        for key in ("matched", "kept", "removed", "keep_mask"):
            self.assertEqual(grouped[key], flat[key])
        self.assertEqual(
            ff.to_spec(rules, "AND", "keep"),
            {
                "combine": "AND",
                "mode": "keep",
                "rules": [
                    {"field": "title", "op": "contains", "value": "a"},
                    {"field": "price", "op": "<", "value": "20"},
                ],
            },
        )


class BrowseTests(unittest.TestCase):
    def test_neutral_browse_returns_all_loaded_rows(self):
        table = _browser_table()
        self.assertEqual(ff.browse_mask(table), [True, True, True])
        self.assertEqual(
            ff.browse_mask(table, query="  ", categories=[""]),
            [True, True, True],
        )

    def test_neutral_and_category_only_browse_skip_search_haystacks(self):
        table = _browser_table()
        original_column_value = ff._column_value

        with mock.patch.object(
            ff,
            "_column_value",
            side_effect=AssertionError("neutral browse should not read fields"),
        ):
            self.assertEqual(
                ff.browse_mask(table, base_mask=[True, False, True]),
                [True, False, True],
            )

        visited_fields = []

        def category_only(table_arg, field, index):
            visited_fields.append(field)
            if field != "category":
                raise AssertionError("category-only browse built a search haystack")
            return original_column_value(table_arg, field, index)

        with mock.patch.object(ff, "_column_value", side_effect=category_only):
            self.assertEqual(
                ff.browse_mask(table, categories=["Lighting"]),
                [False, False, True],
            )
        self.assertEqual(visited_fields, ["category", "category", "category"])

    def test_search_is_literal_case_insensitive_and_ands_tokens(self):
        table = _browser_table()
        self.assertEqual(
            ff.browse_mask(table, query="RED acME"),
            [True, False, False],
        )
        self.assertEqual(
            ff.browse_mask(table, query="["),
            [True, False, False],
        )

    def test_category_selection_is_exact_and_preserves_commas(self):
        table = _browser_table()
        self.assertEqual(
            ff.browse_mask(table, categories=["living, sofas"]),
            [True, False, False],
        )
        self.assertEqual(
            ff.browse_mask(table, categories=["Living"]),
            [False, False, False],
        )

    def test_base_mask_scopes_search_and_length_is_validated(self):
        table = _browser_table()
        self.assertEqual(
            ff.browse_mask(
                table,
                query="acme",
                base_mask=[False, True, True],
            ),
            [False, True, False],
        )
        with self.assertRaises(ValueError):
            ff.browse_mask(table, base_mask=[True])

    def test_browse_rows_reports_total_and_caps_materialised_rows(self):
        result = ff.browse_rows(_browser_table(), query="acme", limit=1)
        self.assertEqual(result["matched"], 2)
        self.assertEqual(result["shown"], 1)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["indices"], [0])
        self.assertEqual(result["rows"][0]["id"], "SKU-1")
        self.assertEqual(len(result["mask"]), 3)


class ExtractionTests(unittest.TestCase):
    def _write(self, content: bytes, suffix: str) -> str:
        fd, path = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return path

    def test_gzip_is_detected_by_magic_not_filename(self):
        plain_wrong_suffix = self._write(GOOGLE_FEED.encode(), ".xml.gz")
        gzip_wrong_suffix = self._write(gzip.compress(GOOGLE_FEED.encode()), ".xml")
        plain = ff.extract(plain_wrong_suffix)
        compressed = ff.extract(gzip_wrong_suffix)
        self.assertEqual(plain.spec, "Google Merchant (g:) RSS")
        self.assertEqual(compressed.spec, plain.spec)
        self.assertEqual(compressed.columns, plain.columns)

    def test_stream_detection_is_not_limited_by_large_xml_header(self):
        xml = (
            "<SHOP><!--"
            + ("x" * 300_000)
            + "--><SHOPITEM><ITEM_ID>1</ITEM_ID><PRODUCTNAME>Sofa</PRODUCTNAME>"
            "<URL>https://example.com/p</URL><IMGURL>https://example.com/i.jpg</IMGURL>"
            "<PRICE_VAT>10</PRICE_VAT><DELIVERY_DATE>0</DELIVERY_DATE>"
            "</SHOPITEM></SHOP>"
        ).encode()
        path = self._write(gzip.compress(xml), ".xml.gz")
        table = ff.extract(path)
        self.assertEqual(table.spec, "Heureka strict")
        self.assertEqual(table.n, 1)

    def test_nested_item_like_tags_do_not_consume_detection_budget(self):
        nested = "<offer><note>{}</note></offer>".format("x" * 10_000) * 5
        xml = (
            "<products><product>"
            + nested
            + "<Identifier>1</Identifier><Name>Sofa</Name>"
            "<Product_url>https://example.com/p</Product_url>"
            "<Image_url>https://example.com/i.jpg</Image_url>"
            "<Price>10</Price></product></products>"
        ).encode()
        path = self._write(gzip.compress(xml), ".xml.gz")
        table = ff.extract(path)
        self.assertEqual(
            table.spec,
            "Compari / Árukereső / Pazaruvaj (case-insensitive)",
        )
        self.assertEqual(table.n, 1)

    def test_google_additional_images_are_counted_without_duplicates(self):
        path = self._write(GOOGLE_FEED.encode(), ".xml")
        table = ff.extract(path)
        self.assertEqual(table.columns["image_count"], [2])

    def test_unknown_small_feed_still_counts_item_like_elements(self):
        path = self._write(
            b"<products><product><foo>bar</foo></product></products>",
            ".xml",
        )
        table = ff.extract(path)
        self.assertEqual(table.spec, "UNKNOWN")
        self.assertEqual(table.total_seen, 1)

    def test_nested_parameter_values_are_indexed(self):
        item = ET.fromstring(
            "<product><attributes><attribute><name>Color</name>"
            "<values><value>Red</value><value>Blue</value></values>"
            "</attribute></attributes></product>"
        )
        self.assertEqual(fs._named_param_values(item), {"color": "Red | Blue"})

    def test_name_attribute_with_nested_values_is_indexed(self):
        item = ET.fromstring(
            '<product><attributes><attribute name="Color">'
            "<values><value>Red</value><value>Blue</value></values>"
            "</attribute></attributes></product>"
        )
        self.assertEqual(fs._named_param_values(item), {"color": "Red | Blue"})

    def test_repeated_and_attribute_only_parameter_values_are_indexed(self):
        repeated = ET.fromstring(
            "<product>"
            "<PARAM><PARAM_NAME>Color</PARAM_NAME><VAL>Red</VAL></PARAM>"
            "<PARAM><PARAM_NAME>Color</PARAM_NAME><VAL>Blue</VAL></PARAM>"
            "</product>"
        )
        attributed = ET.fromstring(
            '<product><attributes><attribute NAME="Color" VALUE="Red"/>'
            "</attributes></product>"
        )
        direct_multi = ET.fromstring(
            "<product><attributes><attribute><name>Color</name>"
            "<value>Red</value><value>Blue</value>"
            "</attribute></attributes></product>"
        )
        self.assertEqual(
            fs._named_param_values(repeated),
            {"color": "Red | Blue"},
        )
        self.assertEqual(
            fs._named_param_values(attributed),
            {"color": "Red"},
        )
        self.assertEqual(
            fs._named_param_values(direct_multi),
            {"color": "Red | Blue"},
        )

    def test_stream_snapshot_stops_after_cap_plus_one(self):
        items = "".join(
            "<item><g:id>{0}</g:id><title>Item {0}</title>"
            "<link>https://example.com/{0}</link>"
            "<g:image_link>https://example.com/{0}.jpg</g:image_link>"
            "<g:price>10 EUR</g:price></item>".format(i)
            for i in range(5)
        )
        xml = (
            '<rss xmlns:g="http://base.google.com/ns/1.0"><channel>'
            + items
            + "</channel></rss>"
        ).encode()
        path = self._write(gzip.compress(xml), ".xml.gz")
        table = ff.extract(path, cap=2)
        self.assertEqual(table.n, 2)
        self.assertEqual(table.total_seen, 3)
        self.assertFalse(table.total_exact)
        self.assertTrue(table.truncated)

    def test_decompressed_xml_has_a_hard_limit(self):
        path = self._write(
            gzip.compress(("<root>" + "<meta>x</meta>" * 100 + "</root>").encode()),
            ".xml.gz",
        )
        with mock.patch.object(ff, "MAX_XML_BYTES", 128):
            with self.assertRaises(ff.FeedParseLimitError):
                ff.extract(path)

    def test_single_product_has_byte_and_node_limits(self):
        large_text = (
            "<products><product><Identifier>1</Identifier>"
            f"<Description>{'x' * 100_000}</Description>"
            "</product></products>"
        ).encode()
        text_paths = [
            self._write(large_text, ".xml"),
            self._write(gzip.compress(large_text), ".xml.gz"),
        ]
        with mock.patch.object(ff, "MAX_ITEM_XML_BYTES", 1024):
            for text_path in text_paths:
                with self.assertRaises(ff.FeedParseLimitError):
                    ff.extract(text_path)

        many_nodes = (
            "<products><product>"
            + "<x>1</x>" * 20
            + "</product></products>"
        ).encode()
        node_paths = [
            self._write(many_nodes, ".xml"),
            self._write(gzip.compress(many_nodes), ".xml.gz"),
        ]
        with mock.patch.object(ff, "MAX_ITEM_NODES", 10):
            for nodes_path in node_paths:
                with self.assertRaises(ff.FeedParseLimitError):
                    ff.extract(nodes_path)


class PublicUrlGuardTests(unittest.TestCase):
    def test_private_destinations_are_rejected(self):
        for url in (
            "http://127.0.0.1/feed.xml",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]/feed.xml",
        ):
            with self.assertRaises(ff.FeedDownloadError):
                ff.assert_public_url(url)

    def test_cgnat_and_tailscale_range_is_rejected(self):
        self.assertFalse(ff._ip_is_global("100.64.0.1"))
        with self.assertRaises(ff.FeedDownloadError):
            ff.assert_public_url("http://100.64.0.1/feed.xml")

    def test_all_resolved_addresses_must_be_public(self):
        with mock.patch.object(
            ff,
            "_resolve_ips",
            return_value=["93.184.216.34", "10.0.0.1"],
        ):
            with self.assertRaises(ff.FeedDownloadError):
                ff.assert_public_url("https://example.com/feed.xml")

    def test_public_hostname_passes(self):
        with mock.patch.object(
            ff,
            "_resolve_ips",
            return_value=["93.184.216.34"],
        ):
            ff.assert_public_url("https://example.com/feed.xml")


class ExportTests(unittest.TestCase):
    def test_id_csv_quotes_commas_quotes_and_newlines(self):
        values = ["A,B", 'quote"inside', "line\nbreak", "00123"]
        parsed = list(csv.reader(io.StringIO(ff.ids_csv(values))))
        self.assertEqual(parsed, [["id"], *[[value] for value in values]])

    def test_id_csv_rejects_spreadsheet_formula_cells(self):
        for value in ("=2+2", " +SUM(A1:A2)", "-1+2", "@cmd"):
            with self.assertRaises(ValueError):
                ff.ids_csv([value])


if __name__ == "__main__":
    unittest.main(verbosity=2)
