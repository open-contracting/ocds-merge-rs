# ocds-merge/tests/test_rules.py
import json
from pathlib import Path

from ocdsmerge_rs import Merger, Rule


def load_schema(version):
    with (Path(__file__).parent / "fixtures" / f"release-schema-{version}.json").open() as f:
        return json.load(f)


def test_get_merge_rules_1_1():
    assert Merger.get_rules(Merger.dereference(load_schema("1__1__4"))) == {
        ("awards", "items", "additionalClassifications"): Rule.REPLACE,
        ("contracts", "items", "additionalClassifications"): Rule.REPLACE,
        ("contracts", "relatedProcesses", "relationship"): Rule.REPLACE,
        ("date",): Rule.OMIT,
        ("id",): Rule.OMIT,
        ("parties", "additionalIdentifiers"): Rule.REPLACE,
        ("parties", "roles"): Rule.REPLACE,
        ("relatedProcesses", "relationship"): Rule.REPLACE,
        ("tag",): Rule.OMIT,
        ("tender", "additionalProcurementCategories"): Rule.REPLACE,
        ("tender", "items", "additionalClassifications"): Rule.REPLACE,
        ("tender", "submissionMethod"): Rule.REPLACE,
        # Deprecated
        ("awards", "amendment", "changes"): Rule.REPLACE,
        ("awards", "amendments", "changes"): Rule.REPLACE,
        ("awards", "suppliers", "additionalIdentifiers"): Rule.REPLACE,
        ("buyer", "additionalIdentifiers"): Rule.REPLACE,
        ("contracts", "amendment", "changes"): Rule.REPLACE,
        ("contracts", "amendments", "changes"): Rule.REPLACE,
        ("contracts", "implementation", "transactions", "payee", "additionalIdentifiers"): Rule.REPLACE,
        ("contracts", "implementation", "transactions", "payer", "additionalIdentifiers"): Rule.REPLACE,
        ("tender", "amendment", "changes"): Rule.REPLACE,
        ("tender", "amendments", "changes"): Rule.REPLACE,
        ("tender", "procuringEntity", "additionalIdentifiers"): Rule.REPLACE,
        ("tender", "tenderers", "additionalIdentifiers"): Rule.REPLACE,
    }


def test_get_merge_rules_1_0():
    assert Merger.get_rules(Merger.dereference(load_schema("1__0__3"))) == {
        ("awards", "amendment", "changes"): Rule.REPLACE,
        ("awards", "items", "additionalClassifications"): Rule.REPLACE,
        ("awards", "suppliers"): Rule.REPLACE,
        ("buyer", "additionalIdentifiers"): Rule.REPLACE,
        ("contracts", "amendment", "changes"): Rule.REPLACE,
        ("contracts", "items", "additionalClassifications"): Rule.REPLACE,
        ("date",): Rule.OMIT,
        ("id",): Rule.OMIT,
        ("ocid",): Rule.OMIT,
        ("tag",): Rule.OMIT,
        ("tender", "amendment", "changes"): Rule.REPLACE,
        ("tender", "items", "additionalClassifications"): Rule.REPLACE,
        ("tender", "procuringEntity", "additionalIdentifiers"): Rule.REPLACE,
        ("tender", "submissionMethod"): Rule.REPLACE,
        ("tender", "tenderers"): Rule.REPLACE,
    }
