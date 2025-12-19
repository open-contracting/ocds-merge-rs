# ocds-merge/tests/test_merge.py
#
# These tests are not present, because the Rust implementation doesn't extend or append:
# - test_extend
# - test_appand
#
# test_sorted_releases_key_error is not present, because `create_exception!` doesn't support multiple inheritance.
import json
import re
import warnings
from copy import deepcopy
from pathlib import Path

import jsonref
import pytest
from ocdsmerge_rs import Merger
from ocdsmerge_rs.exceptions import (
    DuplicateIdValueWarning,
    InconsistentTypeError,
    MissingDateKeyError,
    NonObjectReleaseError,
    NonStringDateValueError,
    NullDateValueError,
    OutOfOrderReleaseError,
)

from tests import path, schema_url, tags


def get_test_cases():
    test_merge_argvalues = []

    for minor_version, path_or_url in (
        ("1.1", path("release-schema-1__1__4.json")),
        ("1.1", schema_url),
        ("1.0", schema_url),
        ("schema", path("schema.json")),
    ):
        if isinstance(path_or_url, Path):
            with path_or_url.open() as f:
                schema = jsonref.load(f)
        else:
            schema = jsonref.load_uri(path_or_url.format(tags[minor_version]))
        for suffix in ("compiled", "versioned"):
            filenames = list(path(minor_version).glob(f"*-{suffix}.json"))
            assert filenames, f"{suffix} fixtures not found"
            test_merge_argvalues += [(filename, schema) for filename in filenames]

    return test_merge_argvalues


@pytest.mark.parametrize(
    ("error", "data"),
    [
        (MissingDateKeyError, {}),
        (NullDateValueError, {"date": None}),
        (NonStringDateValueError, {"date": {}}),
        (NonObjectReleaseError, "{}"),
        (NonObjectReleaseError, []),
        (NonObjectReleaseError, ()),
        (NonObjectReleaseError, set()),
    ],
)
def test_date_errors(error, data, empty_merger):
    for infix in ("compiled", "versioned"):
        with pytest.raises(error):
            getattr(empty_merger, f"create_{infix}_release")([{"date": "2010-01-01"}, data])

    if not isinstance(data, dict):
        with pytest.raises(error):
            empty_merger.create_compiled_release([data])
    else:
        release = deepcopy(data)

        expected = {
            "id": f"None-{data.get('date')}",
            "tag": ["compiled"],
        }

        if data.get("date") is not None:
            expected["date"] = data["date"]

        assert empty_merger.create_compiled_release([release]) == expected

    if not isinstance(data, dict):
        with pytest.raises(error):
            empty_merger.create_versioned_release([data])
    else:
        release = deepcopy(data)
        release["initiationType"] = "tender"

        expected = {
            "initiationType": [
                {
                    "releaseID": None,
                    "releaseDate": data.get("date"),
                    "releaseTag": None,
                    "value": "tender",
                }
            ],
        }

        assert empty_merger.create_versioned_release([release]) == expected


@pytest.mark.parametrize(("filename", "schema"), get_test_cases())
def test_merge(filename, schema):
    merger = Merger(rules=Merger.get_rules(schema))

    infix = "compiled" if filename.name.endswith("-compiled.json") else "versioned"

    with filename.open() as f:
        expected = json.load(f)
    with (filename.parent / re.sub(r"-(?:compiled|versioned)", "", filename.name)).open() as f:
        releases = json.load(f)

    original = deepcopy(releases)

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # no unexpected warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # no unexpected warnings
        if Path(filename).name == "identifier-merge-duplicate-id-compiled.json":
            warnings.filterwarnings("ignore", category=DuplicateIdValueWarning)

        actual = getattr(merger, f"create_{infix}_release")(releases)

    assert releases == original
    assert actual == expected, f"{filename}\n{json.dumps(actual, indent=2)}"


@pytest.mark.parametrize(("value", "infix"), [(1, "1"), ([1], "[1]"), ([{"object": 1}], '[{"object":1}]')])
def test_inconsistent_type_object_last(value, infix, empty_merger):
    data = [
        {"date": "2000-01-01T00:00:00Z", "integer": value},
        {"date": "2000-01-02T00:00:00Z", "integer": {"object": 1}},
    ]

    with pytest.raises(InconsistentTypeError) as excinfo:
        empty_merger.create_compiled_release(data)

    assert (
        str(excinfo.value)
        == f"An earlier release had {infix} for /integer, but the current release has an object with a 'object' key"
    )


def test_inconsistent_type_object_first(empty_merger):
    data = [
        {"date": "2000-01-01T00:00:00Z", "integer": {"object": 1}},
        {"date": "2000-01-02T00:00:00Z", "integer": [{"object": 1}]},
    ]

    with pytest.raises(InconsistentTypeError) as excinfo:
        empty_merger.create_compiled_release(data)

    assert (
        str(excinfo.value) == 'An earlier release had {"object":1} for /integer, but the current release has an array'
    )


@pytest.mark.parametrize("method", ["compiled", "versioned"])
def test_out_of_order_releases(method, empty_merger):
    data = [
        {"date": "2020-01-03T00:00:00Z", "id": "1"},
        {"date": "2020-01-02T00:00:00Z", "id": "2"},
    ]

    with pytest.raises(OutOfOrderReleaseError) as excinfo:
        getattr(empty_merger, f"create_{method}_release")(data)

    assert (
        str(excinfo.value)
        == "Release at index 1 has date '2020-01-02T00:00:00Z' which is less than the previous '2020-01-03T00:00:00Z'"
    )


@pytest.mark.parametrize(("i", "j"), [(0, 0), (0, 1), (1, 0), (1, 1)])
def test_merge_when_array_is_mixed(i, j, simple_merger):
    data = [
        {"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "mixedArray": [{"id": 1}, "foo"]},
        {"ocid": "ocds-213czf-A", "id": "2", "date": "2000-01-02T00:00:00Z", "mixedArray": [{"id": 2}, "bar"]},
    ]

    output = {
        "tag": ["compiled"],
        "id": "ocds-213czf-A-2000-01-02T00:00:00Z",
        "date": "2000-01-02T00:00:00Z",
        "ocid": "ocds-213czf-A",
        "mixedArray": [
            {"id": 2},
            "bar",
        ],
    }

    assert simple_merger.create_compiled_release(data) == output

    actual = deepcopy(data)
    expected = deepcopy(output)
    del actual[i]["mixedArray"][j]
    if i == 1:
        del expected["mixedArray"][j]

    assert simple_merger.create_compiled_release(actual) == expected, f"removed item index {j} from release index {i}"


@pytest.mark.parametrize(("i", "j"), [(0, 0), (0, 1), (1, 0), (1, 1)])
def test_merge_when_array_is_mixed_without_schema(i, j, empty_merger):
    data = [
        {"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "mixedArray": [{"id": 1}, "foo"]},
        {"ocid": "ocds-213czf-A", "id": "2", "date": "2000-01-02T00:00:00Z", "mixedArray": [{"id": 2}, "bar"]},
    ]

    output = {
        "tag": ["compiled"],
        "id": "ocds-213czf-A-2000-01-02T00:00:00Z",
        "date": "2000-01-02T00:00:00Z",
        "ocid": "ocds-213czf-A",
        "mixedArray": [
            {"id": 2},
            "bar",
        ],
    }

    assert empty_merger.create_compiled_release(data) == output

    actual = deepcopy(data)
    expected = deepcopy(output)
    del actual[i]["mixedArray"][j]
    if i == 1:
        del expected["mixedArray"][j]

    if j == 0:
        assert empty_merger.create_compiled_release(actual) == expected, (
            f"removed item index {j} from release index {i}"
        )
    else:
        with pytest.raises(AssertionError):
            assert empty_merger.create_compiled_release(actual) == expected, (
                f"removed item index {j} from release index {i}"
            )


def test_create_versioned_release_mutate(simple_merger):
    data = [
        {"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "tag": ["tender"]},
        {"ocid": "ocds-213czf-A", "id": "2", "date": "2000-01-02T00:00:00Z", "tag": ["tenderUpdate"]},
    ]

    simple_merger.create_versioned_release(data)

    # From Python, the tag field is not removed from the original data - unlike Rust.
    assert data == [
        {"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "tag": ["tender"]},
        {"ocid": "ocds-213czf-A", "id": "2", "date": "2000-01-02T00:00:00Z", "tag": ["tenderUpdate"]},
    ]


def test_arbitrary_precision_greater_than_u64_max(empty_merger):
    # u64::MAX + 1
    data = [{"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "number": 18446744073709551616}]

    result = empty_merger.create_compiled_release(data)

    assert isinstance(result["number"], int)
    assert result["number"] == 18446744073709551616


def test_arbitrary_precision_less_than_i64_min(empty_merger):
    # i64::MIN - 1
    data = [{"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "number": -9223372036854775809}]

    result = empty_merger.create_compiled_release(data)

    assert isinstance(result["number"], int)
    assert result["number"] == -9223372036854775809


def test_arbitrary_precision_float(empty_merger):
    data = [{"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "number": 3.141592653589793238}]

    result = empty_merger.create_compiled_release(data)

    assert isinstance(result["number"], float)
    assert result["number"] == 3.141592653589793238


def test_arbitrary_precision_int(empty_merger):
    data = [{"ocid": "ocds-213czf-A", "id": "1", "date": "2000-01-01T00:00:00Z", "number": 2}]

    result = empty_merger.create_compiled_release(data)

    assert isinstance(result["number"], int)
    assert result["number"] == 2
