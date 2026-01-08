# Tests for RepeatedDateValueWarning
import warnings

import pytest
from ocdsmerge_rs.exceptions import RepeatedDateValueWarning


@pytest.mark.parametrize("method", ["compiled", "versioned"])
def test_repeated_date(method, empty_merger):
    data = [
        {"date": "2020-01-01T00:00:00Z", "id": "1", "field": "value1"},
        {"date": "2020-01-01T00:00:00Z", "id": "2", "field": "value2"},
        {"date": "2020-01-01T00:00:00Z", "id": "3", "field": "value3"},
    ]

    with pytest.warns(RepeatedDateValueWarning) as records:
        getattr(empty_merger, f"create_{method}_release")(data)

    assert len(records) == 2

    assert (
        str(records[0].message)
        == "Release at index 1 has the same date '2020-01-01T00:00:00Z' as the previous release"
    )
    assert vars(records[0].message) == {"date": "2020-01-01T00:00:00Z", "index": 1}

    assert (
        str(records[1].message)
        == "Release at index 2 has the same date '2020-01-01T00:00:00Z' as the previous release"
    )
    assert vars(records[1].message) == {"date": "2020-01-01T00:00:00Z", "index": 2}


def test_repeated_date_error(empty_merger):
    data = [
        {"date": "2020-01-01T00:00:00Z", "id": "1"},
        {"date": "2020-01-01T00:00:00Z", "id": "2"},
    ]

    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=RepeatedDateValueWarning)
        with pytest.raises(RepeatedDateValueWarning) as excinfo:
            empty_merger.create_compiled_release(data)

    assert str(excinfo.value) == "Release at index 1 has the same date '2020-01-01T00:00:00Z' as the previous release"
    assert vars(excinfo.value) == {"date": "2020-01-01T00:00:00Z", "index": 1}


def test_repeated_date_ignore(empty_merger):
    data = [
        {"date": "2020-01-01T00:00:00Z", "id": "1"},
        {"date": "2020-01-01T00:00:00Z", "id": "2"},
    ]

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # no unexpected warnings
        warnings.filterwarnings("ignore", category=RepeatedDateValueWarning)
        empty_merger.create_compiled_release(data)
