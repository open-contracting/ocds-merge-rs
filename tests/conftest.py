from pathlib import Path

import jsonref
import pytest
from ocdsmerge_rs import Merger


@pytest.fixture
def simple_merger():
    with (Path("tests") / "fixtures" / "schema.json").open() as f:
        return Merger(rules=Merger.get_rules(jsonref.load(f)))


@pytest.fixture
def empty_merger():
    with (Path("tests") / "fixtures" / "release-schema-1__1__4.json").open() as f:
        return Merger(rules=Merger.get_rules(jsonref.load(f)))
