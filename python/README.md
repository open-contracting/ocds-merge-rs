# ocdsmerge_rs

Python bindings for the `ocdsmerge` Rust library, which merges JSON texts conforming to the Open Contracting Data Standard.

## Usage

This package provides a `Merger` class that can create compiled and versioned releases from a **sorted** list of OCDS releases.

```python
from ocdsmerge_rs import Merger

# This small schema is for demonstration purposes.
schema = {
    "properties": {
        "tender": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"}
                        }
                    }
                }
            }
        }
    }
}

releases = [
    {"ocid": "ocds-213czf-1", "id": "1", "date": "2020-01-01", "tag": ["tender"], "tender": {"id": "1", "title": "Original tender"}},
    {"ocid": "ocds-213czf-1", "id": "2", "date": "2020-01-02", "tag": ["tenderUpdate"], "tender": {"id": "1", "title": "Updated tender"}},
]

# Dereference the JSON Schema in-place.
Merger.dereference(schema)

# Get merge rules from the dereferenced schema.
rules = Merger.get_rules(schema)

# Create a merger instance.
merger = Merger(rules=rules)

# Create a compiled release.
compiled = merger.create_compiled_release(releases)

# Create a versioned release.
versioned = merger.create_versioned_release(releases)
```

You can [override the merge routine's behavior](https://ocds-merge.readthedocs.io/en/latest/handle-bad-data.html), like in the `ocds-merge` Python package:

```python
from ocdsmerge_rs import Merger, Strategy

merger = Merger(
    rules=rules,
    overrides={
        ('awards',): Strategy.APPEND,
        ('contracts', 'implementation', 'milestones'): Strategy.MERGE_BY_POSITION,
    },
)
```

Copyright (c) 2023 Open Contracting Partnership, released under the MIT license
