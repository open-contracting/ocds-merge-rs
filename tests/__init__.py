import json
from pathlib import Path

tags = {
    "1.0": "1__0__3",
    "1.1": "1__1__4",
}

schema_url = "https://standard.open-contracting.org/schema/{}/release-schema.json"


def path(*args):
    return (Path(__file__).parent / "fixtures").joinpath(*args)


def read(*args, mode="rt", encoding=None, **kwargs):
    with path(*args).open(mode, encoding=encoding, **kwargs) as f:
        return f.read()


def load(*args, **kwargs):
    return json.loads(read(*args, **kwargs))
