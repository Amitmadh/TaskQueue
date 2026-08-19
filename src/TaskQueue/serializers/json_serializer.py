import json
from typing import Any


class JSONSerializer:
    """Default codec: dependency-free and human-readable.

    JSON's type model covers str/int/float/bool/None plus lists and str-keyed
    dicts. Tuples come back as lists; bytes are not supported.
    """

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data)
