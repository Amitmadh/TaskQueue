import json
import pickle
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


class JSONSerializer:
    """Default codec: dependency-free and human-readable.

    JSON's type model covers str/int/float/bool/None plus lists and str-keyed
    dicts. Tuples come back as lists; bytes are not supported.
    """

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class PickleSerializer:
    """Python-only codec. Powerful but unsafe on untrusted data and not portable
    across languages or Python versions — opt in deliberately."""

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)
