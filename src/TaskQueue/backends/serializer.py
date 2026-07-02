import json
import pickle
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Codec for the opaque blobs in a record — the user payload (args/kwargs)
    and the result.

    The contract is round-trip fidelity: `loads(dumps(obj))` reproduces `obj`
    within the codec's type model (e.g. JSON returns tuples as lists). `dumps`
    may raise if an object is not representable. Implementations import nothing
    from the package, so the serializer stays free of import cycles.
    """

    def dumps(self, obj: Any) -> bytes:
        """Serialize an object to bytes. May raise (e.g. `TypeError`) when `obj`
        is not representable in this codec."""
        ...

    def loads(self, data: bytes) -> Any:
        """Deserialize bytes produced by `dumps` back into the original object."""
        ...


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
