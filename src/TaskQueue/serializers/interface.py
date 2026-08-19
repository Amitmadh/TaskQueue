from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Codec for the opaque blobs in a record — the user payload (args/kwargs)
    and the result.

    The contract is round-trip fidelity: `loads(dumps(obj))` reproduces `obj`
    within the codec's type model (e.g. JSON returns tuples as lists). `dumps`
    may raise if an object is not representable.
    """

    def dumps(self, obj: Any) -> bytes:
        """Serialize an object to bytes. May raise (e.g. `TypeError`) when `obj`
        is not representable in this codec."""
        ...

    def loads(self, data: bytes) -> Any:
        """Deserialize bytes produced by `dumps` back into the original object."""
        ...
