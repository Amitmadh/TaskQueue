import pickle
from typing import Any


class PickleSerializer:
    """Python-only codec. Powerful but unsafe on untrusted data and not portable
    across languages or Python versions."""

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)
