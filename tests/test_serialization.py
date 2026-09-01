"""Serialization spec: Jobs survive a cross-process round-trip.

Phase 1 (in-memory) keeps records in a dict and never crosses a process
boundary, but the Job<->record contract is what Phase 3's Redis backend relies
on. A record is portable only if its payload/result blobs are produced by a
real Serializer. JSON is the default (dependency-free, human-readable);
pickle is the Python-only escape hatch.
"""

import pytest

from TaskQueue import (
    JSONSerializer,
    MemoryBackend,
    PickleSerializer,
    Queue,
    Serializer,
)
from TaskQueue.job import Job, JobStatus

ALL_SERIALIZERS = [JSONSerializer(), PickleSerializer()]
_ids = [type(s).__name__ for s in ALL_SERIALIZERS]


@pytest.mark.parametrize("ser", ALL_SERIALIZERS, ids=_ids)
def test_satisfies_protocol(ser: Serializer) -> None:
    assert isinstance(ser, Serializer)


def test_default_serializer_is_json() -> None:
    assert isinstance(Queue(backend=MemoryBackend()).serializer, JSONSerializer)


@pytest.mark.parametrize("ser", ALL_SERIALIZERS, ids=_ids)
def test_json_safe_job_roundtrips_everywhere(ser: Serializer) -> None:
    # int/str/float/None args + str-keyed kwargs + list result: the common case
    j = Job(task_name="a", args=(1, "two", 3.0), kwargs={"x": 1}, result=[1, 2])
    j.status = JobStatus.COMPLETED
    back = Job.from_record(j.to_record(ser), ser)
    assert back == j
    assert back.args == (1, "two", 3.0)
    assert back.kwargs == {"x": 1}
    assert back.result == [1, 2]
    assert back.status is JobStatus.COMPLETED


def test_json_rejects_bytes_args() -> None:
    # A concrete JSON limitation, made explicit: bytes are not representable.
    ser = JSONSerializer()
    j = Job(task_name="a", args=(b"\x00\x01",))
    with pytest.raises(TypeError):
        j.to_record(ser)


def test_unserializable_args_raise() -> None:
    ser = JSONSerializer()
    j = Job(task_name="a", args=(lambda: 1,))
    with pytest.raises(TypeError):
        j.to_record(ser)


@pytest.mark.parametrize("ser", ALL_SERIALIZERS, ids=_ids)
def test_round_trip_preserves_every_field(ser: Serializer) -> None:
    # Drift guard: every public attribute must survive to_record -> from_record.
    # The Job is fully populated with distinct, non-default values (an
    # intentionally odd combination) so that adding a field to Job but wiring it
    # into only some of __init__/to_record/from_record fails here instead of
    # silently dropping data in production.
    j = Job(
        task_name="pkg.mod.task",
        id="fixed-id-123",
        args=(1, "two", 3.0),
        kwargs={"a": 1, "b": [2, 3]},
        status=JobStatus.COMPLETED,  # COMPLETED so `result` is serialized
        result={"x": [1, 2]},
        error="recorded error",
        attempts=4,
    )
    j.request_cancel = True

    back = Job.from_record(j.to_record(ser), ser)

    for name, expected in vars(j).items():
        assert hasattr(back, name), f"field {name!r} missing after round-trip"
        assert getattr(back, name) == expected, (
            f"field {name!r} changed: {getattr(back, name)!r} != {expected!r}"
        )
