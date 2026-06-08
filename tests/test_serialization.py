"""Serialization spec — a Job survives a msgpack round-trip.

Phase 1 (in-memory) never serializes; this guards the Job contract for the
Redis backend in Phase 3, where jobs cross the process boundary as msgpack.
Requires msgpack (`pip install msgpack`); skips cleanly if it is not installed.
"""

import pytest

from TaskQueue.job import Job, JobStatus

msgpack = pytest.importorskip("msgpack")


def test_status_packs_as_its_value() -> None:
    # str-enum packs transparently as the underlying string
    packed = msgpack.packb(JobStatus.COMPLETED)
    assert msgpack.unpackb(packed) == JobStatus.COMPLETED.value


def test_to_dict_is_msgpack_packable() -> None:
    j = Job(task_name="a", args=(1, "two", 3.0), kwargs={"x": b"raw"}, result=[1, 2])
    msgpack.packb(j.to_dict())  # must not raise


def test_job_survives_msgpack_roundtrip() -> None:
    j = Job(task_name="a", args=(1, 2), kwargs={"x": 1})
    j.status = JobStatus.RUNNING
    restored = Job.from_dict(msgpack.unpackb(msgpack.packb(j.to_dict())))
    assert restored == j


def test_bytes_args_survive_roundtrip() -> None:
    # msgpack handles bytes natively (JSON cannot) — a concrete reason to prefer it
    j = Job(task_name="a", args=(b"\x00\x01\x02",))
    restored = Job.from_dict(msgpack.unpackb(msgpack.packb(j.to_dict()), raw=False))
    assert restored.args == (b"\x00\x01\x02",)


def test_non_serializable_args_raise_on_pack() -> None:
    j = Job(task_name="a", args=(lambda: 1,))
    with pytest.raises(TypeError):
        msgpack.packb(j.to_dict())
