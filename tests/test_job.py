"""Job + JobStatus: construction, mutation, to_dict/from_dict, equality.

Serializer-agnostic: these exercise the dict boundary only. The actual
msgpack round-trip lives in test_serialization.py.
"""

import datetime as dt

from TaskQueue.job import Job, JobStatus


class TestJobStatus:
    def test_members(self) -> None:
        names = {s.name for s in JobStatus}
        assert names == {"QUEUED", "RUNNING", "COMPLETED", "FAILED"}

    def test_is_str_enum(self) -> None:
        # str mixin -> packs transparently as its value (json, msgpack, etc.)
        assert isinstance(JobStatus.QUEUED, str)

    def test_roundtrip_by_value(self) -> None:
        for s in JobStatus:
            assert JobStatus(s.value) is s


class TestJobConstruction:
    def test_minimal_defaults(self) -> None:
        j = Job(task_name="add")
        assert j.task_name == "add"
        assert isinstance(j.id, str) and j.id
        assert j.args == ()
        assert j.kwargs == {}
        assert j.status is JobStatus.QUEUED
        assert j.result is None
        assert j.error is None
        assert j.attempts == 3

    def test_created_at_is_tz_aware_utc(self) -> None:
        j = Job(task_name="add")
        assert isinstance(j.created_at, dt.datetime)
        assert j.created_at.tzinfo is not None
        assert j.created_at.utcoffset() == dt.timedelta(0)

    def test_ids_unique(self) -> None:
        assert Job(task_name="a").id != Job(task_name="a").id

    def test_explicit_id(self) -> None:
        assert Job(task_name="a", id="fixed").id == "fixed"

    def test_args_and_kwargs_stored(self) -> None:
        j = Job(task_name="a", args=(1, 2), kwargs={"x": 3})
        assert j.args == (1, 2)
        assert j.kwargs == {"x": 3}

    def test_full_construction(self) -> None:
        j = Job(
            task_name="a",
            id="i",
            args=(1,),
            kwargs={"k": 1},
            status=JobStatus.COMPLETED,
            result=42,
            error=None,
            attempts=2,
        )
        assert j.id == "i"
        assert (j.status, j.result, j.attempts) == (JobStatus.COMPLETED, 42, 2)


class TestJobMutation:
    def test_status_result_error_settable(self) -> None:
        j = Job(task_name="a")
        j.status = JobStatus.RUNNING
        j.result = 5
        j.error = "boom"
        assert j.status is JobStatus.RUNNING
        assert j.result == 5
        assert j.error == "boom"


class TestJobToDict:
    def test_to_dict_keys(self) -> None:
        d = Job(task_name="a").to_dict()
        assert set(d) == {
            "id",
            "task_name",
            "args",
            "kwargs",
            "created_at",
            "status",
            "result",
            "error",
            "attempts",
        }

    def test_status_serialized_as_value(self) -> None:
        j = Job(task_name="a")
        j.status = JobStatus.COMPLETED
        assert j.to_dict()["status"] == JobStatus.COMPLETED.value

    def test_args_serialized_as_list(self) -> None:
        assert Job(task_name="a", args=(1, 2, 3)).to_dict()["args"] == (1, 2, 3)

    def test_roundtrip_preserves_dict(self) -> None:
        j = Job(task_name="a", args=(1, 2), kwargs={"x": 1})
        j.status = JobStatus.RUNNING
        assert Job.from_dict(j.to_dict()).to_dict() == j.to_dict()

    def test_roundtrip_preserves_object(self) -> None:
        j = Job(task_name="a", args=(1, 2), kwargs={"x": 1}, attempts=1)
        assert Job.from_dict(j.to_dict()) == j


class TestJobEquality:
    def test_clone_equals_original(self) -> None:
        j = Job(task_name="a", id="x", args=(1, 2))
        assert Job.from_dict(j.to_dict()) == j

    def test_distinct_jobs_not_equal(self) -> None:
        assert Job(task_name="a") != Job(task_name="a")

    def test_not_equal_to_other_types(self) -> None:
        assert Job(task_name="a") != "a"
