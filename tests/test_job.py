"""Job + JobStatus: construction, mutation, to_record/from_record, equality.

to_record/from_record take a Serializer: control fields stay plain, while
args/kwargs (packed under "payload") and "result" are serializer blobs. These
tests use the bundled PickleSerializer; the cross-serializer guarantees live in
test_serialization.py.
"""

import datetime as dt

from TaskQueue.job import Job, JobStatus
from TaskQueue.serializers import PickleSerializer

SER = PickleSerializer()


class TestJobStatus:
    def test_members(self) -> None:
        names = {s.name for s in JobStatus}
        assert names == {
            "CREATED",
            "QUEUED",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        }

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
        assert j.status is JobStatus.CREATED
        assert j.result is None
        assert j.error is None
        assert j.attempts == 0

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


class TestJobToRecord:
    def test_to_record_keys(self) -> None:
        # error/result are optional keys: absent until populated, so a fresh
        # record carries only the envelope + payload.
        d = Job(task_name="a").to_record(SER)
        assert set(d) == {
            "id",
            "task_name",
            "created_at",
            "status",
            "attempts",
            "payload",
            "request_cancel",
        }

    def test_group_id_absent_when_the_job_belongs_to_no_group(self) -> None:
        # Same rule as error/result: an optional key is absent, never a
        # sentinel. A placeholder string would be a valid-looking group id.
        assert "group_id" not in Job(task_name="a").to_record(SER)

    def test_group_id_written_as_a_plain_string_when_set(self) -> None:
        assert Job(task_name="a", group_id="g1").to_record(SER)["group_id"] == "g1"

    def test_envelope_fields_stay_plain(self) -> None:
        d = Job(task_name="a", attempts=2).to_record(SER)
        assert d["task_name"] == "a"
        assert d["attempts"] == "2"  # stringified: records hold str | bytes only
        assert isinstance(d["created_at"], str)  # isoformat, not a blob

    def test_status_serialized_as_value(self) -> None:
        j = Job(task_name="a")
        j.status = JobStatus.COMPLETED
        assert j.to_record(SER)["status"] == JobStatus.COMPLETED.value

    def test_args_and_kwargs_live_in_payload_blob(self) -> None:
        j = Job(task_name="a", args=(1, 2, 3), kwargs={"x": 1})
        payload = SER.loads(j.to_record(SER)["payload"])
        assert payload == {"args": [1, 2, 3], "kwargs": {"x": 1}}

    def test_unfinished_job_has_no_result_blob(self) -> None:
        # Until a job completes, the result key is absent from the record,
        # regardless of status: nothing has been produced yet.
        for status in (JobStatus.CREATED, JobStatus.QUEUED, JobStatus.RUNNING):
            j = Job(task_name="a", status=status)
            assert "result" not in j.to_record(SER)

    def test_failed_job_has_no_result_blob(self) -> None:
        j = Job(task_name="a", status=JobStatus.FAILED, error="boom")
        assert "result" not in j.to_record(SER)

    def test_completed_none_result_is_recorded_and_distinct(self) -> None:
        # The §2.4 case: a task that completes returning None records a present
        # blob, distinguishable at the record level from "no result yet", and
        # round-trips back to None.
        j = Job(task_name="a", status=JobStatus.COMPLETED, result=None)
        record = j.to_record(SER)
        assert "result" in record  # present, not absent
        assert Job.from_record(record, SER).result is None

    def test_roundtrip_preserves_fields(self) -> None:
        j = Job(task_name="a", args=(1, 2), kwargs={"x": 1}, result=42, attempts=1)
        j.status = JobStatus.COMPLETED
        back = Job.from_record(j.to_record(SER), SER)
        assert back.task_name == "a"
        assert back.args == (1, 2)
        assert back.kwargs == {"x": 1}
        assert back.result == 42
        assert back.attempts == 1
        assert back.status is JobStatus.COMPLETED

    def test_group_id_round_trips(self) -> None:
        j = Job(task_name="a", group_id="g1")
        assert Job.from_record(j.to_record(SER), SER).group_id == "g1"

    def test_record_without_group_id_reads_as_none(self) -> None:
        # Schema drift: a job enqueued before this field existed must still
        # load. Indexing the key instead would raise KeyError inside the
        # worker, which swallows it as a poison record -- the job never
        # reaches a terminal state and its result() waiter hangs forever.
        record = Job(task_name="a").to_record(SER)
        assert "group_id" not in record
        assert Job.from_record(record, SER).group_id is None


class TestJobEquality:
    def test_clone_equals_original(self) -> None:
        j = Job(task_name="a", id="x", args=(1, 2))
        assert Job.from_record(j.to_record(SER), SER) == j

    def test_distinct_jobs_not_equal(self) -> None:
        assert Job(task_name="a") != Job(task_name="a")

    def test_not_equal_to_other_types(self) -> None:
        assert Job(task_name="a") != "a"

    def test_hashable_by_id(self) -> None:
        j = Job(task_name="a", id="x")
        assert hash(j) == hash(Job.from_record(j.to_record(SER), SER))
