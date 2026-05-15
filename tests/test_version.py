import TaskQueue


def test_version_is_string() -> None:
    assert isinstance(TaskQueue.__version__, str)
    assert TaskQueue.__version__.count(".") == 2