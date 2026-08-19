"""CLI spec — argument parsing, target resolution, and process lifecycle.

Almost all of this runs in-process: `build_parser` and `resolve_queue` are split
out of `main` precisely so the interesting behaviour is reachable without
spawning anything. One subprocess test covers what cannot be faked — that the
*installed console script* starts a pool and stops on a signal, which is also
the only way to exercise the `sys.path` insertion (a console script does not put
the working directory on the path, unlike ``python -m``).

The entry-point test is the one nothing else can catch: a typo in
``[project.scripts]`` is invisible to ruff, pyright and every other test here,
and surfaces only when a user types `taskqueue`.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from importlib.metadata import entry_points
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from TaskQueue import __version__
from TaskQueue.cli import (
    TargetError,
    available,
    build_parser,
    main,
    resolve_queue,
)
from TaskQueue.queue import Queue

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.timeout(30)

_ONE_QUEUE = """
    from TaskQueue import MemoryBackend, Queue

    q = Queue(MemoryBackend())
    not_a_queue = 42
"""


# --------------------------------------------------------------------------
# argument parsing
# --------------------------------------------------------------------------


def test_worker_defaults() -> None:
    args = build_parser().parse_args(["worker", "myapp:q"])
    assert (args.command, args.target, args.concurrency) == ("worker", "myapp:q", 1)


def test_worker_flags() -> None:
    args = build_parser().parse_args(
        ["worker", "myapp:q", "-c", "8", "--log-level", "debug"]
    )
    assert (args.concurrency, args.log_level) == (8, "debug")


@pytest.mark.parametrize(
    "argv", [[], ["nonsense"], ["worker"], ["worker", "a:b", "-l", "bogus"]]
)
def test_usage_errors_exit_two_without_starting_anything(argv: list[str]) -> None:
    # argparse's convention: usage errors are exit 2, distinct from the exit 1
    # a bad target gives, so a supervisor can tell "you typed it wrong" from
    # "your app failed to import".
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(argv)
    assert excinfo.value.code == 2


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["--version"])
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out


# --------------------------------------------------------------------------
# listing subcommands
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("command", "expected"),
    [("backends", {"memory", "redis"}), ("serializers", {"json", "pickle"})],
)
def test_listing_reports_the_implementations_on_disk(
    command: str, expected: set[str], capsys: pytest.CaptureFixture[str]
) -> None:
    # Derived from the filenames, not a hardcoded set, so adding an
    # implementation cannot leave the CLI reporting stale names.
    assert main([command]) == 0
    names = capsys.readouterr().out.split()
    assert expected <= set(names)
    assert names == sorted(names)
    # interface.py carries no suffix, so the convention excludes it for free.
    assert "interface" not in names


def test_available_reads_filenames_and_ignores_the_rest(tmp_path: Path) -> None:
    for filename in ("alpha_widget.py", "interface.py", "helpers.py", "beta_widget.py"):
        (tmp_path / filename).write_text("", encoding="utf-8")
    (tmp_path / "nested_widget").mkdir()  # a package, not a module
    assert available([str(tmp_path)], "_widget") == ["alpha", "beta"]


def test_listing_backends_does_not_import_them() -> None:
    # The optional-dependency boundary, asserted rather than assumed: an eager
    # scan would import redis_backend.py and make `taskqueue backends` fail
    # wherever redis is not installed. Run in a fresh interpreter because this
    # session has already imported the backends.
    source = textwrap.dedent("""
        import sys
        import TaskQueue.backends as backends
        from TaskQueue.cli import available

        # MemoryBackend is already imported here: the package root re-exports
        # it. Snapshot first, so this measures what discovery itself imports.
        before = set(sys.modules)
        names = available(backends.__path__, "_backend")
        imported = {m for m in set(sys.modules) - before if m.startswith("TaskQueue")}

        assert "redis" in names, names
        assert not imported, imported
        assert "TaskQueue.backends.redis_backend" not in sys.modules
        print("clean")
    """)
    result = subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "clean"


# --------------------------------------------------------------------------
# target resolution
# --------------------------------------------------------------------------


@pytest.fixture
def module_factory(tmp_path: Path) -> Iterator[Callable[[str, str], str]]:
    """Write an importable throwaway module; yields a builder returning its name."""
    created: list[str] = []
    sys.path.insert(0, str(tmp_path))

    def build(name: str, source: str) -> str:
        import importlib

        (tmp_path / f"{name}.py").write_text(textwrap.dedent(source), encoding="utf-8")
        importlib.invalidate_caches()
        created.append(name)
        return name

    yield build

    sys.path.remove(str(tmp_path))
    for name in created:
        sys.modules.pop(name, None)


def test_resolves_explicit_attribute(
    module_factory: Callable[[str, str], str],
) -> None:
    name = module_factory("cli_one", _ONE_QUEUE)
    assert isinstance(resolve_queue(f"{name}:q"), Queue)


def test_importing_the_target_registers_its_tasks(
    module_factory: Callable[[str, str], str],
) -> None:
    # Why the CLI takes an import string at all: the decorators must run, or the
    # worker claims jobs it has no task registered for.
    name = module_factory(
        "cli_tasks",
        """
        from TaskQueue import MemoryBackend, Queue

        q = Queue(MemoryBackend())

        @q.task(name="demo.add")
        async def add(x: int, y: int) -> int:
            return x + y
        """,
    )
    assert "demo.add" in resolve_queue(f"{name}:q").task_registry


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("no_such_module_at_all:q", "cannot import"),
        ("cli_errors:missing", "has no attribute"),
        ("cli_errors:not_a_queue", "expected a Queue"),
    ],
)
def test_bad_targets_raise_target_error(
    module_factory: Callable[[str, str], str], spec: str, expected: str
) -> None:
    module_factory("cli_errors", _ONE_QUEUE)
    with pytest.raises(TargetError, match=expected):
        resolve_queue(spec)


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param("cli_malformed", id="no-colon"),
        pytest.param(":q", id="empty-module"),
        pytest.param("cli_malformed:", id="empty-queue"),
        pytest.param("", id="empty"),
        pytest.param("::", id="only-colons"),
    ],
)
def test_both_halves_of_the_target_are_required(
    module_factory: Callable[[str, str], str], spec: str
) -> None:
    # Every malformed spec must surface as TargetError, which main() turns into
    # a one-line message and exit 1. The empty-module cases are the sharp ones:
    # they reach importlib.import_module(""), which raises ValueError — not
    # ImportError — so a guard that lets them through escapes resolve_queue's
    # handler entirely and reaches the user as a traceback.
    module_factory("cli_malformed", _ONE_QUEUE)
    with pytest.raises(TargetError, match="invalid target"):
        resolve_queue(spec)


def test_bad_target_exits_one_not_two(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["worker", "no_such_module_at_all:q"]) == 1
    assert "taskqueue:" in capsys.readouterr().err


# --------------------------------------------------------------------------
# the installed console script
# --------------------------------------------------------------------------


def _console_script() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return Path(sys.executable).parent / f"taskqueue{suffix}"


def test_console_script_entry_point_resolves() -> None:
    scripts = {ep.name: ep for ep in entry_points(group="console_scripts")}
    assert "taskqueue" in scripts, "the taskqueue console script is not installed"
    assert scripts["taskqueue"].load() is main


@pytest.mark.slow
def test_console_script_imports_a_target_from_the_working_directory(
    tmp_path: Path,
) -> None:
    # A console script starts with its bin/ directory on sys.path, not the cwd,
    # so without cli.main's insertion this fails with ModuleNotFoundError.
    script = _console_script()
    if not script.exists():  # pragma: no cover - editable install without scripts
        pytest.skip(f"console script not installed at {script}")
    (tmp_path / "cli_smoke.py").write_text(
        textwrap.dedent(_ONE_QUEUE), encoding="utf-8"
    )
    process = subprocess.Popen(
        [str(script), "worker", "cli_smoke:q", "-c", "2"],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # Wait for the pool to announce itself before signalling: terminating
        # during startup kills the process before its signal handler exists,
        # which would make the graceful-shutdown assertion below a race.
        # pytestmark's timeout is the backstop if the line never arrives.
        assert process.stdout is not None
        startup = []
        for line in process.stdout:
            startup.append(line)
            if "worker ready" in line:
                break
        else:  # pragma: no cover - only when startup fails
            pytest.fail(f"worker never became ready:\n{''.join(startup)}")

        # terminate() is SIGTERM on POSIX and TerminateProcess on Windows, so
        # only POSIX exercises the graceful path; both must actually exit.
        process.terminate()
        rest, _ = process.communicate(timeout=15)
    except subprocess.TimeoutExpired:  # pragma: no cover - only on a hang
        process.kill()
        pytest.fail("worker did not exit after terminate()")

    output = "".join(startup) + rest
    assert "ModuleNotFoundError" not in output, output
    if sys.platform != "win32":
        assert process.returncode == 0, output
        assert "worker pool stopped" in output, output
