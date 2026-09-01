"""Every example must reference a real, current public API.

`examples/` is imported by nothing, so a rename in `src/` can leave an example
importing a module that no longer exists and the whole suite still goes green
(this happened when `backends/memory.py` became `backends/memory_backend.py`).
`examples/` is in pyright's `include` as of 2026-09-01, which catches the same
class of breakage - but only when the type checker is run. These tests keep the
guarantee in the test suite, where it holds on every `pytest` run.

They deliberately do not execute the examples: several run a worker pool at
import time, which would make the suite slow and non-deterministic. Instead each
file is parsed and its TaskQueue imports are resolved: the module must be
importable, and every name imported from it must actually exist. That is exactly
the failure mode a rename introduces.
"""

import ast
import importlib
import importlib.util
import pathlib

import pytest

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parent.parent / "examples"
EXAMPLES = sorted(EXAMPLES_DIR.glob("*.py"))


def _taskqueue_imports(tree: ast.Module) -> list[tuple[str, tuple[str, ...]]]:
    """(module, names) for every TaskQueue import in the file; () means `import X`."""
    found: list[tuple[str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            # level > 0 is a relative import, which an example never uses.
            if node.level == 0 and node.module and node.module.startswith("TaskQueue"):
                found.append((node.module, tuple(a.name for a in node.names)))
        elif isinstance(node, ast.Import):
            found.extend(
                (alias.name, ())
                for alias in node.names
                if alias.name.startswith("TaskQueue")
            )
    return found


def test_examples_directory_is_not_empty() -> None:
    # Guards the parametrized tests below: an empty glob would silently pass.
    assert EXAMPLES, f"no examples found under {EXAMPLES_DIR}"


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_parses(path: pathlib.Path) -> None:
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_imports_resolve(path: pathlib.Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for module_name, names in _taskqueue_imports(tree):
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:  # pragma: no cover - only on a real regression
            pytest.fail(f"{path.name}: cannot import {module_name!r} ({exc})")
        missing = [
            n
            for n in names
            if not hasattr(module, n)
            and importlib.util.find_spec(f"{module_name}.{n}") is None
        ]
        assert not missing, f"{path.name}: {module_name} has no {missing}"
