"""
Phase 0 scaffolding tests for TaskQueue.

These tests validate that the project is set up correctly *as a package*,
independent of any business logic. They catch the silent failures that
turn into bugs much later: missing py.typed, wrong package layout,
type-checking not actually strict, lockfile not committed, etc.

Run with: uv run pytest tests/test_scaffolding.py -v
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
PACKAGE_ROOT = REPO_ROOT / "src" / "TaskQueue"


@pytest.fixture(scope="session")
def pyproject() -> dict:
    """Parse pyproject.toml once per session."""
    assert PYPROJECT.exists(), f"pyproject.toml not found at {PYPROJECT}"
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def _run(cmd: list[str], cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    """Run a subprocess and capture output. Does not raise on nonzero exit."""
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# Layout: src/ structure, package files
# ---------------------------------------------------------------------------


class TestLayout:
    """The repository follows the src/ layout convention."""

    def test_src_directory_exists(self) -> None:
        assert (REPO_ROOT / "src").is_dir(), (
            "Expected src/ layout. Move package under src/TaskQueue/."
        )

    def test_package_directory_exists(self) -> None:
        assert PACKAGE_ROOT.is_dir(), f"Package directory missing: {PACKAGE_ROOT}"

    def test_no_flat_layout(self) -> None:
        """There should NOT be a top-level TaskQueue/ directory alongside src/."""
        flat = REPO_ROOT / "TaskQueue"
        assert not flat.exists(), (
            "Found TaskQueue/ at repo root — this conflicts with src/ layout "
            "and causes import shadowing bugs. Remove it."
        )

    def test_init_py_exists(self) -> None:
        assert (PACKAGE_ROOT / "__init__.py").is_file()

    def test_py_typed_marker_exists(self) -> None:
        """PEP 561: without py.typed, downstream type checkers ignore your types."""
        marker = PACKAGE_ROOT / "py.typed"
        assert marker.is_file(), (
            "Missing src/TaskQueue/py.typed. Without this empty marker file, "
            "consumers of your library won't get type checking. Run: "
            "touch src/TaskQueue/py.typed"
        )

    def test_tests_directory_exists(self) -> None:
        assert (REPO_ROOT / "tests").is_dir()


# ---------------------------------------------------------------------------
# Required top-level files
# ---------------------------------------------------------------------------


class TestRequiredFiles:
    """The repo has the files a serious project is expected to have."""

    @pytest.mark.parametrize(
        "filename",
        [
            "README.md",
            "LICENSE",
            "CHANGELOG.md",
            "pyproject.toml",
            ".gitignore",
        ],
    )
    def test_file_exists(self, filename: str) -> None:
        assert (REPO_ROOT / filename).is_file(), f"Missing required file: {filename}"

    def test_readme_is_nonempty(self) -> None:
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        assert len(readme) > 200, (
            "README.md is too short. At minimum it should have a pitch, "
            "the differentiators, a quickstart, and a roadmap."
        )

    def test_readme_mentions_project(self) -> None:
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8").lower()
        assert "taskqueue" in readme

    def test_license_is_recognized(self) -> None:
        license_text = (REPO_ROOT / "LICENSE").read_text()
        # Either MIT or Apache-2.0 is fine; just verify it's a real license.
        assert "MIT License" in license_text or "Apache License" in license_text, (
            "LICENSE doesn't look like MIT or Apache-2.0. Use one of those."
        )

    def test_gitignore_excludes_venv_and_caches(self) -> None:
        gi = (REPO_ROOT / ".gitignore").read_text()
        for pattern in [".venv", "__pycache__", ".ruff_cache", "dist"]:
            assert pattern in gi, f".gitignore should exclude {pattern}"


# ---------------------------------------------------------------------------
# pyproject.toml: project metadata
# ---------------------------------------------------------------------------


class TestProjectMetadata:
    """pyproject.toml is well-formed and has the right metadata."""

    def test_has_project_table(self, pyproject: dict) -> None:
        assert "project" in pyproject

    def test_name_is_TaskQueue(self, pyproject: dict) -> None:
        assert pyproject["project"]["name"] == "TaskQueue"

    def test_version_is_pep440(self, pyproject: dict) -> None:
        version = pyproject["project"]["version"]
        # Loose PEP 440 check: N.N.N with optional pre/dev/post/local suffixes.
        assert re.match(r"^\d+\.\d+\.\d+", version), (
            f"Version not PEP 440-ish: {version}"
        )

    def test_python_requires_311_plus(self, pyproject: dict) -> None:
        """ExceptionGroup and TaskGroup require 3.11. Don't go lower."""
        req = pyproject["project"]["requires-python"]
        assert ">=3.11" in req or ">=3.12" in req, (
            f"requires-python is {req!r}, but structured concurrency needs >=3.11."
        )

    def test_has_description(self, pyproject: dict) -> None:
        desc = pyproject["project"].get("description", "")
        assert len(desc) >= 20, "Description is missing or too short."

    def test_readme_is_referenced(self, pyproject: dict) -> None:
        assert pyproject["project"].get("readme") == "README.md"

    def test_core_has_zero_runtime_dependencies(self, pyproject: dict) -> None:
        """The core package has no runtime deps; backends are extras."""
        deps = pyproject["project"].get("dependencies", [])
        assert deps == [], (
            f"Core should have zero deps; found {deps}. "
            "Move backend dependencies to optional-dependencies."
        )

    def test_dev_extras_present(self, pyproject: dict) -> None:
        extras = pyproject["project"].get("optional-dependencies", {})
        assert "dev" in extras, "Missing [project.optional-dependencies].dev"
        dev_deps = " ".join(extras["dev"])
        for required in ["pytest", "ruff", "pyright"]:
            assert required in dev_deps, f"dev extras missing {required}"


# ---------------------------------------------------------------------------
# pyproject.toml: tool configuration
# ---------------------------------------------------------------------------


class TestToolConfig:
    """Linter, type checker, and test runner are configured strictly."""

    def test_ruff_configured(self, pyproject: dict) -> None:
        assert "ruff" in pyproject.get("tool", {}), "No [tool.ruff] section."

    def test_ruff_lint_rules_selected(self, pyproject: dict) -> None:
        select = pyproject["tool"]["ruff"]["lint"]["select"]
        # A minimal sane set; you can add more.
        for rule in ["E", "F", "I", "UP", "B"]:
            assert rule in select, f"ruff should select rule group {rule!r}"

    def test_pyright_strict_mode(self, pyproject: dict) -> None:
        """Strict mode is non-negotiable for a type-safety pitch."""
        pr = pyproject.get("tool", {}).get("pyright", {})
        assert pr.get("typeCheckingMode") == "strict", (
            "Pyright must be in strict mode. The type-safety story depends on it."
        )

    def test_pytest_strict_markers(self, pyproject: dict) -> None:
        ini = pyproject.get("tool", {}).get("pytest", {}).get("ini_options", {})
        addopts = ini.get("addopts", "")
        assert "--strict-markers" in addopts, (
            "Add --strict-markers to pytest addopts so marker typos fail loudly."
        )

    def test_pytest_asyncio_mode_auto(self, pyproject: dict) -> None:
        ini = pyproject.get("tool", {}).get("pytest", {}).get("ini_options", {})
        assert ini.get("asyncio_mode") == "auto", (
            "Set asyncio_mode='auto' so async tests don't need a decorator."
        )

    def test_coverage_configured(self, pyproject: dict) -> None:
        cov = pyproject.get("tool", {}).get("coverage", {})
        assert cov, "No [tool.coverage] section — coverage won't run in CI."


# ---------------------------------------------------------------------------
# Package contents: __init__.py
# ---------------------------------------------------------------------------


class TestPackageContents:
    """The package itself is importable and minimally sane."""

    def test_package_imports(self) -> None:
        # Confirm Python can actually find and import the package.
        import TaskQueue  # noqa: F401

    def test_version_attribute_exists(self) -> None:
        import TaskQueue

        assert hasattr(TaskQueue, "__version__")

    def test_version_is_string(self) -> None:
        import TaskQueue

        assert isinstance(TaskQueue.__version__, str)

    def test_version_matches_pyproject(self, pyproject: dict) -> None:
        """Drift between pyproject version and __init__ version is a common bug."""
        import TaskQueue

        assert TaskQueue.__version__ == pyproject["project"]["version"], (
            f"Version mismatch: __init__.py={TaskQueue.__version__!r}, "
            f"pyproject.toml={pyproject['project']['version']!r}"
        )

    def test_all_is_defined(self) -> None:
        import TaskQueue

        assert hasattr(TaskQueue, "__all__"), "Define __all__ in __init__.py."
        assert isinstance(TaskQueue.__all__, list)


# ---------------------------------------------------------------------------
# CI / pre-commit
# ---------------------------------------------------------------------------


class TestCI:
    """Continuous integration is wired up."""

    def test_github_workflow_exists(self) -> None:
        wf_dir = REPO_ROOT / ".github" / "workflows"
        assert wf_dir.is_dir(), "Missing .github/workflows/ directory."
        ymls = list(wf_dir.glob("*.yml")) + list(wf_dir.glob("*.yaml"))
        assert ymls, "No workflow files in .github/workflows/."

    def test_ci_workflow_runs_required_checks(self) -> None:
        wf_dir = REPO_ROOT / ".github" / "workflows"
        contents = "\n".join(p.read_text() for p in wf_dir.glob("*.y*ml"))
        for tool in ["ruff", "pyright", "pytest"]:
            assert tool in contents, (
                f"CI workflow doesn't appear to run {tool}. "
                "Every CI run should lint, type-check, and test."
            )

    def test_pre_commit_config_exists(self) -> None:
        # Not strictly required but strongly recommended.
        pc = REPO_ROOT / ".pre-commit-config.yaml"
        if not pc.is_file():
            pytest.skip(
                "No .pre-commit-config.yaml. Recommended but not required at Phase 0."
            )


# ---------------------------------------------------------------------------
# Lockfile (uv)
# ---------------------------------------------------------------------------


class TestLockfile:
    """If using uv, the lockfile should be committed for reproducibility."""

    def test_uv_lock_committed(self) -> None:
        lock = REPO_ROOT / "uv.lock"
        if not lock.is_file():
            pytest.skip("No uv.lock — skipping (only relevant if using uv).")
        # Confirm it's tracked by git, not just sitting locally.
        result = _run(["git", "ls-files", "--error-unmatch", "uv.lock"])
        assert result.returncode == 0, (
            "uv.lock exists but isn't tracked by git."
            "Commit it for reproducible builds."
        )


# ---------------------------------------------------------------------------
# Live tool runs — the real proof that scaffolding works
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestToolsActuallyRun:
    """
    Invoke the tools themselves. Marked 'slow' because each shells out.
    Skip individually if the tool isn't installed locally; CI will catch it.
    """

    def test_ruff_check_passes(self) -> None:
        result = _run(["ruff", "check", "."])
        if result.returncode == 127:
            pytest.skip("ruff not installed locally.")
        assert result.returncode == 0, (
            f"ruff check failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_ruff_format_check_passes(self) -> None:
        result = _run(["ruff", "format", "--check", "."])
        if result.returncode == 127:
            pytest.skip("ruff not installed locally.")
        assert result.returncode == 0, (
            f"ruff format --check failed (run `ruff format .` to fix):\n{result.stdout}"
        )

    def test_pyright_passes(self) -> None:
        result = _run(["pyright"])
        if result.returncode == 127:
            pytest.skip("pyright not installed locally.")
        assert result.returncode == 0, (
            f"pyright failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_package_is_installed_editable(self) -> None:
        """`pip install -e .` should have been run; verify the import path."""
        import TaskQueue

        pkg_path = Path(TaskQueue.__file__).resolve()
        assert "site-packages" in str(pkg_path) or "src/TaskQueue" in str(pkg_path), (
            f"TaskQueue is imported from an unexpected location: {pkg_path}. "
            "Run `uv sync --all-extras --dev` or `pip install -e .` to fix."
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
