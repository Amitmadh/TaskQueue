import re
import tomllib
from pathlib import Path

import TaskQueue

_ROOT = Path(__file__).resolve().parent.parent


def test_version_is_string() -> None:
    assert isinstance(TaskQueue.__version__, str)
    assert TaskQueue.__version__.count(".") == 2


def test_the_packaged_version_matches_the_changelog() -> None:
    """The guard that was missing when the 'v0.3.0' tag shipped a '0.0.3' tree.

    'count(".") == 2' above is true of any three-part string, so the packaged
    version could drift from the one the changelog announces and nothing would
    notice. That is how a tag ended up on a commit whose 'pyproject' still said
    '0.0.3', and an install from it reported that.

    Compares the two SOURCE files rather than 'TaskQueue.__version__', which
    comes from the installed distribution's metadata and lags a 'pyproject' edit
    until the environment is re-synced. A stale venv should not fail this.
    """
    with (_ROOT / "pyproject.toml").open("rb") as handle:
        packaged = tomllib.load(handle)["project"]["version"]

    released = re.findall(
        r"^## \[(\d+\.\d+\.\d+)\]",
        (_ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    assert released, "CHANGELOG.md has no released version heading"
    assert packaged == released[0], (
        f"pyproject says {packaged}, the newest changelog heading says {released[0]}"
    )
