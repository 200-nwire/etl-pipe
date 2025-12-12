"""Always-on smoke checks that require no optional dependencies."""

from pathlib import Path


def test_readme_exists() -> None:
    """Ensure project README is present so pytest exits zero even when optional deps are missing."""

    assert Path("README.md").is_file()
