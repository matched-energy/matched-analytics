from pathlib import Path

from setuptools_scm import get_version  # type: ignore


def truncate_string(input_string: str, max_length: int = 30, suffix: str = "...") -> str:
    if len(input_string) > max_length:
        return input_string[: max_length - len(suffix)] + suffix
    return input_string


def get_code_version() -> str:
    return get_version(root=Path(__file__).parent.parent.parent.parent, fallback_version="unknown")
