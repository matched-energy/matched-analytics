import os
from pathlib import Path

from dotenv import load_dotenv
from setuptools_scm import get_version  # type: ignore


def get_dot_env(key: str) -> str:
    load_dotenv()
    value = os.getenv(key, None)
    if value is None:
        raise Exception(f".env key {key} is None")
    return value


def get_code_version() -> str:
    return get_version(root=Path(__file__).parent.parent.parent.parent, fallback_version="unknown")
