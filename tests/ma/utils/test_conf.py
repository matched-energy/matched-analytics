from ma.utils.conf import get_code_version


def test_get_code_version() -> None:
    version = get_code_version()
    assert version is not None
    assert version != "unknown"
