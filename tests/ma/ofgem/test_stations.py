import data.register
from ma.ofgem.stations import load_from_dir


def test_load_from_dir() -> None:
    stations = load_from_dir(data.register.REGO_ACCREDITED_STATIONS_DIR)
    assert "station_dnc_mw" in stations.columns
    assert len(stations) == 17
