import data.register
from ma.ofgem.stations import load_rego_stations_processed_from_dir


def test_load_from_dir() -> None:
    stations = load_rego_stations_processed_from_dir(data.register.REGO_ACCREDITED_STATIONS_DIR)
    assert "station_dnc_mw" in stations.df.columns
    assert len(stations.df) == 17
