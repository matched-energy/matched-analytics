import os
from pathlib import Path

import pandas as pd

from ma.ofgem.schema_stations import STATIONS_SCHEMA
from ma.utils.pandas import apply_schema


def load(file_path: Path) -> pd.DataFrame:
    stations_raw = pd.read_csv(file_path, header=0)
    stations = apply_schema(stations_raw, STATIONS_SCHEMA)
    stations["station_dnc_mw"] = stations["station_dnc"] / 1e3
    stations.drop(columns=["station_dnc"])
    return stations


def load_from_dir(dir: Path) -> pd.DataFrame:
    return pd.concat(
        [load(Path(entry.path)) for entry in os.scandir(dir) if entry.is_file() and entry.name.endswith(".csv")]
    )
