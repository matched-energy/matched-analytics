import os
from pathlib import Path

import pandas as pd

from ma.ofgem.schema_stations import stations_schema_on_load, transform_stations_schema
from ma.utils.pandas import apply_schema


def load(file_path: Path) -> pd.DataFrame:
    stations_raw = pd.read_csv(file_path, header=0)
    return apply_schema(stations_raw, stations_schema_on_load, transform_stations_schema)


def load_from_dir(dir: Path) -> pd.DataFrame:
    return pd.concat(
        [load(Path(entry.path)) for entry in os.scandir(dir) if entry.is_file() and entry.name.endswith(".csv")]
    )
