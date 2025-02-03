import json
from pathlib import Path

import pandas as pd

import ma.elexon.schema_bmus
from ma.utils.pandas import apply_schema


def load(local_path: Path) -> pd.DataFrame:
    with open(local_path, "r") as file:
        bmus_raw = pd.DataFrame(json.load(file))
    return apply_schema(bmus_raw, ma.elexon.schema_bmus.schema_bmus_on_load)
