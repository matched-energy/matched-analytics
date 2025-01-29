import json
from pathlib import Path
from typing import Optional

import pandas as pd

import ma.elexon.api.endpoints


def bmus(local_path: Optional[Path]) -> pd.DataFrame:
    if local_path:
        with open(local_path, "r") as file:
            bmus_raw = json.load(file)
    else:
        bmus_raw = ma.elexon.api.endpoints.bmunits_all()
    bmrs_bmus = pd.DataFrame(bmus_raw)
    bmrs_bmus["generationCapacity"] = bmrs_bmus["generationCapacity"].astype(float)
    bmrs_bmus["demandCapacity"] = bmrs_bmus["demandCapacity"].astype(float)
    return bmrs_bmus
