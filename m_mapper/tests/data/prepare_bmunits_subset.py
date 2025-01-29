import json
import sys
from pathlib import Path
from typing import List, Optional

import httpx
import pandas as pd

SUBSET_BMUS = [  # Matching the biggest REGO generators
    "T_DRAXX-1",
    "T_DRAXX-2",
    "T_DRAXX-3",
    "T_DRAXX-4",
    "T_WLNYO-3",
    "T_WLNYO-4",
    "T_TKNEW-1",
    "T_TKNWW-1",
    "T_EAAO-1",
    "T_EAAO-2",
    "T_LARYW-1",
    "T_LARYW-2",
    "T_LARYW-3",
    "T_LARYW-4",
    "T_RCBKO-1",
    "T_RCBKO-2",
    "T_MOWEO-1",
    "T_MOWEO-2",
    "T_MOWEO-3",
    "T_BEATO-1",
    "T_BEATO-2",
    "T_BEATO-3",
    "T_BEATO-4",
    "T_HOWAO-3",
    "T_HOWBO-3",
    "T_WDNSO-1",
    "T_WDNSO-2",
    "T_HOWAO-2",
    "T_DDGNO-1",
    "T_DDGNO-2",
    "T_DDGNO-3",
    "T_DDGNO-4",
    "T_HOWAO-1",
    "T_HOWBO-1",
    "T_RMPNO-1",
    "T_RMPNO-2",
    "T_GANW-11",
    "T_GANW-22",
    "T_GANW-13",
    "T_GANW-24",
    "T_HOWBO-2",
    "T_GRGBW-1",
]


def fetch_bmus() -> List:
    response = httpx.get("https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all", timeout=10)
    response.raise_for_status()
    return response.json()


def persist_bmus(output_path, subset_bmus_raw):
    if output_path:
        with open(output_path, "w") as file:
            json.dump(subset_bmus_raw, file, indent=4)


def filter_bmus(all_bmus: List, bmu_ids: Optional[List] = None) -> List:
    if not bmu_ids:
        bmu_ids = SUBSET_BMUS
    all_bmus_df = pd.DataFrame(all_bmus)
    subset_bmus_df = all_bmus_df[all_bmus_df["elexonBmUnit"].isin(bmu_ids)]
    subset_bmus_raw = subset_bmus_df.to_dict(orient="records")
    return subset_bmus_raw


def main(output_path: Path) -> List:
    all_bmus_raw = fetch_bmus()
    subset_bmus_raw = filter_bmus(all_bmus_raw)
    persist_bmus(output_path, subset_bmus_raw)
    return subset_bmus_raw


if __name__ == "__main__":
    main(output_path=Path(sys.argv[1]))
