import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ma.elexon.S0142.schema_S0142 import bmu_vols_schema_on_load, transform_bmu_vols_schema
from ma.utils.pandas import apply_schema


def filter(hh_df: pd.DataFrame, bm_regex: Optional[str] = None, bm_ids: Optional[list] = None) -> pd.DataFrame:
    mask = np.ones(len(hh_df), dtype=bool)
    if bm_ids:
        mask &= hh_df["bm_unit_id"].isin(bm_ids)
    if bm_regex:
        mask &= hh_df["bm_unit_id"].str.contains(bm_regex, regex=True)
    return hh_df[mask]


def group_by_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df_grouped = df.groupby("settlement_datetime").sum(numeric_only=True).reset_index()
    df_grouped["bm_unit_id"] = ",".join(df["bm_unit_id"].unique())
    return df_grouped


def segregate_import_exports(df: pd.DataFrame) -> pd.DataFrame:
    updated_df = df.copy()
    updated_df["bm_unit_metered_volume_+ve_mwh"] = updated_df["bm_unit_metered_volume_mwh"].clip(lower=0)
    updated_df["bm_unit_metered_volume_-ve_mwh"] = updated_df["bm_unit_metered_volume_mwh"].clip(upper=0)
    return updated_df


def load(
    file_path: Path,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    aggregate_bms: bool = True,
) -> pd.DataFrame:
    bm_vols_raw = pd.read_csv(file_path)
    bmu_vols = apply_schema(bm_vols_raw, bmu_vols_schema_on_load, transform_bmu_vols_schema)
    bmu_vols = segregate_import_exports(bmu_vols)
    bmu_vols = filter(bmu_vols, bm_regex=bm_regex, bm_ids=bm_ids)
    if aggregate_bms:
        bmu_vols = group_by_datetime(bmu_vols)
    return bmu_vols


def process_directory(
    input_dir: Path,
    bsc_lead_party_id: str,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    aggregate_bms: bool = True,
    prefixes: Optional[list[str]] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    # Process all files
    all_bmu_vols = [
        load(input_dir / entry.name, bm_regex=bm_regex, bm_ids=bm_ids, aggregate_bms=aggregate_bms)
        for entry in os.scandir(input_dir)
        if entry.is_file()
        and entry.name.endswith(".csv")
        and bsc_lead_party_id in entry.name
        and (prefixes is None or any(entry.name.startswith(p) for p in prefixes))
    ]

    # Concatenate
    concatenated_df = pd.concat(all_bmu_vols).sort_values("settlement_datetime")

    # Persist
    if output_path:
        concatenated_df.to_csv(output_path, index=False)

    # Return
    return concatenated_df


if __name__ == "__main__":
    process_directory(input_dir=Path(sys.argv[1]), bsc_lead_party_id=sys.argv[2], output_path=Path(sys.argv[3]))
