import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import ma.elexon.S0142.schema_S0142
from ma.utils.pandas import apply_schema


def filter(hh_df: pd.DataFrame, bm_regex: Optional[str] = None, bm_ids: Optional[list] = None) -> pd.DataFrame:
    mask = np.ones(len(hh_df), dtype=bool)
    if bm_ids:
        mask &= hh_df["bm_unit_id"].isin(bm_ids)
    if bm_regex:
        mask &= hh_df["bm_unit_id"].str.contains(bm_regex, regex=True)
    return hh_df[mask]


def group_by_sp(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["settlement_date", "settlement_period"]).sum().reset_index()


def segregate_import_exports(df: pd.DataFrame) -> pd.DataFrame:
    updated_df = df.copy()
    updated_df["bm_unit_metered_volume_+ve_mwh"] = updated_df["bm_unit_metered_volume_mwh"].clip(lower=0)
    updated_df["bm_unit_metered_volume_-ve_mwh"] = updated_df["bm_unit_metered_volume_mwh"].clip(upper=0)
    return updated_df


def concat_and_sort(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs)
    df["settlement_date"] = pd.to_datetime(df["settlement_date"], dayfirst=True)
    df = df.sort_values(["settlement_date", "settlement_period"])
    df["settlement_datetime"] = df["settlement_date"] + (df["settlement_period"] - 1) * pd.Timedelta(minutes=30)
    return df


def load(
    file_path: Path,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    aggregate_bms: bool = True,
) -> pd.DataFrame:
    bm_vols_raw = pd.read_csv(file_path)
    bmu_vols = apply_schema(bm_vols_raw, ma.elexon.S0142.schema_S0142.schema_on_load)
    bmu_vols = segregate_import_exports(bmu_vols)
    bmu_vols = filter(bmu_vols, bm_regex=bm_regex, bm_ids=bm_ids)
    if aggregate_bms:
        bmu_vols = group_by_sp(bmu_vols)
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
    concatenated_df = concat_and_sort(all_bmu_vols)

    # Persist
    if output_path:
        concatenated_df.to_csv(output_path, index=False)

    # Return
    return concatenated_df


if __name__ == "__main__":
    process_directory(input_dir=Path(sys.argv[1]), bsc_lead_party_id=sys.argv[2], output_path=Path(sys.argv[3]))
