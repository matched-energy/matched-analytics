import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def filter(hh_df: pd.DataFrame, bm_regex: Optional[str] = None, bm_ids: Optional[list] = None) -> pd.DataFrame:
    mask = np.ones(len(hh_df), dtype=bool)
    if bm_ids:
        mask &= hh_df["BM Unit Id"].isin(bm_ids)
    if bm_regex:
        mask &= hh_df["BM Unit Id"].str.contains(bm_regex, regex=True)
    return hh_df[mask]


def group_by_sp(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["Settlement Date", "Settlement Period"]).sum().reset_index()


def segregate_import_exports(df: pd.DataFrame) -> pd.DataFrame:
    updated_df = df.copy()
    updated_df["BM Unit Metered Volume: +ve"] = updated_df["BM Unit Metered Volume"].clip(lower=0)
    updated_df["BM Unit Metered Volume: -ve"] = updated_df["BM Unit Metered Volume"].clip(upper=0)
    return updated_df


def concat_and_sort(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs)
    df["Settlement Date"] = pd.to_datetime(df["Settlement Date"], dayfirst=True)
    df = df.sort_values(["Settlement Date", "Settlement Period"])
    df["Settlement Datetime"] = df["Settlement Date"] + (df["Settlement Period"] - 1) * pd.Timedelta(minutes=30)
    return df


def process_directory(
    input_dir: Path,
    bsc_lead_party_id: str,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    aggregate_bms: bool = True,
    prefixes: Optional[list[str]] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    list_of_hh_df = []
    for entry in os.scandir(input_dir):
        if (
            entry.is_file()
            and entry.name.endswith(".csv")
            and bsc_lead_party_id in entry.name
            and (prefixes is None or any(entry.name.startswith(p) for p in prefixes))
        ):
            hh_df = segregate_import_exports(pd.read_csv(os.path.join(input_dir, entry.name)))
            hh_df = filter(hh_df, bm_regex=bm_regex, bm_ids=bm_ids)
            if aggregate_bms:
                hh_df = group_by_sp(hh_df)
            list_of_hh_df.append(hh_df)
    concatenated_df = concat_and_sort(list_of_hh_df)

    if output_path:
        concatenated_df.to_csv(output_path, index=False)
    return concatenated_df


if __name__ == "__main__":
    process_directory(input_dir=Path(sys.argv[1]), bsc_lead_party_id=sys.argv[2], output_path=Path(sys.argv[3]))
