import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd


def filter_by_bm_regex(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    return df[df["BM Unit Id"].str.contains(pattern, regex=True)]


def filter_by_bm_ids(df: pd.DataFrame, ids: list[str]) -> pd.DataFrame:
    return df[df["BM Unit Id"].isin(ids)]


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
    bm_regex: str = "^2__",
    bm_ids: Optional[list] = None,
    group_bms: bool = True,
    prefixes: Optional[list[str]] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    list_of_hh_df = []
    for filename in os.listdir(input_dir):
        if not (
            filename.endswith(".csv")
            and bsc_lead_party_id in filename
            and (prefixes is None or any(filename.startswith(p) for p in prefixes))
        ):
            continue
        hh_df = segregate_import_exports(pd.read_csv(os.path.join(input_dir, filename)))
        # TODO: one-pass filtering
        if bm_regex:
            hh_df = filter_by_bm_regex(hh_df, bm_regex)
        if bm_ids:
            hh_df = filter_by_bm_ids(hh_df, bm_ids)
        if group_bms:
            hh_df = group_by_sp(hh_df)
        list_of_hh_df.append(hh_df)
    concatenated_df = concat_and_sort(list_of_hh_df)

    if output_path:
        concatenated_df.to_csv(output_path, index=False)
    return concatenated_df


if __name__ == "__main__":
    process_directory(input_dir=Path(sys.argv[1]), bsc_lead_party_id=sys.argv[2], output_path=Path(sys.argv[3]))
