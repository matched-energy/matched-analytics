import argparse
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from ma.elexon.schema_metering_data import bmu_vols_schema_on_load, transform_bmu_vols_schema
from ma.utils.misc import truncate_string
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


def load_file(
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


def load_dir(
    input_dir: Path,
    bsc_lead_party_id: str,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    aggregate_bms: bool = True,
    filename_prefixes: Optional[list[str]] = None,
) -> pd.DataFrame:
    all_bmu_vols = [
        load_file(input_dir / entry.name, bm_regex=bm_regex, bm_ids=bm_ids, aggregate_bms=aggregate_bms)
        for entry in os.scandir(input_dir)
        if entry.is_file()
        and entry.name.endswith(".csv")
        and bsc_lead_party_id in entry.name
        and (filename_prefixes is None or any(entry.name.startswith(p) for p in filename_prefixes))
    ]
    return pd.concat(all_bmu_vols).sort_values("settlement_datetime")


def get_fig(metering_data: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for bm_unit_id in metering_data["bm_unit_id"].unique():
        bm_unit_data = metering_data[metering_data["bm_unit_id"] == bm_unit_id]

        fig.add_trace(
            go.Scatter(
                x=bm_unit_data["settlement_datetime"],
                y=bm_unit_data["bm_unit_metered_volume_mwh"],
                mode="lines",
                name=truncate_string(bm_unit_id),
            )
        )

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        title="bm_unit_metered_volume vs settlement_datetime",
        showlegend=True,
    )

    return fig


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("bsc_lead_party_id", type=str)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()

    metering_data = load_dir(input_dir=args.input_dir, bsc_lead_party_id=args.bsc_lead_party_id)
    metering_data.to_csv(args.output_path, index=False)
