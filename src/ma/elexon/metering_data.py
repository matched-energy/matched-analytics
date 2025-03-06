import argparse
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from ma.elexon.schema_metering_data import metering_data_schema_on_load, transform_metering_data_schema
from ma.utils.misc import truncate_string
from ma.utils.pandas import apply_schema


def filter(
    metering_data_half_hourly: pd.DataFrame, bm_regex: Optional[str] = None, bm_ids: Optional[list] = None
) -> pd.DataFrame:
    mask = np.ones(len(metering_data_half_hourly), dtype=bool)
    if bm_ids:
        mask &= metering_data_half_hourly["bm_unit_id"].isin(bm_ids)
    if bm_regex:
        mask &= metering_data_half_hourly["bm_unit_id"].str.contains(bm_regex, regex=True)
    return metering_data_half_hourly[mask]


def segregate_import_exports(metering_data_half_hourly: pd.DataFrame) -> pd.DataFrame:
    updated = metering_data_half_hourly.copy()
    updated["bm_unit_metered_volume_+ve_mwh"] = updated["bm_unit_metered_volume_mwh"].clip(lower=0)
    updated["bm_unit_metered_volume_-ve_mwh"] = updated["bm_unit_metered_volume_mwh"].clip(upper=0)
    return updated


def load_file(
    processed_s0142_path: Path,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
) -> pd.DataFrame:
    processed_s0142 = pd.read_csv(processed_s0142_path)
    metering_data_half_hourly = apply_schema(
        processed_s0142, metering_data_schema_on_load, transform_metering_data_schema
    )
    metering_data_half_hourly = segregate_import_exports(metering_data_half_hourly)
    metering_data_half_hourly = filter(metering_data_half_hourly, bm_regex=bm_regex, bm_ids=bm_ids)
    return metering_data_half_hourly


def load_dir(
    processed_s0142_dir: Path,
    bsc_lead_party_id: str,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
    filename_prefixes: Optional[list[str]] = None,
) -> pd.DataFrame:
    metering_data_half_hourly = [
        load_file(processed_s0142_dir / entry.name, bm_regex=bm_regex, bm_ids=bm_ids)
        for entry in os.scandir(processed_s0142_dir)
        if entry.is_file()
        and entry.name.endswith(".csv")
        and bsc_lead_party_id in entry.name
        and (filename_prefixes is None or any(entry.name.startswith(p) for p in filename_prefixes))
    ]
    return pd.concat(metering_data_half_hourly).sort_values("settlement_datetime")


def get_fig(metering_data: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for bm_unit_id in metering_data["bm_unit_id"].unique():
        bm_unit_data = metering_data[metering_data["bm_unit_id"] == bm_unit_id]

        fig.add_trace(
            go.Scatter(
                x=bm_unit_data.index,
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

    metering_data = load_dir(processed_s0142_dir=args.input_dir, bsc_lead_party_id=args.bsc_lead_party_id)
    metering_data.to_csv(args.output_path, index=False)
