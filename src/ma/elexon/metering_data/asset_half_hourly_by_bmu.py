from typing import Dict, Optional

import numpy as np
import pandas as pd
import pandera as pa
import plotly.graph_objects as go

from ma.elexon.metering_data.metering_data_rollup import MeteringDataHalfHourly, MeteringDataHalfHourlyType
from ma.utils.misc import truncate_string
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE

MeteringDataHalfHourlyByBmuType = pd.DataFrame


class MeteringDataHalfHourlyByBmu(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        bsc							                =CS(check=pa.Column(str)),
        settlement_date							    =CS(check=pa.Column(str)),
        settlement_period						    =CS(check=pa.Column(int)),
        settlement_run_type						    =CS(check=pa.Column(str)),
        bm_unit_id							        =CS(check=pa.Column(str)),
        information_imbalance_cashflow	            =CS(check=pa.Column(float)),
        bm_unit_period_non_delivery_charge          =CS(check=pa.Column(float)),
        period_fpn							        =CS(check=pa.Column(float)),
        period_bm_unit_balancing_services_volume    =CS(check=pa.Column(float)),
        period_information_imbalance_volume	        =CS(check=pa.Column(float)),
        period_expected_metered_volume		        =CS(check=pa.Column(float)),
        bm_unit_metered_volume_mwh	    	        =CS(check=pa.Column(float)),
        period_bm_unit_non_delivered_bid_volume	    =CS(check=pa.Column(str)),
        period_bm_unit_non_delivered_offer_volume   =CS(check=pa.Column(str)),
        transmission_loss_factor                    =CS(check=pa.Column(float)),
        transmission_loss_multiplier                =CS(check=pa.Column(float)),
        trading_unit_name                           =CS(check=pa.Column(str)),
        total_trading_unit_metered_volume           =CS(check=pa.Column(float)),
        bm_unit_applicable_balancing_services_volume=CS(check=pa.Column(float)),
        period_supplier_bm_unit_delivered_volume  	=CS(check=pa.Column(float)),
        period_supplier_bm_unit_non_bm_absvd_volume =CS(check=pa.Column(float)),
        settlement_datetime                         =CS(check=pa.Column(DTE(dayfirst=False))), # TODO - make index
    )

    # fmt: on


def filter(
    half_hourly_by_bmu: MeteringDataHalfHourlyType, bm_regex: Optional[str] = None, bm_ids: Optional[list] = None
) -> MeteringDataHalfHourlyType:
    mask = np.ones(len(half_hourly_by_bmu), dtype=bool)
    if bm_ids:
        mask &= half_hourly_by_bmu["bm_unit_id"].isin(bm_ids)
    if bm_regex:
        mask &= half_hourly_by_bmu["bm_unit_id"].str.contains(bm_regex, regex=True)
    return half_hourly_by_bmu[mask]


def segregate_import_exports(half_hourly_by_bmu: MeteringDataHalfHourlyType) -> MeteringDataHalfHourlyType:
    updated = half_hourly_by_bmu.copy()
    updated["bm_unit_metered_volume_+ve_mwh"] = updated["bm_unit_metered_volume_mwh"].clip(lower=0)
    updated["bm_unit_metered_volume_-ve_mwh"] = updated["bm_unit_metered_volume_mwh"].clip(upper=0)
    return updated


def rollup_bmus(half_hourly_by_bmu: MeteringDataHalfHourlyType) -> pd.DataFrame:
    grouped = half_hourly_by_bmu.groupby("settlement_datetime")[
        [
            "period_bm_unit_balancing_services_volume",
            "period_information_imbalance_volume",
            "period_expected_metered_volume",
            "bm_unit_metered_volume_mwh",
            "bm_unit_applicable_balancing_services_volume",
            "period_supplier_bm_unit_delivered_volume",
            "period_supplier_bm_unit_non_bm_absvd_volume",
            "bm_unit_metered_volume_+ve_mwh",
            "bm_unit_metered_volume_-ve_mwh",
        ]
    ].sum()
    grouped["bmu_count"] = len(half_hourly_by_bmu["bm_unit_id"].unique())
    grouped.reset_index(inplace=True)  # TODO .set_index("settlement_datetime", drop=True, inplace=True)
    return grouped


def transform_to_half_hourly(
    half_hourly_by_bmu: MeteringDataHalfHourlyByBmuType,
    bm_regex: Optional[str] = "^2__",
    bm_ids: Optional[list] = None,
) -> MeteringDataHalfHourlyType:
    """Return daily_by_bsc metering data"""
    output = segregate_import_exports(half_hourly_by_bmu)
    output = filter(output, bm_regex=bm_regex, bm_ids=bm_ids)
    output = rollup_bmus(output)
    return MeteringDataHalfHourly.from_dataframe(output)


def get_fig(half_hourly_by_bmu: MeteringDataHalfHourlyType) -> go.Figure:
    fig = go.Figure()

    for bm_unit_id in half_hourly_by_bmu["bm_unit_id"].unique():
        bm_unit_data = half_hourly_by_bmu[half_hourly_by_bmu["bm_unit_id"] == bm_unit_id]

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
