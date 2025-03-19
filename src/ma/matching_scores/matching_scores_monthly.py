from __future__ import annotations

from typing import Dict

import pandas as pd
import pandera as pa
import plotly.graph_objects as go

from ma.ofgem.regos import RegosByTechMonthHolder
from ma.upsampled_supply_hh.consumption import ConsumptionMonthly
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE
from ma.utils.plotly import DEFAULT_PLOTLY_LAYOUT


class MonthlyMatching(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        # TODO #27 - derive schema from SupplyTechEnum 
        timestamp             =CS(check=pa.Index(DTE(dayfirst=False))),
        supply_mwh_biomass    =CS(check=pa.Column(float)),
        supply_mwh_unknown    =CS(check=pa.Column(float)),
        supply_mwh_wind       =CS(check=pa.Column(float)),
        station_count_biomass =CS(check=pa.Column(float)),
        station_count_unknown =CS(check=pa.Column(float)),
        station_count_wind    =CS(check=pa.Column(float)),
        supply_mwh_total      =CS(check=pa.Column(float)),
        current_holder_count  =CS(check=pa.Column(float)),
        consumption_mwh    =CS(check=pa.Column(float)),
        deficit_mwh        =CS(check=pa.Column(float)),
        surplus_mwh        =CS(check=pa.Column(float)),
        matching_score     =CS(check=pa.Column(float)),
    )
    # fmt: on
    from_file_skiprows = 1
    from_file_with_index = True

    def plot(self) -> go.Figure:
        fig = go.Figure()
        df = self.df

        # Consumption
        fig.add_trace(go.Scatter(x=df.index, y=df["consumption_mwh"], mode="lines", name="Consumption"))

        # Supply
        for supply in ["biomass", "unknown", "wind"]:  # TODO #17
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df[f"supply_mwh_{supply}"],
                    mode="lines",
                    name=supply,
                    stackgroup="supply-by-tech",
                )
            )
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["supply_mwh_total"],
                mode="lines",
                name="total",
            )
        )

        # Matching score
        fig.add_trace(go.Scatter(x=df.index, y=df["matching_score"], mode="lines", name="matching score", yaxis="y2"))

        # Update fig
        fig.update_layout(**DEFAULT_PLOTLY_LAYOUT)
        fig.update_layout(
            title="",
            yaxis=dict(title="MWh"),
            yaxis2=dict(title="Matching", overlaying="y", side="right", range=[0, 1]),
            showlegend=True,
        )
        return fig


def calculate_monthly_matching_scores(
    consumption: ConsumptionMonthly, supply: RegosByTechMonthHolder
) -> MonthlyMatching:
    supply_df = supply.df
    supply_df["supply_mwh"] = supply_df["rego_gwh"] * 1000
    supply_df["current_holder_count"] = supply_df.groupby("month")["current_holder"].transform("nunique")
    supply_df["supply_mwh_total"] = supply_df.groupby("month")["supply_mwh"].sum()

    supply_pivoted = supply_df.reset_index().pivot(
        index="month", columns="tech", values=["supply_mwh", "station_count"]
    )
    supply_pivoted.columns = pd.Index([f"{col[0]}_{col[1]}" for col in supply_pivoted.columns])

    supply_pivoted = supply_pivoted.merge(
        supply_df[["supply_mwh_total", "current_holder_count"]], left_index=True, right_on="month"
    )

    df = pd.merge(supply_pivoted, consumption.df, left_index=True, right_index=True)
    df["deficit_mwh"] = (df["consumption_mwh"] - df["supply_mwh_total"]).clip(lower=0)
    df["surplus_mwh"] = (df["supply_mwh_total"] - df["consumption_mwh"]).clip(lower=0)
    df["matching_score"] = 1 - df["deficit_mwh"] / df["consumption_mwh"]
    return MonthlyMatching(df)
