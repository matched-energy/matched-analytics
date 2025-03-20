from __future__ import annotations

from typing import Dict

import pandas as pd
import pandera as pa
import plotly.graph_objects as go

from ma.ofgem.regos import RegosByTechMonthHolder
from ma.retailer.consumption import ConsumptionMonthly
from ma.utils.enums import SupplyTechEnum
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE
from ma.utils.plotly import DEFAULT_PLOTLY_LAYOUT


class MatchMonthly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        timestamp                   =CS(check=pa.Index(DTE(dayfirst=False))),
        supply_total_mwh            =CS(check=pa.Column(float)),
        rego_holder_count           =CS(check=pa.Column(int)),  
        supply_biomass_mwh          =CS(check=pa.Column(float)), 
        supply_biomass_station_count=CS(check=pa.Column(int)),   
        supply_hydro_mwh            =CS(check=pa.Column(float)),  
        supply_hydro_station_count  =CS(check=pa.Column(int)),   
        supply_other_mwh            =CS(check=pa.Column(float)),
        supply_other_station_count  =CS(check=pa.Column(int)),
        supply_solar_mwh            =CS(check=pa.Column(float)),
        supply_solar_station_count  =CS(check=pa.Column(int)),
        supply_wind_mwh             =CS(check=pa.Column(float)),  
        supply_wind_station_count   =CS(check=pa.Column(int)),
        consumption_mwh             =CS(check=pa.Column(float)),
        supply_deficit_mwh          =CS(check=pa.Column(float)),  
        supply_surplus_mwh          =CS(check=pa.Column(float)),  
        matching_score              =CS(check=pa.Column(float)),
    )
    # fmt: on
    from_file_skiprows = 1
    from_file_with_index = True
    validate_column_names = True

    def plot(self) -> go.Figure:
        fig = go.Figure()
        df = self.df

        # Consumption
        fig.add_trace(go.Scatter(x=df.index, y=df["consumption_mwh"], mode="lines", name="Consumption"))

        # Supply
        for tech in SupplyTechEnum.alphabetical_renewables():
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df[f"supply_{tech}_mwh"],
                    mode="lines",
                    name=tech,
                    stackgroup="supply-by-tech",
                )
            )
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["supply_total_mwh"],
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


def make_match_monthly(consumption: ConsumptionMonthly, supply: RegosByTechMonthHolder) -> MatchMonthly:
    supply_df = supply.df
    supply_df["supply_mwh"] = supply_df["rego_gwh"] * 1000

    supply_pivoted = supply_df.groupby("month").agg(
        supply_total_mwh=("supply_mwh", "sum"),
        rego_holder_count=("current_holder", "nunique"),
    )
    for tech in SupplyTechEnum.alphabetical_renewables():
        supply_pivoted = supply_pivoted.join(
            pd.DataFrame(
                {
                    f"supply_{tech}_mwh": supply_df[supply_df["tech"] == tech]["supply_mwh"],
                    f"supply_{tech}_station_count": supply_df[supply_df["tech"] == tech]["station_count"],
                }
            )
        ).fillna(0)

    match = supply_pivoted.join(consumption.df)
    match["supply_deficit_mwh"] = (match["consumption_mwh"] - match["supply_total_mwh"]).clip(lower=0)
    match["supply_surplus_mwh"] = (match["supply_total_mwh"] - match["consumption_mwh"]).clip(lower=0)
    match["matching_score"] = 1 - match["supply_deficit_mwh"] / match["consumption_mwh"]
    return MatchMonthly(match)
