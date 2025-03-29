from typing import Tuple
import pandas as pd


import plotly.graph_objects as go

from ma.utils.enums import SupplyTechEnum
from ma.utils.plotly import DEFAULT_PLOTLY_LAYOUT


def calculate_supply_surplus_deficit(supply: pd.Series, consumption: pd.Series) -> Tuple[pd.Series, pd.Series]:
    surplus = (supply - consumption).clip(lower=0)
    deficit = (consumption - supply).clip(lower=0)
    return surplus, deficit


def calculate_matching_score(deficit: pd.Series, consumption: pd.Series) -> pd.Series:
    return 1 - deficit / consumption


def plot_supply_consumption_matching(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

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
    fig.update_layout(**DEFAULT_PLOTLY_LAYOUT, overwrite=True)
    fig.update_layout(
        title="",
        yaxis=dict(title="MWh"),
        yaxis2=dict(title="Matching", overlaying="y", side="right", range=[0, 1]),
        showlegend=True,
    )
    return fig
