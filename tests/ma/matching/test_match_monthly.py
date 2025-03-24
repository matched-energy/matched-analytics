from typing import Tuple

import pandas as pd
from pytest import approx
import plotly.graph_objects as go
import data.register
from ma.matching.match_monthly import (
    MatchMonthly,
    calculate_matching_score,
    calculate_supply_surplus_deficit,
    make_match_monthly,
)
from ma.ofgem.regos import RegosByTechMonthHolder
from ma.retailer.consumption import ConsumptionMonthly


def test_calculate_supply_surplus_deficit() -> None:
    surplus, deficit = calculate_supply_surplus_deficit(supply=pd.Series([0, 1, 2]), consumption=pd.Series([2, 1, 0]))
    assert list(surplus) == [0, 0, 2]
    assert list(deficit) == [2, 0, 0]


def test_calculate_matching_score() -> None:
    score = calculate_matching_score(deficit=pd.Series([0, 1, 2]), consumption=pd.Series([2, 2, 2]))
    assert list(score) == [1, 0.5, 0]


def setup() -> Tuple[RegosByTechMonthHolder, ConsumptionMonthly, MatchMonthly]:
    supply = RegosByTechMonthHolder(data.register.REGOS_BY_TECH_MONTH_HOLDER)
    consumption = ConsumptionMonthly(data.register.CONSUMPTION_BY_MONTH)
    return supply, consumption, make_match_monthly(consumption=consumption, supply=supply)


def test_match_monthly() -> None:
    _, consumption, match = setup()
    match_df = match.df

    assert len(match_df) == 12
    assert match_df["consumption_mwh"].sum() == consumption["consumption_mwh"].sum()
    pd.testing.assert_series_equal(
        match_df["supply_total_mwh"],
        match_df.filter(regex="supply_.*_mwh")
        .drop(columns=["supply_total_mwh", "supply_surplus_mwh", "supply_deficit_mwh"])
        .sum(axis=1)
        .rename("supply_total_mwh"),
    )
    pd.testing.assert_series_equal(
        match_df["rego_holder_count"].reset_index(drop=True),
        pd.Series([1] * 12),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        match_df["supply_solar_mwh"].reset_index(drop=True),
        pd.Series([0.0] * 12),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        match_df["supply_solar_station_count"].reset_index(drop=True),
        pd.Series([0] * 12),
        check_names=False,
    )

    assert match_df["consumption_mwh"].sum() == approx(1257e3, rel=1e3)
    assert match_df["supply_total_mwh"].sum() == approx(868e3, rel=1e3)
    assert match_df["supply_deficit_mwh"].sum() == approx(394, rel=1e3)
    assert match_df["supply_surplus_mwh"].sum() == approx(5e3, rel=1e3)
    assert match_df["matching_score"].mean() == approx(0.672, rel=1e-3)


def test_match_plot() -> None:
    _, _, match = setup()
    fig = match.plot()
    assert isinstance(fig, go.Figure)


def test_match_monthly_annualised() -> None:
    _, _, match = setup()
    match_annualised = match.transform_to_match_monthly_annualised()
    match_annualised_df = match_annualised.df

    assert len(match_annualised_df) == 1
    assert match_annualised_df["consumption_mwh"].iloc[0] == match["consumption_mwh"].sum()

    assert match_annualised_df["supply_biomass_station_max"].iloc[0] == match["supply_biomass_station_count"].max()
    assert match_annualised_df["matching_score"].iloc[0] == approx(0.6864, rel=1e-4)
