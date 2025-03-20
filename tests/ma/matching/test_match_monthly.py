from typing import Tuple

import pandas as pd
from pytest import approx

import data.register
from ma.matching.match_monthly import MatchMonthly, make_match_monthly
from ma.ofgem.regos import RegosByTechMonthHolder
from ma.upsampled_supply_hh.consumption import ConsumptionMonthly


def setup() -> Tuple[RegosByTechMonthHolder, ConsumptionMonthly, MatchMonthly]:
    supply = RegosByTechMonthHolder(data.register.REGOS_BY_TECH_MONTH_HOLDER)
    consumption = ConsumptionMonthly(data.register.CONSUMPTION_BY_MONTH)
    return supply, consumption, make_match_monthly(consumption, supply)


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
    assert match_df["matching_score"].mean() == approx(0.672, rel=1e-3)


def test_match_plot() -> None:
    _, _, match = setup()
    match.plot()
