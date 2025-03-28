from typing import Tuple
import data.register
import pandas as pd
import plotly.graph_objects as go
import pytest

from ma.matching.match_half_hourly import MatchHalfHourly, make_match_half_hourly
from ma.retailer.consumption import ConsumptionHalfHourly
from ma.retailer.supply_hh import UpsampledSupplyHalfHourly


def setup_upsampled_supply() -> UpsampledSupplyHalfHourly:
    supply_df = pd.read_csv(data.register.SUPPLY_BY_HOUR)
    supply_df = supply_df.set_index("timestamp")
    return UpsampledSupplyHalfHourly(supply_df)


def setup() -> Tuple[UpsampledSupplyHalfHourly, ConsumptionHalfHourly, MatchHalfHourly]:
    hh_supply = setup_upsampled_supply()
    hh_consumption = ConsumptionHalfHourly(data.register.CONSUMPTION_BY_HOUR)
    return hh_supply, hh_consumption, make_match_half_hourly(supply=hh_supply, consumption=hh_consumption)


def test_match_half_hourly_index_mismatch() -> None:
    supply, consumption, _ = setup()
    modified_df = consumption.df.drop(consumption.df.index[0])
    modified_consumption = ConsumptionHalfHourly(modified_df)
    with pytest.raises(ValueError):
        make_match_half_hourly(supply=supply, consumption=modified_consumption)


def test_match_half_hourly() -> None:
    supply, consumption, match = setup()
    match_df = match.df

    assert not match_df.empty
    assert match_df.index.min() >= pd.Timestamp("2023-02-01 00:00:00")
    assert match_df.index.max() < pd.Timestamp("2023-02-02 00:00:00")

    assert not match_df["supply_total_mwh"].isna().any()
    assert not match_df["consumption_mwh"].isna().any()

    assert match_df["consumption_mwh"].sum() == consumption["consumption_mwh"].sum()
    pd.testing.assert_series_equal(
        match_df["supply_total_mwh"],
        match_df.filter(regex="supply_.*_mwh")
        .drop(columns=["supply_total_mwh", "supply_surplus_mwh", "supply_deficit_mwh"])
        .sum(axis=1)
        .rename("supply_total_mwh"),
    )

    pd.testing.assert_series_equal(
        match_df["supply_solar_mwh"].reset_index(drop=True),
        pd.Series([0.0] * 48),
        check_names=False,
    )

    # Scores should be between 0 and 1
    assert (match_df["matching_score"] >= 0).all()
    assert (match_df["matching_score"] <= 1).all()

    # Check first timestep of supply, consumption and matching score
    first_timestep_supply = supply.df.iloc[0]
    first_timestep_consumption = consumption.df.iloc[0]
    first_timestep_matching_score = match_df.iloc[0]

    assert first_timestep_supply["supply_mwh"] == 12620.197276081411
    assert first_timestep_consumption["consumption_mwh"] == 10000

    deficit = max(0, first_timestep_consumption["consumption_mwh"] - first_timestep_supply["supply_mwh"])
    expected_matching_score = 1 - deficit / first_timestep_consumption["consumption_mwh"]
    assert first_timestep_matching_score["matching_score"] == expected_matching_score


def test_match_half_hourly_annualised() -> None:
    _, _, match = setup()
    match_annualised = match.transform_to_match_half_hourly_annualised()
    match_annualised_df = match_annualised.df

    assert len(match_annualised_df) == 1
    assert match_annualised_df["consumption_mwh"].iloc[0] == match["consumption_mwh"].sum()

    assert match_annualised_df["supply_biomass_mwh"].iloc[0] == match["supply_biomass_mwh"].sum()
    assert match_annualised_df["matching_score"].iloc[0] == match["matching_score"].mean()


def test_match_half_hourly_plot() -> None:
    _, _, match = setup()
    fig = match.plot()
    assert isinstance(fig, go.Figure)
