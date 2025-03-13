import pandas as pd
import pytest
from pytest import approx

import data.register
from ma.ofgem.enums import RegoCompliancePeriod as CP
from ma.ofgem.enums import RegoStatus as Status
from ma.ofgem.regos import RegosProcessed, RegosRaw


def get_regos_processed() -> RegosProcessed:
    return RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET).transform_to_regos_processed()


def test_regos_raw_load() -> None:
    regos = RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos.df()) == 327
    assert set(regos["station_name"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["technology_group"]) == set(["Biomass", "Off-shore Wind"])


def test_regos_raw_transform_to_regos_processed() -> None:
    regos = get_regos_processed()
    assert set(regos["tech"]) == set(["biomass", "wind"])
    assert regos["rego_gwh"].sum() == approx(18549.931)


def test_regos_procssed_filter() -> None:
    regos = get_regos_processed()

    assert len(regos.df()) == 327
    assert len(regos.filter(holders=None, statuses=[Status.REDEEMED])) == 220
    assert len(regos.filter(holders=None, statuses=[Status.ISSUED, Status.RETIRED, Status.EXPIRED])) == 107


def test_regos_processed_filter_reporting_period() -> None:
    regos = get_regos_processed()
    regos_filtered = regos.filter(reporting_period=CP.CP20)
    assert len(regos_filtered) == 0  # no regos with start_year_month in Apr 2021 in the subset

    # Create new df and insert a new row for CP20 (Apr 2021-Mar 2022)
    new_row = regos.df().iloc[0].copy()
    new_row["start_year_month"], new_row["end_year_month"] = pd.Timestamp("2021-04-01"), pd.Timestamp("2022-03-31")
    regos_filtered = RegosProcessed(pd.DataFrame([new_row])).filter(reporting_period=CP.CP20)
    assert len(regos_filtered) == 1


def test_regos_processed_groupby_station() -> None:
    regos = RegosProcessed(get_regos_processed().filter(statuses=[Status.REDEEMED]))
    regos_grouped = regos.groupby_station()
    assert len(regos_grouped) == 3
    assert regos_grouped["rego_gwh"].sum() == approx(17114.284)
    assert set(regos_grouped["tech"]) == set(["biomass", "wind"])


def test_regos_processed_groupby_station_NON_UNIQUE() -> None:
    regos_df = get_regos_processed().df()
    with pytest.raises(AssertionError, match="have non-unique values"):
        RegosProcessed(pd.concat([regos_df, regos_df.copy()], axis=0)).groupby_station()


def test_regos_processed_groupby_tech_month_holder() -> None:
    filtered_regos = RegosProcessed(get_regos_processed().filter(holders=["British Gas Trading Ltd"]))
    regos_by_holder = filtered_regos.groupby_tech_month_holder()

    # check shape
    assert set(regos_by_holder["tech"]) == set(["biomass", "wind"])
    assert len(regos_by_holder) == 9

    # check values
    biomass_2022_04 = regos_by_holder[
        (regos_by_holder["tech"] == "biomass") & (regos_by_holder.index == pd.Period("2022-04"))
    ]["rego_gwh"].values[0]
    biomass_2023_03 = regos_by_holder[
        (regos_by_holder["tech"] == "biomass") & (regos_by_holder.index == pd.Period("2023-03"))
    ]["rego_gwh"].values[0]
    wind_2023_03 = regos_by_holder[
        (regos_by_holder["tech"] == "wind") & (regos_by_holder.index == pd.Period("2023-03"))
    ]["rego_gwh"].values[0]

    assert biomass_2022_04 == 37.199
    assert biomass_2023_03 == 500.000
    assert wind_2023_03 == 314.075


def test_regos_processed_expand_multi_month_certificates() -> None:
    """Test that dummy df row spanning multiple months get properly expanded and distributed."""

    test_regos_df = get_regos_processed().df().iloc[0]
    test_regos_df.loc["tech"] = "biomass"
    test_regos_df.loc["current_holder"] = "Test Company"
    test_regos_df.loc["station_name"] = "Test Station"
    test_regos_df.loc["rego_gwh"] = 3.0
    test_regos_df.loc["start_year_month"] = pd.Timestamp("2023-01-01")
    test_regos_df.loc["end_year_month"] = pd.Timestamp("2023-04-01")
    test_regos_df.loc["period_months"] = 3
    test_regos_df.loc["output_period"] = "01/01/2023 - 31/03/2023"
    test_regos = RegosProcessed(pd.DataFrame([test_regos_df]))
    result = test_regos.groupby_tech_month_holder()

    # Check the result, including each of the three expected months
    assert len(result) == 3
    months = [pd.Period("2023-01"), pd.Period("2023-02"), pd.Period("2023-03")]
    for month in months:
        month_data = result[result.index == month]
        assert not month_data.empty, f"Missing data for {month}"
        assert month_data["tech"].iloc[0] == "biomass"
        assert month_data["current_holder"].iloc[0] == "Test Company"
        assert month_data["rego_gwh"].iloc[0] == 1
        assert month_data["station_count"].iloc[0] == 1
        assert month_data["station_count"].iloc[0] == 1
