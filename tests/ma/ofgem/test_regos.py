from typing import List, TypedDict

import pandas as pd
import pytest
from pytest import approx

import data.register
from ma.ofgem.enums import RegoCompliancePeriod as CP
from ma.ofgem.enums import RegoStatus as Status
from ma.ofgem.regos import RegosProcessed, RegosRaw


def get_regos_raw() -> RegosRaw:
    return RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET)


def get_regos_processed() -> RegosProcessed:
    return get_regos_raw().transform_to_regos_processed()


def test_regos_raw_load() -> None:
    regos = RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos.df) == 327
    assert set(regos["station_name"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["technology_group"]) == set(["Biomass", "Off-shore Wind"])


def test_regos_raw_transform_to_regos_processed() -> None:
    regos = get_regos_processed()
    assert set(regos["tech"]) == set(["biomass", "wind"])
    assert regos["rego_mwh"].sum() == approx(18549931.0)


def test_regos_processed_filter() -> None:
    regos = get_regos_processed()

    assert len(regos.df) == 327
    assert len(regos.filter(holders=None, statuses=[Status.REDEEMED]).df) == 220
    assert len(regos.filter(holders=None, statuses=[Status.ISSUED, Status.RETIRED, Status.EXPIRED]).df) == 107


def test_regos_processed_filter_reporting_period() -> None:
    regos = get_regos_processed()
    regos_filtered = regos.filter(reporting_period=CP.CP20)
    assert len(regos_filtered.df) == 0  # no regos with start_year_month in Apr 2021 in the subset

    # Create new df and insert a new row for CP20 (Apr 2021-Mar 2022)
    new_row = regos.df.iloc[0].copy()
    new_row["start_year_month"], new_row["end_year_month"] = pd.Timestamp("2021-04-01"), pd.Timestamp("2022-03-31")
    regos_filtered = RegosProcessed(pd.DataFrame([new_row])).filter(reporting_period=CP.CP20)
    assert len(regos_filtered.df) == 1


def test_regos_processed_groupby_station() -> None:
    regos = get_regos_processed().filter(statuses=[Status.REDEEMED])
    regos_grouped = regos.groupby_station()
    assert len(regos_grouped) == 3
    assert regos_grouped["rego_mwh"].sum() == approx(17114284.0)
    assert set(regos_grouped["tech"]) == set(["biomass", "wind"])


def test_regos_processed_groupby_station_NON_UNIQUE() -> None:
    regos_df = get_regos_processed().df
    with pytest.raises(AssertionError, match="have non-unique values"):
        RegosProcessed(pd.concat([regos_df, regos_df.copy()], axis=0)).groupby_station()


def test_regos_processed_transform_to_tech_month_holder() -> None:
    filtered_regos = get_regos_processed().filter(holders=["British Gas Trading Ltd"])
    regos_by_tech_month_holder = filtered_regos.transform_to_regos_by_tech_month_holder()

    # check shape
    assert set(regos_by_tech_month_holder.df["tech"]) == set(["biomass", "wind"])
    assert len(regos_by_tech_month_holder.df) == 9

    # check values
    biomass_2022_04 = regos_by_tech_month_holder.df[
        (regos_by_tech_month_holder.df["tech"] == "biomass")
        & (regos_by_tech_month_holder.df.index == pd.Timestamp("2022-04"))
    ]["rego_mwh"].values[0]
    biomass_2023_03 = regos_by_tech_month_holder.df[
        (regos_by_tech_month_holder.df["tech"] == "biomass")
        & (regos_by_tech_month_holder.df.index == pd.Timestamp("2023-03"))
    ]["rego_mwh"].values[0]
    wind_2023_03 = regos_by_tech_month_holder.df[
        (regos_by_tech_month_holder.df["tech"] == "wind")
        & (regos_by_tech_month_holder.df.index == pd.Timestamp("2023-03"))
    ]["rego_mwh"].values[0]

    assert biomass_2022_04 == 37199.0
    assert biomass_2023_03 == 500000.0
    assert wind_2023_03 == 314075.0


def test_regos_by_tech_month_holder_expand_multi_month_certificates() -> None:
    """Test that dummy df row spanning multiple months get properly expanded and distributed."""

    test_regos_df = get_regos_processed().df.iloc[0]
    test_regos_df.loc["tech"] = "biomass"
    test_regos_df.loc["current_holder"] = "Test Company"
    test_regos_df.loc["station_name"] = "Test Station"
    test_regos_df.loc["rego_mwh"] = 3.0
    test_regos_df.loc["start_year_month"] = pd.Timestamp("2023-01-01")
    test_regos_df.loc["end_year_month"] = pd.Timestamp("2023-04-01")
    test_regos_df.loc["period_months"] = 3
    test_regos_df.loc["output_period"] = "01/01/2023 - 31/03/2023"
    test_regos = RegosProcessed(pd.DataFrame([test_regos_df]))
    regos_by_tech_month_holder = test_regos.transform_to_regos_by_tech_month_holder()

    # Check the result, including each of the three expected months
    assert len(regos_by_tech_month_holder.df) == 3
    months = [pd.Timestamp("2023-01"), pd.Timestamp("2023-02"), pd.Timestamp("2023-03")]
    for month in months:
        month_data = regos_by_tech_month_holder.df[regos_by_tech_month_holder.df.index == month]
        assert not month_data.empty, f"Missing data for {month}"
        assert month_data["tech"].iloc[0] == "biomass"
        assert month_data["current_holder"].iloc[0] == "Test Company"
        assert month_data["rego_mwh"].iloc[0] == 1
        assert month_data["station_count"].iloc[0] == 1
        assert month_data["station_count"].iloc[0] == 1


def test_parse_date_range() -> None:
    class TestCase(TypedDict):
        rego_format: str
        expected_start: str
        expected_end: str
        expected_duration_months: int

    test_cases: List[TestCase] = [
        {
            "rego_format": "May-2022",
            "expected_start": "2022-05-01",
            "expected_end": "2022-06-01",
            "expected_duration_months": 1,
        },
        {
            "rego_format": "01/05/2022 - 31/05/2022",
            "expected_start": "2022-05-01",
            "expected_end": "2022-06-01",
            "expected_duration_months": 1,
        },
        {
            "rego_format": "01/05/2022 - 30/06/2022",
            "expected_start": "2022-05-01",
            "expected_end": "2022-07-01",
            "expected_duration_months": 2,
        },
        {
            "rego_format": "2022 - 2023",
            "expected_start": "2022-04-01",
            "expected_end": "2023-04-01",
            "expected_duration_months": 12,
        },
    ]

    for test_case in test_cases:
        start, end, months_difference = RegosRaw.parse_date_range(date_str=test_case["rego_format"])
        assert start == pd.to_datetime(test_case["expected_start"])
        assert end == pd.to_datetime(test_case["expected_end"])
        assert months_difference == test_case["expected_duration_months"]


def test_parse_data_range_EXPECTED_FORMAT() -> None:
    """Expect to handle dates that are only of the format:
    * 01/04/2022 - 30/04/2022
    * 2022 - 2023
    * Apr-2022
    """
    regos = get_regos_raw()
    assert regos[~regos["output_period"].str.contains(" - |-|/", na=False)].empty


def test_parse_output_period_NON_FIRST_OR_LAST_DAY_OF_MONTH() -> None:
    regos_df = get_regos_raw().df
    # regos_df.columns = pd.Index(rego_schema_on_load.keys())
    regos_df["output_period"] = "02/04/2022 - 30/04/2022"
    with pytest.raises(ValueError):
        RegosRaw.add_output_period_columns(regos_df)

    regos_df["output_period"] = "01/04/2022 - 2/04/2022"
    with pytest.raises(ValueError):
        RegosRaw.add_output_period_columns(regos_df)

    regos_df["output_period"] = "01/04/2022 - 30/04/2022"
    regos_df_ext = RegosRaw.add_output_period_columns(regos_df)
    assert len(regos_df_ext) == len(regos_df)


def test_parse_output_period_EMPTY_DATAFRAME() -> None:
    regos = RegosRaw(get_regos_raw().df[:0])
    regos_df = RegosRaw.add_output_period_columns(regos.df[:0])
    assert set(["start_year_month", "end_year_month", "period_months"]) < set(regos_df.columns)
