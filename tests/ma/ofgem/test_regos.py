import copy
import pandas as pd
import pytest
from pytest import approx


import data.register
from ma.ofgem.enums import RegoCompliancePeriod as CP, RegoStatus as Status
import ma.ofgem.regos


def test_load() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos) == 220
    assert set(regos["station_name"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["technology_group"]) == set(["Biomass", "Off-shore Wind"])
    assert set(regos["tech"]) == set(["biomass", "wind"])
    assert regos["rego_gwh"].sum() == approx(17114.284)


def test_load_NON_DEFAULT_FILTERING() -> None:
    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=None, schemes=None))
        == 327
    )

    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=[Status.REDEEMED]))
        == 220
    )

    assert (
        len(
            ma.ofgem.regos.load(
                data.register.REGOS_APR2022_MAR2023_SUBSET,
                holders=None,
                statuses=[Status.ISSUED, Status.RETIRED, Status.EXPIRED],
            )
        )
        == 107
    )


def test_filter_reporting_period() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_filtered = ma.ofgem.regos.filter(regos, reporting_period=CP.CP20)
    assert len(regos_filtered) == 0  # no regos with start_year_month in Apr 2021 in the subset

    # Create new df and insert a new row for CP20 (Apr 2021-Mar 2022)
    new_row = regos.iloc[0].copy()
    new_row["start_year_month"], new_row["end_year_month"] = pd.Timestamp("2021-04-01"), pd.Timestamp("2022-03-31")
    regos_filtered = ma.ofgem.regos.filter(pd.DataFrame([new_row]), reporting_period=CP.CP20)
    assert len(regos_filtered) == 1


def test_groupby_station() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_grouped = ma.ofgem.regos.groupby_station(regos)
    assert len(regos_grouped) == 3
    assert regos_grouped["rego_gwh"].sum() == approx(17114.284)
    assert set(regos_grouped["tech"]) == set(["biomass", "wind"])


def test_groupby_station_NON_UNIQUE() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_cp = copy.deepcopy(regos)
    regos_cp["tech"] = "☀️"
    with pytest.raises(AssertionError):
        ma.ofgem.regos.groupby_station(pd.concat([regos, regos_cp], axis=0))


def test_groupby_tech_month_holder() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    filtered_regos = ma.ofgem.regos.filter(regos, holders=["British Gas Trading Ltd"])
    result = ma.ofgem.regos.groupby_tech_month_holder(filtered_regos)

    # check shape
    assert set(result["tech"]) == set(["biomass", "wind"])
    assert len(result) == 9

    # check values
    biomass_2022_04 = result[(result["tech"] == "biomass") & (result.index == pd.Period("2022-04"))]["rego_gwh"].values[
        0
    ]
    biomass_2023_03 = result[(result["tech"] == "biomass") & (result.index == pd.Period("2023-03"))]["rego_gwh"].values[
        0
    ]
    wind_2023_03 = result[(result["tech"] == "wind") & (result.index == pd.Period("2023-03"))]["rego_gwh"].values[0]

    assert biomass_2022_04 == 37.199
    assert biomass_2023_03 == 500.000
    assert wind_2023_03 == 314.075


def test_expand_multi_month_certificates() -> None:
    """Test that dummy df row spanning multiple months get properly expanded and distributed."""

    test_row = {
        "tech": "biomass",
        "current_holder": "Test Company",
        "station_name": "Test Station",
        "rego_gwh": 3.0,  # 3 GWh total across 3 months = 1 GWh per month
        "start_year_month": pd.Timestamp("2023-01-01"),
        "end_year_month": pd.Timestamp("2023-04-01"),  # Exclusive end date
        "period_months": 3,
        "output_period": "01/01/2023 - 31/03/2023",
    }
    test_df = pd.DataFrame([test_row])
    result = ma.ofgem.regos.groupby_tech_month_holder(test_df)

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
