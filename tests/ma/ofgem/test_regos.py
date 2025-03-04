import copy

import pandas as pd
import pytest
from pytest import approx

import data.register
from ma.ofgem.enums import RegoCompliancePeriod, RegoStatus
import ma.ofgem.regos


def test_load() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos) == 220
    assert set(regos["station_name"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["technology_group"]) == set(["Biomass", "Off-shore Wind"])
    assert set(regos["tech_simple"]) == set(["biomass", "wind"])
    assert regos["rego_gwh"].sum() == approx(17114.284)


def test_load_NON_DEFAULT_FILTERING() -> None:
    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=None, schemes=None))
        == 327
    )

    assert (
        len(
            ma.ofgem.regos.load(
                data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=[RegoStatus.REDEEMED]
            )
        )
        == 220
    )

    assert (
        len(
            ma.ofgem.regos.load(
                data.register.REGOS_APR2022_MAR2023_SUBSET,
                holders=None,
                statuses=[RegoStatus.ISSUED, RegoStatus.RETIRED, RegoStatus.EXPIRED],
            )
        )
        == 107
    )


def test_filter_reporting_period() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_filtered = ma.ofgem.regos.filter(regos, reporting_period=RegoCompliancePeriod.CP20)
    assert len(regos_filtered) == 0  # no regos with period_start in Apr 2021 in the subset

    # Create new df and insert a new row for CP20 (Apr 2021-Mar 2022)
    new_row = regos.iloc[0].copy()
    new_row["period_start"], new_row["period_end"] = pd.Timestamp("2021-04-01"), pd.Timestamp("2022-03-31")
    regos_filtered = ma.ofgem.regos.filter(pd.DataFrame([new_row]), reporting_period=RegoCompliancePeriod.CP20)
    assert len(regos_filtered) == 1


def test_groupby_station() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_grouped = ma.ofgem.regos.groupby_station(regos)
    assert len(regos_grouped) == 3
    assert regos_grouped["rego_gwh"].sum() == approx(17114.284)
    assert set(regos_grouped["tech_simple"]) == set(["biomass", "wind"])


def test_groupby_station_NON_UNIQUE() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_cp = copy.deepcopy(regos)
    regos_cp["tech_simple"] = "☀️"
    with pytest.raises(AssertionError):
        ma.ofgem.regos.groupby_station(pd.concat([regos, regos_cp], axis=0))


def test_groupby_tech_month_holder() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    filtered_regos = ma.ofgem.regos.filter(regos, holders=["British Gas Trading Ltd"])
    result = ma.ofgem.regos.groupby_tech_month_holder(filtered_regos)

    # check shape
    assert set(result["tech_simple"]) == set(["biomass", "wind"])
    assert len(result) == 9

    # check values
    biomass_2022_04 = result[(result["tech_simple"] == "biomass") & (result.index == pd.Period("2022-04"))][
        "rego_gwh"
    ].values[0]
    biomass_2023_03 = result[(result["tech_simple"] == "biomass") & (result.index == pd.Period("2023-03"))][
        "rego_gwh"
    ].values[0]
    wind_2023_03 = result[(result["tech_simple"] == "wind") & (result.index == pd.Period("2023-03"))][
        "rego_gwh"
    ].values[0]

    assert biomass_2022_04 == 37.199
    assert biomass_2023_03 == 500.000
    assert wind_2023_03 == 314.075
