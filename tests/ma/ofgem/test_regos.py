import copy

import pandas as pd
import pytest
from pytest import approx

import data.register
import ma.ofgem.regos


def test_load() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos) == 220
    assert set(regos["station_name"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["technology_group"]) == set(["Biomass", "Off-shore Wind"])
    assert set(regos["tech_simple"]) == set(["BIOMASS", "WIND"])
    assert regos["rego_gwh"].sum() == approx(17114.284)


def test_load_NON_DEFAULT_FILTERING() -> None:
    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=None, schemes=None))
        == 327
    )

    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=["Redeemed"])) == 220
    )

    assert (
        len(
            ma.ofgem.regos.load(
                data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=["Issued", "Retired", "Expired"]
            )
        )
        == 107
    )


def test_groupby_station() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_grouped = ma.ofgem.regos.groupby_station(regos)
    assert len(regos_grouped) == 3
    assert regos_grouped["rego_gwh"].sum() == approx(17114.284)
    assert set(regos_grouped["tech_simple"]) == set(["BIOMASS", "WIND"])


def test_groupby_station_NON_UNIQUE() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_cp = copy.deepcopy(regos)
    regos_cp["tech_simple"] = "☀️"
    with pytest.raises(AssertionError):
        ma.ofgem.regos.groupby_station(pd.concat([regos, regos_cp], axis=0))


def test_get_rego_station_volume_by_month() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    # 12 months
    volumes_by_month = ma.ofgem.regos.get_rego_station_volume_by_month(regos, "Drax Power Station (REGO)")
    assert len(volumes_by_month) == 12

    # 6 months
    regos_half_year = regos[regos["start"].dt.month <= 6]
    volumes_by_month = ma.ofgem.regos.get_rego_station_volume_by_month(regos_half_year, "Drax Power Station (REGO)")
    assert len(volumes_by_month) == 6
