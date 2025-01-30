import copy
from typing import List, TypedDict

import pandas as pd
import pytest
from pytest import approx

import data.register
import ma.ofgem.regos


def test_load() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    assert len(regos) == 220
    assert set(regos["Generating Station / Agent Group"]) == set(
        ["Drax Power Station (REGO)", "Triton Knoll Offshore Windfarm", "Walney Extension"]
    )
    assert set(regos["Technology Group"]) == set(["Biomass", "Off-shore Wind"])
    assert set(regos["tech_simple"]) == set(["BIOMASS", "WIND"])
    assert regos["GWh"].sum() == approx(17114.284)


def test_load_NON_DEFAULT_FILTERING() -> None:
    assert (
        len(ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET, holders=None, statuses=None, schemees=None))
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
        start, end, months_difference = ma.ofgem.regos.parse_date_range(date_str=test_case["rego_format"])
        assert start == pd.to_datetime(test_case["expected_start"])
        assert end == pd.to_datetime(test_case["expected_end"])
        assert months_difference == test_case["expected_duration_months"]


def test_parse_data_range_EXPECTED_FORMAT() -> None:
    """Expect to handle dates that are only of the format:
    * 01/04/2022 - 30/04/2022
    * 2022 - 2023
    * Apr-2022
    """
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    assert regos[~regos["Output Period"].str.contains(" - |-|/", na=False)].empty


def test_parse_output_period() -> None:
    regos_raw = ma.ofgem.regos.read_raw(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos = ma.ofgem.regos.parse_output_period(regos_raw)
    assert len(regos)
    assert len(regos) == len(regos_raw)
    assert set(["start", "end", "months_difference"]) < set(regos.columns)


def test_parse_output_period_EMPTY_DATAFRAME() -> None:
    regos_raw = ma.ofgem.regos.read_raw(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos = ma.ofgem.regos.parse_output_period(regos_raw[:0])
    assert set(["start", "end", "months_difference"]) < set(regos.columns)


def test_groupby_station() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_grouped = ma.ofgem.regos.groupby_station(regos)
    assert len(regos_grouped) == 3
    assert regos_grouped["GWh"].sum() == approx(17114.284)
    assert set(regos_grouped["tech_simple"]) == set(["BIOMASS", "WIND"])


def test_groupby_station_NON_UNIQUE() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    regos_cp = copy.deepcopy(regos)
    regos_cp["tech_simple"] = "☀️"
    with pytest.raises(ValueError):
        ma.ofgem.regos.groupby_station(pd.concat([regos, regos_cp], axis=0))
