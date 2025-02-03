from typing import List, TypedDict

import pandas as pd

import data.register
import ma.ofgem.regos
from ma.ofgem.schema_regos import add_output_period_columns, parse_date_range, rego_schema_on_load
from ma.utils.pandas import apply_schema


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
        start, end, months_difference = parse_date_range(date_str=test_case["rego_format"])
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
    assert regos[~regos["output_period"].str.contains(" - |-|/", na=False)].empty


def test_parse_output_period() -> None:
    regos_raw = pd.read_csv(data.register.REGOS_APR2022_MAR2023_SUBSET, skiprows=4)
    regos = apply_schema(regos_raw, rego_schema_on_load)
    regos = add_output_period_columns(regos)
    assert len(regos)
    assert len(regos) == len(regos_raw)
    assert set(["period_start", "period_end", "period_months"]) < set(regos.columns)


def test_parse_output_period_EMPTY_DATAFRAME() -> None:
    regos_raw = pd.read_csv(data.register.REGOS_APR2022_MAR2023_SUBSET, skiprows=4)
    regos = add_output_period_columns(regos_raw[:0])
    assert set(["period_start", "period_end", "period_months"]) < set(regos.columns)
