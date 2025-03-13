from typing import List, TypedDict

import pandas as pd
import pytest

import data.register
from ma.ofgem.regos import RegosRaw
from ma.ofgem.schema_regos import add_output_period_columns, rego_schema_on_load


def get_regos() -> RegosRaw:
    return RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET)


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

    regos = get_regos()
    for test_case in test_cases:
        start, end, months_difference = regos.parse_date_range(date_str=test_case["rego_format"])
        assert start == pd.to_datetime(test_case["expected_start"])
        assert end == pd.to_datetime(test_case["expected_end"])
        assert months_difference == test_case["expected_duration_months"]


def test_parse_data_range_EXPECTED_FORMAT() -> None:
    """Expect to handle dates that are only of the format:
    * 01/04/2022 - 30/04/2022
    * 2022 - 2023
    * Apr-2022
    """
    regos = get_regos()
    assert regos[~regos["output_period"].str.contains(" - |-|/", na=False)].empty


def test_parse_output_period_NON_FIRST_OR_LAST_DAY_OF_MONTH() -> None:
    regos_df = get_regos().df
    regos_df.columns = pd.Index(rego_schema_on_load.keys())
    regos_df["output_period"] = "02/04/2022 - 30/04/2022"
    with pytest.raises(ValueError):
        add_output_period_columns(regos_df)

    regos_df["output_period"] = "01/04/2022 - 2/04/2022"
    with pytest.raises(ValueError):
        add_output_period_columns(regos_df)

    regos_df["output_period"] = "01/04/2022 - 30/04/2022"
    regos = add_output_period_columns(regos_df)
    assert len(regos) == len(regos_df)


def test_parse_output_period_EMPTY_DATAFRAME() -> None:
    regos = RegosRaw(get_regos().df[:0])
    regos_df = regos.add_output_period_columns(regos.df[:0])
    assert set(["start_year_month", "end_year_month", "period_months"]) < set(regos_df.columns)
