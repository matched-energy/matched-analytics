import copy

import pandas as pd
import pandera as pa
import pytest

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import apply_schema

DF_RAW = pd.DataFrame(dict(a=["1"] * 5, b=["foo"] * 5))  # note 'a' is of type str


def test_apply_schema_BOTH_COLUMNS() -> None:
    schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))
    df = apply_schema(copy.deepcopy(DF_RAW), schema)
    pd.testing.assert_index_equal(df.columns, pd.Index(["col_a", "col_b"]))
    assert pd.api.types.is_integer_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])


def test_apply_schema_WITHOUT_CHECKS() -> None:
    schema = dict(col_a=CS(), col_b=CS())
    df = apply_schema(copy.deepcopy(DF_RAW), schema)
    assert pd.api.types.is_object_dtype(df["col_a"])


def test_apply_schema_DROP_COLUMNS() -> None:
    schema = dict(col_a=CS(), col_b=CS(keep=False))
    df = apply_schema(copy.deepcopy(DF_RAW), schema)
    assert pd.api.types.is_object_dtype(df["col_a"])


def test_apply_schema_NOT_ENOUGH_COLS_IN_SCHEMA() -> None:
    schema = dict(col_a=CS())
    with pytest.raises(AssertionError):
        apply_schema(copy.deepcopy(DF_RAW), schema)


def test_apply_schema_PANDERA_VALIDATION_FAIL() -> None:
    schema = dict(col_a=CS(), col_b=CS(check=pa.Column(int)))
    with pytest.raises(pa.errors.SchemaError):
        apply_schema(copy.deepcopy(DF_RAW), schema)


def test_apply_schema_TRANSFORM() -> None:
    schema = dict(_drop_me=CS(check=pa.Column(int), keep=False), col_b=CS(check=pa.Column(str)))

    def f_transform(raw: pd.DataFrame) -> pd.DataFrame:
        df = copy.deepcopy(raw)
        df["keep_me"] = df["_drop_me"] * 10
        return df

    df = apply_schema(copy.deepcopy(DF_RAW), schema, f_transform)
    pd.testing.assert_series_equal(df["keep_me"], pd.Series([10] * 5, name="keep_me"))
