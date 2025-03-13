import copy

import pandas as pd
import pandera as pa
import pytest

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset, apply_schema

DF_RAW = pd.DataFrame(dict(a=["1"] * 5, b=["foo"] * 5))  # note 'a' is of type str


def test_apply_schema_TYPED() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))

    df = Asset(copy.deepcopy(DF_RAW))
    pd.testing.assert_index_equal(df.df().columns, pd.Index(["col_a", "col_b"]))
    assert pd.api.types.is_integer_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])


def test_apply_schema_UNTYPED() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column()))

    df = Asset(copy.deepcopy(DF_RAW))
    assert pd.api.types.is_object_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])


def test_apply_schema_DROP_COLUMNS() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column(), keep=False))

    df = Asset(copy.deepcopy(DF_RAW))
    assert pd.api.types.is_object_dtype(df["col_a"])


def test_apply_schema_WRONG_NUMBER_OF_COLUMNS() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()))

    with pytest.raises(AssertionError):
        Asset(copy.deepcopy(DF_RAW))


def test_apply_schema_PANDERA_VALIDATION_FAIL() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column(int)))

    with pytest.raises(pa.errors.SchemaError):
        Asset(copy.deepcopy(DF_RAW))


def test_apply_schema_INDEX() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Index(int)), col_b=CS(check=pa.Column(str)))

    df_raw = copy.deepcopy(DF_RAW)
    df_raw.set_index("a", drop=True, inplace=True)
    df = Asset(df_raw)
    assert df.df().index.name == "col_a"
    assert df.df().columns == ["col_b"]


def test_apply_schema_INDEX_TYPE_ERROR() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Index(int)))

    df_raw = copy.deepcopy(DF_RAW)
    df_raw.set_index("a", drop=True, inplace=True)
    with pytest.raises(pa.errors.SchemaError):
        Asset(df_raw)


def test_apply_schema_TOO_MANY_INDICES() -> None:
    class Asset(DataFrameAsset):
        schema = dict(
            ind_a=CS(check=pa.Index(int)),
            ind_b=CS(check=pa.Index(str)),
        )

    df_raw = copy.deepcopy(DF_RAW)
    with pytest.raises(ValueError, match="More than one index"):
        Asset(df_raw)


# TODO - https://github.com/matched-energy/matched-analytics/issues/9
def test_apply_schema_TRANSFORM() -> None:
    schema = dict(_drop_me=CS(check=pa.Column(int), keep=False), col_b=CS(check=pa.Column(str)))

    def f_transform(raw: pd.DataFrame) -> pd.DataFrame:
        df = copy.deepcopy(raw)
        df["keep_me"] = df["_drop_me"] * 10
        return df

    df = apply_schema(copy.deepcopy(DF_RAW), schema, f_transform)
    pd.testing.assert_series_equal(df["keep_me"], pd.Series([10] * 5, name="keep_me"))
