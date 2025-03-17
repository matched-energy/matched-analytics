import copy
from pathlib import Path

import pandas as pd
import pandera as pa
import pytest

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset, apply_schema

DF_RAW = pd.DataFrame(dict(a=["1"] * 5, b=["foo"] * 5))  # note 'a' is of type str


def test_schema_typed() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))

    df = Asset(copy.deepcopy(DF_RAW))
    pd.testing.assert_index_equal(df.df.columns, pd.Index(["col_a", "col_b"]))
    assert pd.api.types.is_integer_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])


def test_schema_untyped() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column()))

    df = Asset(copy.deepcopy(DF_RAW))
    assert pd.api.types.is_object_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])


def test_schema_drop_columns() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column(), keep=False))

    df = Asset(copy.deepcopy(DF_RAW))
    assert pd.api.types.is_object_dtype(df["col_a"])


def test_schema_wrong_number_of_columns() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()))

    with pytest.raises(AssertionError, match="Dataframe has wrong number of columns"):
        Asset(copy.deepcopy(DF_RAW))


def test__schema_pandera_validation_fail() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column(int)))

    with pytest.raises(pa.errors.SchemaError, match="Error while coercing 'col_b' to type int64"):
        Asset(copy.deepcopy(DF_RAW))


def test_schema_index() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Index(int)), col_b=CS(check=pa.Column(str)))

    df_raw = copy.deepcopy(DF_RAW)
    df_raw.set_index("a", drop=True, inplace=True)
    df = Asset(df_raw)
    assert df.df.index.name == "col_a"
    assert df.df.columns == ["col_b"]


def test_schema_index_type_error() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Index(int)))

    df_raw = copy.deepcopy(DF_RAW)
    df_raw.set_index("a", drop=True, inplace=True)
    with pytest.raises(pa.errors.SchemaError, match="Error while coercing 'col_a' to type int64"):
        Asset(df_raw)


def test_schema_too_many_indices() -> None:
    class Asset(DataFrameAsset):
        schema = dict(
            ind_a=CS(check=pa.Index(int)),
            ind_b=CS(check=pa.Index(str)),
        )

    df_raw = copy.deepcopy(DF_RAW)
    with pytest.raises(ValueError, match="More than one index"):
        Asset(df_raw)


def test_immutable_on_init() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column()))

    df_raw = copy.deepcopy(DF_RAW)
    df = Asset(df_raw)

    df_raw["a"] *= 2
    df_raw["c"] = 100

    with pytest.raises(AssertionError):
        pd.testing.assert_series_equal(df_raw["a"], df["col_a"])

    assert "c" not in df.df.columns


def test_immutable_no_reassignment() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column()), col_b=CS(check=pa.Column()))

    df = Asset(copy.deepcopy(DF_RAW))
    with pytest.raises(AttributeError, match="Cannot reassign _df"):
        df._df_do_not_mutate = copy.deepcopy(DF_RAW)


def test_immutable_deep_copy() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))

    df = Asset(copy.deepcopy(DF_RAW))

    df_copy = df.df
    df_copy["col_b"] += " ⭐️"
    df_copy["col_c"] = df_copy["col_a"] * 2

    assert "col_c" not in df.df.columns
    assert df[~df["col_b"].str.contains("⭐")]["col_b"].any()


def test_from_file() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))
        from_file_with_index = False
        from_file_skiprows = 1

    df = Asset(Path(f"{Path(__file__).parent}/test.csv"))
    assert set(["col_a", "col_b"]) == set(df.df.columns)


def test_from_file_with_index() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Index(int)), col_b=CS(check=pa.Column(str)))
        from_file_with_index = True
        from_file_skiprows = 1

    df = Asset(Path(f"{Path(__file__).parent}/test.csv"))
    assert df.df.index.name == "col_a"
    assert ["col_b"] == df.df.columns


def test_from_file_skiprows() -> None:
    class Asset(DataFrameAsset):
        schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))
        from_file_with_index = False
        from_file_skiprows = 2

    df = Asset(Path(f"{Path(__file__).parent}/test.csv"))
    assert len(df.df) == 1


# TODO - https://github.com/matched-energy/matched-analytics/issues/9
def test_apply_schema_transform() -> None:
    schema = dict(_drop_me=CS(check=pa.Column(int), keep=False), col_b=CS(check=pa.Column(str)))

    def f_transform(raw: pd.DataFrame) -> pd.DataFrame:
        df = copy.deepcopy(raw)
        df["keep_me"] = df["_drop_me"] * 10
        return df

    df = apply_schema(copy.deepcopy(DF_RAW), schema, f_transform)
    pd.testing.assert_series_equal(df["keep_me"], pd.Series([10] * 5, name="keep_me"))
