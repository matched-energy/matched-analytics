import pandas as pd
import pandera as pa
import pytest

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import apply_schema


def test_apply_schema() -> None:
    df_raw = pd.DataFrame(dict(a=["1"] * 5, b=["foo"] * 5))  # note 'a' is of type str

    # Both columns
    schema = dict(col_a=CS(check=pa.Column(int)), col_b=CS(check=pa.Column(str)))
    df = apply_schema(df_raw, schema)
    pd.testing.assert_index_equal(df.columns, pd.Index(["col_a", "col_b"]))
    assert pd.api.types.is_integer_dtype(df["col_a"])
    assert pd.api.types.is_object_dtype(df["col_b"])

    # Without checks
    schema = dict(col_a=CS(), col_b=CS())
    df = apply_schema(df_raw, schema)
    assert pd.api.types.is_object_dtype(df["col_a"])

    # Drop columns
    schema = dict(col_a=CS(), col_b=CS(keep=False))
    df = apply_schema(df_raw, schema)
    assert pd.api.types.is_object_dtype(df["col_a"])

    # Malformed schema
    schema = dict(col_a=CS())
    with pytest.raises(AssertionError):
        df = apply_schema(df_raw, schema)

    # Pandera fail
    schema = dict(col_a=CS(), col_b=CS(check=pa.Column(int)))
    with pytest.raises(pa.errors.SchemaError):
        df = apply_schema(df_raw, schema)
