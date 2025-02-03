import copy
from typing import Callable, Dict, NotRequired, Optional, TypedDict, Union

import pandas as pd
import pandera as pa


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]
    check: NotRequired[Union[pa.Column, pa.Check]]
    keep: NotRequired[bool]


DateTimeEngine = pa.engines.pandas_engine.DateTime({"dayfirst": True})


def apply_schema(
    df: pd.DataFrame, schema: Dict[str, ColumnSchema], transform: Optional[Callable] = None
) -> pd.DataFrame:
    df = copy.deepcopy(df)

    # Name columns
    new_columns = pd.Index(schema.keys())
    if len(new_columns) != len(df.columns):
        raise AssertionError(
            f"Schema & DataFrame have different number of columns ({len(new_columns)} & {len(df.columns)} respectively)"
        )
    df.columns = new_columns

    # Validate schema
    pandera_schema = pa.DataFrameSchema(
        {col: cs["check"] for col, cs in schema.items() if cs.get("check")}, coerce=True
    )
    df = pandera_schema(df)

    # Transform
    if transform is not None:
        df = transform(df)

    # Drop columns
    df = select_columns(df, exclude=[col for col, cs in schema.items() if not cs.get("keep", True)])

    return df
