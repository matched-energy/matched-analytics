import copy
from typing import Dict, NotRequired, TypedDict, Union

import pandas as pd
import pandera as pa


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]
    check: NotRequired[Union[pa.Column, pa.Check]]
    keep: NotRequired[bool]


def apply_schema(df: pd.DataFrame, schema: Dict[str, ColumnSchema]) -> pd.DataFrame:
    df = copy.deepcopy(df)

    # rename columns
    new_columns = pd.Index(schema.keys())
    if len(new_columns) != len(df.columns):
        raise AssertionError(
            f"Schema & DataFrame have different number of columns ({len(new_columns)} & {len(df.columns)} respectively)"
        )
    df.columns = pd.Index(schema.keys())

    # drop columns
    df = df[[col for col in schema.keys() if schema.get("keep", True)]]

    # validate schema
    pandera_schema = pa.DataFrameSchema(
        {col: cs["check"] for col, cs in schema.items() if cs.get("check")}, coerce=True
    )
    df = pandera_schema(df)

    return df
