import copy
from pathlib import Path
from typing import Dict, NotRequired, TypedDict, Union

import pandas as pd
import pandera as pa


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]
    check: NotRequired[Union[pa.Column, pa.Check]]


def read_raw_with_schema(file_path: Path, schema: Dict[str, ColumnSchema], skip_rows: int = 0) -> pd.DataFrame:
    raw_column_names = [dfs["old_name"] for dfs in schema.values()]
    return pd.read_csv(file_path, names=raw_column_names, skiprows=skip_rows)


# TODO - test
def apply_schema(df: pd.DataFrame, schema: Dict[str, ColumnSchema]) -> pd.DataFrame:
    df = copy.deepcopy(df)

    # rename columns
    old_to_new = {dfs["old_name"]: name for name, dfs in schema.items() if dfs.get("old_name")}
    df = df.rename(columns=old_to_new)

    # only keep columns explicitly defined
    df = df[schema.keys()]

    # validate schema
    pandera_schema = pa.DataFrameSchema(
        {name: dfs["check"] for name, dfs in schema.items() if dfs.get("check")}, coerce=True
    )
    df = pandera_schema(df)

    return df
