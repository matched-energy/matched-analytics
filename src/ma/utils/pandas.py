import copy
from typing import Dict, NotRequired, TypedDict, Union

import pandas as pd
import pandera as pa


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


class DataFrameSchema(TypedDict):
    old_name: NotRequired[str]
    check: NotRequired[Union[pa.Column, pa.Check]]


# TODO - test
def apply_schema(df: pd.DataFrame, schema: Dict[str, DataFrameSchema]) -> pd.DataFrame:
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
