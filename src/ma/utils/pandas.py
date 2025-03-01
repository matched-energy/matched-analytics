import copy
from typing import Callable, Dict, NotRequired, Optional, TypedDict, Union

import pandas as pd
import pandera as pa
from pandera.engines import pandas_engine


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


def DateTimeEngine(dayfirst: bool = True) -> pandas_engine.DateTime:
    # Create a DateTime object with to_datetime_kwargs directly
    # This is the correct way to initialize the DateTime class
    # mypy doesn't recognize to_datetime_kwargs as a valid parameter, but it is at runtime
    return pandas_engine.DateTime(to_datetime_kwargs={"dayfirst": dayfirst})  # type: ignore


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]
    check: NotRequired[Union[pa.Column, pa.Check]]
    keep: NotRequired[bool]


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
    # TODO: Add explanatory note of change to avoid Pylance warning
    pandera_schema = pa.DataFrameSchema(
        {col: cs.get("check") for col, cs in schema.items() if cs.get("check")}, coerce=True
    )
    df = pandera_schema(df)

    # Transform
    if transform is not None:
        df = transform(df)

    # Drop columns
    df = select_columns(df, exclude=[col for col, cs in schema.items() if not cs.get("keep", True)])

    return df
