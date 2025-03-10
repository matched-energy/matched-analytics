import copy
from abc import ABC
from pathlib import Path
from typing import Callable, Dict, NotRequired, Optional, TypedDict, Union

import pandas as pd
import pandera as pa
from pandera.engines import pandas_engine


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


def DateTimeEngine(dayfirst: bool = True) -> pandas_engine.DateTime:
    # mypy doesn't recognize to_datetime_kwargs as a valid parameter, but it is at runtime
    return pandas_engine.DateTime(to_datetime_kwargs={"dayfirst": dayfirst})  # type: ignore


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]  # TODO - remove
    check: NotRequired[Union[pa.Column, pa.Check]]
    keep: NotRequired[bool]


# TODO - remove
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
        {col: cs.get("check") for col, cs in schema.items() if cs.get("check")}, coerce=True
    )
    df = pandera_schema(df)

    # Transform
    if transform is not None:
        df = transform(df)

    # Drop columns
    df = select_columns(df, exclude=[col for col, cs in schema.items() if not cs.get("keep", True)])

    return df


# TODO - test
class DataFrameAsset(ABC):
    schema: Dict[str, ColumnSchema]

    @classmethod
    def _pandera_schema(cls) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {col: cs.get("check") for col, cs in cls.schema.items() if cs.get("check")}, coerce=True, strict=True
        )

    @classmethod
    def from_dataframe(cls, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = copy.deepcopy(dataframe)  # TODO: test

        # Name columns
        new_columns = pd.Index(cls.schema.keys())
        if len(new_columns) != len(dataframe.columns):
            raise AssertionError(
                f"Dataframe has wrong number of columns: expected ({len(new_columns)} got {len(dataframe.columns)}"
            )
        dataframe.columns = new_columns

        # Apply schema
        dataframe = cls._pandera_schema().validate(dataframe)

        # Drop columns
        dataframe = select_columns(
            dataframe, exclude=[col for col, cs in cls.schema.items() if not cs.get("keep", True)]
        )

        return dataframe

    @classmethod
    def from_file(cls, filepath: Path) -> pd.DataFrame:
        return cls.from_dataframe(pd.read_csv(filepath))

    @classmethod
    def write(cls, dataframe: pd.DataFrame, filepath: Path) -> None:
        cls._pandera_schema().validate(dataframe).to_csv(filepath)
