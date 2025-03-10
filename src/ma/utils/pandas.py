import copy
from abc import ABC
from pathlib import Path
from typing import Callable, Dict, NotRequired, Optional, Tuple, TypedDict, Union

import pandas as pd
import pandera as pa
from pandera.engines import pandas_engine


def select_columns(df: pd.DataFrame, exclude: list) -> pd.DataFrame:
    return df[[col for col in df.columns if col not in exclude]]


def DateTimeEngine(dayfirst: bool = True) -> pandas_engine.DateTime:
    # mypy doesn't recognize to_datetime_kwargs as a valid parameter, but it is at runtime
    return pandas_engine.DateTime(to_datetime_kwargs={"dayfirst": dayfirst})  # type: ignore


class ColumnSchema(TypedDict):
    old_name: NotRequired[str]  # TODO - remove https://github.com/matched-energy/matched-analytics/issues/9
    check: NotRequired[Union[pa.Column, pa.Check, pa.Index]]  # TODO - make required, remove Check? issues/9
    keep: NotRequired[bool]


# TODO - https://github.com/matched-energy/matched-analytics/issues/9
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


class DataFrameAsset(ABC):
    schema: Dict[str, ColumnSchema]
    from_file_with_index: bool = True

    @classmethod
    def _pandera_schema(cls) -> Tuple[Dict, Dict, pa.DataFrameSchema]:
        columns: Dict = {}
        index: Dict = {}
        for col, column_schema in cls.schema.items():
            check = column_schema["check"]
            if isinstance(check, pa.Column):
                columns[col] = check
            elif isinstance(check, pa.Index):
                if len(index):
                    raise ValueError("More than one index column defined")
                index = {"check": check, "name": col}
            else:
                raise ValueError("Columns must be of type pa.Column or pa.Index")

        schema = pa.DataFrameSchema(
            columns=columns,
            index=index.get("check"),
            coerce=True,
            strict=True,
        )
        return columns, index, schema

    @classmethod
    def from_dataframe(cls, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = copy.deepcopy(dataframe)  # TODO: https://github.com/matched-energy/matched-analytics/issues/9

        columns, index, schema = cls._pandera_schema()

        # Name columns
        column_names = pd.Index(columns.keys())
        if len(columns) != len(dataframe.columns):
            raise AssertionError(
                f"Dataframe has wrong number of columns: expected {len(column_names)} got {len(dataframe.columns)}"
            )
        dataframe.columns = column_names

        # Name index
        if index_name := index.get("name"):
            dataframe.index.name = index_name

        # Apply schema
        dataframe = schema.validate(dataframe)

        # Drop columns
        dataframe = select_columns(
            dataframe, exclude=[col for col, cs in cls.schema.items() if not cs.get("keep", True)]
        )

        return dataframe

    @classmethod
    def from_file(cls, filepath: Path) -> pd.DataFrame:
        df = pd.read_csv(filepath, index_col=0 if cls.from_file_with_index else None)
        return cls.from_dataframe(df)

    @classmethod
    def write(cls, dataframe: pd.DataFrame, filepath: Path) -> None:
        _, _, schema = cls._pandera_schema()
        schema.validate(dataframe).to_csv(filepath)
