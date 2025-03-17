import copy
from abc import ABC
from pathlib import Path
from typing import Any, Callable, Dict, NotRequired, Optional, TypedDict, Union

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
    from_file_skiprows: int = 0

    def __init__(self, input: Union[pd.DataFrame, Path]):
        self._set_schema()
        if isinstance(input, pd.DataFrame):
            df = input
        elif isinstance(input, Path):
            df = self._read_from_file(input)
        else:
            raise TypeError("Expected Pandas dataframe or pathlib.Path")
        object.__setattr__(self, "_df_do_not_mutate", self._init_from_dataframe(df))

    def _set_schema(self) -> None:
        self._columns: Dict = {}
        self._index: Dict = {}
        for col, column_schema in self.schema.items():
            check = column_schema["check"]
            if isinstance(check, pa.Column):
                self._columns[col] = check
            elif isinstance(check, pa.Index):
                if len(self._index):
                    raise ValueError("More than one index column defined")
                self._index = {"check": check, "name": col}
            else:
                raise ValueError("Columns must be of type pa.Column or pa.Index")

        self.pandera_schema = pa.DataFrameSchema(
            columns=self._columns,
            index=self._index.get("check"),
            coerce=True,
            strict=True,
        )

    def _init_from_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = copy.deepcopy(dataframe)

        # Name columns
        column_names = pd.Index(self._columns.keys())
        if len(self._columns) != len(dataframe.columns):
            raise AssertionError(
                f"Dataframe has wrong number of columns: expected {len(column_names)} got {len(dataframe.columns)}"
            )
        dataframe.columns = column_names

        # Name index
        if index_name := self._index.get("name"):
            dataframe.index.name = index_name

        # Apply schema
        dataframe = self.pandera_schema.validate(dataframe)

        # Drop columns
        dataframe = select_columns(
            dataframe, exclude=[col for col, cs in self.schema.items() if not cs.get("keep", True)]
        )

        return dataframe

    def _read_from_file(self, filepath: Path) -> pd.DataFrame:
        return pd.read_csv(
            filepath,
            index_col=0 if self.from_file_with_index else None,
            skiprows=self.from_file_skiprows,
            header=None,
        )

    def __getattr__(self, name: str) -> Any:
        return self._df_do_not_mutate[name]

    def __getitem__(self, key: str) -> Any:
        return self._df_do_not_mutate[key]

    def __setattr__(self, name: str, value: Any) -> Any:
        if name == "_df_do_not_mutate":
            raise AttributeError("Cannot reassign _df_do_not_mutate!")
        super().__setattr__(name, value)

    @property
    def df(self) -> pd.DataFrame:
        return self._df_do_not_mutate.copy(deep=True)

    def write(self, filepath: Path) -> None:
        self.pandera_schema.validate(self._df_do_not_mutate).to_csv(filepath)
