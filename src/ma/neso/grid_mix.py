from __future__ import annotations

from typing import Dict
from ma.utils.enums import SupplyTechEnum
from ma.utils.pandas import DataFrameAsset
import pandas as pd
import pandera as pa
from ma.utils.io import get_logger
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

logger = get_logger(__name__)


class GridMixRaw(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        DATETIME            =CS(check=pa.Column(DTE(dayfirst=False)), keep=True),
        GAS                 =CS(check=pa.Column(int), keep=True),
        COAL                =CS(check=pa.Column(int), keep=True),
        NUCLEAR             =CS(check=pa.Column(int), keep=True),
        WIND                =CS(check=pa.Column(int), keep=True),
        HYDRO               =CS(check=pa.Column(int), keep=True),
        IMPORTS             =CS(check=pa.Column(int), keep=True),
        BIOMASS             =CS(check=pa.Column(int), keep=True),
        OTHER               =CS(check=pa.Column(int), keep=True),
        SOLAR               =CS(check=pa.Column(int), keep=True),
        STORAGE             =CS(check=pa.Column(int), keep=True),
        GENERATION          =CS(check=pa.Column(int), keep=False),
        CARBON_INTENSITY    =CS(check=pa.Column(int), keep=False),
        LOW_CARBON          =CS(check=pa.Column(int), keep=False),
        ZERO_CARBON         =CS(check=pa.Column(int), keep=False),
        RENEWABLE           =CS(check=pa.Column(int), keep=False),
        FOSSIL              =CS(check=pa.Column(int), keep=False),
        GAS_PERC            =CS(check=pa.Column(float), keep=False),
        COAL_PERC           =CS(check=pa.Column(float), keep=False),
        NUCLEAR_PERC        =CS(check=pa.Column(float), keep=False),
        WIND_PERC           =CS(check=pa.Column(float), keep=False),
        HYDRO_PERC          =CS(check=pa.Column(float), keep=False),
        IMPORTS_PERC        =CS(check=pa.Column(float), keep=False),
        BIOMASS_PERC        =CS(check=pa.Column(float), keep=False),
        OTHER_PERC          =CS(check=pa.Column(float), keep=False),
        SOLAR_PERC          =CS(check=pa.Column(float), keep=False),
        STORAGE_PERC        =CS(check=pa.Column(float), keep=False),
        GENERATION_PERC     =CS(check=pa.Column(float), keep=False),
        LOW_CARBON_PERC     =CS(check=pa.Column(float), keep=False),
        ZERO_CARBON_PERC    =CS(check=pa.Column(float), keep=False),
        RENEWABLE_PERC      =CS(check=pa.Column(float), keep=False),
        FOSSIL_PERC         =CS(check=pa.Column(float), keep=False),
    )
    from_file_with_index = False
    from_file_skiprows = 0
    from_file_header = 0
    # fmt: on

    def transform_to_grid_mix_processed(self) -> GridMixProcessed:
        grid_mix = self.df
        grid_mix.columns = grid_mix.columns.str.lower()  # for readability

        tech_columns = [t.value for t in SupplyTechEnum]
        grid_mix[tech_columns] = grid_mix[tech_columns] / 2  # convert from MW to MWh
        grid_mix.columns = pd.Index([f"{col}_mwh" if col in tech_columns else col for col in grid_mix.columns])
        grid_mix = grid_mix.set_index("datetime")  # for timeseries operations downstream
        return GridMixProcessed(grid_mix)


class GridMixProcessed(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        datetime            =CS(check=pa.Index(DTE(dayfirst=False)), keep=True),
        gas_mwh             =CS(check=pa.Column(float), keep=True),
        coal_mwh            =CS(check=pa.Column(float), keep=True),
        nuclear_mwh         =CS(check=pa.Column(float), keep=True),
        wind_mwh            =CS(check=pa.Column(float), keep=True),
        hydro_mwh           =CS(check=pa.Column(float), keep=True),
        imports_mwh         =CS(check=pa.Column(float), keep=True),
        biomass_mwh         =CS(check=pa.Column(float), keep=True),
        other_mwh           =CS(check=pa.Column(float), keep=True),
        solar_mwh           =CS(check=pa.Column(float), keep=True),
        storage_mwh         =CS(check=pa.Column(float), keep=True),
    )
    # fmt: on

    def filter(self, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp) -> GridMixProcessed:
        """
        Filter by start and end datetime, exclusive of the end datetime.
        """
        filtered_grid_mix = self.df[(self.df.index >= start_datetime) & (self.df.index < end_datetime)]
        return GridMixProcessed(filtered_grid_mix)

    def transform_to_grid_mix_by_tech_month(self) -> GridMixByTechMonth:
        """
        Group by tech and month, and sum the values. Returns MWh per month for each tech.
        """
        grid_mix = self.df.reset_index()
        # Convert to first day of each month for consistent datetime handling
        grid_mix["month"] = pd.to_datetime(grid_mix["datetime"].dt.to_period("M").astype(str))
        grouped = grid_mix.groupby("month")[[f"{t.value}_mwh" for t in SupplyTechEnum]].sum()
        return GridMixByTechMonth(grouped)


class GridMixByTechMonth(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        month               =CS(check=pa.Index(DTE(dayfirst=False))),
        gas_mwh             =CS(check=pa.Column(float), keep=True),
        coal_mwh            =CS(check=pa.Column(float), keep=True),
        nuclear_mwh         =CS(check=pa.Column(float), keep=True),
        wind_mwh            =CS(check=pa.Column(float), keep=True),
        hydro_mwh           =CS(check=pa.Column(float), keep=True),
        imports_mwh         =CS(check=pa.Column(float), keep=True),
        biomass_mwh         =CS(check=pa.Column(float), keep=True),
        other_mwh           =CS(check=pa.Column(float), keep=True),
        solar_mwh           =CS(check=pa.Column(float), keep=True),
        storage_mwh         =CS(check=pa.Column(float), keep=True),
    )
    # fmt: on
