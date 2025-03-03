from typing import Dict

import pandas as pd
import pandera as pa

from ma.utils.enums import ProductionTechEnum
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# fmt: off
grid_mix_schema_on_load: Dict[str, CS] = dict(
    datetime            =CS(check=pa.Column(DTE(dayfirst=False)), keep=True),
    gas                 =CS(check=pa.Column(int), keep=True),
    coal                =CS(check=pa.Column(int), keep=True),
    nuclear             =CS(check=pa.Column(int), keep=True),
    wind                =CS(check=pa.Column(int), keep=True),
    hydro               =CS(check=pa.Column(int), keep=True),
    imports             =CS(check=pa.Column(int), keep=True),
    biomass             =CS(check=pa.Column(int), keep=True),
    other               =CS(check=pa.Column(int), keep=True),
    solar               =CS(check=pa.Column(int), keep=True),
    storage             =CS(check=pa.Column(int), keep=True),
    generation          =CS(check=pa.Column(int), keep=False),
    carbon_intensity    =CS(check=pa.Column(int), keep=False),
    low_carbon          =CS(check=pa.Column(int), keep=False),
    zero_carbon         =CS(check=pa.Column(int), keep=False),
    renewable           =CS(check=pa.Column(int), keep=False),
    fossil              =CS(check=pa.Column(int), keep=False),
    gas_perc            =CS(check=pa.Column(float), keep=False),
    coal_perc           =CS(check=pa.Column(float), keep=False),
    nuclear_perc        =CS(check=pa.Column(float), keep=False),
    wind_perc           =CS(check=pa.Column(float), keep=False),
    hydro_perc          =CS(check=pa.Column(float), keep=False),
    imports_perc        =CS(check=pa.Column(float), keep=False),
    biomass_perc        =CS(check=pa.Column(float), keep=False),
    other_perc          =CS(check=pa.Column(float), keep=False),
    solar_perc          =CS(check=pa.Column(float), keep=False),
    storage_perc        =CS(check=pa.Column(float), keep=False),
    generation_perc     =CS(check=pa.Column(float), keep=False),
    low_carbon_perc     =CS(check=pa.Column(float), keep=False),
    zero_carbon_perc    =CS(check=pa.Column(float), keep=False),
    renewable_perc      =CS(check=pa.Column(float), keep=False),
    fossil_perc         =CS(check=pa.Column(float), keep=False),
)
# fmt: on


def transform_grid_mix_schema(grid_mix: pd.DataFrame) -> pd.DataFrame:
    grid_mix = grid_mix.copy()
    tech_columns = [t.value for t in ProductionTechEnum]
    grid_mix[tech_columns] = grid_mix[tech_columns] / 2  # convert from MW to MWh
    grid_mix.columns = pd.Index([f"{col}_mwh" if col in tech_columns else col for col in grid_mix.columns])

    # Set datetime as index for easier timeseries operations
    grid_mix = grid_mix.set_index("datetime")
    return grid_mix
