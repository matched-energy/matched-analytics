from typing import Dict

import pandas as pd
import pandera as pa

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# fmt: off
historic_gen_schema_on_load: Dict[str, CS] = dict(
    datetime            =CS(check=pa.Column(str)),
    gas                 =CS(check=pa.Column(int)),
    coal                =CS(check=pa.Column(int)),
    nuclear             =CS(check=pa.Column(int)),
    wind                =CS(check=pa.Column(int)),
    hydro               =CS(check=pa.Column(int)),
    imports             =CS(check=pa.Column(int)),
    biomass             =CS(check=pa.Column(int)),
    other               =CS(check=pa.Column(int)),
    solar               =CS(check=pa.Column(int)),
    storage             =CS(check=pa.Column(int)),
    generation          =CS(check=pa.Column(int)),
    carbon_intensity    =CS(check=pa.Column(int)),
    low_carbon          =CS(check=pa.Column(int)),
    zero_carbon         =CS(check=pa.Column(int)),
    renewable           =CS(check=pa.Column(int)),
    fossil              =CS(check=pa.Column(int)),
    gas_perc            =CS(check=pa.Column(float)),
    coal_perc           =CS(check=pa.Column(float)),  
    nuclear_perc        =CS(check=pa.Column(float)),
    wind_perc           =CS(check=pa.Column(float)),
    hydro_perc          =CS(check=pa.Column(float)),
    imports_perc        =CS(check=pa.Column(float)),
    biomass_perc        =CS(check=pa.Column(float)),
    other_perc          =CS(check=pa.Column(float)),
    solar_perc          =CS(check=pa.Column(float)),
    storage_perc        =CS(check=pa.Column(float)),
    generation_perc     =CS(check=pa.Column(float)),
    low_carbon_perc     =CS(check=pa.Column(float)),
    zero_carbon_perc    =CS(check=pa.Column(float)),
    renewable_perc      =CS(check=pa.Column(float)),
    fossil_perc         =CS(check=pa.Column(float)),
)
# fmt: on
