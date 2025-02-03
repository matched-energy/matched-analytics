import copy
from typing import Dict

import pandas as pd
import pandera as pa

from ma.utils.pandas import ColumnSchema as CS

# fmt: off
stations_schema_on_load: Dict[str, CS] = dict(
    accreditation_number         = CS(check=pa.Column(str)), 
    status                       = CS(check=pa.Column(str)), 
    generating_station           = CS(check=pa.Column(str)), 
    scheme                       = CS(check=pa.Column(str)), 
    _station_dnc                 = CS(check=pa.Column(float), keep=False), 
    country                      = CS(check=pa.Column(str)), 
    technology                   = CS(check=pa.Column(str)), 
    contract_type                = CS(check=pa.Column(str)), 
    accreditation_date           = CS(check=pa.Column(str)), 
    commission_date              = CS(check=pa.Column(str)), 
    organisation                 = CS(check=pa.Column(str, nullable=True)), 
    organisation_contact_address = CS(check=pa.Column(str, nullable=True)), 
    organisation_contact_fax     = CS(keep=False), 
    generating_station_address    = CS(check=pa.Column(str, nullable=True)), 
)
# fmt: on


def transform_stations_schema(stations_raw: pd.DataFrame) -> pd.DataFrame:
    stations = copy.deepcopy(stations_raw)
    stations["station_dnc_mw"] = stations["_station_dnc"] / 1e3
    return stations
