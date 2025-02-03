from typing import Dict

import pandas as pd
import pandera as pa

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# fmt: off
rego_schema_on_load: Dict[str, CS] = dict( 
    index                       =CS(check=pa.Column(int)),
    accreditation_number        =CS(check=pa.Column(str)),
    station_name                =CS(check=pa.Column(str)),
    station_tic                 =CS(check=pa.Column(float)),
    scheme                      =CS(check=pa.Column(str)),
    country                     =CS(check=pa.Column(str)),
    technology_group            =CS(check=pa.Column(str)),
    generation_type             =CS(check=pa.Column(str, nullable=True)),
    output_period               =CS(check=pa.Column(str)),
    certificate_count           =CS(check=pa.Column(int)),
    certificate_start           =CS(check=pa.Column(str)),
    certificate_end             =CS(check=pa.Column(str)),
    mwh_per_certificate         =CS(check=pa.Column(float)),
    issue_date                  =CS(check=pa.Column(DTE)),
    certificate_status          =CS(check=pa.Column(str)),
    status_date                 =CS(check=pa.Column(DTE)),
    current_holder              =CS(check=pa.Column(str)),
    company_registration_number =CS(check=pa.Column(str, nullable=True)),
)
# fmt: on


def transform_regos_schema(regos_raw: pd.DataFrame) -> pd.DataFrame:
    regos = regos_raw.copy()
    regos["rego_gwh"] = regos["mwh_per_certificate"] * regos["certificate_count"] / 1e3
    regos["tech_simple"] = regos["technology_group"].map(rego_simplified_tech_categories)
    return regos


rego_simplified_tech_categories = {
    "Photovoltaic": "SOLAR",
    "Hydro": "HYDRO",
    "Wind": "WIND",
    "Biomass": "BIOMASS",
    "Biogas": "BIOMASS",
    "Landfill Gas": "BIOMASS",
    "On-shore Wind": "WIND",
    "Hydro 20MW DNC or less": "HYDRO",
    "Fuelled": "BIOMASS",
    "Off-shore Wind": "WIND",
    "Micro Hydro": "HYDRO",
    "Biomass 50kW DNC or less": "BIOMASS",
}
