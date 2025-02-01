from typing import Dict

import pandera as pa
from pandera.engines import pandas_engine

from ma.utils.pandas import ColumnSchema as CS

timestamp_check = pa.Column(pandas_engine.DateTime({"dayfirst": True}))

# fmt: off
REGO_SCHEMA: Dict[str, CS] = dict( 
    accreditation_number        =CS(old_name="Accreditation No.",                   check=pa.Column(str)),
    station_name                =CS(old_name="Generating Station / Agent Group",    check=pa.Column(str)),
    station_tic                 =CS(old_name="Station TIC",                         check=pa.Column(float)),
    scheme                      =CS(old_name="Scheme",                              check=pa.Column(str)),
    country                     =CS(old_name="Country",                             check=pa.Column(str)),
    technology_group            =CS(old_name="Technology Group",                    check=pa.Column(str)),
    generation_type             =CS(old_name="Generation Type",                     check=pa.Column(str, nullable=True)),
    output_period               =CS(old_name="Output Period",                       check=pa.Column(str)),
    certificate_count           =CS(old_name="No. Of Certificates",                 check=pa.Column(int)),
    certificate_start           =CS(old_name="Start Certificate No.",               check=pa.Column(str)),
    certificate_end             =CS(old_name="End Certificate No.",                 check=pa.Column(str)),
    mwh_per_certificate         =CS(old_name="MWh Per Certificate",                 check=pa.Column(float)),
    issue_date                  =CS(old_name="Issue Date",                          check=timestamp_check),
    certificate_status          =CS(old_name="Certificate Status",                  check=pa.Column(str)),
    status_date                 =CS(old_name="Status Date",                         check=timestamp_check),
    current_holder              =CS(old_name="Current Holder Organisation Name",    check=pa.Column(str)),
    company_registration_number =CS(old_name="Company Registration Number",         check=pa.Column(str, nullable=True)),
)
# fmt: on

SIMPLIFIED_TECH_CATEGORIES = {
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
