from typing import Dict

import pandera as pa
from pandera.engines import pandas_engine

from ma.utils.pandas import DataFrameSchema as DFS

timestamp_check = pa.Column(pandas_engine.DateTime({"dayfirst": True}))

REGO_SCHEMA: Dict[str, DFS] = dict(
    accreditation_number=DFS(old_name="Accreditation No.", check=pa.Column(str)),
    station_name=DFS(old_name="Generating Station / Agent Group", check=pa.Column(str)),
    station_tic=DFS(old_name="Station TIC", check=pa.Column(float)),
    scheme=DFS(old_name="Scheme", check=pa.Column(str)),
    country=DFS(old_name="Country", check=pa.Column(str)),
    technology_group=DFS(old_name="Technology Group", check=pa.Column(str)),
    generation_type=DFS(old_name="Generation Type", check=pa.Column(str, nullable=True)),
    output_period=DFS(old_name="Output Period", check=pa.Column(str)),
    certificate_count=DFS(old_name="No. Of Certificates", check=pa.Column(int)),
    certificate_start=DFS(old_name="Start Certificate No.", check=pa.Column(str)),
    certificate_end=DFS(old_name="End Certificate No.", check=pa.Column(str)),
    mwh_per_certificate=DFS(old_name="MWh Per Certificate", check=pa.Column(float)),
    issue_date=DFS(old_name="Issue Date", check=timestamp_check),
    certificate_status=DFS(old_name="Certificate Status", check=pa.Column(str)),
    status_date=DFS(old_name="Status Date", check=timestamp_check),
    current_holder=DFS(old_name="Current Holder Organisation Name", check=pa.Column(str)),
    company_registration_number=DFS(old_name="Company Registration Number", check=pa.Column(str, nullable=True)),
)
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
