from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pandera as pa
from dateutil.relativedelta import relativedelta

from ma.utils.enums import SupplyTechEnum
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# fmt: off
rego_schema_on_load: Dict[str, CS] = dict( 
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
    issue_date                  =CS(check=pa.Column(DTE(dayfirst=True))),
    certificate_status          =CS(check=pa.Column(str)),
    status_date                 =CS(check=pa.Column(DTE(dayfirst=True))),
    current_holder              =CS(check=pa.Column(str)),
    company_registration_number =CS(check=pa.Column(str, nullable=True)),
)
# fmt: on


def transform_regos_schema(regos_raw: pd.DataFrame) -> pd.DataFrame:
    regos = regos_raw.copy()
    regos["rego_gwh"] = regos["mwh_per_certificate"] * regos["certificate_count"] / 1e3
    regos["tech"] = regos["technology_group"].map(rego_tech_categories)
    regos = add_output_period_columns(regos)
    return regos


rego_tech_categories = {
    "Photovoltaic": SupplyTechEnum.SOLAR,
    "Hydro": SupplyTechEnum.HYDRO,
    "Wind": SupplyTechEnum.WIND,
    "Biomass": SupplyTechEnum.BIOMASS,
    "Biogas": SupplyTechEnum.BIOMASS,
    "Landfill Gas": SupplyTechEnum.BIOMASS,
    "On-shore Wind": SupplyTechEnum.WIND,
    "Hydro 20MW DNC or less": SupplyTechEnum.HYDRO,
    "Fuelled": SupplyTechEnum.BIOMASS,
    "Off-shore Wind": SupplyTechEnum.WIND,
    "Micro Hydro": SupplyTechEnum.HYDRO,
    "Biomass 50kW DNC or less": SupplyTechEnum.BIOMASS,
}


def parse_date_range(date_str: str) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
    # e.g. 01/09/2022 - 30/09/2022
    if "/" in date_str:
        start, end = date_str.split(" - ")
        start_dt = pd.to_datetime(start, dayfirst=True)
        end_dt = pd.to_datetime(end, dayfirst=True) + +np.timedelta64(1, "D")

    # e.g. 2022 - 2023: we presume this should be taken to cover a compliance year
    elif " - " in date_str:
        year_start, year_end = date_str.split(" - ")
        start_dt = pd.to_datetime("01/04/" + year_start, dayfirst=True)
        end_dt = pd.to_datetime("31/03/" + year_end, dayfirst=True) + np.timedelta64(1, "D")

    # e.g. May-2022
    elif "-" in date_str:
        month_year = pd.to_datetime(date_str, format="%b-%Y")
        start_dt = month_year.replace(day=1)
        end_dt = month_year + pd.offsets.MonthEnd(0) + np.timedelta64(1, "D")

    else:
        raise ValueError(r"Invalid date string {}".format(date_str))

    # Check start/end dates are first/last days of month (can be in different months)
    if (
        start_dt.day != 1
        or (end_dt - pd.Timedelta(days=1)).day != ((end_dt - pd.Timedelta(days=1)) + pd.offsets.MonthEnd(0)).day
    ):
        raise ValueError(f"{date_str} days are not the first and last days of month")

    period_duration = relativedelta(end_dt, start_dt)

    # Check period_duration is less than or equal to 1 year
    if period_duration.years > 1 or period_duration.months > 12:
        raise ValueError(f"{date_str} period_duration is more than 12 months")

    months_difference = period_duration.years * 12 + period_duration.months

    return start_dt, end_dt, months_difference


def add_output_period_columns(regos: pd.DataFrame) -> pd.DataFrame:
    column_names = ["start_year_month", "end_year_month", "period_months"]
    period_columns = pd.DataFrame(columns=column_names)
    if not regos.empty:
        period_columns = regos["output_period"].apply(lambda x: pd.Series(parse_date_range(x)))
        period_columns.columns = pd.Index(column_names)
    return pd.concat([regos, period_columns], axis=1)
