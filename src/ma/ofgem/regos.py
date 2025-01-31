from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandera as pa
from dateutil.relativedelta import relativedelta
from pandera.engines import pandas_engine

from ma.utils.pandas import DataFrameSchema as DFS
from ma.utils.pandas import apply_schema

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
REGO_COLUMNS: list[str] = [
    "Accreditation No.",
    "Generating Station / Agent Group",
    "Station TIC",
    "Scheme",
    "Country",
    "Technology Group",
    "Generation Type",
    "Output Period",
    "No. Of Certificates",
    "Start Certificate No.",
    "End Certificate No.",
    "MWh Per Certificate",
    "Issue Date",
    "Certificate Status",
    "Status Date",
    "Current Holder Organisation Name",
    "Company Registration Number",
]

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


def read_raw(rego_file_path: Path) -> pd.DataFrame:
    return pd.read_csv(rego_file_path, names=REGO_COLUMNS, skiprows=4)


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

    period_duration = relativedelta(end_dt, start_dt)
    months_difference = period_duration.years * 12 + period_duration.months

    return start_dt, end_dt, months_difference


def parse_output_period(regos: pd.DataFrame) -> pd.DataFrame:
    # TODO start -> period_start; end -> period_end; months_difference -> period_months;
    # TODO use typed datastructure
    column_names = ["start", "end", "months_difference"]
    period_columns = pd.DataFrame(columns=column_names)
    if not regos.empty:
        period_columns = regos["output_period"].apply(lambda x: pd.Series(parse_date_range(x)))
        period_columns.columns = pd.Index(column_names)
    return pd.concat([regos, period_columns], axis=1)


def add_columns(regos: pd.DataFrame) -> pd.DataFrame:
    regos = regos.copy()
    regos["GWh"] = regos["mwh_per_certificate"] * regos["certificate_count"] / 1e3
    regos["tech_simple"] = regos["technology_group"].map(SIMPLIFIED_TECH_CATEGORIES)
    return regos


def filter(
    regos: pd.DataFrame,
    holders: Optional[List[str]] = None,
    statuses: Optional[List[str]] = None,  # "Redeemed",
    schemes: Optional[List[str]] = None,  # "REGO",
) -> pd.DataFrame:
    filters = []
    if holders:
        filters.append((regos["current_holder"].isin(holders)))
    if statuses:
        filters.append(regos["certificate_status"].isin(statuses))
    if schemes:
        filters.append(regos["scheme"].isin(schemes))

    if not filters:
        return regos
    else:
        return regos.loc[np.logical_and.reduce(filters)]


def load(
    regos_path: Path,
    holders: Optional[List[str]] = None,
    statuses: Optional[List[str]] = ["Redeemed"],
    schemees: Optional[List[str]] = ["REGO"],
) -> pd.DataFrame:
    regos = read_raw(regos_path)
    regos = apply_schema(regos, REGO_SCHEMA)
    regos = parse_output_period(regos)
    regos = add_columns(regos)
    regos = filter(regos, holders=holders, statuses=statuses, schemes=schemees)
    return regos


def groupby_station(regos: pd.DataFrame) -> pd.DataFrame:
    # Check columns that are expected to be unique
    unique_count_by_station = regos.groupby("station_name").agg(
        accredition_number_unique=("accreditation_number", "nunique"),
        company_registration_number_unique=("company_registration_number", "nunique"),
        technology_group_unique=("technology_group", "nunique"),
        generation_type_unique=("generation_type", "nunique"),
        tech_simple_unique=("tech_simple", "nunique"),
    )
    non_unique_by_station = unique_count_by_station[(unique_count_by_station > 1).any(axis=1)]
    if not non_unique_by_station.empty:
        raise AssertionError(f"Stations {list(non_unique_by_station.index)} have non-unique values")

    # Groupby
    regos_by_station = (
        regos.groupby("station_name")
        .agg(
            accredition_number=("accreditation_number", "first"),
            company_registration_number=("company_registration_number", "first"),
            GWh=("GWh", "sum"),
            technology_group=("technology_group", "first"),
            generation_type=("generation_type", "first"),
            tech_simple=("tech_simple", "first"),
        )
        .sort_values(by="GWh", ascending=False)
    )

    # Station output as a fraction of a whole
    regos_by_station["percentage_of_whole"] = regos_by_station["GWh"] / regos_by_station["GWh"].sum() * 100

    return regos_by_station.reset_index()


def get_rego_station_volume_by_month(
    regos: pd.DataFrame,
    rego_station_name: str,
) -> pd.DataFrame:
    rego_station_volumes_by_month = (
        regos[(regos["station_name"] == rego_station_name) & (regos["months_difference"] == 1)]
        .groupby(["start", "end", "months_difference"])
        .agg(dict(GWh="sum"))
    )

    months_count = len(rego_station_volumes_by_month)
    if months_count > 12:
        raise AssertionError(
            f"Don't expect reporting to be more granuular than monthly: {rego_station_name} has {months_count} periods in the year"
        )

    return rego_station_volumes_by_month.reset_index().set_index("start").sort_index()
