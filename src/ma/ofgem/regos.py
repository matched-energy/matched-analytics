from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

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
        period_columns = regos["Output Period"].apply(lambda x: pd.Series(parse_date_range(x)))
        period_columns.columns = pd.Index(column_names)
    return pd.concat([regos, period_columns], axis=1)


def add_columns(regos: pd.DataFrame) -> pd.DataFrame:
    regos = regos.copy()
    regos["GWh"] = regos["MWh Per Certificate"] * regos["No. Of Certificates"] / 1e3
    regos["tech_simple"] = regos["Technology Group"].map(SIMPLIFIED_TECH_CATEGORIES)
    return regos


def filter(
    regos: pd.DataFrame,
    holders: Optional[List[str]] = None,
    statuses: Optional[List[str]] = None,  # "Redeemed",
    schemes: Optional[List[str]] = None,  # "REGO",
) -> pd.DataFrame:
    filters = []
    if holders:
        filters.append((regos["Current Holder Organisation Name"].isin(holders)))
    if statuses:
        filters.append(regos["Certificate Status"].isin(statuses))
    if schemes:
        filters.append(regos["Scheme"].isin(schemes))

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
    regos = parse_output_period(regos)
    regos = add_columns(regos)
    regos = filter(regos, holders=holders, statuses=statuses, schemes=schemees)
    return regos


def groupby_station(regos: pd.DataFrame) -> pd.DataFrame:
    # Check columns that are expected to be unique
    unique_count_by_station = regos.groupby("Generating Station / Agent Group").agg(
        accredition_number_unique=("Accreditation No.", "nunique"),
        company_registration_number_unique=("Company Registration Number", "nunique"),
        technology_group_unique=("Technology Group", "nunique"),
        generation_type_unique=("Generation Type", "nunique"),
        tech_simple_unique=("tech_simple", "nunique"),
    )
    non_unique_by_station = unique_count_by_station[(unique_count_by_station > 1).any(axis=1)]
    if not non_unique_by_station.empty:
        raise AssertionError(f"Generating stations {list(non_unique_by_station.index)} have non-unique values")

    # Groupby
    regos_by_station = (
        regos.groupby("Generating Station / Agent Group")
        .agg(
            accredition_number=("Accreditation No.", "first"),
            company_registration_number=("Company Registration Number", "first"),
            GWh=("GWh", "sum"),
            technology_group=("Technology Group", "first"),
            generation_type=("Generation Type", "first"),
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
        regos[(regos["Generating Station / Agent Group"] == rego_station_name) & (regos["months_difference"] == 1)]
        .groupby(["start", "end", "months_difference"])
        .agg(dict(GWh="sum"))
    )

    months_count = len(rego_station_volumes_by_month)
    if months_count > 12:
        raise AssertionError(
            f"Don't expect reporting to be more granuular than monthly: {rego_station_name} has {months_count} periods in the year"
        )

    return rego_station_volumes_by_month.reset_index().set_index("start").sort_index()
