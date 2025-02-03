from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from ma.ofgem.schema_regos import rego_schema_on_load, transform_regos_schema
from ma.utils.pandas import apply_schema


def load(
    regos_path: Path,
    holders: Optional[List[str]] = None,
    statuses: Optional[List[str]] = ["Redeemed"],
    schemes: Optional[List[str]] = ["REGO"],
) -> pd.DataFrame:
    regos = pd.read_csv(regos_path, skiprows=4, header=None)
    regos = apply_schema(regos, rego_schema_on_load, transform_regos_schema)
    regos = filter(regos, holders=holders, statuses=statuses, schemes=schemes)
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
            rego_gwh=("rego_gwh", "sum"),
            technology_group=("technology_group", "first"),
            generation_type=("generation_type", "first"),
            tech_simple=("tech_simple", "first"),
        )
        .sort_values(by="rego_gwh", ascending=False)
    )

    # Station output as a fraction of a whole
    regos_by_station["percentage_of_whole"] = regos_by_station["rego_gwh"] / regos_by_station["rego_gwh"].sum() * 100

    return regos_by_station.reset_index()


def get_rego_station_volume_by_month(
    regos: pd.DataFrame,
    rego_station_name: str,
) -> pd.DataFrame:
    rego_station_volumes_by_month = (
        regos[(regos["station_name"] == rego_station_name) & (regos["months_difference"] == 1)]
        .groupby(["start", "end", "months_difference"])
        .agg(dict(rego_gwh="sum"))
    )

    months_count = len(rego_station_volumes_by_month)
    if months_count > 12:
        raise AssertionError(
            f"Don't expect reporting to be more granuular than monthly: {rego_station_name} has {months_count} periods in the year"
        )

    return rego_station_volumes_by_month.reset_index().set_index("start").sort_index()
