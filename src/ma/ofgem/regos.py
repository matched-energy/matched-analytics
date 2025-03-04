from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd

from ma.ofgem.enums import RegoCompliancePeriod, RegoScheme, RegoStatus
from ma.ofgem.schema_regos import rego_schema_on_load, transform_regos_schema
from ma.utils.pandas import apply_schema


def load(
    regos_path: Path,
    holders: Optional[list[str]] = None,
    statuses: Optional[list[RegoStatus]] = [RegoStatus.REDEEMED],
    schemes: Optional[list[RegoScheme]] = [RegoScheme.REGO],
    reporting_period: Optional[RegoCompliancePeriod] = None,
) -> pd.DataFrame:
    regos = pd.read_csv(regos_path, skiprows=4, header=None)
    regos = apply_schema(regos, rego_schema_on_load, transform_regos_schema)
    regos = filter(regos, holders=holders, statuses=statuses, schemes=schemes, reporting_period=reporting_period)
    return regos


def filter(
    regos: pd.DataFrame,
    holders: Optional[list[str]] = None,
    statuses: Optional[list[RegoStatus]] = None,
    schemes: Optional[list[RegoScheme]] = [RegoScheme.REGO],
    reporting_period: Optional[RegoCompliancePeriod] = None,
) -> pd.DataFrame:
    filters = []
    if holders:
        filters.append((regos["current_holder"].isin(holders)))

    if statuses:
        filters.append(regos["certificate_status"].isin(statuses))

    if schemes:
        filters.append(regos["scheme"].isin(schemes))

    if reporting_period:
        start_date, end_date = reporting_period.date_range
        period_start = pd.Timestamp(start_date)
        period_end = pd.Timestamp(end_date)
        period_filter = (regos["period_start"] >= period_start) & (regos["period_end"] <= period_end)
        filters.append(period_filter)

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


def groupby_tech_month_holder(regos: pd.DataFrame) -> pd.DataFrame:
    # Extract month from period_start for grouping
    regos = regos.copy()
    regos["month"] = regos["period_start"].dt.to_period("M")

    # Groupby tech, month, and holder
    regos_by_tech_month_holder = (
        regos.groupby(["tech_simple", "month", "current_holder"])
        .agg(
            rego_gwh=("rego_gwh", "sum"),
            station_count=("station_name", "nunique"),
        )
        .sort_values(by=["tech_simple", "month", "current_holder"])
    )

    results = regos_by_tech_month_holder.reset_index().set_index("month")
    return results
