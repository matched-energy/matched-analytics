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
        start_year_month = pd.Timestamp(start_date)
        end_year_month = pd.Timestamp(end_date)
        period_filter = (regos["start_year_month"] >= start_year_month) & (regos["end_year_month"] < end_year_month)
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
        tech_category_unique=("tech_category", "nunique"),
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
            tech_category=("tech_category", "first"),
        )
        .sort_values(by="rego_gwh", ascending=False)
    )

    # Station output as a fraction of a whole
    regos_by_station["percentage_of_whole"] = regos_by_station["rego_gwh"] / regos_by_station["rego_gwh"].sum() * 100

    return regos_by_station.reset_index()


def _expand_multi_month_certificates(regos: pd.DataFrame) -> pd.DataFrame:
    """
    Expand certificates that span multiple months into separate rows, with
    the generation amount evenly distributed across each month.
    """
    expanded_rows = []

    for _, row in regos.iterrows():
        # If there's only one month, keep the row as is
        if row["period_months"] == 1:
            expanded_rows.append(row.to_dict())
        else:
            # Create a row for each month in the period
            start_date = row["start_year_month"]
            months_to_generate = row["period_months"]

            # Calculate the per-month generation
            per_month_gwh = row["rego_gwh"] / months_to_generate

            # Generate rows for each month in the period
            for month_offset in range(months_to_generate):
                month_date = start_date + pd.DateOffset(months=month_offset)
                new_row = row.to_dict().copy()
                new_row["start_year_month"] = month_date  # Update start_year_month
                new_row["rego_gwh"] = per_month_gwh  # Distribute generation evenly
                expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows)


def groupby_tech_month_holder(regos: pd.DataFrame) -> pd.DataFrame:
    # Extract month from start_year_month for grouping
    regos = regos.copy()

    # Expand certificates that span multiple months
    # Check if period_months exists and there are records with multi-month periods
    if "period_months" in regos.columns and (regos["period_months"] > 1).any():
        regos = _expand_multi_month_certificates(regos)

    regos["month"] = regos["start_year_month"].dt.to_period("M")

    # Groupby tech, month, and holder
    regos_by_tech_month_holder = (
        regos.groupby(["tech_category", "month", "current_holder"])
        .agg(
            rego_gwh=("rego_gwh", "sum"),
            station_count=("station_name", "nunique"),
        )
        .sort_values(by=["tech_category", "month", "current_holder"])
    )

    results = regos_by_tech_month_holder.reset_index().set_index("month")
    return results
