from typing import Dict

import pandas as pd

from ma.mapper.common import MappingException
from ma.ofgem.regos import RegosProcessed


def get_rego_station_volume_stats(
    monthly_volumes: pd.DataFrame,
    station_dnc_mw: float,
) -> Dict:
    total_gwh = monthly_volumes["rego_gwh"].sum()
    total_mwh = total_gwh * 1e3
    months_count = len(monthly_volumes)
    nameplate_mwh = station_dnc_mw * 24 * 365 * months_count / 12
    return dict(
        rego_total_volume=total_gwh,
        rego_capacity_factor=total_mwh / nameplate_mwh,
        rego_sample_months=months_count,
    )


def get_generator_profile(rego_station_name: str, regos: RegosProcessed, accredited_stations: pd.DataFrame) -> dict:
    rego_accreditation_numbers = regos.df()[regos["station_name"] == rego_station_name]["accreditation_number"].unique()
    if not len(rego_accreditation_numbers) == 1:
        raise MappingException(
            f"Found multiple accreditation numbers for {rego_station_name}: {rego_accreditation_numbers}"
        )

    rego_accreditation_number = rego_accreditation_numbers[0]
    accredited_station = accredited_stations[
        (accredited_stations["accreditation_number"] == rego_accreditation_number)
        & (accredited_stations["scheme"] == "REGO")
    ]
    if not len(accredited_station) == 1:
        raise MappingException(
            f"Expected 1 accredited_station for {rego_accreditation_numbers} but found"
            + f"{list(accredited_station['generating_station'][:5])}"
        )

    return dict(
        {
            "rego_station_name": rego_station_name,
            "rego_accreditation_number": rego_accreditation_number,
            "rego_station_dnc_mw": accredited_station.iloc[0]["station_dnc_mw"],
            "rego_station_technology": accredited_station.iloc[0]["technology"],
        }
    )


def get_rego_station_volume_by_month(
    regos: RegosProcessed,
    rego_station_name: str,
) -> pd.DataFrame:
    rego_station_volumes_by_month = (
        regos.df()[(regos["station_name"] == rego_station_name) & (regos["period_months"] == 1)]
        .groupby(["start_year_month", "end_year_month", "period_months"])
        .agg(dict(rego_gwh="sum"))
    )

    months_count = len(rego_station_volumes_by_month)
    if months_count > 12:
        raise AssertionError(
            f"Don't expect reporting to be more granuular than monthly: {rego_station_name} has {months_count} periods in the year"
        )

    return rego_station_volumes_by_month.sort_index().reset_index().set_index("start_year_month")
