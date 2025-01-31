from typing import Dict

import pandas as pd

from ma.mapper.common import MappingException


def get_rego_station_volume_stats(
    monthly_volumes: pd.DataFrame,
    station_dnc_mw: float,
) -> Dict:
    total_gwh = monthly_volumes["GWh"].sum()
    total_mwh = total_gwh * 1e3
    months_count = len(monthly_volumes)
    nameplate_mwh = station_dnc_mw * 24 * 365 * months_count / 12
    return dict(
        rego_total_volume=total_gwh,
        rego_capacity_factor=total_mwh / nameplate_mwh,
        rego_sample_months=months_count,
    )


def get_generator_profile(rego_station_name: str, regos: pd.DataFrame, accredited_stations: pd.DataFrame) -> dict:
    rego_accreditation_numbers = regos[regos["Generating Station / Agent Group"] == rego_station_name][
        "Accreditation No."
    ].unique()
    if not len(rego_accreditation_numbers) == 1:
        raise MappingException(
            f"Found multiple accreditation numbers for {rego_station_name}: {rego_accreditation_numbers}"
        )

    rego_accreditation_number = rego_accreditation_numbers[0]
    accredited_station = accredited_stations[
        (accredited_stations["AccreditationNumber"] == rego_accreditation_number)
        & (accredited_stations["Scheme"] == "REGO")
    ]
    if not len(accredited_station) == 1:
        raise MappingException(
            f"Expected 1 accredited_station for {rego_accreditation_numbers} but found"
            + f"{list(accredited_station['GeneratingStation'][:5])}"
        )

    return dict(
        {
            "rego_station_name": rego_station_name,
            "rego_accreditation_number": rego_accreditation_number,
            "rego_station_dnc_mw": accredited_station.iloc[0]["StationDNC_MW"],
            "rego_station_technology": accredited_station.iloc[0]["Technology"],
        }
    )
