from pathlib import Path
from typing import Optional

import pandas as pd

import ma.elexon.models
import ma.mapper.utils
from ma.mapper.bmu_helpers import get_matching_bmus_dict, validate_matching_bmus
from ma.mapper.common import MappingException
from ma.mapper.filter_on_aggregate_data import appraise_energy_volumes, appraise_rated_power
from ma.mapper.filter_on_bmu_meta_data import get_matching_bmus
from ma.mapper.rego_helpers import get_generator_profile
from ma.mapper.summarise_and_score import summarise_mapping_and_mapping_strength
from ma.ofgem.regos import groupby_station, load
from ma.ofgem.stations import load_accredited_stations

LOGGER = ma.mapper.utils.get_logger("ma.mapper")


def map_station(
    rego_station_name: str,
    regos: pd.DataFrame,
    accredited_stations: pd.DataFrame,
    bmus: pd.DataFrame,
    expected_mappings: Optional[dict] = None,
) -> pd.DataFrame:
    if not expected_mappings:
        expected_mappings = {}
    expected_mapping = expected_mappings.get(rego_station_name, dict(bmu_ids=[], override=False))

    generator_profile = {}
    matching_bmus = None
    try:
        # Get details of a REGO generator
        generator_profile.update(get_generator_profile(rego_station_name, regos, accredited_stations))

        # Add matching BMUs
        matching_bmus = get_matching_bmus(generator_profile, bmus, expected_mapping)
        validate_matching_bmus(matching_bmus)
        generator_profile.update(get_matching_bmus_dict(matching_bmus))

        # Appraise rated power
        generator_profile.update(appraise_rated_power(generator_profile))

        # Appraise energy volumes
        generator_profile.update(appraise_energy_volumes(generator_profile, regos))

    except MappingException as e:
        LOGGER.warning(str(e) + str(generator_profile))
    LOGGER.debug(ma.mapper.utils.to_yaml_text(generator_profile))
    return summarise_mapping_and_mapping_strength(generator_profile)


def map_station_range(
    start: int,
    stop: int,
    regos: pd.DataFrame,
    accredited_stations: pd.DataFrame,
    bmus: pd.DataFrame,
    expected_mappings: Optional[dict] = None,
) -> pd.DataFrame:
    regos_by_station = groupby_station(regos)
    station_summaries = []
    for i in range(start, stop):
        station_summaries.append(
            map_station(
                regos_by_station.iloc[i]["Generating Station / Agent Group"],
                regos,
                accredited_stations,
                bmus,
                expected_mappings,
            )
        )
    return pd.concat(station_summaries)


def main(
    start: int,
    stop: int,
    regos_path: Path,
    accredited_stations_dir: Path,
    bmus_path: Path,
    expected_mappings_file: Optional[Path] = None,
) -> pd.DataFrame:
    return map_station_range(
        start=start,
        stop=stop,
        regos=load(regos_path),
        accredited_stations=load_accredited_stations(accredited_stations_dir),
        bmus=ma.elexon.models.bmus(bmus_path),
        expected_mappings=(ma.mapper.utils.from_yaml_file(expected_mappings_file) if expected_mappings_file else {}),
    )
