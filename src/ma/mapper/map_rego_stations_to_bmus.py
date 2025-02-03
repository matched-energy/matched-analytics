import pprint
from pathlib import Path
from typing import Optional

import click
import pandas as pd

import ma.elexon.bmus
import ma.ofgem.regos
import ma.ofgem.stations
from ma.mapper.bmu_helpers import get_matching_bmus_dict, validate_matching_bmus
from ma.mapper.common import MappingException
from ma.mapper.filter_on_aggregate_data import appraise_energy_volumes, appraise_rated_power
from ma.mapper.filter_on_bmu_meta_data import get_matching_bmus
from ma.mapper.rego_helpers import get_generator_profile
from ma.mapper.summarise_and_score import abbreviate_summary, score_mapping, summarise_profile
from ma.utils.io import get_logger

LOGGER = get_logger("ma.mapper")


def map_station(
    rego_station_name: str,
    regos: pd.DataFrame,
    accredited_stations: pd.DataFrame,
    bmus: pd.DataFrame,
    S0142_csv_dir: Path,
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
        generator_profile.update(appraise_energy_volumes(generator_profile, regos, S0142_csv_dir))

        LOGGER.debug("\n" + pprint.pformat(generator_profile, width=100))

    except MappingException as e:
        LOGGER.warning(str(e))
        LOGGER.warning("\n" + pprint.pformat(generator_profile, width=100))

    scores = score_mapping(generator_profile)
    profile = summarise_profile(generator_profile)

    return pd.concat([profile, scores], axis=1)


def map_station_range(
    start: int,
    stop: int,
    regos: pd.DataFrame,
    accredited_stations: pd.DataFrame,
    bmus: pd.DataFrame,
    bmu_vol_dir: Path,
    expected_mappings: Optional[dict] = None,
    mappings_path: Optional[Path] = None,
    abbreviated_mappings_path: Optional[Path] = None,
) -> pd.DataFrame:
    regos_by_station = ma.ofgem.regos.groupby_station(regos)
    summaries = []
    for i in range(start, stop):
        summaries.append(
            map_station(
                regos_by_station.iloc[i]["station_name"],
                regos,
                accredited_stations,
                bmus,
                bmu_vol_dir,
                expected_mappings,
            )
        )

    summary = pd.concat(summaries)
    if mappings_path:
        summary.to_csv(mappings_path, float_format="%.2f")
    if abbreviated_mappings_path:
        abbreviate_summary(summary).to_csv(abbreviated_mappings_path, float_format="%.2f")
    return summary


@click.command()
@click.option("--start", type=int)
@click.option("--stop", type=int)
@click.option("--regos-path", type=click.Path(exists=True, path_type=Path))
@click.option("--accredited-stations-dir", type=click.Path(exists=True, path_type=Path))
@click.option("--bmus-path", type=click.Path(exists=True, path_type=Path))
@click.option("--bmu-vol-dir", type=click.Path(exists=True, path_type=Path))
@click.option("--expected-mappings-file", type=click.Path(exists=True, path_type=Path), default=None)
@click.option("--mappings-path", type=click.Path(path_type=Path), default=None)
@click.option("--abbreviated-mappings-path", type=click.Path(path_type=Path), default=None)
def cli(
    start: int,
    stop: int,
    regos_path: Path,
    accredited_stations_dir: Path,
    bmus_path: Path,
    bmu_vol_dir: Path,
    expected_mappings_file: Optional[Path] = None,
    mappings_path: Optional[Path] = None,
    abbreviated_mappings_path: Optional[Path] = None,
) -> None:
    map_station_range(
        start,
        stop,
        ma.ofgem.regos.load(regos_path),
        ma.ofgem.stations.load_from_dir(accredited_stations_dir),
        ma.elexon.bmus.load(bmus_path),
        bmu_vol_dir,
        (ma.utils.io.from_yaml_file(expected_mappings_file) if expected_mappings_file else {}),
        mappings_path,
        abbreviated_mappings_path,
    )


if __name__ == "__main__":
    cli()
