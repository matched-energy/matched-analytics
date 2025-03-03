from pathlib import Path
from ma.utils.enums import ProductionTechEnum
from ma.utils.pandas import apply_schema
from ma.neso.schema_grid_mix import grid_mix_schema_on_load, transform_grid_mix_schema
import pandas as pd
import httpx
import argparse
from ma.utils.io import get_logger

logger = get_logger(__name__)


def download(
    output_file_path: Path,
) -> None:
    csv_url = "https://api.neso.energy/dataset/88313ae5-94e4-4ddc-a790-593554d8c6b9/resource/f93d1835-75bc-43e5-84ad-12472b180a98/download/df_fuel_ckan.csv"

    with httpx.Client(follow_redirects=True) as client:
        response = client.get(csv_url)
        with open(output_file_path, "wb") as file:
            file.write(response.content)

    logger.info(f"Downloaded CSV file to {output_file_path}")


def load(grid_mix_path: Path) -> pd.DataFrame:
    grid_mix = pd.read_csv(grid_mix_path)
    grid_mix = apply_schema(grid_mix, grid_mix_schema_on_load, transform_grid_mix_schema)
    return grid_mix


def filter(grid_mix: pd.DataFrame, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp) -> pd.DataFrame:
    """
    Filter by start and end datetime, exclusive of the end datetime.
    """
    if not isinstance(grid_mix.index, pd.DatetimeIndex):
        raise TypeError("grid_mix must have a DatetimeIndex")

    filtered_grid_mix = grid_mix[(grid_mix.index >= start_datetime) & (grid_mix.index < end_datetime)]
    return filtered_grid_mix


def groupby_tech_and_month(grid_mix: pd.DataFrame) -> pd.DataFrame:
    """
    Group by tech and month, and sum the values. Returns MWh per month for each tech.
    """
    grid_mix = grid_mix.reset_index()
    grid_mix = grid_mix.assign(year=grid_mix["datetime"].dt.year, month=grid_mix["datetime"].dt.month)
    return grid_mix.groupby(["year", "month"])[[f"{t.value}_mwh" for t in ProductionTechEnum]].sum()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output_file_path",
        nargs="?",
        type=Path,
    )
    args = parser.parse_args()
    download(args.output_file_path)
