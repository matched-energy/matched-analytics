from pathlib import Path
from ma.utils.enums import TechEnum
from ma.utils.pandas import apply_schema
from ma.neso.schema_historic_gen import historic_gen_schema_on_load
import pandas as pd
import data.register
import httpx
from ma.utils.io import get_logger

logger = get_logger(__name__)


def download(
    output_file_path: Path = Path(data.register.NESO_FUEL_CKAN_CSV),
) -> None:
    csv_url = "https://api.neso.energy/dataset/88313ae5-94e4-4ddc-a790-593554d8c6b9/resource/f93d1835-75bc-43e5-84ad-12472b180a98/download/df_fuel_ckan.csv"

    with httpx.Client(follow_redirects=True) as client:
        response = client.get(csv_url)
        with open(output_file_path, "wb") as file:
            file.write(response.content)

    logger.info(f"Downloaded CSV file to {output_file_path}")


def load(historic_gen_path: Path) -> pd.DataFrame:
    historic_gen = pd.read_csv(historic_gen_path)
    historic_gen = apply_schema(historic_gen, historic_gen_schema_on_load)
    # Convert datetime column to pandas Timestamp
    historic_gen["datetime"] = pd.to_datetime(historic_gen["datetime"])
    return historic_gen


def filter(historic_gen: pd.DataFrame, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp) -> pd.DataFrame:
    """
    Filter by start and end datetime, inclusive of the end datetime.
    """
    return historic_gen[(historic_gen["datetime"] >= start_datetime) & (historic_gen["datetime"] <= end_datetime)]


def groupby_tech_and_month(historic_gen: pd.DataFrame) -> pd.DataFrame:
    """
    Group by tech and month, and sum the values. Returns MWh per month for each tech.
    """
    historic_gen = historic_gen.assign(year=historic_gen["datetime"].dt.year, month=historic_gen["datetime"].dt.month)
    return (
        historic_gen.groupby(["year", "month"])[[t.value for t in TechEnum]].sum() / 2
    )  # Divide by 2 because we're converting to MWh


if __name__ == "__main__":
    download()
