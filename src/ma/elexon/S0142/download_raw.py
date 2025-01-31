import datetime
import os
import sys
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

import ma.utils.io

LOG = ma.utils.io.get_logger(__name__)

## Get Elexon API key
try:
    load_dotenv()
    API_KEY = os.getenv("ELEXON_API_KEY")
    assert API_KEY is not None
except AssertionError:
    raise Exception("Unable to load ELEXON_API_KEY from .env file")

BASE_URL = "https://downloads.elexonportal.co.uk/p114"


def download_file(filename: str, download_dir: Path) -> None:
    local_filepath = download_dir / filename
    if not local_filepath.is_file():
        LOG.debug(rf"Downloading {filename}")
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/download?key={API_KEY}&filename={filename}")
            with open(local_filepath, "wb") as f:
                f.write(response.content)
    else:
        LOG.debug(rf"Skipping {filename}")


def filter_files(files: dict, pattern: str = "_SF_") -> list[str]:
    if len(files) == 0:
        return []
    else:
        return [f for f in files.keys() if pattern in f]


def get_dict_of_files(date: pd.Timestamp) -> dict:
    """Returns a dict with filenames as the keys"""
    with httpx.Client() as client:
        response = client.get(f"{BASE_URL}/list?key={API_KEY}&date={date:%Y-%m-%d}&filter=s0142")
        response.raise_for_status()
    return response.json()


def main(start_date: pd.Timestamp, end_date: pd.Timestamp, download_dir: Path) -> None:
    date = start_date
    while date < end_date:
        for f in filter_files(get_dict_of_files(date)):
            download_file(f, download_dir)
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    main(pd.Timestamp(sys.argv[1]), pd.Timestamp(sys.argv[2]), Path(sys.argv[3]))
