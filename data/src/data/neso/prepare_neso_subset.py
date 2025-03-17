from data.register import NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023
import pandas as pd
from pathlib import Path
import argparse


def get_subset(path: Path, output_path: Path) -> None:
    start_date = pd.Timestamp("2023-02-01")
    end_date = pd.Timestamp("2023-04-01")
    neso_df = pd.read_csv(path)
    neso_df["DATETIME"] = pd.to_datetime(neso_df["DATETIME"])
    neso_df = neso_df[(neso_df["DATETIME"] >= start_date) & (neso_df["DATETIME"] < end_date)]
    neso_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input CSV file")
    parser.add_argument(
        "--output_path",
        type=str,
        default=str(NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023),
        help="Path to the output CSV file",
    )
    args = parser.parse_args()
    get_subset(Path(args.input_path), Path(args.output_path))
