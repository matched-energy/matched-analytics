# Get a subset of the NEOS data

from data.register import NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023
import pandas as pd
from pathlib import Path
import argparse


def get_subset(path: Path, output_path: Path) -> None:
    df = pd.read_csv(path)
    df["DATETIME"] = pd.to_datetime(df["DATETIME"])
    filtered_df = df[(df["DATETIME"] >= pd.Timestamp("2024-01-01")) & (df["DATETIME"] < pd.Timestamp("2024-01-04"))]
    filtered_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input CSV file")
    parser.add_argument(
        "--output_path",
        type=str,
        default=str(NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023),
        help="Path to the output CSV file",
    )
    args = parser.parse_args()
    get_subset(Path(args.input_path), Path(args.output_path))
