# Get a subset of the NEOS data

import pandas as pd
from pathlib import Path
import data.register


def get_subset(path: Path, output_path: Path) -> None:
    df = pd.read_csv(path)
    df["DATETIME"] = pd.to_datetime(df["DATETIME"])
    filtered_df = df[(df["DATETIME"] >= pd.Timestamp("2024-01-01")) & (df["DATETIME"] < pd.Timestamp("2024-01-04"))]
    filtered_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    get_subset(data.register.NESO_FUEL_CKAN_CSV, data.register.NESO_FUEL_CKAN_CSV_SUBSET)
