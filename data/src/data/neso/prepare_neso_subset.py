# Get a subset of the NEOS data

import pandas as pd
from pathlib import Path
import data.register


def get_subset(path: Path, output_path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # trim to first 17521 rows (i.e. 2009's data)
    df = df.iloc[:17521]
    df.to_csv(output_path, index=False)
    return df


if __name__ == "__main__":
    get_subset(data.register.NESO_FUEL_CKAN_CSV, data.register.NESO_FUEL_CKAN_CSV_SUBSET)
