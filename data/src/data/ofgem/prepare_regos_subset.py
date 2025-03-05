import copy
import sys
from pathlib import Path

import pandas as pd

import ma.ofgem.regos

STATIONS = [
    "Drax Power Station (REGO)",
    "Walney Extension",
    "Triton Knoll Offshore Windfarm",
]


def main(input_path: Path, output_path: Path) -> pd.DataFrame:
    regos = ma.ofgem.regos.load(input_path)
    regos = regos[regos["station_name"].isin(STATIONS)]
    skip_rows = copy.deepcopy(regos[:4])
    skip_rows.loc[:, "station_name"] = "SKIPPED_ROW"
    regos = pd.concat([skip_rows, regos])  # mimic four rows that have to be ignored from Ofgem download
    regos.to_csv(output_path, header=False, index=False)
    return regos


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
