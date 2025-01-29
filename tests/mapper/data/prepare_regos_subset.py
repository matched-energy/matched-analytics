import sys
from pathlib import Path

import pandas as pd

import ma.mapper.data.regos

STATIONS = [
    "Drax Power Station (REGO)",
    "Walney Extension",
    "Triton Knoll Offshore Windfarm",
]


def main(input_path: Path, output_path: Path) -> pd.DataFrame:
    regos = ma.mapper.data.regos.read_from_file(input_path)
    regos = regos[regos["Generating Station / Agent Group"].isin(STATIONS)]
    regos.to_csv(output_path)
    return regos


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
