import argparse
import copy
from pathlib import Path

import pandas as pd

from ma.ofgem.regos import RegosRaw

STATIONS = [
    "Drax Power Station (REGO)",
    "Walney Extension",
    "Triton Knoll Offshore Windfarm",
]


def main(input_path: Path, output_path: Path) -> pd.DataFrame:
    regos_df = pd.read_csv(
        input_path,
        header=RegosRaw.from_file_header,
        skiprows=RegosRaw.from_file_skiprows,
        names=list(RegosRaw.schema.keys()),
    )
    regos_df = regos_df[regos_df["station_name"].isin(STATIONS)]
    skip_rows = copy.deepcopy(regos_df[: RegosRaw.from_file_skiprows])
    skip_rows.loc[:, "station_name"] = "SKIPPED_ROW"
    regos_df = pd.concat([skip_rows, regos_df])  # mimic four rows that have to be ignored from Ofgem download
    regos_df.to_csv(output_path, header=False, index=False)
    return regos_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_path",
        type=Path,
    )
    parser.add_argument(
        "output_path",
        type=Path,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.input_path, args.output_path)
