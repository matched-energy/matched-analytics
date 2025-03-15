import copy
import sys
from pathlib import Path

import pandas as pd

from ma.ofgem.regos import RegosRaw

STATIONS = [
    "Drax Power Station (REGO)",
    "Walney Extension",
    "Triton Knoll Offshore Windfarm",
]


def main(input_path: Path, output_path: Path) -> pd.DataFrame:
    regos_df = RegosRaw(input_path).df
    regos_df = regos_df[regos_df["station_name"].isin(STATIONS)]
    skip_rows = copy.deepcopy(regos_df[:4])
    skip_rows.loc[:, "station_name"] = "SKIPPED_ROW"
    regos_df = pd.concat([skip_rows, regos_df])  # mimic four rows that have to be ignored from Ofgem download
    regos_df.to_csv(output_path, header=False, index=False)
    return regos_df


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
