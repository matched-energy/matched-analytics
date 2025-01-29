from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import ma.elexon.S0142.process_raw  # noqa: F401
from ma.elexon.S0142 import process_raw


def run_process_file(bsc_party_ids: List[str]) -> Dict[str, pd.DataFrame]:
    return {
        bsc_party_id: S0142_df
        for bsc_party_id, S0142_df in process_raw.process_file(
            input_path=Path(__file__).parent / "data" / "S0142_20230330_SF_20230425121906.gz",
            bsc_party_ids=bsc_party_ids,
        )
    }


@pytest.mark.slow
def test_process_file() -> None:
    expected_metrics = {
        "PURE": {"Unique BM Units": 55, "Number of rows": 55 * 48, "BM Unit Metered Volume": 376.365},
        "MERCURY": {"Unique BM Units": 90, "Number of rows": 90 * 48, "BM Unit Metered Volume": -34926.051},
    }
    expected_types = {  # non-exhaustive
        "BM Unit Id": str,
        "Settlement Period": np.int64,
        "Period BM Unit Balancing Services Volume": np.float64,
        "Period Information Imbalance Volume": np.float64,
        "Period Expected Metered Volume": np.float64,
        "BM Unit Metered Volume": np.float64,
        "Transmission Loss Factor": np.float64,
        "Transmission Loss Multiplier": np.float64,
        "Trading Unit Name": str,
        "Total Trading Unit Metered Volume": np.float64,
    }
    for bsc_party_id, S0142_df in run_process_file(bsc_party_ids=list(expected_metrics.keys())).items():
        assert len(S0142_df) == expected_metrics[bsc_party_id]["Number of rows"]
        assert len(S0142_df["BM Unit Id"].unique()) == expected_metrics[bsc_party_id]["Unique BM Units"]
        assert S0142_df["BM Unit Metered Volume"].sum() == expected_metrics[bsc_party_id]["BM Unit Metered Volume"]

    for expected_col, expected_type in expected_types.items():
        assert expected_col in S0142_df.columns
        assert isinstance(S0142_df[expected_col].iloc[0], expected_type)


@patch("os.listdir")
@patch("glob.glob")
def test_main(mock_glob: MagicMock, mock_listdir: MagicMock) -> None:
    mock_listdir.return_value = ["S0142_file1.csv.gz", "S0142_file2.csv.gz"]
    mock_glob.return_value = []
    mock_process_file = MagicMock(return_value=[("BSC1", MagicMock())])

    with patch("ma.elexon.S0142.process_raw.process_file", mock_process_file):
        ma.elexon.S0142.process_raw.process_directory(
            input_dir=Path("/fake/input"),
            output_dir=Path("/fake/output"),
            bsc_party_ids=["BSC1", "BSC2"],
        )

        assert mock_process_file.call_count == 2
        mock_process_file.assert_any_call(Path("/fake/input/S0142_file1.csv.gz"), ["BSC1", "BSC2"])
        mock_process_file.assert_any_call(Path("/fake/input/S0142_file2.csv.gz"), ["BSC1", "BSC2"])


if __name__ == "__main__":
    run_process_file(["PURE"])
