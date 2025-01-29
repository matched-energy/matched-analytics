import pandas as pd
from pytest import approx

import ma.elexon.S0142.plot as plot
import ma.elexon.S0142.process_csv as process_csv
import test_data.register


def run_simple(group_bms: bool) -> pd.DataFrame:
    load = process_csv.process_directory(
        input_dir=test_data.register.S0142_CSV_DIR,
        bsc_lead_party_id="GOLD",
        group_bms=group_bms,
    )
    return load


def test_aggregated() -> None:
    load = run_simple(group_bms=True)
    assert load["BM Unit Metered Volume"].sum() == approx(-6945.599)
    assert len(load["BM Unit Id"].unique()) == 1


def test_disaggregated() -> None:
    load = run_simple(group_bms=False)
    assert load["BM Unit Metered Volume"].sum() == approx(-6945.599)
    assert len(load["BM Unit Id"].unique()) == 14


def test_plot() -> None:
    load = run_simple(group_bms=True)
    plot.get_fig(load)
