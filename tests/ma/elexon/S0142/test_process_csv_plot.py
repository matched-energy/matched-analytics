from typing import List, Optional

import pandas as pd
from pytest import approx

import data.register
import ma.elexon.S0142.plot as plot
import ma.elexon.S0142.process_csv as process_csv


def run_simple(
    aggregate_bms: bool = True,
    bm_regex: Optional[str] = None,
    bm_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    load = process_csv.process_directory(
        input_dir=data.register.S0142_CSV_DIR,
        bsc_lead_party_id="GOLD",
        aggregate_bms=aggregate_bms,
        bm_regex=bm_regex,
        bm_ids=bm_ids,
    )
    return load


def test_filtering_and_grouping() -> None:
    # Aggregated
    load = run_simple(aggregate_bms=True)
    assert load["BM Unit Metered Volume"].sum() == approx(-6945.599)
    assert len(load["BM Unit Id"].unique()) == 1

    # Unaggregated BMs
    load = run_simple(aggregate_bms=False)
    assert load["BM Unit Metered Volume"].sum() == approx(-6945.599)
    assert len(load["BM Unit Id"].unique()) == 14

    # REGEX
    load = run_simple(aggregate_bms=False, bm_regex="2__[AB]GESL000")
    assert len(load["BM Unit Id"].unique()) == 2

    # BM selection
    load = run_simple(aggregate_bms=False, bm_ids=["2__AGESL000"])
    assert len(load["BM Unit Id"].unique()) == 1


def test_plot() -> None:
    load = run_simple(aggregate_bms=True)
    plot.get_fig(load)
