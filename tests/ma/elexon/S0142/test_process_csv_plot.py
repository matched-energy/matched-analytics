from typing import List, Optional

import pandas as pd
from pytest import approx

import data.register
import ma.elexon.S0142.plot as plot
import ma.elexon.S0142.process_csv as process_csv


def wrapper_process_directory(
    aggregate_bms: bool = True,
    bm_regex: Optional[str] = None,
    bm_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    bm_vols = process_csv.process_directory(
        input_dir=data.register.S0142_CSV_DIR,
        bsc_lead_party_id="GOLD",
        aggregate_bms=aggregate_bms,
        bm_regex=bm_regex,
        bm_ids=bm_ids,
    )
    return bm_vols


def test_filtering_and_grouping() -> None:
    # Aggregated
    bm_vols = wrapper_process_directory(aggregate_bms=True)
    assert bm_vols["bm_unit_metered_volume"].sum() == approx(-6945.599)
    assert len(bm_vols["bm_unit_id"].unique()) == 1

    # Unaggregated BMs
    bm_vols = wrapper_process_directory(aggregate_bms=False)
    assert bm_vols["bm_unit_metered_volume"].sum() == approx(-6945.599)
    assert len(bm_vols["bm_unit_id"].unique()) == 14

    # REGEX
    bm_vols = wrapper_process_directory(aggregate_bms=False, bm_regex="2__[AB]GESL000")
    assert len(bm_vols["bm_unit_id"].unique()) == 2

    # BM selection
    bm_vols = wrapper_process_directory(aggregate_bms=False, bm_ids=["2__AGESL000"])
    assert len(bm_vols["bm_unit_id"].unique()) == 1


def test_plot() -> None:
    load = wrapper_process_directory(aggregate_bms=True)
    plot.get_fig(load)
