from pytest import approx

import data.register
from ma.elexon.metering_data import asset_half_hourly_by_bmu
from ma.elexon.S0142 import process_raw


def get_half_hourly_by_bmu() -> asset_half_hourly_by_bmu.MeteringDataHalfHourlyByBmuType:
    half_hourly_by_bmu = process_raw.transform_to_half_hourly_by_bmu(
        process_raw.ProcessedS0142.from_file(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV)
    )
    assert half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum() == approx(-3417.849)
    assert len(half_hourly_by_bmu["bm_unit_id"].unique()) == 14
    return half_hourly_by_bmu


def test_rollup_bmus() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    half_hourly = asset_half_hourly_by_bmu.rollup_bmus(
        asset_half_hourly_by_bmu.segregate_import_exports(half_hourly_by_bmu)
    )
    assert half_hourly["bm_unit_metered_volume_mwh"].sum() == approx(-3417.849)


def test_filter_by_bmu_regex() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    filtered_half_hourly_by_bmu = asset_half_hourly_by_bmu.filter(half_hourly_by_bmu, bm_regex="2__[AB]GESL000")
    assert len(filtered_half_hourly_by_bmu["bm_unit_id"].unique()) == 2


def test_filter_by_bmu_id() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    filtered_half_hourly_by_bmu = asset_half_hourly_by_bmu.filter(half_hourly_by_bmu, bm_ids=["2__AGESL000"])
    assert len(filtered_half_hourly_by_bmu["bm_unit_id"].unique()) == 1


def test_plot() -> None:
    load = get_half_hourly_by_bmu()
    asset_half_hourly_by_bmu.get_fig(load)
