from pytest import approx

import data.register
from ma.elexon.metering_data.metering_data_by_half_hour_and_bmu import MeteringDataHalfHourlyByBmu
from ma.elexon.S0142.processed_S0142 import ProcessedS0142


def get_half_hourly_by_bmu() -> MeteringDataHalfHourlyByBmu:
    half_hourly_by_bmu = ProcessedS0142(
        data.register.S0142_20230330_SF_20230425121906_GOLD_CSV
    ).transform_to_half_hourly_by_bmu()
    return half_hourly_by_bmu


def test_init() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    assert half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum() == approx(-3417.849)
    assert len(half_hourly_by_bmu["bm_unit_id"].unique()) == 14


def test_filter_by_bmu_regex() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    filtered_half_hourly_by_bmu = MeteringDataHalfHourlyByBmu.filter(half_hourly_by_bmu, bm_regex="2__[AB]GESL000")
    assert len(filtered_half_hourly_by_bmu["bm_unit_id"].unique()) == 2


def test_filter_by_bmu_id() -> None:
    half_hourly_by_bmu = get_half_hourly_by_bmu()
    filtered_half_hourly_by_bmu = half_hourly_by_bmu.filter(bm_ids=["2__AGESL000"])
    assert len(filtered_half_hourly_by_bmu["bm_unit_id"].unique()) == 1


def test_plot() -> None:
    get_half_hourly_by_bmu().get_fig()


def test_transform_to_half_hourly() -> None:
    half_hourly = get_half_hourly_by_bmu().transform_to_half_hourly()
    assert half_hourly["bm_unit_metered_volume_mwh"].sum() == approx(-3417.849)
    assert half_hourly["bm_unit_metered_volume_+ve_mwh"].sum() == approx(0)
    assert half_hourly["bm_unit_metered_volume_-ve_mwh"].sum() == approx(-3417.849)
