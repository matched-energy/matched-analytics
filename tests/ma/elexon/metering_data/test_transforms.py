import data.register
from ma.elexon.metering_data.metering_data_by_half_hour_and_bmu import MeteringDataHalfHourlyByBmu
from ma.elexon.metering_data.metering_data_by_time import MeteringDataDaily, MeteringDataHalfHourly, MeteringDataMonthly
from ma.elexon.S0142.process_raw import ProcessedS0142


def test_transforms() -> None:
    day_1_s0142 = ProcessedS0142.from_file(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV)
    day_2_s0142 = ProcessedS0142.from_file(data.register.S0142_20230331_SF_20230426191253_GOLD_CSV)
    day_1_s0142_bm_mwh_sum = day_1_s0142["bm_unit_metered_volume_mwh"].sum()
    day_2_s0142_bm_mwh_sum = day_2_s0142["bm_unit_metered_volume_mwh"].sum()

    day_1_half_hourly_by_bmu = ProcessedS0142.transform_to_half_hourly_by_bmu(day_1_s0142)
    assert len(day_1_half_hourly_by_bmu) == len(day_1_s0142)
    assert day_1_half_hourly_by_bmu.index.name == "settlement_datetime"
    assert day_1_half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    day_2_half_hourly_by_bmu = ProcessedS0142.transform_to_half_hourly_by_bmu(day_2_s0142)

    day_1_half_hourly = MeteringDataHalfHourlyByBmu.transform_to_half_hourly(day_1_half_hourly_by_bmu)
    assert len(day_1_half_hourly) == 48
    assert day_1_half_hourly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    day_2_half_hourly = MeteringDataHalfHourlyByBmu.transform_to_half_hourly(day_2_half_hourly_by_bmu)

    day_1_daily = MeteringDataHalfHourly.transform_to_daily(day_1_half_hourly)
    assert len(day_1_daily) == 1
    assert day_1_daily["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    assert day_1_daily["settlement_period_count"].unique() == 48
    day_2_daily = MeteringDataHalfHourly.transform_to_daily(day_2_half_hourly)

    monthly = MeteringDataDaily.transform_to_monthly([day_1_daily, day_2_daily])
    assert len(monthly) == 1
    assert monthly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum
    assert monthly["day_count"].unique() == 2

    yearly = MeteringDataMonthly.transform_to_yearly([monthly])
    assert len(yearly) == 1
    assert yearly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum
    assert yearly["month_count"].unique() == 1
    assert len(yearly) == 1
    assert yearly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum
    assert yearly["month_count"].unique() == 1
