import data.register
import ma.elexon.metering_data.asset_half_hourly_by_bmu
import ma.elexon.metering_data.metering_data_rollup
import ma.elexon.metering_data_rollup
from ma.elexon.S0142 import process_raw
from ma.utils.enums import TemporalGranularity


def test_transforms() -> None:
    day_1_s0142 = process_raw.ProcessedS0142.from_file(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV)
    day_2_s0142 = process_raw.ProcessedS0142.from_file(data.register.S0142_20230331_SF_20230426191253_GOLD_CSV)
    day_1_s0142_bm_mwh_sum = day_1_s0142["bm_unit_metered_volume_mwh"].sum()
    day_2_s0142_bm_mwh_sum = day_2_s0142["bm_unit_metered_volume_mwh"].sum()

    day_1_half_hourly_by_bmu = process_raw.transform_to_half_hourly_by_bmu(day_1_s0142)
    assert len(day_1_half_hourly_by_bmu) == len(day_1_s0142)
    assert "settlement_datetime" in day_1_half_hourly_by_bmu.columns
    assert day_1_half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    day_2_half_hourly_by_bmu = process_raw.transform_to_half_hourly_by_bmu(day_2_s0142)

    day_1_half_hourly = ma.elexon.metering_data.asset_half_hourly_by_bmu.transform_to_half_hourly(day_1_half_hourly_by_bmu)
    assert len(day_1_half_hourly) == 48
    assert day_1_half_hourly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    day_2_half_hourly = ma.elexon.metering_data.asset_half_hourly_by_bmu.transform_to_half_hourly(day_2_half_hourly_by_bmu)

    day_1_daily = ma.elexon.metering_data.metering_data_rollup.transform_to_daily(day_1_half_hourly)
    assert len(day_1_daily) == 1
    assert day_1_daily["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum
    day_2_daily = ma.elexon.metering_data.metering_data_rollup.transform_to_daily(day_2_half_hourly)

    monthly = ma.elexon.metering_data.metering_data_rollup.transform_from_daily(
        [day_1_daily, day_2_daily], TemporalGranularity.MONTHLY
    )
    assert len(monthly) == 1
    assert monthly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum

    yearly = ma.elexon.metering_data.metering_data_rollup.transform_from_daily([monthly], TemporalGranularity.YEARLY)
    assert len(yearly) == 1
    assert yearly["bm_unit_metered_volume_mwh"].sum() == day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum
