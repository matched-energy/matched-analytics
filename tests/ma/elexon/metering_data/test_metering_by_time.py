import pandas as pd
import pytest
from pytest import approx

import data.register
from ma.elexon.metering_data.metering_data_by_time import MeteringDataDaily, MeteringDataMonthly
from ma.elexon.S0142.processed_S0142 import ProcessedS0142


def test_transforms() -> None:
    day_1_s0142 = ProcessedS0142(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV)
    day_2_s0142 = ProcessedS0142(data.register.S0142_20230331_SF_20230426191253_GOLD_CSV)
    day_1_s0142_bm_mwh_sum = day_1_s0142["bm_unit_metered_volume_mwh"].sum()
    day_2_s0142_bm_mwh_sum = day_2_s0142["bm_unit_metered_volume_mwh"].sum()

    day_1_half_hourly_by_bmu = day_1_s0142.transform_to_half_hourly_by_bmu()
    assert len(day_1_half_hourly_by_bmu.df) == len(day_1_s0142.df)
    assert day_1_half_hourly_by_bmu.df.index.name == "settlement_datetime"
    assert day_1_half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum() == approx(day_1_s0142_bm_mwh_sum)
    day_2_half_hourly_by_bmu = day_2_s0142.transform_to_half_hourly_by_bmu()

    day_1_half_hourly = day_1_half_hourly_by_bmu.transform_to_half_hourly()
    assert len(day_1_half_hourly.df) == 48
    assert day_1_half_hourly["bm_unit_metered_volume_mwh"].sum() == approx(day_1_s0142_bm_mwh_sum)
    day_2_half_hourly = day_2_half_hourly_by_bmu.transform_to_half_hourly()

    day_1_daily = day_1_half_hourly.transform_to_daily()
    assert len(day_1_daily.df) == 1
    assert day_1_daily["bm_unit_metered_volume_mwh"].sum() == approx(day_1_s0142_bm_mwh_sum)
    assert day_1_daily["settlement_period_count"].unique() == 48
    day_2_daily = day_2_half_hourly.transform_to_daily()

    monthly = MeteringDataDaily.aggregate_to_monthly([day_1_daily.df, day_2_daily.df])
    assert len(monthly.df) == 1
    assert monthly["bm_unit_metered_volume_mwh"].sum() == approx(day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum)
    assert monthly["day_count"].unique() == 2

    yearly = MeteringDataMonthly.aggregate_to_yearly([monthly.df])
    assert len(yearly.df) == 1
    assert yearly["bm_unit_metered_volume_mwh"].sum() == approx(day_1_s0142_bm_mwh_sum + day_2_s0142_bm_mwh_sum)
    assert yearly["month_count"].unique() == 1


def test_transform_daily_to_monthly_assert_single_month() -> None:
    with pytest.raises(AssertionError, match="span multiple"):
        MeteringDataDaily.aggregate_to_monthly(
            [
                pd.DataFrame({"value": range(48)}, index=pd.date_range(f"2025-{m:02d}-11", periods=48, freq="30min"))
                for m in [3, 4]
            ]
        )


def test_transform_monthly_to_yearly_assert_single_year() -> None:
    with pytest.raises(AssertionError, match="span multiple"):
        MeteringDataMonthly.aggregate_to_yearly(
            [
                pd.DataFrame({"value": range(30)}, index=pd.date_range(f"{y}-03-01", periods=30, freq="D"))
                for y in [2025, 2026]
            ]
        )


def test_transform_assert_no_duplicate_timestamps() -> None:
    with pytest.raises(AssertionError, match="duplicate"):
        MeteringDataMonthly.aggregate_to_yearly(
            [pd.DataFrame({"value": range(30)}, index=pd.date_range("2025-03-01", periods=30, freq="D"))] * 2
        )
