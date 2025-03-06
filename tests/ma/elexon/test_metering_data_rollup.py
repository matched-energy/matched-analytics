import pandas as pd
import pytest

import data.register
from ma.elexon.metering_data import load_file
from ma.elexon.metering_data_rollup import rollup_bmus, rollup_from_daily, rollup_to_daily
from ma.utils.enums import TemporalGranularity


def test_rollup_bmus() -> None:
    metering_data_half_hourly_by_bmu = load_file(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV)
    metering_data_half_hourly = rollup_bmus(metering_data_half_hourly_by_bmu)

    assert len(metering_data_half_hourly) == 48
    assert (
        metering_data_half_hourly_by_bmu["bm_unit_metered_volume_mwh"].sum()
        == metering_data_half_hourly["bm_unit_metered_volume_mwh"].sum()
    )


def test_rollup_to_daily() -> None:
    metering_data_half_hourly = rollup_bmus(load_file(data.register.S0142_20230330_SF_20230425121906_GOLD_CSV))
    metering_data_daily = rollup_to_daily(metering_data_half_hourly)

    assert isinstance(metering_data_daily.index, pd.DatetimeIndex)
    assert len(metering_data_daily) == 1
    assert metering_data_daily["bm_unit_metered_volume_mwh"].iloc[0] == -3417.849
    assert metering_data_daily["settlement_period_count"].iloc[0] == 48


def test_rollup_to_daily_multiple_days() -> None:
    date1 = pd.Timestamp("2025-03-01")
    date2 = pd.Timestamp("2025-03-02")
    times = pd.date_range(date1, periods=24, freq="30min").append(pd.date_range(date2, periods=24, freq="30min"))
    data = pd.DataFrame({"value": range(48)}, index=times)

    with pytest.raises(AssertionError, match="Data should not span days"):
        rollup_to_daily(data)


def test_rollup_from_daily_monthly() -> None:
    metering_data_daily_dataframes = [
        rollup_to_daily(rollup_bmus(load_file(half_hourly)))
        for half_hourly in [
            data.register.S0142_20230330_SF_20230425121906_GOLD_CSV,
            data.register.S0142_20230331_SF_20230426191253_GOLD_CSV,
        ]
    ]
    metering_data_monthly = rollup_from_daily(metering_data_daily_dataframes, TemporalGranularity.MONTHLY)

    assert len(metering_data_monthly) == 1
    assert metering_data_monthly.loc["2023-03-01", "settlement_period_count"] == 96
    assert metering_data_monthly.loc["2023-03-01", "day_count"] == 2
    assert metering_data_monthly.loc["2023-03-01", "bm_unit_metered_volume_mwh"] == -6945.599


def test_rollup_from_daily_yearly() -> None:
    metering_data_daily_dataframes = [
        rollup_to_daily(rollup_bmus(load_file(half_hourly)))
        for half_hourly in [
            data.register.S0142_20230330_SF_20230425121906_GOLD_CSV,
            data.register.S0142_20230331_SF_20230426191253_GOLD_CSV,
        ]
    ]
    metering_data_monthly = rollup_from_daily(metering_data_daily_dataframes, TemporalGranularity.MONTHLY)
    metering_data_yearly = rollup_from_daily([metering_data_monthly], TemporalGranularity.YEARLY)

    assert len(metering_data_yearly) == 1
    assert metering_data_yearly.loc["2023-01-01", "settlement_period_count"] == 96
    assert metering_data_yearly.loc["2023-01-01", "day_count"] == 2
    assert metering_data_yearly.loc["2023-01-01", "month_count"] == 1
    assert metering_data_monthly.loc["2023-03-01", "bm_unit_metered_volume_mwh"] == -6945.599


def test_rollup_from_daily_empty_list() -> None:
    with pytest.raises(AssertionError, match="Input list must not be empty"):
        rollup_from_daily([], TemporalGranularity.MONTHLY)
        rollup_from_daily([], TemporalGranularity.MONTHLY)
