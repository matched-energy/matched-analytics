import pandas as pd

from ma.utils.enums import TemporalGranularity


def rollup_bmus(metering_data_half_hourly: pd.DataFrame) -> pd.DataFrame:
    grouped = metering_data_half_hourly.groupby("settlement_datetime").sum(numeric_only=True)
    grouped["bm_unit_id"] = ",".join(metering_data_half_hourly["bm_unit_id"].unique())
    return grouped


def rollup_to_daily(metering_data_half_hourly: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(metering_data_half_hourly.index, pd.DatetimeIndex)  # appease mypy
    days = metering_data_half_hourly.index.to_period("D")
    assert len(days.unique()) == 1, "Data should not span days"

    daily_total = (
        metering_data_half_hourly[
            [
                "period_bm_unit_balancing_services_volume",
                "period_information_imbalance_volume",
                "period_expected_metered_volume",
                "bm_unit_metered_volume_mwh",
                "bm_unit_applicable_balancing_services_volume",
                "period_supplier_bm_unit_delivered_volume",
                "period_supplier_bm_unit_non_bm_absvd_volume",
                "bm_unit_metered_volume_+ve_mwh",
                "bm_unit_metered_volume_-ve_mwh",
            ]
        ]
        .sum()
        .to_frame()
        .T
    )
    daily_total["settlement_period_count"] = len(metering_data_half_hourly)
    daily_total.index = pd.Index([days[0]]).to_timestamp()
    return daily_total


def rollup_from_daily(metering_data_dataframes: list[pd.DataFrame], granularity: TemporalGranularity) -> pd.DataFrame:
    assert metering_data_dataframes, "Input list must not be empty"

    daily = pd.concat(metering_data_dataframes)
    assert isinstance(daily.index, pd.DatetimeIndex)  # appease mypy
    daily[granularity.noun] = daily.index.to_period(granularity.pandas_period)

    monthly = daily.groupby(granularity.noun).sum().sort_values(by=granularity.noun)
    monthly[f"{granularity.preceeding.noun}_count"] = len(metering_data_dataframes)

    monthly.index = monthly.index.to_timestamp()  # type: ignore
    return monthly
    return monthly
