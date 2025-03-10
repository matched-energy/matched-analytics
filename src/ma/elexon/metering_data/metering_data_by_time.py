# TODO - rename
import copy
from typing import Callable, Dict, List, Union

import pandas as pd
import pandera as pa

from ma.utils.enums import TemporalGranularity
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE

MeteringDataHalfHourlyType = pd.DataFrame
MeteringDataDailyType = pd.DataFrame
MeteringDataMonthlyType = pd.DataFrame
MeteringDataYearlyType = pd.DataFrame


class MeteringDataHalfHourly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = {
        'settlement_datetime'                           :CS(check=pa.Index(DTE(dayfirst=False))),
        'period_bm_unit_balancing_services_volume'      :CS(check=pa.Column(float)),
        'period_information_imbalance_volume'           :CS(check=pa.Column(float)),
        'period_expected_metered_volume'                :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_mwh'                    :CS(check=pa.Column(float)),
        'bm_unit_applicable_balancing_services_volume'  :CS(check=pa.Column(float)),
        'period_supplier_bm_unit_delivered_volume'      :CS(check=pa.Column(float)),
        'period_supplier_bm_unit_non_bm_absvd_volume'   :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_+ve_mwh'                :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_-ve_mwh'                :CS(check=pa.Column(float)),
        'bmu_count'                                     :CS(check=pa.Column(int)),
    }
    # fmt: on


class MeteringDataDaily(MeteringDataHalfHourly):
    schema: Dict[str, CS] = {
        **copy.deepcopy(MeteringDataHalfHourly.schema),
        "settlement_period_count": CS(check=pa.Column(int)),
    }


class MeteringDataMonthly(MeteringDataHalfHourly):
    schema: Dict[str, CS] = {
        **copy.deepcopy(MeteringDataHalfHourly.schema),
        "day_count": CS(check=pa.Column(int)),
    }


class MeteringDataYearly(MeteringDataHalfHourly):
    schema: Dict[str, CS] = {
        **copy.deepcopy(MeteringDataHalfHourly.schema),
        "month_count": CS(check=pa.Column(int)),
    }


def transform_to_daily(
    metering_data_half_hourly: MeteringDataHalfHourlyType,
) -> MeteringDataDailyType:
    """Rollup a single, half-hourly dataframe to a daily dataframe"""
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
                "bmu_count",
            ]
        ]
        .sum()
        .to_frame()
        .T
    )
    daily_total["settlement_period_count"] = len(metering_data_half_hourly)
    daily_total.index = pd.Index([days[0]]).to_timestamp()
    return MeteringDataDaily.from_dataframe(daily_total)


def _transform_to_monthly_or_yearly(
    metering_data_dataframes: list[Union[MeteringDataDailyType, MeteringDataMonthlyType]],
    granularity: TemporalGranularity,
    drop_column: str,
    from_dataframe: Callable,
) -> Union[MeteringDataMonthlyType, MeteringDataYearlyType]:
    """Rollup a list of daily/monthly dataframes to a singly monthly/yearly dataframe"""
    assert metering_data_dataframes, "Input list must not be empty"

    input = pd.concat(metering_data_dataframes)
    assert isinstance(input.index, pd.DatetimeIndex)  # appease mypy
    input[granularity.noun] = input.index.to_period(granularity.pandas_period)

    output = input.groupby(granularity.noun).sum().sort_values(by=granularity.noun)
    output.drop(drop_column, axis=1, inplace=True)
    output[f"{granularity.preceeding.noun}_count"] = len(metering_data_dataframes)

    output.index = output.index.to_timestamp()  # type: ignore

    return from_dataframe(output)


def transform_to_monthly(
    metering_data_dataframes: List[MeteringDataDailyType],
) -> MeteringDataMonthlyType:
    """Rollup a list of daily dataframes to a single monthly dataframe"""
    return _transform_to_monthly_or_yearly(
        metering_data_dataframes,
        TemporalGranularity.MONTHLY,
        "settlement_period_count",
        MeteringDataMonthly.from_dataframe,
    )


def transform_to_yearly(
    metering_data_dataframes: List[MeteringDataMonthlyType],
) -> MeteringDataYearlyType:
    """Rollup a list of daily dataframes to a single monthly dataframe"""
    return _transform_to_monthly_or_yearly(
        metering_data_dataframes,
        TemporalGranularity.YEARLY,
        "day_count",
        MeteringDataYearly.from_dataframe,
    )
