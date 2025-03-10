# TODO - rename
import copy
from typing import Dict, Union

import pandas as pd
import pandera as pa

from ma.utils.enums import TemporalGranularity
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE

#### TODO
# DONE remove unnecessary columns from meteringdatahalfhourly
# - move transforms to dedicated module
# asset_half_hourly_by_bmu
# asset_half_hourly
# ...
# transforms
# - finish test_transform
# - add index
# - commit _new_ code
# - integrate with dagster
# - migrate matched-analytics to new code
# - delete old code

MeteringDataHalfHourlyType = pd.DataFrame


class MeteringDataHalfHourly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = { 
        'settlement_datetime'                           :CS(check=pa.Column(DTE(dayfirst=False))),
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


MeteringDataDailyType = pd.DataFrame
MeteringDataDaily = MeteringDataHalfHourly
MeteringDataMonthly = MeteringDataHalfHourly
MeteringDataMonthlyType = pd.DataFrame
MeteringDataYearly = MeteringDataHalfHourly
MeteringDataYearlyType = pd.DataFrame


def transform_to_daily(
    metering_data_half_hourly: MeteringDataHalfHourlyType,
) -> MeteringDataDailyType:
    """Rollup a single, half-hourly dataframe to a daily dataframe"""
    # TODO assert isinstance(metering_data_half_hourly.index, pd.DatetimeIndex)  # appease mypy
    metering_data_half_hourly = copy.deepcopy(metering_data_half_hourly)  # TODO delete
    metering_data_half_hourly.set_index("settlement_datetime", drop=True, inplace=True)  # TODO delete
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
    return daily_total


def transform_from_daily(
    metering_data_dataframes: list[Union[MeteringDataDailyType, MeteringDataMonthlyType]],
    granularity: TemporalGranularity,
) -> Union[MeteringDataMonthlyType, MeteringDataYearlyType]:
    """Rollup a list of daily/monthly dataframes to a singly monthly/yearly dataframe"""
    assert metering_data_dataframes, "Input list must not be empty"

    daily = pd.concat(metering_data_dataframes)
    assert isinstance(daily.index, pd.DatetimeIndex)  # appease mypy
    daily[granularity.noun] = daily.index.to_period(granularity.pandas_period)

    monthly = daily.groupby(granularity.noun).sum().sort_values(by=granularity.noun)
    monthly[f"{granularity.preceeding.noun}_count"] = len(metering_data_dataframes)

    monthly.index = monthly.index.to_timestamp()  # type: ignore
    return monthly
