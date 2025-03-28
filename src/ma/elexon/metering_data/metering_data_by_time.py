from __future__ import annotations

from typing import Any, Dict, List, Sequence, Type

import pandas as pd
import pandera as pa

from ma.retailer.consumption import ConsumptionHalfHourly, ConsumptionMonthly
from ma.utils.enums import TemporalGranularity
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE


class MeteringDataHalfHourly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = {
        'settlement_datetime'                           :CS(check=pa.Index(DTE(dayfirst=False))),
        'period_bm_unit_balancing_services_volume'      :CS(check=pa.Column(float)),
        'period_information_imbalance_volume'           :CS(check=pa.Column(float)),
        'period_expected_metered_volume'                :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_mwh'                    :CS(check=pa.Column(float)),
        'bm_unit_applicable_balancing_services_volume'  :CS(check=pa.Column(float)),
        'period_retailer_bm_unit_delivered_volume'      :CS(check=pa.Column(float)),
        'period_retailer_bm_unit_non_bm_absvd_volume'   :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_+ve_mwh'                :CS(check=pa.Column(float)),
        'bm_unit_metered_volume_-ve_mwh'                :CS(check=pa.Column(float)),
        'bmu_count'                                     :CS(check=pa.Column(int)),
    }
    # fmt: on
    from_file_skiprows = 1

    def transform_to_consumption_half_hourly(self) -> ConsumptionHalfHourly:
        df = self.df
        consumption_half_hourly = pd.DataFrame(
            dict(
                consumption_mwh=-df["bm_unit_metered_volume_mwh"],  # inversion of sign
            ),
            index=df.index,
        )
        return ConsumptionHalfHourly(consumption_half_hourly)

    def transform_to_daily(self) -> MeteringDataDaily:
        """Rollup a single, half-hourly dataframe to a daily dataframe"""
        metering_data_half_hourly = self.df
        assert isinstance(metering_data_half_hourly.index, pd.DatetimeIndex)  # appease mypy
        assert len(metering_data_half_hourly) in (46, 48, 50), (  # robust to daylight savings
            f"Got {len(metering_data_half_hourly)} periods from {metering_data_half_hourly.index.min()} to {metering_data_half_hourly.index.max()}"
        )
        days = metering_data_half_hourly.index.to_period("D")

        daily_total = (
            metering_data_half_hourly[
                [
                    "period_bm_unit_balancing_services_volume",
                    "period_information_imbalance_volume",
                    "period_expected_metered_volume",
                    "bm_unit_metered_volume_mwh",
                    "bm_unit_applicable_balancing_services_volume",
                    "period_retailer_bm_unit_delivered_volume",
                    "period_retailer_bm_unit_non_bm_absvd_volume",
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
        return MeteringDataDaily(daily_total)


class MeteringDataDaily(DataFrameAsset):
    schema: Dict[str, CS] = MeteringDataHalfHourly.schema_copy() | {"settlement_period_count": CS(check=pa.Column(int))}
    from_file_skiprows = 1

    @classmethod
    def aggregate_to_monthly(cls, metering_data_list: List[MeteringDataDaily]) -> MeteringDataMonthly:
        """Rollup a list of daily dataframes to a single monthly dataframe.

        All inputs must be in a single month."""
        return _transform_to_monthly_or_yearly(
            metering_data_list, TemporalGranularity.MONTHLY, "settlement_period_count", MeteringDataMonthly
        )


class MeteringDataMonthly(DataFrameAsset):
    schema: Dict[str, CS] = MeteringDataHalfHourly.schema_copy() | {"day_count": CS(check=pa.Column(int))}
    from_file_skiprows = 1

    @classmethod
    def aggregate_to_yearly(
        cls,
        metering_data_list: List[MeteringDataMonthly],
    ) -> MeteringDataYearly:
        """Rollup a list of monthly dataframes to a single yearly dataframe.

        All inputs must be in a single year."""
        return _transform_to_monthly_or_yearly(
            metering_data_list,
            TemporalGranularity.YEARLY,
            "day_count",
            MeteringDataYearly,
        )

    def transform_to_consumption_monthly(self) -> ConsumptionMonthly:
        df = self.df
        consumption_monthly = pd.DataFrame(
            dict(
                consumption_mwh=-df["bm_unit_metered_volume_mwh"],  # inversion of sign
            ),
            index=df.index,
        )
        return ConsumptionMonthly(consumption_monthly)


class MeteringDataYearly(DataFrameAsset):
    schema: Dict[str, CS] = MeteringDataHalfHourly.schema_copy() | {"month_count": CS(check=pa.Column(int))}
    from_file_skiprows = 1


def _check_time_range(dfs: List[pd.DataFrame], granularity: TemporalGranularity) -> None:
    combined_index = pd.concat([df.index.to_series() for df in dfs])

    ### Check time range contained within month / year
    if granularity == TemporalGranularity.MONTHLY:
        unique_values = combined_index.dt.month.unique()
        assert len(unique_values) == 1, f"Dataframes span multiple months: {unique_values}"
    elif granularity == TemporalGranularity.YEARLY:
        unique_values = combined_index.dt.year.unique()
        assert len(unique_values) == 1, f"Dataframes span multiple years: {unique_values}"
    else:
        raise ValueError("Expect monthly or yearly granularity")

    ### Check no duplicates
    duplicate_count = combined_index.duplicated().sum()
    assert not duplicate_count, f"{duplicate_count} duplicated timestamps"


def _transform_to_monthly_or_yearly(
    metering_data_list: Sequence[MeteringDataDaily | MeteringDataMonthly],
    granularity: TemporalGranularity,
    drop_column: str,
    output_class: Type,
) -> Any:  # Any so that can return MeteringDataMonthly/Yearly
    """Rollup a list of daily/monthly dataframes to a singly monthly/yearly dataframe"""
    assert metering_data_list, "Input list must not be empty"
    metering_data_dataframes = [df.df for df in metering_data_list]
    _check_time_range(metering_data_dataframes, granularity)

    input = pd.concat(metering_data_dataframes)
    assert isinstance(input.index, pd.DatetimeIndex)  # appease mypy
    input[granularity.noun] = input.index.to_period(granularity.pandas_period)

    output = input.groupby(granularity.noun).sum().sort_values(by=granularity.noun)
    output.drop(drop_column, axis=1, inplace=True)
    output[f"{granularity.preceeding.noun}_count"] = len(metering_data_dataframes)

    output.index = output.index.to_timestamp()  # type: ignore

    return output_class(output)
