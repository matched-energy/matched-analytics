import copy
from pathlib import Path
from typing import Dict

import pandas as pd

from ma.elexon.metering_data.metering_data_by_half_hour_and_bmu import MeteringDataHalfHourlyByBmu
from ma.elexon.S0142.processed_S0142 import ProcessedS0142
from ma.mapper.common import MappingException


def half_hourly_to_monthly_volumes(metering_data_half_hourly: pd.DataFrame) -> pd.DataFrame:
    metering_data_half_hourly = copy.deepcopy(metering_data_half_hourly)
    assert isinstance(metering_data_half_hourly.index, pd.DatetimeIndex)  # appease mypy
    metering_data_half_hourly["settlement_datetime"] = metering_data_half_hourly.index
    metering_data_half_hourly["settlement_month"] = metering_data_half_hourly["settlement_datetime"].dt.month
    metering_data_monthly = (
        metering_data_half_hourly.groupby("settlement_month")
        .agg(
            {
                "settlement_datetime": "first",
                "bm_unit_metered_volume_mwh": "sum",
            }
        )
        .sort_values("settlement_datetime")
    ).set_index("settlement_datetime")
    return metering_data_monthly


def get_bmu_volumes_by_month(
    bsc_lead_party_id: str,
    bm_ids: list,
    S0142_csv_dir: Path,
) -> pd.DataFrame:
    metering_data_half_hourly = pd.concat(
        [
            MeteringDataHalfHourlyByBmu.transform_to_half_hourly(
                ProcessedS0142.transform_to_half_hourly_by_bmu(ProcessedS0142.from_file(f)),
                bm_regex=None,
                bm_ids=bm_ids,
            )
            for f in (S0142_csv_dir / Path(bsc_lead_party_id)).iterdir()
            if f.is_file()
        ]
    ).sort_index()
    metering_data_monthly = half_hourly_to_monthly_volumes(metering_data_half_hourly)
    metering_data_monthly["bm_unit_metered_volume_gwh"] = metering_data_monthly["bm_unit_metered_volume_mwh"] / 1e3
    return metering_data_monthly[["bm_unit_metered_volume_gwh"]]


def get_bmu_volume_stats(monthly_vols: pd.DataFrame, bmus_total_net_capacity: float) -> Dict:
    total_gwh = monthly_vols["bm_unit_metered_volume_gwh"].sum()
    total_mwh = total_gwh * 1e3
    months_count = len(monthly_vols)
    nameplate_mwh = bmus_total_net_capacity * 24 * 365 * months_count / 12
    return dict(
        bmu_total_volume=total_gwh, bmu_capacity_factor=total_mwh / nameplate_mwh, bmu_sample_months=months_count
    )


def validate_matching_bmus(bmus: pd.DataFrame) -> None:
    try:
        assert len(bmus["lead_party_name"].unique()) == 1
        assert len(bmus["lead_party_id"].unique()) == 1
        assert len(bmus["fuel_type"].unique()) == 1
    except AssertionError:
        raise MappingException(
            "Expected one lead party and fuel type but got"
            + ", ".join(
                [
                    str(t)
                    for t in bmus[["lead_party_name", "lead_party_id", "fuel_type"]].itertuples(index=False, name=None)
                ]
            )
        )


def get_matching_bmus_dict(bmus: pd.DataFrame) -> dict:
    return dict(
        bmus=[
            dict(
                bmu_unit=bmu["elexon_bm_unit"],
                bmu_demand_capacity=bmu["demand_capacity"],
                bmu_generation_capacity=bmu["generation_capacity"],
                bmu_production_or_consumption_flag=bmu["production_or_consumption_flag"],
                bmu_transmission_loss_factor=bmu["transmission_loss_factor"],
            )
            for i, bmu in bmus.iterrows()
        ],
        bmus_total_demand_capacity=bmus["demand_capacity"].sum(),
        bmus_total_generation_capacity=bmus["generation_capacity"].sum(),
        bmu_lead_party_name=bmus.iloc[0]["lead_party_name"],
        bmu_lead_party_id=bmus.iloc[0]["lead_party_id"],
        bmu_fuel_type=bmus.iloc[0]["fuel_type"],
        lead_party_name_intersection_count=bmus.iloc[0]["lead_party_name_intersection_count"],
        lead_party_name_contiguous_words=bmus.iloc[0]["lead_party_name_contiguous_words"],
    )
