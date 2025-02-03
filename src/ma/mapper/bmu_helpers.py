import copy
from pathlib import Path
from typing import Dict

import pandas as pd

import ma.elexon.S0142.process_csv
from ma.mapper.common import MappingException


def half_hourly_to_monthly_volumes(half_hourly_volumes: pd.DataFrame) -> pd.DataFrame:
    half_hourly_volumes = copy.deepcopy(half_hourly_volumes)
    half_hourly_volumes["Settlement Month"] = half_hourly_volumes["Settlement Date"].dt.month
    monthly_volumes = (
        half_hourly_volumes.groupby("Settlement Month")
        .agg(
            {
                "Settlement Date": "first",
                "BM Unit Metered Volume": "sum",
            }
        )
        .sort_values("Settlement Date")
    ).set_index("Settlement Date")
    return monthly_volumes


def get_bmu_volumes_by_month(
    bsc_lead_party_id: str,
    bm_ids: list,
    S0142_csv_dir: Path,
) -> pd.DataFrame:
    half_hourly_vols = ma.elexon.S0142.process_csv.process_directory(
        input_dir=S0142_csv_dir / Path(bsc_lead_party_id),
        bsc_lead_party_id=bsc_lead_party_id,
        bm_regex=None,
        bm_ids=bm_ids,
        aggregate_bms=True,
        output_path=None,
    )

    monthly_vols = half_hourly_to_monthly_volumes(half_hourly_vols)
    monthly_vols["BM Unit Metered Volume GWh"] = monthly_vols["BM Unit Metered Volume"] / 1e3
    return monthly_vols[["BM Unit Metered Volume GWh"]]


def get_bmu_volume_stats(monthly_vols: pd.DataFrame, bmus_total_net_capacity: float) -> Dict:
    total_gwh = monthly_vols["BM Unit Metered Volume GWh"].sum()
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
