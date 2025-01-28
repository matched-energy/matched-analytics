import copy
import json
from pathlib import Path
from typing import Tuple

import m_elexon.S0142.process_csv
import pandas as pd

from m_mapper.common import MappingException


def load_bmrs_bmus(bmrs_bm_units_path: Path) -> pd.DataFrame:
    """Load BMUS as downloaded from:
    https://bmrs.elexon.co.uk/api-documentation/endpoint/reference/bmunits/all
    """
    with open(bmrs_bm_units_path, "r") as file:
        json_list = json.load(file)
    bmrs_bmus = pd.DataFrame(json_list)
    bmrs_bmus["generationCapacity"] = bmrs_bmus["generationCapacity"].astype(float)
    bmrs_bmus["demandCapacity"] = bmrs_bmus["demandCapacity"].astype(float)
    return bmrs_bmus


def half_hourly_to_monthly_volumes(half_hourly_volumes: pd.DataFrame) -> pd.DataFrame:
    cp_half_hourly_volumes = copy.deepcopy(half_hourly_volumes)
    cp_half_hourly_volumes["Settlement Month"] = half_hourly_volumes["Settlement Date"].dt.month
    monthly_volumes = (
        cp_half_hourly_volumes.groupby("Settlement Month")
        .agg(
            {
                "Settlement Date": "first",
                "BM Unit Metered Volume": "sum",
            }
        )
        .sort_values("Settlement Date")
    ).set_index("Settlement Date")
    monthly_volumes["BM Unit Metered Volume"] /= 1e3  # TODO - standardise units
    return monthly_volumes


def get_monthly_volumes(
    bsc_lead_party_id: str, bm_ids: list, bmus_total_net_capacity: float
) -> Tuple[dict, pd.DataFrame]:
    try:
        volumes_df = m_elexon.S0142.process_csv.process_directory(
            input_dir=Path("/Users/jjk/data/2024-12-12-CP2023-all-bscs-s0142/") / Path(bsc_lead_party_id),
            bsc_lead_party_id=bsc_lead_party_id,
            bm_regex=None,
            bm_ids=bm_ids,
            group_bms=True,
            output_path=None,
        )
        total_volume = volumes_df["BM Unit Metered Volume"].sum()
        return (
            dict(
                bmu_total_volume=total_volume,
                bmu_capacity_factor=total_volume / (bmus_total_net_capacity * 24 * 365),
                bmu_sampling_months=12,  # TODO: test for this!
            ),
            half_hourly_to_monthly_volumes(volumes_df),
        )
    except Exception as e:
        raise MappingException(f"Failed to extract bm volumes by month {e}")


def validate_matching_bmus(bmus: pd.DataFrame) -> None:
    try:
        assert len(bmus["leadPartyName"].unique()) == 1
        assert len(bmus["leadPartyId"].unique()) == 1
        assert len(bmus["fuelType"].unique()) == 1
    except AssertionError:
        raise MappingException(
            "Expected one lead party and fuel type but got"
            + ", ".join(
                [str(t) for t in bmus[["leadPartyName", "leadPartyId", "fuelType"]].itertuples(index=False, name=None)]
            )
        )


def get_matching_bmus_dict(bmus: pd.DataFrame) -> dict:
    return dict(
        bmus=[
            dict(
                bmu_unit=bmu["elexonBmUnit"],  # TODO --> bmu_id
                bmu_demand_capacity=bmu["demandCapacity"],
                bmu_generation_capacity=bmu["generationCapacity"],
                bmu_production_or_consumption_flag=bmu["productionOrConsumptionFlag"],
                bmu_transmission_loss_factor=bmu["transmissionLossFactor"],
            )
            for i, bmu in bmus.iterrows()
        ],
        bmus_total_demand_capacity=bmus["demandCapacity"].sum(),
        bmus_total_generation_capacity=bmus["generationCapacity"].sum(),
        bmu_lead_party_name=bmus.iloc[0]["leadPartyName"],
        bmu_lead_party_id=bmus.iloc[0]["leadPartyId"],
        bmu_fuel_type=bmus.iloc[0]["fuelType"],
        lead_party_name_intersection_count=bmus.iloc[0]["leadPartyName_intersection_count"],
        lead_party_name_contiguous_words=bmus.iloc[0]["leadPartyName_contiguous_words"],
    )
