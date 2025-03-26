from pathlib import Path

import pandas as pd

from ma.mapper.bmu_helpers import get_bmu_volume_stats, get_bmu_volumes_by_month
from ma.mapper.rego_helpers import get_rego_station_volume_by_month, get_rego_station_volume_stats
from ma.ofgem.regos import RegosProcessed


def appraise_rated_power(generator_profile: dict) -> dict:
    bmus_total_net_capacity = (
        generator_profile["bmus_total_demand_capacity"] + generator_profile["bmus_total_generation_capacity"]
    )
    return dict(
        bmus_total_net_capacity=bmus_total_net_capacity,
        rego_bmu_net_power_ratio=generator_profile["rego_station_dnc_mw"] / bmus_total_net_capacity,
    )


def appraise_energy_volumes(generator_profile: dict, regos: RegosProcessed, S0142_csv_dir: Path) -> dict:
    # REGO volumes
    rego_monthly_vols = get_rego_station_volume_by_month(
        regos,
        generator_profile["rego_station_name"],
    )
    rego_volume_stats = get_rego_station_volume_stats(
        rego_monthly_vols,
        generator_profile["rego_station_dnc_mw"],
    )
    generator_profile.update(rego_volume_stats)

    # BMU volumes
    bmu_monthly_vols = get_bmu_volumes_by_month(
        generator_profile["bmu_lead_party_id"],
        [bmu["bmu_unit"] for bmu in generator_profile["bmus"]],
        S0142_csv_dir,
    )
    bmu_volume_stats = get_bmu_volume_stats(
        bmu_monthly_vols,
        generator_profile["bmus_total_net_capacity"],
    )
    generator_profile.update(bmu_volume_stats)

    # Inner join:
    monthly_vols = pd.merge(
        rego_monthly_vols,
        bmu_monthly_vols,
        left_index=True,
        right_index=True,
    )
    monthly_vols["rego_to_bmu_ratio"] = monthly_vols["rego_mwh"] / monthly_vols["bm_unit_metered_volume_mwh"]
    monthly_vols.index.name = "start_year_month"
    monthly_vols_summary = [
        dict(
            start_year_month=row.index,
            end_year_month=row["end_year_month"],
            bmu_mwh=row["bm_unit_metered_volume_mwh"],
            rego_mwh=row["rego_mwh"],
            rego_to_bmu_ratio=row["rego_to_bmu_ratio"],
        )
        for _, row in monthly_vols.reset_index().iterrows()
    ]
    return dict(
        rego_bmu_volume_ratio_median=monthly_vols["rego_to_bmu_ratio"].median(),
        rego_bmu_volume_ratio_min=monthly_vols["rego_to_bmu_ratio"].min(),
        rego_bmu_volume_ratio_max=monthly_vols["rego_to_bmu_ratio"].max(),
        monthly_energy_volumes=monthly_vols_summary,
    )
