from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from ma.utils.io import from_yaml_file


def compare_to_expected(mapping_scores: pd.DataFrame, expected_mappings_file: Path) -> pd.DataFrame:
    """Left join"""
    expected_mappings = from_yaml_file(expected_mappings_file)
    comparisons = []
    for _, row in mapping_scores.iterrows():
        rego_station_name = row["rego_name"]
        bmu_ids = [id.strip() for id in row["bmu_ids"].split(",")]
        expected_bmu_ids = expected_mappings.get(rego_station_name, {}).get("bmu_ids", [])
        comparisons.append(
            dict(
                rego_station_name=rego_station_name,
                bmu_ids=", ".join(bmu_ids),
                expected_bmu_ids=", ".join(expected_bmu_ids),
                verified=(None if not expected_bmu_ids else (set(bmu_ids) == set(expected_bmu_ids))),
            )
        )
    return pd.DataFrame(comparisons)


def get_p_values_for_metric(
    metric_name: str,
    value: Any,
    p_value_ranges: List[Dict],
) -> dict:
    p_val_dict = {f"s: {metric_name}": value}
    for s in p_value_ranges:
        p_val_dict[f"s: p({s['lower']}, {s['upper']})"] = s["p"] if (s["lower"] <= value < s["upper"]) else 1
    return p_val_dict


def get_p_values_for_all_metrics(generator_profile: dict) -> List:
    return [
        get_p_values_for_metric(
            metric_name="words_intersection",
            value=generator_profile.get("lead_party_name_intersection_count", 0),
            p_value_ranges=[
                dict(lower=1, upper=3, p=0.5),
                dict(lower=3, upper=float("inf"), p=0.1),
            ],
        ),
        get_p_values_for_metric(
            metric_name="contiguous_words",
            value=generator_profile.get("lead_party_name_contiguous_words", 0),
            p_value_ranges=[dict(lower=3, upper=float("inf"), p=0.1)],
        ),
        get_p_values_for_metric(
            metric_name="volume_ratio_p50",
            value=generator_profile.get("rego_bmu_volume_ratio_median", 0),
            p_value_ranges=[
                dict(lower=0.7, upper=1.05, p=0.5),
                dict(lower=0.9, upper=1.05, p=0.1),
            ],
        ),
        get_p_values_for_metric(
            metric_name="volume_ratio_min",
            value=generator_profile.get("rego_bmu_volume_ratio_min", 0),
            p_value_ranges=[dict(lower=0.1, upper=1.0, p=0.5)],
        ),
        get_p_values_for_metric(
            metric_name="volume_ratio_max",
            value=generator_profile.get("rego_bmu_volume_ratio_max", 0),
            p_value_ranges=[dict(lower=0.5, upper=1.1, p=0.5)],
        ),
        get_p_values_for_metric(
            metric_name="power_ratio",
            value=generator_profile.get("rego_bmu_net_power_ratio", 0),
            p_value_ranges=[
                dict(lower=0.5, upper=2, p=0.5),
                dict(lower=0.95, upper=1.05, p=0.1),
            ],
        ),
    ]


def score_mapping(generator_profile: dict) -> pd.DataFrame:
    p_val_list = get_p_values_for_all_metrics(generator_profile)
    p_val_names = [name for p_val in p_val_list for name in p_val.keys()]
    p_val_values = [value for p_val in p_val_list for value in p_val.values()]
    p_vals_df = pd.DataFrame([p_val_values], columns=p_val_names)
    p_vals_df["s: p_overall"] = p_vals_df[[col for col in p_vals_df.columns if "p(" in col]].prod(axis=1)
    p_vals_df["score"] = label_score(p_vals_df["s: p_overall"].iloc[0])
    return p_vals_df.reset_index(drop=True)


def label_score(p: float) -> str:
    if p <= 1e-3:
        return "near certain"
    if p <= 1e-2:
        return "very probable"
    if p <= 0.125:
        return "likely"
    if p <= 0.25:
        return "possible"
    return "unknown"


def summarise_profile(generator_profile: dict) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rego_name": generator_profile.get("rego_station_name"),
                "accreditation_number": generator_profile.get("rego_accreditation_number"),
                "lead_party_name": generator_profile.get("bmu_lead_party_name"),
                "lead_party_id": generator_profile.get("bmu_lead_party_id"),
                "rego_technology": generator_profile.get("rego_station_technology"),
                "bmu_fuel_type": generator_profile.get("bmu_fuel_type"),
                "bmu_ids": ", ".join([bmu["bmu_unit"] for bmu in generator_profile.get("bmus", [])]),
                "rego_mw": generator_profile.get("rego_station_dnc_mw"),
                "bmu_net_mw": generator_profile.get("bmus_total_net_capacity"),
                "rego_bmu_power_ratio": generator_profile.get("rego_bmu_net_power_ratio"),
                "rego_gwh": generator_profile.get("rego_total_volume"),
                "bmu_gwh": generator_profile.get("bmu_total_volume"),
                "rego_bmu_monthly_volume_ratio_min": generator_profile.get("rego_bmu_volume_ratio_min"),
                "rego_bmu_monthly_volume_ratio_median": generator_profile.get("rego_bmu_volume_ratio_median"),
                "rego_bmu_monthly_volume_ratio_max": generator_profile.get("rego_bmu_volume_ratio_max"),
                "rego_capacity_factor": generator_profile.get("rego_capacity_factor"),
                "bmu_capacity_factor": generator_profile.get("bmu_capacity_factor"),
                "rego_sample_months": generator_profile.get("rego_sample_months"),
                "bmu_sample_months": generator_profile.get("bmu_sample_months"),
            }
        ]
    )


def abbreviate_summary(summary: pd.DataFrame) -> pd.DataFrame:
    return summary[[col for col in summary.columns if "s:" not in col]]
