import copy
from typing import Optional, Set, Tuple

import numpy as np
import pandas as pd

from ma.mapper.common import MappingException


################################################################################
# HELPER FUNCTIONS
################################################################################
def words(name: str) -> list:
    return [] if name is None else [word.strip("()") for word in name.lower().split()]


def contiguous_words(l_name: str, r_name: str) -> int:
    count = 0
    for left, right in zip(words(l_name), words(r_name)):
        if left == right:
            count += 1
        else:
            break
    return count


def intersection(series: pd.Series, value: str, ignore: Optional[Set] = None) -> Tuple[pd.Series, pd.Series]:
    if ignore is None:
        ignore = set([])
    intersection_count = series.apply(
        lambda x: (
            0
            if x is None
            else len((set([word.strip("()") for word in x.lower().split()]) & set(words(value))) - ignore)
        )
    )
    max_count_filter = (intersection_count > 0) & (intersection_count == max(intersection_count))
    return intersection_count, max_count_filter


################################################################################
# FILTERS
################################################################################
def filter_on_name_contiguous(station_profile: dict, bmus: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    lead_party_count = bmus["lead_party_name"].apply(
        lambda x: contiguous_words(station_profile["rego_station_name"], x)
    )
    bmu_count = bmus["bm_unit_name"].apply(lambda x: contiguous_words(station_profile["rego_station_name"], x))
    # max_count = pd.Series(map(max, lead_party_count, bmu_count))
    max_count = pd.Series([max(x, y) for x, y in zip(lead_party_count, bmu_count)])
    max_count_filter = (max_count > 0) & (max_count == max(max_count))
    return max_count, max_count_filter


def filter_on_name_intersection(station_profile: dict, bmus: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    lead_party_count, _ = intersection(
        bmus["lead_party_name"],
        station_profile["rego_station_name"],
        ignore=set(["wind", "farm", "windfarm", "limited", "ltd"]),
    )
    bmu_intersection_count, _ = intersection(
        bmus["bm_unit_name"],
        station_profile["rego_station_name"],
        ignore=set(["wind", "farm", "windfarm", "limited", "ltd"]),
    )
    max_count = pd.Series([max(x, y) for x, y in zip(lead_party_count, bmu_intersection_count)])
    max_count_filter = (max_count > 0) & (max_count == max(max_count))
    return max_count, max_count_filter


def filter_on_fuel_type(station_profile: dict, bmus: pd.DataFrame) -> pd.Series:
    _, filter = intersection(
        bmus["fuel_type"],
        station_profile["rego_station_technology"],
    )
    return filter


def filter_on_generation_capacity(station_profile: dict, bmus: pd.DataFrame) -> pd.Series:
    return (station_profile["rego_station_dnc_mw"] / 10 < bmus["generation_capacity"]) & (
        bmus["generation_capacity"] < station_profile["rego_station_dnc_mw"] * 2
    )


################################################################################
# APPLY FILTERS
################################################################################
def define_bmu_match_features_and_filters(station_profile: dict, bmus: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    features = pd.DataFrame(index=bmus.index)
    filters = []

    filter = filter_on_generation_capacity(station_profile, bmus)
    filters.append(filter)

    filter = filter_on_fuel_type(station_profile, bmus)
    filters.append(filter)

    feature, filter = filter_on_name_intersection(station_profile, bmus)
    features["lead_party_name_intersection_count"] = feature
    filters.append(filter)

    feature, filter = filter_on_name_contiguous(station_profile, bmus)
    features["lead_party_name_contiguous_words"] = feature
    filters.append(filter)

    return features, filters


def apply_bmu_match_filters(bmus: pd.DataFrame, filters: list) -> pd.DataFrame:
    filtered_bmus = bmus.loc[np.logical_and.reduce(filters)]

    try:
        assert len(filtered_bmus) > 0
    except AssertionError:
        warning = "No matching BMUs found"
        raise MappingException(warning)

    return filtered_bmus


def get_matching_bmus(generator_profile: dict, bmus: pd.DataFrame, expected_mapping: dict) -> pd.DataFrame:
    # Determine if should rate expected BMUs or search over all BMUs
    expected_overrides = expected_mapping["bmu_ids"] and expected_mapping.get("override")
    bmus_to_search = (
        bmus[bmus["elexon_bm_unit"].isin(expected_mapping["bmu_ids"])] if expected_overrides else copy.deepcopy(bmus)
    )

    # Define matching features and filters
    bmu_match_features, bmu_match_filters = define_bmu_match_features_and_filters(generator_profile, bmus_to_search)
    bmus_to_search = bmus_to_search.join(bmu_match_features, how="outer")

    # Return expected / filtered BMUs with matching
    return bmus_to_search if expected_overrides else apply_bmu_match_filters(bmus_to_search, bmu_match_filters)
