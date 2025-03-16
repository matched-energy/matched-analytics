from typing import TypedDict

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from data.register import NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023, REGOS_APR2022_MAR2023_SUBSET
from ma.ofgem.regos import RegosProcessed, RegosRaw
from ma.neso.grid_mix import GridMixProcessed, GridMixRaw
from ma.upsampled_supply_hh.upsampled_supply_hh import (
    UpsampledSupplyHalfHourly,
    _validate_date_ranges,
    upsample_supplier_monthly_supply_to_hh,
)


def get_processed_regos() -> RegosProcessed:
    return RegosRaw(REGOS_APR2022_MAR2023_SUBSET).transform_to_regos_processed()


class UpsamplerIO(TypedDict):
    result: UpsampledSupplyHalfHourly
    start_datetime: pd.Timestamp
    end_datetime: pd.Timestamp
    grid_mix_data: GridMixProcessed
    grid_mix_hh: GridMixProcessed
    supply_by_supplier_data: DataFrame
    trimmed_supply_by_supplier_data: DataFrame
    rego_holder_reference: str


@pytest.fixture
def upsampler_io() -> UpsamplerIO:
    """Create a fixture with test data for upsampled supply tests."""
    start_datetime = pd.Timestamp("2023-02-01 00:00")
    end_datetime = pd.Timestamp("2023-04-01 00:00")
    rego_holder_reference = "Drax Energy Solutions Limited (Supplier)"

    # Load the data
    grid_mix_data = GridMixRaw(NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023).transform_grid_mix_schema()
    regos_processed = get_processed_regos()
    trimmed_regos_processed = RegosProcessed(regos_processed.df.head(26))

    # Get upsampled result
    result = upsample_supplier_monthly_supply_to_hh(
        rego_holder_reference=rego_holder_reference,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        grid_mix_hh=grid_mix_data,
        regos_processed=trimmed_regos_processed,
    )

    # Filter grid mix data for the test period
    grid_mix_hh = grid_mix_data.filter(start_datetime, end_datetime)

    # Assert and validate expected types for type checking
    assert isinstance(result.df.index, pd.DatetimeIndex)
    assert isinstance(grid_mix_hh.df.index, pd.DatetimeIndex)

    return {
        "result": result,
        "grid_mix_data": grid_mix_data,
        "grid_mix_hh": grid_mix_hh,
        "supply_by_supplier_data": regos_processed.df,
        "trimmed_supply_by_supplier_data": trimmed_regos_processed.df,
        "rego_holder_reference": rego_holder_reference,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }


def test_upsampled_row_count(upsampler_io: UpsamplerIO) -> None:
    """Test that the upsampled data has the expected number of rows."""
    result = upsampler_io["result"]
    assert len(result.df) == 2832  # 48 half-hours for 31 March and 28 days for February


def test_monthly_aggregation_matches_original(upsampler_io: UpsamplerIO) -> None:
    """Verify that the half-hourly volumes, when aggregated by month,
    match the original monthly volumes.
    """
    result = upsampler_io["result"]

    feb_start = pd.Timestamp("2023-02-01")
    feb_end = pd.Timestamp("2023-03-01")
    mar_start = pd.Timestamp("2023-03-01")
    mar_end = pd.Timestamp("2023-04-01")

    feb_mask = (result.df.index >= feb_start) & (result.df.index < feb_end)
    feb_2023_total = result.df[feb_mask]["supply_mwh"].sum()
    mar_mask = (result.df.index >= mar_start) & (result.df.index < mar_end)
    mar_2023_total = result.df[mar_mask]["supply_mwh"].sum()

    # Expected values, calculated in spreadsheet
    drax_feb_biomass = 641467
    drax_mar_biomass = 650422

    assert feb_2023_total == drax_feb_biomass
    assert mar_2023_total == drax_mar_biomass


def test_march_biomass_total(upsampler_io: UpsamplerIO) -> None:
    """Verify total sum for March - the total upsampled generation
    should match supplier's monthly total for biomass.
    """
    result = upsampler_io["result"]

    mar_start = pd.Timestamp("2023-03-01")
    mar_end = pd.Timestamp("2023-04-01")

    supplier_biomass_total_mwh = 650422.0  # Drax Energy's biomass generation for March 2023

    # Filter data for March biomass using timestamp comparison
    march_mask = (result.df.index >= mar_start) & (result.df.index < mar_end)
    march_biomass_results = result.df[march_mask & (result.df["tech"] == "biomass")]["supply_mwh"]

    # Verify total sum matches expected value
    assert march_biomass_results.sum() == pytest.approx(supplier_biomass_total_mwh, rel=1e-5)


def test_march_biomass_scaling_factor(upsampler_io: UpsamplerIO) -> None:
    """Check the scaling output matches expected scaling for March biomass."""
    result = upsampler_io["result"]
    grid_mix_hh = upsampler_io["grid_mix_hh"]

    mar_start = pd.Timestamp("2023-03-01")
    mar_end = pd.Timestamp("2023-04-01")

    grid_biomass_total_mwh = 1175050.5  # Total biomass in grid for March 2023
    supplier_biomass_total_mwh = 650422.0  # Drax Energy's biomass generation for March 2023
    expected_scaling_factor = supplier_biomass_total_mwh / grid_biomass_total_mwh

    # Filter data for March biomass using timestamp comparison
    march_mask = (result.df.index >= mar_start) & (result.df.index < mar_end)
    march_biomass_results = result.df[march_mask & (result.df["tech"] == "biomass")]
    march_biomass_values = march_biomass_results["supply_mwh"]

    # Get the biomass values from the March grid mix
    march_grid_mask = (grid_mix_hh.df.index >= mar_start) & (grid_mix_hh.df.index < mar_end)
    march_grid_mix = grid_mix_hh.df[march_grid_mask]

    # Test single point (first half-hour)
    grid_first_hh_biomass = march_grid_mix["biomass_mwh"].iloc[0]
    actual_scaling = march_biomass_values.iloc[0] / grid_first_hh_biomass
    assert actual_scaling == pytest.approx(expected_scaling_factor, rel=1e-5)

    # Test all points - verify ratio is constant across all half-hours
    ratio = march_biomass_values / march_grid_mix.loc[march_biomass_values.index, "biomass_mwh"]
    assert np.all(np.isclose(ratio, expected_scaling_factor, rtol=1e-5))


def _get_test_validation_data() -> tuple[GridMixProcessed, RegosProcessed]:
    """Helper function to load test data for date range validation tests."""
    grid_mix_data = GridMixRaw(NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023).transform_grid_mix_schema()
    regos_data = get_processed_regos()
    return grid_mix_data, regos_data


def test_date_range_validation_invalid_start() -> None:
    """Test date range validation when start date is before dataset range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    invalid_start = pd.Timestamp("2000-01-01")
    valid_end = pd.Timestamp("2023-03-31")  # End at the latest possible date for REGOS

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, valid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date is before the earliest date in the grid mix data." in error_msg
    assert "Start date is before the earliest date in the REGOS data." in error_msg


def test_date_range_validation_invalid_end() -> None:
    """Test date range validation when end date is after dataset range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    valid_start = pd.Timestamp("2023-03-01")  # Start at the latest date in both datasets
    invalid_end = pd.Timestamp("2040-01-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(valid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "End date is after the latest date in the grid mix data." in error_msg
    assert "End date is after the latest date in the REGOS data." in error_msg


def test_date_range_validation_both_invalid() -> None:
    """Test date range validation when both start and end dates are invalid."""
    grid_mix_data, regos_data = _get_test_validation_data()

    invalid_start = pd.Timestamp("2000-01-01")
    invalid_end = pd.Timestamp("2040-01-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date is before the earliest date in the grid mix data." in error_msg
    assert "End date is after the latest date in the grid mix data." in error_msg


def test_date_range_validation_no_data_in_range() -> None:
    """Test date range validation when there's no data in the specified range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    # For grid_mix, data only exists in March 2023, so choose a valid month but outside this range
    no_data_start = pd.Timestamp("2022-06-01")  # In REGOS range but before grid_mix
    no_data_end = pd.Timestamp("2025-07-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(no_data_start, no_data_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date is before the earliest date in the grid mix data." in error_msg
    assert "End date is after the latest date in the grid mix data." in error_msg


def test_date_range_validation_missing_half_hourly_points() -> None:
    """Test date range validation when there are missing half-hourly data points."""
    grid_mix_data, regos_data = _get_test_validation_data()
    grid_mix_with_gaps = GridMixProcessed(grid_mix_data.df)

    # Get a valid date range that has data
    valid_start = pd.Timestamp("2023-02-01")
    valid_end = pd.Timestamp("2023-03-01")

    # Filter to this range first
    mask = (grid_mix_with_gaps.df.index >= valid_start) & (grid_mix_with_gaps.df.index < valid_end)

    # Remove some specific timestamps to create gaps (drop every 10th point)
    indices_to_drop = grid_mix_with_gaps.df[mask].index[::10]
    grid_mix_with_gaps = GridMixProcessed(grid_mix_with_gaps.df.drop(indices_to_drop))

    # Now the validation should fail because of missing half-hourly points
    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(valid_start, valid_end, grid_mix_with_gaps, regos_data)

    error_msg = str(excinfo.value)
    assert "Missing half-hourly data points" in error_msg
    assert "Expected" in error_msg and "but found" in error_msg
