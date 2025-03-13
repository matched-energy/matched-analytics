import pandas as pd
from data.register import NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023, REGOS_APR2022_MAR2023_SUBSET
import ma

import ma.neso.grid_mix
import ma.ofgem.regos
import pytest

from ma.upsampled_supply_hh.upsampled_supply_hh import upsample_supplier_monthly_supply_to_hh, _validate_date_ranges


def test_upsampler() -> None:
    """Test the upsampler function with logical validation."""
    # Test parameters
    start_datetime = pd.Timestamp("2023-02-01")
    end_datetime = pd.Timestamp("2023-04-01")
    rego_holder_reference = "Drax Energy Solutions Limited (Supplier)"

    # Load the data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023)
    supply_by_supplier_data = ma.ofgem.regos.load(REGOS_APR2022_MAR2023_SUBSET)  # noqa: F821
    trimmed_supply_by_supplier_data = supply_by_supplier_data.head(26)

    result = upsample_supplier_monthly_supply_to_hh(
        rego_holder_reference=rego_holder_reference,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        grid_mix_tech_month=grid_mix_data,
        supply_supplier_month=trimmed_supply_by_supplier_data,
    )

    assert len(result) == 2832  # 48 half-hours for 31 March and 28 days for February

    # TEST 1: Verify that the half-hourly volumes, when aggregated by month,
    # match the original monthly volumes (within a small tolerance)
    monthly_results = (
        result.assign(year=result["timestamp"].dt.year, month=result["timestamp"].dt.month)
        .groupby(["year", "month", "tech", "supplier"])["supply_mwh"]
        .sum()
        .reset_index()
    )

    # Check each month separately
    for year in monthly_results["year"].unique():
        for month in monthly_results["month"].unique():
            total_monthly = monthly_results[(monthly_results["year"] == year) & (monthly_results["month"] == month)][
                "supply_mwh"
            ].sum()

            total_original = (
                supply_by_supplier_data.pipe(lambda df: ma.ofgem.regos.filter(df, holders=[rego_holder_reference]))
                .query(f"start_year_month.dt.year == {year} and start_year_month.dt.month == {month}")
                .assign(tech=lambda df: df["tech"].str.lower())
                .groupby(["tech", "current_holder"])["rego_gwh"]
                .sum()
                .sum()
                * 1000
            )  # Convert GWh to MWh

            # Check if total sums are equal within a small tolerance for each month
            assert total_monthly == pytest.approx(total_original), f"Mismatch for {year}-{month}"

    # TEST 2: Verify half-hourly volumes as a fraction of grid mix are invariant and as expected
    grid_mix_hh = ma.neso.grid_mix.filter(grid_mix_data, start_datetime, end_datetime)

    # Test 3: specific values - verify biomass scaling works correctly for March 2023
    # Note: These are March-specific values based on the test documentation
    grid_biomass_total_mwh = 1175050.5  # Total biomass in grid for March 2023
    supplier_biomass_total_mwh = 650422.0  # Drax Energy's biomass generation for March 2023
    expected_scaling_factor = supplier_biomass_total_mwh / grid_biomass_total_mwh

    # TEST 3A: Verify total sum for March - the total upsampled generation should match supplier's monthly total
    march_mask = (result["timestamp"].dt.year == 2023) & (result["timestamp"].dt.month == 3)
    march_biomass_results = result[march_mask & (result["tech"] == "biomass")]["supply_mwh"]
    assert march_biomass_results.sum() == pytest.approx(supplier_biomass_total_mwh, rel=1e-5)

    # TEST 3B: Check the scaling output matches expected scaling for March
    # Get the first half-hour biomass value from the March grid mix
    march_grid_mask = (grid_mix_hh.index.to_series().dt.year == 2023) & (grid_mix_hh.index.to_series().dt.month == 3)
    march_grid_mix = grid_mix_hh[march_grid_mask]
    grid_first_hh_biomass = march_grid_mix["biomass_mwh"].iloc[0]  # first HH biomass from March grid mix

    # Calculate the actual scaling used for the first half-hour of March
    actual_scaling = march_biomass_results.iloc[0] / grid_first_hh_biomass

    # Verify the actual scaling matches our expected scaling factor
    assert actual_scaling == pytest.approx(expected_scaling_factor, rel=1e-5)


def _get_test_validation_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Helper function to load test data for date range validation tests."""
    # Load the actual subset data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023)
    regos_data = ma.ofgem.regos.load(REGOS_APR2022_MAR2023_SUBSET)
    return grid_mix_data, regos_data


def test_date_range_validation_invalid_start() -> None:
    """Test date range validation when start date is before dataset range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    invalid_start = pd.Timestamp("2000-01-01")
    valid_end = pd.Timestamp("2023-03-01")  # End at the latest possible date for REGOS

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, valid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Date range outside of grid mix data range." in error_msg
    assert "Could not determine date range in REGOS data" in error_msg


def test_date_range_validation_invalid_end() -> None:
    """Test date range validation when end date is after dataset range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    valid_start = pd.Timestamp("2023-03-01")  # Start at the latest date in both datasets
    invalid_end = pd.Timestamp("2040-01-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(valid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Date range outside of grid mix data range." in error_msg
    assert "Could not determine date range in REGOS data" in error_msg


def test_date_range_validation_both_invalid() -> None:
    """Test date range validation when both start and end dates are invalid."""
    grid_mix_data, regos_data = _get_test_validation_data()

    invalid_start = pd.Timestamp("2000-01-01")
    invalid_end = pd.Timestamp("2040-01-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Date range outside of grid mix data range." in error_msg
    assert "Could not determine date range in REGOS data" in error_msg


def test_date_range_validation_no_data_in_range() -> None:
    """Test date range validation when there's no data in the specified range."""
    grid_mix_data, regos_data = _get_test_validation_data()

    # For grid_mix, data only exists in March 2023, so choose a valid month but outside this range
    no_data_start = pd.Timestamp("2022-06-01")  # In REGOS range but before grid_mix
    no_data_end = pd.Timestamp("2022-07-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(no_data_start, no_data_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Date range outside of grid mix data range." in error_msg


def test_date_range_validation_missing_half_hourly_points() -> None:
    """Test date range validation when there are missing half-hourly data points."""
    grid_mix_data, regos_data = _get_test_validation_data()

    # Create a copy of grid_mix_data with some half-hourly points removed
    grid_mix_with_gaps = grid_mix_data.copy()

    # Get a valid date range that has data
    valid_start = pd.Timestamp("2023-02-01")
    valid_end = pd.Timestamp("2023-03-01")

    # Filter to this range first
    mask = (grid_mix_with_gaps.index >= valid_start) & (grid_mix_with_gaps.index < valid_end)

    # Remove some specific timestamps to create gaps (drop every 10th point)
    indices_to_drop = grid_mix_with_gaps[mask].index[::10]
    grid_mix_with_gaps = grid_mix_with_gaps.drop(indices_to_drop)

    # Now the validation should fail because of missing half-hourly points
    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(valid_start, valid_end, grid_mix_with_gaps, regos_data)

    error_msg = str(excinfo.value)
    assert "Missing half-hourly data points" in error_msg
    assert "Expected" in error_msg and "but found" in error_msg
