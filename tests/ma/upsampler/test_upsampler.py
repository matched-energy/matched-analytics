import pandas as pd
from data.register import NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023, REGOS_APR2022_MAR2023_SUBSET
import ma
from ma.upsampler.upsample_supply_monthly_gen_to_hh import upsample_supply_monthly_gen_to_hh
from ma.upsampler.upsample_supply_monthly_gen_to_hh import _validate_date_ranges
import ma.neso.grid_mix
import ma.ofgem.regos
import pytest


def test_upsampler() -> None:
    """Test the upsampler function with logical validation."""
    # Test parameters
    start_datetime = pd.Timestamp("2023-03-01")
    end_datetime = pd.Timestamp("2023-04-01")
    rego_holder_reference = "Drax Energy Solutions Limited (Supplier)"

    # Load the data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023)
    gen_by_supplier_data = ma.ofgem.regos.load(REGOS_APR2022_MAR2023_SUBSET)  # noqa: F821
    trimmed_gen_by_supplier_data = gen_by_supplier_data.head(3)

    # save the HH and the gen_by_supplier_data to csv
    grid_mix_data.to_csv("grid_mix_data.csv", index=False)
    trimmed_gen_by_supplier_data.to_csv("trimmed_gen_by_supplier_data.csv", index=False)

    result = upsample_supply_monthly_gen_to_hh(
        rego_holder_reference=rego_holder_reference,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        grid_mix_tech_month=grid_mix_data,
        gen_supplier_month=trimmed_gen_by_supplier_data,
    )

    # save the result to csv
    result.to_csv("result.csv", index=False)

    # Expect 48 half-hours * 31 days = 1488 rows
    assert len(result) == 1488

    # TEST 1: Verify that the half-hourly volumes, when aggregated by month,
    # match the original monthly volumes (within a small tolerance)
    total_monthly = (
        result.assign(year=result["timestamp"].dt.year, month=result["timestamp"].dt.month)
        .groupby(["year", "month", "tech", "supplier"])["generation_mwh"]
        .sum()
        .sum()
    )

    total_original = (
        gen_by_supplier_data.pipe(lambda df: ma.ofgem.regos.filter(df, holders=[rego_holder_reference]))
        .query(
            f"start_year_month.dt.year == {start_datetime.year} and start_year_month.dt.month == {start_datetime.month}"
        )
        .assign(tech=lambda df: df["tech_category"].str.lower())
        .groupby(["tech", "current_holder"])["rego_gwh"]
        .sum()
        .sum()
        * 1000
    )  # Convert GWh to MWh

    # Check if total sums are equal within a small tolerance
    assert total_monthly == pytest.approx(total_original)

    # TEST 2: Verify half-hourly volumes as a fraction of grid mix are invariant and as expected
    grid_mix_hh = ma.neso.grid_mix.filter(grid_mix_data, start_datetime, end_datetime)

    for tech in result["tech"].unique():
        # Calculate and verify ratio consistency
        supplier_gen = result[result["tech"] == tech].set_index("timestamp")
        ratio_df = supplier_gen.join(grid_mix_hh[f"{tech}_mwh"], how="inner")
        ratio_df["ratio"] = ratio_df["generation_mwh"] / ratio_df[f"{tech}_mwh"]

        # Verify ratio invariance across days (should be nearly constant)
        daily_ratios = ratio_df.groupby(ratio_df.index.date)["ratio"].mean()
        assert daily_ratios.std() < 1e-10, f"Ratio varies across days for tech {tech}"

        # Verify ratio accuracy compared to original data
        filtered_supplier = gen_by_supplier_data.pipe(
            lambda df: ma.ofgem.regos.filter(df, holders=[rego_holder_reference])
        ).query(
            f"start_year_month.dt.year == {start_datetime.year} and start_year_month.dt.month == {start_datetime.month} and tech_category.str.lower() == @tech"
        )

        supplier_total = filtered_supplier["rego_gwh"].sum() * 1000  # GWh to MWh

        # Calculate grid monthly total and verify ratio
        grid_total = grid_mix_hh[f"{tech}_mwh"].sum()
        assert ratio_df["ratio"].mean() == pytest.approx(supplier_total / grid_total, rel=1e-5)

    # Test 4: specific values - verify biomass scaling works correctly
    grid_biomass_total_mwh = 1175050.5  # Total biomass in grid for March 2023
    supplier_biomass_total_mwh = 650422.0  # Drax Energy's biomass generation for March 2023
    expected_scaling_factor = supplier_biomass_total_mwh / grid_biomass_total_mwh

    # TEST 4A: Verify total sum - the total upsampled generation should match supplier's monthly total
    biomass_results = result[result["tech"] == "biomass"]["generation_mwh"]
    assert biomass_results.sum() == pytest.approx(supplier_biomass_total_mwh, rel=1e-5)

    # TEST 4B: Check the scaling output matches expected scaling
    # Get the first half-hour biomass value from the grid mix
    grid_first_hh_biomass = grid_mix_data.iloc[0]["biomass_mwh"]  # first HH biomass from grid mix

    # Calculate the actual scaling used for the first half-hour
    actual_scaling = biomass_results.iloc[0] / grid_first_hh_biomass

    # Verify the actual scaling matches our expected scaling factor
    assert actual_scaling == pytest.approx(expected_scaling_factor, rel=1e-5)


def _get_test_validation_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Helper function to load test data for date range validation tests."""
    # Load the actual subset data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023)
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
    assert "No grid mix data available within the specified date range." in error_msg


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
