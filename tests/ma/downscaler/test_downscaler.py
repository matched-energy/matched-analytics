import pandas as pd
from data.register import NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023, REGOS_APR2022_MAR2023_SUBSET
import ma
from ma.downscaler.downscale_supply_monthly_gen_to_hh import downscale_supply_monthly_gen_to_hh
from ma.downscaler.downscale_supply_monthly_gen_to_hh import _validate_date_ranges
import ma.neso.grid_mix
import ma.ofgem.regos
import pytest


def test_downscaler() -> None:
    """Test the downscaler function with logical validation."""
    # Test parameters
    start_datetime = pd.Timestamp("2023-03-01")
    end_datetime = pd.Timestamp("2023-04-01")

    # Load the data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023)
    gen_by_supplier_data = ma.ofgem.regos.load(REGOS_APR2022_MAR2023_SUBSET)  # noqa: F821
    trimmed_gen_by_supplier_data = gen_by_supplier_data.head(
        3
    )  # Contains three bundles of certs: Drax (biomas and wind) and ACT (wind)

    # Run the downscaler
    result = downscale_supply_monthly_gen_to_hh(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        grid_mix_tech_month=grid_mix_data,
        gen_supplier_month=trimmed_gen_by_supplier_data,
    )

    # 48 hours * 31 days * 3 series (ACT wind, Drax wind, Drax Biomass)
    assert len(result) == 4464

    # Scaling factor explanation:
    # The downscaler distributes total generation of each technology (e.g., biomass) among suppliers
    # based on their proportion of REGOs (certificates) for that technology.

    # REGO certificate counts:
    british_gas_certs = 500000  # British Gas has 500,000 biomass certificates
    drax_certs = 650422  # Drax has 650,422 biomass certificates
    total_certs = british_gas_certs + drax_certs  # Total biomass certificates

    # The proportion of total biomass generation allocated to each supplier should match
    # their proportion of the total certificates:
    # British Gas proportion = british_gas_certs / total_certs
    # Drax proportion = drax_certs / total_certs

    # Therefore, the ratio of British Gas to Drax generation should be:
    expected_ratio = british_gas_certs / drax_certs  # Approximately 0.769

    # Check that the ratio of biomass generation maintains this certificate ratio
    biomass_data = result[result["tech"] == "biomass"]
    timestamps = biomass_data["timestamp"].unique()

    for timestamp in timestamps:
        timestamp_data = biomass_data[biomass_data["timestamp"] == timestamp]
        british_gas_biomass = timestamp_data[timestamp_data["supplier"] == "British Gas Trading Ltd"][
            "generation_mwh"
        ].values[0]
        drax_biomass = timestamp_data[timestamp_data["supplier"] == "Drax Energy Solutions Limited (Supplier)"][
            "generation_mwh"
        ].values[0]

        # For each timestamp, the ratio of generation should match the ratio of certificates
        assert british_gas_biomass / drax_biomass == pytest.approx(expected_ratio)

        # Optional additional check: verify that the total generation is allocated proportionally
        total_biomass_at_timestamp = british_gas_biomass + drax_biomass
        assert british_gas_biomass / total_biomass_at_timestamp == pytest.approx(british_gas_certs / total_certs)
        assert drax_biomass / total_biomass_at_timestamp == pytest.approx(drax_certs / total_certs)


def test_date_range_validation():
    """Test the date range validation functionality with the actual subset data."""
    # Load the actual subset data
    grid_mix_data = ma.neso.grid_mix.load(NESO_FUEL_CKAN_CSV_SUBSET_APR2022_MAR2023)
    regos_data = ma.ofgem.regos.load(REGOS_APR2022_MAR2023_SUBSET)

    # Test 1: Invalid start date (2000-01-01 is before the grid mix dataset)
    invalid_start = pd.Timestamp("2000-01-01")
    valid_end = pd.Timestamp("2023-03-01")  # End at the latest possible date for REGOS

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, valid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date 2000-01-01 00:00:00 is before available grid mix data" in error_msg
    assert "Could not determine date range in REGOS data" in error_msg
    assert "No grid mix data available within the specified date range" in error_msg

    # Test 2: Invalid end date (2040-01-01 is after both datasets)
    valid_start = pd.Timestamp("2023-03-01")  # Start at the latest date in both datasets
    invalid_end = pd.Timestamp("2040-01-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(valid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "End date 2040-01-01 00:00:00 is after available grid mix data" in error_msg
    assert "Could not determine date range in REGOS data" in error_msg

    # Test 3: Both invalid start and end dates
    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(invalid_start, invalid_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date 2000-01-01 00:00:00 is before available grid mix data" in error_msg
    assert "End date 2040-01-01 00:00:00 is after available grid mix data" in error_msg
    assert "Could not determine date range in REGOS data" in error_msg

    # Test 4: No data in range (choose a range where we know there's no data)
    # For grid_mix, data only exists in March 2023, so choose a valid month but outside this range
    no_data_start = pd.Timestamp("2022-06-01")  # In REGOS range but before grid_mix
    no_data_end = pd.Timestamp("2022-07-01")

    with pytest.raises(ValueError) as excinfo:
        _validate_date_ranges(no_data_start, no_data_end, grid_mix_data, regos_data)

    error_msg = str(excinfo.value)
    assert "Start date 2022-06-01 00:00:00 is before available grid mix data" in error_msg
