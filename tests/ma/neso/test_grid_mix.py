import pandas as pd
from pytest import approx

import data.register
import ma.neso.grid_mix
from ma.utils.enums import TechEnum


def test_load() -> None:
    # Load data using our test function
    historic_gen = ma.neso.grid_mix.load(data.register.NESO_FUEL_CKAN_CSV_SUBSET)

    # Check that data was loaded
    assert len(historic_gen) > 0

    # Check that datetime was properly converted
    assert isinstance(historic_gen["datetime"].iloc[0], pd.Timestamp)

    # Check that all required columns exist
    for tech in TechEnum:
        assert tech.value in historic_gen.columns


def test_filter() -> None:
    historic_gen = ma.neso.grid_mix.load(data.register.NESO_FUEL_CKAN_CSV_SUBSET)

    # Get min and max dates from the data
    min_date = historic_gen["datetime"].min()
    max_date = historic_gen["datetime"].max()
    mid_date = min_date + (max_date - min_date) / 2

    # Test filtering for the entire range
    filtered_all = ma.neso.grid_mix.filter(historic_gen, min_date, max_date)
    assert len(filtered_all) == len(historic_gen)

    # Test filtering for the first half
    filtered_first_half = ma.neso.grid_mix.filter(historic_gen, min_date, mid_date)
    assert len(filtered_first_half) < len(historic_gen)
    assert filtered_first_half["datetime"].max() <= mid_date

    # Test filtering for the second half
    filtered_second_half = ma.neso.grid_mix.filter(historic_gen, mid_date, max_date)
    assert len(filtered_second_half) < len(historic_gen)
    assert filtered_second_half["datetime"].min() >= mid_date

    # Test filtering for a non-existent range
    future_date = max_date + pd.Timedelta(days=365)
    filtered_empty = ma.neso.grid_mix.filter(historic_gen, future_date, future_date + pd.Timedelta(days=1))
    assert len(filtered_empty) == 0


def test_groupby_tech_and_month() -> None:
    historic_gen = ma.neso.grid_mix.load(data.register.NESO_FUEL_CKAN_CSV_SUBSET)

    # Group by tech and month
    grouped = ma.neso.grid_mix.groupby_tech_and_month(historic_gen)

    # Check that the grouping was done correctly
    assert grouped.index.names == ["year", "month"]

    # Check that all tech columns exist in the grouped data
    for tech in TechEnum:
        assert tech.value in grouped.columns

    # Check that the total generation is the same before and after grouping
    total_gen_before = historic_gen["generation"].sum()
    total_gen_after = grouped["generation"].sum()
    assert total_gen_after == approx(total_gen_before / 2)  # Divide by 2 because we're converting to MWh

    # Check that number of rows matches unique year-month combinations
    unique_year_months = (
        historic_gen[["datetime"]]
        .assign(year=historic_gen["datetime"].dt.year, month=historic_gen["datetime"].dt.month)
        .drop_duplicates(["year", "month"])
    )
    assert len(grouped) == len(unique_year_months)


def test_data_specific_values() -> None:
    historic_gen = ma.neso.grid_mix.load(data.register.NESO_FUEL_CKAN_CSV_SUBSET)

    start_date = pd.Timestamp("2009-01-01 00:00:00")
    end_date = pd.Timestamp("2009-01-01 23:30:00")
    filtered_data = ma.neso.grid_mix.filter(historic_gen, start_date, end_date)

    # Verify we have expected data for this time period
    assert len(filtered_data) == 48

    assert filtered_data["gas"].sum() == 531151
    assert filtered_data["coal"].sum() == 824282
    assert filtered_data["nuclear"].sum() == 335251
    assert filtered_data["wind"].sum() == 6039
    assert filtered_data["hydro"].sum() == 14840
    assert filtered_data["imports"].sum() == 86503
    assert filtered_data["biomass"].sum() == 0
    assert filtered_data["other"].sum() == 0
    assert filtered_data["solar"].sum() == 0
    assert filtered_data["storage"].sum() == 18688
