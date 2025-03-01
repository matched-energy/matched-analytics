from pytest import approx

import data.register
import ma.neso.grid_mix
from ma.utils.enums import TechEnum


def test_groupby_tech_and_month() -> None:
    historic_gen = ma.neso.grid_mix.load(data.register.NESO_FUEL_CKAN_CSV_SUBSET)
    grouped = ma.neso.grid_mix.groupby_tech_and_month(historic_gen)

    # Check that the grouping was done correctly, and all tech columns exist
    assert grouped.index.names == ["year", "month"]
    for tech in TechEnum:
        assert tech.value in grouped.columns

    expected_gas_sum = 538611.5  # Sum of gas column subset in Excel, divided by 2 to convert to MWH as load() does
    jan_2024_gas = grouped.loc[(2024, 1), "gas"]
    assert jan_2024_gas == approx(expected_gas_sum)
