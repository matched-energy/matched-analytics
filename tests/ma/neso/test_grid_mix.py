import pandas as pd
from pytest import approx

import data.register
from ma.utils.enums import SupplyTechEnum
from ma.neso.grid_mix import GridMixRaw


def test_groupby_tech_and_month() -> None:
    historic_gen = GridMixRaw(data.register.NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023).transform_to_grid_mix_processed()
    grouped = historic_gen.transform_to_grid_mix_by_tech_month().df

    # Check that the grouping was done correctly, and all tech columns exist
    assert grouped.index.names == ["month"]

    for tech in SupplyTechEnum:
        assert f"{tech.value}_mwh" in grouped.columns

    expected_gas_sum = 8503211.0  # Sum of gas column subset in Excel, divided by 2 to convert to MWH as load() does
    jan_2024_gas = grouped.at[pd.Timestamp("2023-03-01"), "gas_mwh"]
    assert jan_2024_gas == approx(expected_gas_sum)
