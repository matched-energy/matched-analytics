import pandas as pd
from pytest import approx

import data.register
from ma.matching.match_monthly import make_match_monthly
from ma.ofgem.regos import RegosByTechMonthHolder
from ma.upsampled_supply_hh.consumption import ConsumptionMonthly


def test_match_monthly() -> None:
    supply = RegosByTechMonthHolder(data.register.REGOS_BY_TECH_MONTH_HOLDER)
    consumption = ConsumptionMonthly(data.register.CONSUMPTION_BY_MONTH)
    match = make_match_monthly(consumption, supply).df

    assert len(match) == 12
    assert match["consumption_mwh"].sum() == consumption["consumption_mwh"].sum()
    pd.testing.assert_series_equal(
        match["supply_mwh_total"],
        match.filter(like="supply_").drop(columns=["supply_mwh_total"]).sum(axis=1).rename("supply_mwh_total"),
    )
    pd.testing.assert_series_equal(
        match["current_holder_count"].reset_index(drop=True),
        pd.Series([1.0] * 12),
        check_names=False,
    )
    assert match["matching_score"].mean() == approx(0.672, rel=1e-3)
