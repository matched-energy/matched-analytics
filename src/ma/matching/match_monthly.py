from __future__ import annotations


import pandas as pd

from ma.matching.match_half_hourly import (
    MatchHalfHourly,
    MatchHalfHourlyAnnualised,
    calculate_matching_score,
    calculate_supply_surplus_deficit,
)
from ma.ofgem.regos import RegosByTechMonthHolder
from ma.retailer.consumption import ConsumptionMonthly
from ma.utils.enums import SupplyTechEnum


def make_match_monthly(consumption: ConsumptionMonthly, supply: RegosByTechMonthHolder) -> MatchMonthly:
    supply_df = supply.df
    supply_df["supply_mwh"] = supply_df["rego_mwh"]

    supply_pivoted = supply_df.groupby("month").agg(
        supply_total_mwh=("supply_mwh", "sum"),
        rego_holder_count=("current_holder", "nunique"),
    )
    for tech in SupplyTechEnum.alphabetical_renewables():
        supply_pivoted = supply_pivoted.join(
            pd.DataFrame(
                {
                    f"supply_{tech}_mwh": supply_df[supply_df["tech"] == tech]["supply_mwh"],
                    f"supply_{tech}_station_count": supply_df[supply_df["tech"] == tech]["station_count"],
                }
            )
        ).fillna(0)

    match_df = supply_pivoted.join(consumption.df)

    match_df["supply_surplus_mwh"], match_df["supply_deficit_mwh"] = calculate_supply_surplus_deficit(
        supply=match_df["supply_total_mwh"],
        consumption=match_df["consumption_mwh"],
    )
    match_df["matching_score"] = calculate_matching_score(
        deficit=match_df["supply_deficit_mwh"], consumption=match_df["consumption_mwh"]
    )

    return MatchMonthly(match_df)


class MatchMonthly(MatchHalfHourly):
    def transform_to_match_monthly_annualised(self) -> MatchMonthlyAnnualised:
        return MatchMonthlyAnnualised(self._transform_to_match_annualised())


class MatchMonthlyAnnualised(MatchHalfHourlyAnnualised):
    pass
