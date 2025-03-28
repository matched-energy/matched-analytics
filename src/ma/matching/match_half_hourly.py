from __future__ import annotations
from ma.retailer.consumption import ConsumptionHalfHourly
from ma.retailer.supply_hh import UpsampledSupplyHalfHourly
from ma.utils.enums import SupplyTechEnum
from ma.matching.utils import (
    calculate_matching_score,
    calculate_supply_surplus_deficit,
    plot_supply_consumption_matching,
)
from ma.utils.pandas import DataFrameAsset
import pandas as pd

from typing import Dict

import pandera as pa
import plotly.graph_objects as go
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE


def make_match_half_hourly(
    supply: UpsampledSupplyHalfHourly,
    consumption: ConsumptionHalfHourly,
) -> MatchHalfHourly:
    supply_df = supply.df
    consumption_df = consumption.df

    if not supply_df.index.equals(consumption_df.index):
        raise ValueError(
            f"Supply and consumption cover different time periods: {supply_df.index.min()} to {supply_df.index.max()} and {consumption_df.index.min()} to {consumption_df.index.max()}"
        )

    supply_pivoted = supply_df.groupby("timestamp").agg(
        supply_total_mwh=("supply_mwh", "sum"),
        rego_holder_count=("retailer", "nunique"),
    )

    for tech in SupplyTechEnum.alphabetical_renewables():
        tech_supply = supply_df[supply_df["tech"] == tech]
        supply_pivoted = supply_pivoted.join(
            pd.DataFrame(
                {
                    f"supply_{tech}_mwh": tech_supply["supply_mwh"],
                    f"supply_{tech}_station_count": tech_supply["retailer"].nunique(),
                }
            )
        ).fillna(0)

    match_df = supply_pivoted.join(consumption_df, how="inner")

    match_df["supply_surplus_mwh"], match_df["supply_deficit_mwh"] = calculate_supply_surplus_deficit(
        supply=match_df["supply_total_mwh"],
        consumption=match_df["consumption_mwh"],
    )

    match_df["matching_score"] = calculate_matching_score(
        deficit=match_df["supply_deficit_mwh"],
        consumption=match_df["consumption_mwh"],
    )

    return MatchHalfHourly(match_df)


class MatchHalfHourly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        timestamp                   =CS(check=pa.Index(DTE(dayfirst=False))),
        supply_total_mwh            =CS(check=pa.Column(float)),
        rego_holder_count           =CS(check=pa.Column(int)),  
        supply_biomass_mwh          =CS(check=pa.Column(float)), 
        supply_biomass_station_count=CS(check=pa.Column(int)),   
        supply_hydro_mwh            =CS(check=pa.Column(float)),  
        supply_hydro_station_count  =CS(check=pa.Column(int)),   
        supply_other_mwh            =CS(check=pa.Column(float)),
        supply_other_station_count  =CS(check=pa.Column(int)),
        supply_solar_mwh            =CS(check=pa.Column(float)),
        supply_solar_station_count  =CS(check=pa.Column(int)),
        supply_wind_mwh             =CS(check=pa.Column(float)),  
        supply_wind_station_count   =CS(check=pa.Column(int)),
        consumption_mwh             =CS(check=pa.Column(float)),
        supply_surplus_mwh          =CS(check=pa.Column(float)),  
        supply_deficit_mwh          =CS(check=pa.Column(float)),  
        matching_score              =CS(check=pa.Column(float, checks=[pa.Check.greater_than_or_equal_to(0), pa.Check.less_than_or_equal_to(1)])),
    )
    # fmt: on
    from_file_skiprows = 1
    from_file_with_index = True

    def plot(self) -> go.Figure:
        return plot_supply_consumption_matching(self.df)

    # This function is overriden below and in all derived classes so the return type is specific to the class
    # (e.g. in MatchMonthly, the return type for the transform function is MatchHalfHourlyAnnualised)
    def _transform_to_match_annualised(self) -> pd.DataFrame:
        # fmt: off
        agg = (self.df.reset_index()
                .groupby(lambda _: 0)
                .agg(
                    timestamp                   =("timestamp",           "first"),
                    supply_total_mwh            =("supply_total_mwh",    "sum"),
                    rego_holder_max             =("rego_holder_count",   "max"),
                    supply_biomass_mwh          =("supply_biomass_mwh",  "sum"),
                    supply_biomass_station_max  =("supply_biomass_station_count", "max"),
                    supply_hydro_mwh            =("supply_hydro_mwh",    "sum"),
                    supply_hydro_station_max    =("supply_hydro_station_count", "max"),
                    supply_other_mwh            =("supply_other_mwh",    "sum"),
                    supply_other_station_max    =("supply_other_station_count", "max"),
                    supply_solar_mwh            =("supply_solar_mwh",    "sum"),
                    supply_solar_station_max    =("supply_solar_station_count", "max"),
                    supply_wind_mwh             =("supply_wind_mwh",     "sum"),
                    supply_wind_station_max     =("supply_wind_station_count", "max"),
                    consumption_mwh             =("consumption_mwh",     "sum"),
                    supply_surplus_mwh          =("supply_surplus_mwh",  "sum"),
                    supply_deficit_mwh          =("supply_deficit_mwh",  "sum"),
                )
                .set_index("timestamp", drop=True)
            )
        agg["matching_score"] = calculate_matching_score(
                deficit=agg["supply_deficit_mwh"],
                consumption=agg["consumption_mwh"],
            )
        # fmt: on

        return pd.DataFrame(agg)

    def transform_to_match_half_hourly_annualised(self) -> MatchHalfHourlyAnnualised:
        return MatchHalfHourlyAnnualised(self._transform_to_match_annualised())


class MatchHalfHourlyAnnualised(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        timestamp                   =CS(check=pa.Index(DTE(dayfirst=False))),
        supply_total_mwh            =CS(check=pa.Column(float)),
        rego_holder_max             =CS(check=pa.Column(int)),  
        supply_biomass_mwh          =CS(check=pa.Column(float)), 
        supply_biomass_station_max  =CS(check=pa.Column(int)),   
        supply_hydro_mwh            =CS(check=pa.Column(float)),  
        supply_hydro_station_max    =CS(check=pa.Column(int)),   
        supply_other_mwh            =CS(check=pa.Column(float)),
        supply_other_station_max    =CS(check=pa.Column(int)),
        supply_solar_mwh            =CS(check=pa.Column(float)),
        supply_solar_station_max    =CS(check=pa.Column(int)),
        supply_wind_mwh             =CS(check=pa.Column(float)),  
        supply_wind_station_max     =CS(check=pa.Column(int)),
        consumption_mwh             =CS(check=pa.Column(float)),
        supply_deficit_mwh          =CS(check=pa.Column(float)),  
        supply_surplus_mwh          =CS(check=pa.Column(float)),  
        matching_score              =CS(check=pa.Column(float, checks=[pa.Check.greater_than_or_equal_to(0), pa.Check.less_than_or_equal_to(1)])),
    )
    # fmt: on
    from_file_skiprows = 1
    from_file_with_index = True
