from __future__ import annotations
from typing import Dict, Type, TypeVar
import pandas as pd
import pandera as pa
import plotly.graph_objects as go

from ma.ofgem.regos import RegosByTechMonthHolder
from ma.retailer.consumption import ConsumptionHalfHourly, ConsumptionMonthly
from ma.retailer.supply_hh import UpsampledSupplyHalfHourly
from ma.utils.pandas import DataFrameAsset, ColumnSchema as CS, DateTimeEngine as DTE
from ma.matching.utils import (
    plot_supply_consumption_matching,
    calculate_matching_score,
    calculate_supply_surplus_deficit,
)
from ma.utils.enums import SupplyTechEnum


T = TypeVar("T", bound="MatchBase")


class MatchBase(DataFrameAsset):
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

    @classmethod
    def make(
        cls: Type[T],
        supply: UpsampledSupplyHalfHourly | RegosByTechMonthHolder,
        consumption: ConsumptionHalfHourly | ConsumptionMonthly,
    ) -> T:
        """Create a match object from supply and consumption data.

        Args:
            supply: Either half-hourly supply data or monthly REGO data
            consumption: Either half-hourly or monthly consumption data

        Returns:
            A MatchBase instance (either MatchHalfHourly or MatchMonthly)
        """
        supply_df = supply.df.copy()

        # check the correct combination of supply and consumption types
        if isinstance(supply, UpsampledSupplyHalfHourly) and isinstance(consumption, ConsumptionHalfHourly):
            pass
        elif isinstance(supply, RegosByTechMonthHolder) and isinstance(consumption, ConsumptionMonthly):
            pass
        else:
            raise ValueError(
                f"Supply and consumption must be either both half-hourly or both monthly, got supply type {type(supply)} and consumption type {type(consumption)} "
            )

        # Standardize column names
        if isinstance(supply, RegosByTechMonthHolder):
            supply_df["supply_mwh"] = supply_df["rego_mwh"]
            supply_df["holder"] = supply_df["current_holder"]
            # Monthly data is already indexed by month
            supply_pivoted = supply_df.groupby(level=0).agg(
                supply_total_mwh=("supply_mwh", "sum"),
                rego_holder_count=("holder", "nunique"),
            )
        else:
            supply_df["holder"] = supply_df["retailer"]
            if not supply_df.index.equals(consumption.df.index):
                raise ValueError(
                    f"Supply and consumption cover different time periods: {supply_df.index.min()} to {supply_df.index.max()} and {consumption.df.index.min()} to {consumption.df.index.max()}"
                )
            supply_pivoted = supply_df.groupby("timestamp").agg(
                supply_total_mwh=("supply_mwh", "sum"),
                rego_holder_count=("holder", "nunique"),
            )

        # Add technology-specific supply data
        for tech in SupplyTechEnum.alphabetical_renewables():
            tech_supply = supply_df[supply_df["tech"] == tech]
            supply_pivoted = supply_pivoted.join(
                pd.DataFrame(
                    {
                        f"supply_{tech}_mwh": tech_supply["supply_mwh"],
                        f"supply_{tech}_station_count": tech_supply["holder"].nunique(),
                    }
                )
            ).fillna(0)

        # Join with consumption data
        match_df = supply_pivoted.join(consumption.df, how="inner")

        # Calculate matching metrics
        match_df["supply_surplus_mwh"], match_df["supply_deficit_mwh"] = calculate_supply_surplus_deficit(
            supply=match_df["supply_total_mwh"],
            consumption=match_df["consumption_mwh"],
        )

        match_df["matching_score"] = calculate_matching_score(
            deficit=match_df["supply_deficit_mwh"],
            consumption=match_df["consumption_mwh"],
        )

        return cls(match_df)

    # This function is overridden in all derived classes so the return type is specific to the class
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

        return agg

    def plot(self) -> go.Figure:
        return plot_supply_consumption_matching(self.df)


class MatchAnnualisedBase(DataFrameAsset):
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
