from enum import StrEnum
from typing import List


class TemporalGranularity(StrEnum):
    HALF_HOURLY = "half-hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"

    @property
    def noun(self) -> str:
        """Returns the singular noun form corresponding to the granularity level."""
        mappings = {
            self.HALF_HOURLY: "half-hour",
            self.DAILY: "day",
            self.MONTHLY: "month",
            self.YEARLY: "year",
        }
        return mappings[self]

    @property
    def pandas_period(self) -> str:
        """Returns the string to be used in to_period()"""
        if self == TemporalGranularity.HALF_HOURLY:
            raise ValueError("No Pandas period for half-hourly granularity")

        mappings = {
            self.DAILY: "D",
            self.MONTHLY: "M",
            self.YEARLY: "Y",
        }
        return mappings[self]

    @property
    def preceeding(self) -> "TemporalGranularity":
        """Return the next-most level of granularity"""
        if self == TemporalGranularity.HALF_HOURLY:
            raise ValueError("No preceeding granularity for half-hourly")
        mappings = {
            self.DAILY: TemporalGranularity.HALF_HOURLY,
            self.MONTHLY: TemporalGranularity.DAILY,
            self.YEARLY: TemporalGranularity.MONTHLY,
        }
        return mappings[self]


class SupplyTechEnum(StrEnum):
    GAS = "gas"
    COAL = "coal"
    NUCLEAR = "nuclear"
    WIND = "wind"
    HYDRO = "hydro"
    IMPORTS = "imports"
    BIOMASS = "biomass"
    OTHER = "other"
    SOLAR = "solar"
    STORAGE = "storage"

    @classmethod
    def alphabetical_renewables(cls) -> List:
        """Ordering allows deterministic schema generation."""
        return [
            cls.BIOMASS,
            cls.HYDRO,
            cls.OTHER,
            cls.SOLAR,
            cls.WIND,
        ]
