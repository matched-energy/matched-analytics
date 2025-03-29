from __future__ import annotations
from ma.matching.base import MatchAnnualisedBase, MatchBase


class MatchHalfHourly(MatchBase):
    def transform_to_match_half_hourly_annualised(self) -> MatchHalfHourlyAnnualised:
        return MatchHalfHourlyAnnualised(self._transform_to_match_annualised())


class MatchHalfHourlyAnnualised(MatchAnnualisedBase):
    pass
