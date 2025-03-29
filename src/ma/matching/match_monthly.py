from __future__ import annotations


from ma.matching.base import MatchAnnualisedBase, MatchBase


class MatchMonthly(MatchBase):
    def transform_to_match_monthly_annualised(self) -> MatchMonthlyAnnualised:
        return MatchMonthlyAnnualised(self._transform_to_match_annualised())


class MatchMonthlyAnnualised(MatchAnnualisedBase):
    pass
