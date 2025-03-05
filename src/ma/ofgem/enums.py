from enum import StrEnum
from typing import Tuple
from datetime import date


class RegoStatus(StrEnum):
    ISSUED = "Issued"
    RETIRED = "Retired"
    REVOKED = "Revoked"
    EXPIRED = "Expired"
    REDEEMED = "Redeemed"


class RegoScheme(StrEnum):
    REGO = "REGO"
    RO = "RO"


class RegoCompliancePeriod(StrEnum):
    CP20 = "CP20"  # April 2021 to March 2022
    CP21 = "CP21"  # April 2022 to March 2023
    CP22 = "CP22"  # April 2023 to March 2024
    CP23 = "CP23"  # April 2024 to March 2025

    @property
    def date_range(self) -> Tuple[date, date]:
        """Return the start and exclusive end dates for this compliance period."""
        mappings = {
            self.CP20: (date(2021, 4, 1), date(2022, 4, 1)),
            self.CP21: (date(2022, 4, 1), date(2023, 4, 1)),
            self.CP22: (date(2023, 4, 1), date(2024, 4, 1)),
            self.CP23: (date(2024, 4, 1), date(2025, 4, 1)),
        }
        return mappings[self]
