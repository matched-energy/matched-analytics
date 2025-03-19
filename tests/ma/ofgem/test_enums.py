from datetime import date

from ma.ofgem.enums import RegoCompliancePeriod


def test_rego_compliance_period_months() -> None:
    cp = RegoCompliancePeriod.CP22
    start_date, end_date = cp.date_range
    months = cp.months
    assert start_date == date(2023, 4, 1)
    assert end_date == date(2024, 4, 1)
    assert len(set(months)) == 12
    assert min(months) == start_date
    assert max(months) == date(2024, 3, 1)
