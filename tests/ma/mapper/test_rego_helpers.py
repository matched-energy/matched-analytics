from pytest import approx

import data.register
import ma.mapper.rego_helpers
import ma.ofgem.regos


def test_get_rego_station_volume_stats() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)
    monthly_volumes = ma.mapper.rego_helpers.get_rego_station_volume_by_month(regos, "Drax Power Station (REGO)")
    stats = ma.mapper.rego_helpers.get_rego_station_volume_stats(monthly_volumes=monthly_volumes, station_dnc_mw=3865.0)
    assert stats["rego_total_volume"] == approx(12425.565)
    assert stats["rego_capacity_factor"] == approx(12425.565 * 1e3 / (3865 * 24 * 365))
    assert stats["rego_sample_months"] == 12


def test_get_rego_station_volume_by_month() -> None:
    regos = ma.ofgem.regos.load(data.register.REGOS_APR2022_MAR2023_SUBSET)

    # 12 months
    volumes_by_month = ma.mapper.rego_helpers.get_rego_station_volume_by_month(regos, "Drax Power Station (REGO)")
    assert len(volumes_by_month) == 12

    # 6 months
    regos_half_year = regos[regos["period_start"].dt.month <= 6]
    volumes_by_month = ma.mapper.rego_helpers.get_rego_station_volume_by_month(
        regos_half_year, "Drax Power Station (REGO)"
    )
    assert len(volumes_by_month) == 6
