from ma.utils.enums import SupplyTechEnum


def test_supply_tech_enum() -> None:
    renewables = SupplyTechEnum.alphabetical_renewables()
    assert sorted(renewables) == renewables
