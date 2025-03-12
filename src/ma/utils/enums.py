# technology enums


from enum import StrEnum


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
