# technology enums


from enum import Enum


class ProductionTechEnum(Enum):
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
