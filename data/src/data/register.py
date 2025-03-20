from pathlib import Path

DATA_DIR_ROOT = Path(__file__).resolve().parent


################################################################################
### SOURCES
################################################################################
### ELEXON
DIR_ELEXON = DATA_DIR_ROOT / "elexon"
S0142_20230330_SF_20230425121906_GZ = DIR_ELEXON / "S0142_20230330_SF_20230425121906.gz"
S0142_20230330_SF_20230425121906_GOLD_CSV = DIR_ELEXON / "S0142_20230330_SF_20230425121906_GOLD.csv"
S0142_20230331_SF_20230426191253_GOLD_CSV = DIR_ELEXON / "S0142_20230331_SF_20230426191253_GOLD.csv"
S0142_CSV_DIR = DIR_ELEXON
BMUNITS_SUBSET = DIR_ELEXON / "bmunits_SUBSET.json"


### OFGEM
DIR_OFGEM = DATA_DIR_ROOT / "ofgem"
REGOS_APR2022_MAR2023_SUBSET = DIR_OFGEM / "regos_apr2022_mar2023_SUBSET.csv"
REGO_ACCREDITED_STATIONS_DIR = DIR_OFGEM / "rego_accredited_stations"


### NESO
DIR_NESO = DATA_DIR_ROOT / "neso"
NESO_FUEL_CKAN_CSV_SUBSET_FEB2023_MAR2023 = DIR_NESO / "df_fuel_ckan_feb2023_mar2023.csv"


################################################################################
### PROCESSED
################################################################################
DIR_PROCESSED = DATA_DIR_ROOT / "processed"
REGOS_BY_TECH_MONTH_HOLDER = DIR_PROCESSED / "regos_by_tech_month_holder.csv"
CONSUMPTION_BY_MONTH = DIR_PROCESSED / "consumption_by_month.csv"
