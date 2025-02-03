Mapper
====================


Map [Ofgem REGO Generating Stations](https://renewablesandchp.ofgem.gov.uk/) to [Elexon BM Units](https://bmrs.elexon.co.uk/api-documentation/endpoint/reference/bmunits/all).

Prerequisites
----------
- Parsed S0142 files (see [/elexon/S0142](../elexon/S0142))
- BMUs (see [/elexon/api](../elexon/api))
- Redeemed REGO certificates and accredited stations (see [/ofgem](../ofgem))

Mapping approach
----------
Mapping is done in two phases:

#### 1. Fuzzy meta-data match
A fuzzy match is made between:
- REGO station name, lead party name, and BM unit name
- fuel type
- generation capacity

The fuzzy match yields zero, one, or more likely matching BM units.

See `filter_on_bmu_meta_data.py`

#### 2. Validate against power & energy
Candidate BM units appraised in terms of:
- their aggregate power rating, versus the nameplate capacity of the REGO station
- monthly metered volumes, versus retired REGOs

See `filter_on_aggregate_data.py`

Mapping score
----------
A qualitative mapping score is given in terms of the match between:
- registered names
- min/median/max monthly volumes
- rated power capacities

The scores given are:
- `near certain`
- `very probable`
- `likely`
- `possible`

See `summarise_and_score.py`

Example invocation
----------

    python map_rego_stations_to_bmus.py \
        --start 0                                                           \  # Start with the biggest REGO generator
        --stop 100                                                          \  # ... and end with the 100th biggest
        --regos-path ${MATCHED_DATA}/raw/regos-apr2022-mar2023.csv          \  # REGOS, as downloaded from Ofgem
        --accredited-stations-dir ${MATCHED_DATA}/raw/accredited-stations/  \  # Accredited stations, as downloaded from Ofgem
        --bmus-path ${MATCHED_DATA}/raw/bmrs_bm_units-20241211.json         \  # BM Units, as downloaded from Elexon
        --bmu-vol-dir ${MATCHED_DATA}/processed/S0142                       \  # BM Metered Volumes, as processed from Elexon S0142 files
        --expected-mappings-file expected_mappings.yaml                     \  # Overrides (see checked in file)
        --mappings-path mappings.csv                                        \  #
        --abbreviated-mappings-path abbreviated-mappings.csv                \  # Human-readable output
