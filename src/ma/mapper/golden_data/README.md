python map_rego_stations_to_bmus.py                                        \
        --start 0                                                          \
        --stop 10                                                          \
        --regos-path ${MATCHED_DATA}/raw/regos-apr2022-mar2023.csv         \
        --accredited-stations-dir ${MATCHED_DATA}/raw/accredited-stations/ \
        --bmus-path ${MATCHED_DATA}/raw/bmrs_bm_units-20241211.json        \
        --bmu-vol-dir ${MATCHED_DATA}/2024-12-12-CP2023-all-bscs-s0142     \
        --expected-mappings-file expected_mappings.yaml                    \
        --mappings-path mappings.csv                                       \
        --abbreviated-mappings-path abbreviated-mappings.csv
