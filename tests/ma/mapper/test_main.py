import pandas as pd
from pandas.testing import assert_frame_equal

import data.register
import ma.mapper.map_rego_stations_to_bmus


# TODO - test on energy volumes and other dimensions
def test_end_to_end() -> None:
    mappings = ma.mapper.map_rego_stations_to_bmus.main(
        start=0,
        stop=3,
        regos_path=data.register.REGOS_APR2022_MAR2023_SUBSET,
        accredited_stations_dir=data.register.REGO_ACCREDITED_STATIONS_DIR,
        bmus_path=data.register.BMUNITS_SUBSET,
    )
    expected_mappings = pd.DataFrame(
        [
            {
                "rego_name": "Drax Power Station (REGO)",
                "bmu_ids": "T_DRAXX-1, T_DRAXX-2, T_DRAXX-3, T_DRAXX-4",
            },
            {"rego_name": "Walney Extension", "bmu_ids": "T_WLNYO-3, T_WLNYO-4"},
            {
                "rego_name": "Triton Knoll Offshore Windfarm",
                "bmu_ids": "T_TKNEW-1, T_TKNWW-1",
            },
        ]
    )
    assert_frame_equal(
        mappings[["rego_name", "bmu_ids"]].reset_index(drop=True),
        expected_mappings.reset_index(drop=True),
    )
