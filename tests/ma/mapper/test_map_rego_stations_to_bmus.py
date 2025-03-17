from pathlib import Path
from typing import TypedDict
from unittest.mock import patch

import pandas as pd
from pandas import Timestamp
from pandas.testing import assert_frame_equal

import data.register
import ma.elexon.bmus
import ma.mapper.map_rego_stations_to_bmus
import ma.ofgem.stations
from ma.ofgem.enums import RegoStatus
from ma.ofgem.regos import RegosRaw

monthly_vols = {
    "DRAX": {
        "settlement_date": {
            0: Timestamp("2022-04-01 00:00:00"),
            1: Timestamp("2022-05-01 00:00:00"),
            2: Timestamp("2022-06-01 00:00:00"),
            3: Timestamp("2022-07-01 00:00:00"),
            4: Timestamp("2022-08-01 00:00:00"),
            5: Timestamp("2022-09-01 00:00:00"),
            6: Timestamp("2022-10-01 00:00:00"),
            7: Timestamp("2022-11-01 00:00:00"),
            8: Timestamp("2022-12-01 00:00:00"),
            9: Timestamp("2023-01-01 00:00:00"),
            10: Timestamp("2023-02-01 00:00:00"),
            11: Timestamp("2023-03-01 00:00:00"),
        },
        "bm_unit_metered_volume_gwh": {
            0: 1069.243975,
            1: 663.4200629999999,
            2: 829.629625,
            3: 1193.7619639999998,
            4: 1196.456162,
            5: 1316.8971780000002,
            6: 652.3836729999999,
            7: 1100.2471340000002,
            8: 1195.1905570000001,
            9: 1088.878322,
            10: 1155.049862,
            11: 1116.0452309999998,
        },
    },
    "DONG012": {
        "settlement_date": {
            0: Timestamp("2022-04-01 00:00:00"),
            1: Timestamp("2022-05-01 00:00:00"),
            2: Timestamp("2022-06-01 00:00:00"),
            3: Timestamp("2022-07-01 00:00:00"),
            4: Timestamp("2022-08-01 00:00:00"),
            5: Timestamp("2022-09-01 00:00:00"),
            6: Timestamp("2022-10-01 00:00:00"),
            7: Timestamp("2022-11-01 00:00:00"),
            8: Timestamp("2022-12-01 00:00:00"),
            9: Timestamp("2023-01-01 00:00:00"),
            10: Timestamp("2023-02-01 00:00:00"),
            11: Timestamp("2023-03-01 00:00:00"),
        },
        "bm_unit_metered_volume_gwh": {
            0: 167.99957500000002,
            1: 218.15741699999998,
            2: 154.42182699999998,
            3: 166.141122,
            4: 108.94156100000001,
            5: 192.038186,
            6: 323.52999800000003,
            7: 305.876183,
            8: 229.220029,
            9: 302.296335,
            10: 241.54166,
            11: 255.950893,
        },
    },
    "TKWFL": {
        "settlement_date": {
            0: Timestamp("2022-04-01 00:00:00"),
            1: Timestamp("2022-05-01 00:00:00"),
            2: Timestamp("2022-06-01 00:00:00"),
            3: Timestamp("2022-07-01 00:00:00"),
            4: Timestamp("2022-08-01 00:00:00"),
            5: Timestamp("2022-09-01 00:00:00"),
            6: Timestamp("2022-10-01 00:00:00"),
            7: Timestamp("2022-11-01 00:00:00"),
            8: Timestamp("2022-12-01 00:00:00"),
            9: Timestamp("2023-01-01 00:00:00"),
            10: Timestamp("2023-02-01 00:00:00"),
            11: Timestamp("2023-03-01 00:00:00"),
        },
        "bm_unit_metered_volume_gwh": {
            0: 133.596191,
            1: 190.23191699999998,
            2: 158.67947,
            3: 151.134425,
            4: 102.622888,
            5: 190.144336,
            6: 329.537852,
            7: 319.912963,
            8: 336.395608,
            9: 397.339706,
            10: 270.58805800000005,
            11: 313.96476,
        },
    },
}


def mock_get(
    bsc_lead_party_id: str,
    bm_ids: list,
    S0142_csv_dir: Path,
) -> pd.DataFrame:
    return pd.DataFrame(monthly_vols[bsc_lead_party_id]).set_index("settlement_date")


def test_end_to_end() -> None:
    regos = RegosRaw(data.register.REGOS_APR2022_MAR2023_SUBSET).transform_to_regos_processed()
    regos = regos.filter(statuses=[RegoStatus.REDEEMED])
    with patch("ma.mapper.filter_on_aggregate_data.get_bmu_volumes_by_month") as get_vols:
        get_vols.side_effect = mock_get
        mappings = ma.mapper.map_rego_stations_to_bmus.map_station_range(
            start=0,
            stop=3,
            regos=regos,
            accredited_stations=ma.ofgem.stations.load_rego_stations_processed_from_dir(
                data.register.REGO_ACCREDITED_STATIONS_DIR
            ),
            bmus=ma.elexon.bmus.load(data.register.BMUNITS_SUBSET),
            bmu_vol_dir=Path("/mocked"),
        )

    class ValidationData(TypedDict):
        rego_name: str
        bmu_ids: str
        lead_party_name: str
        lead_party_id: str
        rego_mw: float
        bmu_net_mw: float
        rego_gwh: float
        bmu_gwh: float
        rego_bmu_monthly_volume_ratio_median: float

    expected_mappings = pd.DataFrame(
        [
            ValidationData(
                {
                    "rego_name": "Drax Power Station (REGO)",
                    "bmu_ids": "T_DRAXX-1, T_DRAXX-2, T_DRAXX-3, T_DRAXX-4",
                    "lead_party_name": "Drax Power Ltd",
                    "lead_party_id": "DRAX",
                    "rego_mw": 3865,
                    "bmu_net_mw": 2551.067,
                    "rego_gwh": 12425.565,
                    "bmu_gwh": 12577.203,
                    "rego_bmu_monthly_volume_ratio_median": 1.032024,
                }
            ),
            ValidationData(
                {
                    "rego_name": "Walney Extension",
                    "bmu_ids": "T_WLNYO-3, T_WLNYO-4",
                    "lead_party_name": "Walney Extension Ltd",
                    "lead_party_id": "DONG012",
                    "rego_mw": 648,
                    "bmu_net_mw": 650.349,
                    "rego_gwh": 2383.633,
                    "bmu_gwh": 2666.114,
                    "rego_bmu_monthly_volume_ratio_median": 0.967947,
                }
            ),
            ValidationData(
                {
                    "rego_name": "Triton Knoll Offshore Windfarm",
                    "bmu_ids": "T_TKNEW-1, T_TKNWW-1",
                    "lead_party_name": "Triton Knoll Offshore Wind",
                    "lead_party_id": "TKWFL",
                    "rego_mw": 847.477,
                    "bmu_net_mw": 810.87,
                    "rego_gwh": 2305.086,
                    "bmu_gwh": 2894.148,
                    "rego_bmu_monthly_volume_ratio_median": 0.797787,
                }
            ),
        ]
    )
    assert_frame_equal(
        mappings[ValidationData.__annotations__.keys()].reset_index(drop=True),
        expected_mappings.reset_index(drop=True),
    )
