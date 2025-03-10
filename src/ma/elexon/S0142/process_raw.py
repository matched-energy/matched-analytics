import glob
import os
from pathlib import Path
from typing import Dict, Generator, Optional, Tuple

import pandas as pd
import pandera as pa

from ma.elexon.metering_data.metering_data_by_half_hour_and_bmu import (
    MeteringDataHalfHourlyByBmu,
    MeteringDataHalfHourlyByBmuType,
)
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset

# TODO - apply_schema() at this point

COLUMN_MAP_BP7: dict[int, str] = {
    1: "BM Unit Id",
    2: "Information Imbalance Cashflow",
    3: "BM Unit Period Non-Delivery Charge",
    4: "Period FPN",
    5: "Period BM Unit Balancing Services Volume",
    6: "Period Information Imbalance Volume",
    7: "Period Expected Metered Volume",
    8: "BM Unit Metered Volume",
    9: "Period BM Unit Non-Delivered Bid Volume",
    10: "Period BM Unit Non-Delivered Offer Volume",
    11: "Transmission Loss Factor",
    12: "Transmission Loss Multiplier",
    13: "Trading Unit Name",
    14: "Total Trading Unit Metered Volume",
    15: "BM Unit Applicable Balancing Services Volume",
    16: "Period Supplier BM Unit Delivered Volume",
    17: "Period Supplier BM Unit Non BM ABSVD Volume",
}


def update_bsc_df_types_in_place(df: pd.DataFrame) -> None:
    float_columns = [
        "BM Unit Metered Volume",
        "Period BM Unit Balancing Services Volume",
        "Period Expected Metered Volume",
        "Period Information Imbalance Volume",
        "Transmission Loss Factor",
        "Transmission Loss Multiplier",
        "BM Unit Applicable Balancing Services Volume",
        "Period Supplier BM Unit Delivered Volume",
        "Period Supplier BM Unit Non BM ABSVD Volume",
    ]
    int_columns = ["Settlement Period"]

    df[float_columns] = df[float_columns].astype(float)
    df[int_columns] = df[int_columns].astype(int)


def get_bsc_df(
    S0142_df: pd.DataFrame, start_row: int, bsc_party_id: str, header: str, column_map: dict
) -> pd.DataFrame:
    lines = []
    for _, line in S0142_df[start_row + 1 :].iterrows():
        if line[0] == "SP7":
            SP = int(line[1])
        elif line[0] == header:  # Look for header (BP7) section
            lines.append(line.to_list() + [SP, bsc_party_id])
        elif line[0] == "BPH":
            break

    bsc_df = pd.DataFrame(lines, columns=list(range(len(lines[0]) - 2)) + ["Settlement Period", "BSC"])
    # Settlement Date
    settlement_date = str(S0142_df.loc[1, 1])
    bsc_df["Settlement Date"] = settlement_date[6:8] + "/" + settlement_date[4:6] + "/" + settlement_date[0:4]
    # Settlement Run Type
    bsc_df["Settlement Run Type"] = S0142_df.loc[1, 2]

    return bsc_df.rename(columns=column_map)


def get_bsc_df_map(S0142_df: pd.DataFrame, bsc_party_ids: list[str]) -> Generator[Tuple[str, pd.DataFrame], None, None]:
    for row_number, row in enumerate(S0142_df.iterrows()):
        row_data = row[1]
        if row_data[0] == "BPH":  # Look for BPH section
            if row_data[8] in bsc_party_ids or bsc_party_ids == ["all!"]:  # Look for BSC Party
                bsc_party_id = row_data[8]
                bsc_df = get_bsc_df(S0142_df, row_number, bsc_party_id, "BP7", COLUMN_MAP_BP7)
                bsc_df.reset_index(drop=True, inplace=True)
                update_bsc_df_types_in_place(bsc_df)
                yield bsc_party_id, bsc_df


def process_file(input_path: Path, bsc_party_ids: list[str]) -> Generator[Tuple[str, pd.DataFrame], None, None]:
    df = pd.read_csv(
        input_path,
        compression="gzip",
        header=None,
        delimiter="|",
        low_memory=False,
        names=range(0, 76),
    )
    for bsc_party_id, df_final in get_bsc_df_map(df, bsc_party_ids):
        yield bsc_party_id, df_final


def process_directory(
    input_dir: Path,
    output_dir: Path,
    bsc_party_ids: list[str],
    prefixes: Optional[list[str]] = None,
) -> None:
    filenames = sorted(
        [
            filename
            for filename in os.listdir(input_dir)
            if filename.startswith("S0142")
            and filename.endswith(".gz")
            and (prefixes is None or any(filename.startswith(p) for p in prefixes))
        ]
    )
    for filename in filenames:
        input_path = os.path.join(input_dir, filename)
        output_path_prefix = os.path.join(output_dir, filename.strip(".gz"))
        if glob.glob(output_path_prefix + "*"):
            print(f"Skipping {input_path}")
        else:
            for bsc, load in process_file(Path(input_path), bsc_party_ids):
                if output_path_prefix:
                    load.to_csv(
                        output_path_prefix + "_{}.csv".format(bsc),
                        index=False,
                    )


ProcessedS0142Type = pd.DataFrame


class ProcessedS0142(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        bsc                                         =CS(check=pa.Column(str)),
        settlement_date                             =CS(check=pa.Column(str)),
        settlement_period                           =CS(check=pa.Column(int)),
        settlement_run_type                         =CS(check=pa.Column(str)),
        bm_unit_id                                  =CS(check=pa.Column(str)),
        information_imbalance_cashflow              =CS(check=pa.Column(float)),
        bm_unit_period_non_delivery_charge          =CS(check=pa.Column(float)),
        period_fpn                                  =CS(check=pa.Column(float)),
        period_bm_unit_balancing_services_volume    =CS(check=pa.Column(float)),
        period_information_imbalance_volume         =CS(check=pa.Column(float)),
        period_expected_metered_volume              =CS(check=pa.Column(float)),
        bm_unit_metered_volume_mwh                  =CS(check=pa.Column(float)),
        period_bm_unit_non_delivered_bid_volume     =CS(check=pa.Column(str)),
        period_bm_unit_non_delivered_offer_volume   =CS(check=pa.Column(str)),
        transmission_loss_factor                    =CS(check=pa.Column(float)),
        transmission_loss_multiplier                =CS(check=pa.Column(float)),
        trading_unit_name                           =CS(check=pa.Column(str)),
        total_trading_unit_metered_volume           =CS(check=pa.Column(float)),
        bm_unit_applicable_balancing_services_volume=CS(check=pa.Column(float)),
        period_supplier_bm_unit_delivered_volume    =CS(check=pa.Column(float)),
        period_supplier_bm_unit_non_bm_absvd_volume =CS(check=pa.Column(float)),
    )
    from_file_with_index = False 
    # fmt: on


def transform_to_half_hourly_by_bmu(processed_s0142: ProcessedS0142Type) -> MeteringDataHalfHourlyByBmuType:
    """Return half_hourly_by_bmu metering data"""
    output = ProcessedS0142.from_dataframe(processed_s0142)
    output["settlement_date"] = pd.to_datetime(output["settlement_date"], dayfirst=True)
    output["settlement_datetime"] = output["settlement_date"] + (output["settlement_period"] - 1) * pd.Timedelta(
        minutes=30
    )
    output = output.reset_index(drop=True).set_index("settlement_datetime")
    return MeteringDataHalfHourlyByBmu.from_dataframe(output)
