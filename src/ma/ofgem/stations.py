from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

import pandas as pd
import pandera as pa

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset


def load_rego_stations_processed_from_dir(dir: Path) -> RegoStationsProcessed:
    return RegoStationsProcessed(
        pd.concat(
            [
                RegoStationsRaw(Path(entry.path)).transform_to_rego_stations_processed().df.reset_index(drop=True)
                for entry in os.scandir(dir)
                if entry.is_file() and entry.name.endswith(".csv")
            ],
            ignore_index=True,
            join="outer",
        )
    )


class RegoStationsRaw(DataFrameAsset):
    # fmt: off
    schema : Dict[str, CS] = dict(
        accreditation_number         = CS(check=pa.Column(str)), 
        status                       = CS(check=pa.Column(str)), 
        generating_station           = CS(check=pa.Column(str)), 
        scheme                       = CS(check=pa.Column(str)), 
        station_dnc                  = CS(check=pa.Column(float)), 
        country                      = CS(check=pa.Column(str)), 
        technology                   = CS(check=pa.Column(str)), 
        contract_type                = CS(check=pa.Column(str)), 
        accreditation_date           = CS(check=pa.Column(str)), 
        commission_date              = CS(check=pa.Column(str)), 
        organisation                 = CS(check=pa.Column(str, nullable=True)), 
        organisation_contact_address = CS(check=pa.Column(str, nullable=True)), 
        organisation_contact_fax     = CS(check=pa.Column(str, nullable=True), keep=False), 
        generating_station_address   = CS(check=pa.Column(str, nullable=True)), 
    )
    from_file_skiprows = 1
    from_file_with_index = False
    # fmt: on

    def transform_to_rego_stations_processed(self) -> RegoStationsProcessed:
        stations = self.df
        stations["station_dnc_mw"] = stations["station_dnc"] / 1e3
        stations.drop(columns=["station_dnc"], inplace=True)
        return RegoStationsProcessed(stations)


class RegoStationsProcessed(DataFrameAsset):
    # fmt: off
    schema : Dict[str, CS] = dict(
        accreditation_number         = CS(check=pa.Column(str)), 
        status                       = CS(check=pa.Column(str)), 
        generating_station           = CS(check=pa.Column(str)), 
        scheme                       = CS(check=pa.Column(str)), 
        country                      = CS(check=pa.Column(str)), 
        technology                   = CS(check=pa.Column(str)), 
        contract_type                = CS(check=pa.Column(str)), 
        accreditation_date           = CS(check=pa.Column(str)), 
        commission_date              = CS(check=pa.Column(str)), 
        organisation                 = CS(check=pa.Column(str, nullable=True)), 
        organisation_contact_address = CS(check=pa.Column(str, nullable=True)), 
        generating_station_address   = CS(check=pa.Column(str, nullable=True)), 
        station_dnc_mw               = CS(check=pa.Column(float)), 
    )
    from_file_skiprows = 1
    from_file_with_index = False
    # fmt: on
