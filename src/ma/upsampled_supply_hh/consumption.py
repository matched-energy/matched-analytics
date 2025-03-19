from typing import Dict

import pandera as pa

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE


class ConsumptionMonthly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        timestamp         =CS(check=pa.Index(DTE(dayfirst=False))),
        consumption_mwh   =CS(check=pa.Column(float)),
    )
    # fmt: on
