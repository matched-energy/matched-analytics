from typing import Dict

import pandera as pa

from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# fmt: off
STATIONS_SCHEMA: Dict[str, CS] = dict(
    accreditation_number         = CS(check=pa.Column(str)), 
    status                       = CS(check=pa.Column(str)), 
    generating_station           = CS(check=pa.Column(str)), 
    scheme                       = CS(check=pa.Column(str)), 
    station_dnc                  = CS(check=pa.Column(float)), 
    country                      = CS(check=pa.Column(str)), 
    technology                   = CS(check=pa.Column(str)), 
    contract_type                = CS(check=pa.Column(str)), 
    accreditation_date           = CS(check=pa.Column(DTE)), 
    commission_date              = CS(check=pa.Column(DTE)), 
    organisation                 = CS(check=pa.Column(str)), 
    organisation_contact_address = CS(check=pa.Column(str)), 
    organisation_contact_fax     = CS(keep=False), 
    generating_sation_address    = CS(check=pa.Column(str)), 
)
# fmt: on
