from typing import Dict

import pandera as pa

from ma.utils.pandas import ColumnSchema as CS

# fmt: off
schema_on_load: Dict[str, CS] = dict(
    bsc							                =CS(check=pa.Column(str)),
    settlement_date							    =CS(check=pa.Column(str)),
    settlement_period						    =CS(check=pa.Column(int)),
    settlement_run_type						    =CS(check=pa.Column(str)),
    bm_unit_id							        =CS(check=pa.Column(str)),
    information_imbalance_cashflow	            =CS(check=pa.Column(float)),
    bm_unit_period_non_delivery_charge          =CS(check=pa.Column(float)),
    period_fpn							        =CS(check=pa.Column(float)),
    period_bm_unit_balancing_services_volume    =CS(check=pa.Column(float)),
    period_information_imbalance_volume	        =CS(check=pa.Column(float)),
    period_expected_metered_volume		        =CS(check=pa.Column(float)),
    bm_unit_metered_volume_mwh	    	        =CS(check=pa.Column(float)),
    period_bm_unit_non_delivered_bid_volume	    =CS(check=pa.Column(str)),
    period_bm_unit_non_delivered_offer_volume   =CS(check=pa.Column(str)),
    transmission_loss_factor                    =CS(check=pa.Column(float)),
    transmission_loss_multiplier                =CS(check=pa.Column(float)),
    trading_unit_name                           =CS(check=pa.Column(str)),
    total_trading_unit_metered_volume           =CS(check=pa.Column(float)),
    bm_unit_applicable_balancing_services_volume=CS(check=pa.Column(float)),
    period_supplier_bm_unit_delivered_volume  	=CS(check=pa.Column(float)),
    period_supplier_bm_unit_non_bm_absvd_volume =CS(check=pa.Column(float)),
)
# fmt: on
