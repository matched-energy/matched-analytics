import pandera as pa

from ma.utils.pandas import ColumnSchema as CS

# fmt: off
schema_bmus_on_load = dict(
    national_grid_bm_unit                               = CS(check=pa.Column(str)),
    elexon_bm_unit                                      = CS(check=pa.Column(str)),
    eic                                                 = CS(check=pa.Column(str, nullable=True)),
    fuel_type                                           = CS(check=pa.Column(str)),
    lead_party_name                                     = CS(check=pa.Column(str)),
    bm_unit_type                                        = CS(check=pa.Column(str)),
    fpn_flag                                            = CS(check=pa.Column(str)),
    bm_unit_name                                        = CS(check=pa.Column(str)),
    lead_party_id                                       = CS(check=pa.Column(str)),
    demand_capacity                                     = CS(check=pa.Column(float)),
    generation_capacity                                 = CS(check=pa.Column(float)),
    production_or_consumption_flag                      = CS(check=pa.Column(str)),
    transmission_loss_factor                            = CS(check=pa.Column(float)),
    working_day_credit_assessment_import_capability     = CS(check=pa.Column(str), keep=False),
    non_working_day_credit_assessment_import_capability = CS(check=pa.Column(str), keep=False),
    working_day_credit_assessment_export_capability     = CS(check=pa.Column(str), keep=False),
    non_working_day_credit_assessment_export_capability = CS(check=pa.Column(str), keep=False),
    credit_qualifying_status                            = CS(check=pa.Column(str), keep=False),
    demand_in_production_flag                           = CS(check=pa.Column(str)),
    gsp_group_id                                        = CS(check=pa.Column(str, nullable=True), keep=False),
    gsp_group_name                                      = CS(check=pa.Column(str, nullable=True)),
    interconnector_id                                   = CS(check=pa.Column(str, nullable=True)),
)
# fmt: on
