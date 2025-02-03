import pandera as pa

from ma.utils.pandas import ColumnSchema as CS

# fmt: off
schema_bmus_on_load = dict(
    national_grid_bm_unit                               = CS(check=pa.Column(str)),
    elexon_bm_unit                                      = CS(check=pa.Column(str, nullable=True)),
    eic                                                 = CS(check=pa.Column(str, nullable=True)),
    fuel_type                                           = CS(check=pa.Column(str, nullable=True)),
    lead_party_name                                     = CS(check=pa.Column(str, nullable=True)),
    bm_unit_type                                        = CS(check=pa.Column(str, nullable=True)),
    fpn_flag                                            = CS(check=pa.Column(str, nullable=True)),
    bm_unit_name                                        = CS(check=pa.Column(str, nullable=True)),
    lead_party_id                                       = CS(check=pa.Column(str, nullable=True)),
    demand_capacity                                     = CS(check=pa.Column(float, nullable=True)),
    generation_capacity                                 = CS(check=pa.Column(float, nullable=True)),
    production_or_consumption_flag                      = CS(check=pa.Column(str, nullable=True)),
    transmission_loss_factor                            = CS(check=pa.Column(float, nullable=True)),
    working_day_credit_assessment_import_capability     = CS(check=pa.Column(str, nullable=True), keep=False),
    non_working_day_credit_assessment_import_capability = CS(check=pa.Column(str, nullable=True), keep=False),
    working_day_credit_assessment_export_capability     = CS(check=pa.Column(str, nullable=True), keep=False),
    non_working_day_credit_assessment_export_capability = CS(check=pa.Column(str, nullable=True), keep=False),
    credit_qualifying_status                            = CS(check=pa.Column(str, nullable=True), keep=False),
    demand_in_production_flag                           = CS(check=pa.Column(str, nullable=True)),
    gsp_group_id                                        = CS(check=pa.Column(str, nullable=True), keep=False),
    gsp_group_name                                      = CS(check=pa.Column(str, nullable=True)),
    interconnector_id                                   = CS(check=pa.Column(str, nullable=True)),
)
# fmt: on
