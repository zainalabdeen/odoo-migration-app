# -*- coding: utf-8 -*-
from odoo import models, fields


class DbColumnConfig(models.Model):
    _name = 'column.config'
    _description = 'Field Mapping'
    _order = 'sequence, id'

    sequence = fields.Integer(default=10)

    table_id = fields.Many2one('table.config', string='Table', required=True, ondelete='cascade')

    source_col = fields.Char(string='Source Field', required=True, index=True)
    dist_col = fields.Char(string='Destination Field', required=True, index=True)
    migration_type = fields.Selection([('normal', 'Normal'),('m2m', 'Many2Many'),('attachment','Attachment'),
            ('method', 'Method'),('special', 'Special'),('skip','Skip')], default='normal', required=True)

    source_field_description = fields.Char(string='Source Label')
    dist_field_description = fields.Char(string='Destination Label')

    source_ttype = fields.Char(string='Source Type')
    dist_ttype = fields.Char(string='Destination Type')

    source_relation = fields.Char(string='Source Relation')
    dist_relation = fields.Char(string='Destination Relation')

    source_required = fields.Boolean(string='Source Required')
    dist_required = fields.Boolean(string='Destination Required')

    source_store = fields.Boolean(string='Source Stored')
    dist_store = fields.Boolean(string='Destination Stored')

    source_exists = fields.Boolean(default=True)
    dist_exists = fields.Boolean(default=False)

    migrate = fields.Boolean(
        string='Migrate',
        default=True,
        help='If disabled, this field is excluded from migration.'
    )

    selection = fields.Boolean(string='Map ID To Selection')
    is_src_translate = fields.Boolean(string='Src Translation JSONB')
    is_dist_translate = fields.Boolean(string='dist Translation JSONB')
    analytic_distribution = fields.Boolean(string='Analytic Distribution JSONB')

    mapping_status = fields.Selection([
        ('draft', 'Draft'),
        ('mapped', 'Mapped'),
        ('not_found', 'Not Found'),
        ('skip', 'Skipped'),
    ], default='draft')

    match_score = fields.Float(string='Match Score')
    notes = fields.Char()

    source_m2m_relation_table = fields.Char(string="Source M2M Relation Table")
    dist_m2m_relation_table = fields.Char(string="Dest M2M Relation Table")

    src_db_column = fields.Char(string="Src DB Column Name")
    dist_db_column = fields.Char(string="dist DB Column Name")
    is_dist_company_dependent = fields.Boolean(string='Dist Company Dependent')

    _sql_constraints = [
        ('column_config_unique', 'unique(table_id, source_col)', 'Source field must be unique per table.'),
    ]

    def get_m2m_fields(self):
        return self.column_ids.filtered(
            lambda c: c.source_ttype == 'many2many' and c.migrate
        )