# -*- coding: utf-8 -*-
from odoo import models, fields, api


class MigrationApp(models.Model):
    _name = 'migration.app'
    _description = 'Migration Application'
    _order = 'name'

    name = fields.Char(required=True, index=True)
    shortdesc = fields.Char(string='Description')

    config_id = fields.Many2one('database.config', required=True, ondelete='cascade')

    source_installed = fields.Boolean(string='Installed In Source')
    dist_installed = fields.Boolean(string='Installed In Destination')
    exists_in_dist = fields.Boolean(string='Exists In Destination', compute='_compute_exists_in_dist', store=True)

    table_config_ids = fields.Many2many(
        'table.config',
        'table_config_migration_app_rel',
        'app_id',
        'table_config_id',
        string='Tables'
    )

    is_primary_for_any_model = fields.Boolean(string='Primary For Any Model', default=False)
    model_count = fields.Integer(string='Models', compute='_compute_model_count')

    _sql_constraints = [
        ('migration_app_unique', 'unique(name, config_id)', 'Module must be unique per configuration.'),
    ]

    @api.depends('dist_installed')
    def _compute_exists_in_dist(self):
        for rec in self:
            rec.exists_in_dist = bool(rec.dist_installed)

    @api.depends('table_config_ids')
    def _compute_model_count(self):
        for rec in self:
            rec.model_count = len(rec.table_config_ids)