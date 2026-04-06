# -*- coding: utf-8 -*-
from odoo import models, fields


class MigrationMap(models.Model):
    _name = 'migration.map'
    _description = 'Migration ID Map'
    _order = 'model, source_id'

    migration_id = fields.Many2one('data.migration', required=True, ondelete='cascade')
    model = fields.Char(required=True, index=True)
    source_id = fields.Integer(required=True, index=True)
    dist_id = fields.Integer(index=True)

    _sql_constraints = [
        ('migration_map_unique', 'unique(migration_id, model, source_id)', 'Source ID map must be unique per migration and model.'),
    ]