# -*- coding: utf-8 -*-
from odoo import models, fields, api


class MigrationDashboard(models.Model):
    _name = 'migration.dashboard'
    _description = 'Migration Dashboard'

    name = fields.Char(required=True, default='Migration Dashboard')
    config_id = fields.Many2one('database.config', ondelete='cascade')

    total_tables = fields.Integer()
    ready_tables = fields.Integer()
    mapped_tables = fields.Integer()
    migrated_tables = fields.Integer()
    failed_tables = fields.Integer()

    total_errors = fields.Integer()
    retry_errors = fields.Integer()

    progress = fields.Float()

    @api.depends()
    def action_refresh(self):
        for rec in self:
            tables = self.env['table.config'].search([('config_id', '=', rec.config_id.id)])
            migrations = self.env['data.migration'].search([('config_id', '=', rec.config_id.id)])
            errors = self.env['migration.error'].search([('migration_id.config_id', '=', rec.config_id.id)])

            rec.total_tables = len(tables)
            rec.ready_tables = len(tables.filtered(lambda t: t.can_migrate))
            rec.mapped_tables = len(tables.filtered(lambda t: t.model_mapping_state == 'mapped' and t.field_mapping_state in ('mapped', 'partial')))
            rec.migrated_tables = sum(m.table_done_count for m in migrations)
            rec.failed_tables = len(set(errors.mapped('table_config_id').ids))
            rec.total_errors = len(errors)
            rec.retry_errors = len(errors.filtered(lambda e: e.state == 'retry'))

            rec.progress = (float(rec.migrated_tables) / rec.total_tables * 100.0) if rec.total_tables else 0.0