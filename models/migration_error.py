# -*- coding: utf-8 -*-
from odoo import models, fields


class MigrationError(models.Model):
    _name = 'migration.error'
    _description = 'Migration Error Log'
    _order = 'id desc'

    migration_id = fields.Many2one('data.migration', required=True, ondelete='cascade')
    table_config_id = fields.Many2one('table.config', string='Table')
    source_model = fields.Char()
    source_table = fields.Char()
    message = fields.Text(required=True)
    payload = fields.Text()
    state = fields.Selection([
        ('new', 'New'),
        ('retry', 'Retry'),
        ('done', 'Done'),
        ('ignored', 'Ignored'),
    ], default='new')

    def action_mark_retry(self):
        for rec in self:
            rec.state = 'retry'

    def action_mark_done(self):
        for rec in self:
            rec.state = 'done'

    def action_ignore(self):
        for rec in self:
            rec.state = 'ignored'