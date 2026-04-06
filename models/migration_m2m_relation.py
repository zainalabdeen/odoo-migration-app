from odoo import models, fields


class MigrationM2MRelation(models.Model):
    _name = 'migration.m2m.relation'
    _description = 'M2M Relation Migration Tracker'
    _order = 'src_relation_table'

    migration_id = fields.Many2one('data.migration', required=True, ondelete='cascade')

    src_relation_table = fields.Char(required=True, index=True)
    dist_relation_table = fields.Char(required=True, index=True)

    source_model = fields.Char()
    relation_model = fields.Char()  # target model of M2M

    state = fields.Selection([
        ('pending', 'Pending'),
        ('done', 'Done'),
        ('failed', 'Failed'),
    ], default='pending')

    note = fields.Text()

    _sql_constraints = [
        ('unique_relation_per_migration',
         'unique(migration_id, src_relation_table)',
         'Relation table already processed in this migration!')
    ]