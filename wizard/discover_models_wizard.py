# -*- coding: utf-8 -*-
from odoo import models, fields, _
from odoo.exceptions import UserError

from odoo.addons.advanced_odoo_migration.models.migration_utils import safe_table_name_from_model


class DiscoverModelsWizard(models.TransientModel):
    _name = 'discover.models.wizard'
    _description = 'Discover Source Models Wizard'

    config_id = fields.Many2one('database.config', required=True)
    app_ids = fields.Many2many('migration.app', string='Apps Filter')
    clear_existing = fields.Boolean(default=False)
    include_transient = fields.Boolean(default=False)

    def action_discover(self):
        self.ensure_one()

        config = self.config_id
        src_cur, src_conn = config._do_connection(
            config.source_host, config.source_db,
            config.source_user, config.source_password,
            config.source_port
        )

        try:
            src_cur.execute("""
                SELECT
                    m.id,
                    m.model,
                    m.name,
                    m.transient,
                    m.state,
                    m.info,
                    -- all modules
                    COALESCE(string_agg(DISTINCT imd.module, ','), '') AS modules,

                    -- PRIMARY MODULE (first created XML record)
                    (
                        SELECT imd2.module
                        FROM ir_model_data imd2
                        JOIN ir_module_module mod2
                            ON mod2.name = imd2.module
                        AND mod2.state = 'installed'
                        WHERE imd2.model = 'ir.model'
                        AND imd2.res_id = m.id
                        ORDER BY imd2.id
                        LIMIT 1
                    ) AS primary_module
                FROM ir_model m
                    LEFT JOIN ir_model_data imd
                    ON imd.model = 'ir.model'
                    AND imd.res_id = m.id

                    LEFT JOIN ir_module_module mod
                        ON mod.name = imd.module
                    AND mod.state = 'installed'

                    WHERE m.model IS NOT NULL

                    GROUP BY m.id, m.model
                    ORDER BY m.model
            """)
            
            models_meta = config._fetchall_dict(src_cur)

            if self.clear_existing:
                config.table_config_ids.unlink()

            app_name_filter = set(self.app_ids.mapped('name')) if self.app_ids else set()

            for meta in models_meta:
                model_name = meta.get('model')
                if not model_name:
                    continue
                if not self.include_transient and meta.get('transient'):
                    continue

                modules_text = (meta.get('modules') or '').strip()
                module_names = [m.strip() for m in modules_text.split(',') if m.strip()]

                if app_name_filter and not set(module_names).intersection(app_name_filter):
                    continue

                source_table = safe_table_name_from_model(model_name)

                table_rec = self.env['table.config'].search([
                    ('config_id', '=', config.id),
                    ('source_model', '=', model_name),
                ], limit=1)

                vals = {
                    'name': meta.get('name') or model_name,
                    'description': meta.get('info') or model_name,
                    'config_id': config.id,
                    'source_model': model_name,
                    'dist_model': model_name,
                    'source_table': source_table,
                    'dist_table': source_table,
                    'source_model_exists': True,
                    'source_table_exists': True,
                }

                if table_rec:
                    table_rec.write(vals)
                else:
                    table_rec = self.env['table.config'].create(vals)

                app_records = self.env['migration.app']
                for idx, app_name in enumerate(module_names):
                    app = self.env['migration.app'].search([
                        ('config_id', '=', config.id),
                        ('name', '=', app_name),
                    ], limit=1)
                    if not app:
                        app = self.env['migration.app'].create({
                            'config_id': config.id,
                            'name': app_name,
                            'source_installed': True,
                            'dist_installed': False,
                        })
                    app_records |= app
                    if app.name == meta.get('primary_module',False):
                        table_rec.primary_app_id = app.id
                        app.is_primary_for_any_model = True

                if app_records:
                    table_rec.app_ids = [(6, 0, app_records.ids)]

                #TODO : check pass src_cur,src_conn as params instead of open new connection optional
                table_rec.action_refresh_source_fields()
        finally:
            src_cur.close()
            src_conn.close()

        return {'type': 'ir.actions.act_window_close'}