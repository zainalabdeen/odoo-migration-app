# -*- coding: utf-8 -*-
import logging
import psycopg2

from odoo import models, fields, api, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class DatabaseConfiguration(models.Model):
    _name = 'database.config'
    _description = 'Database Configuration'

    name = fields.Char(string='Name', required=True)
    description = fields.Char(string='Description')

    source_host = fields.Char(string='Source Host', required=True)
    source_db = fields.Char(string='Source Database', required=True)
    source_user = fields.Char(string='Source User', required=True)
    source_password = fields.Char(string='Source Password', required=True)
    source_port = fields.Char(string='Source Port', required=True, default='5432')

    dist_host = fields.Char(string='Destination Host', required=True)
    dist_db = fields.Char(string='Destination Database', required=True)
    dist_user = fields.Char(string='Destination User', required=True)
    dist_password = fields.Char(string='Destination Password', required=True)
    dist_port = fields.Char(string='Destination Port', required=True, default='5432')

    app_ids = fields.One2many('migration.app', 'config_id', string='Apps')
    table_config_ids = fields.One2many('table.config', 'config_id', string='Table Configs')

    dashboard_id = fields.Many2one('migration.dashboard', string='Dashboard')

    def _do_connection(self, host, db, user, password, port):
        try:
            conn = psycopg2.connect(
                host=host,
                database=db,
                user=user,
                password=password,
                port=port,
            )
            return conn.cursor(), conn
        except psycopg2.Error as e:
            raise UserError(_("Database connection failed:\n%s") % str(e))

    def _fetchall_dict(self, cursor):
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def action_test_source_connection(self):
        for rec in self:
            cursor, conn = rec._do_connection(
                rec.source_host, rec.source_db, rec.source_user, rec.source_password, rec.source_port
            )
            try:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                raise UserError(_("Source connection successful:\n%s") % version)
            finally:
                cursor.close()
                conn.close()

    def action_test_destination_connection(self):
        for rec in self:
            cursor, conn = rec._do_connection(
                rec.dist_host, rec.dist_db, rec.dist_user, rec.dist_password, rec.dist_port
            )
            try:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                raise UserError(_("Destination connection successful:\n%s") % version)
            finally:
                cursor.close()
                conn.close()

    def _get_installed_modules(self, cursor):
        cursor.execute("""
            SELECT name, shortdesc, state
            FROM ir_module_module
            WHERE state = 'installed'
            ORDER BY name
        """)
        return self._fetchall_dict(cursor)

    def action_discover_apps(self):
        for rec in self:
            src_cur, src_conn = rec._do_connection(
                rec.source_host, rec.source_db, rec.source_user, rec.source_password, rec.source_port
            )
            dst_cur, dst_conn = rec._do_connection(
                rec.dist_host, rec.dist_db, rec.dist_user, rec.dist_password, rec.dist_port
            )
            try:
                source_modules = rec._get_installed_modules(src_cur)
                dest_modules = rec._get_installed_modules(dst_cur)
                dest_names = {m['name'] for m in dest_modules}

                for module in source_modules:
                    existing = self.env['migration.app'].search([
                        ('config_id', '=', rec.id),
                        ('name', '=', module['name']),
                    ], limit=1)

                    vals = {
                        'config_id': rec.id,
                        'name': module['name'],
                        'shortdesc': module.get('shortdesc'),
                        'source_installed': True,
                        'dist_installed': module['name'] in dest_names,
                    }
                    if existing:
                        existing.write(vals)
                    else:
                        self.env['migration.app'].create(vals)
            finally:
                src_cur.close()
                src_conn.close()
                dst_cur.close()
                dst_conn.close()

    def action_open_discover_models_wizard(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('Discover Source Models'),
            'res_model': 'discover.models.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_config_id': self.id,
            }
        }

    def action_map_all_destination_models(self):
        for rec in self:
            rec.table_config_ids.action_map_destination_model_table()

    def action_map_all_destination_fields(self):
        for rec in self:
            rec.table_config_ids.action_map_destination_fields()

    def action_smart_map_all_fields(self):
        for rec in self:
            rec.table_config_ids.action_smart_map_fields_v2()

    def action_refresh_all_source_fields(self):
        for rec in self:
            rec.table_config_ids.action_refresh_source_fields()

    def action_recompute_dashboard(self):
        for rec in self:
            dashboard = rec.dashboard_id
            if not dashboard:
                dashboard = self.env['migration.dashboard'].create({
                    'name': rec.name,
                    'config_id': rec.id,
                })
                rec.dashboard_id = dashboard.id
            dashboard.action_refresh()