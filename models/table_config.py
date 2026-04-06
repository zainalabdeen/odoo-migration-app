# -*- coding: utf-8 -*-
import logging

from odoo import models, fields, api, _
from odoo.exceptions import UserError

from .migration_utils import (
    MODEL_RENAMES_11_TO_19,
    find_best_field_match,
    safe_table_name_from_model,
)

_logger = logging.getLogger(__name__)


class AdditionalColumnConfig(models.Model):
    _name = 'additional.column.config'
    _description = 'Additional Destination Column'

    table_id = fields.Many2one('table.config', string='Table', required=True, ondelete='cascade')
    dist_col = fields.Char(string='Column', required=True)
    field_description = fields.Char(string='Label')
    required = fields.Boolean(string='Required')
    ttype = fields.Char(string='Type')
    migrate = fields.Boolean(string='Migrate',default=True,help='If disabled, this field is excluded from migration.')
    default_value = fields.Char(string='Default Value')


class TableConfig(models.Model):
    _name = 'table.config'
    _description = 'Database Tables and Model Mapping'
    _order = 'sequence, source_model, source_table'

    sequence = fields.Integer(default=10)

    name = fields.Char(string='Name', required=True)
    description = fields.Char('Description')
    config_id = fields.Many2one('database.config', string='Connection Config', required=True, ondelete='cascade')

    source_company_id = fields.Char('Source Company ID')
    dist_company_id = fields.Char('Destination Company ID')

    source_model = fields.Char(string='Source Model', index=True)
    dist_model = fields.Char(string='Destination Model', index=True)

    source_table = fields.Char(string='Source Table', required=True, index=True)
    dist_table = fields.Char(string='Destination Table', required=True, index=True)

    source_model_exists = fields.Boolean(default=True)
    dist_model_exists = fields.Boolean(default=False)
    source_table_exists = fields.Boolean(default=True)
    dist_table_exists = fields.Boolean(default=False)

    app_ids = fields.Many2many(
        'migration.app',
        'table_config_migration_app_rel',
        'table_config_id',
        'app_id',
        string='Apps'
    )
    primary_app_id = fields.Many2one('migration.app', string='Primary Defining App')

    model_mapping_state = fields.Selection([
        ('draft', 'Draft'),
        ('mapped', 'Mapped'),
        ('not_found', 'Not Found'),
    ], default='draft')

    field_mapping_state = fields.Selection([
        ('draft', 'Draft'),
        ('mapped', 'Mapped'),
        ('partial', 'Partial'),
        ('not_found', 'Not Found'),
    ], default='draft')

    remove_null = fields.Boolean(string="Keep Null Constraint ??")
    no_id = fields.Boolean(string="No ID Column?")

    can_migrate = fields.Boolean(compute='_compute_can_migrate', store=True)
    missing_field_count = fields.Integer(compute='_compute_can_migrate', store=True)
    dependency_level = fields.Integer(compute='_compute_dependency_level', store=True)

    column_ids = fields.One2many('column.config', 'table_id', string='Field Mapping', required=True)
    additional_column_ids = fields.One2many('additional.column.config', 'table_id', string='Additional Columns')

    _sql_constraints = [
        ('table_config_unique', 'unique(config_id, source_model, source_table)', 'Source model/table must be unique per configuration.'),
    ]

    @api.model
    def default_get(self, fields_list):
        res = super(TableConfig, self).default_get(fields_list)
        if 'column_ids' in fields_list or not fields_list:
            res.setdefault('column_ids', [(0, 0, {'source_col': 'id', 'dist_col': 'id', 'mapping_status': 'mapped', 'dist_exists': True, 'source_exists': True})])
        return res

    @api.depends('dist_model_exists', 'dist_table_exists', 'column_ids.dist_exists', 'column_ids.migrate')
    def _compute_can_migrate(self):
        for rec in self:
            missing = rec.column_ids.filtered(lambda c: c.migrate and not c.dist_exists)
            rec.missing_field_count = len(missing)
            rec.can_migrate = rec.dist_model_exists and rec.dist_table_exists and not missing

    @api.depends('column_ids.source_relation', 'column_ids.migrate')
    def _compute_dependency_level(self):
        for rec in self:
            rec.dependency_level = len(rec.column_ids.filtered(lambda c: c.migrate and c.source_relation))

    def _connect_source(self):
        self.ensure_one()
        return self.config_id._do_connection(
            self.config_id.source_host, self.config_id.source_db,
            self.config_id.source_user, self.config_id.source_password,
            self.config_id.source_port
        )

    def _connect_dest(self):
        self.ensure_one()
        return self.config_id._do_connection(
            self.config_id.dist_host, self.config_id.dist_db,
            self.config_id.dist_user, self.config_id.dist_password,
            self.config_id.dist_port
        )

    def _get_model_fields_metadata(self, cursor, model_name):
        cursor.execute("""
            SELECT
                f.name,
                f.field_description,
                f.ttype,
                f.relation,
                f.required,
                f.store,
                f.translate,
                f.relation_table
            FROM ir_model_fields f
            JOIN ir_model m ON m.id = f.model_id
            WHERE m.model = %s AND f.store = TRUE AND f.ttype != 'one2many'
            ORDER BY f.name
        """, (model_name,))
        return self.config_id._fetchall_dict(cursor)

    def _get_table_column_names(self, cursor, table_name):
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        return [row[0] for row in cursor.fetchall()]

    def apply_model_rename(self):
        for rec in self:
            if rec.source_model in MODEL_RENAMES_11_TO_19:
                rec.dist_model = MODEL_RENAMES_11_TO_19[rec.source_model]
                rec.dist_table = safe_table_name_from_model(rec.dist_model)

    def action_refresh_source_fields(self):
        for rec in self:
            if not rec.source_model:
                raise UserError(_("Source model is required on %s") % rec.name)

            src_cur, src_conn = rec._connect_source()
            try:
                field_meta = rec._get_model_fields_metadata(src_cur, rec.source_model)
                db_columns = rec._get_table_column_names(src_cur,rec.source_table)
                existing_by_name = {c.source_col: c for c in rec.column_ids}

                if 'id' not in existing_by_name:
                    self.env['column.config'].create({
                        'table_id': rec.id,
                        'source_col': 'id',
                        'dist_col': 'id',
                        'source_field_description': 'ID',
                        'source_ttype': 'integer',
                        'dist_ttype': 'integer',
                        'source_exists': True,
                        'dist_exists': True,
                        'mapping_status': 'mapped',
                        'sequence': 1,
                    })

                seq = 10
                
                for fld in field_meta:
                    vals = {
                        'table_id': rec.id,
                        'source_col': fld['name'],
                        'dist_col': fld['name'],
                        'source_field_description': fld.get('field_description'),
                        'source_ttype': fld.get('ttype'),
                        'source_relation': fld.get('relation'),
                        'source_m2m_relation_table': fld.get('relation_table'),
                        'source_required': bool(fld.get('required')),
                        'source_store': bool(fld.get('store')),
                        'is_src_translate': bool(fld.get('translate')),
                        'source_exists': True,
                        'sequence': seq,
                        'src_db_column': fld['name'] if fld['name'] in db_columns else False,
                    }
                    
                    if fld.get('ttype') == 'many2many':
                        vals['migration_type'] = 'm2m'
                    elif fld.get('ttype') == 'binary':
                        if vals['src_db_column']:
                            vals['migration_type'] = 'normal'
                        else:
                            vals['migration_type'] = 'attachment'
                    seq += 10

                    if fld['name'] in existing_by_name:
                        existing_by_name[fld['name']].write(vals)
                    else:
                        self.env['column.config'].create(vals)

                rec.field_mapping_state = 'draft'
            finally:
                src_cur.close()
                src_conn.close()

    def action_map_destination_model_table(self):
        for rec in self:
            rec.apply_model_rename()

            dst_cur, dst_conn = rec._connect_dest()
            try:
                dst_cur.execute("""
                    SELECT model
                    FROM ir_model
                    WHERE model = %s
                    LIMIT 1
                """, (rec.dist_model or rec.source_model,))
                model_row = dst_cur.fetchone()

                rec.dist_model_exists = bool(model_row)
                if not rec.dist_model:
                    rec.dist_model = rec.source_model

                if not rec.dist_table:
                    rec.dist_table = safe_table_name_from_model(rec.dist_model or rec.source_model)

                dst_cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = %s
                    )
                """, (rec.dist_table,))
                table_exists = dst_cur.fetchone()[0]
                rec.dist_table_exists = table_exists

                rec.model_mapping_state = 'mapped' if rec.dist_model_exists and rec.dist_table_exists else 'not_found'
            finally:
                dst_cur.close()
                dst_conn.close()

    def action_map_destination_fields(self):
        for rec in self:
            rec.action_map_destination_model_table()
            if not rec.dist_model_exists:
                rec.field_mapping_state = 'not_found'
                continue

            dst_cur, dst_conn = rec._connect_dest()
            try:
                dest_field_meta = rec._get_model_fields_metadata(dst_cur, rec.dist_model)
                db_columns = rec._get_table_column_names(dst_cur,rec.dist_table)
                dest_by_name = {f['name']: f for f in dest_field_meta}
                mapped_count = 0

                for col in rec.column_ids:
                    dest = dest_by_name.get(col.dist_col or col.source_col)
                    if dest:
                        col.write({
                            'dist_col': dest['name'],
                            'dist_field_description': dest.get('field_description'),
                            'dist_ttype': dest.get('ttype'),
                            'dist_relation': dest.get('relation'),
                            'dist_m2m_relation_table': dest.get('relation_table'),
                            'dist_required': bool(dest.get('required')),
                            'dist_store': bool(dest.get('store')),
                            'is_dist_translate': bool(dest.get('translate')),
                            'dist_exists': True,
                            'mapping_status': 'mapped',
                            'match_score': 100.0 if col.source_col == dest['name'] else col.match_score,
                            'dist_db_column': dest['name'] if dest['name'] in db_columns else False,
                        })
                        mapped_count += 1
                    else:
                        col.write({
                            'dist_exists': False,
                            'mapping_status': 'not_found',
                            'match_score': 0.0,
                        })

                total = len(rec.column_ids)
                if mapped_count == 0:
                    rec.field_mapping_state = 'not_found'
                elif mapped_count == total:
                    rec.field_mapping_state = 'mapped'
                else:
                    rec.field_mapping_state = 'partial'
            finally:
                dst_cur.close()
                dst_conn.close()

    def _map_relation_fields_first(self, dest_fields):
        self.ensure_one()
        mapped = 0
        for col in self.column_ids.filtered(lambda c: c.migrate and not c.dist_exists and c.source_relation):
            candidates = [f for f in dest_fields if f.get('relation') == col.source_relation]
            if len(candidates) == 1:
                target = candidates[0]
                col.write({
                    'dist_col': target['name'],
                    'dist_field_description': target.get('field_description'),
                    'dist_ttype': target.get('ttype'),
                    'dist_relation': target.get('relation'),
                    'dist_required': bool(target.get('required')),
                    'dist_store': bool(target.get('store')),
                    'dist_exists': True,
                    'mapping_status': 'mapped',
                    'match_score': 90.0,
                    'notes': 'Relation-based auto mapping',
                })
                mapped += 1
        return mapped

    def action_smart_map_fields_v2(self):
        for rec in self:
            rec.action_map_destination_model_table()
            if not rec.dist_model_exists:
                continue

            dst_cur, dst_conn = rec._connect_dest()
            try:
                dest_fields = rec._get_model_fields_metadata(dst_cur, rec.dist_model)
                rec._map_relation_fields_first(dest_fields)

                mapped_count = 0
                for col in rec.column_ids.filtered(lambda c: c.migrate):
                    if col.dist_exists:
                        mapped_count += 1
                        continue

                    source_field = {
                        'name': col.source_col,
                        'ttype': col.source_ttype,
                        'relation': col.source_relation,
                    }
                    match, score = find_best_field_match(source_field, dest_fields, threshold=55.0)
                    if match:
                        col.write({
                            'dist_col': match['name'],
                            'dist_field_description': match.get('field_description'),
                            'dist_ttype': match.get('ttype'),
                            'dist_relation': match.get('relation'),
                            'dist_required': bool(match.get('required')),
                            'dist_store': bool(match.get('store')),
                            'dist_exists': True,
                            'mapping_status': 'mapped',
                            'match_score': score,
                            'notes': 'AI smart mapping',
                        })
                        mapped_count += 1
                    else:
                        col.write({
                            'dist_exists': False,
                            'mapping_status': 'not_found',
                            'match_score': score,
                            'notes': 'No confident match',
                        })

                total = len(rec.column_ids.filtered(lambda c: c.migrate))
                if mapped_count == 0:
                    rec.field_mapping_state = 'not_found'
                elif mapped_count == total:
                    rec.field_mapping_state = 'mapped'
                else:
                    rec.field_mapping_state = 'partial'
            finally:
                dst_cur.close()
                dst_conn.close()

    @api.onchange('source_model')
    def _onchange_source_model(self):
        if self.source_model and not self.source_table:
            self.source_table = safe_table_name_from_model(self.source_model)
        if self.source_model and not self.dist_model:
            self.dist_model = self.source_model
        if self.dist_model and not self.dist_table:
            self.dist_table = safe_table_name_from_model(self.dist_model)