# -*- coding: utf-8 -*-
import json
import logging

from psycopg2 import sql

from odoo import models, fields, api, _
from odoo.exceptions import UserError

from .migration_utils import (
    ACCOUNT_TYPE_MAP_11_TO_19,
    build_dependency_graph,
    topological_sort_dependency_graph,
    rows_to_copy_buffer,
    csv_bytes_from_rows,
)

_logger = logging.getLogger(__name__)


class DataMigration(models.Model):
    _name = 'data.migration'
    _description = 'Data Migration Main Screen'
    _order = 'id desc'

    name = fields.Char('Name', required=True)
    config_id = fields.Many2one('database.config', 'Config', required=True)
    table_ids = fields.Many2many('table.config', string="Tables", required=True)

    state = fields.Selection([
        ('draft', 'Draft'),
        ('validated', 'Validated'),
        ('running', 'Running'),
        ('done', 'Migration Completed'),
        ('failed', 'Failed'),
    ], readonly=True, index=True, copy=False, default='draft')

    constrain = fields.Boolean(string="Remove Constraint?")
    null = fields.Boolean(string="Remove Null Constraint?")

    constrain_state = fields.Selection([
        ('not', 'Not Removed'),
        ('removed', 'Removed'),
        ('placed', 'Placed')
    ], string="Constraint State", default='not')

    null_state = fields.Selection([
        ('not', 'Not Removed'),
        ('removed', 'Removed'),
        ('placed', 'Placed')
    ], string="Null State", default='not')

    constrain_file = fields.Binary(string="Constraint File")
    constrain_filename = fields.Char(string="Constraint Filename")
    null_file = fields.Binary(string="Null Constraint File")
    null_filename = fields.Char(string="Null Filename")

    date_from = fields.Date(string="Date From", help="Based on Create Date Only")
    date_to = fields.Date(string="Date To")

    table_done_count = fields.Integer(default=0)
    row_done_count = fields.Integer(default=0)
    row_failed_count = fields.Integer(default=0)

    error_ids = fields.One2many('migration.error', 'migration_id', string='Errors')
    map_ids = fields.One2many('migration.map', 'migration_id', string='ID Maps')

    dashboard_id = fields.Many2one('migration.dashboard', string='Dashboard')
    m2m_relation_ids = fields.One2many('migration.m2m.relation','migration_id',string='M2M Relations')

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

    def _ensure_dashboard(self):
        self.ensure_one()
        dashboard = self.dashboard_id or self.config_id.dashboard_id
        if not dashboard:
            dashboard = self.env['migration.dashboard'].create({
                'name': self.name,
                'config_id': self.config_id.id,
            })
            self.dashboard_id = dashboard.id
            self.config_id.dashboard_id = dashboard.id
        return dashboard

    def _log_error(self, table, message, payload=None):
        self.ensure_one()
        values = {
            'migration_id': self.id,
            'table_config_id': table.id if table else False,
            'source_model': table.source_model if table else False,
            'source_table': table.source_table if table else False,
            'message': message,
            'payload': json.dumps(payload, ensure_ascii=False, default=str) if payload else False,
        }
        self.env['migration.error'].create(values)

    def _get_ordered_tables(self):
        self.ensure_one()
        graph = build_dependency_graph(self.table_ids.filtered(lambda t: t.can_migrate))
        ordered_models = topological_sort_dependency_graph(graph)
        ordered = self.env['table.config']
        for model_name in ordered_models:
            ordered |= self.table_ids.filtered(lambda t: t.source_model == model_name)
        ordered |= self.table_ids.filtered(lambda t: t not in ordered)
        return ordered

    def action_sort_dependencies(self):
        for rec in self:
            ordered = rec._get_ordered_tables()
            rec.table_ids = [(6, 0, ordered.ids)]

    def action_validate(self):
        for rec in self:
            messages = []
            for tbl in rec.table_ids:
                if not tbl.dist_model_exists:
                    messages.append("%s: destination model not found" % tbl.source_model)
                if not tbl.dist_table_exists:
                    messages.append("%s: destination table not found" % tbl.dist_table)
                missing = tbl.column_ids.filtered(lambda c: c.migrate and not c.dist_exists)
                for col in missing:
                    messages.append("%s.%s: destination field not found" % (tbl.source_model, col.source_col))
            if messages:
                raise UserError(_("Validation failed:\n%s") % '\n'.join(messages))
            rec.state = 'validated'

    def _prepare_select_statement(self, table):
        select_parts = []
        migrate_columns = table.column_ids.filtered(lambda c: c.migrate and c.source_exists and c.migration_type == 'normal').sorted(key=lambda c: c.sequence or 0)

        if not migrate_columns:
            raise UserError(_("No source fields selected for migration in %s") % table.source_table)

        for col in migrate_columns:
            if col.analytic_distribution:
                expr = "CAST(jsonb_build_object(%s, 100.0) AS VARCHAR) AS %s" % (col.source_col, col.source_col)
            elif col.is_dist_translate:
                expr = "CAST(jsonb_build_object('en_US', %s, 'ar_001', %s) AS VARCHAR) AS %s" % (
                    col.source_col, col.source_col, col.source_col
                )
            else:
                expr = col.source_col
            select_parts.append(expr)

        statement = "SELECT %s FROM %s" % (', '.join(select_parts), table.source_table)

        where_parts = []
        if table.source_table == 'res_partner':
            where_parts.append("name != 'Administrator'")
        if table.source_company_id:
            where_parts.append("company_id = %s" % table.source_company_id)
        if self.date_from and self.date_to:
            where_parts.append("create_date BETWEEN '%s' AND '%s'" % (self.date_from, self.date_to))
        elif self.date_from:
            where_parts.append("create_date >= '%s'" % self.date_from)
        elif self.date_to:
            where_parts.append("create_date <= '%s'" % self.date_to)

        if where_parts:
            statement += " WHERE " + " AND ".join(where_parts)

        return statement, migrate_columns

    def _prepare_insert_statement(self, table, mapped_columns):
        dest_columns = []
        placeholder_count = 0

        for col in mapped_columns:
            if col.dist_exists:
                dest_columns.append(col.dist_col)
                placeholder_count += 1

        for add_col in table.additional_column_ids:
            dest_columns.append(add_col.dist_col)

        if table.dist_company_id:
            dest_columns.append('company_id')

        if not dest_columns:
            raise UserError(_("No destination fields selected for migration in %s") % table.dist_table)

        return dest_columns

    def _transform_special_rows(self, table, rows, migrate_columns):
        transformed = []
        src_names = [c.source_col for c in migrate_columns]

        for row in rows:
            data = dict(zip(src_names, row))

            # account.account type mapping
            if table.source_table in ('account_account', 'account_group'):
                if 'internal_type' in data and data['internal_type'] in ACCOUNT_TYPE_MAP_11_TO_19:
                    data['internal_type'] = ACCOUNT_TYPE_MAP_11_TO_19[data['internal_type']]
                if 'user_type_id' in data and data['user_type_id'] in ACCOUNT_TYPE_MAP_11_TO_19:
                    data['user_type_id'] = ACCOUNT_TYPE_MAP_11_TO_19[data['user_type_id']]

            # Odoo 11 invoice -> Odoo 19 move field rename support
            if table.source_model == 'account.invoice':
                if 'type' in data and 'move_type' not in data:
                    data['move_type'] = data.pop('type')
                if 'date_invoice' in data and 'invoice_date' not in data:
                    data['invoice_date'] = data.pop('date_invoice')
                if 'date_due' in data and 'invoice_date_due' not in data:
                    data['invoice_date_due'] = data.pop('date_due')
                if 'comment' in data and 'narration' not in data:
                    data['narration'] = data.pop('comment')

            transformed.append(data)
        return transformed

    def _map_fk_value(self, model_name, source_id):
        self.ensure_one()
        if not source_id:
            return None
        mapping = self.env['migration.map'].search([
            ('migration_id', '=', self.id),
            ('model', '=', model_name),
            ('source_id', '=', int(source_id)),
        ], limit=1)
        return mapping.dist_id if mapping else None

    def _remember_id_map(self, model_name, source_id, dist_id):
        self.ensure_one()
        if source_id is None or dist_id is None:
            return
        existing = self.env['migration.map'].search([
            ('migration_id', '=', self.id),
            ('model', '=', model_name),
            ('source_id', '=', int(source_id)),
        ], limit=1)
        vals = {
            'migration_id': self.id,
            'model': model_name,
            'source_id': int(source_id),
            'dist_id': int(dist_id),
        }
        if existing:
            existing.write(vals)
        else:
            self.env['migration.map'].create(vals)

    def _resolve_row_for_destination(self, table, row_data, mapped_columns):
        out = []
        source_id = row_data.get('id')

        for col in mapped_columns:
            if not col.dist_exists:
                continue

            value = row_data.get(col.source_col)

            if col.selection and isinstance(value, int) and table.source_table in ('account_account', 'account_group'):
                value = ACCOUNT_TYPE_MAP_11_TO_19.get(value, value)

            if col.source_relation and col.dist_relation:
                mapped_id = self._map_fk_value(col.source_relation, value)
                #TODO : Check if need action of not-exist
                if mapped_id is not None:
                    value = mapped_id
                

            out.append(value)

        for add_col in table.additional_column_ids:
            out.append(add_col.default_value or None)

        if table.dist_company_id:
            out.append(int(table.dist_company_id))

        return source_id, tuple(out)

    def _bulk_insert(self, dest_cursor, dest_conn, table_name, dest_columns, rows):
        if not rows:
            return 0

        inserted = 0
        try:
            buffer_ = rows_to_copy_buffer(rows)
            columns_sql = sql.SQL(',').join(map(sql.Identifier, dest_columns))
            query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT text)").format(
                sql.Identifier(table_name),
                columns_sql,
            )
            dest_cursor.copy_expert(query.as_string(dest_conn), buffer_)
            dest_conn.commit()
            inserted = len(rows)
            return inserted
        except Exception as e:
            dest_conn.rollback()

        placeholders = ', '.join(['%s'] * len(dest_columns))
        insert_query = 'INSERT INTO %s (%s) VALUES (%s)' % (
            table_name,
            ', '.join(dest_columns),
            placeholders
        )
        if 'id' in dest_columns:
            insert_query += ' ON CONFLICT (id) DO NOTHING'

        for row in rows:
            try:
                dest_cursor.execute(insert_query, row)
                inserted += 1
            except Exception as e:
                dest_conn.rollback()
                _logger.exception("Row insert failed on %s", table_name)
                raise e
        dest_conn.commit()
        return inserted

    def _update_sequence(self, dest_cursor, dest_conn, table_name):
        try:
            dest_cursor.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table_name,))
            seq = dest_cursor.fetchone()[0]
            if seq:
                dest_cursor.execute(
                    'SELECT setval(%s, COALESCE((SELECT MAX(id) FROM %s), 1), true)' % ('%s', table_name),
                    (seq,)
                )
                dest_conn.commit()
        except Exception:
            dest_conn.rollback()

    def source_data_fetching(self, source_cursor, statement):
        source_cursor.execute(statement)
        return source_cursor.fetchall()

    def action_remove_constrain(self):
        for rec in self:
            dst_cur, dst_conn = rec._connect_dest()
            all_rows = []
            try:
                for tbl in rec.table_ids:
                    dst_cur.execute("""
                        SELECT
                            tc.constraint_name,
                            tc.table_name,
                            kcu.column_name,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                         AND ccu.table_schema = tc.table_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                          AND tc.table_name = %s
                    """, (tbl.dist_table,))
                    rows = dst_cur.fetchall()
                    all_rows.extend(rows)
                    for row in rows:
                        dst_cur.execute('ALTER TABLE "%s" DROP CONSTRAINT "%s"' % (row[1], row[0]))
                    dst_conn.commit()

                if all_rows:
                    rec.constrain_file = csv_bytes_from_rows(
                        ['constraint_name', 'table_name', 'column_name', 'foreign_table_name', 'foreign_column_name'],
                        all_rows
                    )
                    rec.constrain_filename = 'constraints.csv'
                rec.constrain_state = 'removed'
            finally:
                dst_cur.close()
                dst_conn.close()

    def action_remove_null_constrain(self):
        for rec in self:
            dst_cur, dst_conn = rec._connect_dest()
            all_rows = []
            try:
                for tbl in rec.table_ids:
                    dst_cur.execute("""
                        SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = %s
                          AND column_name != 'id'
                          AND is_nullable = 'NO'
                    """, (tbl.dist_table,))
                    rows = dst_cur.fetchall()
                    all_rows.extend(rows)
                    for row in rows:
                        dst_cur.execute('ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL' % (row[0], row[1]))
                    dst_conn.commit()

                if all_rows:
                    rec.null_file = csv_bytes_from_rows(['table_name', 'column_name'], all_rows)
                    rec.null_filename = 'not_null_constraints.csv'
                rec.null_state = 'removed'
            finally:
                dst_cur.close()
                dst_conn.close()

    def action_set_constrain(self):
        for rec in self:
            if not rec.constrain_file:
                raise UserError(_("No stored constraints file found."))

            import base64, csv, io
            content = base64.b64decode(rec.constrain_file).decode('utf-8')
            reader = csv.DictReader(io.StringIO(content))

            dst_cur, dst_conn = rec._connect_dest()
            try:
                for row in reader:
                    query = """
                        ALTER TABLE "{table_name}"
                        ADD CONSTRAINT "{constraint_name}"
                        FOREIGN KEY ("{column_name}")
                        REFERENCES "{foreign_table_name}" ("{foreign_column_name}")
                        MATCH SIMPLE
                        ON UPDATE NO ACTION
                        ON DELETE SET NULL
                        DEFERRABLE INITIALLY DEFERRED
                    """.format(**row)
                    dst_cur.execute(query)
                dst_conn.commit()
                rec.constrain_state = 'placed'
            finally:
                dst_cur.close()
                dst_conn.close()

    def action_set_null_constrain(self):
        for rec in self:
            if not rec.null_file:
                raise UserError(_("No stored not-null file found."))

            import base64, csv, io
            content = base64.b64decode(rec.null_file).decode('utf-8')
            reader = csv.DictReader(io.StringIO(content))

            dst_cur, dst_conn = rec._connect_dest()
            try:
                for row in reader:
                    query = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL' % (
                        row['table_name'], row['column_name']
                    )
                    dst_cur.execute(query)
                dst_conn.commit()
                rec.null_state = 'placed'
            finally:
                dst_cur.close()
                dst_conn.close()
    def _migrate_m2m_table(self, table, column, src_cur, dst_cur, dst_conn):
        
        src_rel_table = column.source_m2m_relation_table
        dist_rel_table = column.dist_m2m_relation_table

        if not src_rel_table:
            return

        # 🔍 check if already processed
        relation_rec = self.env['migration.m2m.relation'].search([
            ('migration_id', '=', self.id),
            ('src_relation_table', '=', src_rel_table)
        ], limit=1)

        if relation_rec:
            if relation_rec.state == 'done':
                _logger.info(f"Skipping already migrated M2M: {src_rel_table}")
                return
        else:
            relation_rec = self.env['migration.m2m.relation'].create({
                'migration_id': self.id,
                'src_relation_table': src_rel_table,
                'dist_relation_table': dist_rel_table,
                'source_model': table.source_model,
                'relation_model': column.source_relation,
                'state': 'pending'
            })

        try:
            _logger.info(f"Migrating M2M: {src_rel_table}")

            # detect columns
            src_cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (src_rel_table,))
            cols = [r[0] for r in src_cur.fetchall()]

            if len(cols) != 2:
                relation_rec.state = 'failed'
                relation_rec.note = 'Invalid M2M structure'
                return

            col1, col2 = cols

            # fetch data
            src_cur.execute(f"SELECT {col1}, {col2} FROM {src_rel_table}")
            rows = src_cur.fetchall()

            mapped_rows = []

            for r in rows:
                src_id_1, src_id_2 = r

                new_id_1 = self._map_fk_value(table.source_model, src_id_1)
                new_id_2 = self._map_fk_value(column.source_relation, src_id_2)

                if new_id_1 and new_id_2:
                    mapped_rows.append((new_id_1, new_id_2))

            if not mapped_rows:
                relation_rec.state = 'done'
                relation_rec.note = 'No valid rows'
                return

            buffer_ = rows_to_copy_buffer(mapped_rows)

            dst_cur.copy_expert(
                f"COPY {dist_rel_table} ({col1},{col2}) FROM STDIN WITH (FORMAT text)",
                buffer_
            )
            dst_conn.commit()

            relation_rec.state = 'done'
            relation_rec.note = f"{len(mapped_rows)} rows migrated"

        except Exception as e:
            dst_conn.rollback()
            relation_rec.state = 'failed'
            relation_rec.note = str(e)

            self._log_error(table, f"M2M {rel_table}: {str(e)}")

    def action_migrate(self):
        for rec in self:
            rec.action_validate()

            src_cur, src_conn = rec._connect_source()
            dst_cur, dst_conn = rec._connect_dest()
            dashboard = rec._ensure_dashboard()

            rec.state = 'running'
            rec.table_done_count = 0
            rec.row_done_count = 0
            rec.row_failed_count = 0

            try:
                ordered_tables = rec._get_ordered_tables()
                dashboard.total_tables = len(ordered_tables)
                dashboard.migrated_tables = 0
                dashboard.failed_tables = 0
                dashboard.progress = 0.0

                for tbl in ordered_tables:
                    try:
                        select_statement, mapped_columns = rec._prepare_select_statement(tbl)
                        source_rows = rec.source_data_fetching(src_cur, select_statement)
                        row_dicts = rec._transform_special_rows(tbl, source_rows, mapped_columns)
                        dest_columns = rec._prepare_insert_statement(tbl, mapped_columns)

                        prepared_rows = []
                        for row_data in row_dicts:
                            source_id, payload = rec._resolve_row_for_destination(tbl, row_data, mapped_columns)
                            prepared_rows.append((source_id, payload))

                        
                        rows_only = [payload for _, payload in prepared_rows]
                        inserted = rec._bulk_insert(dst_cur, dst_conn, tbl.dist_table, dest_columns, rows_only)

                        if not tbl.no_id and 'id' in dest_columns:
                            id_index = dest_columns.index('id')
                            for source_id, payload in prepared_rows:
                                dist_id = payload[id_index]
                                rec._remember_id_map(tbl.source_model, source_id, dist_id)

                        if not tbl.no_id:
                            rec._update_sequence(dst_cur, dst_conn, tbl.dist_table)
                        
                        m2m_fields = tbl.column_ids.filtered(lambda c: c.migrate and c.migration_type == 'm2m')

                        for m2m_col in m2m_fields:
                            rec._migrate_m2m_table(tbl,m2m_col,src_cur,dst_cur,dst_conn)

                        rec.table_done_count += 1
                        rec.row_done_count += inserted
                        dashboard.migrated_tables = rec.table_done_count
                        dashboard.progress = (float(rec.table_done_count) / dashboard.total_tables * 100.0) if dashboard.total_tables else 0.0

                    except Exception as table_error:
                        dst_conn.rollback()
                        rec.row_failed_count += 1
                        dashboard.failed_tables += 1
                        rec._log_error(tbl, str(table_error))
                        _logger.exception("Migration failed for table %s", tbl.source_table)

                rec.state = 'done' if not rec.error_ids else 'failed'
                dashboard.action_refresh()

            finally:
                src_cur.close()
                src_conn.close()
                dst_cur.close()
                dst_conn.close()

    def action_retry_failed(self):
        for rec in self:
            retry_tables = rec.error_ids.filtered(lambda e: e.state in ('new', 'retry')).mapped('table_config_id')
            if not retry_tables:
                raise UserError(_("No retryable errors found."))
            rec.table_ids = [(6, 0, retry_tables.ids)]
            rec.action_migrate()
    def action_retry_m2m(self):
        failed = self.m2m_relation_ids.filtered(lambda r: r.state == 'failed')
        for rel in failed:
            rel.state = 'pending'