# -*- coding: utf-8 -*-
import csv
import io
import json
import difflib
import base64
import logging
from collections import defaultdict, deque

from psycopg2 import sql

_logger = logging.getLogger(__name__)


POSTGRESQL_RESERVED = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric',
    'both', 'case', 'cast', 'check', 'collate', 'column', 'constraint', 'create',
    'current_catalog', 'current_date', 'current_role', 'current_time', 'current_timestamp',
    'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end',
    'except', 'false', 'fetch', 'for', 'foreign', 'from', 'grant', 'group', 'having',
    'in', 'initially', 'intersect', 'into', 'lateral', 'leading', 'limit', 'localtime',
    'localtimestamp', 'not', 'null', 'offset', 'on', 'only', 'or', 'order', 'placing',
    'primary', 'references', 'returning', 'select', 'session_user', 'some', 'symmetric',
    'table', 'then', 'to', 'trailing', 'true', 'union', 'unique', 'user', 'using',
    'variadic', 'when', 'where', 'window', 'with'
}

ACCOUNT_TYPE_MAP_11_TO_19 = {
    1: 'asset_receivable',
    2: 'asset_cash',
    3: 'asset_current',
    4: 'asset_non_current',
    5: 'asset_prepayments',
    6: 'asset_fixed',
    7: 'liability_payable',
    8: 'liability_credit_card',
    9: 'liability_current',
    10: 'liability_non_current',
    11: 'equity',
    12: 'equity_unaffected',
    13: 'income',
    14: 'income_other',
    15: 'expense',
    16: 'expense_depreciation',
    17: 'expense_direct_cost',
    18: 'off_balance',
    22: 'view',
    23: 'liability_accrual',
}

FIELD_RENAMES_11_TO_19 = {
    'date_invoice': ['invoice_date'],
    'type': ['move_type'],
    'internal_type': ['account_type'],
    'user_type_id': ['account_type'],
    'communication': ['ref'],
    'number': ['payment_reference'],
    'date_due': ['invoice_date_due'],
    'origin': ['invoice_origin'],
    'reference': ['ref'],
    'partner_shipping_id': ['partner_shipping_id'],
    'comment': ['narration'],
    'company_currency_id': ['currency_id'],
    'payment_term': ['invoice_payment_term_id'],
}

MODEL_RENAMES_11_TO_19 = {
    'account.invoice': 'account.move',
    'account.invoice.line': 'account.move.line',
}

TOKEN_SYNONYMS = {
    'partner': ['customer', 'client', 'commercial'],
    'invoice': ['move', 'bill'],
    'amount': ['total', 'balance'],
    'user': ['employee'],
    'date': ['day'],
    'reference': ['ref'],
    'account': ['ledger'],
}


def quote_identifier(name):
    if not name:
        return name
    if name.lower() in POSTGRESQL_RESERVED or not name.replace('_', '').isalnum():
        return '"%s"' % name
    return name


def normalize_name(name):
    return (name or '').strip().lower()


def tokenize_name(name):
    return [x for x in normalize_name(name).split('_') if x]


def similarity(a, b):
    return difflib.SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def semantic_score(source_name, dest_name):
    src_tokens = tokenize_name(source_name)
    dst_tokens = tokenize_name(dest_name)
    score = 0.0
    for s in src_tokens:
        for d in dst_tokens:
            if s == d:
                score += 10.0
            elif s in TOKEN_SYNONYMS and d in TOKEN_SYNONYMS[s]:
                score += 8.0
    return score


def score_field_match(source_field, dest_field):
    src_name = source_field.get('name') or ''
    dst_name = dest_field.get('name') or ''
    src_ttype = source_field.get('ttype')
    dst_ttype = dest_field.get('ttype')
    src_relation = source_field.get('relation')
    dst_relation = dest_field.get('relation')

    score = 0.0

    if src_name == dst_name:
        score += 100.0

    if src_name in FIELD_RENAMES_11_TO_19 and dst_name in FIELD_RENAMES_11_TO_19[src_name]:
        score += 85.0

    score += similarity(src_name, dst_name) * 50.0
    score += semantic_score(src_name, dst_name)

    if src_ttype and dst_ttype and src_ttype == dst_ttype:
        score += 20.0

    if src_relation and dst_relation and src_relation == dst_relation:
        score += 30.0

    if src_name.endswith('_id') and dst_name.endswith('_id'):
        score += 10.0

    return score


def find_best_field_match(source_field, dest_fields, threshold=60.0):
    best = None
    best_score = 0.0
    for dest in dest_fields:
        current = score_field_match(source_field, dest)
        if current > best_score:
            best_score = current
            best = dest
    if best_score >= threshold:
        return best, best_score
    return None, best_score


def build_dependency_graph(table_records):
    """
    graph[node] = set(dependencies)
    """
    graph = defaultdict(set)
    all_models = set(table_records.mapped('source_model'))
    for tbl in table_records:
        graph[tbl.source_model] |= set()
        for col in tbl.column_ids.filtered(lambda c: c.migrate and c.source_relation):
            if col.source_relation in all_models and col.source_relation != tbl.source_model:
                graph[tbl.source_model].add(col.source_relation)
    return graph


def topological_sort_dependency_graph(graph):
    indegree = {node: 0 for node in graph}
    reverse_map = defaultdict(set)

    for node, deps in graph.items():
        indegree.setdefault(node, 0)
        for dep in deps:
            indegree.setdefault(dep, 0)
            indegree[node] += 1
            reverse_map[dep].add(node)

    queue = deque(sorted([node for node, degree in indegree.items() if degree == 0]))
    ordered = []

    while queue:
        node = queue.popleft()
        ordered.append(node)
        for child in sorted(reverse_map[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(indegree):
        remaining = [node for node, degree in indegree.items() if degree > 0]
        ordered.extend(sorted(remaining))
    return ordered


def csv_bytes_from_rows(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return base64.b64encode(output.getvalue().encode('utf-8'))


def rows_to_copy_buffer(rows):
    output = io.StringIO()
    for row in rows:
        safe = []
        for val in row:
            if val is None:
                safe.append(r'\N')
            elif isinstance(val, bool):
                safe.append('t' if val else 'f')
            elif isinstance(val, (dict, list)):
                safe.append(json.dumps(val, ensure_ascii=False))
            else:
                safe.append(str(val).replace('\t', ' ').replace('\n', ' '))
        output.write('\t'.join(safe) + '\n')
    output.seek(0)
    return output


def safe_table_name_from_model(model_name):
    return (model_name or '').replace('.', '_')