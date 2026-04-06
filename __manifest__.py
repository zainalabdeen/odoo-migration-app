# -*- coding: utf-8 -*-
{
    'name': 'Data Migration Framework',
    'version': '1.0.0',
    'summary': 'Odoo 11 -> 19 data migration framework with discovery, mapping, validation, dashboard, and execution',
    'category': 'Tools',
    'author': 'Custom',
    'depends': ['base'],
    'data': [
        'security/ir.model.access.csv',
        'views/database_config_views.xml',
        'views/migration_app_views.xml',
        'views/table_config_views.xml',
        'views/data_migration_views.xml',
        'views/dashboard_views.xml',
        'views/discover_models_wizard_views.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}