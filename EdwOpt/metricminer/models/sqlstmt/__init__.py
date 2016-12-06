
from __future__ import print_function

def get_all_schemas():
    return """
SELECT trim(c.cat_name) || '.' || trim(s.schema_name)
    FROM hp_system_catalog.system_schema.schemata s,
         hp_system_catalog.system_schema.catsys c
    WHERE c.CAT_UID = s.CAT_UID
FOR READ UNCOMMITTED ACCESS IN SHARE MODE"""


def get_schema(catalog):
    return """
SELECT s.schema_name
FROM hp_system_catalog.system_schema.schemata s,
     hp_system_catalog.system_schema.catsys c
WHERE s.cat_uid = c.cat_uid
     AND TRIM(c.cat_name) = '%(catalog)s'
""" % {'catalog': catalog}


def get_tables(catalog):
    return """
SELECT trim(s.schema_name) || '.' || trim(o.object_name)
FROM %(catalog)s.hp_definition_schema.objects o,
    hp_system_catalog.system_schema.schemata s
WHERE o.schema_uid = s.schema_uid
    AND o.object_security_class in ('UT', 'UM', 'SM', 'MU')
    AND o.object_name_space = 'TA'
    AND o.object_type = 'BT'
FOR READ UNCOMMITTED ACCESS IN SHARE MODE""" % {'catalog': catalog}


def get_cols(catalog, table):
    return """
SELECT o.object_name, c.* 
FROM %(catalog)s.hp_definition_schema.objects o,
     %(catalog)s.hp_definition_schema.cols c
WHERE o.object_uid = c.object_uid
    AND o.object_name = '%(table)s'
ORDER by c.column_number""" % {'catalog': catalog, 'table': table}


def get_sql(action):
    """ get the report sql for a specific dsn and action"""
    import importlib

    action = action.replace(' ', '_')

    try:
        model = importlib.import_module('%s.%s' % (__name__, action))
        return model.SQL
    except ImportError:
        return ''
    except AttributeError:
        return ''

