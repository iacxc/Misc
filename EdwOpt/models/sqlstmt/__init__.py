
from __future__ import print_function

def get_all_catalogs():
    return """
SELECT trim(cat_name)
    FROM hp_system_catalog.system_schema.catsys
FOR READ UNCOMMITTED ACCESS IN SHARE MODE"""


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
     AND TRIM(c.cat_name) = '{catalog}'
""".format(catalog=catalog)


def get_tables(catalog):
    return """
SELECT trim(s.schema_name) || '.' || trim(o.object_name)
FROM {catalog}.hp_definition_schema.objects o,
    hp_system_catalog.system_schema.schemata s
WHERE o.schema_uid = s.schema_uid
    AND o.object_security_class in ('UT', 'UM', 'SM', 'MU')
    AND o.object_name_space = 'TA'
    AND o.object_type = 'BT'
FOR READ UNCOMMITTED ACCESS IN SHARE MODE""".format(catalog=catalog)

def get_partitions(tablename):
    return """
SELECT TRIM(CATALOG_NAME) AS CATALOG_NAME,
       TRIM(SCHEMA_NAME) AS SCHEMA_NAME,
       TRIM(OBJECT_NAME) AS OBJECT_NAME,
       TRIM(PARTITION_NAME) AS PARTITION_NAME,
       PARTITION_NUM,
       ROW_COUNT,
       INSERTED_ROW_COUNT,
       DELETED_ROW_COUNT,
       UPDATED_ROW_COUNT,
       PRIMARY_EXTENTS,
       SECONDARY_EXTENTS,
       MAX_EXTENTS,
       ALLOCATED_EXTENTS,
       (PRIMARY_EXTENTS + (SECONDARY_EXTENTS * (MAX_EXTENTS -1))) * 2048 AS MAX_SIZE,
       CURRENT_EOF,
       COMPRESSION_TYPE,
       COMPRESSED_EOF_SECTORS,
       COMPRESSION_RATIO,
       RFORK_EOF,
       ACCESS_COUNTER 
FROM TABLE(DISK LABEL STATISTICS({tablename})) 
ORDER BY PARTITION_NUM FOR READ UNCOMMITTED ACCESS
""".format(tablename=tablename)
 
def get_cols(catalog, table):
    return """
SELECT o.object_name, c.column_name, c.fs_data_type
FROM {catalog}.hp_definition_schema.objects o,
     {catalog}.hp_definition_schema.cols c
WHERE o.object_uid = c.object_uid
    AND o.object_name = '{table}'
ORDER by c.column_number""".format(catalog=catalog, table=table)


def get_sql(action):
    """ get the report sql for a specific dsn and action"""
    import importlib

    action = action.replace(' ', '_')

    try:
        model = importlib.import_module('{}.{}'.format(__name__, action))
        return model.SQL
    except ImportError:
        return ''
    except AttributeError:
        return ''

