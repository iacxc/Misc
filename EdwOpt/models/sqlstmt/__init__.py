
from __future__ import print_function

def get_all_catalogs():
    return """
SELECT cat_uid, 
       trim(cat_name) as catalog_name,
       trim(local_smd_volume) as local_smd_volume,
       cat_owner
FROM hp_system_catalog.system_schema.catsys
FOR READ UNCOMMITTED ACCESS IN SHARE MODE"""


def get_all_schemas():
    return """
SELECT c.cat_uid,
       trim(c.cat_name) as catalog_name, 
       s.schema_uid,
       trim(s.schema_name) as schema_name,
       s.schema_owner,
       trim(s.schema_subvolume) as schema_subvolume
FROM hp_system_catalog.system_schema.schemata s,
     hp_system_catalog.system_schema.catsys c
WHERE c.CAT_UID = s.CAT_UID
FOR READ UNCOMMITTED ACCESS IN SHARE MODE"""


def get_schemas(catalog):
    return """
SELECT s.schema_uid,
       trim(s.schema_name) as schema_name,
       s.schema_owner,
       trim(s.schema_subvolume) as schema_subvolume
FROM hp_system_catalog.system_schema.schemata s,
     hp_system_catalog.system_schema.catsys c
WHERE s.cat_uid = c.cat_uid
     AND TRIM(c.cat_name) = '{catalog}'
""".format(catalog=catalog.upper())


def get_tables(catalog):
    return """
SELECT trim(s.schema_name) as schema_name, 
       trim(o.object_name) as table_name
FROM {catalog}.hp_definition_schema.objects o,
    hp_system_catalog.system_schema.schemata s
WHERE o.schema_uid = s.schema_uid
    AND o.object_security_class in ('UT', 'UM', 'SM', 'MU')
    AND o.object_name_space = 'TA'
    AND o.object_type = 'BT'
FOR READ UNCOMMITTED ACCESS IN SHARE MODE""".format(catalog=catalog.upper())


def get_table_files(catalog, schema=None, table=None):
    return """
    SELECT trim(s.schema_name) as schema_name, 
           trim(o.object_name) as table_name, 
           trim(p.data_source) || '.' || trim(p.file_suffix) as file_name
    FROM {catalog}.hp_definition_schema.objects o
         LEFT JOIN {catalog}.hp_definition_schema.partitions p
                    ON o.object_uid = p.object_uid
         LEFT JOIN hp_system_catalog.system_schema.schemata s
                    ON o.schema_uid = s.schema_uid
    WHERE o.object_security_class = 'UT'
           and o.object_type = 'BT'
           and o.object_name_space = 'TA'""".format(catalog=catalog) +\
           (" and s.schema_name ='{0}'".format(schema) if schema else "") +\
           (" and o.object_name ='{0}'".format(table) if table else "") +\
    """ ORDER BY s.schema_name, o.object_name, p.data_source
    FOR READ UNCOMMITTED ACCESS IN SHARE MODE""".format(catalog=catalog)


def get_single_partition_objs(catalog):
    return """
SELECT o.OBJECT_UID, TRIM(o.OBJECT_NAME) as OBJECT_NAME, 
       o.OBJECT_NAME_SPACE, o.OBJECT_TYPE 
FROM
    (SELECT object_uid 
     FROM {catalog}.HP_DEFINITION_SCHEMA.PARTITIONS 
     HAVING count(*) = 1 
     GROUP by 1) AS ou
     LEFT JOIN {catalog}.hp_definition_schema.objects o
          ON ou.object_uid = o.object_uid""".format(catalog=catalog)


def get_table_partitions(*params):
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
""".format(tablename=".".join(params))


def get_cols(catalog, table):
    """ columns for a table """
    return """
SELECT o.object_uid, 
       trim(o.object_name) as table_name, 
       trim(c.column_name) as column_name, 
       c.fs_data_type,
       trim(c.sql_data_type) as sql_data_type,
       c.column_size
FROM {catalog}.hp_definition_schema.objects o,
     {catalog}.hp_definition_schema.cols c
WHERE o.object_uid = c.object_uid
    AND o.object_name = '{table}'
ORDER by c.column_number""".format(catalog=catalog.upper(), 
                                   table=table.upper())


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

