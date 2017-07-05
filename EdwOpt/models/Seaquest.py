from __future__ import print_function

import csv

import sqlstmt


NVARCHAR  = -9
WCHAR     = -8
TINYINT   = -6
BIGINT    = -5
CHAR      = 1
NUMERIC   = 2
INTEGER   = 4
SMALLINT  = 5
FLOAE     = 6
DOUBLE    = 8
VARCHAR   = 12
DATE      = 91
TIME      = 92
TIMESTAMP = 93

def ft_text(ft):
    fieldtype = {TINYINT: 'INTEGER',
                 SMALLINT: 'INTEGER',
                 INTEGER: 'INTEGER',
                 BIGINT: 'BIGINT',
                 DATE: 'TIMESTAMP',
                 TIME: 'TIMESTAMP',
                 TIMESTAMP: 'TIMESTAMP',
                 CHAR: 'CHAR',
                 WCHAR: 'CHAR',
                 NVARCHAR: 'VARCHAR',
                 VARCHAR: 'VARCHAR',
                 NUMERIC: 'DOUBLE',
    }
    return fieldtype[ft]
 

class Table(object):
    def __init__(self, catname, schname, tname, ddl=None):
        self.__name = tname
        self.__fullname = '.'.join([catname, schname, tname])
        self.__ddl = ddl

    @property
    def name(self):
        return self.__name

    @property
    def fullname(self):
        return self.__fullname

    @property
    def ddl(self):
        return self.__ddl

    def set_ddl(self, ddl):
        self.__ddl = ddl

    def __repr__(self):
        return '<Table %s>' % self.fullname


class Schema(object):
    def __init__(self, catname, schname):
        self.__name = schname
        self.__fullname = '%s.%s' % (catname, schname)
        self.__tables = []

    @property
    def name(self):
        return self.__name

    @property
    def fullname(self):
        return self.__fullname

    @property
    def tables(self):
        return self.__tables

    def get_table(self, tname):
        for table in self.tables:
            if table.name == tname:
                return table

    def add_table(self, table):
        self.__tables.append(table)

    def __repr__(self):
        return '<Schema %s>' % self.fullname


class Catalog(object):
    def __init__(self, name):
        self.__name = name
        self.__schemas = []

    @property
    def name(self):
        return self.__name

    @property
    def schemas(self):
        return self.__schemas

    def get_schema(self, schname):
        for sch in self.schemas:
            if sch.name == schname:
                return sch

    def add_schema(self, schema):
        self.__schemas.append(schema)

    def __repr__(self):
        return '<Catalog %s>' % self.name


class Database(object):
    def __init__(self, options, debug=True):
        try:
            import jdbc
            self.__db = jdbc.get_connection(
                           'jdbc:hpt4jdbc://%s:18650' % options.server, options)
            self.get_tables = jdbc.get_tables
            self.get_columns = jdbc.get_columns
        except ImportError:
            import odbc
            self.__db = odbc.get_connection(options)
            self.get_tables = odbc.get_tables
            self.get_columns = odbc.get_columns

        self.__debug = debug
        self.__catalogs = []

    @property
    def catalogs(self):
        return self.__catalogs

    def cursor(self):
        return self.__db.cursor()

    def add_catalog(self, catalog):
        self.__catalogs.append(catalog)

    def log_debug(self, msg):
        if __debug__ and self.__debug:
                print(msg)

    def _runsql(self, sqlstr, *params):
        """ run a sql, return the cursor """
        self.log_debug(sqlstr)
        self.log_debug(params)

        cursor = self.cursor()
        cursor.execute(sqlstr, params)

        return cursor

    def _fill_struct(self):
        if len(self.catalogs) > 0:   # prevent doing it twice
            return

        # first get all catalogs and schemas
        result = self._runsql(sqlstmt.get_all_catalogs())
        for row in result.fetchall():
            catname = row[0]
            cat = Catalog(catname)

            # get all tables for catalog
            cursch = None
            for row in self.get_tables(self.cursor(), catname):
                schname = row[1]
                tablename = row[2]
                if cursch is None or cursch.name != schname:
                    if cursch:
                        cat.add_schema(cursch)
                    cursch = Schema(catname, schname)

                table = Table(catname, schname, tablename)
                cursch.add_table(table)

            if cursch:
                cat.add_schema(cursch)

            self.add_catalog(cat)

    def pullobjs(self):
        self._fill_struct()

    def dumpobjs(self):
        for cat in self.catalogs:
            print(cat)
            for sch in cat.schemas:
                print('    %s' % sch)
                for table in sch.tables:
                    print('        %s' % table)

    def getddl(self, catalog, schema, table):
        self.log_debug('%s.%s.%s' % (catalog, schema, table))
        ddl = ["CREATE TABLE %s(" % table]
        firstfield = True
        for row in self.get_columns(self.cursor(), catalog, schema, table):
            if firstfield:
                ddl.append("    %s %s" % (row[3].strip(), ft_text(row[4])))
                firstfield = False
            else:
                ddl.append("   ,%s %s" % (row[3].strip(), ft_text(row[4])))

        ddl.append(");")
#                    "ROW FORMAT DELIMITED",
#                    "FIELDS TERMINATED BY'|'",
#                    "STORED AS TEXTFILE;"])

        if len(ddl) > 2:
            return "\n".join(ddl)

    def getall(self, sqlstr, *params):
        """ _runsql a query, return generator of Rows """
        result = self._runsql(sqlstr, *params)

        return ([desc[0] for desc in result.description],
                result.fetchall())

    def getone(self, sqlstr, *params):
        """ execute a query, return the first row """
        result = self._runsql(sqlstr, *params)

        return ([desc[0] for desc in result.description],
                result.fetchone())

    def dumpdata(self, table=None, sql=None, *params):
        if table is None and sql is None:
            return

        if sql is None:
            sql = "SELECT * FROM %s" % table
            params = []
        
        result = self._runsql(sql, *params)

        if table is None:
            table = 'TABLE'

        with file('%s.sql' % table, 'wb') as ddlfile:
            ddlfile.write("CREATE TABLE %s(\n" % table)
            firstfield = True
            for desc in result.description:
                if firstfield:
                    ddlfile.write("    %s %s\n" % (desc[0], ft_text(desc[1])))
                    firstfield = False
                else:
                    ddlfile.write("   ,%s %s\n" % (desc[0], ft_text(desc[1])))

            ddlfile.write(");\n")

        with file('%s.csv' % table, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for row in result.fetchall():
                writer.writerow(row)

    def get_partitions(self, tablename):
        """ get partitions for a table """
        query_text = \
"""SELECT TRIM(CATALOG_NAME) AS CATALOG_NAME,
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
FROM TABLE(DISK LABEL STATISTICS({0})) 
ORDER BY PARTITION_NUM FOR READ UNCOMMITTED ACCESS
""".format(tablename)
        return self.getall(query_text)

class WMSSystem(object):
    def __init__(self, options):
        try:
            import jdbc
            self.__db = jdbc.get_connection(
                           'jdbc:hpt4jdbc://%s:18650' % options.server, options)
        except ImportError:
            import odbc
            self.__db = odbc.get_connection(options)


    def run_cmd(self, cmd):
        cursor = self.__db.cursor()
        cursor.execute("WMSOPEN")
        cursor.execute(cmd)

        return ([desc[0] for desc in cursor.description],
                cursor.fetchall())
