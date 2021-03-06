"""
   Seaquest.py
"""

from __future__ import print_function

import csv

from Common import make_enum
from models import sqlstmt


DATATYPE = make_enum('DATATYPE',
                     NVARCHAR=-9,
                     WCHAR=-8,
                     TINYINT=-6,
                     BIGINT=-5,
                     CHAR=1,
                     NUMERIC=2,
                     INTEGER=4,
                     SMALLINT=5,
                     FLOAE=6,
                     DOUBLE=8,
                     VARCHAR=12,
                     DATE=91,
                     TIME=92,
                     TIMESTAMP=93,
                     )


def ft_text(ft):
    """ return the string for a field type """
    return {DATATYPE.TINYINT: 'INTEGER',
            DATATYPE.SMALLINT: 'INTEGER',
            DATATYPE.INTEGER: 'INTEGER',
            DATATYPE.BIGINT: 'BIGINT',
            DATATYPE.DATE: 'TIMESTAMP',
            DATATYPE.TIME: 'TIMESTAMP',
            DATATYPE.TIMESTAMP: 'TIMESTAMP',
            DATATYPE.CHAR: 'CHAR',
            DATATYPE.WCHAR: 'CHAR',
            DATATYPE.NVARCHAR: 'VARCHAR',
            DATATYPE.VARCHAR: 'VARCHAR',
            DATATYPE.NUMERIC: 'DOUBLE',
            }.get(ft)


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


class DBObj(object):
    def __init__(self, connection, debug=True):
        self.__conn = connection
        self.__debug = debug

    @property
    def connection(self):
        return self.__conn

    def log_debug(self, msg):
        """ log debug """
        if __debug__ and self.__debug:
            print(msg)

    def cursor(self):
        return self.__conn.cursor()

    def runsql(self, sqlstr, *params):
        cursor = self.cursor()
        cursor.execute(sqlstr, *params)

        return cursor

    def getone(self, sqlstr, *params):
        """ execute a query, return the first row """
        cursor = self.runsql(sqlstr, *params)

        return ([desc[0] for desc in cursor.description],
                cursor.fetchone())

    def getall(self, sqlstr, *params):
        cursor = self.runsql(sqlstr, *params)

        return ([desc[0] for desc in cursor.description],
                cursor.fetchall())


class Database(DBObj):
    def __init__(self, connection, debug=True):
        super(Database, self).__init__(connection, debug)

        self.__catalogs = []

    @property
    def catalogs(self):
        return self.__catalogs

    def add_catalog(self, catalog):
        self.__catalogs.append(catalog)

    def get_all_catalogs(self):
        return self.getall(sqlstmt.get_all_catalogs())

    def get_all_schemas(self):
        return self.getall(sqlstmt.get_all_schemas())

    def get_schemas(self, catalog):
        return self.getall(sqlstmt.get_schemas(catalog))

    def get_tables(self, catalog):
        return self.getall(sqlstmt.get_tables(catalog))

    def get_table_files(self, catalog, schema=None, table=None):
        return self.getall(sqlstmt.get_table_files(catalog, schema, table))

    def get_table_partitions(self, *params):
        """ get partitions for a table """
        return self.getall(sqlstmt.get_table_partitions(*params))

    def get_single_partition_objs(self, catalog):
        return self.getall(sqlstmt.get_single_partition_objs(catalog))

    def get_table_columns(self, catalog, table):
        return self.getall(sqlstmt.get_cols(catalog, table))

    def _fill_struct(self):
        if len(self.catalogs) > 0:   # prevent doing it twice
            return

        # first get all catalogs and schemas
        fields, rows = self.get_all_catalogs()
        for row in rows:
            catname = row[0]
            cat = Catalog(catname)

            # get all tables for catalog
            cursch = None
            for r in self.connection.get_tables(catname):
                schname = r[1]
                tablename = r[2]
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
        for row in self.connection.get_columns(catalog, schema, table):
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

    def dumpdata(self, table=None, sql=None, *params):
        """ dump data to file"""
        if table is None and sql is None:
            return

        if sql is None:
            sql = "SELECT * FROM %s" % table
            params = []
        
        cursor = self.runsql(sql, *params)

        if table is None:
            table = 'TABLE'

        with open('%s.sql' % table, 'wb') as ddlfile:
            ddlfile.write("CREATE TABLE %s(\n" % table)
            firstfield = True
            for desc in cursor.description:
                if firstfield:
                    ddlfile.write("    %s %s\n" % (desc[0], ft_text(desc[1])))
                    firstfield = False
                else:
                    ddlfile.write("   ,%s %s\n" % (desc[0], ft_text(desc[1])))

            ddlfile.write(");\n")

        with open('%s.csv' % table, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for row in cursor.fetchall():
                writer.writerow(row)

WMSCOMMAND = make_enum('WMSCOMMAND',
                       OPENWMS='WMSOPEN',
                       STATUS_WMS='STATUS WMS',
                       STATUS_QUERY_ALL='STATUS QUERIES ALL MERGED',
                       STATUS_QUERY='STATUS QUERY {0} MERGED',
                       STATUS_SERVICE='STATUS SERVICE {0}',
                       STATUS_RULE='STATUS RULE {0}',
                       )


class WMSSystem(DBObj):
    """ WMS System """
    def __init__(self, connection, debug=True):
        super(WMSSystem, self).__init__(connection, debug)

    def cursor(self):
        cursor = super(WMSSystem, self).cursor()
        cursor.execute(WMSCOMMAND.OPENWMS)

        return cursor

    #
    # WMS commands
    def status(self):
        """ get the status of wms system """
        return self.getall(WMSCOMMAND.STATUS_WMS)

    def status_query(self, queryid=None):
        """ get the status of a query/all queries """
        return self.getall(WMSCOMMAND.STATUS_QUERY_ALL) if queryid is None \
            else self.getall(WMSCOMMAND.STATUS_QUERY.format(queryid))

    def status_service(self, service_name=None):
        """ get the status of services """
        return self.getall(WMSCOMMAND.STATUS_SERVICE.format(
            'ALL' if service_name is None else service_name))

    def status_rule(self, rule_name=None):
        """ get the status of services """
        return self.getall(WMSCOMMAND.STATUS_RULE.format(
            'ALL' if rule_name is None else rule_name))

>>>>>>> 807a790b99239072afd5835dfeddd542448cbaed
