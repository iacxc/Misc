from __future__ import print_function

import csv

import sqlstmt


class Table(object):
    def __init__(self, schname, tname, ddl=None):
        self.__name = '%s.%s' % (schname, tname)
        self.__ddl = ddl

    @property
    def name(self):
        return self.__name

    @property
    def ddl(self):
        return self.__ddl

    def set_ddl(self, ddl):
        self.__ddl = ddl

    def __repr__(self):
        return '<Table %s>' % self.name


class Schema(object):
    def __init__(self, catname, schname):
        self.__name = '%s.%s' % (catname, schname)
        self.__tables = []

    @property
    def name(self):
        return self.__name

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
        return '<Schema %s>' % self.name


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
        except ImportError:
            import odbc
            self.__db = odbc.get_connection(options)

        self.__debug = debug
        self.__catalogs = []

    @property
    def catalogs(self):
        return self.__catalogs

    def add_catalog(self, catalog):
        self.__catalogs.append(catalog)

    def log_debug(self, msg):
        if __debug__ and self.__debug:
                print(msg)

    def _runsql(self, sqlstr, *params):
        """ run a sql, return the cursor """
        self.log_debug(sqlstr)

        cursor = self.__db.cursor()
        cursor.execute(sqlstr, *params)

        return cursor

    def _fill_struct(self):
        if len(self.catalogs) > 0:   # prevent doing it twice
            return

        # first get all catalogs and schemas
        result = self._runsql(sqlstmt.get_all_schemas())
        curcat = None
        for row in result.fetchall():
            catname, schname = row[0].split('.', 1)
            if curcat is None or curcat.name != catname:          # a new catalog
                if curcat:
                    self.add_catalog(curcat)
                curcat = Catalog(catname)

            schema = Schema(catname, schname)

            curcat.add_schema(schema)

        if curcat:
            self.add_catalog(curcat)

        # get all tables
        for catalog in self.catalogs:
            result = self._runsql(sqlstmt.get_tables(catalog.name))
            for row in result.fetchall():
                schname, tname = row[0].split('.', 1)
                schname = '%s.%s' % (catalog.name, schname)

                table = Table(schname, tname)
                catalog.get_schema(schname).add_table(table)

    def pullobjs(self):
        self._fill_struct()

    def dumpobjs(self):
        for cat in self.catalogs:
            print(cat)
            for sch in cat.schemas:
                print('    %s' % sch)
                for table in sch.tables:
                    print('        %s' % table)

    def getddl(self, catalog, tname):
        sqlstr = sqlstmt.get_cols(catalog, tname.upper())
        result = self._runsql(sqlstr)

        fieldtype = {
            0: 'STRING',
            2: 'STRING',
            66: 'STRING',
            130: 'SMALLINT',
            132: 'INT',
            133: 'INT',
            134: 'BIGINT',
            192: 'TIMESTAMP'
        }
        ddl = ["CREATE TABLE %s(" % tname]
        firstfield = True
        for row in result.fetchall():
            if firstfield:
                ddl.append("    %s %s" % (row[1].strip(), 
                                          fieldtype[row[2]]))
                firstfield = False
            else:
                ddl.append("   ,%s %s" % (row[1].strip(), 
                                          fieldtype[row[2]]))

        ddl.extend([")",
                    "ROW FORMAT DELIMITED",
                    "FIELDS TERMINATED BY'|'",
                    "STORED AS TEXTFILE;"])

        return "\n".join(ddl)

    def getall(self, sqlstr, *params):
        """ _runsql a query, return generator of Rows """
        result = self._runsql(sqlstr, *params)

        return {'fields': [desc[0] for desc in result.description],
                'rows': result.fetchall()}

    def getone(self, sqlstr, *params):
        """ execute a query, return the first row """
        result = self._runsql(sqlstr, *params)

        return {'fields': [desc[0] for desc in result.description],
                'row': result.fetchone()}

    def dumpdata(self, table=None, sql=None, *params):
        if table is None and sql is None:
            return

        if sql is None:
            sql = "SELECT * FROM %s" % table
            params = []
        
        result = self._runsql(sql, *params)

        if table is None:
            table = 'TABLE'

        with file('%s.header' % table, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([desc[0] for desc in result.description])

        with file('%s.csv' % table, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for row in result.fetchall():
                writer.writerow(row)