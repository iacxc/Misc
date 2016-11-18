#!/usr/bin/python -O

from __future__ import print_function

import pyodbc

import sqlstmts


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

g_test = True


class SQDB(object):
    def __init__(self, dsn, debug=True):
        connstr = 'DSN=%(dsn)s;UID=%(uid)s;PWD=%(pwd)s' % {
            'dsn': dsn, 'uid': 'cheng-xin.cai@hpe.com', 'pwd': 'Iam@hpe.com'}

        self.__db = pyodbc.connect(connstr)
        self.__debug = debug

        self.__catalogs = []

    @property
    def catalogs(self):
        return self.__catalogs

    def add_catalog(self, catalog):
        self.__catalogs.append(catalog)

    def log_debug(self, msg):
        if self.__debug:
            print(msg)

    def _getddl(self, tname):
        global g_test

        if not g_test:
            return

        g_test = False
        cursor = self.__db.cursor()
        sqlstr = 'SHOWDDL %s' % tname
        self.log_debug(sqlstr)

        cursor.execute(sqlstr)
        return '\n'.join(row[0] for row in cursor.fetchall())

    def _fill_struct(self):

        # first get all catalogs and schemas
        sqlstr = sqlstmts.get_all_schemas()
        cursor = self.__db.cursor()
        self.log_debug(sqlstr)

        cursor.execute(sqlstr)
        curcat = None
        for row in cursor.fetchall():
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
            sqlstr = sqlstmts.get_tables(catalog.name)

            cursor = self.__db.cursor()
            self.log_debug(sqlstr)

            cursor.execute(sqlstr)
            for row in cursor.fetchall():
                schname, tname = row[0].split('.', 1)
                schname = '%s.%s' % (catalog.name, schname)

                table = Table(schname, tname)
                table.set_ddl(self._getddl(table.name))
                catalog.get_schema(schname).add_table(table)

    def pull(self):
        self._fill_struct()

    def dumpstru(self, ddl=False):
        for cat in self.catalogs:
            print(cat)
            for sch in cat.schemas:
                print('    %s' % sch)
                for table in sch.tables:
                    print('        %s' % table)
                    if ddl:
                        print(table.ddl)


if __name__ == '__main__':
    db = SQDB('EMR_EDW')
    db.pull()
    db.dumpstru(True)
