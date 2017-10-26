
import pyodbc


def get_connection(options):
    return ODBCConnection(options)


class ODBCConnection(object):
    def __init__(self, options):
        connstr = ';'.join(['Dsn=%s' % options.dsn,
                            'Uid=%s' % options.user,
                            'Pwd=%s' % options.password,
                            'App=%s' % getattr(options, 'Application', 'HPDM'),
                            'Retrycount=3',
                            'Retrytime=5000',
			    'charset=utf16',
                            ])
        self.__conn = pyodbc.connect(connstr)

    def cursor(self):
        return self.__conn.cursor()

    def get_tables(self, catalog, schema=None, table=None):
        cursor = self.cursor()
        params = {'catalog': catalog,
                  'tableType': 'TABLE'}
        if table:
            params['table'] = table
        if schema:
            params['schema'] = schema

        return cursor.tables(**params).fetchall()

    def get_columns(self, catalog, schema=None, table=None, column=None):
        cursor = self.cursor()
        params = {'catalog': catalog}
        if table:
            params['table'] = table
        if schema:
            params['schema'] = schema
        if column:
            params['column'] = schema

        return cursor.columns(**params).fetchall()


