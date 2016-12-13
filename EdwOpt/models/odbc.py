
import pyodbc


def get_connection(options):
    connstr = ';'.join(['Dsn=%s' % options.dsn,
                        'Uid=%s' % options.user,
                        'Pwd=%s' % options.password,
                        'App=%s' % 'Metric Miner',
                        'Retrycount=3',
                        'Retrytime=5000',
                        ])

    conn = pyodbc.connect(connstr)

    return conn


def get_tables(cursor, catalog, schema=None, table=None):
    params = {'catalog': catalog,
              'tableType': 'TABLE'}
    if table:
        params['table'] = table
    if schema:
        params['schema'] = schema

    return cursor.tables(**params).fetchall()


def get_columns(cursor, catalog, schema=None, table=None, column=None):
    params = {'catalog': catalog}
    if table:
        params['table'] = table
    if schema:
        params['schema'] = schema
    if column:
        params['column'] = schema

    return cursor.columns(**params).fetchall()
