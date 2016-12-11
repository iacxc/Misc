
import pyodbc

def get_connection(dsn, user, password):
    connstr = ';'.join(['Dsn=%s' % dsn,
                        'Uid=%s' % user,
                        'Pwd=%s' % password,
                        'App=%s' % 'Metric Miner',
                        'Retrycount=3',
                        'Retrytime=5000',
                        ])

    conn = pyodbc.connect(connstr)

    return conn
