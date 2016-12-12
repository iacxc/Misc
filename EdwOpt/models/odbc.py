
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
