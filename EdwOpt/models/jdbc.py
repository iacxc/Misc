
import java
import os
import sys

from com.ziclix.python.sql import PyConnection


class DriverNotFound(Exception):
    pass


class ServerError(Exception):
    pass


def get_connection(url, options):
    classpath = os.getenv('CLASSPATH')

    if getattr(options, 'classpath', None) is not None:
        if classpath:
            classpath = ':'.join([classpath,  options.classpath])
        else:
            classpath = options.classpath

    if classpath:
        for path in classpath.split(':'):
            if not path in sys.path:
                sys.path.append(path)

    try:
        Driver = __import__(options.driver, 
                            globals(), locals(), options.driver)
    except ImportError:
        raise DriverNotFound('Cannot import %s, please check CLASSPATH' % 
                              options.driver)

    props = java.util.Properties()
    for attr in ('user', 'password'):
        if hasattr(options, attr):
            props.put(attr, getattr(options, attr))
    if hasattr(options, 'dsn'):
        props.put('serverDataSource', options.dsn)

    conn = Driver().connect(url, props)

    return PyConnection(conn)


def get_tables(cursor, catalog, schema='%', table='%'):
    cursor.tables(catalog, schema, table, ['TABLE'])

    return cursor.fetchall()


def get_columns(cursor, catalog, schema='%', table='%', column='%'):
    cursor.columns(catalog, schema, table, column)

    return cursor.fetchall()