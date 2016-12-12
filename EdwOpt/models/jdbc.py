
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
    if options.classpath:
        if classpath:
            classpath = classpath + ':' + options.classpath
        else:
            classpath = options.classpath

    if classpath:
        for path in classpath.split(':'):
            sys.path.append(path)

    try:
        Driver = __import__(options.driver, 
                            globals(), locals(), options.driver)
    except ImportError:
        raise DriverNotFound('Cannot import %s, please check CLASSPATH' % 
                              options.driver)

    props = java.util.Properties()
    props.put('user', options.user)
    props.put('password', options.password)
    props.put('serverDataSource', options.dsn)

    if options.server is None:
        raise ServerError('Server name cannot be empty')

    conn = Driver().connect(url, props)

    return PyConnection(conn)