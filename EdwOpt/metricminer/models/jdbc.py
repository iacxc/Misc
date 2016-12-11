
import java
from com.ziclix.python.sql import PyConnection

def get_connection(server, user, password):
    props = java.util.Properties()
    props.put('user', user)
    props.put('password', password)
    props.put('serverDataSource', 'Admin_Load_DataSource')

    import sys
    sys.path.append('c:\caiche\lib\java\hpt4jdbc.jar')

    import com.hp.jdbc.HPT4Driver as Driver
    conn = Driver().connect('jdbc:hpt4jdbc://%s:18650' % server, props)

    return PyConnection(conn)