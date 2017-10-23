
from __future__ import print_function

from collections import namedtuple
import json

from models import Seaquest

class Opt():
    pass

def print_rows(fields, rows):
    RowType = namedtuple("Row", fields)
    for row in rows:
        print(*row)
        print(json.dumps(RowType(*row)._asdict(), indent=4))


opts = Opt()
opts.classpath = "/home/caiche/lib/java/hpt4jdbc.jar"
opts.driver = "com.hp.jdbc.HPT4Driver"
opts.server = "sqws114.houston.hp.com"
opts.port = 18650
opts.dsn = "sqdev11"
opts.user = "sqdev11"
opts.password = "redhat06"

try:
    import jdbc
    conn = jdbc.get_connection('jdbc:hpt4jdbc://%s:%d' % (opts.server, opts.port),opts)
except ImportError:
    import odbc
    conn = odbc.get_connection(opts)

db = Seaquest.Database(conn)

#db.pullobjs()
#db.dumpobjs()

#print_rows(*db.get_table_partitions(
#    "MANAGEABILITY", "INSTANCE_REPOSITORY", "METRIC_NODE_TABLE"))

#print_rows(*db.get_table_files(
#    "MANAGEABILITY", "INSTANCE_REPOSITORY"))

#print_rows(*db.get_single_partition_objs("manageability"))
print_rows(*db.get_all_schemas())