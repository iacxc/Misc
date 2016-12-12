
from __future__ import print_function

import os
import sys
from models import Seaquest


def time_it(func, *args, **kwargs):
    import time

    t0 = time.time()
    result = func(*args, **kwargs)
    t1 = time.time()

    print("Elapsed time: %.3f" % (t1-t0))

    return result

        
if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-c", "--classpath",  dest="classpath")
    parser.add_option("-d", "--driver",  dest="driver", 
            default="com.hp.jdbc.HPT4Driver")
    parser.add_option("--user",  dest="user", default="cheng-xin.cai@hpe.com",
           help="user name of db server")
    parser.add_option("--password",  dest="password",
           default=os.getenv("HTTP_PASS"),
           help="password of db server")

    parser.add_option("--dsn",  default="Admin_Load_DataSource" )
    parser.add_option("--server")

    parser.add_option("--table",  help="table name")

    parser.add_option("--sql",  help="sql statement")

    (opts, args) = parser.parse_args()

    if opts.user is None or opts.password is None:
        print("either user or password can not be empty")
        sys.exit(1)

    if opts.table is None and opts.sql is None:
        print("both table and sql can not be empty")
        sys.exit(1)

    try:
        db = Seaquest.Database(opts)

#       time_it(db.pullobjs)
#       time_it(db.dumpobjs)

        if opts.table:
            table = opts.table
            catalog, schema = 'manageability', 'instance_repository'

            if opts.table.count('.') == 1:
                schema, table = opts.table.split('.')
            elif opts.table.count('.') == 2:
                catalog, schema, table = opts.table.split('.')

            print(time_it(db.getddl, catalog, table))

#       time_it(db.dumpdata, table, opts.sql)
    except Exception as e:
        print(e)
        parser.print_help()

