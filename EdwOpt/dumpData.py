
from __future__ import print_function

import os
import sys
from models import SqDB

        
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
        db = SqDB.Database(opts)
        if opts.table.count('.') == 0:
            catalog, schema, table = \
                'manageability', 'instance_repository', opts.table
        elif opts.table.count('.') == 1:
            catalog = 'manageability'
            schema, table = opts.table.split('.')
        else:
            catalog, schema, table = opts.table.split('.')

        print(db.getddl(catalog, table))

        db.dumpdata(table, opts.sql)
    except Exception as e:
        print(e)
        parser.print_help()

