
from __future__ import print_function

import os
import sys

from models import Seaquest, sqlstmt
import util


if __name__ == "__main__":
    from optparse import OptionParser

    ACTIONS = ('events', 'se_delta')

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
    parser.add_option("--action", 
        help="action to dump, must provide start and end time")
    parser.add_option("--start", 
        help="start time, format is YYYY-mm-dd HH:MM:SS.XXX")
    parser.add_option("--end", 
        help="end time, format is YYYY-mm-dd HH:MM:SS.XXX")

    (opts, args) = parser.parse_args()

    if opts.user is None or opts.password is None:
        print("either user or password can not be empty")
        sys.exit(1)

    if opts.table is None and opts.sql is None:
        print("both table and sql can not be empty")
        sys.exit(1)

    if opts.action:
        if not opts.action in ACTIONS:
            print("Invalid 'action' %s provided" % opts.action)
            sys.exit(2)
        if  (opts.start or opts.end) is None:
            print("either start and end cannot be empty when action provided")
            sys.exit(1)

        if opts.sql:
            print("Warning, sql will be overwritten when action provided")

    db = Seaquest.Database(opts)

#       util.time_it(db.pullobjs)
#       util.time_it(db.dumpobjs)

    if opts.table:
        table = opts.table
        catalog, schema = '%', '%'

        if opts.table.count('.') == 1:
            schema, table = opts.table.split('.')
        elif opts.table.count('.') == 2:
            catalog, schema, table = opts.table.split('.')

        print(util.time_it(db.getddl, catalog, schema, table))
    else:
        table = None

    if opts.action:
        from datetime import datetime
        opts.sql = sqlstmt.get_sql(opts.action)
        start_dt = datetime.strptime(opts.start, '%Y-%m-%d %H:%M:%S.%f')
        end_dt = datetime.strptime(opts.end, '%Y-%m-%d %H:%M:%S.%f')
        params = [start_dt, end_dt]
    else:
        params = []

    util.time_it(db.dumpdata, table, opts.sql, *params)

