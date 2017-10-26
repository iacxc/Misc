
from __future__ import print_function

from collections import namedtuple
import json

from models import Seaquest

class Opt():
    pass

def wms_status(wms):
    fields, row = wms.status()

    RowType = namedtuple("Row", fields)
    print(json.dumps(RowType(*row)._asdict(), indent=4))


def wms_query(wms):
    fields, rows = wms.status_query()

    RowType = namedtuple("Row", fields)
    for row in rows:
        queryid = row[0]
        print(json.dumps(RowType(*row)._asdict(), indent=4))

    print(queryid)
    fields, rows = wms.status_query(queryid)
    print(json.dumps(RowType(*rows[0])._asdict(), indent=4))


def wms_service(wms):
    fields, rows = wms.status_service()

    RowType = namedtuple("Row", fields)
    for row in rows:
        service_name = row[0]
        print(json.dumps(RowType(*row)._asdict(), indent=4))

    print(service_name)
    fields, rows = wms.status_service(service_name)
    print(json.dumps(RowType(*rows[0])._asdict(), indent=4))


def wms_rule(wms):
    fields, rows = wms.status_rule()

    RowType = namedtuple("Row", fields)
    for row in rows:
        rule = row[1]
        print(json.dumps(RowType(*row)._asdict(), indent=4))

    print(rule)

    fields, rows = wms.status_rule(rule)
    for row in rows:
        print(json.dumps(RowType(*row)._asdict(), indent=4))


opts = Opt()
opts.classpath = "/home/caiche/lib/java/hpt4jdbc.jar"
opts.driver = "com.hp.jdbc.HPT4Driver"
opts.server = "sqws114.houston.hp.com"
opts.port = 18650
opts.dsn = "sqdev11"
opts.user = "sqdev11"
opts.password = "redhat06"

import odbc
conn = odbc.get_connection(opts)
db = Seaquest.Database(conn, False)

db.runsql("select count(*) from manageability.instance_repository.metric_node_1")

wms = Seaquest.WMSSystem(conn)

#wms_status(wms)
#wms_query(wms)
wms_service(wms)
#wms_rule(wms)
