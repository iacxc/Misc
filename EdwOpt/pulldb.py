#!/usr/bin/python -O

from __future__ import print_function

import SqDB


if __name__ == '__main__':
    db = SqDB.Database('EMR_EDW')
#    db.pullobjs()
#    db.dumpobjs()

#    for row in db.getall('Select [ANY 100] * from MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NODE_TABLE'):
#        print(row)

#    print(db.getddl('NEO.EDW_DBA.MVS_UMD'))
    from datetime import datetime
    sqlstr = """Select [ANY 100] *
from MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NODE_TABLE
where gen_ts_lct between ? and ?
"""
    d1 = datetime(2016, 11, 1, 15, 30)
    d2 = datetime(2016, 11, 2, 5, 30, 0, 999000)

    for row in db.getall(sqlstr, d1, d2):
        print(row)
