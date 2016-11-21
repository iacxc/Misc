#!/usr/bin/python -O

from __future__ import print_function

from metricminer.models import SqDB

if __name__ == '__main__':
    db = SqDB.Database('EMR_EDW')
#    db.pullobjs()
#    db.dumpobjs()

#    for row in db.getall('Select [ANY 100] * from MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NODE_TABLE'):
#        print(row)

    print(db.getddl('NEO.EDW_DBA.MVS_UMD'))
    from datetime import datetime
    sqlstr = """
"""
    d1 = datetime(2016, 11, 16, 1, 0, 0)
    d2 = datetime(2016, 11, 17, 1, 0, 0)

    result = db.getall(sqlstr) #, d1, d1)
    for row in result['rows']:
        print(row)
