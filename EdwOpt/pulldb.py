#!/usr/bin/python -O

from __future__ import print_function

import SqDB


if __name__ == '__main__':
    db = SqDB.Database('EMR_EDW')
    db.pullobjs()
    db.dumpobjs()

    for row in db.getall('Select [ANY 100] * from MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NODE_TABLE'):
        print(row)

    print(db.getddl('NEO.EDW_DBA.MVS_UMD'))
