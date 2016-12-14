#!/bin/sh

jython dumpData.py --classpath /h/caiche/lib/java/hpt4jdbc.jar \
    --driver com.hp.jdbc.HPT4Driver \
    --server zeo.houston.hp.com \
    --dsn ZEO_EDW \
    --user cheng-xin.cai@hpe.com \
    --password Iam@hpe.com \
    --table events \
    --action events \
    --start "2016-11-01 00:00:00.000" \
    --end "2016-11-02 00:00:00.000" 


