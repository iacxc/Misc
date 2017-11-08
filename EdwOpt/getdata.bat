

jython dumpData.py --classpath c:\caiche\lib\java\hpt4jdbc.jar ^
    --driver com.hp.jdbc.HPT4Driver ^
    --server zeo.houston.hp.com ^
    --dsn ZEO_EDW ^
    --user %USER% ^
    --password %PASS% ^
    --table se_delta ^
    --action se_delta ^
    --start "2016-11-01 00:00:00.000" ^
    --end "2016-11-02 00:00:00.000" ^


