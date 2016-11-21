Report = 'WMS/workload sums for 10-60 Min Intervals'
SQL = '''SELECT
    DATE_TIME
,   NODE_ID
,   TRIM(IP_ADDRESS_ID     ) AS IP_ADDRESS_ID
,   TRIM(PROCESS_NAME      ) AS PROCESS_NAME
,   WMS_NODE_ID
,   TRIM(WMS_NODE_NAME     ) AS WMS_NODE_NAME
,   TRIM(WMS_NODE_LIST     ) AS WMS_NODE_LIST
,   SUM(TOTAL_QUERIES      ) AS SUM_TOTAL_QUERIES
,   SUM(TOTAL_EXEC         ) AS SUM_TOTAL_EXEC
,   SUM(TOTAL_WAIT         ) AS SUM_TOTAL_WAIT
,   SUM(TOTAL_HOLD         ) AS SUM_TOTAL_HOLD
,   SUM(TOTAL_SUSPEND      ) AS SUM_TOTAL_SUSPEND
,   SUM(TOTAL_REJECT       ) AS SUM_TOTAL_REJECT
,   SUM(TOTAL_CANCEL       ) AS SUM_TOTAL_CANCEL
,   SUM(TOTAL_COMPLETE     ) AS SUM_TOTAL_COMPLETE
,   SUM(AVG_EXEC_SECS      ) AS SUM_AVG_EXEC_SECS
,   SUM(AVG_WAIT_SECS      ) AS SUM_AVG_WAIT_SECS
,   SUM(AVG_HOLD_SECS      ) AS SUM_AVG_HOLD_SECS
,   SUM(AVG_SUSPEND_SECS   ) AS SUM_AVG_SUSPEND_SECS
,   SUM(CONN_RULE_TRIGGERED) AS SUM_CONN_RULE_TRIGGERED
,   SUM(COMP_RULE_TRIGGERED) AS SUM_COMP_RULE_TRIGGERED
,   SUM(EXEC_RULE_TRIGGERED) AS SUM_EXEC_RULE_TRIGGERED
FROM (SELECT
        CASE '%d'
        WHEN '10' THEN
            CAST(CAST((GEN_TS_LCT) AS DATE) AS TIMESTAMP(0))
            + CAST(HOUR((GEN_TS_LCT)) AS INTERVAL HOUR)
            + CAST(CAST(SUBSTRING(CAST(GEN_TS_LCT AS CHAR(26)) FROM 15 FOR 1) AS INTEGER)*10 AS INTERVAL MINUTE)
        WHEN '60' THEN
            CAST(CAST((GEN_TS_LCT) AS DATE) AS TIMESTAMP(0))
            + CAST(HOUR((GEN_TS_LCT)) AS INTERVAL HOUR)
        END
        AS DATE_TIME
    ,   *
    FROM
        MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_WORKLOAD_SUMMARY_1
    WHERE
        GEN_TS_LCT BETWEEN ? AND ?
    ) AS A
GROUP BY
    DATE_TIME
,   NODE_ID
,   IP_ADDRESS_ID
,   PROCESS_NAME
,   WMS_NODE_ID
,   WMS_NODE_NAME
,   WMS_NODE_LIST
ORDER BY
    DATE_TIME
,   NODE_ID
FOR READ UNCOMMITTED ACCESS IN SHARE MODE''' % 10


