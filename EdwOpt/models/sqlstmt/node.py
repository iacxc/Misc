Report = 'Node/Aggregates 1-10-60 Min Intervals'
SQL = '''SELECT
    DATE_TIME
,   IP_ADDRESS_ID
,   CAST(AVG(AVG_TOTAL   ) AS DEC(6,2)) AS "AVG_TOTAL_BUSY"
,   CAST(AVG(AVG_USER    ) AS DEC(6,2)) AS "AVG_USER_BUSY"
,   CAST(AVG(AVG_NICE    ) AS DEC(6,2)) AS "AVG_NICE_BUSY"
,   CAST(AVG(AVG_SYSTEM  ) AS DEC(6,2)) AS "AVG_SYSTEM_BUSY"
,   CAST(AVG(AVG_IDLE    ) AS DEC(6,2)) AS "AVG_IDLE_BUSY"
,   CAST(AVG(AVG_IOWAIT  ) AS DEC(6,2)) AS "AVG_IOWAIT_BUSY"
,   CAST(AVG(AVG_IRQ     ) AS DEC(6,2)) AS "AVG_IRQ_BUSY"
,   CAST(AVG(AVG_SOFT_IRQ) AS DEC(6,2)) AS "AVG_SOFT_IRQ_BUSY"
,   CAST(AVG(AVG_STEAL   ) AS DEC(6,2)) AS "AVG_STEAL_BUSY"
,   CAST(AVG(AVG_GUEST   ) AS DEC(6,2)) AS "AVG_GUEST_BUSY"
FROM (SELECT
        CASE '%d'
        WHEN '1' THEN
            CAST(CAST((GEN_TS_LCT) AS DATE) AS TIMESTAMP(0))
            + CAST(HOUR((GEN_TS_LCT)) AS INTERVAL HOUR)
            + CAST(CAST(SUBSTRING(CAST(GEN_TS_LCT AS CHAR(26)) FROM 15 FOR 2) AS INTEGER) AS INTERVAL MINUTE)
        WHEN '10' THEN
            CAST(CAST((GEN_TS_LCT) AS DATE) AS TIMESTAMP(0))
            + CAST(HOUR((GEN_TS_LCT)) AS INTERVAL HOUR)
            + CAST(CAST(SUBSTRING(CAST(GEN_TS_LCT AS CHAR(26)) FROM 15 FOR 1) AS INTEGER)*10 AS INTERVAL MINUTE)
        WHEN '60' THEN
            CAST(CAST((GEN_TS_LCT) AS DATE) AS TIMESTAMP(0))
            + CAST(HOUR((GEN_TS_LCT)) AS INTERVAL HOUR)
        END
        AS DATE_TIME
,       IP_ADDRESS_ID
,       AVG_TOTAL
,       AVG_USER
,       AVG_NICE
,       AVG_SYSTEM
,       AVG_IDLE
,       AVG_IOWAIT
,       AVG_IRQ
,       AVG_SOFT_IRQ
,       AVG_STEAL
,       AVG_GUEST
     FROM
         MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NODE_1
     WHERE
         GEN_TS_LCT BETWEEN ? AND ?
    ) AS A
GROUP BY
    DATE_TIME
,   IP_ADDRESS_ID
ORDER BY
    DATE_TIME
,   IP_ADDRESS_ID
FOR READ UNCOMMITTED ACCESS IN SHARE MODE''' % 1

