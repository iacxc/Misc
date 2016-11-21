Report = 'Queries/Concurrent Max 01 minute increments - Emtire System'
SQL = '''SELECT
    CAST(CAST((TIMEST) AS DATE) AS TIMESTAMP(0))
        + CAST(HOUR((TIMEST)) AS INTERVAL HOUR)
        + CAST(CAST(SUBSTRING(CAST(TIMEST AS CHAR(26)) FROM 15 FOR 2) AS INTEGER) AS INTERVAL MINUTE)
    AS DATE_TIME
,   MAX(CONC) AS MAX_CONCURRENCY
FROM (SELECT A.EV AS TIMEST,
             SUM(A.D) OVER (ORDER BY A.EV ROWS UNBOUNDED PRECEDING) AS CONC
    FROM (SELECT
            EXEC_START_LCT_TS AS EV,
            (1)               AS D
        FROM (SELECT
                MIN(EXEC_START_LCT_TS) EXEC_START_LCT_TS ,
                MAX(EXEC_END_LCT_TS) EXEC_END_LCT_TS
            FROM
                MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_QUERY_3
            WHERE
                EXEC_START_LCT_TS BETWEEN ? AND ?
                AND EXEC_START_LCT_TS IS NOT NULL
                AND EXEC_END_LCT_TS IS NOT NULL
            GROUP BY
                QUERY_ID
            ) STRT
        WHERE
            EXEC_END_LCT_TS IS NOT NULL
        UNION ALL
        SELECT
            EXEC_END_LCT_TS AS EV,
            (-1)            AS D
        FROM (SELECT
                MIN(EXEC_START_LCT_TS) EXEC_START_LCT_TS ,
                MAX(EXEC_END_LCT_TS) EXEC_END_LCT_TS
            FROM
                MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_QUERY_3
            WHERE
                EXEC_START_LCT_TS BETWEEN ? AND ?
                AND EXEC_START_LCT_TS IS NOT NULL
                AND EXEC_END_LCT_TS IS NOT NULL
            GROUP BY
                QUERY_ID
            ) END_TIME
        WHERE
            EXEC_END_LCT_TS IS NOT NULL
        ) A
    ) C
    GROUP BY 1
ORDER BY 1
READ UNCOMMITTED ACCESS IN SHARE MODE'''


