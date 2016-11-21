Report = 'Memory/Aggregates 1-10-60 Min Intervals'
SQL = '''SELECT
    DATE_TIME
,   TRIM(IP_ADDRESS_ID          )              AS IP_ADDRESS_ID
,   CAST(AVG(TOTAL/1048576      ) AS DEC(8,2)) AS AVG_GB_TOTAL_MEM
,   CAST(AVG(FREE_MEMORY/1048576) AS DEC(8,2)) AS AVG_GB_FREE_MEM
,   CAST(AVG(BUFFERS/1048576    ) AS DEC(8,2)) AS AVG_GB_BUFFERS_MEM
,   CAST(AVG(CACHED/1048576     ) AS DEC(8,2)) AS AVG_GB_CACHED_MEM
,   CAST(AVG(SWAP_CACHED/1048576) AS DEC(8,2)) AS AVG_GB_SWAP_CACHED_MEM
,   CAST(AVG(ACTIVE/1048576     ) AS DEC(8,2)) AS AVG_GB_ACTIVE_MEM
,   CAST(AVG(INACTIVE/1048576   ) AS DEC(8,2)) AS AVG_GB_INACTIVE_MEM
,   CAST(AVG(SWAP_TOTAL/1048576 ) AS DEC(8,2)) AS AVG_GB_SWAP_TOTAL_MEM
,   CAST(AVG(SWAP_FREE/1048576  ) AS DEC(8,2)) AS AVG_GB_SWAP_FREE_MEM
FROM (SELECT
        CASE '%s'
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
,       TOTAL
,       FREE_MEMORY
,       BUFFERS
,       CACHED
,       SWAP_CACHED
,       ACTIVE
,       INACTIVE
,       SWAP_TOTAL
,       SWAP_FREE
    FROM
        MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_MEMORY_1
    WHERE
      GEN_TS_LCT BETWEEN ? AND ?
    ) AS A
GROUP BY
     DATE_TIME
,   IP_ADDRESS_ID
ORDER BY
     DATE_TIME
,   IP_ADDRESS_ID
FOR READ UNCOMMITTED ACCESS IN SHARE MODE'''

