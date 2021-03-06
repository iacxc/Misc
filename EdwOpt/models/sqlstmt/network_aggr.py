Report = 'Network/Aggregates 1-10-60 Min Intervals'
SQL = '''SELECT
    DATE_TIME
,   TRIM(INTERFACE         )                   AS INTERFACE
,   CAST(SUM(RCV_BYTES     ) AS NUMERIC(18,0)) AS SUM_RCV_BYTES
,   CAST(SUM(RCV_PACKETS   ) AS NUMERIC(18,0)) AS SUM_RCV_PACKETS
,   CAST(SUM(RCV_ERRS      ) AS NUMERIC(18,0)) AS SUM_RCV_ERRS
,   CAST(SUM(RCV_DROP      ) AS NUMERIC(18,0)) AS SUM_RCV_DROP
,   CAST(SUM(RCV_FIFO      ) AS NUMERIC(18,0)) AS SUM_RCV_FIFO
,   CAST(SUM(RCV_FRAME     ) AS NUMERIC(18,0)) AS SUM_RCV_FRAME
,   CAST(SUM(RCV_COMPRESSED) AS NUMERIC(18,0)) AS SUM_RCV_COMPRESSED
,   CAST(SUM(RCV_MULTICAST ) AS NUMERIC(18,0)) AS SUM_RCV_MULTICAST
,   CAST(SUM(TXN_BYTES     ) AS NUMERIC(18,0)) AS SUM_TXN_BYTES
,   CAST(SUM(TXN_PACKETS   ) AS NUMERIC(18,0)) AS SUM_TXN_PACKETS
,   CAST(SUM(TXN_ERRS      ) AS NUMERIC(18,0)) AS SUM_TXN_ERRS
,   CAST(SUM(TXN_DROP      ) AS NUMERIC(18,0)) AS SUM_TXN_DROP
,   CAST(SUM(TXN_FIFO      ) AS NUMERIC(18,0)) AS SUM_TXN_FIFO
,   CAST(SUM(TXN_COLLS     ) AS NUMERIC(18,0)) AS SUM_TXN_COLLS
,   CAST(SUM(TXN_CARRIER   ) AS NUMERIC(18,0)) AS SUM_TXN_CARRIER
,   CAST(SUM(TXN_COMPRESSED) AS NUMERIC(18,0)) AS SUM_TXN_COMPRESSED
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
,       INTERFACE
,       RCV_BYTES
,       RCV_PACKETS
,       RCV_ERRS
,       RCV_DROP
,       RCV_FIFO
,       RCV_FRAME
,       RCV_COMPRESSED
,       RCV_MULTICAST
,       TXN_BYTES
,       TXN_PACKETS
,       TXN_ERRS
,       TXN_DROP
,       TXN_FIFO
,       TXN_COLLS
,       TXN_CARRIER
,       TXN_COMPRESSED
    FROM
        MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_NETWORK_1
    WHERE
        GEN_TS_LCT BETWEEN ? AND ?
    ) AS A
GROUP BY
    DATE_TIME
   ,INTERFACE
ORDER BY
   DATE_TIME
  ,INTERFACE
FOR READ UNCOMMITTED ACCESS IN SHARE MODE''' % 1

