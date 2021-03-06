Report = 'Session/Slow LDAPs 1-10-60 Min Intervals'
SQL = '''SELECT
    DATE_TIME
,   NODE_ID
,   TRIM(VOLUME_NAME)                          AS "VOLUME_NAME"
,   SUM(REQUESTS)                              AS "SUM_REQUESTS"
,   SUM(SERVICE_TIME)                          AS "SUM_SERVICE_TIME"
,   SUM(READDATA_CALLS)                        AS "SUM_READDATA_CALLS"
,   SUM(READDATA_TIME)                         AS "SUM_READDATA_TIME"
,   SUM(READDATA_BYTES)                        AS "SUM_READDATA_BYTES"
,   SUM(REPLY_CALLS)                           AS "SUM_REPLY_CALLS"
,   SUM(REPLY_TIME)                            AS "SUM_REPLY_TIME"
,   SUM(REPLY_BYTES)                           AS "SUM_REPLY_BYTES"
,   SUM(DISK_IOS)                              AS "SUM_DISK_IOS"
,   SUM(IO_TRANSIT_WAITS)                      AS "SUM_IO_TRANSIT_WAITS"
,   SUM(REQUEST_IO_WAIT_TIME)                  AS "SUM_REQUEST_IO_WAIT_TIME"
,   SUM(REQUEST_MESSAGES)                      AS "SUM_REQUEST_MESSAGES"
,   SUM(REQUEST_MESSAGE_WAIT_TIME)             AS "SUM_REQUEST_MESSAGE_WAIT_TIME"
,   SUM(REQUEST_SEMAPHORE_WAITS)               AS "SUM_REQUEST_SEMAPHORE_WAITS"
,   SUM(REQUEST_SEM_WAIT_TIME)                 AS "SUM_REQUEST_SEM_WAIT_TIME"
,   SUM(FILESEM_WAIT_TIME)                     AS "SUM_FILESEM_WAIT_TIME"
,   SUM(VOLSEM_WAITS)                          AS "SUM_VOLSEM_WAITS"
,   SUM(VOLSEM_WAIT_TIME)                      AS "SUM_VOLSEM_WAIT_TIME"
,   SUM(CACHESEM_WAIT_TIME)                    AS "SUM_CACHESEM_WAIT_TIME"
,   SUM(REQUESTS_QUEUED)                       AS "SUM_REQUESTS_QUEUED"
,   SUM(REQUEST_QUEUE_TIME)                    AS "SUM_REQUEST_QUEUE_TIME"
,   SUM(REQUESTS_QUEUED_PREEMPT)               AS "SUM_REQUESTS_QUEUED_PREEMPT"
,   SUM(REQUESTS_QUEUED_LOCK)                  AS "SUM_REQUESTS_QUEUED_LOCK"
,   SUM(LOCK_WAITS)                            AS "SUM_LOCK_WAITS"
,   SUM(LOCK_WAIT_TIME)                        AS "SUM_LOCK_WAIT_TIME"
,   SUM(LOCK_WAIT_TIMEOUTS)                    AS "SUM_LOCK_WAIT_TIMEOUTS"
,   SUM(REQUESTS_DEFERRED)                     AS "SUM_REQUESTS_DEFERRED"
,   SUM(DEFERRAL_TIME)                         AS "SUM_DEFERRAL_TIME"
,   SUM(OPEN_REQUESTS)                         AS "SUM_OPEN_REQUESTS"
,   SUM(CLOSE_REQUESTS)                        AS "SUM_CLOSE_REQUESTS"
,   SUM(ASE_AUDIT_REQUESTS)                    AS "SUM_ASE_AUDIT_REQUESTS"
,   SUM(ASE_SERVICE_TIME)                      AS "SUM_ASE_SERVICE_TIME"
,   SUM(DV_MESSAGES)                           AS "SUM_DV_MESSAGES"
,   SUM(CHECKPOINTS)                           AS "SUM_CHECKPOINTS"
,   SUM(DV_MESSAGE_TIME)                       AS "SUM_DV_MESSAGE_TIME"
,   SUM(CHECKPOINT_TIME)                       AS "SUM_CHECKPOINT_TIME"
,   SUM(DV_MESSAGE_BYTES)                      AS "SUM_DV_MESSAGE_BYTES"
,   SUM(CHECKPOINT_BYTES)                      AS "SUM_CHECKPOINT_BYTES"
,   SUM(SESSIONS)                              AS "SUM_SESSIONS"
,   SUM(SESSION_FIRST_REQUESTS)                AS "SUM_SESSION_FIRST_REQUESTS"
,   SUM(SESSION_NEXT_REQUESTS)                 AS "SUM_SESSION_NEXT_REQUESTS"
,   SUM(SESSION_REQUEST_TIME)                  AS "SUM_SESSION_REQUEST_TIME"
,   SUM(SESSION_SERVICE_TIME)                  AS "SUM_SESSION_SERVICE_TIME"
,   SUM(TRANSACTIONS)                          AS "SUM_TRANSACTIONS"
,   SUM(TRANSACTION_TIME)                      AS "SUM_TRANSACTION_TIME"
,   SUM(TRANSACTION_ABORTS)                    AS "SUM_TRANSACTION_ABORTS"
,   SUM(INSERTS)                               AS "SUM_INSERTS"
,   SUM(UPDATES)                               AS "SUM_UPDATES"
,   SUM(DELETES)                               AS "SUM_DELETES"
,   SUM(FETCHES)                               AS "SUM_FETCHES"
,   SUM(POSITIONS)                             AS "SUM_POSITIONS"
,   SUM(VSBB_INSERTS)                          AS "SUM_VSBB_INSERTS"
,   SUM(SIDETREE_INSERTS)                      AS "SUM_SIDETREE_INSERTS"
,   SUM(FASTPATH_FETCHES)                      AS "SUM_FASTPATH_FETCHES"
,   SUM(FASTPATH_INSERTS)                      AS "SUM_FASTPATH_INSERTS"
,   SUM(FASTPATH_DELETES)                      AS "SUM_FASTPATH_DELETES"
,   SUM(FASTPATH_UPDATES)                      AS "SUM_FASTPATH_UPDATES"
,   SUM(FASTPATH_BULK_FETCHES)                 AS "SUM_FASTPATH_BULK_FETCHES"
,   SUM(CACHE_CALLS)                           AS "SUM_CACHE_CALLS"
,   SUM(CACHE_SERVICE_TIME)                    AS "SUM_CACHE_SERVICE_TIME"
,   SUM(PREFETCHES)                            AS "SUM_PREFETCHES"
,   SUM(LOCK_CALLS)                            AS "SUM_LOCK_CALLS"
,   SUM(LOCK_SERVICE_TIME)                     AS "SUM_LOCK_SERVICE_TIME"
,   SUM(AC_CALLS)                              AS "SUM_AC_CALLS"
,   SUM(AC_SERVICE_TIME)                       AS "SUM_AC_SERVICE_TIME"
,   SUM(SAVEPOINTS)                            AS "SUM_SAVEPOINTS"
,   SUM(SAVEPOINT_ROLLBACKS)                   AS "SUM_SAVEPOINT_ROLLBACKS"
,   SUM(DATA_PARITY_ERRORS)                    AS "SUM_DATA_PARITY_ERRORS"
,   SUM(RECOVERED_PARITY_ERRORS)               AS "SUM_RECOVERED_PARITY_ERRORS"
,   SUM(CMAP_PARITY_ERRORS)                    AS "SUM_CMAP_PARITY_ERRORS"
,   SUM(HARDWARE_COMPRESSION_BUSY_ERRORS)      AS "SUM_HARDWARE_COMPRESSION_BUSY_ERRORS"
,   SUM(HARDWARE_COMPRESSION_USING_SOFTWARE)   AS "SUM_HARDWARE_COMPRESSION_USING_SOFTWARE"
,   SUM(HARDWARE_COMP_SOFTWARE)                AS "SUM_HARDWARE_COMP_SOFTWARE"
,   SUM(HARDWARE_DCOMP_SOFTWARE)               AS "SUM_HARDWARE_DCOMP_SOFTWARE"
,   SUM(COMPRESSIONS)                          AS "SUM_COMPRESSIONS"
,   SUM(COMP_BLOCKS)                           AS "SUM_COMP_BLOCKS"
,   SUM(DECOMPRESSIONS)                        AS "SUM_DECOMPRESSIONS"
,   SUM(DECOMP_BLOCKS)                         AS "SUM_DECOMP_BLOCKS"
,   SUM(COMPRESSION_SERVICE_TIME)              AS "SUM_COMPRESSION_SERVICE_TIME"
,   SUM(DECOMPRESSION_SERVICE_TIME)            AS "SUM_DECOMPRESSION_SERVICE_TIME"
,   SUM(COMP_SECTORS)                          AS "SUM_COMP_SECTORS"
,   SUM(DECOMP_SECTORS)                        AS "SUM_DECOMP_SECTORS"
,   SUM(PRIMARY_PARITY_ERRORS)                 AS "SUM_PRIMARY_PARITY_ERRORS"
,   SUM(MIRROR_PARITY_ERRORS)                  AS "SUM_MIRROR_PARITY_ERRORS"
,   SUM(PRIMARY_IOS)                           AS "SUM_PRIMARY_IOS"
,   SUM(MIRROR_IOS)                            AS "SUM_MIRROR_IOS"
,   SUM(PRIMARY_SERVICE_TIME)                  AS "SUM_PRIMARY_SERVICE_TIME"
,   SUM(MIRROR_SERVICE_TIME)                   AS "SUM_MIRROR_SERVICE_TIME"
,   SUM(ELAPSED_TIME)                          AS "SUM_ELAPSED_TIME"
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
    ,   *
    FROM MANAGEABILITY.INSTANCE_REPOSITORY.METRIC_SE_DELTA_1
    WHERE
        GEN_TS_LCT BETWEEN ? and ?
    ) AS A
GROUP BY
    DATE_TIME
,   NODE_ID
,   VOLUME_NAME
ORDER BY
    DATE_TIME
,   NODE_ID
,   VOLUME_NAME
FOR READ UNCOMMITTED ACCESS IN SHARE MODE''' % 1

