-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PMT_STS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component SQ_bc_basemoneyreceived, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_basemoneyreceived AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_pmt_ev_id,
                $2  AS lkp_edw_strt_dttm,
                $3  AS lkp_edw_end_dttm,
                $4  AS src_pmt_ev_id,
                $5  AS src_pmt_sys_type_cd,
                $6  AS src_pmt_sts_dttm,
                $7  AS src_pmt_sts_rsn_type_cd,
                $8  AS retired,
                $9  AS rnk,
                $10 AS sourcedata,
                $11 AS targetdata,
                $12 AS ins_upd_flag,
                $13 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          tgt_lkp_pmt_sts.pmt_ev_id     AS lkp_pmt_ev_id,
                                                                  tgt_lkp_pmt_sts.edw_strt_dttm AS lkp_edw_strt_dttm,
                                                                  tgt_lkp_pmt_sts.edw_end_dttm  AS lkp_edw_end_dttm,
                                                                  xlat_src.pmt_ev_id            AS src_pmt_ev_id,
                                                                  xlat_src.pmt_sys_type_cd      AS src_pmt_sys_type_cd,
                                                                  xlat_src.pmt_sts_dttm         AS src_pmt_sts_dttm,
                                                                  xlat_src.pmt_sts_rsn_type_cd  AS src_pmt_sts_rsn_type_cd,
                                                                  xlat_src.retired              AS retired,
                                                                  xlat_src.rnk                  AS rnk,
                                                                  /* --Source Data */
                                                                  cast (trim(cast(coalesce(cast(xlat_src.pmt_sts_dttm AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))) AS VARCHAR(60)))
                                                                                  ||trim(coalesce(cast(xlat_src.pmt_sts_rsn_type_cd AS VARCHAR(50)),''UNK''))
                                                                                  ||trim(xlat_src.pmt_sys_type_cd) AS VARCHAR(1000)) AS sourcedata,
                                                                  /* --Target Data */
                                                                  cast (trim(cast(coalesce(cast(tgt_lkp_pmt_sts.pmt_sts_dttm AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))) AS VARCHAR(60)))
                                                                                  ||trim(coalesce(cast(tgt_lkp_pmt_sts.pmt_sts_rsn_type_cd AS VARCHAR(50)),''UNK''))
                                                                                  ||trim(tgt_lkp_pmt_sts.pmt_sts_type_cd) AS VARCHAR(1000)) AS targetdata,
                                                                  /* -flag */
                                                                  CASE
                                                                                  WHEN targetdata IS NULL THEN ''I''
                                                                                  WHEN targetdata IS NOT NULL
                                                                                  AND             sourcedata = targetdata THEN ''R''
                                                                                  WHEN targetdata IS NOT NULL
                                                                                  AND             sourcedata <> targetdata THEN ''U''
                                                                  END AS ins_upd_flag
                                                  FROM
                                                                  /* ----source query with expression */
                                                                  (
                                                                                  SELECT          src.ev_act_type_code,
                                                                                                  src.key1,
                                                                                                  coalesce(src.SUBTYPE,''UNK'')                              AS SUBTYPE,
                                                                                                  src.sts_dt                                               AS pmt_sts_dttm ,
                                                                                                  coalesce(xlat_pmt_sts_type_cd.tgt_idntftn_val,''UNK'')     AS pmt_sys_type_cd,
                                                                                                  coalesce(xlat_pmt_sts_rsn_type_cd.tgt_idntftn_val,''UNK'') AS pmt_sts_rsn_type_cd,
                                                                                                  ev_id_lkp.ev_id                                          AS pmt_ev_id,
                                                                                                  src.retired,
                                                                                                  src.rnk
                                                                                  FROM
                                                                                                  /*select * from*/
                                                                                                  (
                                                                                                           SELECT   ev_act_type_code,
                                                                                                                    key1,
                                                                                                                    SUBTYPE,
                                                                                                                    sts_dt,
                                                                                                                    sts_type_code,
                                                                                                                    sts_rsn_cd,
                                                                                                                    retired,
                                                                                                                    rank() over (PARTITION BY ev_act_type_code, key1, SUBTYPE ORDER BY sts_dt ) AS rnk
                                                                                                           FROM     (
                                                                                                                                    /***************************bc_basemoneyreceived****************************/
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE14''                                AS ev_act_type_code ,
                                                                                                                                                    cast(bc_basemoneyreceived.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                        AS SUBTYPE ,
                                                                                                                                                    receiveddate_stg                                 AS sts_dt ,
                                                                                                                                                    ''PMT_STS_TYPE10''                                 AS sts_type_code ,
                                                                                                                                                    bctl_paymentreversalreason.typecode_stg          AS sts_rsn_cd,
                                                                                                                                                    bc_basemoneyreceived.retired_stg                 AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                    inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                                                                    ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                                                                    left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                                                                                                    ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                                                                                                    WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                                                                            ''DirectBillMoneyRcvd'' ,
                                                                                                                                                                                            ''ZeroDollarDMR'',
                                                                                                                                                                                            ''ZeroDollarReversal'')
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE25''                                AS ev_act_type_code ,
                                                                                                                                                    cast(bc_basemoneyreceived.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                        AS SUBTYPE ,
                                                                                                                                                    reversaldate_stg                                 AS sts_dt ,
                                                                                                                                                    ''PMT_STS_TYPE6''                                  AS sts_type_code ,
                                                                                                                                                    bctl_paymentreversalreason.typecode_stg          AS sts_rsn_cd,
                                                                                                                                                    bc_basemoneyreceived.retired_stg                 AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                    inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                                                                    ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                                                                    left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                                                                                                    ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                                                                                                    WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                                                                            ''DirectBillMoneyRcvd'',
                                                                                                                                                                                            ''ZeroDollarDMR'',
                                                                                                                                                                                            ''ZeroDollarReversal'')
                                                                                                                                    AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE14''                                 AS ev_act_type_code ,
                                                                                                                                                    cast( bc_basemoneyreceived.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                         AS SUBTYPE ,
                                                                                                                                                    bc_unappliedfund.createtime_stg                   AS sts_dt ,
                                                                                                                                                    ''PMT_STS_TYPE7''                                   AS sts_type_code ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                           AS sts_rsn_cd,
                                                                                                                                                    bc_basemoneyreceived.retired_stg                  AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                    inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                                                                    ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                                                                    left outer join db_t_prod_stag.bc_unappliedfund
                                                                                                                                    ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                                                                                                    WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                                                                            ''DirectBillMoneyRcvd'',
                                                                                                                                                                                            ''ZeroDollarDMR'',
                                                                                                                                                                                            ''ZeroDollarReversal'')
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /***************************bc_basemoneyreceived****************************/
                                                                                                                                    UNION
                                                                                                                                    /**************************bc_basedistitem****************************/
                                                                                                                                    SELECT ''EV_ACTVY_TYPE26''                           AS ev_act_type_code ,
                                                                                                                                           cast(bc_basedistitem.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                   AS SUBTYPE ,
                                                                                                                                           bc_basedistitem.executeddate_stg            AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE8''                             AS sts_type_code ,
                                                                                                                                           cast('''' AS VARCHAR(60))                     AS sts_rsn_cd,
                                                                                                                                           bc_basedistitem.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_basedistitem
                                                                                                                                    WHERE  bc_basedistitem.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_basedistitem.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE27''                           AS ev_act_type_code ,
                                                                                                                                           cast(bc_basedistitem.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                   AS SUBTYPE ,
                                                                                                                                           bc_basedistitem.reverseddate_stg            AS sts_dt,
                                                                                                                                           ''PMT_STS_TYPE7''                             AS sts_type_code ,
                                                                                                                                           cast('''' AS VARCHAR(60))                     AS sts_rsn_cd,
                                                                                                                                           bc_basedistitem.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_basedistitem
                                                                                                                                    WHERE  bc_basedistitem.reverseddate_stg IS NOT NULL
                                                                                                                                    AND    bc_basedistitem.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_basedistitem.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /***************************bc_basedistitem****************************/
                                                                                                                                    UNION
                                                                                                                                    /***************************bc_basenonrecdistitem****************************/
                                                                                                                                    SELECT ''EV_ACTVY_TYPE28''                                  AS ev_act_type_code ,
                                                                                                                                           cast( bc_basenonrecdistitem.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                          AS SUBTYPE ,
                                                                                                                                           executeddate_stg                                   AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE21''                                   AS sts_type_code ,
                                                                                                                                           cast('''' AS VARCHAR(60))                            AS sts_rsn_cd,
                                                                                                                                           bc_basenonrecdistitem.retired_stg                  AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_basenonrecdistitem
                                                                                                                                    WHERE  bc_basenonrecdistitem.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_basenonrecdistitem.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE29''                                  AS ev_act_type_code ,
                                                                                                                                           cast( bc_basenonrecdistitem.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                          AS SUBTYPE ,
                                                                                                                                           reverseddate_stg                                   AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE6''                                    AS sts_type_code ,
                                                                                                                                           cast('''' AS VARCHAR(60))                            AS sts_rsn_cd,
                                                                                                                                           bc_basenonrecdistitem.retired_stg                  AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_basenonrecdistitem
                                                                                                                                    WHERE  bc_basenonrecdistitem.reverseddate_stg IS NOT NULL
                                                                                                                                    AND    bc_basenonrecdistitem.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_basenonrecdistitem.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /***************************bc_basenonrecdistitem****************************/
                                                                                                                                    UNION
                                                                                                                                    /***************************bc_suspensepayment****************************/
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE30''                               AS ev_act_type_code ,
                                                                                                                                                    cast( bc_suspensepayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                       AS SUBTYPE ,
                                                                                                                                                    bc_suspensepayment.createtime_stg               AS sts_dt ,
                                                                                                                                                    bctl_suspensepaymentstatus.typecode_stg         AS sts_type_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                         AS sts_rsn_cd,
                                                                                                                                                    bc_suspensepayment.retired_stg                  AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_suspensepayment
                                                                                                                                    left outer join db_t_prod_stag.bctl_suspensepaymentstatus
                                                                                                                                    ON              bctl_suspensepaymentstatus.id_stg=bc_suspensepayment.status_stg
                                                                                                                                    WHERE           bc_suspensepayment.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_suspensepayment.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /***************************bc_suspensepayment****************************/
                                                                                                                                    UNION
                                                                                                                                    /***********************************Outgoing Payment Status********************************************/
                                                                                                                                    SELECT     ''EV_ACTVY_TYPE31''                               AS ev_act_type_code ,
                                                                                                                                               cast( bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                               ''FINANCL''                                       AS SUBTYPE ,
                                                                                                                                               bc_outgoingpayment.updatetime_stg               AS sts_dt ,
                                                                                                                                               bctl_outgoingpaymentstatus.typecode_stg         AS sts_typ_cd ,
                                                                                                                                               cast('''' AS VARCHAR(60))                         AS sts_rsn_cd,
                                                                                                                                               bc_outgoingpayment.retired_stg                  AS retired
                                                                                                                                    FROM       db_t_prod_stag.bc_outgoingpayment
                                                                                                                                    inner join db_t_prod_stag.bctl_outgoingpaymentstatus
                                                                                                                                    ON         bctl_outgoingpaymentstatus.id_stg=bc_outgoingpayment.status_stg
                                                                                                                                    WHERE      bctl_outgoingpaymentstatus.typecode_stg <> ''issued''
                                                                                                                                               /*  ''issued'' is also coming from ''PMT_STS_TYPE23'' below */
                                                                                                                                    AND        bc_outgoingpayment.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND        bc_outgoingpayment.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE31''                              AS ev_act_type_code ,
                                                                                                                                           cast(bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                      AS SUBTYPE ,
                                                                                                                                           bc_outgoingpayment.issuedate_stg               AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE23''                               AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                        AS sts_rsn_cd,
                                                                                                                                           bc_outgoingpayment.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_outgoingpayment
                                                                                                                                    WHERE  bc_outgoingpayment.issuedate_stg IS NOT NULL
                                                                                                                                    AND    bc_outgoingpayment.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_outgoingpayment.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE31''                               AS ev_act_type_code ,
                                                                                                                                           cast( bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                       AS SUBTYPE
                                                                                                                                           /* , bc_outgoingpayment.PaidDate AS sts_dt */
                                                                                                                                           ,
                                                                                                                                           bc_outgoingpayment.cleareddate_alfa_stg AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE24''                        AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                 AS sts_rsn_cd,
                                                                                                                                           bc_outgoingpayment.retired_stg          AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_outgoingpayment
                                                                                                                                           /* where bc_outgoingpayment.PaidDate is not null */
                                                                                                                                    WHERE  bc_outgoingpayment.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_outgoingpayment.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE31''                              AS ev_act_type_code ,
                                                                                                                                           cast(bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                      AS SUBTYPE ,
                                                                                                                                           bc_outgoingpayment.rejecteddate_stg            AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE25''                               AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                        AS sts_rsn_cd,
                                                                                                                                           bc_outgoingpayment.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_outgoingpayment
                                                                                                                                    WHERE  bc_outgoingpayment.rejecteddate_stg IS NOT NULL
                                                                                                                                    AND    bc_outgoingpayment.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_outgoingpayment.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /********************************************************************************************************/
                                                                                                                                    UNION
                                                                                                                                    /***********************************disbursement status**************************************************/
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE32''                           AS ev_act_type_code ,
                                                                                                                                                    cast(bc_disbursement.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                   AS SUBTYPE ,
                                                                                                                                                    bc_disbursement.updatetime_stg              AS sts_dt ,
                                                                                                                                                    bctl_disbursementstatus.typecode_stg        AS sts_typ_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                     AS sts_rsn_cd,
                                                                                                                                                    bc_disbursement.retired_stg                 AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_disbursement
                                                                                                                                    left outer join db_t_prod_stag.bctl_disbursementstatus
                                                                                                                                    ON              bctl_disbursementstatus.id_stg=bc_disbursement.status_stg
                                                                                                                                    WHERE           bc_disbursement.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_disbursement.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE32''                            AS ev_act_type_code ,
                                                                                                                                                    cast( bc_disbursement.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                    AS SUBTYPE ,
                                                                                                                                                    bc_disbursement.approvaldate_stg             AS sts_dt ,
                                                                                                                                                    bctl_approvalstatus.typecode_stg             AS sts_typ_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                      AS sts_rsn_cd,
                                                                                                                                                    bc_disbursement.retired_stg                  AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_disbursement
                                                                                                                                    left outer join db_t_prod_stag.bctl_approvalstatus
                                                                                                                                    ON              bctl_approvalstatus.id_stg=bc_disbursement.approvalstatus_stg
                                                                                                                                    WHERE           bc_disbursement.approvaldate_stg IS NOT NULL
                                                                                                                                    AND             bc_disbursement.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_disbursement.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE32''                           AS ev_act_type_code ,
                                                                                                                                           cast(bc_disbursement.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                   AS SUBTYPE ,
                                                                                                                                           bc_disbursement.closedate_stg               AS sts_dt ,
                                                                                                                                           ''PMT_STS_TYPE12''                            AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                     AS sts_rsn_cd,
                                                                                                                                           bc_disbursement.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_disbursement
                                                                                                                                    WHERE  bc_disbursement.closedate_stg IS NOT NULL
                                                                                                                                    AND    bc_disbursement.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_disbursement.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /*********************************************************************************************************/
                                                                                                                                    UNION
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE33''                        AS ev_act_type_code ,
                                                                                                                                                    cast( bc_writeoff.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                AS SUBTYPE ,
                                                                                                                                                    bc_writeoff.approvaldate_stg             AS sts_dt ,
                                                                                                                                                    bctl_approvalstatus.typecode_stg         AS sts_typ_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                  AS sts_rsn_cd,
                                                                                                                                                    bc_writeoff.retired_stg                  AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_writeoff
                                                                                                                                    left outer join db_t_prod_stag.bctl_approvalstatus
                                                                                                                                    ON              bctl_approvalstatus.id_stg=bc_writeoff.approvalstatus_stg
                                                                                                                                    WHERE           bc_writeoff.approvaldate_stg IS NOT NULL
                                                                                                                                    AND             reversed_stg=0
                                                                                                                                    AND             bc_writeoff.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_writeoff.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    /**********************************write off reversal status******************************************************/
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE34''                       AS ev_act_type_code ,
                                                                                                                                                    cast(bc_writeoff.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                               AS SUBTYPE ,
                                                                                                                                                    bc_writeoff.approvaldate_stg            AS sts_dt ,
                                                                                                                                                    bctl_approvalstatus.typecode_stg        AS sts_typ_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                 AS sts_rsn_cd,
                                                                                                                                                    bc_writeoff.retired_stg                 AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_writeoff
                                                                                                                                    left outer join db_t_prod_stag.bctl_approvalstatus
                                                                                                                                    ON              bctl_approvalstatus.id_stg=bc_writeoff.approvalstatus_stg
                                                                                                                                    WHERE           bc_writeoff.approvaldate_stg IS NOT NULL
                                                                                                                                    AND             reversed_stg=1
                                                                                                                                    AND             bc_writeoff.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_writeoff.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    SELECT ''EV_ACTVY_TYPE34''                       AS ev_act_type_code ,
                                                                                                                                           cast(bc_writeoff.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                               AS SUBTYPE ,
                                                                                                                                           bc_writeoff.createtime_stg              AS eff_dt ,
                                                                                                                                           ''PMT_STS_TYPE6''                         AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                 AS sts_rsn_cd,
                                                                                                                                           bc_writeoff.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.bc_writeoff
                                                                                                                                    WHERE  bc_writeoff.createtime_stg IS NOT NULL
                                                                                                                                    AND    reversed_stg=1
                                                                                                                                    AND    bc_writeoff.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    bc_writeoff.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    /*************************Claim Check Event*******************/
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE24''                          AS ev_act_type_code,
                                                                                                                                                    cast(cc_transaction.id_stg AS VARCHAR(60)) AS key1,
                                                                                                                                                    ''FINANCL''                                  AS SUBTYPE ,
                                                                                                                                                    cc_transaction.createtime_stg              AS eff_dt ,
                                                                                                                                                    cctl_transactionstatus.typecode_stg        AS sts_typ_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                    AS sts_rsn_cd ,
                                                                                                                                                    cc_transaction.retired_stg                 AS retired
                                                                                                                                    FROM            db_t_prod_stag.cc_transaction
                                                                                                                                    join            db_t_prod_stag.cctl_transaction
                                                                                                                                    ON              cctl_transaction.id_stg=cc_transaction.subtype_stg
                                                                                                                                    join            db_t_prod_stag.cctl_transactionstatus
                                                                                                                                    ON              cc_transaction.status_stg = cctl_transactionstatus.id_stg
                                                                                                                                    left outer join db_t_prod_stag.cc_check
                                                                                                                                    ON              cc_check.id_stg = cc_transaction.checkid_stg
                                                                                                                                    WHERE           cctl_transaction.typecode_stg=''Payment''
                                                                                                                                    AND             cc_transaction.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             cc_transaction.updatetime_stg <= (:END_DTTM)
                                                                                                                                    OR              cc_check.updatetime_stg >(:START_DTTM)
                                                                                                                                    AND             cc_check.updatetime_stg <= (:END_DTTM)
                                                                                                                                    UNION
                                                                                                                                    /************ Claim Recovery***********/
                                                                                                                                    SELECT ''EV_ACTVY_TYPE23''                          AS ev_act_type_code ,
                                                                                                                                           cast(cc_transaction.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                           ''FINANCL''                                  AS SUBTYPE ,
                                                                                                                                           cc_transaction.createtime_stg              AS eff_dt ,
                                                                                                                                           cctl_transactionstatus.typecode_stg        AS sts_typ_cd ,
                                                                                                                                           cast('''' AS VARCHAR(60))                    AS sts_rsn_cd ,
                                                                                                                                           cc_transaction.retired_stg                 AS retired
                                                                                                                                    FROM   db_t_prod_stag.cc_transaction
                                                                                                                                    join   db_t_prod_stag.cctl_transaction
                                                                                                                                    ON     cctl_transaction.id_stg=cc_transaction.subtype_stg
                                                                                                                                    join   db_t_prod_stag.cctl_transactionstatus
                                                                                                                                    ON     cc_transaction.status_stg = cctl_transactionstatus.id_stg
                                                                                                                                    WHERE  cc_transaction.checknum_alfa_stg IS NOT NULL
                                                                                                                                    AND    cctl_transaction.typecode_stg=''Recovery''
                                                                                                                                    AND    cc_transaction.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND    cc_transaction.updatetime_stg <= (:END_DTTM)
                                                                                                                                    /*********************************************************************************************************/
                                                                                                                                    UNION
                                                                                                                                    /*  Payment Request */
                                                                                                                                    SELECT          ''EV_ACTVY_TYPE35''                              AS ev_act_type_code ,
                                                                                                                                                    cast( bc_paymentrequest.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''FINANCL''                                      AS SUBTYPE ,
                                                                                                                                                    bc_paymentrequest.statusdate_stg               AS sts_dt ,
                                                                                                                                                    bctl_paymentrequeststatus.typecode_stg         AS sts_type_cd ,
                                                                                                                                                    cast('''' AS VARCHAR(60))                        AS sts_rsn_cd ,
                                                                                                                                                    bc_paymentrequest.retired_stg                  AS retired
                                                                                                                                    FROM            db_t_prod_stag.bc_paymentrequest
                                                                                                                                    left outer join db_t_prod_stag.bctl_paymentrequeststatus
                                                                                                                                    ON              bctl_paymentrequeststatus.id_stg=bc_paymentrequest.status_stg
                                                                                                                                    WHERE           bc_paymentrequest.updatetime_stg > (:START_DTTM)
                                                                                                                                    AND             bc_paymentrequest.updatetime_stg <= (:END_DTTM) ) x ) AS src
                                                                                                  /* --lookups */
                                                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS teradata_etl_ref_xlat
                                                                                  ON              teradata_etl_ref_xlat.src_idntftn_val =ev_act_type_code
                                                                                  AND             teradata_etl_ref_xlat.tgt_idntftn_nm =''EV_ACTVY_TYPE''
                                                                                                  /*  AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''derived'',  ''CCTL_ACTIVITYCATEGORY.TYPECODE'' ) */
                                                                                  AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                            ''DS'' )
                                                                                  AND             teradata_etl_ref_xlat.expn_dt =''9999-12-31''
                                                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_pmt_sts_rsn_type_cd
                                                                                  ON              xlat_pmt_sts_rsn_type_cd.src_idntftn_val =sts_rsn_cd
                                                                                  AND             xlat_pmt_sts_rsn_type_cd.tgt_idntftn_nm =''PMT_STS_RSN_TYPE''
                                                                                  AND             xlat_pmt_sts_rsn_type_cd.src_idntftn_nm = ''derived ''
                                                                                  AND             xlat_pmt_sts_rsn_type_cd.src_idntftn_sys =''DS''
                                                                                  AND             xlat_pmt_sts_rsn_type_cd.expn_dt =''9999-12-31''
                                                                                  left outer join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.tgt_idntftn_val                    AS tgt_idntftn_val,
                                                                                                                                  a.src_idntftn_val                    AS src_idntftn_val
                                                                                                                  FROM            db_t_prod_core.teradata_etl_ref_xlat AS a
                                                                                                                  WHERE           lower(src_idntftn_nm) IN ( ''derived '',
                                                                                                                                                            ''bctl_disbursementstatus.typecode'',
                                                                                                                                                            ''bctl_suspensepaymentstatus.typecode'',
                                                                                                                                                            ''bctl_outgoingpaymentstatus.typecode'',
                                                                                                                                                            ''bctl_approvalstatus.typecode'',
                                                                                                                                                            ''bctl_disbursementstatus.typecode'',
                                                                                                                                                            ''cctl_transactionstatus.typecode'')
                                                                                                                  AND             src_idntftn_sys IN (''DS'' ,
                                                                                                                                                      ''GW'')
                                                                                                                  AND             expn_dt=''9999-12-31''
                                                                                                                  AND             tgt_idntftn_nm= ''PMT_STS_TYPE'' ) AS xlat_pmt_sts_type_cd
                                                                                  ON              xlat_pmt_sts_type_cd.src_idntftn_val=sts_type_code
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   src_ev.ev_id            AS ev_id,
                                                                                                                    src_ev.src_trans_id     AS src_trans_id,
                                                                                                                    src_ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                                                                                    src_ev.ev_actvy_type_cd AS ev_actvy_type_cd
                                                                                                           FROM     db_t_prod_core.ev       AS src_ev qualify row_number() over(PARTITION BY src_ev.ev_sbtype_cd ,src_ev.ev_actvy_type_cd ,src_ev.src_trans_id ORDER BY src_ev.edw_end_dttm DESC)= 1) AS ev_id_lkp
                                                                                  ON              ev_id_lkp.src_trans_id =src.key1
                                                                                  AND             ev_id_lkp.ev_sbtype_cd =SUBTYPE
                                                                                  AND             ev_id_lkp.ev_actvy_type_cd =teradata_etl_ref_xlat.tgt_idntftn_val ) AS xlat_src
                                                  left outer join
                                                                  (
                                                                           SELECT   pmt_sts.pmt_sts_dttm AS pmt_sts_dttm,
                                                                                    /* PMT_STS.PMT_STS_COMT_TXT AS PMT_STS_COMT_TXT, --EIM_30318_PMT_STS- MD5 updates to remove unnecessary CDC INFORMATION_SCHEMA.columns */
                                                                                    pmt_sts.pmt_sts_rsn_type_cd AS pmt_sts_rsn_type_cd,
                                                                                    pmt_sts.edw_strt_dttm       AS edw_strt_dttm,
                                                                                    pmt_sts.edw_end_dttm        AS edw_end_dttm,
                                                                                    pmt_sts.pmt_ev_id           AS pmt_ev_id,
                                                                                    pmt_sts.pmt_sts_type_cd     AS pmt_sts_type_cd
                                                                           FROM     db_t_prod_core.pmt_sts      AS pmt_sts qualify row_number() over(PARTITION BY pmt_sts.pmt_ev_id ORDER BY pmt_sts.edw_end_dttm DESC) = 1) AS tgt_lkp_pmt_sts
                                                  ON              tgt_lkp_pmt_sts.pmt_ev_id=xlat_src.pmt_ev_id ) src ) );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
         SELECT sq_bc_basemoneyreceived.lkp_pmt_ev_id                                  AS lkp_pmt_ev_id,
                sq_bc_basemoneyreceived.lkp_edw_strt_dttm                              AS lkp_edw_strt_dttm,
                sq_bc_basemoneyreceived.lkp_edw_end_dttm                               AS lkp_edw_end_dttm,
                sq_bc_basemoneyreceived.src_pmt_ev_id                                  AS src_pmt_ev_id,
                sq_bc_basemoneyreceived.src_pmt_sys_type_cd                            AS src_pmt_sys_type_cd,
                sq_bc_basemoneyreceived.src_pmt_sts_dttm                               AS src_pmt_sts_dttm,
                sq_bc_basemoneyreceived.src_pmt_sts_rsn_type_cd                        AS src_pmt_sts_rsn_type_cd,
                :prcs_id                                                               AS prcs_id,
                current_timestamp                                                      AS starttime,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS endtime,
                sq_bc_basemoneyreceived.retired                                        AS retired,
                sq_bc_basemoneyreceived.rnk                                            AS rnk,
                sq_bc_basemoneyreceived.ins_upd_flag                                   AS ins_upd_flag,
                sq_bc_basemoneyreceived.source_record_id
         FROM   sq_bc_basemoneyreceived );
  -- Component rtr_pmt_sts_ins_upd_Insert, Type ROUTER Output Group Insert
create or replace TEMPORARY table rtr_pmt_sts_ins_upd_Insert as
  SELECT exp_cdc_check.lkp_pmt_ev_id           AS lkp_pmt_ev_id,
         exp_cdc_check.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_cdc_check.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_cdc_check.src_pmt_ev_id           AS src_pmt_ev_id,
         exp_cdc_check.src_pmt_sys_type_cd     AS src_pmt_sys_type_cd,
         exp_cdc_check.src_pmt_sts_dttm        AS src_pmt_sts_dttm,
         exp_cdc_check.src_pmt_sts_rsn_type_cd AS src_pmt_sts_rsn_type_cd,
         exp_cdc_check.prcs_id                 AS src_prcs_id,
         exp_cdc_check.starttime               AS starttime,
         exp_cdc_check.endtime                 AS endtime,
         exp_cdc_check.retired                 AS retired,
         exp_cdc_check.rnk                     AS rnk,
         exp_cdc_check.ins_upd_flag            AS o_ins_upd,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.src_pmt_ev_id IS NOT NULL
  AND    ( (
                       o_ins_upd = ''I''
                OR     o_ins_upd = ''U'' )
         OR     (
                       exp_cdc_check.retired = 0
                AND    exp_cdc_check.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_pmt_sts_ins_upd_Retire, Type ROUTER Output Group Retire
  create or replace TEMPORARY table rtr_pmt_sts_ins_upd_Retire as
  SELECT exp_cdc_check.lkp_pmt_ev_id           AS lkp_pmt_ev_id,
         exp_cdc_check.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_cdc_check.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_cdc_check.src_pmt_ev_id           AS src_pmt_ev_id,
         exp_cdc_check.src_pmt_sys_type_cd     AS src_pmt_sys_type_cd,
         exp_cdc_check.src_pmt_sts_dttm        AS src_pmt_sts_dttm,
         exp_cdc_check.src_pmt_sts_rsn_type_cd AS src_pmt_sts_rsn_type_cd,
         exp_cdc_check.prcs_id                 AS src_prcs_id,
         exp_cdc_check.starttime               AS starttime,
         exp_cdc_check.endtime                 AS endtime,
         exp_cdc_check.retired                 AS retired,
         exp_cdc_check.rnk                     AS rnk,
         exp_cdc_check.ins_upd_flag            AS o_ins_upd,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  o_ins_upd = ''R''
  AND    exp_cdc_check.retired != 0
  AND    exp_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_pmt_sts_upd1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_pmt_sts_upd1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_pmt_sts_ins_upd_retire.lkp_pmt_ev_id     AS lkp_pmt_ev_id3,
                rtr_pmt_sts_ins_upd_retire.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                NULL                                         AS lkp_edw_end_dttm3,
                rtr_pmt_sts_ins_upd_retire.retired           AS retired3,
                rtr_pmt_sts_ins_upd_retire.src_prcs_id       AS in_prcs_id4,
                1                                            AS update_strategy_action,
				source_record_id
         FROM   rtr_pmt_sts_ins_upd_retire );
  -- Component exp_pass_to_tgt_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
  (
         SELECT upd_pmt_sts_upd1.lkp_pmt_ev_id3     AS lkp_pmt_ev_id3,
                upd_pmt_sts_upd1.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                current_timestamp                   AS o_enddate,
                upd_pmt_sts_upd1.source_record_id
         FROM   upd_pmt_sts_upd1 );
  -- Component upd_pmt_sts_INSERT, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_pmt_sts_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_pmt_sts_ins_upd_insert.src_pmt_ev_id           AS in_pmt_ev_id1,
                rtr_pmt_sts_ins_upd_insert.src_pmt_sys_type_cd     AS in_pmt_sys_type_cd1,
                rtr_pmt_sts_ins_upd_insert.src_pmt_sts_dttm        AS in_pmt_sts_dttm1,
                rtr_pmt_sts_ins_upd_insert.src_pmt_sts_rsn_type_cd AS in_pmt_sts_rsn_type_cd1,
                rtr_pmt_sts_ins_upd_insert.src_prcs_id             AS in_prcs_id1,
                rtr_pmt_sts_ins_upd_insert.starttime               AS starttime1,
                rtr_pmt_sts_ins_upd_insert.endtime                 AS endtime1,
                rtr_pmt_sts_ins_upd_insert.retired                 AS retired1,
                rtr_pmt_sts_ins_upd_insert.rnk                     AS rnk1,
                0                                                  AS update_strategy_action,
				source_record_id
         FROM   rtr_pmt_sts_ins_upd_insert );
  -- Component tgt_PMT_STS_UPD_Retire_Reject, Type TARGET
  merge
  INTO         db_t_prod_core.pmt_sts
  USING        exp_pass_to_tgt_upd1
  ON (
                            pmt_sts.pmt_ev_id = exp_pass_to_tgt_upd1.lkp_pmt_ev_id3
               AND          pmt_sts.edw_strt_dttm = exp_pass_to_tgt_upd1.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    pmt_ev_id = exp_pass_to_tgt_upd1.lkp_pmt_ev_id3,
         edw_strt_dttm = exp_pass_to_tgt_upd1.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_upd1.o_enddate;
  
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
         SELECT upd_pmt_sts_insert.in_pmt_ev_id1                                                   AS in_pmt_ev_id1,
                upd_pmt_sts_insert.in_pmt_sys_type_cd1                                             AS in_pmt_sys_type_cd1,
                upd_pmt_sts_insert.in_pmt_sts_dttm1                                                AS in_pmt_sts_dttm1,
                upd_pmt_sts_insert.in_pmt_sts_rsn_type_cd1                                         AS in_pmt_sts_rsn_type_cd1,
                upd_pmt_sts_insert.in_prcs_id1                                                     AS in_prcs_id1,
                dateadd (second,  ( 2 * ( upd_pmt_sts_insert.rnk1 - 1 ) ), current_timestamp  ) AS starttime1,
                CASE
                       WHEN upd_pmt_sts_insert.retired1 != 0 THEN current_timestamp
                       ELSE upd_pmt_sts_insert.endtime1
                END AS o_edw_end_dttm,
                upd_pmt_sts_insert.source_record_id
         FROM   upd_pmt_sts_insert );
  -- Component tgt_PMT_STS_New_Insert, Type TARGET
  INSERT INTO db_t_prod_core.pmt_sts
              (
                          pmt_ev_id,
                          pmt_sts_type_cd,
                          pmt_sts_dttm,
                          pmt_sts_rsn_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exptrans.in_pmt_ev_id1           AS pmt_ev_id,
         exptrans.in_pmt_sys_type_cd1     AS pmt_sts_type_cd,
         exptrans.in_pmt_sts_dttm1        AS pmt_sts_dttm,
         exptrans.in_pmt_sts_rsn_type_cd1 AS pmt_sts_rsn_type_cd,
         exptrans.in_prcs_id1             AS prcs_id,
         exptrans.starttime1              AS edw_strt_dttm,
         exptrans.o_edw_end_dttm          AS edw_end_dttm
  FROM   exptrans;
  
  -- Component tgt_PMT_STS_New_Insert, Type Post SQL
  UPDATE db_t_prod_core.pmt_sts
    SET    edw_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT pmt_ev_id,
                                         pmt_sts_type_cd,
                                         edw_strt_dttm,
                                         pmt_sts_dttm ,
                                         max(edw_strt_dttm) over (PARTITION BY pmt_ev_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.pmt_sts ) a

  WHERE  pmt_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    pmt_sts.pmt_ev_id=a.pmt_ev_id
  AND    pmt_sts.pmt_sts_type_cd=a.pmt_sts_type_cd
  AND    lead IS NOT NULL;

END;
';