-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINCL_EV_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;

BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'');
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'');


  -- Component src_sq_bc_basemoneyreceived, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE src_sq_bc_basemoneyreceived AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ev_id,
                $2  AS doc_id,
                $3  AS fincl_ev_type,
                $4  AS eff_dt,
                $5  AS refnumber,
                $6  AS trans_host_num,
                $7  AS funccode,
                $8  AS ar_invc_ln_num,
                $9  AS chargecategorycode,
                $10 AS tranfer_method_typ,
                $11 AS glmonth,
                $12 AS glyear,
                $13 AS accountingdaynum,
                $14 AS accountingmonthnum,
                $15 AS accountingyearnum,
                $16 AS createtime,
                $17 AS fincl_ev_prd_end_dt,
                $18 AS retired,
                $19 AS ev_med_type_cd,
                $20 AS funds_tfr_type_cd,
                $21 AS rnk,
                $22 AS tgt_edw_strt_dttm,
                $23 AS tgt_edw_end_dttm,
                $24 AS concat_sourcedata,
                $25 AS concat_targetdata,
                $26 AS ins_upd_flag,
                $27 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH intrm_fncl_ev AS
                                  (
                                                  SELECT DISTINCT ev_act_type_code,
                                                                  key1,
                                                                  SUBTYPE,
                                                                  financl_ev_type,
                                                                  eff_dt,
                                                                  refnumber,
                                                                  trans_host_num,
                                                                  funccd,
                                                                  inv_invoicenumber,
                                                                  invitem_id,
                                                                  chargecategorycode,
                                                                  tranfer_method_typ,
                                                                  ev_strt_dt,
                                                                  ev_end_dt,
                                                                  glmonth,
                                                                  glyear,
                                                                  cast(accountingdate AS DATE) AS accountingdate,
                                                                  fincl_ev_prd_strt_dt,
                                                                  retired,
                                                                  ev_med_type_cd,
                                                                  funds_tfr_type_cd,
                                                                  query_id,
                                                                  row_number () over ( PARTITION BY ev_act_type_code, key1, SUBTYPE ORDER BY updatetime ,fincl_ev_prd_strt_dt , accountingdate ,financl_ev_type , eff_dt ,refnumber,trans_host_num , funccd, inv_invoicenumber, invitem_id, chargecategorycode ,tranfer_method_typ ,ev_strt_dt , ev_end_dt ,glmonth ,glyear ) AS rnk
                                                  FROM            (
                                                                                  SELECT DISTINCT cast(pctl_job.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg                       AS key1 ,
                                                                                                  ''EV_SBTYPE3''                               AS SUBTYPE ,
                                                                                                  cast(NULL AS VARCHAR(60))                  AS financl_ev_type ,
                                                                                                  cast(NULL AS DATE)                         AS eff_dt ,
                                                                                                  cast(NULL AS VARCHAR(60))                  AS refnumber ,
                                                                                                  cast('''' AS   VARCHAR(60))                  AS trans_host_num ,
                                                                                                  cast(NULL AS VARCHAR(60))                  AS funccd ,
                                                                                                  cast(NULL AS VARCHAR(255))                 AS inv_invoicenumber ,
                                                                                                  cast(NULL AS bigint)                       AS invitem_id ,
                                                                                                  cast(NULL AS VARCHAR(60))                  AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                  AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                    AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                    AS ev_end_dt ,
                                                                                                  cast(NULL AS INT)                          AS glmonth ,
                                                                                                  cast(NULL AS INT)                          AS glyear ,
                                                                                                  cast(NULL AS DATE)                         AS accountingdate ,
                                                                                                  pc_job.createtime_stg                      AS fincl_ev_prd_strt_dt ,
                                                                                                  pc_job.retired_stg                         AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                  AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                 AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0010'' AS VARCHAR(10))                  AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS updatetime
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  inner join      db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join       db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join db_t_prod_stag.pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  WHERE           pc_policyperiod.updatetime_stg > (:START_DTTM)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:END_DTTM)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  UNION ALL
                                                                                  /* ** Payment received- non-reversed*********************/
                                                                                  SELECT          cast(''EV_ACTVY_TYPE14'' AS           VARCHAR(60))                  AS ev_act_type_code ,
                                                                                                  cast(bc_basemoneyreceived.id_stg AS VARCHAR(60))                  AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                                      AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                                  AS financl_ev_type ,
                                                                                                  receiveddate_stg                                                  AS eff_dt ,
                                                                                                  cast(bc_basemoneyreceived.receiptnumber_alfa_stg AS VARCHAR(60))  AS refnumber ,
                                                                                                  cast(bc_basemoneyreceived.refnumber_stg AS          VARCHAR(60))  AS trans_host_num ,
                                                                                                  cast(''BILL'' AS                                      VARCHAR(60))  AS funccd ,
                                                                                                  cast(NULL AS                                        VARCHAR(255)) AS inv_invoicenumber ,
                                                                                                  cast(NULL AS                                        bigint)       AS invitem_id ,
                                                                                                  cast(NULL AS                                        VARCHAR(60))  AS chargecategorycode ,
                                                                                                  bctl_paymentmethod.typecode_stg                                   AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_end_dt ,
                                                                                                  gle.glmonth_stg                                                   AS glmonth ,
                                                                                                  gle.glyear_stg                                                    AS glyear ,
                                                                                                  gle.accountingdate_stg                                            AS accountingdate ,
                                                                                                  bc_basemoneyreceived.createtime_stg                               AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_basemoneyreceived.retired_stg                                  AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                         AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                        AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0020'' AS VARCHAR(10))                                         AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)                        AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                  ON              bctl_basemoneyreceived.id_stg = bc_basemoneyreceived.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_basemoneyreceived.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg=bc_paymentinstrument.paymentmethod_stg
                                                                                  left outer join db_t_prod_stag.bc_dbmoneyrcvdcontext
                                                                                  ON              bc_basemoneyreceived.id_stg = bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg
                                                                                  left outer join db_t_prod_stag.bc_transaction
                                                                                  ON              bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg
                                                                                  left outer join db_t_prod_stag.bctl_transaction
                                                                                  ON              bc_transaction.subtype_stg=bctl_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.gl_eventstaging_bc gle
                                                                                  ON              gle.publicid_stg = bc_transaction.publicid_stg
                                                                                  AND             gle.rootentity_stg = bctl_transaction.typecode_stg
                                                                                  WHERE           bc_basemoneyreceived.reversaldate_stg IS NULL
                                                                                  AND             bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                                                          ''ZeroDollarDMR'' ,
                                                                                                                                          ''ZeroDollarReversal'')
                                                                                  AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  SELECT          cast(''EV_ACTVY_TYPE14'' AS           VARCHAR(60))                  AS ev_act_type_code ,
                                                                                                  cast(bc_basemoneyreceived.id_stg AS VARCHAR(60))                  AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                                      AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                                  AS financl_ev_type ,
                                                                                                  receiveddate_stg                                                  AS eff_dt ,
                                                                                                  cast(bc_basemoneyreceived.receiptnumber_alfa_stg AS VARCHAR(60))  AS refnumber ,
                                                                                                  cast(bc_basemoneyreceived.refnumber_stg AS          VARCHAR(60))  AS trans_host_num ,
                                                                                                  cast(''BILL'' AS                                      VARCHAR(60))  AS funccd ,
                                                                                                  cast(NULL AS                                        VARCHAR(255)) AS inv_invoicenumber ,
                                                                                                  cast(NULL AS                                        bigint)       AS invitem_id ,
                                                                                                  cast(NULL AS                                        VARCHAR(60))  AS chargecategorycode ,
                                                                                                  bctl_paymentmethod.typecode_stg                                   AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_end_dt ,
                                                                                                  gle.glmonth_stg                                                   AS glmonth ,
                                                                                                  gle.glyear_stg                                                    AS glyear ,
                                                                                                  gle.accountingdate_stg                                            AS accountingdate ,
                                                                                                  bc_basemoneyreceived.createtime_stg                               AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_basemoneyreceived.retired_stg                                  AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                         AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                        AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0030'' AS VARCHAR(10))                                         AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)                        AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                  ON              bctl_basemoneyreceived.id_stg = bc_basemoneyreceived.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_basemoneyreceived.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg=bc_paymentinstrument.paymentmethod_stg
                                                                                  left outer join db_t_prod_stag.bc_dbmoneyrcvdcontext
                                                                                  ON              bc_basemoneyreceived.id_stg = bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg
                                                                                  left outer join db_t_prod_stag.bc_transaction
                                                                                  ON              bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg
                                                                                  left outer join db_t_prod_stag.bctl_transaction
                                                                                  ON              bc_transaction.subtype_stg=bctl_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_revtrans a
                                                                                  ON              a.ownerid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_revtrans b
                                                                                  ON              b.foreignentityid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.gl_eventstaging_bc gle
                                                                                  ON              gle.publicid_stg = bc_transaction.publicid_stg
                                                                                  AND             gle.rootentity_stg = bctl_transaction.typecode_stg
                                                                                  WHERE           bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                                                  AND             b.foreignentityid_stg IS NOT NULL
                                                                                  AND             a.ownerid_stg IS NULL
                                                                                  AND             bctl_basemoneyreceived.typecode_stg IN ( ''PaymentMoneyReceived'' ,
                                                                                                                                          ''DirectBillMoneyRcvd'' ,
                                                                                                                                          ''ZeroDollarDMR'' ,
                                                                                                                                          ''ZeroDollarReversal'')
                                                                                  AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* **************************bc_basemoneyreceived****************************/
                                                                                  SELECT          cast(''EV_ACTVY_TYPE25'' AS           VARCHAR(60))                  AS ev_act_type_code ,
                                                                                                  cast(bc_basemoneyreceived.id_stg AS VARCHAR(60))                  AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                                      AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                                  AS financl_ev_type ,
                                                                                                  reversaldate_stg                                                  AS eff_dt ,
                                                                                                  cast(bc_basemoneyreceived.receiptnumber_alfa_stg AS VARCHAR(60))  AS refnumber_num ,
                                                                                                  cast(bc_basemoneyreceived.refnumber_stg AS          VARCHAR(60))  AS trans_host_num ,
                                                                                                  cast(''BILL'' AS                                      VARCHAR(60))  AS funccd ,
                                                                                                  cast(NULL AS                                        VARCHAR(255)) AS inv_invoicenumber ,
                                                                                                  cast(NULL AS                                        bigint)       AS invitem_id ,
                                                                                                  cast(NULL AS                                        VARCHAR(60))  AS chargecategorycode ,
                                                                                                  bctl_paymentmethod.typecode_stg                                   AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                           AS ev_end_dt ,
                                                                                                  gle.glmonth_stg                                                   AS glmonth ,
                                                                                                  gle.glyear_stg                                                    AS glyear ,
                                                                                                  gle.accountingdate_stg                                            AS accountingdate ,
                                                                                                  bc_basemoneyreceived.createtime_stg                               AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_basemoneyreceived.retired_stg                                  AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                         AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                        AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0040'' AS VARCHAR(10))                                         AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)                        AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                  ON              bctl_basemoneyreceived.id_stg = bc_basemoneyreceived.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_basemoneyreceived.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg=bc_paymentinstrument.paymentmethod_stg
                                                                                  left outer join db_t_prod_stag.bc_dbmoneyrcvdcontext
                                                                                  ON              bc_basemoneyreceived.id_stg = bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg
                                                                                  left outer join db_t_prod_stag.bc_transaction
                                                                                  ON              bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg
                                                                                  left outer join db_t_prod_stag.bctl_transaction
                                                                                  ON              bc_transaction.subtype_stg=bctl_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_revtrans a
                                                                                  ON              a.ownerid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_revtrans b
                                                                                  ON              b.foreignentityid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.gl_eventstaging_bc gle
                                                                                  ON              gle.publicid_stg = bc_transaction.publicid_stg
                                                                                  AND             gle.rootentity_stg = bctl_transaction.typecode_stg
                                                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN ( ''PaymentMoneyReceived'',
                                                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                                                          ''ZeroDollarDMR'' ,
                                                                                                                                          ''ZeroDollarReversal'')
                                                                                  AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                                                  AND             a.ownerid_stg IS NOT NULL
                                                                                  AND             b.foreignentityid_stg IS NULL
                                                                                  AND             bc_basemoneyreceived.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_basemoneyreceived.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* ************************** DB_T_PROD_STAG.bc_basedistitem - TRANSACTIONS ****************************/
                                                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE26'' AS      VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                            AS financl_ev_type ,
                                                                                                  bc_basedistitem.executeddate_stg            AS eff_dt ,
                                                                                                  cast('''' AS VARCHAR(60))                     AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                     AS trans_host_num ,
                                                                                                  ''BILL''                                      AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                   inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                          invitem_id ,
                                                                                                  bctl_chargecategory.typecode_stg               chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                   AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                     AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                     AS ev_end_dt ,
                                                                                                  glb.glmonth ,
                                                                                                  glb.glyear ,
                                                                                                  cast(NULL AS DATE)                              AS accountingdate ,
                                                                                                  bc_basedistitem.createtime_stg                  AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_basedistitem.retired_stg                     AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                  AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                 AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0050'' AS VARCHAR(10))                  AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join            db_t_prod_stag.bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join            db_t_prod_stag.bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join            db_t_prod_stag.bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join            db_t_prod_stag.bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join            db_t_prod_stag.bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join            db_t_prod_stag.bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   directbillpaymentitemid_stg,
                                                                                                                    glyear,
                                                                                                                    glmonth
                                                                                                           FROM     (
                                                                                                                              SELECT    bc_chargeinstancecontext.directbillpaymentitemid_stg,
                                                                                                                                        CASE
                                                                                                                                                  WHEN max(glyear_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1)= min(glyear_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1) THEN glyear_stg
                                                                                                                                                  ELSE NULL
                                                                                                                                        END AS glyear,
                                                                                                                                        CASE
                                                                                                                                                  WHEN max(glmonth_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1) = min(glmonth_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1)THEN glmonth_stg
                                                                                                                                                  ELSE NULL
                                                                                                                                        END AS glmonth
                                                                                                                              FROM      db_t_prod_stag.bc_chargeinstancecontext
                                                                                                                              left join db_t_prod_stag.bc_transaction
                                                                                                                              ON        bc_transaction.id_stg = bc_chargeinstancecontext.transactionid_stg
                                                                                                                              left join db_t_prod_stag.gl_eventstaging_bc
                                                                                                                              ON        bc_transaction.publicid_stg=gl_eventstaging_bc.publicid_stg
                                                                                                                              WHERE     eventname_stg=''TransactionAdded''
                                                                                                                              AND       trans_flag_stg=''N'') gl_eventstaging qualify row_number() over ( PARTITION BY directbillpaymentitemid_stg ORDER BY glyear DESC,glmonth DESC) = 1 ) glb
                                                                                  ON              glb.directbillpaymentitemid_stg = bc_basedistitem.id_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* *********************** DB_T_PROD_STAG.bc_basedistitem - TRANSACTIONS-REVERSED ************************/
                                                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE27'' AS      VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                            AS financl_ev_type ,
                                                                                                  bc_basedistitem.reverseddate_stg            AS eff_dt ,
                                                                                                  cast('''' AS VARCHAR(60))                     AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                     AS trans_host_num ,
                                                                                                  ''BILL''                                      AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                AS inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                       AS invitem_id ,
                                                                                                  bctl_chargecategory.typecode_stg            AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                   AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                     AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                     AS ev_end_dt ,
                                                                                                  glb.glmonth ,
                                                                                                  glb.glyear ,
                                                                                                  cast(NULL AS DATE)                              AS accountingdate ,
                                                                                                  bc_basedistitem.createtime_stg                  AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_basedistitem.retired_stg                     AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                  AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                 AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0060'' AS VARCHAR(10))                  AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join            db_t_prod_stag.bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join            db_t_prod_stag.bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join            db_t_prod_stag.bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join            db_t_prod_stag.bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join            db_t_prod_stag.bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join            db_t_prod_stag.bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   directbillpaymentitemid_stg,
                                                                                                                    glyear,
                                                                                                                    glmonth
                                                                                                           FROM     (
                                                                                                                              SELECT    bc_chargeinstancecontext.directbillpaymentitemid_stg,
                                                                                                                                        CASE
                                                                                                                                                  WHEN max(glyear_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1)= min(glyear_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1) THEN glyear_stg
                                                                                                                                                  ELSE NULL
                                                                                                                                        END AS glyear,
                                                                                                                                        CASE
                                                                                                                                                  WHEN max(glmonth_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1) = min(glmonth_stg) over ( PARTITION BY gl_eventstaging_bc.publicid_stg ORDER BY 1)THEN glmonth_stg
                                                                                                                                                  ELSE NULL
                                                                                                                                        END AS glmonth
                                                                                                                              FROM      db_t_prod_stag.bc_chargeinstancecontext
                                                                                                                              left join db_t_prod_stag.bc_transaction
                                                                                                                              ON        bc_transaction.id_stg = bc_chargeinstancecontext.transactionid_stg
                                                                                                                              left join db_t_prod_stag.gl_eventstaging_bc
                                                                                                                              ON        bc_transaction.publicid_stg=gl_eventstaging_bc.publicid_stg
                                                                                                                              WHERE     eventname_stg=''TransactionAdded''
                                                                                                                              AND       trans_flag_stg=''Y'') gl_eventstaging qualify row_number() over ( PARTITION BY directbillpaymentitemid_stg ORDER BY glyear DESC,glmonth DESC) = 1 ) glb
                                                                                  ON              glb.directbillpaymentitemid_stg = bc_basedistitem.id_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:END_DTTM)
                                                                                  AND             bc_basedistitem.reverseddistid_stg IS NOT NULL
                                                                                  UNION ALL
                                                                                  /* **************************bc_basenonrecdistitem****************************/
                                                                                  SELECT cast(''EV_ACTVY_TYPE28'' AS            VARCHAR(60)) AS ev_act_type_code ,
                                                                                         cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                         ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                                         ''FINCL_EV_TYPE3''                                  AS financl_ev_type ,
                                                                                         bc_basenonrecdistitem.executeddate_stg            AS eff_dt ,
                                                                                         cast('''' AS VARCHAR(60))                           AS refnumber ,
                                                                                         cast('''' AS VARCHAR(60))                           AS trans_host_num ,
                                                                                         ''BILL''                                            AS funccd ,
                                                                                         cast(NULL AS VARCHAR(255))                        AS inv_invoicenumber ,
                                                                                         cast(NULL AS bigint)                              AS invitem_id ,
                                                                                         cast(NULL AS VARCHAR(60))                         AS chargecategorycode ,
                                                                                         cast(NULL AS VARCHAR(60))                         AS tranfer_method_typ ,
                                                                                         cast(NULL AS timestamp)                           AS ev_strt_dt ,
                                                                                         cast(NULL AS timestamp)                           AS ev_end_dt ,
                                                                                         NULL                                              AS glmonth ,
                                                                                         NULL                                              AS glyear ,
                                                                                         cast(NULL AS DATE)                                AS accountingdate ,
                                                                                         bc_basenonrecdistitem.createtime_stg              AS fincl_ev_prd_strt_dt ,
                                                                                         bc_basenonrecdistitem.retired_stg                 AS retired ,
                                                                                         cast(NULL AS      VARCHAR(60))                         AS ev_med_type_cd ,
                                                                                         cast(NULL AS      VARCHAR(255))                        AS funds_tfr_type_cd ,
                                                                                         cast(''DLY0070'' AS VARCHAR(10))                         AS query_id ,
                                                                                         cast(''1900-01-01 00:00:00.000000'' AS timestamp)        AS updatetime
                                                                                  FROM   db_t_prod_stag.bc_basenonrecdistitem
                                                                                  WHERE  bc_basenonrecdistitem.updatetime_stg > (:START_DTTM)
                                                                                  AND    bc_basenonrecdistitem.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* **************************DB_T_PROD_STAG.bc_basenonrecdistitem - Reversed****************************/
                                                                                  SELECT cast(''EV_ACTVY_TYPE29'' AS            VARCHAR(60)) AS ev_act_type_code ,
                                                                                         cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                         ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                                         ''FINCL_EV_TYPE3''                                  AS financl_ev_type ,
                                                                                         bc_basenonrecdistitem.reverseddate_stg            AS eff_dt ,
                                                                                         cast('''' AS VARCHAR(60))                           AS refnumber ,
                                                                                         cast('''' AS VARCHAR(60))                           AS trans_host_num ,
                                                                                         ''BILL''                                            AS funccd ,
                                                                                         cast(NULL AS VARCHAR(255))                        AS inv_invoicenumber ,
                                                                                         cast(NULL AS bigint)                              AS invitem_id ,
                                                                                         cast(NULL AS VARCHAR(60))                         AS chargecategorycode ,
                                                                                         cast(NULL AS VARCHAR(60))                         AS tranfer_method_typ ,
                                                                                         cast(NULL AS timestamp)                           AS ev_strt_dt ,
                                                                                         cast(NULL AS timestamp)                           AS ev_end_dt ,
                                                                                         NULL                                              AS glmonth ,
                                                                                         NULL                                              AS glyear ,
                                                                                         cast(NULL AS DATE)                                AS accountingdate ,
                                                                                         bc_basenonrecdistitem.createtime_stg              AS fincl_ev_prd_strt_dt ,
                                                                                         bc_basenonrecdistitem.retired_stg                 AS retired ,
                                                                                         cast(NULL AS      VARCHAR(60))                         AS ev_med_type_cd ,
                                                                                         cast(NULL AS      VARCHAR(255))                        AS funds_tfr_type_cd ,
                                                                                         cast(''DLY0080'' AS VARCHAR(10))                         AS query_id ,
                                                                                         cast(''1900-01-01 00:00:00.000000'' AS timestamp)        AS updatetime
                                                                                  FROM   db_t_prod_stag.bc_basenonrecdistitem
                                                                                  WHERE  bc_basenonrecdistitem.updatetime_stg > (:START_DTTM)
                                                                                  AND    bc_basenonrecdistitem.updatetime_stg <= (:END_DTTM)
                                                                                  AND    bc_basenonrecdistitem.reverseddate_stg IS NOT NULL
                                                                                  UNION ALL
                                                                                  /* **************************bc_suspensepayment****************************/
                                                                                  SELECT cast(''EV_ACTVY_TYPE30'' AS         VARCHAR(60)) AS ev_act_type_code ,
                                                                                         cast(bc_suspensepayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                         ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                         ''FINCL_EV_TYPE3''                               AS financl_ev_type ,
                                                                                         bc_suspensepayment.paymentdate_stg             AS eff_dt ,
                                                                                         bc_suspensepayment.receiptnumber_alfa_stg      AS refnumber ,
                                                                                         bc_suspensepayment.refnumber_stg               AS trans_host_num ,
                                                                                         ''BILL''                                         AS funccd ,
                                                                                         cast(NULL AS VARCHAR(255))                     AS inv_invoicenumber ,
                                                                                         cast(NULL AS bigint)                           AS invitem_id ,
                                                                                         cast(NULL AS VARCHAR(60))                      AS chargecategorycode ,
                                                                                         cast(NULL AS VARCHAR(60))                      AS tranfer_method_typ ,
                                                                                         cast(NULL AS timestamp)                        AS ev_strt_dt ,
                                                                                         cast(NULL AS timestamp)                        AS ev_end_dt ,
                                                                                         NULL                                           AS glmonth ,
                                                                                         NULL                                           AS glyear ,
                                                                                         cast(NULL AS DATE)                             AS accountingdate ,
                                                                                         bc_suspensepayment.createtime_stg              AS fincl_ev_prd_strt_dt ,
                                                                                         bc_suspensepayment.retired_stg                 AS retired ,
                                                                                         cast(NULL AS      VARCHAR(60))                      AS ev_med_type_cd ,
                                                                                         cast(NULL AS      VARCHAR(255))                     AS funds_tfr_type_cd ,
                                                                                         cast(''DLY0090'' AS VARCHAR(10))                      AS query_id ,
                                                                                         cast(''1900-01-01 00:00:00.000000'' AS timestamp)     AS updatetime
                                                                                  FROM   db_t_prod_stag.bc_suspensepayment
                                                                                  WHERE  bc_suspensepayment.updatetime_stg > (:START_DTTM)
                                                                                  AND    bc_suspensepayment.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  SELECT          cast(''EV_ACTVY_TYPE31'' AS         VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_outgoingpayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                               AS financl_ev_type ,
                                                                                                  bc_outgoingpayment.issuedate_stg               AS eff_dt ,
                                                                                                  bc_outgoingpayment.refnumber_stg               AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                        AS trans_host_num ,
                                                                                                  ''BILL''                                         AS funccd ,
                                                                                                  cast(NULL AS VARCHAR(255))                     AS inv_invoicenumber ,
                                                                                                  cast(NULL AS bigint)                           AS invitem_id ,
                                                                                                  cast(NULL AS VARCHAR(60))                      AS chargecategorycode ,
                                                                                                  bctl_paymentmethod.typecode_stg                AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                        AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                        AS ev_end_dt ,
                                                                                                  glb.glmonth_stg                                AS glmonth ,
                                                                                                  glb.glyear_stg                                 AS glyear ,
                                                                                                  glb.accountingdate_stg                         AS accountingdate ,
                                                                                                  bc_outgoingpayment.createtime_stg              AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_outgoingpayment.retired_stg                 AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                      AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                     AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0100'' AS VARCHAR(10))                      AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)     AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg
                                                                                  left join       db_t_prod_stag.gl_eventstaging_bc glb
                                                                                  ON              glb.publicid_stg = bc_outgoingpayment.publicid_stg
                                                                                  AND             upper(glb.rootentity_stg) = ''OUTGOINGDISBPMNT''
                                                                                  WHERE           bc_outgoingpayment.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_outgoingpayment.updatetime_stg <= (:END_DTTM) qualify row_number() over ( PARTITION BY key1, refnumber,eff_dt ORDER BY accountingdate ASC,glyear DESC,glmonth DESC)=1
                                                                                  UNION ALL
                                                                                  SELECT    cast(''EV_ACTVY_TYPE32'' AS      VARCHAR(60)) AS ev_act_type_code ,
                                                                                            cast(bc_disbursement.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                            ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                            ''FINCL_EV_TYPE3''                            AS financl_ev_type ,
                                                                                            bc_disbursement.duedate_stg                 AS eff_dt ,
                                                                                            bc_disbursement.disbursementnumber_stg      AS refnumber ,
                                                                                            cast('''' AS VARCHAR(60))                     AS trans_host_num ,
                                                                                            ''BILL''                                      AS funccd ,
                                                                                            cast(NULL AS VARCHAR(255))                  AS inv_invoicenumber ,
                                                                                            cast(NULL AS bigint)                        AS invitem_id ,
                                                                                            cast(NULL AS VARCHAR(60))                   AS chargecategorycode ,
                                                                                            cast(NULL AS VARCHAR(60))                   AS tranfer_method_typ ,
                                                                                            cast(NULL AS timestamp)                     AS ev_strt_dt ,
                                                                                            cast(NULL AS timestamp)                     AS ev_end_dt ,
                                                                                            glb.glmonth_stg                             AS glmonth ,
                                                                                            glb.glyear_stg                              AS glyear ,
                                                                                            glb.accountingdate_stg                      AS accountingdate ,
                                                                                            bc_disbursement.createtime_stg              AS fincl_ev_prd_strt_dt ,
                                                                                            bc_disbursement.retired_stg                 AS retired ,
                                                                                            cast(NULL AS      VARCHAR(60))                   AS ev_med_type_cd ,
                                                                                            cast(NULL AS      VARCHAR(255))                  AS funds_tfr_type_cd ,
                                                                                            cast(''DLY0110'' AS VARCHAR(10))                   AS query_id ,
                                                                                            cast(''1900-01-01 00:00:00.000000'' AS timestamp)  AS updatetime
                                                                                  FROM      db_t_prod_stag.bc_disbursement
                                                                                  left join db_t_prod_stag.gl_eventstaging_bc glb
                                                                                  ON        glb.publicid_stg = bc_disbursement.publicid_stg
                                                                                  AND       upper(glb.rootentity_stg) = ''OUTGOINGDISBPMNT''
                                                                                  WHERE     bc_disbursement.updatetime_stg > (:START_DTTM)
                                                                                  AND       bc_disbursement.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* ****************************REVWRITEOFF***************************** */
                                                                                  SELECT    cast(''EV_ACTVY_TYPE34'' AS  VARCHAR(60))         AS ev_act_type_code ,
                                                                                            cast(bc_writeoff.id_stg AS VARCHAR(60))         AS key1 ,
                                                                                            ''EV_SBTYPE2''                                    AS SUBTYPE ,
                                                                                            ''FINCL_EV_TYPE3''                                AS financl_ev_type ,
                                                                                            bc_writeoff.executiondate_stg                   AS eff_dt ,
                                                                                            cast('''' AS VARCHAR(60))                         AS refnumber ,
                                                                                            cast('''' AS VARCHAR(60))                         AS trans_host_num ,
                                                                                            ''BILL''                                          AS funccd ,
                                                                                            bc_invoice.invoicenumber_stg                    AS invoicenumber ,
                                                                                            bc_invoiceitem.id_stg                           AS id_invoiceitem ,
                                                                                            bctl_chargecategory.typecode_stg                AS typecode_chargecategory ,
                                                                                            cast(NULL AS VARCHAR(60))                       AS tranfer_method_typ ,
                                                                                            cast(NULL AS timestamp)                         AS ev_strt_dt ,
                                                                                            cast(NULL AS timestamp)                         AS ev_end_dt ,
                                                                                            NULL                                            AS glmonth ,
                                                                                            NULL                                            AS glyear ,
                                                                                            cast(NULL AS DATE)                              AS accountingdate ,
                                                                                            bc_writeoff.createtime_stg                      AS fincl_ev_prd_strt_dt ,
                                                                                            bc_writeoff.retired_stg                         AS retired ,
                                                                                            cast(NULL AS      VARCHAR(60))                  AS ev_med_type_cd ,
                                                                                            cast(NULL AS      VARCHAR(255))                 AS funds_tfr_type_cd ,
                                                                                            cast(''DLY0120'' AS VARCHAR(10))                  AS query_id ,
                                                                                            cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS updatetime
                                                                                  FROM      db_t_prod_stag.bc_writeoff
                                                                                  left join db_t_prod_stag.bc_invoiceitem
                                                                                  ON        bc_invoiceitem.id_stg =bc_writeoff.itemwritenoff_alfa_stg
                                                                                  left join db_t_prod_stag.bc_invoice
                                                                                  ON        bc_invoice.id_stg =bc_invoiceitem.invoiceid_stg
                                                                                  left join db_t_prod_stag.bc_charge
                                                                                  ON        bc_charge.id_stg =bc_invoiceitem.chargeid_stg
                                                                                  left join db_t_prod_stag.bc_chargepattern
                                                                                  ON        bc_chargepattern.id_stg =bc_charge.chargepatternid_stg
                                                                                  left join db_t_prod_stag.bctl_chargecategory
                                                                                  ON        bctl_chargecategory.id_stg =bc_chargepattern.category_stg
                                                                                  left join db_t_prod_stag.bc_revwriteoff
                                                                                  ON        bc_writeoff.id_stg = bc_revwriteoff.ownerid_stg
                                                                                  WHERE     bc_revwriteoff.ownerid_stg IS NOT NULL
                                                                                  AND       bc_writeoff.updatetime_stg > (:START_DTTM)
                                                                                  AND       bc_writeoff.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /* ****************************WRITEOFF***************************** */
                                                                                  SELECT    cast(''EV_ACTVY_TYPE33'' AS  VARCHAR(60))         AS ev_act_type_code ,
                                                                                            cast(bc_writeoff.id_stg AS VARCHAR(60))         AS key1 ,
                                                                                            ''EV_SBTYPE2''                                    AS SUBTYPE ,
                                                                                            ''FINCL_EV_TYPE3''                                AS financl_ev_type ,
                                                                                            bc_writeoff.executiondate_stg                   AS eff_dt ,
                                                                                            cast('''' AS VARCHAR(60))                         AS refnumber ,
                                                                                            cast('''' AS VARCHAR(60))                         AS trans_host_num ,
                                                                                            ''BILL''                                          AS funccd ,
                                                                                            bc_invoice.invoicenumber_stg                    AS invoicenumber ,
                                                                                            bc_invoiceitem.id_stg                           AS id_invoiceitem ,
                                                                                            bctl_chargecategory.typecode_stg                AS typecode_chargecategory ,
                                                                                            cast(NULL AS VARCHAR(60))                       AS tranfer_method_typ ,
                                                                                            cast(NULL AS timestamp)                         AS ev_strt_dt ,
                                                                                            cast(NULL AS timestamp)                         AS ev_end_dt ,
                                                                                            NULL                                            AS glmonth ,
                                                                                            NULL                                            AS glyear ,
                                                                                            cast(NULL AS DATE)                              AS accountingdate ,
                                                                                            bc_writeoff.createtime_stg                      AS fincl_ev_prd_strt_dt ,
                                                                                            bc_writeoff.retired_stg                         AS retired ,
                                                                                            cast(NULL AS      VARCHAR(60))                  AS ev_med_type_cd ,
                                                                                            cast(NULL AS      VARCHAR(255))                 AS funds_tfr_type_cd ,
                                                                                            cast(''DLY0130'' AS VARCHAR(10))                  AS query_id ,
                                                                                            cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS updatetime
                                                                                  FROM      db_t_prod_stag.bc_writeoff
                                                                                  left join db_t_prod_stag.bc_invoiceitem
                                                                                  ON        bc_invoiceitem.id_stg =bc_writeoff.itemwritenoff_alfa_stg
                                                                                  left join db_t_prod_stag.bc_invoice
                                                                                  ON        bc_invoice.id_stg =bc_invoiceitem.invoiceid_stg
                                                                                  left join db_t_prod_stag.bc_charge
                                                                                  ON        bc_charge.id_stg =bc_invoiceitem.chargeid_stg
                                                                                  left join db_t_prod_stag.bc_chargepattern
                                                                                  ON        bc_chargepattern.id_stg =bc_charge.chargepatternid_stg
                                                                                  left join db_t_prod_stag.bctl_chargecategory
                                                                                  ON        bctl_chargecategory.id_stg =bc_chargepattern.category_stg
                                                                                  left join db_t_prod_stag.bc_revwriteoff
                                                                                  ON        bc_writeoff.id_stg = bc_revwriteoff.ownerid_stg
                                                                                  WHERE     bc_revwriteoff.ownerid_stg IS NULL
                                                                                  AND       bc_writeoff.updatetime_stg > (:START_DTTM)
                                                                                  AND       bc_writeoff.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  /***************************Billing Transaction*************************/
                                                                                  /**EIM-42673**/
                                                                                  SELECT DISTINCT cast(bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_transaction.id_stg AS         VARCHAR(50)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                       AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                   AS financl_ev_type ,
                                                                                                  bc_transaction.transactiondate_stg                 AS eff_dt ,
                                                                                                  bc_transaction.transactionnumber_stg               AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                            AS trans_host_num ,
                                                                                                  ''BILL''                                             AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                       AS inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                              AS invitem_id ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_chargecategory.typecode_stg IS NULL THEN f.typecode_stg
                                                                                                                  ELSE bctl_chargecategory.typecode_stg
                                                                                                  END                                                                                      AS chargecategorycode,
                                                                                                  cast(NULL AS VARCHAR(60))                                                                AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                                                  AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                                                  AS ev_end_dt ,
                                                                                                  NULL                                                                                     AS glmonth ,
                                                                                                  NULL                                                                                     AS glyear ,
                                                                                                  cast(accountingdate_stg AS DATE)                                                         AS accountingdate ,
                                                                                                  bc_transaction.createtime_stg                                                            AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_transaction.retired_stg                                                               AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                                           AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                                          AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0140'' AS VARCHAR(10))                                                           AS query_id ,
                                                                                                  coalesce(bc_invoiceitem.updatetime_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_transaction
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg =bc_transaction.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_lineitem
                                                                                  ON              bc_lineitem.transactionid_stg =bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_taccount
                                                                                  ON              bc_taccount.id_stg =bc_lineitem.taccountid_stg
                                                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg =bc_taccount.taccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bc_itemevent
                                                                                  ON              bc_itemevent.transactionid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_invoiceitem
                                                                                  ON              bc_invoiceitem.id_stg = bc_itemevent.invoiceitemid_stg
                                                                                  left outer join db_t_prod_stag.bc_invoice
                                                                                  ON              bc_invoice.id_stg = bc_invoiceitem.invoiceid_stg
                                                                                  left outer join db_t_prod_stag.bc_charge
                                                                                  ON              bc_invoiceitem.chargeid_stg = bc_charge.id_stg
                                                                                  left outer join db_t_prod_stag.bc_chargepattern
                                                                                  ON              bc_charge.chargepatternid_stg = bc_chargepattern.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg = bc_chargepattern.category_stg
                                                                                  left join       db_t_prod_stag.gl_eventstaging_bc glbc
                                                                                  ON              glbc.publicid_stg = bc_transaction.publicid_stg
                                                                                  AND             bctl_transaction.typecode_stg = glbc.rootentity_stg
                                                                                  left outer join db_t_prod_stag.bc_chargeinstancecontext
                                                                                  ON              bc_chargeinstancecontext.transactionid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_charge e
                                                                                  ON              e.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                                                  left outer join db_t_prod_stag.bc_chargepattern h
                                                                                  ON              h.id_stg = e.chargepatternid_stg
                                                                                  left outer join db_t_prod_stag.bctl_chargecategory f
                                                                                  ON              f.id_stg = h.category_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.updatetime_stg > (:START_DTTM)
                                                                                                                  AND             bc_transaction.updatetime_stg <= (:END_DTTM))
                                                                                                  OR              (
                                                                                                                                  bc_policyperiod.updatetime_stg > (:START_DTTM)
                                                                                                                  AND             bc_policyperiod.updatetime_stg <= (:END_DTTM)))
                                                                                  UNION ALL
                                                                                  /**EIM-42673**/
                                                                                  SELECT DISTINCT cast(''rvrs''
                                                                                                                  || ''-''
                                                                                                                  || bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_transaction.id_stg AS                       VARCHAR(50)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                                     AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                                 AS financl_ev_type ,
                                                                                                  bc_transaction.transactiondate_stg                               AS eff_dt ,
                                                                                                  bc_transaction.transactionnumber_stg                             AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                                          AS trans_host_num ,
                                                                                                  ''BILL''                                                           AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                                     AS inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                                            AS invitem_id ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_chargecategory.typecode_stg IS NULL THEN f.typecode_stg
                                                                                                                  ELSE bctl_chargecategory.typecode_stg
                                                                                                  END                                                                                      AS chargecategorycode,
                                                                                                  cast(NULL AS VARCHAR(60))                                                                AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                                                  AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                                                  AS ev_end_dt ,
                                                                                                  NULL                                                                                     AS glmonth ,
                                                                                                  NULL                                                                                     AS glyear ,
                                                                                                  cast(accountingdate_stg AS DATE)                                                         AS accountingdate ,
                                                                                                  bc_transaction.createtime_stg                                                            AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_transaction.retired_stg                                                               AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                                           AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                                          AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0150'' AS VARCHAR(10))                                                           AS query_id ,
                                                                                                  coalesce(bc_invoiceitem.updatetime_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS updatetime
                                                                                  FROM            db_t_prod_stag.bc_transaction
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg =bc_transaction.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_lineitem
                                                                                  ON              bc_lineitem.transactionid_stg =bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_taccount
                                                                                  ON              bc_taccount.id_stg =bc_lineitem.taccountid_stg
                                                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg =bc_taccount.taccountcontainerid_stg
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bc_itemevent
                                                                                  ON              bc_itemevent.transactionid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_invoiceitem
                                                                                  ON              bc_invoiceitem.id_stg = bc_itemevent.invoiceitemid_stg
                                                                                  left outer join db_t_prod_stag.bc_invoice
                                                                                  ON              bc_invoice.id_stg = bc_invoiceitem.invoiceid_stg
                                                                                  left outer join db_t_prod_stag.bc_charge
                                                                                  ON              bc_invoiceitem.chargeid_stg = bc_charge.id_stg
                                                                                  left outer join db_t_prod_stag.bc_chargepattern
                                                                                  ON              bc_charge.chargepatternid_stg = bc_chargepattern.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg = bc_chargepattern.category_stg
                                                                                  left join       db_t_prod_stag.bc_revtrans
                                                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                                  left join       db_t_prod_stag.gl_eventstaging_bc glbc
                                                                                  ON              glbc.publicid_stg = bc_transaction.publicid_stg
                                                                                  AND             bctl_transaction.typecode_stg = glbc.rootentity_stg
                                                                                  left outer join db_t_prod_stag.bc_chargeinstancecontext
                                                                                  ON              bc_chargeinstancecontext.transactionid_stg = bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_charge e
                                                                                  ON              e.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                                                  left outer join db_t_prod_stag.bc_chargepattern h
                                                                                  ON              h.id_stg = e.chargepatternid_stg
                                                                                  left outer join db_t_prod_stag.bctl_chargecategory f
                                                                                  ON              f.id_stg = h.category_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.updatetime_stg > (:START_DTTM)
                                                                                                                  AND             bc_transaction.updatetime_stg <= (:END_DTTM))
                                                                                                  OR              (
                                                                                                                                  bc_policyperiod.updatetime_stg > (:START_DTTM)
                                                                                                                  AND             bc_policyperiod.updatetime_stg <= (:END_DTTM)))
                                                                                  AND             bc_revtrans.ownerid_stg IS NOT NULL
                                                                                  UNION ALL
                                                                                  /***************************Claim Check Event****************************/
                                                                                  SELECT          cast(''EV_ACTVY_TYPE24'' AS     VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(cc_transaction.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                               AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE1''                           AS financl_ev_type ,
                                                                                                  cast(NULL AS timestamp)                    AS eff_dt ,
                                                                                                  cast('''' AS   VARCHAR(60))                    AS refnumber ,
                                                                                                  cast(NULL AS VARCHAR(60))                    AS trans_host_num ,
                                                                                                  ''''                                           AS funccd ,
                                                                                                  cast(NULL AS VARCHAR(255))                   AS inv_invoicenumber ,
                                                                                                  cast(NULL AS bigint)                         AS invitem_id ,
                                                                                                  cast(NULL AS VARCHAR(60))                    AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                    AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                      AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                      AS ev_end_dt ,
                                                                                                  NULL                                         AS glmonth ,
                                                                                                  NULL                                         AS glyear ,
                                                                                                  cast(NULL AS DATE)                           AS accountingdate ,
                                                                                                  cc_transaction.createtime_stg                AS fincl_ev_prd_strt_dt ,
                                                                                                  cc_transaction.retired_stg                   AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                    AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                   AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0160'' AS VARCHAR(10))                    AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)   AS updatetime
                                                                                  FROM            db_t_prod_stag.cc_transaction
                                                                                  inner join      db_t_prod_stag.cctl_transaction
                                                                                  ON              cctl_transaction.id_stg = cc_transaction.subtype_stg
                                                                                  inner join      db_t_prod_stag.cctl_transactionstatus AS cctlts
                                                                                  ON              cc_transaction.status_stg= cctlts.id_stg
                                                                                  inner join      db_t_prod_stag.cc_claim
                                                                                  ON              cc_claim.id_stg=cc_transaction.claimid_stg
                                                                                  inner join      db_t_prod_stag.cctl_claimstate
                                                                                  ON              cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                  inner join      db_t_prod_stag.cc_policy
                                                                                  ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                  left outer join db_t_prod_stag.cc_exposure
                                                                                  ON              cc_exposure.id_stg=cc_transaction.exposureid_stg
                                                                                  left outer join db_t_prod_stag.cc_check
                                                                                  ON              cc_check.id_stg = cc_transaction.checkid_stg
                                                                                  left outer join db_t_prod_stag.cc_user
                                                                                  ON              cc_transaction.createuserid_stg = cc_user.id_stg
                                                                                  left outer join db_t_prod_stag.cc_contact
                                                                                  ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                  left join       db_t_prod_stag.gl_eventstaging_cc gle
                                                                                  ON              cc_transaction.publicid_stg = gle.publicid_stg
                                                                                  WHERE           cctl_claimstate.name_stg <> ''Draft''
                                                                                  AND             cctl_transaction.typecode_stg = ''Payment''
                                                                                  AND             ((
                                                                                                                                  cc_transaction.updatetime_stg >(:START_DTTM)
                                                                                                                  AND             cc_transaction.updatetime_stg <= (:END_DTTM))
                                                                                                  OR              (
                                                                                                                                  cc_check.updatetime_stg >(:START_DTTM)
                                                                                                                  AND             cc_check.updatetime_stg <= (:END_DTTM)))
                                                                                  AND             (
                                                                                                                  CASE
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''voided''
                                                                                                                                                  AND             gle.payload_new_stg=''voided_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''voided''
                                                                                                                                                  AND             gle.payload_new_stg= ''voided_15'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''transferred''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''transferred''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_13'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg ='' transferred''
                                                                                                                                                  AND             gle.payload_new_stg=''cleared_13'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg=''recoded_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg = ''recoded_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg=''issued_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''cleared_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''requested_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''voided_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_14'') THEN ''N''
                                                                                                                                                  /*EIM-41121*/
                                                                                                                                  ELSE ''Y''
                                                                                                                  END) = ''Y''
                                                                                  UNION ALL
                                                                                  /***************************Cliam Recovery****************************/
                                                                                  SELECT          cast(''EV_ACTVY_TYPE23'' AS     VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(cc_transaction.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                               AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE5''                           AS financl_ev_type ,
                                                                                                  cast(NULL AS timestamp)                    AS eff_dt ,
                                                                                                  cast('''' AS                               VARCHAR(60))                    AS refnumber ,
                                                                                                  cast(cc_transaction.checknum_alfa_stg AS VARCHAR(60))                    AS trans_host_num ,
                                                                                                  ''''                                                                       AS funccd ,
                                                                                                  cast(NULL AS VARCHAR(255))                                               AS inv_invoicenumber ,
                                                                                                  cast(NULL AS bigint)                                                     AS invitem_id ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                                  AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                                  AS ev_end_dt ,
                                                                                                  NULL                                                                     AS glmonth ,
                                                                                                  NULL                                                                     AS glyear ,
                                                                                                  cast(NULL AS DATE)                                                       AS accountingdate ,
                                                                                                  cc_transaction.createtime_stg                                            AS fincl_ev_prd_strt_dt ,
                                                                                                  cc_transaction.retired_stg                                               AS retired ,
                                                                                                  cast(NULL AS      VARCHAR(60))                                                AS ev_med_type_cd ,
                                                                                                  cast(NULL AS      VARCHAR(255))                                               AS funds_tfr_type_cd ,
                                                                                                  cast(''DLY0170'' AS VARCHAR(10))                                                AS query_id ,
                                                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp)                               AS updatetime
                                                                                  FROM            db_t_prod_stag.cc_transaction
                                                                                  inner join      db_t_prod_stag.cctl_transaction
                                                                                  ON              cctl_transaction.id_stg = cc_transaction.subtype_stg
                                                                                  inner join      db_t_prod_stag.cctl_transactionstatus AS cctlts
                                                                                  ON              cc_transaction.status_stg= cctlts.id_stg
                                                                                  inner join      db_t_prod_stag.cc_claim
                                                                                  ON              cc_claim.id_stg=cc_transaction.claimid_stg
                                                                                  inner join      db_t_prod_stag.cctl_claimstate
                                                                                  ON              cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                  inner join      db_t_prod_stag.cc_policy
                                                                                  ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                  left outer join db_t_prod_stag.cc_exposure
                                                                                  ON              cc_exposure.id_stg=cc_transaction.exposureid_stg
                                                                                  left outer join db_t_prod_stag.cc_check
                                                                                  ON              cc_check.id_stg = cc_transaction.checkid_stg
                                                                                  left outer join db_t_prod_stag.cc_user
                                                                                  ON              cc_transaction.createuserid_stg = cc_user.id_stg
                                                                                  left outer join db_t_prod_stag.cc_contact
                                                                                  ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                  left join       db_t_prod_stag.gl_eventstaging_cc gle
                                                                                  ON              cc_transaction.publicid_stg = gle.publicid_stg
                                                                                  WHERE           cctl_claimstate.name_stg <> ''Draft''
                                                                                  AND             cctl_transaction.typecode_stg = ''Recovery''
                                                                                  AND             cc_transaction.checknum_alfa_stg IS NOT NULL
                                                                                  AND             ((
                                                                                                                                  cc_transaction.updatetime_stg >(:START_DTTM)
                                                                                                                  AND             cc_transaction.updatetime_stg <= (:END_DTTM))
                                                                                                  OR              (
                                                                                                                                  cc_check.updatetime_stg >(:START_DTTM)
                                                                                                                  AND             cc_check.updatetime_stg <= (:END_DTTM)))
                                                                                  AND             (
                                                                                                                  CASE
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''voided''
                                                                                                                                                  AND             gle.payload_new_stg=''voided_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''voided''
                                                                                                                                                  AND             gle.payload_new_stg= ''voided_15'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''transferred''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''transferred''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_13'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg ='' transferred''
                                                                                                                                                  AND             gle.payload_new_stg=''cleared_13'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg=''recoded_11'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg = ''recoded_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg=''issued_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''cleared_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''requested_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''voided_14'') THEN ''N''
                                                                                                                                  WHEN (
                                                                                                                                                                  cctlts.typecode_stg = ''recoded''
                                                                                                                                                  AND             gle.payload_new_stg= ''transferred_14'') THEN ''N''
                                                                                                                                                  /*EIM-41121*/
                                                                                                                                  ELSE ''Y''
                                                                                                                  END) = ''Y''
                                                                                  UNION ALL
                                                                                  SELECT     cast(''EV_ACTVY_TYPE35'' AS                 VARCHAR(60))  AS ev_act_type_code ,
                                                                                             cast(bc_paymentrequest.id_stg AS          VARCHAR(60))  AS key1 ,
                                                                                             cast(''EV_SBTYPE2'' AS                      VARCHAR(60))  AS SUBTYPE ,
                                                                                             cast(''FINCL_EV_TYPE3'' AS                  VARCHAR(60))  AS financl_ev_type ,
                                                                                             cast(bc_paymentrequest.requestdate_stg AS DATE)         AS eff_dt ,
                                                                                             cast(bc_paymentrequest.tracenumber_stg AS VARCHAR(60))  AS refnumber ,
                                                                                             cast('''' AS                                VARCHAR(60))  AS trans_host_num ,
                                                                                             cast(''BILL'' AS                            VARCHAR(60))  AS funccd ,
                                                                                             cast(bc_invoice.invoicenumber_stg AS      VARCHAR(255)) AS inv_invoicenumber ,
                                                                                             cast(NULL AS                              bigint)       AS invitem_id ,
                                                                                             cast(NULL AS                              VARCHAR(60))  AS chargecategorycode ,
                                                                                             cast(bctl_paymentmethod.typecode_stg AS   VARCHAR(60))  AS tranfer_method_typ ,
                                                                                             cast(NULL AS timestamp)                                 AS ev_strt_dt ,
                                                                                             cast(NULL AS timestamp)                                 AS ev_end_dt ,
                                                                                             NULL                                                    AS glmonth ,
                                                                                             NULL                                                    AS glyear ,
                                                                                             cast(NULL AS DATE)                                      AS accountingdate ,
                                                                                             bc_paymentrequest.createtime_stg                        AS fincl_ev_prd_strt_dt ,
                                                                                             bc_paymentrequest.retired_stg                           AS retired ,
                                                                                             cast(NULL AS      VARCHAR(60))                               AS ev_med_type_cd ,
                                                                                             cast(NULL AS      VARCHAR(255))                              AS funds_tfr_type_cd ,
                                                                                             cast(''DLY0180'' AS VARCHAR(10))                               AS query_id ,
                                                                                             cast(''1900-01-01 00:00:00.000000'' AS timestamp)              AS updatetime
                                                                                  FROM       db_t_prod_stag.bc_paymentrequest
                                                                                  inner join db_t_prod_stag.bctl_paymentrequeststatus
                                                                                  ON         bc_paymentrequest.status_stg = bctl_paymentrequeststatus.id_stg
                                                                                  left join  db_t_prod_stag.bc_paymentinstrument
                                                                                  ON         bc_paymentrequest.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left join  db_t_prod_stag.bctl_paymentmethod
                                                                                  ON         bc_paymentinstrument.paymentmethod_stg = bctl_paymentmethod.id_stg
                                                                                  left join  db_t_prod_stag.bc_invoice
                                                                                  ON         bc_paymentrequest.invoiceid_stg=bc_invoice.id_stg
                                                                                  WHERE      bc_paymentrequest.updatetime_stg > (:START_DTTM)
                                                                                  AND        bc_paymentrequest.updatetime_stg <= (:END_DTTM)
                                                                                  UNION ALL
                                                                                  SELECT    cast(''EV_ACTVY_TYPE36'' AS       VARCHAR(60)) AS ev_act_type_code ,
                                                                                            cast(bc_unappliedfund.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                            ''EV_SBTYPE2''                                 AS SUBTYPE ,
                                                                                            ''FINCL_EV_TYPE3''                             AS financl_ev_type ,
                                                                                            bc_unappliedfund.createtime_stg              AS eff_dt ,
                                                                                            cast('''' AS     VARCHAR(60))                      AS refnumber ,
                                                                                            cast('''' AS     VARCHAR(60))                      AS trans_host_num ,
                                                                                            cast(''BILL'' AS VARCHAR(60))                      AS funccd ,
                                                                                            cast(NULL AS   VARCHAR(255))                     AS inv_invoicenumber ,
                                                                                            cast(NULL AS   bigint)                           AS invitem_id ,
                                                                                            cast(NULL AS   VARCHAR(60))                      AS chargecategorycode ,
                                                                                            cast('''' AS     VARCHAR(60))                      AS tranfer_method_typ ,
                                                                                            cast(NULL AS timestamp)                          AS ev_strt_dt ,
                                                                                            cast(NULL AS timestamp)                          AS ev_end_dt ,
                                                                                            NULL                                             AS glmonth ,
                                                                                            NULL                                             AS glyear ,
                                                                                            cast(NULL AS DATE)                               AS accountingdate ,
                                                                                            bc_unappliedfund.createtime_stg                  AS fincl_ev_prd_strt_dt ,
                                                                                            cast(''0'' AS       INT)                                 AS retired ,
                                                                                            cast(NULL AS      VARCHAR(60))                         AS ev_med_type_cd ,
                                                                                            cast(NULL AS      VARCHAR(255))                        AS funds_tfr_type_cd ,
                                                                                            cast(''DLY0190'' AS VARCHAR(10))                         AS query_id ,
                                                                                            cast(''1900-01-01 00:00:00.000000'' AS timestamp)        AS updatetime
                                                                                  FROM      db_t_prod_stag.bc_unappliedfund
                                                                                  left join db_t_prod_stag.bc_account
                                                                                  ON        bc_unappliedfund.accountid_stg = bc_account.id_stg
                                                                                  left join db_t_prod_stag.bc_taccount
                                                                                  ON        bc_unappliedfund.taccountid_stg = bc_taccount.id_stg
                                                                                  WHERE     (
                                                                                                      bc_unappliedfund.updatetime_stg > (:START_DTTM)
                                                                                            AND       bc_unappliedfund.updatetime_stg <= (:END_DTTM))
                                                                                  OR        (
                                                                                                      bc_taccount.updatetime_stg > (:START_DTTM)
                                                                                            AND       bc_taccount.updatetime_stg <= (:END_DTTM)) ) src_fncl_ev )
                  /* *********************Source Query ends here ********************* */
                  SELECT          xlat_src.ev_id,
                                  doc_id,
                                  financl_ev_type_cd,
                                  eff_date,
                                  refnumber,
                                  trans_host_num ,
                                  funccd,
                                  xlat_src.ar_invc_ln_num,
                                  xlat_src.ar_invc_ln_amt_type_cd,
                                  xlat_src.funds_tfr_meth_type_cd,
                                  CASE
                                                  WHEN tgt_fincl_ev.gl_mth_num IS NOT NULL
                                                  AND             query_id IN (''DLY0010'',
                                                                               ''DLY0140'',
                                                                               ''DLY0150'') THEN tgt_fincl_ev.gl_mth_num
                                                  ELSE glmonth
                                  END AS gl_mth_num_new,
                                  CASE
                                                  WHEN tgt_fincl_ev.gl_yr_num IS NOT NULL
                                                  AND             query_id IN (''DLY0010'',
                                                                               ''DLY0140'',
                                                                               ''DLY0150'') THEN tgt_fincl_ev.gl_yr_num
                                                  ELSE glyear
                                  END AS gl_yr_num_new,
                                  CASE
                                                  WHEN tgt_fincl_ev.accntg_dy_num IS NOT NULL
                                                  AND             query_id IN (''DLY0010'',
                                                                               ''DLY0140'',
                                                                               ''DLY0150'') THEN tgt_fincl_ev.accntg_dy_num
                                                  ELSE xlat_src.accntg_dy_num
                                  END AS accntg_dy_num_new,
                                  CASE
                                                  WHEN tgt_fincl_ev.accntg_mth_num IS NOT NULL
                                                  AND             query_id IN (''DLY0010'',
                                                                               ''DLY0140'',
                                                                               ''DLY0150'') THEN tgt_fincl_ev.accntg_mth_num
                                                  ELSE xlat_src.accntg_mth_num
                                  END AS accntg_mth_num_new,
                                  CASE
                                                  WHEN tgt_fincl_ev.accntg_yr_num IS NOT NULL
                                                  AND             query_id IN (''DLY0010'',
                                                                               ''DLY0140'',
                                                                               ''DLY0150'') THEN tgt_fincl_ev.accntg_yr_num
                                                  ELSE xlat_src.accntg_yr_num
                                  END AS accntg_yr_num_new,
                                  xlat_src.fincl_ev_prd_strt_dt ,
                                  xlat_src.fincl_ev_prd_end_dt,
                                  retired,
                                  xlat_src.ev_med_type_cd,
                                  xlat_src.funds_tfr_type_cd,
                                  rnk,
                                  tgt_fincl_ev.edw_strt_dttm,
                                  tgt_fincl_ev.edw_end_dttm,
                                  cast( (xlat_src.financl_ev_type_cd
                                                  || cast(TO_CHAR(eff_date ,''YYYYMMDD'') AS VARCHAR(10))
                                                  || trim( coalesce(refnumber,0))
                                                  || trim(coalesce( xlat_src.ar_invc_ln_amt_type_cd,0))
                                                  || trim( xlat_src.funds_tfr_meth_type_cd)
                                                  || cast(coalesce(gl_mth_num_new,0) AS     INTEGER)
                                                  || cast(coalesce( gl_yr_num_new,0) AS     INTEGER)
                                                  || cast(coalesce(accntg_dy_num_new,0) AS  INTEGER)
                                                  || cast(coalesce(accntg_mth_num_new,0) AS INTEGER)
                                                  || cast(coalesce(accntg_yr_num_new,0) AS  INTEGER)
                                                  || cast(xlat_src.fincl_ev_prd_strt_dt AS  VARCHAR(30)) ) AS VARCHAR(1100)) AS sourcedata,
                                  cast( (tgt_fincl_ev.fincl_ev_type_cd
                                                  || cast(to_char(tgt_fincl_ev.funds_tfr_eff_dttm ,''YYYYMMDD'') AS VARCHAR(10))
                                                  || trim(coalesce(tgt_fincl_ev.funds_tfr_ref_num,0))
                                                  || trim(coalesce(tgt_fincl_ev.ar_invc_ln_amt_type_cd,0))
                                                  || trim(coalesce(tgt_fincl_ev.funds_tfr_meth_type_cd,0))
                                                  || coalesce(cast(trim(tgt_fincl_ev.gl_mth_num) AS INTEGER),0)
                                                  || cast(coalesce(tgt_fincl_ev.gl_yr_num,0) AS      INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_dy_num,0) AS  INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_mth_num,0) AS INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_yr_num,0) AS  INTEGER)
                                                  || cast(tgt_fincl_ev.fincl_ev_prd_strt_dttm AS     VARCHAR(30)) ) AS VARCHAR(1100)) AS targetdata,
                                  CASE
                                                  WHEN tgt_fincl_ev.ev_id IS NULL THEN ''I''
                                                  WHEN tgt_fincl_ev.ev_id IS NOT NULL
                                                  AND             sourcedata <> targetdata THEN ''U''
                                                  WHEN tgt_fincl_ev.ev_id IS NOT NULL
                                                  AND             sourcedata = targetdata
                                                  AND             (
                                                                                  (
                                                                                                  to_char(coalesce(doc_id,0))
                                                                                                                  || trim(coalesce(xlat_src.ar_invc_ln_num,0))))<>(to_char(coalesce(tgt_fincl_ev.ar_invc_id,0))
                                                                                  || trim(coalesce(tgt_fincl_ev.ar_invc_ln_num,0)))
                                                  AND             (
                                                                                  (
                                                                                                  to_char(coalesce(doc_id,0))
                                                                                                                  || trim(coalesce(xlat_src.ar_invc_ln_num,0))))<>''00'' THEN ''U''
                                                  ELSE ''R''
                                  END AS ins_upd_flag
                  FROM            (
                                                  SELECT          evt.ev_id,
                                                                  dc.doc_id,
                                                                  coalesce(xlat_fincl_ev_type.tgt_idntftn_val, ''UNK'') AS financl_ev_type_cd,
                                                                  coalesce(src.eff_dt, cast(''1900-01-01'' AS DATE ))   AS eff_date,
                                                                  refnumber,
                                                                  trans_host_num,
                                                                  funccd,
                                                                  ar_il.ar_invc_ln_num,
                                                                  xlat_invc_amt_type.tgt_idntftn_val                    AS ar_invc_ln_amt_type_cd,
                                                                  coalesce(xlat_funds_trans_meth.tgt_idntftn_val,''UNK'') AS funds_tfr_meth_type_cd,
                                                                  glmonth,
                                                                  glyear,
                                                                  extract(day FROM accountingdate)   AS accntg_dy_num,
                                                                  extract(month FROM accountingdate) AS accntg_mth_num,
                                                                  extract(year FROM accountingdate)  AS accntg_yr_num,
                                                                  fincl_ev_prd_strt_dt ,
                                                                  to_timestamp_ntz(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DDBHH:MI:SS.S(6)'' ) AS fincl_ev_prd_end_dt,
                                                                  retired,
                                                                  ev_med_type_cd,
                                                                  funds_tfr_type_cd,
                                                                  rnk,
                                                                  query_id
                                                  FROM            intrm_fncl_ev                        AS src
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_act_type_cd
                                                  ON              xlat_act_type_cd.src_idntftn_val = src.ev_act_type_code
                                                  AND             xlat_act_type_cd.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                  AND             xlat_act_type_cd.src_idntftn_sys IN (''GW'',
                                                                                                       ''DS'' )
                                                  AND             xlat_act_type_cd.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_sbtype
                                                  ON              xlat_ev_sbtype.src_idntftn_val = src.SUBTYPE
                                                  AND             xlat_ev_sbtype.tgt_idntftn_nm= ''EV_SBTYPE''
                                                  AND             xlat_ev_sbtype.src_idntftn_nm = ''derived''
                                                  AND             xlat_ev_sbtype.src_idntftn_sys=''DS''
                                                  AND             xlat_ev_sbtype.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_doc_type
                                                  ON              xlat_doc_type.src_idntftn_val = ''DOC_TYPE3''
                                                  AND             xlat_doc_type.tgt_idntftn_nm= ''DOC_TYPE''
                                                  AND             xlat_doc_type.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_doc_ctgy_type
                                                  ON              xlat_doc_ctgy_type.src_idntftn_val = ''DOC_CTGY_TYPE4''
                                                  AND             xlat_doc_ctgy_type.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                  AND             xlat_doc_ctgy_type.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_funds_trans_meth
                                                  ON              xlat_funds_trans_meth.src_idntftn_val = src.tranfer_method_typ
                                                  AND             xlat_funds_trans_meth.tgt_idntftn_nm= ''FUNDS_TFR_METH_TYPE''
                                                  AND             xlat_funds_trans_meth.src_idntftn_nm IN (''derived'',
                                                                                                           ''bctl_paymentmethod.Typecode'')
                                                  AND             xlat_funds_trans_meth.src_idntftn_sys IN (''DS'' ,
                                                                                                            ''GW'')
                                                  AND             xlat_funds_trans_meth.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_fincl_ev_type
                                                  ON              xlat_fincl_ev_type.src_idntftn_val = src.financl_ev_type
                                                  AND             xlat_fincl_ev_type.tgt_idntftn_nm= ''FINCL_EV_TYPE''
                                                  AND             xlat_fincl_ev_type.src_idntftn_nm= ''derived ''
                                                  AND             xlat_fincl_ev_type.src_idntftn_sys=''DS''
                                                  AND             xlat_fincl_ev_type.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_invc_amt_type
                                                  ON              xlat_invc_amt_type.src_idntftn_val = src.chargecategorycode
                                                  AND             xlat_invc_amt_type.tgt_idntftn_nm= ''INVC_AMT_TYPE''
                                                  AND             xlat_invc_amt_type.src_idntftn_nm= ''bctl_chargecategory.TYPECODE''
                                                  AND             xlat_invc_amt_type.src_idntftn_sys=''GW''
                                                  AND             xlat_invc_amt_type.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                           SELECT   ar_invc_ln_num,
                                                                                    ar_invc_id,
                                                                                    host_invc_ln_num
                                                                           FROM     db_t_prod_core.ar_invc_ln
                                                                           WHERE    host_invc_ln_num IN
                                                                                                         (
                                                                                                         SELECT DISTINCT cast(invitem_id AS VARCHAR(50))
                                                                                                         FROM            intrm_fncl_ev) qualify row_number() over( PARTITION BY host_invc_ln_num ORDER BY edw_end_dttm DESC) = 1 ) AS ar_il
                                                  ON              cast(ar_il.host_invc_ln_num AS VARCHAR(20)) = cast(src.invitem_id AS VARCHAR(20))
                                                  left outer join
                                                                  (
                                                                           SELECT   ev_id,
                                                                                    src_trans_id,
                                                                                    ev_sbtype_cd,
                                                                                    ev_actvy_type_cd
                                                                           FROM     db_t_prod_core.ev
                                                                           WHERE    src_trans_id IN
                                                                                                     (
                                                                                                     SELECT DISTINCT key1
                                                                                                     FROM            intrm_fncl_ev) qualify row_number() over( PARTITION BY ev_sbtype_cd,ev_actvy_type_cd,src_trans_id ORDER BY edw_end_dttm DESC) = 1 ) AS evt
                                                  ON              evt.src_trans_id = src.key1
                                                  AND             evt.ev_actvy_type_cd = xlat_act_type_cd.tgt_idntftn_val
                                                  AND             evt.ev_sbtype_cd = xlat_ev_sbtype.tgt_idntftn_val
                                                  left outer join
                                                                  (
                                                                           SELECT   doc_id,
                                                                                    doc_issur_num,
                                                                                    doc_type_cd,
                                                                                    doc_ctgy_type_cd
                                                                           FROM     db_t_prod_core.doc
                                                                           WHERE    doc_issur_num IN
                                                                                                      (
                                                                                                      SELECT DISTINCT inv_invoicenumber
                                                                                                      FROM            intrm_fncl_ev) qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS dc
                                                  ON              dc.doc_issur_num = src.inv_invoicenumber
                                                  AND             dc.doc_type_cd = xlat_doc_type.tgt_idntftn_val
                                                  AND             dc.doc_ctgy_type_cd = xlat_doc_ctgy_type.tgt_idntftn_val ) AS xlat_src
                  left outer join
                                  (
                                           SELECT   fincl_ev_prd_strt_dttm ,
                                                    fincl_ev_type_cd ,
                                                    funds_tfr_eff_dttm ,
                                                    funds_tfr_ref_num ,
                                                    funds_tfr_meth_type_cd ,
                                                    ar_invc_id ,
                                                    ar_invc_ln_num ,
                                                    ar_invc_ln_amt_type_cd,
                                                    gl_mth_num,
                                                    gl_yr_num,
                                                    accntg_dy_num ,
                                                    accntg_mth_num ,
                                                    accntg_yr_num ,
                                                    edw_strt_dttm ,
                                                    edw_end_dttm ,
                                                    ev_id
                                           FROM     db_t_prod_core.fincl_ev
                                           WHERE    ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   intrm_fncl_ev)) qualify row_number() over( PARTITION BY ev_id ORDER BY edw_end_dttm DESC) = 1 ) AS tgt_fincl_ev
                  ON              xlat_src.ev_id = tgt_fincl_ev.ev_id
                  ORDER BY        xlat_src.ev_id,
                                  rnk ) src ) );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
         SELECT src_sq_bc_basemoneyreceived.ev_id                                      AS ev_id,
                src_sq_bc_basemoneyreceived.fincl_ev_type                              AS fincl_ev_type_cd,
                src_sq_bc_basemoneyreceived.eff_dt                                     AS funds_tfr_eff_dt,
                src_sq_bc_basemoneyreceived.refnumber                                  AS funds_tfr_ref_num,
                src_sq_bc_basemoneyreceived.doc_id                                     AS ar_invc_id,
                src_sq_bc_basemoneyreceived.ar_invc_ln_num                             AS ar_invc_ln_num,
                src_sq_bc_basemoneyreceived.chargecategorycode                         AS ar_invc_ln_amt_type_cd,
                src_sq_bc_basemoneyreceived.funccode                                   AS func_cd,
                :prcs_id                                                               AS out_prcs_id,
                src_sq_bc_basemoneyreceived.tranfer_method_typ                         AS funds_tfr_meth_type_cd,
                src_sq_bc_basemoneyreceived.glmonth                                    AS glmonth,
                src_sq_bc_basemoneyreceived.glyear                                     AS glyear,
                src_sq_bc_basemoneyreceived.accountingdaynum                           AS accounting_day,
                src_sq_bc_basemoneyreceived.accountingmonthnum                         AS accounting_month,
                src_sq_bc_basemoneyreceived.accountingyearnum                          AS accounting_year,
                src_sq_bc_basemoneyreceived.createtime                                 AS createtime,
                src_sq_bc_basemoneyreceived.fincl_ev_prd_end_dt                        AS fincl_ev_prd_end_dt,
                src_sq_bc_basemoneyreceived.retired                                    AS retired,
                src_sq_bc_basemoneyreceived.trans_host_num                             AS trans_host_num,
                src_sq_bc_basemoneyreceived.ev_med_type_cd                             AS ev_med_type_cd,
                src_sq_bc_basemoneyreceived.funds_tfr_type_cd                          AS funds_tfr_type_cd,
                src_sq_bc_basemoneyreceived.rnk                                        AS rnk,
                current_timestamp                                                      AS edw_start_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                src_sq_bc_basemoneyreceived.tgt_edw_strt_dttm                          AS tgt_edw_strt_dttm,
                src_sq_bc_basemoneyreceived.tgt_edw_end_dttm                           AS tgt_edw_end_dttm,
                src_sq_bc_basemoneyreceived.ins_upd_flag                               AS ins_upd_flag,
                src_sq_bc_basemoneyreceived.source_record_id
         FROM   src_sq_bc_basemoneyreceived );
  -- Component rtr_fincl_ev_insupd_Grp_Insert, Type ROUTER Output Group Grp_Insert
  create or replace TEMPORARY TABLE rtr_fincl_ev_insupd_grp_insert AS
  SELECT exp_data_transformation.ev_id                  AS ev_id,
         exp_data_transformation.fincl_ev_type_cd       AS fincl_ev_type_cd,
         exp_data_transformation.funds_tfr_eff_dt       AS funds_tfr_eff_dt,
         exp_data_transformation.funds_tfr_ref_num      AS funds_tfr_ref_num,
         exp_data_transformation.ar_invc_id             AS ar_invc_id,
         exp_data_transformation.ar_invc_ln_num         AS ar_invc_ln_num,
         exp_data_transformation.ar_invc_ln_amt_type_cd AS ar_invc_ln_amt_type_cd,
         exp_data_transformation.func_cd                AS func_cd,
         exp_data_transformation.out_prcs_id            AS prcs_id,
         exp_data_transformation.funds_tfr_meth_type_cd AS funds_tfr_meth_type_cd,
         exp_data_transformation.glmonth                AS glmonth,
         exp_data_transformation.glyear                 AS glyear,
         exp_data_transformation.accounting_day         AS accounting_day,
         exp_data_transformation.accounting_month       AS accounting_month,
         exp_data_transformation.accounting_year        AS accounting_year,
         exp_data_transformation.createtime             AS fincl_ev_prd_strt_dt,
         exp_data_transformation.fincl_ev_prd_end_dt    AS fincl_ev_prd_end_dt,
         exp_data_transformation.retired                AS retired,
         exp_data_transformation.trans_host_num         AS trans_host_num,
         exp_data_transformation.ev_med_type_cd         AS ev_med_type_cd,
         exp_data_transformation.funds_tfr_type_cd      AS funds_tfr_type_cd,
         exp_data_transformation.rnk                    AS rnk,
         exp_data_transformation.edw_start_dttm         AS edw_start_dttm,
         exp_data_transformation.edw_end_dttm           AS edw_end_dttm,
         exp_data_transformation.tgt_edw_strt_dttm      AS tgt_edw_strt_dttm,
         exp_data_transformation.tgt_edw_end_dttm       AS tgt_edw_end_dttm,
         exp_data_transformation.ins_upd_flag           AS ins_upd_flag,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.ev_id IS NOT NULL
  AND    (
                exp_data_transformation.ins_upd_flag = ''I''
         OR     exp_data_transformation.ins_upd_flag = ''U''
         OR     (
                       exp_data_transformation.retired = 0
                AND    exp_data_transformation.tgt_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_data_transformation.ev_id IS NOT NULL ) );
  
  -- Component rtr_fincl_ev_insupd_Retired, Type ROUTER Output Group Retired
  create or replace TEMPORARY TABLE rtr_fincl_ev_insupd_retired AS
  SELECT exp_data_transformation.ev_id                  AS ev_id,
         exp_data_transformation.fincl_ev_type_cd       AS fincl_ev_type_cd,
         exp_data_transformation.funds_tfr_eff_dt       AS funds_tfr_eff_dt,
         exp_data_transformation.funds_tfr_ref_num      AS funds_tfr_ref_num,
         exp_data_transformation.ar_invc_id             AS ar_invc_id,
         exp_data_transformation.ar_invc_ln_num         AS ar_invc_ln_num,
         exp_data_transformation.ar_invc_ln_amt_type_cd AS ar_invc_ln_amt_type_cd,
         exp_data_transformation.func_cd                AS func_cd,
         exp_data_transformation.out_prcs_id            AS prcs_id,
         exp_data_transformation.funds_tfr_meth_type_cd AS funds_tfr_meth_type_cd,
         exp_data_transformation.glmonth                AS glmonth,
         exp_data_transformation.glyear                 AS glyear,
         exp_data_transformation.accounting_day         AS accounting_day,
         exp_data_transformation.accounting_month       AS accounting_month,
         exp_data_transformation.accounting_year        AS accounting_year,
         exp_data_transformation.createtime             AS fincl_ev_prd_strt_dt,
         exp_data_transformation.fincl_ev_prd_end_dt    AS fincl_ev_prd_end_dt,
         exp_data_transformation.retired                AS retired,
         exp_data_transformation.trans_host_num         AS trans_host_num,
         exp_data_transformation.ev_med_type_cd         AS ev_med_type_cd,
         exp_data_transformation.funds_tfr_type_cd      AS funds_tfr_type_cd,
         exp_data_transformation.rnk                    AS rnk,
         exp_data_transformation.edw_start_dttm         AS edw_start_dttm,
         exp_data_transformation.edw_end_dttm           AS edw_end_dttm,
         exp_data_transformation.tgt_edw_strt_dttm      AS tgt_edw_strt_dttm,
         exp_data_transformation.tgt_edw_end_dttm       AS tgt_edw_end_dttm,
         exp_data_transformation.ins_upd_flag           AS ins_upd_flag,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.ins_upd_flag = ''R''
  AND    exp_data_transformation.retired != 0
  AND    exp_data_transformation.tgt_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    exp_data_transformation.ev_id IS NOT NULL /*- - > NOT
  INSERT
  OR
  UPDATE ,
         no CHANGE IN VALUES - - > but data IS retired - - >
  UPDATE these records WITH current_timestamp*/
  ;
  
  -- Component upd_fincl_ev_upd_Retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_fincl_ev_upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_insupd_retired.ev_id             AS fincl_ev_id,
                NULL                                          AS prcs_id,
                rtr_fincl_ev_insupd_retired.tgt_edw_strt_dttm AS tgt_edw_strt_dttm,
                1                                     AS update_strategy_action,
				rtr_fincl_ev_insupd_retired.source_record_id
         FROM   rtr_fincl_ev_insupd_retired );
  -- Component upd_fincl_ev_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_fincl_ev_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_insupd_grp_insert.ev_id                  AS ev_id,
                rtr_fincl_ev_insupd_grp_insert.fincl_ev_type_cd       AS fincl_ev_type_cd,
                rtr_fincl_ev_insupd_grp_insert.funds_tfr_eff_dt       AS funds_tfr_eff_dt,
                rtr_fincl_ev_insupd_grp_insert.funds_tfr_ref_num      AS funds_tfr_ref_num,
                rtr_fincl_ev_insupd_grp_insert.func_cd                AS func_cd,
                rtr_fincl_ev_insupd_grp_insert.prcs_id                AS prcs_id,
                rtr_fincl_ev_insupd_grp_insert.ar_invc_ln_amt_type_cd AS ar_invc_ln_amt_type_cd,
                rtr_fincl_ev_insupd_grp_insert.ar_invc_id             AS ar_invc_id,
                rtr_fincl_ev_insupd_grp_insert.ar_invc_ln_num         AS ar_invc_ln_num,
                rtr_fincl_ev_insupd_grp_insert.funds_tfr_meth_type_cd AS funds_tfr_meth_type_cd,
                rtr_fincl_ev_insupd_grp_insert.edw_start_dttm         AS edw_start_dttm2,
                rtr_fincl_ev_insupd_grp_insert.edw_end_dttm           AS edw_end_dttm2,
                rtr_fincl_ev_insupd_grp_insert.glmonth                AS glmonth1,
                rtr_fincl_ev_insupd_grp_insert.glyear                 AS glyear1,
                rtr_fincl_ev_insupd_grp_insert.accounting_day         AS accounting_day1,
                rtr_fincl_ev_insupd_grp_insert.accounting_month       AS accounting_month1,
                rtr_fincl_ev_insupd_grp_insert.accounting_year        AS accounting_year1,
                rtr_fincl_ev_insupd_grp_insert.fincl_ev_prd_strt_dt   AS fincl_ev_prd_strt_dt1,
                rtr_fincl_ev_insupd_grp_insert.fincl_ev_prd_end_dt    AS fincl_ev_prd_end_dt1,
                rtr_fincl_ev_insupd_grp_insert.retired                AS retired1,
                rtr_fincl_ev_insupd_grp_insert.trans_host_num         AS trans_host_num1,
                rtr_fincl_ev_insupd_grp_insert.rnk                    AS rnk1,
                rtr_fincl_ev_insupd_grp_insert.ev_med_type_cd         AS ev_med_type_cd1,
                rtr_fincl_ev_insupd_grp_insert.funds_tfr_type_cd      AS funds_tfr_type_cd1,
                0                                                     AS update_strategy_action,
				source_record_id
         FROM   rtr_fincl_ev_insupd_grp_insert );
  -- Component exp_pass_to_target_upd_Retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd_retired AS
  (
         SELECT upd_fincl_ev_upd_retired.fincl_ev_id       AS fincl_ev_id,
                current_timestamp                          AS o_edw_end_dttm,
                upd_fincl_ev_upd_retired.tgt_edw_strt_dttm AS lkp_edw_strt_dttm3,
                upd_fincl_ev_upd_retired.source_record_id
         FROM   upd_fincl_ev_upd_retired );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_fincl_ev_ins.ev_id                  AS ev_id,
                upd_fincl_ev_ins.fincl_ev_type_cd       AS fincl_ev_type_cd,
                upd_fincl_ev_ins.funds_tfr_eff_dt       AS funds_tfr_eff_dt,
                upd_fincl_ev_ins.funds_tfr_ref_num      AS funds_tfr_ref_num,
                upd_fincl_ev_ins.prcs_id                AS prcs_id,
                :P_DEFAULT_STR_CD                       AS bnk_tfr_ev_type_cd,
                upd_fincl_ev_ins.ar_invc_ln_amt_type_cd AS ar_invc_ln_amt_type_cd,
                upd_fincl_ev_ins.ar_invc_id             AS ar_invc_id,
                upd_fincl_ev_ins.ar_invc_ln_num         AS ar_invc_ln_num,
                upd_fincl_ev_ins.funds_tfr_meth_type_cd AS funds_tfr_meth_type_cd1,
                upd_fincl_ev_ins.glmonth1               AS glmonth1,
                upd_fincl_ev_ins.glyear1                AS glyear1,
                upd_fincl_ev_ins.accounting_day1        AS accounting_day1,
                upd_fincl_ev_ins.accounting_month1      AS accounting_month1,
                upd_fincl_ev_ins.accounting_year1       AS accounting_year1,
                upd_fincl_ev_ins.fincl_ev_prd_strt_dt1  AS fincl_ev_prd_strt_dt1,
                upd_fincl_ev_ins.fincl_ev_prd_end_dt1   AS fincl_ev_prd_end_dt1,
                CASE
                       WHEN upd_fincl_ev_ins.retired1 = 0 THEN upd_fincl_ev_ins.edw_end_dttm2
                       ELSE current_timestamp
                END                                                                     AS o_edw_end_dttm,
                upd_fincl_ev_ins.trans_host_num1                                        AS trans_host_num1,
                dateadd(''second'', ( 2 * ( upd_fincl_ev_ins.rnk1 - 1 ) ), current_timestamp) AS edw_strt_dttm,
                upd_fincl_ev_ins.source_record_id
         FROM   upd_fincl_ev_ins );
  -- Component tgt_fincl_ev_upd_Retired, Type TARGET
  merge
  INTO         db_t_prod_core.fincl_ev
  USING        exp_pass_to_target_upd_retired
  ON (
                            fincl_ev.ev_id = exp_pass_to_target_upd_retired.fincl_ev_id
               AND          fincl_ev.edw_strt_dttm = exp_pass_to_target_upd_retired.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_pass_to_target_upd_retired.fincl_ev_id,
         edw_strt_dttm = exp_pass_to_target_upd_retired.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd_retired.o_edw_end_dttm;
  
  -- Component tgt_fincl_ev_upd_Retired, Type Post SQL
  UPDATE db_t_prod_core.fincl_ev
    SET    edw_strt_dttm =edw_strt_dttm_new,
         edw_end_dttm =''9999-12-31 23:59:59.999999''
  FROM   (
                         SELECT DISTINCT ev_id,
                                         edw_strt_dttm,
                                         fincl_ev_prd_strt_dttm,
                                         max(edw_strt_dttm ) over(PARTITION BY ev_id ORDER BY ar_invc_id DESC ROWS BETWEEN 1 following AND             1 following) + interval ''1 second'' AS edw_strt_dttm_new,
                                         ar_invc_ln_num,
                                         ar_invc_id
                         FROM            db_t_prod_core.fincl_ev
                         WHERE           (
                                                         ev_id,edw_strt_dttm, fincl_ev_prd_strt_dttm) IN
                                                                                                          (
                                                                                                          SELECT DISTINCT ev_id,
                                                                                                                          edw_strt_dttm,
                                                                                                                          fincl_ev_prd_strt_dttm
                                                                                                          FROM            db_t_prod_core.fincl_ev
                                                                                                          GROUP BY        1,2,3
                                                                                                          HAVING          count(*)>1))tgt

  WHERE  fincl_ev.ev_id=tgt.ev_id
  AND    fincl_ev.edw_strt_dttm =tgt.edw_strt_dttm
  AND    fincl_ev.ar_invc_ln_num=tgt.ar_invc_ln_num
  AND    fincl_ev.ar_invc_id=tgt.ar_invc_id
  AND    edw_strt_dttm_new IS NOT NULL;
  
  ;
  UPDATE db_t_prod_core.fincl_ev
  SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_id,
                                         edw_strt_dttm,
                                         fincl_ev_prd_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ev_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.fincl_ev ) a

  WHERE  fincl_ev.edw_strt_dttm = a.edw_strt_dttm
  AND    fincl_ev.ev_id=a.ev_id
  AND    lead1 IS NOT NULL;
  
  /**new POST sql**/
  UPDATE db_t_prod_core.fincl_ev
  SET    ar_invc_ln_num =updt.src_ar_invc_ln_num,
         ar_invc_id=updt.src_ar_invc_id
  FROM   (
                         SELECT          ev_act_type_code,
                                         key1,
                                         ''FINANCL''   AS SUBTYPE,
                                         ''FNDTRNSFR'' AS financl_ev_type,
                                         refnumber,
                                         inv_invoicenumber,
                                         invitem_id,
                                         coalesce(stg.src_eff_dt, cast(''1900-01-01'' AS DATE )) AS src_eff_dt,
                                         src_fincl_ev_prd_strt_dt ,
                                         evt.ev_id            AS src_ev_id,
                                         ar_il.ar_invc_ln_num AS src_ar_invc_ln_num,
                                         doc_id               AS src_ar_invc_id
                         FROM            (
                                                    SELECT     key1,
                                                               ev_act_type_code,
                                                               SUBTYPE,
                                                               src_eff_dt,
                                                               refnumber,
                                                               bc_trans_updatetime,
                                                               src_fincl_ev_prd_strt_dt,
                                                               k.invoicenumber_stg AS inv_invoicenumber ,
                                                               i.id_stg            AS invitem_id ,
                                                               k.updatetime_stg    AS invoice_updatetime,
                                                               i.updatetime_stg    AS invoiceitem_updatetime
                                                    FROM       (
                                                                               SELECT DISTINCT cast(bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                               cast(bc_transaction.id_stg AS         INTEGER)     AS key1 ,
                                                                                               ''EV_SBTYPE2''                                       AS SUBTYPE ,
                                                                                               ''FINCL_EV_TYPE3''                                   AS financl_ev_type ,
                                                                                               bc_transaction.transactiondate_stg                 AS src_eff_dt ,
                                                                                               bc_transaction.createtime_stg                      AS src_fincl_ev_prd_strt_dt,
                                                                                               bc_transaction.transactionnumber_stg               AS refnumber ,
                                                                                               bc_transaction.updatetime_stg                         bc_trans_updatetime
                                                                               FROM            db_t_prod_stag.bc_transaction bc_transaction
                                                                               inner join      db_t_prod_stag.bctl_transaction bctl_transaction
                                                                               ON              bctl_transaction.id_stg =bc_transaction.subtype_stg
                                                                               left outer join db_t_prod_stag.bc_itemevent bc_itemevent
                                                                               ON              bc_itemevent.transactionid_stg = bc_transaction.id_stg
                                                                               WHERE           bc_itemevent.transactionid_stg IS NULL
                                                                               AND             (
                                                                                                               bc_transaction.updatetime_stg > (:START_DTTM)
                                                                                               AND             bc_transaction.updatetime_stg <= (:END_DTTM)) )b
                                                    join       db_t_prod_stag.bc_chargeinstancecontext bc_chargeinstancecontext
                                                    ON         bc_chargeinstancecontext.transactionid_stg =key1
                                                    join       db_t_prod_stag.bc_charge e
                                                    ON         e.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                    join       db_t_prod_stag.bc_chargepattern h
                                                    ON         h.id_stg = e.chargepatternid_stg
                                                    join       db_t_prod_stag.bctl_chargecategory f
                                                    ON         f.id_stg = h.category_stg
                                                    join       db_t_prod_stag.bc_invoiceitem i
                                                    ON         i.chargeid_stg = e.id_stg
                                                    inner join db_t_prod_stag.bctl_invoiceitemtype j
                                                    ON         j.id_stg=i.type_stg
                                                    join       db_t_prod_stag.bc_invoice k
                                                    ON         k.id_stg = i.invoiceid_stg qualify row_number() over(PARTITION BY ev_act_type_code,key1,src_eff_dt,refnumber ORDER BY bc_trans_updatetime DESC,k.updatetime_stg DESC, i.updatetime_stg DESC)=1
                                                    UNION
                                                    SELECT     key1,
                                                               ev_act_type_code,
                                                               SUBTYPE,
                                                               src_eff_dt,
                                                               refnumber,
                                                               bc_trans_updatetime,
                                                               src_fincl_ev_prd_strt_dt,
                                                               k.invoicenumber_stg AS inv_invoicenumber ,
                                                               i.id_stg            AS invitem_id ,
                                                               k.updatetime_stg    AS invoice_updatetime,
                                                               i.updatetime_stg    AS invoiceitem_updatetime
                                                    FROM       (
                                                                               SELECT DISTINCT cast(''rvrs''
                                                                                                               || ''-''
                                                                                                               || bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                               cast(bc_transaction.id_stg AS                       INTEGER)     AS key1 ,
                                                                                               ''EV_SBTYPE2''                                                     AS SUBTYPE ,
                                                                                               ''FINCL_EV_TYPE3''                                                 AS financl_ev_type ,
                                                                                               bc_transaction.transactiondate_stg                               AS src_eff_dt ,
                                                                                               bc_transaction.createtime_stg                                    AS src_fincl_ev_prd_strt_dt,
                                                                                               bc_transaction.transactionnumber_stg                             AS refnumber ,
                                                                                               bc_transaction.updatetime_stg                                       bc_trans_updatetime
                                                                               FROM            db_t_prod_stag.bc_transaction bc_transaction
                                                                               inner join      db_t_prod_stag.bctl_transaction bctl_transaction
                                                                               ON              bctl_transaction.id_stg =bc_transaction.subtype_stg
                                                                               left outer join db_t_prod_stag.bc_itemevent bc_itemevent
                                                                               ON              bc_itemevent.transactionid_stg = bc_transaction.id_stg
                                                                               left join       db_t_prod_stag.bc_revtrans bc_revtrans
                                                                               ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                               WHERE           bc_itemevent.transactionid_stg IS NULL
                                                                               AND             bc_revtrans.ownerid_stg IS NOT NULL
                                                                               AND             (
                                                                                                               bc_transaction.updatetime_stg > (:START_DTTM)
                                                                                               AND             bc_transaction.updatetime_stg <= (:END_DTTM)) )b
                                                    join       db_t_prod_stag.bc_chargeinstancecontext bc_chargeinstancecontext
                                                    ON         bc_chargeinstancecontext.transactionid_stg =key1
                                                    join       db_t_prod_stag.bc_charge e
                                                    ON         e.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                    join       db_t_prod_stag.bc_chargepattern h
                                                    ON         h.id_stg = e.chargepatternid_stg
                                                    join       db_t_prod_stag.bctl_chargecategory f
                                                    ON         f.id_stg = h.category_stg
                                                    join       db_t_prod_stag.bc_invoiceitem i
                                                    ON         i.chargeid_stg = e.id_stg
                                                    inner join db_t_prod_stag.bctl_invoiceitemtype j
                                                    ON         j.id_stg=i.type_stg
                                                    join       db_t_prod_stag.bc_invoice k
                                                    ON         k.id_stg = i.invoiceid_stg qualify row_number() over(PARTITION BY ev_act_type_code,key1,src_eff_dt,refnumber ORDER BY bc_trans_updatetime DESC,k.updatetime_stg DESC, i.updatetime_stg DESC)=1 )stg
                         left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_act_type_cd
                         ON              xlat_act_type_cd.src_idntftn_val = stg.ev_act_type_code
                         AND             xlat_act_type_cd.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                         AND             xlat_act_type_cd.src_idntftn_sys IN (''GW'',
                                                                              ''DS'' )
                         AND             xlat_act_type_cd.expn_dt=''9999-12-31''
                         left outer join
                                         (
                                                  SELECT   ev_id,
                                                           src_trans_id,
                                                           ev_sbtype_cd,
                                                           ev_actvy_type_cd
                                                  FROM     db_t_prod_core.ev qualify row_number() over( PARTITION BY ev_sbtype_cd,ev_actvy_type_cd,src_trans_id ORDER BY edw_end_dttm DESC) = 1 ) AS evt
                         ON              evt.src_trans_id = stg.key1
                         AND             evt.ev_actvy_type_cd = xlat_act_type_cd.tgt_idntftn_val
                         AND             evt.ev_sbtype_cd =''FINANCL''
                         left outer join
                                         (
                                                  SELECT   ar_invc_ln_num,
                                                           ar_invc_id,
                                                           host_invc_ln_num
                                                  FROM     db_t_prod_core.ar_invc_ln qualify row_number() over( PARTITION BY host_invc_ln_num ORDER BY edw_end_dttm DESC) = 1 ) AS ar_il
                         ON              cast(ar_il.host_invc_ln_num AS VARCHAR(20)) = cast(stg.invitem_id AS VARCHAR(20))
                         left outer join
                                         (
                                                  SELECT   doc_id,
                                                           doc_issur_num,
                                                           doc_type_cd,
                                                           doc_ctgy_type_cd
                                                  FROM     db_t_prod_core.doc qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS dc
                         ON              dc.doc_issur_num = stg.inv_invoicenumber
                         AND             dc.doc_type_cd = ''INVOICE''
                         AND             dc.doc_ctgy_type_cd = ''BILL'' )updt

  WHERE  ev_id=updt.src_ev_id
  AND    fincl_ev_prd_strt_dttm=updt.src_fincl_ev_prd_strt_dt
  AND    funds_tfr_eff_dttm=updt.src_eff_dt
  AND    funds_tfr_ref_num=updt.refnumber
  AND    ar_invc_ln_num IS NULL
  AND    cast(edw_end_dttm AS DATE)=''9999-12-31'';
  
  -- Component tgt_fincl_ev_insert, Type TARGET
  INSERT INTO db_t_prod_core.fincl_ev
              (
                          ev_id,
                          fincl_ev_prd_strt_dttm,
                          fincl_ev_prd_end_dttm,
                          fincl_ev_type_cd,
                          funds_tfr_host_num,
                          funds_tfr_eff_dttm,
                          funds_tfr_ref_num,
                          funds_tfr_meth_type_cd,
                          bnk_tfr_ev_type_cd,
                          ar_invc_id,
                          ar_invc_ln_num,
                          ar_invc_ln_amt_type_cd,
                          prcs_id,
                          gl_mth_num,
                          gl_yr_num,
                          accntg_dy_num,
                          accntg_mth_num,
                          accntg_yr_num,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_ins.ev_id                   AS ev_id,
         exp_pass_to_target_ins.fincl_ev_prd_strt_dt1   AS fincl_ev_prd_strt_dttm,
         exp_pass_to_target_ins.fincl_ev_prd_end_dt1    AS fincl_ev_prd_end_dttm,
         exp_pass_to_target_ins.fincl_ev_type_cd        AS fincl_ev_type_cd,
         exp_pass_to_target_ins.trans_host_num1         AS funds_tfr_host_num,
         exp_pass_to_target_ins.funds_tfr_eff_dt        AS funds_tfr_eff_dttm,
         exp_pass_to_target_ins.funds_tfr_ref_num       AS funds_tfr_ref_num,
         exp_pass_to_target_ins.funds_tfr_meth_type_cd1 AS funds_tfr_meth_type_cd,
         exp_pass_to_target_ins.bnk_tfr_ev_type_cd      AS bnk_tfr_ev_type_cd,
         exp_pass_to_target_ins.ar_invc_id              AS ar_invc_id,
         exp_pass_to_target_ins.ar_invc_ln_num          AS ar_invc_ln_num,
         exp_pass_to_target_ins.ar_invc_ln_amt_type_cd  AS ar_invc_ln_amt_type_cd,
         exp_pass_to_target_ins.prcs_id                 AS prcs_id,
         exp_pass_to_target_ins.glmonth1                AS gl_mth_num,
         exp_pass_to_target_ins.glyear1                 AS gl_yr_num,
         exp_pass_to_target_ins.accounting_day1         AS accntg_dy_num,
         exp_pass_to_target_ins.accounting_month1       AS accntg_mth_num,
         exp_pass_to_target_ins.accounting_year1        AS accntg_yr_num,
         exp_pass_to_target_ins.edw_strt_dttm           AS edw_strt_dttm,
         exp_pass_to_target_ins.o_edw_end_dttm          AS edw_end_dttm
  FROM   exp_pass_to_target_ins;

END;
';