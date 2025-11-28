-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINCL_EV_AMT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1)::STRING;
  start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1)::STRING;
  end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1)::STRING;
  prcs_id := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''prcs_id'' LIMIT 1)::STRING;
BEGIN


-- PIPELINE START FOR 1
  -- Component sq_bc_invoice, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ev_id,
                $2  AS fincl_ev_amt_type_code,
                $3  AS busn_dt,
                $4  AS amount,
                $5  AS amt_ctgy_type_code,
                $6  AS retired,
                $7  AS rnk,
                $8  AS validquote,
                $9  AS agmt_id,
                $10 AS quotn_id,
                $11 AS taccount_typecode,
                $12 AS eff_dt,
                $13 AS tgt_ev_id_p,
                $14 AS tgt_fincl_ev_amt_type_cd_p,
                $15 AS tgt_fincl_ev_amt_dttm_p,
                $16 AS tgt_ev_trans_amt_p,
                $17 AS tgt_edw_strt_dttm_p,
                $18 AS tgt_edw_end_dttm_p,
                $19 AS ev_id_dup_chk,
                $20 AS var_calc_chksm,
                $21 AS var_orig_chksm,
                $22 AS edw_strt_dttm,
                $23 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH intrm_fincl_ev_amt AS
                                  (
                                                  SELECT DISTINCT ev_act_type_code,
                                                                  key1,
                                                                  SUBTYPE,
                                                                  busn_dt,
                                                                  eff_dt,
                                                                  ev_amt_type_code,
                                                                  amt,
                                                                  ev_strt_dt,
                                                                  ev_end_dt,
                                                                  retired,
                                                                  validquote,
                                                                  amt_ctgy_typecode,
                                                                  agmt_host_id,
                                                                  agmt_type,
                                                                  src_sys,
                                                                  nk_job_nbr,
                                                                  vers_nbr,
                                                                  taccount_typecode,
                                                                  termnumber ,
                                                                  rank() over(PARTITION BY ev_act_type_code,key1,SUBTYPE,ev_amt_type_code ORDER BY eff_dt,validquote,amt,taccount_typecode,amt_ctgy_typecode ASC, agmt_host_id DESC ) AS rnk
                                                  FROM            (
                                                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE14'' AS           VARCHAR(50)) ev_act_type_code ,
                                                                                                  cast(bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  cast(''EV_SBTYPE2'' AS                VARCHAR(50)) AS SUBTYPE ,
                                                                                                  receiveddate_stg                                 AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basemoneyreceived.updatetime_stg       AS eff_dt ,
                                                                                                  cast(''FINCL_EV_AMT_TYPE1'' AS VARCHAR(60)) AS ev_amt_type_code ,
                                                                                                  bc_basemoneyreceived.amount_stg           AS amt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_end_dt  ,
                                                                                                  bc_basemoneyreceived.retired_stg          AS retired ,
                                                                                                  1                                         AS validquote ,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM   db_t_prod_stag.bctl_basemoneyreceived)bctl_basemoneyreceived
                                                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM   db_t_prod_stag.bc_unappliedfund)bc_unappliedfund
                                                                                  ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                unappliedfundid_stg
                                                                                                         FROM   db_t_prod_stag.bc_invoicestream)bc_invoicestream
                                                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT accountnumber_stg,
                                                                                                                id_stg
                                                                                                         FROM   db_t_prod_stag.bc_account)bc_account
                                                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                                                          ''ZeroDollarDMR'',
                                                                                                                                          ''ZeroDollarReversal'')
                                                                                  AND             bc_basemoneyreceived.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basemoneyreceived.updatetime_stg <= (:end_dttm)
                                                                                  /* -25145 */
                                                                                  UNION
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE25'' ev_act_type_code ,
                                                                                                  cast(bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                     AS SUBTYPE ,
                                                                                                  reversaldate_stg                                 AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basemoneyreceived.updatetime_stg       AS eff_dt ,
                                                                                                  cast(''FINCL_EV_AMT_TYPE2'' AS VARCHAR(60)) AS ev_amt_type_code ,
                                                                                                  bc_basemoneyreceived.amount_stg           AS amt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_end_dt  ,
                                                                                                  bc_basemoneyreceived.retired_stg          AS retired ,
                                                                                                  1                                         AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.bctl_basemoneyreceived) bctl_basemoneyreceived
                                                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_unappliedfund) bc_unappliedfund
                                                                                  ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                unappliedfundid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoicestream) bc_invoicestream
                                                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT accountnumber_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                                                          ''ZeroDollarDMR'',
                                                                                                                                          ''ZeroDollarReversal'')
                                                                                  AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                                                  AND             bc_basemoneyreceived.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basemoneyreceived.updatetime_stg <= (:end_dttm)
                                                                                  /* -54 */
                                                                                  /***************************bc_basemoneyreceived****************************/
                                                                                  UNION
                                                                                  /***************************bc_basedistitem****************************/
                                                                                  /* -----sq3----- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE26'' ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  bc_basedistitem.executeddate_stg            AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basedistitem.updatetime_stg         AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE3''                   AS ev_amt_type_code ,
                                                                                                  bc_basedistitem.grossamounttoapply_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_end_dt ,
                                                                                                  bc_basedistitem.retired_stg            AS retired ,
                                                                                                  1                                      AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))              AS amt_ctgy_typecode ,
                                                                                                  cast('''' AS   VARCHAR(64))              AS agmt_host_id ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS termnumber ,
                                                                                                  cast('''' AS   VARCHAR(60))              AS agmt_type ,
                                                                                                  ''SRC_SYS5''                             AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))              AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                type_stg,
                                                                                                                chargeid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_invoiceitemtype) bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                chargepatternid_stg
                                                                                                         FROM db_t_prod_stag.bc_charge) bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                category_stg
                                                                                                         FROM db_t_prod_stag.bc_chargepattern) bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_chargecategory) bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE26'' ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  bc_basedistitem.executeddate_stg            AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basedistitem.updatetime_stg         AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE3''                   AS ev_amt_type_code ,
                                                                                                  bc_basedistitem.grossamounttoapply_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_end_dt ,
                                                                                                  bc_basedistitem.retired_stg            AS retired ,
                                                                                                  1                                      AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))              AS amt_ctgy_typecode ,
                                                                                                  cast('''' AS   VARCHAR(64))              AS agmt_host_id ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS termnumber ,
                                                                                                  cast('''' AS   VARCHAR(60))              AS agmt_type ,
                                                                                                  ''SRC_SYS5''                             AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))              AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                type_stg,
                                                                                                                chargeid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_invoiceitemtype) bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                chargepatternid_stg
                                                                                                         FROM db_t_prod_stag.bc_charge) bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                category_stg
                                                                                                         FROM db_t_prod_stag.bc_chargepattern) bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_chargecategory) bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:end_dttm)
                                                                                  AND             bc_basedistitem.reverseddistid_stg IS NOT NULL
                                                                                  /* -----sq3----- */
                                                                                  UNION
                                                                                  /* ---sq4---- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE27'' ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  bc_basedistitem.reverseddate_stg            AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basedistitem.updatetime_stg         AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1''                   AS ev_amt_type_code ,
                                                                                                  bc_basedistitem.grossamounttoapply_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_end_dt  ,
                                                                                                  bc_basedistitem.retired_stg            AS retired ,
                                                                                                  1                                      AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))              AS amt_ctgy_typecode ,
                                                                                                  cast('''' AS   VARCHAR(64))              AS agmt_host_id ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS termnumber ,
                                                                                                  cast('''' AS   VARCHAR(60))              AS agmt_type ,
                                                                                                  ''SRC_SYS5''                             AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))              AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                type_stg,
                                                                                                                chargeid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_invoiceitemtype) bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                chargepatternid_stg
                                                                                                         FROM db_t_prod_stag.bc_charge) bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                category_stg
                                                                                                         FROM db_t_prod_stag.bc_chargepattern) bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_chargecategory) bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:end_dttm)
                                                                                  AND             bc_basedistitem.reverseddate_stg IS NOT NULL
                                                                                  UNION
                                                                                  /* --check if needed----- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE27'' ev_act_type_code ,
                                                                                                  cast(bc_basedistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                  bc_basedistitem.reverseddate_stg            AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basedistitem.updatetime_stg         AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1''                   AS ev_amt_type_code ,
                                                                                                  bc_basedistitem.grossamounttoapply_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_end_dt  ,
                                                                                                  bc_basedistitem.retired_stg            AS retired ,
                                                                                                  1                                      AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))              AS amt_ctgy_typecode ,
                                                                                                  cast('''' AS   VARCHAR(64))              AS agmt_host_id ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS termnumber ,
                                                                                                  cast('''' AS   VARCHAR(60))              AS agmt_type ,
                                                                                                  ''SRC_SYS5''                             AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))              AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))              AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                type_stg,
                                                                                                                chargeid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_invoiceitemtype) bctl_invoiceitemtype
                                                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                chargepatternid_stg
                                                                                                         FROM db_t_prod_stag.bc_charge) bc_charge
                                                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                category_stg
                                                                                                         FROM db_t_prod_stag.bc_chargepattern) bc_chargepattern
                                                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_chargecategory) bctl_chargecategory
                                                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                  WHERE           bc_basedistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basedistitem.updatetime_stg <= (:end_dttm)
                                                                                  AND             reverseddate_stg IS NOT NULL
                                                                                  /* --check if needed---- */
                                                                                  /* --sq4------ */
                                                                                  /***************************bc_basedistitem****************************/
                                                                                  UNION
                                                                                  /***************************bc_basenonrecdistitem****************************/
                                                                                  /* --sq5----- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE28'' ev_act_type_code ,
                                                                                                  cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                                                  executeddate_stg                                  AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basenonrecdistitem.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1''                 AS ev_amt_type_code ,
                                                                                                  grossamounttoapply_stg               AS amt ,
                                                                                                  cast(NULL AS timestamp)              AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)              AS ev_end_dt ,
                                                                                                  bc_basenonrecdistitem.retired_stg    AS retired,
                                                                                                  1                                    AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))            AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basenonrecdistitem
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_basedist) bc_basedist
                                                                                  ON              bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT basedistid_stg,
                                                                                                                unappliedfundid_stg,
                                                                                                                accountid_stg
                                                                                                         FROM db_t_prod_stag.bc_basemoneyreceived) bc_basemoneyreceived
                                                                                  ON              bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_unappliedfund) bc_unappliedfund
                                                                                  ON              bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                unappliedfundid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoicestream) bc_invoicestream
                                                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT accountnumber_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                  WHERE           bc_basenonrecdistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basenonrecdistitem.updatetime_stg <= (:end_dttm)
                                                                                  /* ---sq5 end---- */
                                                                                  /* --sq6 start--- */
                                                                                  UNION
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE29'' ev_act_type_code ,
                                                                                                  cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                                                  reverseddate_stg                                  AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_basenonrecdistitem.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1''                 AS ev_amt_type_code ,
                                                                                                  grossamounttoapply_stg               AS amt ,
                                                                                                  cast(NULL AS timestamp)              AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)              AS ev_end_dt ,
                                                                                                  bc_basenonrecdistitem.retired_stg    AS retired,
                                                                                                  1                                    AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))            AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_basenonrecdistitem
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_basedist) bc_basedist
                                                                                  ON              bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT basedistid_stg,
                                                                                                                unappliedfundid_stg,
                                                                                                                accountid_stg
                                                                                                         FROM db_t_prod_stag.bc_basemoneyreceived) bc_basemoneyreceived
                                                                                  ON              bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_unappliedfund) bc_unappliedfund
                                                                                  ON              bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                unappliedfundid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoicestream) bc_invoicestream
                                                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT accountnumber_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                  WHERE           bc_basenonrecdistitem.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_basenonrecdistitem.updatetime_stg <= (:end_dttm)
                                                                                  AND             bc_basenonrecdistitem.reverseddate_stg IS NOT NULL
                                                                                  /* ---sq6 end--- */
                                                                                  /***************************bc_basenonrecdistitem****************************/
                                                                                  UNION
                                                                                  /* --sq7 start--- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE30''                              AS ev_act_type_code ,
                                                                                                  cast(bc_suspensepayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                  bc_suspensepayment.paymentdate_stg             AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_suspensepayment.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1''              AS ev_amt_type_code ,
                                                                                                  amount_stg                        AS amt ,
                                                                                                  cast(NULL AS timestamp)           AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)           AS ev_end_dt ,
                                                                                                  bc_suspensepayment.retired_stg    AS retired,
                                                                                                  1                                 AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))         AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_suspensepayment.policynumber_stg IS NOT NULL THEN bc_suspensepayment.policynumber_stg
                                                                                                                  WHEN bc_suspensepayment.billingreferencenumber_alfa_stg IS NOT NULL THEN bc_suspensepayment.billingreferencenumber_alfa_stg
                                                                                                                  WHEN bc_suspensepayment.accountnumber_stg IS NOT NULL THEN bc_suspensepayment.accountnumber_stg
                                                                                                                  ELSE NULL
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS VARCHAR(60)) AS termnumber ,
                                                                                                  cast(
                                                                                                  CASE
                                                                                                                  WHEN bc_suspensepayment.policynumber_stg IS NOT NULL THEN ''POL''
                                                                                                                  WHEN bc_suspensepayment.billingreferencenumber_alfa_stg IS NOT NULL THEN ''INV''
                                                                                                                  WHEN bc_suspensepayment.accountnumber_stg IS NOT NULL THEN ''ACT''
                                                                                                                  ELSE ''UNK''
                                                                                                  END AS VARCHAR(60))       AS agmt_type ,
                                                                                                  ''SRC_SYS5''                AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_suspensepayment
                                                                                  WHERE           bc_suspensepayment.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_suspensepayment.updatetime_stg <= (:end_dttm)
                                                                                  /* --sq7 end---- */
                                                                                  UNION
                                                                                  /* --sq8 start---- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE31''              AS ev_act_type_code ,
                                                                                                  cast( a.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                   AS SUBTYPE ,
                                                                                                  a.issuedate_stg                AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  a.updatetime_stg          AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE4''      AS ev_amt_type_code ,
                                                                                                  a.amount_stg              AS amt ,
                                                                                                  cast(NULL AS timestamp)   AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)   AS ev_end_dt ,
                                                                                                  a.retired_stg             AS retired,
                                                                                                  1                         AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50)) AS amt_ctgy_typecode
                                                                                                  /*,CASE WHEN bc_invoicestream.BillingReferenceNumber_Alfa_stg IS NULL THEN acc.AccountNumber_stg ELSE bc_invoicestream.BillingReferenceNumber_Alfa_stg end AS agmt_host_id*/
                                                                                                  ,
                                                                                                  bc_policyperiod.policynumber_stg                    AS agmt_host_id ,
                                                                                                  cast(bc_policyperiod.termnumber_stg AS VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''POLTRM'' AS                       VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                                          AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))                           AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))                           AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))                           AS taccount_typecode
                                                                                  FROM            (
                                                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                                                  bc_paymentinstrument.paymentmethod_stg AS paymentmethod,
                                                                                                                                  bctl_paymentmethod.typecode_stg        AS fund_trnsfr_mthd_typ
                                                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                  ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                  ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg) a,
                                                                                                  (
                                                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                                                  bc_disbursement.status_stg AS bcdisbursementstatus
                                                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                  left outer join db_t_prod_stag.bc_disbursement
                                                                                                                  ON              bc_outgoingpayment.disbursementid_stg = bc_disbursement.id_stg) b
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                unappliedfundid_stg,
                                                                                                                accountid_stg,
                                                                                                                policyperiod_alfa_stg
                                                                                                         FROM db_t_prod_stag.bc_disbursement) bc_disbursement
                                                                                  ON              bc_disbursement.id_stg=b.disbursementid_stg
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_policyperiod.id_stg = bc_disbursement.policyperiod_alfa_stg
                                                                                                  /*LEFT JOIN (SELECT id_stg FROM DB_T_PROD_STAG.bc_unappliedfund) bc_unappliedfund ON bc_unappliedfund.id_stg=bc_disbursement.UnappliedFundID_stg
LEFT JOIN (SELECT UnappliedFundID_stg,PolicyID_stg,BillingReferenceNumber_Alfa_stg FROM DB_T_PROD_STAG.bc_invoicestream) bc_invoicestream ON bc_invoicestream.UnappliedFundID_stg=bc_unappliedfund.id_stg
LEFT JOIN (SELECT AccountNumber_stg,id_stg FROM bc_account)acc ON bc_disbursement.AccountID_stg = acc.id_stg*/
                                                                                  WHERE           a.id_stg=b.id_stg
                                                                                  AND             a.updatetime_stg > (:start_dttm)
                                                                                  AND             a.updatetime_stg <= (:end_dttm)
                                                                                  /* ---sq8 end--- */
                                                                                  UNION
                                                                                  /* --sq9 start--- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE32''                            AS ev_act_type_code ,
                                                                                                  cast(bc_disbursement.id_stg AS VARCHAR(60) ) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                 AS SUBTYPE ,
                                                                                                  bc_disbursement.closedate_stg                AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_disbursement.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE4''           AS ev_amt_type_code ,
                                                                                                  amount_stg                     AS amt ,
                                                                                                  cast(NULL AS timestamp)        AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)        AS ev_end_dt ,
                                                                                                  bc_disbursement.retired_stg    AS retired,
                                                                                                  1                              AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))      AS amt_ctgy_typecode
                                                                                                  /* ,CASE WHEN bc_invoicestream.BillingReferenceNumber_Alfa_stg IS NULL THEN bc_account.AccountNumber_stg ELSE bc_invoicestream.BillingReferenceNumber_Alfa_stg end AS agmt_host_id  */
                                                                                                  ,
                                                                                                  bc_policyperiod.policynumber_stg                    AS agmt_host_id ,
                                                                                                  cast(bc_policyperiod.termnumber_stg AS VARCHAR(60)) AS termnumber
                                                                                                  /* ,Cast(NULL AS VARCHAR(60)) AS Termnumber */
                                                                                                  ,
                                                                                                  cast(''POLTRM'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                    AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))     AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))     AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))     AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_disbursement
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_policyperiod.id_stg = bc_disbursement.policyperiod_alfa_stg
                                                                                                  /*LEFT JOIN (SELECT id_stg FROM DB_T_PROD_STAG.bc_unappliedfund) bc_unappliedfund ON bc_unappliedfund.id_stg=bc_disbursement.UnappliedFundID_stg
LEFT JOIN (SELECT BillingReferenceNumber_Alfa_stg,UnappliedFundID_stg FROM DB_T_PROD_STAG.bc_invoicestream) bc_invoicestream ON bc_invoicestream.UnappliedFundID_stg=bc_unappliedfund.id_stg
LEFT JOIN (SELECT AccountNumber_stg,id_stg FROM DB_T_PROD_STAG.bc_account) bc_account ON bc_disbursement.AccountID_stg = bc_account.id_stg*/
                                                                                  WHERE           bc_disbursement.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_disbursement.updatetime_stg <= (:end_dttm)
                                                                                  /* -177 */
                                                                                  /* --sq9 end---- */
                                                                                  UNION
                                                                                  /* --sq10 start--- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE33''                       AS ev_act_type_code ,
                                                                                                  cast(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                                                  bc_writeoff.executiondate_stg           AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_writeoff.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE4''       AS ev_amt_type_code ,
                                                                                                  bc_writeoff.amount_stg     AS amt ,
                                                                                                  cast(NULL AS timestamp)    AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)    AS ev_end_dt ,
                                                                                                  bc_writeoff.retired_stg    AS retired,
                                                                                                  1                          AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))  AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_writeoff
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                chargeid_stg,
                                                                                                                policyperiodid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoicestreamid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoicestream) bc_invoicestream
                                                                                  ON              bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_taccountcontainer) bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT hiddentaccountcontainerid_stg,
                                                                                                                policyid_stg
                                                                                                         FROM   db_t_prod_stag.bc_policyperiod) b
                                                                                  ON              b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                accountid_stg
                                                                                                         FROM db_t_prod_stag.bc_policy) bc_policy
                                                                                  ON              bc_policy.id_stg=b.policyid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                accountnumber_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_account.id_stg=bc_policy.accountid_stg
                                                                                  WHERE           bc_writeoff.id_stg NOT IN
                                                                                                  (
                                                                                                         SELECT ownerid_stg
                                                                                                         FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                  AND             bc_writeoff.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_writeoff.updatetime_stg <= (:end_dttm)
                                                                                  /* --sq 10 end---- */
                                                                                  UNION
                                                                                  /* --sq11 start--- */
                                                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE34'' AS  VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                                                  bc_writeoff.executiondate_stg           AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_writeoff.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE4''       AS ev_amt_type_code ,
                                                                                                  bc_writeoff.amount_stg     AS amt ,
                                                                                                  cast(NULL AS timestamp)    AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)    AS ev_end_dt ,
                                                                                                  bc_writeoff.retired_stg    AS retired,
                                                                                                  1                          AS validquote ,
                                                                                                  cast(NULL AS VARCHAR(50))  AS amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                  END                       AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60)) AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60)) AS agmt_type ,
                                                                                                  ''SRC_SYS5''                 AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))  AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))  AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_writeoff
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoiceid_stg,
                                                                                                                chargeid_stg,
                                                                                                                policyperiodid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoiceitem) bc_invoiceitem
                                                                                  ON              bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                invoicestreamid_stg
                                                                                                         FROM db_t_prod_stag.bc_invoice) bc_invoice
                                                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT billingreferencenumber_alfa_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_invoicestream) bc_invoicestream
                                                                                  ON              bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bc_taccountcontainer) bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT hiddentaccountcontainerid_stg,
                                                                                                                policyid_stg
                                                                                                         FROM   db_t_prod_stag.bc_policyperiod) b
                                                                                  ON              b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                accountid_stg
                                                                                                         FROM db_t_prod_stag.bc_policy) bc_policy
                                                                                  ON              bc_policy.id_stg=b.policyid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                accountnumber_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_account.id_stg=bc_policy.accountid_stg
                                                                                  WHERE           bc_writeoff.id_stg IN
                                                                                                  (
                                                                                                         SELECT ownerid_stg
                                                                                                         FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                  AND             bc_writeoff.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_writeoff.updatetime_stg <= (:end_dttm)
                                                                                  /* --sq11 end--- */
                                                                                  /***************************Policy Transaction****************************/
                                                                                  UNION
                                                                                  /* --sq12 start--- */
                                                                                  SELECT DISTINCT cast(pctl_job.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(pc_job.jobnumber_stg AS  VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE3''                               AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg            AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg   AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE5''             AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.totalcostrpt_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)          AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)          AS ev_end_dt ,
                                                                                                  pc_job.retired_stg               AS retired,
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR (60))AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.totalcostrpt_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -31201 */
                                                                                  /* -sq12 end--- */
                                                                                  UNION
                                                                                  /* --sq13 start--- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg                  AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg         AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE6''                   AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.transactioncostrpt_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                     AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM   db_t_prod_stag.pctl_reasoncode)rejectreason
                                                                                  ON              rejectreason.id_stg = pc_job.rejectreason_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM   db_t_prod_stag.pctl_reasoncode)cancelreason
                                                                                  ON              cancelreason.id_stg = pc_job.cancelreasoncode_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.transactioncostrpt_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -18479/18446 */
                                                                                  /* -sq13 end-- */
                                                                                  UNION
                                                                                  /* -sq 14 start--- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg                    AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg           AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE7''                     AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.totalpremadjrpt_alfa_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                  AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                  AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                       AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.totalpremadjrpt_alfa_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -19872/19844 */
                                                                                  /* --sq 14 end-- */
                                                                                  UNION
                                                                                  /* --sq 15 start-- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg               AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg      AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE8''                AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.totalpremiumrpt_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)             AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)             AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                  AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM   db_t_prod_stag.pctl_reasoncode)rejectreason
                                                                                  ON              rejectreason.id_stg = pc_job.rejectreason_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM   db_t_prod_stag.pctl_reasoncode)cancelreason
                                                                                  ON              cancelreason.id_stg = pc_job.cancelreasoncode_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                nonrenewreason_stg
                                                                                                         FROM db_t_prod_stag.pc_policyterm) pc_policyterm
                                                                                  ON              pc_policyterm.id_stg = pc_policyperiod.policytermid_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pctl_reinstatecode) pctl_reinstatecode
                                                                                  ON              pctl_reinstatecode.id_stg = pc_job.reinstatecode_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pctl_nonrenewalcode) pctl_nonrenewalcode
                                                                                  ON              pctl_nonrenewalcode.id_stg = pc_policyterm.nonrenewreason_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pctl_renewalcode) pctl_renewalcode
                                                                                  ON              pctl_renewalcode.id_stg = pc_job.renewalcode_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg
                                                                                                         FROM db_t_prod_stag.pc_policyline) pc_policyline
                                                                                  ON              pc_policyline.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                contactid_stg
                                                                                                         FROM db_t_prod_stag.pc_user) pc_user
                                                                                  ON              pc_job.createuserid_stg = pc_user.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pc_contact) pc_contact
                                                                                  ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pctl_quotetype) pctl_quotetype
                                                                                  ON              pctl_quotetype.id_stg=pc_job.quotetype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.pctl_riskstatus_alfa) pctl_riskstatus_alfa
                                                                                  ON              pctl_riskstatus_alfa.id_stg=pc_job.risk_alfa_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT ownerid_stg,
                                                                                                                foreignentityid_stg
                                                                                                         FROM db_t_prod_stag.pc_jobpolicyperiod) pc_jobpolicyperiod
                                                                                  ON              pc_job.id_stg = pc_jobpolicyperiod.ownerid_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM  db_t_prod_stag. pc_policyperiod) pcp1
                                                                                  ON              pc_jobpolicyperiod.foreignentityid_stg = pcp1.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.totalpremiumrpt_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -20646/20617 */
                                                                                  /* -sq15 end--- */
                                                                                  UNION
                                                                                  /* -sq16 start--- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg                     AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg            AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE9''                      AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.transactionpremiumrpt_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                   AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                        AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.transactionpremiumrpt_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -18474 */
                                                                                  /* -sq 16 end--- */
                                                                                  UNION
                                                                                  /* --sq 17 start-- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg                         AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg                AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE10''                         AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.totaldiscountpremrpt_alfa_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                       AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                       AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                            AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                                   AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                             AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg                          AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                            AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                            AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                            AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                autolatepaybillingperiodicity_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pcx_holineratingfactor_alfa) pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.totaldiscountpremrpt_alfa_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                  /* -6439/6433 */
                                                                                  /* --sq17 end--- */
                                                                                  UNION
                                                                                  /* -sq 18 start-- */
                                                                                  SELECT DISTINCT pctl_job.typecode_stg AS ev_act_type_code ,
                                                                                                  pc_job.jobnumber_stg  AS key1 ,
                                                                                                  ''EV_SBTYPE3''          AS SUBTYPE
                                                                                                  /* ,pc_job.CreateTime AS busn_dt */
                                                                                                  ,
                                                                                                  pc_job.createtime_stg                          AS busn_dt ,
                                                                                                  pc_policyperiod.updatetime_stg                 AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE11''                          AS ev_amt_type_code ,
                                                                                                  pc_policyperiod.totalsurchargepremrpt_alfa_stg AS amt ,
                                                                                                  cast(NULL AS timestamp)                        AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                        AS ev_end_dt ,
                                                                                                  pc_job.retired_stg                             AS retired,
                                                                                                  /* case when pc_job.ValidQuote in (''F'',''FALSE'',''0'') and  vj.JobID>0  then 0 else 1 end as ValidQuote, */
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.quotematuritylevel_stg = 1
                                                                                                                  AND             vj.jobid_stg>0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END                                       AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))                 AS amt_ctgy_typecode ,
                                                                                                  pc_policyperiod.publicid_stg              AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))                AS termnumber ,
                                                                                                  cast(''PPV'' AS VARCHAR(60))                AS agmt_type ,
                                                                                                  ''SRC_SYS4''                                AS src_sys ,
                                                                                                  cast(pc_job.jobnumber_stg AS             VARCHAR(60)) AS nk_job_nbr ,
                                                                                                  cast(pc_policyperiod.branchnumber_stg AS VARCHAR(60)) AS vers_nbr,
                                                                                                  cast(NULL AS                             VARCHAR(60)) AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_policyperiodstatus) pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  inner join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.pctl_job) pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT branchid_stg,
                                                                                                                sourceofbusiness_alfa_stg,
                                                                                                                expirationdate_stg
                                                                                                         FROM db_t_prod_stag.pc_effectivedatedfields) pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT jobid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                  WHERE           quotematuritylevel_stg IN(2,3) ) vj
                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                  WHERE           pc_policyperiod.totalsurchargepremrpt_alfa_stg IS NOT NULL
                                                                                  AND             pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  /* -5930/5924 */
                                                                                  /* --sq 18 end--- */
                                                                                  UNION
                                                                                  /* --sq 19 start--- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE35'' ev_act_type_code ,
                                                                                                  cast(bc_paymentrequest.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                  AS SUBTYPE ,
                                                                                                  bc_paymentrequest.requestdate_stg             AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_paymentrequest.updatetime_stg AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE14''            AS ev_amt_type_code ,
                                                                                                  bc_paymentrequest.amount_stg     AS amt ,
                                                                                                  cast(NULL AS timestamp)          AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)          AS ev_end_dt ,
                                                                                                  bc_paymentrequest.retired_stg    AS retired,
                                                                                                  1                                AS validquote,
                                                                                                  cast(NULL AS VARCHAR(50))        AS amt_ctgy_typecode ,
                                                                                                  billingreferencenumber_alfa_stg  AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))        AS termnumber ,
                                                                                                  cast(''INV'' AS VARCHAR(60))        AS agmt_type ,
                                                                                                  ''SRC_SYS5''                        AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))         AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))         AS vers_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))         AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_paymentrequest
                                                                                  join
                                                                                                  (
                                                                                                         SELECT id_stg
                                                                                                         FROM db_t_prod_stag.bctl_paymentrequeststatus) bctl_paymentrequeststatus
                                                                                  ON              bc_paymentrequest.status_stg = bctl_paymentrequeststatus.id_stg
                                                                                  WHERE           bc_paymentrequest.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_paymentrequest.updatetime_stg <= (:end_dttm)
                                                                                  /* -sq 19 end--- */
                                                                                  UNION
                                                                                  /* -sq 20 start-- */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE36''                              AS ev_act_type_code ,
                                                                                                  cast (bc_unappliedfund.id_stg AS VARCHAR (50)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                  bc_unappliedfund.updatetime_stg                AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  CASE
                                                                                                                  WHEN bc_unappliedfund.updatetime_stg > bc_taccount.updatetime_stg THEN bc_unappliedfund.updatetime_stg
                                                                                                                  ELSE bc_taccount.updatetime_stg
                                                                                                  END AS eff_dt ,
                                                                                                  ''FINCL_EV_AMT_TYPE1'' ev_amt_type_code ,
                                                                                                  bc_taccount.balancedenorm_stg           AS amt ,
                                                                                                  cast(NULL AS timestamp)                 AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                 AS ev_end_dt ,
                                                                                                  cast(''0'' AS INT)                        AS retired,
                                                                                                  1                                       AS validquote ,
                                                                                                  bctl_taccountpatternsuffix.typecode_stg AS amt_ctgy_typecode ,
                                                                                                  bc_account.accountnumber_stg            AS agmt_host_id ,
                                                                                                  cast(NULL AS  VARCHAR(60))               AS termnumber ,
                                                                                                  cast(''ACT'' AS VARCHAR(60))               AS agmt_type ,
                                                                                                  ''SRC_SYS5''                               AS src_sys ,
                                                                                                  cast(NULL AS                           VARCHAR(60))                AS nk_job_nbr ,
                                                                                                  cast(NULL AS                           VARCHAR(60))                AS vers_nbr,
                                                                                                  cast(bctl_taccounttype.typecode_stg AS VARCHAR(60))                AS taccount_typecode
                                                                                  FROM            db_t_prod_stag.bc_unappliedfund
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT accountnumber_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bc_account) bc_account
                                                                                  ON              bc_unappliedfund.accountid_stg = bc_account.id_stg
                                                                                  left join       db_t_prod_stag.bc_taccount
                                                                                  ON              bc_unappliedfund.taccountid_stg = bc_taccount.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                taccountownerpatternid_stg,
                                                                                                                suffix_stg,
                                                                                                                taccounttype_stg
                                                                                                         FROM db_t_prod_stag.bc_taccountpattern) bc_taccountpattern
                                                                                  ON              bc_taccount.taccountpatternid_stg = bc_taccountpattern.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT id_stg,
                                                                                                                typecode_stg
                                                                                                         FROM db_t_prod_stag.bctl_taccountpatternsuffix) bctl_taccountpatternsuffix
                                                                                  ON              bc_taccountpattern.suffix_stg = bctl_taccountpatternsuffix.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT typecode_stg,
                                                                                                                id_stg
                                                                                                         FROM db_t_prod_stag.bctl_taccounttype) bctl_taccounttype
                                                                                  ON              bc_taccountpattern.taccounttype_stg = bctl_taccounttype.id_stg
                                                                                  WHERE           (
                                                                                                                  bc_unappliedfund.updatetime_stg > (:start_dttm)
                                                                                                  AND             bc_unappliedfund.updatetime_stg <= (:end_dttm))
                                                                                  OR              (
                                                                                                                  bc_taccount.updatetime_stg > (:start_dttm)
                                                                                                  AND             bc_taccount.updatetime_stg <= (:end_dttm)) ) AS out_loop1 )
                  /* -----src end--- */
                  SELECT          src.ev_id,
                                  src.fincl_ev_amt_type_cd,
                                  src.fincl_ev_amt_dttm,
                                  src.amt,
                                  src.fincl_ev_amt_ctgy_type_cd,
                                  retired,
                                  rnk,
                                  validquote,
                                  src.agmt_id,
                                  src.quotn_id,
                                  src.tacct_type_cd,
                                  src.trans_strt_dttm,
                                  fin_ev_amt_p.ev_id                AS tgt_ev_id,
                                  fin_ev_amt_p.fincl_ev_amt_type_cd AS tgt_fincl_ev_amt_type_cd,
                                  fin_ev_amt_p.fincl_ev_amt_dttm    AS tgt_fincl_ev_amt_dttm,
                                  fin_ev_amt_p.ev_trans_amt         AS tgt_ev_trans_amt,
                                  fin_ev_amt_p.edw_strt_dttm,
                                  fin_ev_amt_p.edw_end_dttm,
                                  ev_id_dup_chk.ev_id AS ev_id_dup_chk,
                                  CASE
                                                  WHEN fin_ev_amt_np.ev_id IS NULL THEN cast( coalesce(cast(to_char(src.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)),0)
                                                                                  || coalesce(src.amt,0)
                                                                                  || coalesce(trim(src.fincl_ev_amt_ctgy_type_cd),0)
                                                                                  || coalesce(src.agmt_id,0) AS VARCHAR(255))
                                                  WHEN fin_ev_amt_p.ev_id IS NULL THEN cast( coalesce(cast(to_char(src.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)),0)
                                                                                  || coalesce(src.amt,0)
                                                                                  || coalesce(trim(src.fincl_ev_amt_ctgy_type_cd),0)
                                                                                  || coalesce(src.agmt_id,0)
                                                                                  || coalesce(src.quotn_id,0) AS VARCHAR(255))
                                  END var_calc_chksm,
                                  CASE
                                                  WHEN fin_ev_amt_np.ev_id IS NULL THEN
                                                                  CASE
                                                                                  WHEN fin_ev_amt_p.ev_id IS NOT NULL THEN cast( ( coalesce( cast( to_char(fin_ev_amt_p.fincl_ev_amt_dttm, ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)),0)
                                                                                                                  || coalesce(fin_ev_amt_p.ev_trans_amt,0)
                                                                                                                  || coalesce(trim(fin_ev_amt_p.fincl_ev_amt_ctgy_type_cd),0)
                                                                                                                  || coalesce(fin_ev_amt_p.agmt_id,0)) AS VARCHAR(255))
                                                                                  ELSE NULL
                                                                  END
                                                  WHEN fin_ev_amt_p.ev_id IS NULL THEN
                                                                  CASE
                                                                                  WHEN fin_ev_amt_np.ev_id IS NOT NULL THEN cast( ( coalesce( cast(to_char( fin_ev_amt_np.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'' ) AS VARCHAR(30)),0)
                                                                                                                  || coalesce(fin_ev_amt_np.ev_trans_amt,0)
                                                                                                                  ||coalesce(trim(fin_ev_amt_np.fincl_ev_amt_ctgy_type_cd),0)
                                                                                                                  || coalesce(fin_ev_amt_np.agmt_id,0)
                                                                                                                  || coalesce(fin_ev_amt_np.quotn_id,0)) AS VARCHAR(255))
                                                                                  ELSE NULL
                                                                  END
                                  END                  var_orig_chksm,
                                  current_timestamp AS edw_strt_dttm
                  FROM            (
                                                  SELECT          ev_id,
                                                                  decode(xlat_src_cd.tgt_idntftn_val,
                                                                         ''GWPC'', ev_id,
                                                                         NULL) AS ev_id_policy,
                                                                  decode(xlat_src_cd.tgt_idntftn_val,
                                                                         ''GWPC'', NULL,
                                                                         ev_id)                                                               AS ev_id_nonpolicy ,
                                                                  coalesce(xlat_ev_act_type_cd.tgt_idntftn_val, ''UNK'')                        AS ev_act_type_code1,
                                                                  coalesce(xlat_sub_type1.tgt_idntftn_val, ''UNK'')                             AS subtype1,
                                                                  coalesce(out_loop.busn_dt, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS fincl_ev_amt_dttm,
                                                                  coalesce(out_loop.eff_dt, cast(''1900-01-01 00:00:00.000000'' AS timestamp) ) AS trans_strt_dttm,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  xlat_amt_type_cd.tgt_idntftn_val IS NULL
                                                                                                  AND             (
                                                                                                                                  out_loop.ev_amt_type_code = ''debit''
                                                                                                                  OR              out_loop.ev_amt_type_code =''credit'') ) THEN upper(out_loop.ev_amt_type_code)
                                                                                  WHEN xlat_amt_type_cd.tgt_idntftn_val IS NOT NULL THEN xlat_amt_type_cd.tgt_idntftn_val
                                                                                  ELSE ''UNK''
                                                                  END                        AS fincl_ev_amt_type_cd,
                                                                  cast(amt AS DECIMAL(25,2)) AS amt,
                                                                  retired,
                                                                  validquote,
                                                                  coalesce(xlat_ctgy_cd.tgt_idntftn_val, ''UNK'') AS fincl_ev_amt_ctgy_type_cd,
                                                                  xlat_src_cd.tgt_idntftn_val                   AS src_cd,
                                                                  CASE
                                                                                  When agmt_type IN (''ACT'',
                                                                                                                    ''INV'',
                                                                                                                    ''PPV'') THEN coalesce(agmt_act.agmt_id,-1)
                                                                                  When agmt_type =''POL'' THEN coalesce(agmt_pol.agmt_id,-1)
                                                                                  When agmt_type =''POLTRM'' THEN coalesce(agmt_poltrm.agmt_id,-1)
                                                                                  ELSE -1
                                                                  END                               AS agmt_id,
                                                                  ins_quotn.quotn_id                AS quotn_id,
                                                                  xlat_tacct_typecd.tgt_idntftn_val AS tacct_type_cd,
                                                                  rnk
                                                  FROM            intrm_fincl_ev_amt AS out_loop
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_tacct_typecd
                                                  ON              xlat_tacct_typecd.src_idntftn_val =out_loop.taccount_typecode
                                                  AND             xlat_tacct_typecd.tgt_idntftn_nm= ''TACCT_TYPE''
                                                  AND             xlat_tacct_typecd.src_idntftn_nm = ''bctl_taccounttype.typecode''
                                                  AND             xlat_tacct_typecd.src_idntftn_sys =''GW''
                                                  AND             xlat_tacct_typecd.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_sub_type1
                                                  ON              xlat_sub_type1.src_idntftn_val = out_loop.SUBTYPE
                                                  AND             xlat_sub_type1.tgt_idntftn_nm= ''EV_SBTYPE''
                                                  AND             xlat_sub_type1.src_idntftn_nm = ''derived''
                                                  AND             xlat_sub_type1.src_idntftn_sys =''DS''
                                                  AND             xlat_sub_type1.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_ctgy_cd
                                                  ON              xlat_ctgy_cd.src_idntftn_val = out_loop.amt_ctgy_typecode
                                                  AND             xlat_ctgy_cd.tgt_idntftn_nm IN ( ''CTGY_TYPE'')
                                                  AND             xlat_ctgy_cd.src_idntftn_nm= ''bctl_taccountpatternsuffix.typecode ''
                                                  AND             xlat_ctgy_cd.src_idntftn_sys=''GW''
                                                  AND             xlat_ctgy_cd.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_ev_act_type_cd
                                                  ON              xlat_ev_act_type_cd.src_idntftn_val = out_loop.ev_act_type_code
                                                  AND             xlat_ev_act_type_cd.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                  AND             xlat_ev_act_type_cd.src_idntftn_nm IN (''derived'',
                                                                                                         ''CCTL_ACTIVITYCATEGORY.TYPECODE'' ,
                                                                                                         ''PCTL_JOB.TYPECODE'',
                                                                                                         ''bctl_transaction.TYPECODE'')
                                                  AND             xlat_ev_act_type_cd.src_idntftn_sys IN (''GW'',
                                                                                                          ''DS'' )
                                                  AND             xlat_ev_act_type_cd.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_src_cd
                                                  ON              xlat_src_cd.src_idntftn_val = src_sys
                                                  AND             xlat_src_cd.tgt_idntftn_nm = ''SRC_SYS''
                                                  AND             xlat_src_cd.src_idntftn_nm= ''derived''
                                                  AND             xlat_src_cd.src_idntftn_sys=''DS''
                                                  AND             xlat_src_cd.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                         SELECT src_idntftn_val,
                                                                                tgt_idntftn_nm,
                                                                                src_idntftn_nm,
                                                                                src_idntftn_sys,
                                                                                expn_dt,
                                                                                tgt_idntftn_val
                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat) xlat_amt_type_cd
                                                  ON              xlat_amt_type_cd.src_idntftn_val = out_loop.ev_amt_type_code
                                                  AND             xlat_amt_type_cd.tgt_idntftn_nm IN ( ''FINCL_EV_AMT_TYPE'' ,
                                                                                                      ''bctl_ledgerside.typecode'')
                                                  AND             xlat_amt_type_cd.src_idntftn_nm= ''derived ''
                                                  AND             xlat_amt_type_cd.src_idntftn_sys=''DS''
                                                  AND             xlat_amt_type_cd.expn_dt=''9999-12-31''
                                                  left outer join
                                                                  (
                                                                           SELECT   ev_id,
                                                                                    ev_desc,
                                                                                    ev_strt_dttm ,
                                                                                    ev_end_dttm,
                                                                                    ev_rsn_cd,
                                                                                    agmt_id,
                                                                                    prcsd_src_sys_cd,
                                                                                    func_cd,
                                                                                    ev_dttm,
                                                                                    edw_strt_dttm,
                                                                                    src_trans_id,
                                                                                    ev_sbtype_cd,
                                                                                    ev_actvy_type_cd
                                                                           FROM     db_t_prod_core.ev
                                                                           WHERE    src_trans_id IN
                                                                                                     (
                                                                                                     SELECT DISTINCT key1
                                                                                                     FROM            intrm_fincl_ev_amt) qualify row_number() over(PARTITION BY ev_sbtype_cd,ev_actvy_type_cd, src_trans_id ORDER BY edw_end_dttm DESC) = 1) evt
                                                  ON              evt.src_trans_id = out_loop.key1
                                                  AND             evt.ev_sbtype_cd=subtype1
                                                  AND             evt.ev_actvy_type_cd= ev_act_type_code1
                                                  left outer join
                                                                  (
                                                                           SELECT   agmt_id,
                                                                                    host_agmt_num,
                                                                                    agmt_type_cd,
                                                                                    term_num ,
                                                                                    nk_src_key
                                                                           FROM     db_t_prod_core.agmt
                                                                           WHERE    nk_src_key IN
                                                                                                   (
                                                                                                   SELECT DISTINCT agmt_host_id
                                                                                                   FROM            intrm_fincl_ev_amt) qualify row_number() over(PARTITION BY nk_src_key, host_agmt_num ORDER BY edw_end_dttm DESC) = 1 ) agmt_act
                                                  ON              agmt_act.nk_src_key= out_loop.agmt_host_id
                                                  AND             agmt_act.agmt_type_cd = agmt_type
                                                  left outer join
                                                                  (
                                                                         SELECT agmt_id,
                                                                                host_agmt_num,
                                                                                agmt_type_cd,
                                                                                edw_end_dttm
                                                                         FROM   db_t_prod_core.agmt
                                                                         WHERE  host_agmt_num IN
                                                                                                  (
                                                                                                  SELECT DISTINCT agmt_host_id
                                                                                                  FROM            intrm_fincl_ev_amt)
                                                                         AND    edw_end_dttm =''9999-12-31 23:59:59.999999''
                                                                         AND    agmt_type_cd=''POL'' ) AS agmt_pol
                                                  ON              agmt_pol.host_agmt_num = out_loop.agmt_host_id
                                                  AND             agmt_pol.agmt_type_cd = ''POL''
                                                  left outer join
                                                                  (
                                                                         SELECT agmt_id,
                                                                                nk_src_key,
                                                                                term_num,
                                                                                host_agmt_num
                                                                         FROM   db_t_prod_core.agmt
                                                                         WHERE  agmt_type_cd = ''POLTRM''
                                                                         AND    edw_end_dttm =''9999-12-31 23:59:59.999999'' ) agmt_poltrm
                                                  ON              agmt_poltrm.host_agmt_num = out_loop.agmt_host_id
                                                  AND             agmt_poltrm.term_num = out_loop.termnumber
                                                  left outer join
                                                                  (
                                                                           SELECT   quotn_id ,
                                                                                    nk_job_nbr ,
                                                                                    vers_nbr
                                                                           FROM     db_t_prod_core.insrnc_quotn
                                                                           WHERE    nk_job_nbr IN
                                                                                                   (
                                                                                                   SELECT DISTINCT nk_job_nbr
                                                                                                   FROM            intrm_fincl_ev_amt) qualify row_number() over(PARTITION BY nk_job_nbr, vers_nbr, src_sys_cd ORDER BY edw_end_dttm DESC) = 1 ) ins_quotn
                                                  ON              ins_quotn.nk_job_nbr=out_loop.nk_job_nbr
                                                  AND             ins_quotn.vers_nbr=out_loop.vers_nbr ) AS src
                  left outer join
                                  (
                                           SELECT   ev_id ,
                                                    tacct_type_cd ,
                                                    fincl_ev_amt_ctgy_type_cd,
                                                    agmt_id,
                                                    fincl_ev_amt_type_cd ,
                                                    fincl_ev_amt_dttm ,
                                                    ev_trans_amt
                                           FROM     db_t_prod_core.fincl_ev_amt
                                           WHERE    edw_end_dttm = ''9999/12/31 23:59:59.999999''
                                           AND      ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   intrm_fincl_ev_amt)) qualify row_number() over(PARTITION BY ev_id, fincl_ev_amt_type_cd, fincl_ev_amt_dttm,ev_trans_amt ORDER BY edw_end_dttm DESC) = 1 ) AS ev_id_dup_chk
                  ON              ev_id_dup_chk.ev_id= src.ev_id
                  AND             trim(ev_id_dup_chk.fincl_ev_amt_type_cd)= trim(src.fincl_ev_amt_type_cd)
                  AND             cast(to_char(ev_id_dup_chk.fincl_ev_amt_dttm, ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)) = cast(to_char(src.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30))
                  AND             ev_id_dup_chk.ev_trans_amt=src.amt
                  left outer join
                                  (
                                           SELECT   fincl_ev_amt_dttm ,
                                                    ev_trans_amt,
                                                    agmt_id ,
                                                    quotn_id ,
                                                    edw_strt_dttm ,
                                                    edw_end_dttm ,
                                                    ev_id ,
                                                    fincl_ev_amt_ctgy_type_cd ,
                                                    fincl_ev_amt_type_cd
                                           FROM     db_t_prod_core.fincl_ev_amt
                                           WHERE    ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   intrm_fincl_ev_amt)) qualify row_number() over(PARTITION BY ev_id, fincl_ev_amt_type_cd, quotn_id ORDER BY edw_end_dttm DESC) = 1 ) AS fin_ev_amt_p
                  ON              fin_ev_amt_p.ev_id= src.ev_id_policy
                  AND             trim( fin_ev_amt_p.fincl_ev_amt_type_cd)=trim(src.fincl_ev_amt_type_cd)
                  AND             coalesce(fin_ev_amt_p.quotn_id,9999)=coalesce(src.quotn_id,9999)
                  left outer join
                                  (
                                           SELECT   fincl_ev_amt_dttm ,
                                                    ev_trans_amt,
                                                    agmt_id ,
                                                    quotn_id ,
                                                    edw_strt_dttm ,
                                                    edw_end_dttm ,
                                                    ev_id ,
                                                    fincl_ev_amt_ctgy_type_cd ,
                                                    fincl_ev_amt_type_cd
                                           FROM     db_t_prod_core.fincl_ev_amt
                                           WHERE    ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   intrm_fincl_ev_amt)) qualify row_number() over(PARTITION BY ev_id, fincl_ev_amt_type_cd ORDER BY edw_end_dttm DESC) = 1 ) AS fin_ev_amt_np
                  ON              fin_ev_amt_np.ev_id= src.ev_id_nonpolicy
                  AND             trim(fin_ev_amt_np.fincl_ev_amt_type_cd)=trim( src.fincl_ev_amt_type_cd ) ) src ) );

  -- Component exp_check_flag, Type EXPRESSION
  CREATE OR replace TEMPORARY TABLE exp_check_flag AS
  (
         SELECT sq_bc_invoice.ev_id                                                    AS ev_id,
                sq_bc_invoice.fincl_ev_amt_type_code                                   AS fincl_ev_amt_type_cd,
                sq_bc_invoice.busn_dt                                                  AS fincl_ev_amt_dttm,
                sq_bc_invoice.amount                                                   AS ev_trans_amt,
                :prcs_id                                                               AS out_prcs_id,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                sq_bc_invoice.amt_ctgy_type_code                                       AS amt_ctgy_typecode,
                sq_bc_invoice.retired                                                  AS retired,
                sq_bc_invoice.rnk                                                      AS rnk,
                sq_bc_invoice.validquote                                               AS validquote,
                sq_bc_invoice.agmt_id                                                  AS agmt_id,
                sq_bc_invoice.quotn_id                                                 AS quotn_id,
                sq_bc_invoice.taccount_typecode                                        AS tacct_type_cd,
                sq_bc_invoice.eff_dt                                                   AS trans_strt_dttm,
                sq_bc_invoice.tgt_ev_id_p                                              AS lkp_ev_id_p,
                sq_bc_invoice.tgt_fincl_ev_amt_type_cd_p                               AS lkp_fincl_ev_amt_type_cd_p,
                sq_bc_invoice.tgt_fincl_ev_amt_dttm_p                                  AS lkp_fincl_ev_amt_dttm_p,
                sq_bc_invoice.tgt_ev_trans_amt_p                                       AS lkp_ev_trans_amt_p,
                sq_bc_invoice.tgt_edw_strt_dttm_p                                      AS lkp_edw_strt_dttm_p,
                sq_bc_invoice.tgt_edw_end_dttm_p                                       AS lkp_edw_end_dttm_p,
                sq_bc_invoice.edw_strt_dttm                                            AS edw_strt_dttm,
                CASE
                       WHEN sq_bc_invoice.var_orig_chksm IS NULL THEN ''I''
                       ELSE
                              CASE
                                     WHEN sq_bc_invoice.var_orig_chksm != sq_bc_invoice.var_calc_chksm
                                     AND    (
                                                   (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''UNAPPLIEDAMT'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''TTLPREM'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''TTLCOST'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''CHGCOST'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''NETDISCSRCH'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''CHGPREMIUM'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''TTLDISC'' )
                                            OR     (
                                                          sq_bc_invoice.fincl_ev_amt_type_code != ''TTLCHRG'' ) )
                                     AND    sq_bc_invoice.ev_id_dup_chk IS NULL THEN ''U''
                                     ELSE
                                            CASE
                                                   WHEN sq_bc_invoice.var_orig_chksm != sq_bc_invoice.var_calc_chksm
                                                   AND    (
                                                                 (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''UNAPPLIEDAMT'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''TTLPREM'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''TTLCOST'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''CHGCOST'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''NETDISCSRCH'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''CHGPREMIUM'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''TTLDISC'' )
                                                          OR     (
                                                                        sq_bc_invoice.fincl_ev_amt_type_code = ''TTLCHRG'' ) ) THEN ''U''
                                                   ELSE ''R''
                                            END
                              END
                END AS out_ins_upd,
                sq_bc_invoice.source_record_id
         FROM   sq_bc_invoice 
  );
  
  -- Component rtr_fincl_ev_amt_insupd_INSERT, Type ROUTER Output Group INSERT
  CREATE OR REPLACE TEMPORARY TABLE rtr_fincl_ev_amt_insupd_INSERT AS (
  SELECT exp_check_flag.ev_id                      AS ev_id,
         exp_check_flag.fincl_ev_amt_type_cd       AS fincl_ev_amt_type_cd,
         exp_check_flag.fincl_ev_amt_dttm          AS fincl_ev_amt_dttm,
         exp_check_flag.ev_trans_amt               AS ev_trans_amt,
         exp_check_flag.out_prcs_id                AS prcs_id,
         exp_check_flag.out_edw_end_dttm           AS out_edw_end_dttm,
         exp_check_flag.retired                    AS retired,
         exp_check_flag.amt_ctgy_typecode          AS amt_ctgy_typecode,
         exp_check_flag.rnk                        AS rnk,
         exp_check_flag.validquote                 AS validquote,
         exp_check_flag.agmt_id                    AS agmt_id,
         exp_check_flag.quotn_id                   AS quotn_id,
         exp_check_flag.tacct_type_cd              AS tgt_idntftn_val,
         exp_check_flag.trans_strt_dttm            AS trans_strt_dttm,
         exp_check_flag.lkp_ev_id_p                AS lkp_fincl_ev_amt_ev_id,
         exp_check_flag.lkp_fincl_ev_amt_type_cd_p AS lkp_fincl_ev_amt_type_cd,
         exp_check_flag.lkp_fincl_ev_amt_dttm_p    AS lkp_fincl_ev_amt_dttm,
         exp_check_flag.lkp_ev_trans_amt_p         AS lkp_ev_trans_amt,
         exp_check_flag.lkp_edw_strt_dttm_p        AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm_p         AS lkp_edw_end_dttm,
         exp_check_flag.edw_strt_dttm              AS edw_strt_dttm,
         exp_check_flag.out_ins_upd                AS out_ins_upd,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.out_ins_upd = ''I''
  AND    exp_check_flag.ev_id IS NOT NULL
  OR     exp_check_flag.out_ins_upd = ''U''
  OR     (
                exp_check_flag.retired = 0
         AND    exp_check_flag.lkp_edw_end_dttm_p != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
  );
  
  -- Component rtr_fincl_ev_amt_insupd_RETIRED, Type ROUTER Output Group RETIRED
  CREATE OR REPLACE TEMPORARY TABLE rtr_fincl_ev_amt_insupd_RETIRED AS (
  SELECT exp_check_flag.ev_id                      AS ev_id,
         exp_check_flag.fincl_ev_amt_type_cd       AS fincl_ev_amt_type_cd,
         exp_check_flag.fincl_ev_amt_dttm          AS fincl_ev_amt_dttm,
         exp_check_flag.ev_trans_amt               AS ev_trans_amt,
         exp_check_flag.out_prcs_id                AS prcs_id,
         exp_check_flag.out_edw_end_dttm           AS out_edw_end_dttm,
         exp_check_flag.retired                    AS retired,
         exp_check_flag.amt_ctgy_typecode          AS amt_ctgy_typecode,
         exp_check_flag.rnk                        AS rnk,
         exp_check_flag.validquote                 AS validquote,
         exp_check_flag.agmt_id                    AS agmt_id,
         exp_check_flag.quotn_id                   AS quotn_id,
         exp_check_flag.tacct_type_cd              AS tgt_idntftn_val,
         exp_check_flag.trans_strt_dttm            AS trans_strt_dttm,
         exp_check_flag.lkp_ev_id_p                AS lkp_fincl_ev_amt_ev_id,
         exp_check_flag.lkp_fincl_ev_amt_type_cd_p AS lkp_fincl_ev_amt_type_cd,
         exp_check_flag.lkp_fincl_ev_amt_dttm_p    AS lkp_fincl_ev_amt_dttm,
         exp_check_flag.lkp_ev_trans_amt_p         AS lkp_ev_trans_amt,
         exp_check_flag.lkp_edw_strt_dttm_p        AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm_p         AS lkp_edw_end_dttm,
         exp_check_flag.edw_strt_dttm              AS edw_strt_dttm,
         exp_check_flag.out_ins_upd                AS out_ins_upd,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.out_ins_upd = ''R''
  AND    exp_check_flag.retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm_p = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  -- > NOT INSERT OR UPDATE , no CHANGE IN VALUES 
  -- > but data IS retired 
  -- > UPDATE these records WITH current_timestamp
  );
  
  -- Component upd_fincl_ev_ins, Type UPDATE
  CREATE OR replace TEMPORARY TABLE upd_fincl_ev_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_amt_insupd_insert.ev_id                AS ev_id,
                rtr_fincl_ev_amt_insupd_insert.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                rtr_fincl_ev_amt_insupd_insert.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                rtr_fincl_ev_amt_insupd_insert.ev_trans_amt         AS ev_trans_amt,
                rtr_fincl_ev_amt_insupd_insert.prcs_id              AS prcs_id,
                rtr_fincl_ev_amt_insupd_insert.edw_strt_dttm        AS edw_strt_dttm1,
                rtr_fincl_ev_amt_insupd_insert.out_edw_end_dttm     AS out_edw_end_dttm1,
                rtr_fincl_ev_amt_insupd_insert.retired              AS retired1,
                rtr_fincl_ev_amt_insupd_insert.amt_ctgy_typecode    AS o_amt_ctgy_typecode1,
                rtr_fincl_ev_amt_insupd_insert.rnk                  AS rnk1,
                rtr_fincl_ev_amt_insupd_insert.validquote           AS validquote1,
                rtr_fincl_ev_amt_insupd_insert.agmt_id              AS agmt_id1,
                rtr_fincl_ev_amt_insupd_insert.quotn_id             AS quotn_id1,
                rtr_fincl_ev_amt_insupd_insert.tgt_idntftn_val      AS tgt_idntftn_val,
                rtr_fincl_ev_amt_insupd_insert.trans_strt_dttm      AS trans_strt_dttm1,
                rtr_fincl_ev_amt_insupd_insert.source_record_id,
                0                                                   AS update_strategy_action
         FROM   rtr_fincl_ev_amt_insupd_insert );
  -- Component upd_fincl_ev_amt_upd_Retire_rejected, Type UPDATE
  CREATE OR replace TEMPORARY TABLE upd_fincl_ev_amt_upd_retire_rejected AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_amt_insupd_retired.lkp_fincl_ev_amt_ev_id   AS ev_id,
                rtr_fincl_ev_amt_insupd_retired.lkp_fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                rtr_fincl_ev_amt_insupd_retired.lkp_fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                rtr_fincl_ev_amt_insupd_retired.lkp_ev_trans_amt         AS ev_trans_amt,
                rtr_fincl_ev_amt_insupd_retired.prcs_id                  AS prcs_id,
                rtr_fincl_ev_amt_insupd_retired.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_fincl_ev_amt_insupd_retired.edw_strt_dttm            AS edw_strt_dttm3,
                rtr_fincl_ev_amt_insupd_retired.source_record_id,
                1                                                AS update_strategy_action
         FROM   rtr_fincl_ev_amt_insupd_retired );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_fincl_ev_ins.ev_id                                                                AS ev_id,
                upd_fincl_ev_ins.fincl_ev_amt_type_cd                                                 AS fincl_ev_amt_type_cd,
                upd_fincl_ev_ins.fincl_ev_amt_dttm                                                    AS fincl_ev_amt_dttm,
                upd_fincl_ev_ins.ev_trans_amt                                                         AS ev_trans_amt,
                upd_fincl_ev_ins.prcs_id                                                              AS prcs_id,
                upd_fincl_ev_ins.o_amt_ctgy_typecode1                                                 AS o_amt_ctgy_typecode1,
                dateadd(''second'', ( 2 * ( upd_fincl_ev_ins.rnk1 - 1 ) ), upd_fincl_ev_ins.edw_strt_dttm1) AS vt_edw_strt_dttm1,
                vt_edw_strt_dttm1                                                                     AS o_edw_strt_dttm,
                CASE
                       WHEN upd_fincl_ev_ins.retired1 != 0
                       OR     upd_fincl_ev_ins.validquote1 = 0 THEN dateadd(''second'', 1, vt_edw_strt_dttm1)
                       ELSE upd_fincl_ev_ins.out_edw_end_dttm1
                END                               AS o_edw_end_dttm,
                upd_fincl_ev_ins.agmt_id1         AS agmt_id1,
                upd_fincl_ev_ins.quotn_id1        AS quotn_id1,
                upd_fincl_ev_ins.tgt_idntftn_val  AS tgt_idntftn_val,
                upd_fincl_ev_ins.trans_strt_dttm1 AS trans_strt_dttm1,
                CASE
                       WHEN upd_fincl_ev_ins.retired1 != 0
                       OR     upd_fincl_ev_ins.validquote1 = 0 THEN dateadd(''second'', 1, upd_fincl_ev_ins.trans_strt_dttm1)
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trans_end_dttm1,
                upd_fincl_ev_ins.source_record_id
         FROM   upd_fincl_ev_ins );
  -- Component tgt_fincl_ev_amt_ins, Type TARGET
  INSERT INTO db_t_prod_core.fincl_ev_amt
              (
                          ev_id,
                          fincl_ev_amt_type_cd,
                          fincl_ev_amt_dttm,
                          ev_trans_amt,
                          fincl_ev_amt_ctgy_type_cd,
                          agmt_id,
                          quotn_id,
                          tacct_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.ev_id                AS ev_id,
         exp_pass_to_target_ins.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
         exp_pass_to_target_ins.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
         exp_pass_to_target_ins.ev_trans_amt         AS ev_trans_amt,
         exp_pass_to_target_ins.o_amt_ctgy_typecode1 AS fincl_ev_amt_ctgy_type_cd,
         exp_pass_to_target_ins.agmt_id1             AS agmt_id,
         exp_pass_to_target_ins.quotn_id1            AS quotn_id,
         exp_pass_to_target_ins.tgt_idntftn_val      AS tacct_type_cd,
         exp_pass_to_target_ins.prcs_id              AS prcs_id,
         exp_pass_to_target_ins.o_edw_strt_dttm      AS edw_strt_dttm,
         exp_pass_to_target_ins.o_edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm1     AS trans_strt_dttm,
         exp_pass_to_target_ins.out_trans_end_dttm1  AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component exp_pass_to_target_upd_Retire_rejected, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd_retire_rejected AS
  (
         SELECT upd_fincl_ev_amt_upd_retire_rejected.ev_id                AS ev_id,
                upd_fincl_ev_amt_upd_retire_rejected.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                upd_fincl_ev_amt_upd_retire_rejected.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                upd_fincl_ev_amt_upd_retire_rejected.ev_trans_amt         AS ev_trans_amt,
                upd_fincl_ev_amt_upd_retire_rejected.lkp_edw_strt_dttm3   AS lkp_edw_strt_dttm3,
                upd_fincl_ev_amt_upd_retire_rejected.edw_strt_dttm3       AS o_edw_end_dttm3,
                upd_fincl_ev_amt_upd_retire_rejected.source_record_id
         FROM   upd_fincl_ev_amt_upd_retire_rejected );
  -- Component tgt_fincl_ev_amt_upd_Retire_rejected, Type TARGET
  merge
  INTO         db_t_prod_core.fincl_ev_amt
  USING        exp_pass_to_target_upd_retire_rejected
  ON (fincl_ev_amt.ev_id = exp_pass_to_target_upd_retire_rejected.ev_id)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_pass_to_target_upd_retire_rejected.ev_id,
         fincl_ev_amt_type_cd = exp_pass_to_target_upd_retire_rejected.fincl_ev_amt_type_cd,
         fincl_ev_amt_dttm = exp_pass_to_target_upd_retire_rejected.fincl_ev_amt_dttm,
         ev_trans_amt = exp_pass_to_target_upd_retire_rejected.ev_trans_amt,
         edw_strt_dttm = exp_pass_to_target_upd_retire_rejected.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd_retire_rejected.o_edw_end_dttm3,
         trans_end_dttm = exp_pass_to_target_upd_retire_rejected.o_edw_end_dttm3;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component sq_bc_invoice_billing_transaction, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice_billing_transaction AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ev_id,
                $2  AS fincl_ev_amt_type_code,
                $3  AS busn_dt,
                $4  AS amount,
                $5  AS amt_ctgy_type_code,
                $6  AS retired,
                $7  AS trm_nbr,
                $8  AS rnk,
                $9  AS validquote,
                $10 AS agmt_id,
                $11 AS quotn_id,
                $12 AS taccount_typecode,
                $13 AS eff_dt,
                $14 AS tgt_ev_id_np,
                $15 AS tgt_fincl_ev_amt_type_cd_np,
                $16 AS tgt_fincl_ev_amt_dttm_np,
                $17 AS tgt_ev_trans_amt_np,
                $18 AS tgt_edw_strt_dttm_np,
                $19 AS tgt_edw_end_dttm_np,
                $20 AS ev_id_dup_chk,
                $21 AS var_calc_chksm,
                $22 AS var_orig_chksm,
                $23 AS edw_strt_dttm,
                $24 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                  /****BC INVOICE BILLING TRANSACTION****/
                                  WITH out_loop AS
                                  (
                                                  /***********************Billing Transaction ********************************/
                                                  SELECT DISTINCT ev_act_type_code,
                                                                  key1,
                                                                  SUBTYPE,
                                                                  busn_dt,
                                                                  eff_dt,
                                                                  ev_amt_type_code,
                                                                  amt,
                                                                  ev_strt_dt,
                                                                  ev_end_dt,
                                                                  retired,
                                                                  validquote,
                                                                  amt_ctgy_typecode,
                                                                  agmt_host_id,
                                                                  agmt_type,
                                                                  src_sys,
                                                                  nk_job_nbr,
                                                                  vers_nbr,
                                                                  taccount_typecode,
                                                                  trm_nbr,
                                                                  rank() over(PARTITION BY ev_act_type_code,key1,SUBTYPE,ev_amt_type_code,amt_ctgy_typecode,taccount_typecode ORDER BY eff_dt,validquote,amt, agmt_host_id DESC ) AS rnk
                                                  FROM            (
                                                                                  SELECT DISTINCT bctl_transaction.typecode_stg                AS ev_act_type_code ,
                                                                                                  cast (bc_transaction.id_stg AS VARCHAR (50)) AS key1 ,
                                                                                                  ''FINANCL''                                    AS SUBTYPE ,
                                                                                                  bc_transaction.transactiondate_stg           AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_transaction.updatetime_stg                      AS eff_dt ,
                                                                                                  bctl_ledgerside.typecode_stg                       AS ev_amt_type_code ,
                                                                                                  bc_lineitem.amount_stg                             AS amt ,
                                                                                                  cast(NULL AS timestamp)                            AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                            AS ev_end_dt ,
                                                                                                  bc_transaction.retired_stg                         AS retired,
                                                                                                  1                                                  AS validquote ,
                                                                                                  nvl(bctl_taccountpatternsuffix.typecode_stg,''UNK'')    amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.policynumber_stg
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN bc_account.accountnumber_stg
                                                                                                                  ELSE ''''
                                                                                                  END AS agmt_host_id ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN cast(''POLTRM'' AS VARCHAR(50))
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN ''ACT''
                                                                                                                  ELSE NULL
                                                                                                  END                                       AS agmt_type ,
                                                                                                  ''GWBC''                                    AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS vers_nbr,
                                                                                                  nvl(bctl_taccounttype.typecode_stg,''UNK'')    taccount_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.termnumber_stg
                                                                                                                  ELSE NULL
                                                                                                  END AS trm_nbr
                                                                                  FROM            db_t_prod_stag.bc_transaction
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg=bc_transaction.subtype_stg
                                                                                  join            db_t_prod_stag.bc_lineitem
                                                                                  ON              bc_lineitem.transactionid_stg=bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_taccount
                                                                                  ON              bc_taccount.id_stg=bc_lineitem.taccountid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccount
                                                                                  ON              bctl_taccount.id_stg=bc_taccount.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg=bc_taccount.taccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccountcontainer
                                                                                  ON              bctl_taccountcontainer.id_stg=bc_taccountcontainer.subtype_stg
                                                                                  left join       db_t_prod_stag.bc_account
                                                                                  ON              bc_taccountcontainer.id_stg = bc_account.hiddentaccountcontainerid_stg
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                                                  join            db_t_prod_stag.bctl_ledgerside
                                                                                  ON              bctl_ledgerside.id_stg=bc_lineitem.type_stg
                                                                                  left join       db_t_prod_stag.bc_taccountpattern
                                                                                  ON              bc_taccount.taccountpatternid_stg = bc_taccountpattern.id_stg
                                                                                  left join       db_t_prod_stag.bc_tacctownerpattern
                                                                                  ON              bc_taccountpattern.taccountownerpatternid_stg = bc_tacctownerpattern.id_stg
                                                                                  left join       db_t_prod_stag.bctl_taccountpatternsuffix
                                                                                  ON              bc_taccountpattern.suffix_stg = bctl_taccountpatternsuffix.id_stg
                                                                                  left join       db_t_prod_stag.bctl_taccounttype
                                                                                  ON              bc_taccountpattern.taccounttype_stg = bctl_taccounttype.id_stg
                                                                                  left join       db_t_prod_stag.bc_revtrans
                                                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.updatetime_stg > (:start_dttm)
                                                                                                                  AND             bc_transaction.updatetime_stg <= (:end_dttm))
                                                                                                  OR              (
                                                                                                                                  bc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                  AND             bc_policyperiod.updatetime_stg <= (:end_dttm)) )
                                                                                  UNION
                                                                                  SELECT DISTINCT ''rvrs''
                                                                                                                  || ''-''
                                                                                                                  || bctl_transaction.typecode_stg AS ev_act_type_code ,
                                                                                                  cast (bc_transaction.id_stg AS VARCHAR (50))     AS key1 ,
                                                                                                  ''FINANCL''                                        AS SUBTYPE ,
                                                                                                  bc_transaction.transactiondate_stg               AS busn_dt
                                                                                                  /* ,Createtime as busn_dt */
                                                                                                  ,
                                                                                                  bc_transaction.updatetime_stg                      AS eff_dt ,
                                                                                                  bctl_ledgerside.typecode_stg                       AS ev_amt_type_code ,
                                                                                                  bc_lineitem.amount_stg                             AS amt ,
                                                                                                  cast(NULL AS timestamp)                            AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                            AS ev_end_dt ,
                                                                                                  bc_transaction.retired_stg                         AS retired,
                                                                                                  1                                                  AS validquote ,
                                                                                                  nvl(bctl_taccountpatternsuffix.typecode_stg,''UNK'')    amt_ctgy_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.policynumber_stg
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN bc_account.accountnumber_stg
                                                                                                                  ELSE ''''
                                                                                                  END AS agmt_host_id ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN cast(''POLTRM'' AS VARCHAR(50))
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN ''ACT''
                                                                                                                  ELSE NULL
                                                                                                  END                                       AS agmt_type ,
                                                                                                  ''GWBC''                                    AS src_sys ,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS nk_job_nbr ,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS vers_nbr,
                                                                                                  nvl(bctl_taccounttype.typecode_stg,''UNK'')    taccount_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.termnumber_stg
                                                                                                                  ELSE NULL
                                                                                                  END AS trm_nbr
                                                                                  FROM            db_t_prod_stag.bc_transaction
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg=bc_transaction.subtype_stg
                                                                                  join            db_t_prod_stag.bc_revtrans
                                                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                                  AND             bc_revtrans.ownerid_stg IS NOT NULL
                                                                                  join            db_t_prod_stag.bc_lineitem
                                                                                  ON              bc_lineitem.transactionid_stg=bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_taccount
                                                                                  ON              bc_taccount.id_stg=bc_lineitem.taccountid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccount
                                                                                  ON              bctl_taccount.id_stg=bc_taccount.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg=bc_taccount.taccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccountcontainer
                                                                                  ON              bctl_taccountcontainer.id_stg=bc_taccountcontainer.subtype_stg
                                                                                  left join       db_t_prod_stag.bc_account
                                                                                  ON              bc_taccountcontainer.id_stg = bc_account.hiddentaccountcontainerid_stg
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                                                  join            db_t_prod_stag.bctl_ledgerside
                                                                                  ON              bctl_ledgerside.id_stg=bc_lineitem.type_stg
                                                                                  left join       db_t_prod_stag.bc_taccountpattern
                                                                                  ON              bc_taccount.taccountpatternid_stg = bc_taccountpattern.id_stg
                                                                                  left join       db_t_prod_stag.bc_tacctownerpattern
                                                                                  ON              bc_taccountpattern.taccountownerpatternid_stg = bc_tacctownerpattern.id_stg
                                                                                  left join       db_t_prod_stag.bctl_taccountpatternsuffix
                                                                                  ON              bc_taccountpattern.suffix_stg = bctl_taccountpatternsuffix.id_stg
                                                                                  left join       db_t_prod_stag.bctl_taccounttype
                                                                                  ON              bc_taccountpattern.taccounttype_stg = bctl_taccounttype.id_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.updatetime_stg > (:start_dttm)
                                                                                                                  AND             bc_transaction.updatetime_stg <= (:end_dttm))
                                                                                                  OR             (
                                                                                                                                  bc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                  AND             bc_policyperiod.updatetime_stg <= (:end_dttm)) )
                                                                                  UNION
                                                                                  SELECT DISTINCT bctl_transaction.typecode_stg              AS ev_act_type_code,
                                                                                                  cast(bc_transaction.id_stg AS VARCHAR (50))AS key1,
                                                                                                  ''FINANCL''                                  AS SUBTYPE,
                                                                                                  bc_transaction.transactiondate_stg         AS busn_dt,
                                                                                                  ev_stag.creationts_stg                     AS eff_dt,
                                                                                                  /* --change */
                                                                                                  ''FINCL_EV_AMT_TYPE16''           AS ev_amt_type_code,
                                                                                                  ev_stag.payload_transamount_stg AS amt,
                                                                                                  /* --change  */
                                                                                                  cast(NULL AS timestamp)    AS ev_strt_dt,
                                                                                                  cast(NULL AS timestamp)    AS ev_end_dt,
                                                                                                  bc_transaction.retired_stg AS retired,
                                                                                                  1                          AS validquote,
                                                                                                  ''UNK''                      AS amt_ctgy_typecode,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.policynumber_stg
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN bc_account.accountnumber_stg
                                                                                                                  ELSE ''''
                                                                                                  END AS agmt_host_id,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN cast(''POLTRM'' AS VARCHAR(50))
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN ''ACT''
                                                                                                                  ELSE NULL
                                                                                                  END                                       AS agmt_type,
                                                                                                  ''GWBC''                                    AS src_sys,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS nk_job_nbr,
                                                                                                  cast(NULL AS VARCHAR(60))                 AS vers_nbr,
                                                                                                  nvl(bctl_taccounttype.typecode_stg,''UNK'')    taccount_typecode ,
                                                                                                  CASE
                                                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.termnumber_stg
                                                                                                                  ELSE NULL
                                                                                                  END AS trm_nbr
                                                                                  FROM            db_t_prod_stag.bc_transaction
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg=bc_transaction.subtype_stg
                                                                                  inner join      db_t_prod_stag.bc_lineitem
                                                                                  ON              bc_lineitem.transactionid_stg=bc_transaction.id_stg
                                                                                  left outer join db_t_prod_stag.bc_taccount
                                                                                  ON              bc_taccount.id_stg=bc_lineitem.taccountid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccount
                                                                                  ON              bctl_taccount.id_stg=bc_taccount.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                                                  ON              bc_taccountcontainer.id_stg=bc_taccount.taccountcontainerid_stg
                                                                                  left outer join db_t_prod_stag.bctl_taccountcontainer
                                                                                  ON              bctl_taccountcontainer.id_stg=bc_taccountcontainer.subtype_stg
                                                                                  left join       db_t_prod_stag.bc_account
                                                                                  ON              bc_taccountcontainer.id_stg = bc_account.hiddentaccountcontainerid_stg
                                                                                  left join       db_t_prod_stag.bc_policyperiod
                                                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                                                  left join       db_t_prod_stag.bc_taccountpattern
                                                                                  ON              bc_taccount.taccountpatternid_stg = bc_taccountpattern.id_stg
                                                                                  left join       db_t_prod_stag.bctl_taccounttype
                                                                                  ON              bc_taccountpattern.taccounttype_stg = bctl_taccounttype.id_stg
                                                                                  join            db_t_prod_stag.gl_eventstaging_bc ev_stag
                                                                                  ON              bc_transaction.publicid_stg = ev_stag.publicid_stg
                                                                                  AND             bctl_transaction.typecode_stg = ev_stag.rootentity_stg
                                                                                  WHERE           bc_transaction.updatetime_stg > (:start_dttm)
                                                                                  AND             bc_transaction.updatetime_stg <= (:end_dttm) ) AS out_loop1 )
                  /* --sq end-- */
                  SELECT          src.ev_id,
                                  src.fincl_ev_amt_type_cd,
                                  src.fincl_ev_amt_dttm,
                                  src.amt,
                                  src.fincl_ev_amt_ctgy_type_cd,
                                  retired,
                                  trm_nbr,
                                  rnk,
                                  validquote,
                                  src.agmt_id,
                                  src.quotn_id,
                                  src.tacct_type_cd,
                                  src.trans_strt_dttm,
                                  fin_ev_amt_np.ev_id                AS tgt_ev_id,
                                  fin_ev_amt_np.fincl_ev_amt_type_cd AS tgt_fincl_ev_amt_type_cd,
                                  fin_ev_amt_np.fincl_ev_amt_dttm    AS tgt_fincl_ev_amt_dttm,
                                  fin_ev_amt_np.ev_trans_amt         AS tgt_ev_trans_amt,
                                  fin_ev_amt_np.edw_strt_dttm,
                                  fin_ev_amt_np.edw_end_dttm,
                                  ev_id_dup_chk.ev_id AS ev_id_dup_chk,
                                  cast( coalesce(cast(to_char(src.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)),0)
                                                  || coalesce(src.amt,0)
                                                  || coalesce(src.agmt_id,0)
                                                  || coalesce(src.quotn_id,0) AS VARCHAR(255)) AS var_calc_chksm,
                                  cast( ( coalesce( cast(to_char( fin_ev_amt_np.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'' ) AS VARCHAR(30)),0)
                                                  || coalesce(fin_ev_amt_np.ev_trans_amt,0)
                                                  || coalesce(fin_ev_amt_np.agmt_id,0)
                                                  || coalesce(fin_ev_amt_np.quotn_id,0)) AS VARCHAR(255)) AS var_orig_chksm ,
                                  current_timestamp                                                       AS edw_strt_dttm
                  FROM            (
                                                  SELECT          evt.ev_id,
                                                                  ev_id                                                                       AS ev_id_nonpolicy ,
                                                                  coalesce(xlat_ev_act_type_cd.tgt_idntftn_val, ''UNK'')                        AS ev_act_type_code1,
                                                                  coalesce(SUBTYPE, ''UNK'')                                                    AS subtype1,
                                                                  coalesce(out_loop.busn_dt, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS fincl_ev_amt_dttm,
                                                                  coalesce(out_loop.eff_dt, cast(''1900-01-01 00:00:00.000000'' AS timestamp) ) AS trans_strt_dttm,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  xlat_amt_type_cd.tgt_idntftn_val IS NULL
                                                                                                  AND             (
                                                                                                                                  out_loop.ev_amt_type_code = ''debit''
                                                                                                                  OR              out_loop.ev_amt_type_code =''credit'') ) THEN upper(out_loop.ev_amt_type_code)
                                                                                  WHEN xlat_amt_type_cd.tgt_idntftn_val IS NOT NULL THEN xlat_amt_type_cd.tgt_idntftn_val
                                                                                  ELSE ''UNK''
                                                                  END                        AS fincl_ev_amt_type_cd,
                                                                  cast(amt AS DECIMAL(25,2)) AS amt,
                                                                  retired,
                                                                  validquote,
                                                                  coalesce(xlat_ctgy_cd.tgt_idntftn_val, ''UNK'') AS fincl_ev_amt_ctgy_type_cd,
                                                                  src_sys                        AS src_cd,
                                                                  CASE
                                                                                  When agmt_type IN (''ACT'',
                                                                                                                    ''INV'') THEN coalesce(agmt_act.agmt_id,-1)
                                                                                  When agmt_type =''POLTRM'' THEN coalesce(agmt_polterm.agmt_id,-1)
                                                                                  When agmt_type =''POL'' THEN coalesce(agmt_pol.agmt_id,-1)
                                                                                  ELSE -1
                                                                  END                               AS agmt_id,
                                                                  ins_quotn.quotn_id                AS quotn_id,
                                                                  xlat_tacct_typecd.tgt_idntftn_val AS tacct_type_cd,
                                                                  trm_nbr,
                                                                  rnk
                                                  FROM            out_loop
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat xlat_tacct_typecd
                                                  ON              xlat_tacct_typecd.src_idntftn_val =out_loop.taccount_typecode
                                                  AND             xlat_tacct_typecd.tgt_idntftn_nm= ''TACCT_TYPE''
                                                  AND             xlat_tacct_typecd.src_idntftn_nm = ''bctl_taccounttype.typecode''
                                                  AND             xlat_tacct_typecd.src_idntftn_sys =''GW''
                                                  AND             xlat_tacct_typecd.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat xlat_ctgy_cd
                                                  ON              xlat_ctgy_cd.src_idntftn_val = out_loop.amt_ctgy_typecode
                                                  AND             xlat_ctgy_cd.tgt_idntftn_nm IN ( ''CTGY_TYPE'')
                                                  AND             xlat_ctgy_cd.src_idntftn_nm= ''bctl_taccountpatternsuffix.typecode ''
                                                  AND             xlat_ctgy_cd.src_idntftn_sys=''GW''
                                                  AND             xlat_ctgy_cd.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat xlat_ev_act_type_cd
                                                  ON              xlat_ev_act_type_cd.src_idntftn_val = out_loop.ev_act_type_code
                                                  AND             xlat_ev_act_type_cd.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                  AND             xlat_ev_act_type_cd.src_idntftn_nm IN (''derived'',
                                                                                                         ''CCTL_ACTIVITYCATEGORY.TYPECODE'' ,
                                                                                                         ''PCTL_JOB.TYPECODE'',
                                                                                                         ''bctl_transaction.TYPECODE'')
                                                  AND             xlat_ev_act_type_cd.src_idntftn_sys IN (''GW'',
                                                                                                          ''DS'' )
                                                  AND             xlat_ev_act_type_cd.expn_dt=''9999-12-31''
                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat xlat_amt_type_cd
                                                  ON              xlat_amt_type_cd.src_idntftn_val = out_loop.ev_amt_type_code
                                                  AND             xlat_amt_type_cd.tgt_idntftn_nm IN ( ''FINCL_EV_AMT_TYPE'' ,
                                                                                                      ''bctl_ledgerside.typecode'')
                                                  AND             xlat_amt_type_cd.src_idntftn_nm= ''derived ''
                                                  AND             xlat_amt_type_cd.src_idntftn_sys=''DS''
                                                  AND             xlat_amt_type_cd.expn_dt=''9999-12-31''
                                                  join
                                                                  (
                                                                           SELECT   ev_id,
                                                                                    ev_desc,
                                                                                    ev_strt_dttm ,
                                                                                    ev_end_dttm,
                                                                                    ev_rsn_cd,
                                                                                    agmt_id,
                                                                                    prcsd_src_sys_cd,
                                                                                    func_cd,
                                                                                    ev_dttm,
                                                                                    edw_strt_dttm,
                                                                                    src_trans_id,
                                                                                    ev_sbtype_cd,
                                                                                    ev_actvy_type_cd
                                                                           FROM     db_t_prod_core.ev
                                                                           WHERE    src_trans_id IN
                                                                                                     (
                                                                                                     SELECT DISTINCT key1
                                                                                                     FROM            out_loop) qualify row_number() over(PARTITION BY ev_sbtype_cd,ev_actvy_type_cd, src_trans_id ORDER BY edw_end_dttm DESC) = 1) evt
                                                  ON              evt.src_trans_id = out_loop.key1
                                                  AND             evt.ev_sbtype_cd=subtype1
                                                  AND             evt.ev_actvy_type_cd= ev_act_type_code1
                                                  left outer join
                                                                  (
                                                                           SELECT   agmt_id,
                                                                                    host_agmt_num,
                                                                                    agmt_type_cd,
                                                                                    term_num ,
                                                                                    nk_src_key
                                                                           FROM     db_t_prod_core.agmt
                                                                           WHERE    nk_src_key IN
                                                                                                   (
                                                                                                   SELECT DISTINCT agmt_host_id
                                                                                                   FROM            out_loop) qualify row_number() over(PARTITION BY nk_src_key, host_agmt_num ORDER BY edw_end_dttm DESC) = 1 ) agmt_act
                                                  ON              agmt_act.nk_src_key= out_loop.agmt_host_id
                                                  AND             agmt_act.agmt_type_cd = agmt_type
                                                  left outer join
                                                                  (
                                                                         SELECT agmt_id,
                                                                                host_agmt_num,
                                                                                agmt_type_cd,
                                                                                edw_end_dttm
                                                                         FROM   db_t_prod_core.agmt
                                                                         WHERE  host_agmt_num IN
                                                                                                  (
                                                                                                  SELECT DISTINCT agmt_host_id
                                                                                                  FROM            out_loop)
                                                                         AND    edw_end_dttm =''9999-12-31 23:59:59.999999''
                                                                         AND    agmt_type_cd=''POL'' ) AS agmt_pol
                                                  ON              agmt_pol.host_agmt_num = out_loop.agmt_host_id
                                                  AND             agmt_pol.agmt_type_cd = ''POL''
                                                  left outer join
                                                                  (
                                                                         SELECT agmt_id,
                                                                                host_agmt_num,
                                                                                agmt_type_cd,
                                                                                term_num,
                                                                                edw_end_dttm
                                                                         FROM   db_t_prod_core.agmt
                                                                         WHERE  host_agmt_num IN
                                                                                                  (
                                                                                                  SELECT DISTINCT agmt_host_id
                                                                                                  FROM            out_loop)
                                                                         AND    edw_end_dttm =''9999-12-31 23:59:59.999999''
                                                                         AND    agmt_type_cd=''POLTRM'' ) AS agmt_polterm
                                                  ON              agmt_polterm.host_agmt_num = out_loop.agmt_host_id
                                                  AND             agmt_polterm.agmt_type_cd = ''POLTRM''
                                                  AND             agmt_polterm.term_num=cast(out_loop.trm_nbr AS INTEGER)
                                                                  /* -HERE */
                                                  left outer join
                                                                  (
                                                                           SELECT   quotn_id ,
                                                                                    nk_job_nbr ,
                                                                                    vers_nbr
                                                                           FROM     db_t_prod_core.insrnc_quotn
                                                                           WHERE    nk_job_nbr IN
                                                                                                   (
                                                                                                   SELECT DISTINCT nk_job_nbr
                                                                                                   FROM            out_loop) qualify row_number() over(PARTITION BY nk_job_nbr, vers_nbr, src_sys_cd ORDER BY edw_end_dttm DESC) = 1 ) ins_quotn
                                                  ON              ins_quotn.nk_job_nbr=out_loop.nk_job_nbr
                                                  AND             ins_quotn.vers_nbr=out_loop.vers_nbr ) AS src
                  left outer join
                                  (
                                           SELECT   fincl_ev_amt_dttm ,
                                                    ev_trans_amt,
                                                    agmt_id ,
                                                    quotn_id ,
                                                    edw_strt_dttm ,
                                                    edw_end_dttm ,
                                                    ev_id ,
                                                    fincl_ev_amt_ctgy_type_cd ,
                                                    tacct_type_cd,
                                                    fincl_ev_amt_type_cd
                                           FROM     db_t_prod_core.fincl_ev_amt
                                           WHERE    ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   out_loop)) qualify row_number() over(PARTITION BY ev_id, fincl_ev_amt_type_cd,tacct_type_cd,fincl_ev_amt_ctgy_type_cd ORDER BY edw_end_dttm DESC) = 1 ) AS fin_ev_amt_np
                  ON              fin_ev_amt_np.ev_id= src.ev_id_nonpolicy
                  AND             trim(fin_ev_amt_np.fincl_ev_amt_type_cd)=trim( src.fincl_ev_amt_type_cd )
                  AND             trim(fin_ev_amt_np.fincl_ev_amt_ctgy_type_cd)=trim(src.fincl_ev_amt_ctgy_type_cd)
                  AND             (
                                                  trim( fin_ev_amt_np.tacct_type_cd))= trim(src.tacct_type_cd)
                  left outer join
                                  (
                                           SELECT   ev_id ,
                                                    tacct_type_cd ,
                                                    fincl_ev_amt_ctgy_type_cd,
                                                    fincl_ev_amt_type_cd ,
                                                    fincl_ev_amt_dttm ,
                                                    ev_trans_amt
                                           FROM     db_t_prod_core.fincl_ev_amt
                                           WHERE    ev_id IN
                                                              (
                                                              SELECT DISTINCT ev_id
                                                              FROM            db_t_prod_core.ev
                                                              WHERE           ev.src_trans_id IN
                                                                              (
                                                                                     SELECT key1
                                                                                     FROM   out_loop)) qualify row_number() over(PARTITION BY ev_id, fincl_ev_amt_type_cd,tacct_type_cd, fincl_ev_amt_ctgy_type_cd,ev_trans_amt ORDER BY edw_end_dttm DESC) = 1 ) AS ev_id_dup_chk
                  ON              ev_id_dup_chk.ev_id= src.ev_id
                  AND             (
                                                  trim( ev_id_dup_chk.tacct_type_cd))= (trim(src.tacct_type_cd))
                  AND             trim(ev_id_dup_chk.fincl_ev_amt_ctgy_type_cd)=trim(src.fincl_ev_amt_ctgy_type_cd)
                  AND             trim(ev_id_dup_chk.fincl_ev_amt_type_cd)= trim(src.fincl_ev_amt_type_cd)
                  AND             cast(to_char(ev_id_dup_chk.fincl_ev_amt_dttm , ''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30)) = cast(to_char(src.fincl_ev_amt_dttm ,''YYYY-MM-DD HH24:MI:SS'') AS VARCHAR(30))
                  AND             ev_id_dup_chk.ev_trans_amt=src.amt
                  WHERE           var_orig_chksm IS NULL
                  OR              var_orig_chksm<>var_calc_chksm ) src ) );
  -- Component exp_check_flag1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag1 AS
  (
         SELECT sq_bc_invoice_billing_transaction.ev_id                                AS ev_id,
                sq_bc_invoice_billing_transaction.fincl_ev_amt_type_code               AS fincl_ev_amt_type_cd,
                sq_bc_invoice_billing_transaction.busn_dt                              AS fincl_ev_amt_dttm,
                sq_bc_invoice_billing_transaction.amount                               AS ev_trans_amt,
                :prcs_id                                                               AS out_prcs_id,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                sq_bc_invoice_billing_transaction.amt_ctgy_type_code                   AS amt_ctgy_typecode,
                sq_bc_invoice_billing_transaction.retired                              AS retired,
                sq_bc_invoice_billing_transaction.trm_nbr                              AS trm_nbr,
                sq_bc_invoice_billing_transaction.rnk                                  AS rnk,
                sq_bc_invoice_billing_transaction.validquote                           AS validquote,
                sq_bc_invoice_billing_transaction.agmt_id                              AS agmt_id,
                sq_bc_invoice_billing_transaction.quotn_id                             AS quotn_id,
                sq_bc_invoice_billing_transaction.taccount_typecode                    AS tacct_type_cd,
                sq_bc_invoice_billing_transaction.eff_dt                               AS trans_strt_dttm,
                sq_bc_invoice_billing_transaction.tgt_ev_id_np                         AS lkp_ev_id_np,
                sq_bc_invoice_billing_transaction.tgt_fincl_ev_amt_type_cd_np          AS lkp_fincl_ev_amt_type_cd_np,
                sq_bc_invoice_billing_transaction.tgt_fincl_ev_amt_dttm_np             AS lkp_fincl_ev_amt_dttm_np,
                sq_bc_invoice_billing_transaction.tgt_ev_trans_amt_np                  AS lkp_ev_trans_amt_np,
                sq_bc_invoice_billing_transaction.tgt_edw_strt_dttm_np                 AS lkp_edw_strt_dttm_np,
                sq_bc_invoice_billing_transaction.tgt_edw_end_dttm_np                  AS lkp_edw_end_dttm_np,
                sq_bc_invoice_billing_transaction.edw_strt_dttm                        AS edw_strt_dttm,
                CASE
                       WHEN sq_bc_invoice_billing_transaction.var_orig_chksm IS NULL THEN ''I''
                       ELSE
                              CASE
                                     WHEN sq_bc_invoice_billing_transaction.var_orig_chksm != sq_bc_invoice_billing_transaction.var_calc_chksm
                                     AND    sq_bc_invoice_billing_transaction.ev_id_dup_chk IS NULL THEN ''U''
                                     ELSE ''R''
                              END
                END AS out_ins_upd,
                sq_bc_invoice_billing_transaction.source_record_id
         FROM   sq_bc_invoice_billing_transaction );
  -- Component rtr_fincl_ev_amt_insupd1_INSERT, Type ROUTER Output Group INSERT
  CREATE OR REPLACE TEMPORARY TABLE rtr_fincl_ev_amt_insupd1_INSERT AS (
  SELECT exp_check_flag1.ev_id                       AS ev_id,
         exp_check_flag1.fincl_ev_amt_type_cd        AS fincl_ev_amt_type_cd,
         exp_check_flag1.fincl_ev_amt_dttm           AS fincl_ev_amt_dttm,
         exp_check_flag1.ev_trans_amt                AS ev_trans_amt,
         exp_check_flag1.out_prcs_id                 AS prcs_id,
         exp_check_flag1.out_edw_end_dttm            AS out_edw_end_dttm,
         exp_check_flag1.amt_ctgy_typecode           AS amt_ctgy_typecode,
         exp_check_flag1.retired                     AS retired,
         exp_check_flag1.trm_nbr                     AS trm_nbr,
         exp_check_flag1.rnk                         AS rnk,
         exp_check_flag1.validquote                  AS validquote,
         exp_check_flag1.agmt_id                     AS agmt_id,
         exp_check_flag1.quotn_id                    AS quotn_id,
         exp_check_flag1.tacct_type_cd               AS tgt_idntftn_val,
         exp_check_flag1.trans_strt_dttm             AS trans_strt_dttm,
         exp_check_flag1.lkp_ev_id_np                AS lkp_fincl_ev_amt_ev_id,
         exp_check_flag1.lkp_fincl_ev_amt_type_cd_np AS lkp_fincl_ev_amt_type_cd,
         exp_check_flag1.lkp_fincl_ev_amt_dttm_np    AS lkp_fincl_ev_amt_dttm,
         exp_check_flag1.lkp_ev_trans_amt_np         AS lkp_ev_trans_amt,
         exp_check_flag1.lkp_edw_strt_dttm_np        AS lkp_edw_strt_dttm,
         exp_check_flag1.lkp_edw_end_dttm_np         AS lkp_edw_end_dttm,
         exp_check_flag1.edw_strt_dttm               AS edw_strt_dttm,
         exp_check_flag1.out_ins_upd                 AS out_ins_upd,
         exp_check_flag1.source_record_id
  FROM   exp_check_flag1
  WHERE  exp_check_flag1.out_ins_upd = ''I''
  AND    exp_check_flag1.ev_id IS NOT NULL
  OR     exp_check_flag1.out_ins_upd = ''U''
  OR     (
                exp_check_flag1.retired = 0
         AND    exp_check_flag1.lkp_edw_end_dttm_np != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
  );
  
  -- Component rtr_fincl_ev_amt_insupd1_RETIRED, Type ROUTER Output Group RETIRED
  CREATE OR REPLACE TEMPORARY TABLE rtr_fincl_ev_amt_insupd1_RETIRED AS (
  SELECT exp_check_flag1.ev_id                       AS ev_id,
         exp_check_flag1.fincl_ev_amt_type_cd        AS fincl_ev_amt_type_cd,
         exp_check_flag1.fincl_ev_amt_dttm           AS fincl_ev_amt_dttm,
         exp_check_flag1.ev_trans_amt                AS ev_trans_amt,
         exp_check_flag1.out_prcs_id                 AS prcs_id,
         exp_check_flag1.out_edw_end_dttm            AS out_edw_end_dttm,
         exp_check_flag1.amt_ctgy_typecode           AS amt_ctgy_typecode,
         exp_check_flag1.retired                     AS retired,
         exp_check_flag1.trm_nbr                     AS trm_nbr,
         exp_check_flag1.rnk                         AS rnk,
         exp_check_flag1.validquote                  AS validquote,
         exp_check_flag1.agmt_id                     AS agmt_id,
         exp_check_flag1.quotn_id                    AS quotn_id,
         exp_check_flag1.tacct_type_cd               AS tgt_idntftn_val,
         exp_check_flag1.trans_strt_dttm             AS trans_strt_dttm,
         exp_check_flag1.lkp_ev_id_np                AS lkp_fincl_ev_amt_ev_id,
         exp_check_flag1.lkp_fincl_ev_amt_type_cd_np AS lkp_fincl_ev_amt_type_cd,
         exp_check_flag1.lkp_fincl_ev_amt_dttm_np    AS lkp_fincl_ev_amt_dttm,
         exp_check_flag1.lkp_ev_trans_amt_np         AS lkp_ev_trans_amt,
         exp_check_flag1.lkp_edw_strt_dttm_np        AS lkp_edw_strt_dttm,
         exp_check_flag1.lkp_edw_end_dttm_np         AS lkp_edw_end_dttm,
         exp_check_flag1.edw_strt_dttm               AS edw_strt_dttm,
         exp_check_flag1.out_ins_upd                 AS out_ins_upd,
         exp_check_flag1.source_record_id
  FROM   exp_check_flag1
  WHERE  exp_check_flag1.out_ins_upd = ''R''
  AND    exp_check_flag1.retired != 0
  AND    exp_check_flag1.lkp_edw_end_dttm_np = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) 
  -- > NOT INSERT OR UPDATE , no CHANGE IN VALUES 
  -- > but data IS retired 
  -- > UPDATE these records WITH current_timestamp
  );
  
  -- Component upd_fincl_ev_amt_upd_Retire_rejected1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_fincl_ev_amt_upd_retire_rejected1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_amt_insupd1_retired.lkp_fincl_ev_amt_ev_id   AS ev_id,
                rtr_fincl_ev_amt_insupd1_retired.lkp_fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                rtr_fincl_ev_amt_insupd1_retired.lkp_fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                rtr_fincl_ev_amt_insupd1_retired.lkp_ev_trans_amt         AS ev_trans_amt,
                rtr_fincl_ev_amt_insupd1_retired.prcs_id                  AS prcs_id,
                rtr_fincl_ev_amt_insupd1_retired.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_fincl_ev_amt_insupd1_retired.edw_strt_dttm            AS edw_strt_dttm3,
                rtr_fincl_ev_amt_insupd1_retired.source_record_id,
                1                                                 AS update_strategy_action
         FROM   rtr_fincl_ev_amt_insupd1_retired );
  -- Component upd_fincl_ev_ins1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_fincl_ev_ins1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fincl_ev_amt_insupd1_insert.ev_id                AS ev_id,
                rtr_fincl_ev_amt_insupd1_insert.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                rtr_fincl_ev_amt_insupd1_insert.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                rtr_fincl_ev_amt_insupd1_insert.ev_trans_amt         AS ev_trans_amt,
                rtr_fincl_ev_amt_insupd1_insert.prcs_id              AS prcs_id,
                rtr_fincl_ev_amt_insupd1_insert.edw_strt_dttm        AS edw_strt_dttm1,
                rtr_fincl_ev_amt_insupd1_insert.out_edw_end_dttm     AS out_edw_end_dttm1,
                rtr_fincl_ev_amt_insupd1_insert.retired              AS retired1,
                rtr_fincl_ev_amt_insupd1_insert.amt_ctgy_typecode    AS o_amt_ctgy_typecode1,
                rtr_fincl_ev_amt_insupd1_insert.rnk                  AS rnk1,
                rtr_fincl_ev_amt_insupd1_insert.validquote           AS validquote1,
                rtr_fincl_ev_amt_insupd1_insert.agmt_id              AS agmt_id1,
                rtr_fincl_ev_amt_insupd1_insert.quotn_id             AS quotn_id1,
                rtr_fincl_ev_amt_insupd1_insert.tgt_idntftn_val      AS tgt_idntftn_val,
                rtr_fincl_ev_amt_insupd1_insert.trans_strt_dttm      AS trans_strt_dttm1,
                rtr_fincl_ev_amt_insupd1_insert.source_record_id,
                0                                                    AS update_strategy_action
         FROM   rtr_fincl_ev_amt_insupd1_insert );
  -- Component exp_pass_to_target_upd_Retire_rejected1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd_retire_rejected1 AS
  (
         SELECT upd_fincl_ev_amt_upd_retire_rejected1.ev_id                AS ev_id,
                upd_fincl_ev_amt_upd_retire_rejected1.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
                upd_fincl_ev_amt_upd_retire_rejected1.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
                upd_fincl_ev_amt_upd_retire_rejected1.ev_trans_amt         AS ev_trans_amt,
                upd_fincl_ev_amt_upd_retire_rejected1.lkp_edw_strt_dttm3   AS lkp_edw_strt_dttm3,
                upd_fincl_ev_amt_upd_retire_rejected1.edw_strt_dttm3       AS o_edw_end_dttm3,
                upd_fincl_ev_amt_upd_retire_rejected1.source_record_id
         FROM   upd_fincl_ev_amt_upd_retire_rejected1 );
  -- Component exp_pass_to_target_ins1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins1 AS
  (
         SELECT upd_fincl_ev_ins1.ev_id                                                                 AS ev_id,
                upd_fincl_ev_ins1.fincl_ev_amt_type_cd                                                  AS fincl_ev_amt_type_cd,
                upd_fincl_ev_ins1.fincl_ev_amt_dttm                                                     AS fincl_ev_amt_dttm,
                upd_fincl_ev_ins1.ev_trans_amt                                                          AS ev_trans_amt,
                upd_fincl_ev_ins1.prcs_id                                                               AS prcs_id,
                upd_fincl_ev_ins1.o_amt_ctgy_typecode1                                                  AS o_amt_ctgy_typecode1,
                dateadd(''second'', ( 2 * ( upd_fincl_ev_ins1.rnk1 - 1 ) ), upd_fincl_ev_ins1.edw_strt_dttm1) AS vt_edw_strt_dttm1,
                vt_edw_strt_dttm1                                                                       AS o_edw_strt_dttm,
                CASE
                       WHEN upd_fincl_ev_ins1.retired1 != 0
                       OR     upd_fincl_ev_ins1.validquote1 = 0 THEN dateadd(''second'', 1, vt_edw_strt_dttm1)
                       ELSE upd_fincl_ev_ins1.out_edw_end_dttm1
                END                                AS o_edw_end_dttm,
                upd_fincl_ev_ins1.agmt_id1         AS agmt_id1,
                upd_fincl_ev_ins1.quotn_id1        AS quotn_id1,
                upd_fincl_ev_ins1.tgt_idntftn_val  AS tgt_idntftn_val,
                upd_fincl_ev_ins1.trans_strt_dttm1 AS trans_strt_dttm1,
                CASE
                       WHEN upd_fincl_ev_ins1.retired1 != 0
                       OR     upd_fincl_ev_ins1.validquote1 = 0 THEN dateadd(''second'', 1, upd_fincl_ev_ins1.trans_strt_dttm1)
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trans_end_dttm1,
                upd_fincl_ev_ins1.source_record_id
         FROM   upd_fincl_ev_ins1 );
  -- Component tgt_fincl_ev_amt_upd_Retire_rejected1, Type TARGET
  merge
  INTO         db_t_prod_core.fincl_ev_amt
  USING        exp_pass_to_target_upd_retire_rejected1
  ON (fincl_ev_amt.ev_id = exp_pass_to_target_upd_retire_rejected1.ev_id)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_pass_to_target_upd_retire_rejected1.ev_id,
         fincl_ev_amt_type_cd = exp_pass_to_target_upd_retire_rejected1.fincl_ev_amt_type_cd,
         fincl_ev_amt_dttm = exp_pass_to_target_upd_retire_rejected1.fincl_ev_amt_dttm,
         ev_trans_amt = exp_pass_to_target_upd_retire_rejected1.ev_trans_amt,
         edw_strt_dttm = exp_pass_to_target_upd_retire_rejected1.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd_retire_rejected1.o_edw_end_dttm3,
         trans_end_dttm = exp_pass_to_target_upd_retire_rejected1.o_edw_end_dttm3;
  
  -- Component tgt_fincl_ev_amt_ins1, Type TARGET
  INSERT INTO db_t_prod_core.fincl_ev_amt
              (
                          ev_id,
                          fincl_ev_amt_type_cd,
                          fincl_ev_amt_dttm,
                          ev_trans_amt,
                          fincl_ev_amt_ctgy_type_cd,
                          agmt_id,
                          quotn_id,
                          tacct_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins1.ev_id                AS ev_id,
         exp_pass_to_target_ins1.fincl_ev_amt_type_cd AS fincl_ev_amt_type_cd,
         exp_pass_to_target_ins1.fincl_ev_amt_dttm    AS fincl_ev_amt_dttm,
         exp_pass_to_target_ins1.ev_trans_amt         AS ev_trans_amt,
         exp_pass_to_target_ins1.o_amt_ctgy_typecode1 AS fincl_ev_amt_ctgy_type_cd,
         exp_pass_to_target_ins1.agmt_id1             AS agmt_id,
         exp_pass_to_target_ins1.quotn_id1            AS quotn_id,
         exp_pass_to_target_ins1.tgt_idntftn_val      AS tacct_type_cd,
         exp_pass_to_target_ins1.prcs_id              AS prcs_id,
         exp_pass_to_target_ins1.o_edw_strt_dttm      AS edw_strt_dttm,
         exp_pass_to_target_ins1.o_edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_target_ins1.trans_strt_dttm1     AS trans_strt_dttm,
         exp_pass_to_target_ins1.out_trans_end_dttm1  AS trans_end_dttm
  FROM   exp_pass_to_target_ins1;
  
  -- PIPELINE END FOR 2
  -- Component tgt_fincl_ev_amt_upd_Retire_rejected1, Type Post SQL
  UPDATE db_t_prod_core.fincl_ev_amt
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_id,
                                         fincl_ev_amt_type_cd,
                                         nvl(quotn_id,9999)                   AS quotn_id,
                                         nvl(fincl_ev_amt_ctgy_type_cd,''UNK'') AS fincl_ev_amt_ctgy_type_cd ,
                                         nvl(tacct_type_cd,''UNK'')             AS tacct_type_cd ,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ev_id,fincl_ev_amt_type_cd, nvl(quotn_id,9999),nvl(fincl_ev_amt_ctgy_type_cd,''UNK''),nvl(tacct_type_cd,''UNK'') ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY ev_id,fincl_ev_amt_type_cd, nvl(quotn_id,9999),nvl(fincl_ev_amt_ctgy_type_cd,''UNK''),nvl(tacct_type_cd,''UNK'') ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.fincl_ev_amt qualify lead IS NOT NULL ) a

  WHERE  fincl_ev_amt.edw_strt_dttm = a.edw_strt_dttm
         --and EV_ID in (select distinct EV_ID from EV where EV_SBTYPE_CD=''PLCYTRNS'')
  AND    fincl_ev_amt.ev_id=a.ev_id
  AND    fincl_ev_amt.fincl_ev_amt_type_cd=a.fincl_ev_amt_type_cd
  AND    nvl(fincl_ev_amt.quotn_id,9999)=nvl(a.quotn_id,9999)
  AND    nvl(fincl_ev_amt.fincl_ev_amt_ctgy_type_cd,''UNK'')=nvl(a.fincl_ev_amt_ctgy_type_cd,''UNK'')
  AND    nvl(fincl_ev_amt.tacct_type_cd,''UNK'')=nvl(a.tacct_type_cd,''UNK'')
  AND    fincl_ev_amt.trans_strt_dttm <>fincl_ev_amt.trans_end_dttm
  AND    lead IS NOT NULL;

END;
';