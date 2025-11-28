-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINCL_EV_BILTRANS_GL_INSUPD("WORKLET_NAME" VARCHAR)
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
                                                                  row_number () over (PARTITION BY ev_act_type_code, key1, SUBTYPE ORDER BY fincl_ev_prd_strt_dt , accountingdate DESC,financl_ev_type ,eff_dt ,refnumber,trans_host_num , funccd, inv_invoicenumber, invitem_id, chargecategorycode ,tranfer_method_typ ,ev_strt_dt ,ev_end_dt ,glmonth ,glyear ) AS rnk
                                                  FROM            (
                                                                                  /***************************Billing Transaction*************************/
                                                                                  SELECT DISTINCT cast(bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_transaction.id_stg AS         VARCHAR(50)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                       AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                   AS financl_ev_type ,
                                                                                                  bc_transaction.transactiondate_stg                 AS eff_dt ,
                                                                                                  bc_transaction.transactionnumber_stg               AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                            AS trans_host_num
                                                                                                  /* BIL_DA_0458 */
                                                                                                  ,
                                                                                                  ''BILL''                                                                                  AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                                                            AS inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                                                                   AS invitem_id ,
                                                                                                  bctl_chargecategory.typecode_stg                                                        AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                               AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                                                 AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                                                 AS ev_end_dt ,
                                                                                                  glb.gl_extr_mo_stg                                                                      AS glmonth ,
                                                                                                  glb.gl_extr_yr_stg                                                                      AS glyear ,
                                                                                                  glb.accountingdate_stg                                                                  AS accountingdate ,
                                                                                                  bc_transaction.createtime_stg                                                           AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_transaction.retired_stg                                                              AS retired ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                               AS ev_med_type_cd ,
                                                                                                  cast(NULL AS VARCHAR(255))                                                              AS funds_tfr_type_cd ,
                                                                                                  coalesce(bc_invoiceitem.updatetime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS updatetime
                                                                                  FROM            db_t_prod_stag.gl_eventstaging_bc_monthly                                               AS glb
                                                                                  inner join      db_t_prod_stag.bc_transaction
                                                                                  ON              glb.publicid_stg = bc_transaction.publicid_stg
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg = bc_transaction.subtype_stg
                                                                                  AND             glb.rootentity_stg = bctl_transaction.typecode_stg
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
                                                                                  WHERE           glb.gl_extr_yr_stg = cast(:GL_END_MTH_ID/100 AS INTEGER)
                                                                                  AND             glb.gl_extr_mo_stg = mod(:GL_END_MTH_ID , 100)
                                                                                  /* ------------------------------------------------------------------------------------------------------ */
                                                                                  UNION ALL
                                                                                  SELECT DISTINCT cast(''rvrs''
                                                                                                                  || ''-''
                                                                                                                  || bctl_transaction.typecode_stg AS VARCHAR(60)) AS ev_act_type_code ,
                                                                                                  cast(bc_transaction.id_stg AS                       VARCHAR(50)) AS key1 ,
                                                                                                  ''EV_SBTYPE2''                                                     AS SUBTYPE ,
                                                                                                  ''FINCL_EV_TYPE3''                                                 AS financl_ev_type ,
                                                                                                  bc_transaction.transactiondate_stg                               AS eff_dt ,
                                                                                                  bc_transaction.transactionnumber_stg                             AS refnumber ,
                                                                                                  cast('''' AS VARCHAR(60))                                          AS trans_host_num
                                                                                                  /* BIL_DA_0458 */
                                                                                                  ,
                                                                                                  ''BILL''                                                                                  AS funccd ,
                                                                                                  bc_invoice.invoicenumber_stg                                                            AS inv_invoicenumber ,
                                                                                                  bc_invoiceitem.id_stg                                                                   AS invitem_id ,
                                                                                                  bctl_chargecategory.typecode_stg                                                        AS chargecategorycode ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                               AS tranfer_method_typ ,
                                                                                                  cast(NULL AS timestamp)                                                                 AS ev_strt_dt ,
                                                                                                  cast(NULL AS timestamp)                                                                 AS ev_end_dt ,
                                                                                                  glb.gl_extr_mo_stg                                                                      AS glmonth ,
                                                                                                  glb.gl_extr_yr_stg                                                                      AS glyear ,
                                                                                                  glb.accountingdate_stg                                                                  AS accountingdate ,
                                                                                                  bc_transaction.createtime_stg                                                           AS fincl_ev_prd_strt_dt ,
                                                                                                  bc_transaction.retired_stg                                                              AS retired ,
                                                                                                  cast(NULL AS VARCHAR(60))                                                               AS ev_med_type_cd ,
                                                                                                  cast(NULL AS VARCHAR(255))                                                              AS funds_tfr_type_cd ,
                                                                                                  coalesce(bc_invoiceitem.updatetime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS updatetime
                                                                                  FROM            db_t_prod_stag.gl_eventstaging_bc_monthly glb
                                                                                  inner join      db_t_prod_stag.bc_transaction
                                                                                  ON              glb.publicid_stg = bc_transaction.publicid_stg
                                                                                  inner join      db_t_prod_stag.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg = bc_transaction.subtype_stg
                                                                                  AND             glb.rootentity_stg = bctl_transaction.typecode_stg
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
                                                                                  WHERE           glb.gl_extr_yr_stg = cast(:GL_END_MTH_ID/100 AS INTEGER)
                                                                                  AND             glb.gl_extr_mo_stg = mod(:GL_END_MTH_ID , 100)
                                                                                  AND             bc_revtrans.ownerid_stg IS NOT NULL ) src_fncl_ev )
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
                                  glmonth                 AS gl_mth_num_new,
                                  glyear                  AS gl_yr_num_new,
                                  xlat_src.accntg_dy_num  AS accntg_dy_num_new,
                                  xlat_src.accntg_mth_num AS accntg_mth_num_new,
                                  xlat_src.accntg_yr_num  AS accntg_yr_num_new,
                                  /*case when  tgt_fincl_ev.GL_MTH_NUM is not null then  tgt_fincl_ev.GL_MTH_NUM else  GLMonth end  as GL_MTH_NUM_New,
case when  tgt_fincl_ev.GL_YR_NUM is not null  then  tgt_fincl_ev.GL_YR_NUM ELSE GLYear  end as GL_YR_NUM_New,
CASE WHEN tgt_fincl_ev.ACCNTG_DY_NUM IS NOT NULL THEN tgt_fincl_ev.ACCNTG_DY_NUM ELSE  xlat_src.ACCNTG_DY_NUM END AS ACCNTG_DY_NUM_NEW,
CASE WHEN tgt_fincl_ev.ACCNTG_MTH_NUM IS NOT NULL THEN tgt_fincl_ev.ACCNTG_MTH_NUM ELSE  xlat_src.ACCNTG_MTH_NUM END AS ACCNTG_MTH_NUM_NEW,
CASE WHEN tgt_fincl_ev.ACCNTG_YR_NUM IS NOT NULL THEN tgt_fincl_ev.ACCNTG_YR_NUM ELSE  xlat_src.ACCNTG_YR_NUM END AS ACCNTG_YR_NUM_NEW,*/
                                  xlat_src.fincl_ev_prd_strt_dt ,
                                  xlat_src.fincl_ev_prd_end_dt,
                                  retired,
                                  xlat_src.ev_med_type_cd,
                                  xlat_src.funds_tfr_type_cd,
                                  rnk,
                                  tgt_fincl_ev.edw_strt_dttm,
                                  tgt_fincl_ev.edw_end_dttm,
                                  cast( (xlat_src.financl_ev_type_cd
                                                  || cast(to_char(eff_date , ''YYYYMMDD'') AS VARCHAR(10))
                                                  || trim( coalesce(refnumber,0))
                                                  || to_char(coalesce(doc_id,0))
                                                  || trim(coalesce(xlat_src.ar_invc_ln_num,0))
                                                  || trim(coalesce( xlat_src.ar_invc_ln_amt_type_cd,0))
                                                  || trim( xlat_src.funds_tfr_meth_type_cd)
                                                  || cast(coalesce(gl_mth_num_new,0) AS     INTEGER)
                                                  || cast(coalesce( gl_yr_num_new,0) AS     INTEGER)
                                                  || cast(coalesce(accntg_dy_num_new,0) AS  INTEGER)
                                                  || cast(coalesce(accntg_mth_num_new,0) AS INTEGER)
                                                  || cast(coalesce(accntg_yr_num_new,0) AS  INTEGER)
                                                  || cast(xlat_src.fincl_ev_prd_strt_dt AS  VARCHAR(30)) ) AS VARCHAR(1100)) AS sourcedata,
                                  cast( (tgt_fincl_ev.fincl_ev_type_cd
                                                  || cast(to_char(tgt_fincl_ev.funds_tfr_eff_dttm , ''YYYYMMDD'') AS VARCHAR(10))
                                                  || trim(coalesce(tgt_fincl_ev.funds_tfr_ref_num,0))
                                                  || to_char(coalesce(tgt_fincl_ev.ar_invc_id,0))
                                                  || trim(coalesce(tgt_fincl_ev.ar_invc_ln_num,0))
                                                  || trim(coalesce(tgt_fincl_ev.ar_invc_ln_amt_type_cd,0))
                                                  || trim(coalesce(tgt_fincl_ev.funds_tfr_meth_type_cd,0))
                                                  || coalesce(cast(trim(tgt_fincl_ev.gl_mth_num) AS INTEGER),0)
                                                  || cast(coalesce(tgt_fincl_ev.gl_yr_num,0) AS      INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_dy_num,0) AS  INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_mth_num,0) AS INTEGER)
                                                  || cast(coalesce(tgt_fincl_ev.accntg_yr_num,0) AS  INTEGER)
                                                  || cast(tgt_fincl_ev.fincl_ev_prd_strt_dttm AS     VARCHAR(30))) AS VARCHAR(1100)) AS targetdata,
                                  CASE
                                                  WHEN tgt_fincl_ev.ev_id IS NULL THEN ''I''
                                                  WHEN tgt_fincl_ev.ev_id IS NOT NULL
                                                  AND             sourcedata <> targetdata THEN ''U''
                                                  WHEN tgt_fincl_ev.ev_id IS NOT NULL
                                                  AND             sourcedata = targetdata THEN ''R''
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
                                                                  to_timestamp_ntz(''9999-12-31 23:59:59.999999'' ,''YYYY-MM-DD HH24:MI:SS.S(6)'' ) AS fincl_ev_prd_end_dt,
                                                                  retired,
                                                                  ev_med_type_cd,
                                                                  funds_tfr_type_cd,
                                                                  rnk
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
                                                                           SELECT   ev_id ,
                                                                                    src_trans_id ,
                                                                                    ev_sbtype_cd ,
                                                                                    ev_actvy_type_cd
                                                                           FROM     db_t_prod_core.ev
                                                                           WHERE    src_trans_id IN
                                                                                                     (
                                                                                                     SELECT DISTINCT key1
                                                                                                     FROM            intrm_fncl_ev) qualify row_number() over(PARTITION BY ev_sbtype_cd,ev_actvy_type_cd,src_trans_id ORDER BY edw_end_dttm DESC) = 1 ) AS evt
                                                  ON              evt.src_trans_id = src.key1
                                                  AND             evt.ev_actvy_type_cd = xlat_act_type_cd.tgt_idntftn_val
                                                  AND             evt.ev_sbtype_cd = xlat_ev_sbtype.tgt_idntftn_val
                                                  left outer join
                                                                  (
                                                                           SELECT   ar_invc_ln_num ,
                                                                                    ar_invc_id ,
                                                                                    host_invc_ln_num
                                                                           FROM     db_t_prod_core.ar_invc_ln
                                                                           WHERE    host_invc_ln_num IN
                                                                                                         (
                                                                                                         SELECT DISTINCT cast(invitem_id AS VARCHAR(50))
                                                                                                         FROM            intrm_fncl_ev) qualify row_number() over(PARTITION BY host_invc_ln_num ORDER BY edw_end_dttm DESC) = 1 ) AS ar_il
                                                  ON              cast(ar_il.host_invc_ln_num AS VARCHAR(20)) = cast(src.invitem_id AS VARCHAR(20))
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
                                                                                                      FROM            intrm_fncl_ev) qualify row_number () over (PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS dc
                                                  ON              dc.doc_issur_num = src.inv_invoicenumber
                                                  AND             dc.doc_type_cd = xlat_doc_type.tgt_idntftn_val
                                                  AND             dc.doc_ctgy_type_cd = xlat_doc_ctgy_type.tgt_idntftn_val ) AS xlat_src
                  left outer join
                                  (
                                         SELECT fincl_ev_prd_strt_dttm ,
                                                fincl_ev_type_cd ,
                                                funds_tfr_host_num ,
                                                funds_tfr_eff_dttm ,
                                                funds_tfr_ref_num ,
                                                funds_tfr_meth_type_cd ,
                                                ar_invc_id ,
                                                ar_invc_ln_num ,
                                                ar_invc_ln_amt_type_cd ,
                                                gl_mth_num ,
                                                gl_yr_num ,
                                                accntg_dy_num ,
                                                accntg_mth_num ,
                                                accntg_yr_num ,
                                                edw_strt_dttm ,
                                                edw_end_dttm ,
                                                ev_med_type_cd ,
                                                funds_tfr_type_cd ,
                                                ev_id
                                         FROM   db_t_prod_core.fincl_ev
                                         WHERE  ev_id IN
                                                          (
                                                          SELECT DISTINCT ev_id
                                                          FROM            db_t_prod_core.ev
                                                          WHERE           ev.src_trans_id IN
                                                                          (
                                                                                 SELECT key1
                                                                                 FROM   intrm_fncl_ev))
                                         AND    edw_end_dttm = ''9999-12-31 23:59:59.999999'' ) AS tgt_fincl_ev
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
  create or replace temporary table rtr_fincl_ev_insupd_grp_insert as
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
  create or replace temporary table rtr_fincl_ev_insupd_retired as
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
  UPDATE these records WITH current_timestamp
  */
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
				rtr_fincl_ev_insupd_grp_insert.source_record_id
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