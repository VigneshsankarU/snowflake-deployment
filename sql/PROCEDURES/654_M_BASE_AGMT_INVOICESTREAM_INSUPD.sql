-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_INVOICESTREAM_INSUPD("WORKLET_NAME" VARCHAR)
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
BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');


  -- Component LKP_TERADATA_ETL_REF_STMT_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_stmt_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CYCL_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_billingperiodicity.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_BILLING_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_billing_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''BILG_METH_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_policyperiodbillingmethod.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_DATA_SRC, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_data_src AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DATA_SRC_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_STMT_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_stmt_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''STMT_ML_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_invoicedeliverymethod.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_STREAM_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_stream_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INVC_STREM_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_billinglevel.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_XMITL_MODE_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_xmitl_mode_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''XMITL_MODE_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_paymentmethod.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_XREF_AGMNT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_agmnt AS
  (
         SELECT dir_agmt.agmt_id                    AS agmt_id,
                ltrim(rtrim(dir_agmt.nk_src_key))   AS nk_src_key,
                dir_agmt.term_num                   AS term_num,
                ltrim(rtrim(dir_agmt.agmt_type_cd)) AS agmt_type_cd
         FROM   db_t_prod_core.dir_agmt
         WHERE  agmt_type_cd=''INV'' );
  -- Component SQ_agmt_invoicestream_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_agmt_invoicestream_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS billingreferencenumber_alfa,
                $2  AS invoice_stream_type_cd,
                $3  AS statement_mail_type_cd,
                $4  AS transmittal_mode_type_cd,
                $5  AS accountnumber,
                $6  AS billing_method_type_cd,
                $7  AS stmt_cycl_cd,
                $8  AS eventdate,
                $9  AS paymentduedate,
                $10 AS retired_invoicestream,
                $11 AS updatetime,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT a.billingreferencenumber_alfa,
                                                                  a.invoice_stream_type_cd,
                                                                  a.statement_mail_type_cd,
                                                                  a.transmittal_mode_type_cd,
                                                                  a.accountnumber,
                                                                  a.billing_method_type_cd,
                                                                  a.stmt_cycl_cd,
                                                                  a.eventdate,
                                                                  to_date (''1990-01-01'' ,''yyyy-mm-dd'') plnd_exprtn_dt,
                                                                  a.retired_invoicestream,
                                                                  a.inv_trans_strt_dttm
                                                  FROM            (
                                                                                  SELECT DISTINCT x.billingreferencenumber_alfa AS billingreferencenumber_alfa,
                                                                                                  x.invoice_stream_type_cd      AS invoice_stream_type_cd,
                                                                                                  x.statement_mail_type_cd      AS statement_mail_type_cd,
                                                                                                  CASE
                                                                                                                  WHEN x.transmittal_mode_type_cd IS NULL THEN ''responsive''
                                                                                                                  ELSE x.transmittal_mode_type_cd
                                                                                                  END AS transmittal_mode_type_cd,
                                                                                                  x.policynumber,
                                                                                                  x.accountnumber,
                                                                                                  x.billing_meth_type_cd AS billing_method_type_cd,
                                                                                                  max(x.updatetime)      AS updatetime,
                                                                                                  x.agmt_rltd_rsn_cd,
                                                                                                  x.typecode AS stmt_cycl_cd ,
                                                                                                  x.termnumber,
                                                                                                  min(x.eventdate)      AS eventdate,
                                                                                                  max(x.paymentduedate) AS paymentduedate,
                                                                                                  x.retired_invoicestream,
                                                                                                  x.retired_account,
                                                                                                  x.retired_policyperiod,
                                                                                                  (:START_DTTM)              AS start_dttm,
                                                                                                  (:END_DTTM)                AS end_dttm,
                                                                                                  max(x.updatetime_agmtrltd) AS updatetime_agmtrltd,
                                                                                                  max(x.inv_trans_strt_dttm) AS inv_trans_strt_dttm
                                                                                  FROM            ( select DISTINCT bc_invoicestream_stg.billingreferencenumber_alfa_stg AS billingreferencenumber_alfa, bctl_billinglevel_stg.typecode_stg AS invoice_stream_type_cd, bctl_invoicedeliverymethod_stg.typecode_stg AS statement_mail_type_cd , bctl_paymentmethod_stg.typecode_stg AS transmittal_mode_type_cd , bc_policyperiod_stg.policynumber_stg AS policynumber, bc_account_stg.accountnumber_stg AS accountnumber, bctl_policyperiodbillingmethod_stg.typecode_stg AS billing_meth_type_cd, bc_invoicestream_stg.updatetime_stg AS updatetime , ''BILLTOPLCYTRM'' agmt_rltd_rsn_cd , bctl_periodicity_stg.typecode_stg AS typecode, termnumber_stg AS termnumber, bc_invoice_stg.eventdate_stg AS eventdate, bc_invoice_stg.paymentduedate_stg AS paymentduedate, bc_invoicestream_stg.retired_stg AS retired_invoicestream, bc_account_stg.retired_stg AS retired_account, 0 AS retired_policyperiod, bc_policyperiod_stg.updatetime_stg AS updatetime_agmtrltd, (:START_DTTM) AS start_dttm, (:END_DTTM) AS end_dttm,
                                                                                                  CASE
                                                                                                                  WHEN bc_policyperiod_stg.updatetime_stg > bc_invoicestream_stg.updatetime_stg THEN bc_policyperiod_stg.updatetime_stg
                                                                                                                  ELSE bc_invoicestream_stg.updatetime_stg
                                                                                                  END                             AS inv_trans_strt_dttm FROM db_t_prod_stag.bc_policyperiod AS bc_policyperiod_stg
                                                                                  inner join      db_t_prod_stag.bc_invoicestream AS bc_invoicestream_stg
                                                                                  ON              bc_policyperiod_stg.primaryinvoicestream_alfa_stg=bc_invoicestream_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bc_invoice AS bc_invoice_stg
                                                                                  ON              bc_invoicestream_stg.id_stg=bc_invoice_stg.invoicestreamid_stg
                                                                                  left outer join db_t_prod_stag.bc_account AS bc_account_stg
                                                                                  ON              bc_account_stg.id_stg = bc_invoicestream_stg.accountid_stg
                                                                                  left outer join db_t_prod_stag.bc_acctpmntinst AS bc_acctpmntinst_stg
                                                                                  ON              bc_account_stg.id_stg=bc_acctpmntinst_stg.ownerid_stg
                                                                                  left outer join db_t_prod_stag.bctl_billinglevel AS bctl_billinglevel_stg
                                                                                  ON              bc_account_stg.billinglevel_stg = bctl_billinglevel_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_policyperiodbillingmethod AS bctl_policyperiodbillingmethod_stg
                                                                                  ON              bctl_policyperiodbillingmethod_stg.id_stg=bc_policyperiod_stg.billingmethod_stg
                                                                                  left outer join db_t_prod_stag.bctl_periodicity bctl_periodicity_stg
                                                                                  ON              bc_invoicestream_stg.periodicity_stg = bctl_periodicity_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_invoicedeliverymethod AS bctl_invoicedeliverymethod_stg
                                                                                  ON              bctl_invoicedeliverymethod_stg.id_stg=bc_account_stg.invoicedeliverytype_stg
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument AS bc_paymentinstrument_stg
                                                                                  ON              bc_paymentinstrument_stg.id_stg = coalesce(bc_invoicestream_stg.overridingpaymentinstrumentid_stg,bc_acctpmntinst_stg.foreignentityid_stg)
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod AS bctl_paymentmethod_stg
                                                                                  ON              bctl_paymentmethod_stg.id_stg = bc_paymentinstrument_stg.paymentmethod_stg WHERE (
                                                                                                                  bc_invoicestream_stg.updatetime_stg> (:START_DTTM)
                                                                                                  AND             bc_invoicestream_stg.updatetime_stg <= (:END_DTTM))
                                                                                  OR              (
                                                                                                                  bc_policyperiod_stg.updatetime_stg> (:START_DTTM)
                                                                                                  AND             bc_policyperiod_stg.updatetime_stg <= (:END_DTTM))
                                                                                  UNION ALL
                                                                                  SELECT DISTINCT bc_invoicestream_stg.billingreferencenumber_alfa_stg AS billingreferencenumber_alfa,
                                                                                                  bctl_billinglevel_stg.typecode_stg                   AS invoice_stream_type_cd,
                                                                                                  bctl_invoicedeliverymethod_stg.typecode_stg          AS statement_mail_type_cd ,
                                                                                                  bctl_paymentmethod_stg.typecode_stg                  AS transmittal_mode_type_cd ,
                                                                                                  bc_policyperiod_stg.policynumber_stg                 AS policynumber,
                                                                                                  bc_account_stg.accountnumber_stg                     AS accountnumber,
                                                                                                  bctl_policyperiodbillingmethod_stg.typecode_stg      AS billing_meth_type_cd,
                                                                                                  bc_invoicestream_stg.updatetime_stg                  AS updatetime,
                                                                                                  ''ACCTTOBILL''                                            agmt_rltd_rsn_cd,
                                                                                                  bctl_periodicity_stg.typecode_stg                    AS typecode,
                                                                                                  NULL                                                 AS termnumber,
                                                                                                  bc_invoice_stg.eventdate_stg                         AS eventdate,
                                                                                                  bc_invoice_stg.paymentduedate_stg                    AS paymentduedate,
                                                                                                  bc_invoicestream_stg.retired_stg                        retired_invoicestream,
                                                                                                  0                                                    AS retired_account,
                                                                                                  bc_policyperiod_stg.retired_stg                         retired_policyperiod,
                                                                                                  bc_invoicestream_stg.updatetime_stg                     updatetime_agmtrltd,
                                                                                                  (:START_DTTM)                                        AS start_dttm,
                                                                                                  (:END_DTTM)                                          AS end_dttm,
                                                                                                  CASE
                                                                                                                  WHEN bc_policyperiod_stg.updatetime_stg > bc_invoicestream_stg.updatetime_stg THEN bc_policyperiod_stg.updatetime_stg
                                                                                                                  ELSE bc_invoicestream_stg.updatetime_stg
                                                                                                  END                             AS inv_trans_strt_dttm
                                                                                  FROM            db_t_prod_stag.bc_invoicestream AS bc_invoicestream_stg
                                                                                  inner join      db_t_prod_stag.bc_account       AS bc_account_stg
                                                                                  ON              bc_account_stg.id_stg = bc_invoicestream_stg.accountid_stg
                                                                                  left outer join db_t_prod_stag.bc_invoice AS bc_invoice_stg
                                                                                  ON              bc_invoicestream_stg.id_stg=bc_invoice_stg.invoicestreamid_stg
                                                                                  left outer join db_t_prod_stag.bc_acctpmntinst AS bc_acctpmntinst_stg
                                                                                  ON              bc_account_stg.id_stg=bc_acctpmntinst_stg.ownerid_stg
                                                                                  left outer join db_t_prod_stag.bctl_billinglevel AS bctl_billinglevel_stg
                                                                                  ON              bc_account_stg.billinglevel_stg = bctl_billinglevel_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_invoicedeliverymethod AS bctl_invoicedeliverymethod_stg
                                                                                  ON              bctl_invoicedeliverymethod_stg.id_stg=bc_account_stg.invoicedeliverytype_stg
                                                                                  left outer join db_t_prod_stag.bc_policy AS bc_policy_stg
                                                                                  ON              bc_invoicestream_stg.policyid_stg=bc_policy_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bc_policyperiod AS bc_policyperiod_stg
                                                                                  ON              bc_policyperiod_stg.policyid_stg=bc_policy_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_policyperiodbillingmethod AS bctl_policyperiodbillingmethod_stg
                                                                                  ON              bctl_policyperiodbillingmethod_stg.id_stg=bc_policyperiod_stg.billingmethod_stg
                                                                                  left outer join db_t_prod_stag.bctl_periodicity AS bctl_periodicity_stg
                                                                                  ON              bc_invoicestream_stg.periodicity_stg = bctl_periodicity_stg.id_stg
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument AS bc_paymentinstrument_stg
                                                                                  ON              bc_paymentinstrument_stg.id_stg = coalesce(bc_invoicestream_stg.overridingpaymentinstrumentid_stg,bc_acctpmntinst_stg.foreignentityid_stg)
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod AS bctl_paymentmethod_stg
                                                                                  ON              bctl_paymentmethod_stg.id_stg = bc_paymentinstrument_stg.paymentmethod_stg
                                                                                  WHERE           (
                                                                                                                  bc_invoicestream_stg.updatetime_stg> (:START_DTTM)
                                                                                                  AND             bc_invoicestream_stg.updatetime_stg <= (:END_DTTM) )
                                                                                  OR              (
                                                                                                                  bc_policyperiod_stg.updatetime_stg> (:START_DTTM)
                                                                                                  AND             bc_policyperiod_stg.updatetime_stg <= (:END_DTTM)) ) x
                                                                                  GROUP BY        x.billingreferencenumber_alfa ,
                                                                                                  x.invoice_stream_type_cd ,
                                                                                                  x.statement_mail_type_cd ,
                                                                                                  x.transmittal_mode_type_cd ,
                                                                                                  x.policynumber,
                                                                                                  x.accountnumber,
                                                                                                  x.billing_meth_type_cd ,
                                                                                                  x.agmt_rltd_rsn_cd,
                                                                                                  x.typecode ,
                                                                                                  x.termnumber,
                                                                                                  x.retired_invoicestream,
                                                                                                  x.retired_account,
                                                                                                  x.retired_policyperiod,
                                                                                                  x.start_dttm,
                                                                                                  x.end_dttm,
                                                                                                  x.updatetime_agmtrltd ) AS a
                                                                  /* ---------------------------------------------dropzone mapping query of m_dz_agmt_invoicestream_x */
                                                  WHERE           a.billingreferencenumber_alfa IS NOT NULL qualify row_number() over(PARTITION BY a.billingreferencenumber_alfa ORDER BY a. eventdate,a.paymentduedate,inv_trans_strt_dttm DESC,billing_method_type_cd DESC ) =1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_agmt_invoicestream_x.billingreferencenumber_alfa               AS billingreferencenumber_alfa,
                sq_agmt_invoicestream_x.invoice_stream_type_cd                    AS invoice_stream_type_cd,
                sq_agmt_invoicestream_x.statement_mail_type_cd                    AS statement_mail_type_cd,
                sq_agmt_invoicestream_x.transmittal_mode_type_cd                  AS transmittal_mode_type_cd,
                sq_agmt_invoicestream_x.accountnumber                             AS accountnumber,
                sq_agmt_invoicestream_x.billing_method_type_cd                    AS billing_method_type_cd,
                sq_agmt_invoicestream_x.stmt_cycl_cd                              AS stmt_cycl_cd,
                to_char ( sq_agmt_invoicestream_x.eventdate , ''YYYY-MM-DD'' )      AS v_periodstart,
                to_date ( v_periodstart , ''YYYY-MM-DD'' )                          AS o_periodstart,
                to_char ( sq_agmt_invoicestream_x.paymentduedate , ''YYYY-MM-DD'' ) AS v_cancellationdate,
                to_date ( v_cancellationdate , ''YYYY-MM-DD'' )                     AS o_cancellationdate,
                sq_agmt_invoicestream_x.retired_invoicestream                     AS retired,
                sq_agmt_invoicestream_x.updatetime                                AS updatetime,
                sq_agmt_invoicestream_x.source_record_id
         FROM   sq_agmt_invoicestream_x );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.billingreferencenumber_alfa AS billingreferencenumber_alfa,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STREAM_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STREAM_CD */
                      END AS out_invc_stream_type_cd,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STMT_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STMT_CD */
                      END AS out_stmt_ml_type_cd_type_cd,
                      CASE
                                WHEN lkp_5.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_XMITL_MODE_TYPE_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_6.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_XMITL_MODE_TYPE_CD */
                      END                                   AS out_xmitl_mode_type_cd,
                      ''INV''                                 AS in_agmt_type_code,
                      ltrim ( rtrim ( in_agmt_type_code ) ) AS out_agmt_type_code,
                      CASE
                                WHEN lkp_7.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BILLING_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_8.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BILLING_CD */
                      END      AS out_bilg_meth_type_cd,
                      ''UNK''    AS out_agmt_cur_sts_cd,
                      ''UNK''    AS out_agmt_obtnd_cd,
                      ''UNK''    AS out_agmt_sbtype_cd,
                      ''UNK''    AS out_agmt_objtv_type_cd,
                      ''UNK''    AS out_mkt_risk_type_cd,
                      ''UNK''    AS out_ntwk_srvr_agmt_type_cd,
                      ''UNK''    AS out_frmlty_type_cd,
                      ''UNK''    AS out_agmt_idntftn_cd,
                      ''UNK''    AS out_trmtn_type_cd,
                      ''UNK''    AS out_int_pmt_meth_type_cd,
                      :prcs_id AS out_prcs_id,
                      CASE
                                WHEN lkp_9.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_STMT_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_10.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_STMT_CD */
                      END        AS out_stmt_cycl_cd,
                      ''SRC_SYS5'' AS src_cd,
                      lkp_11.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                              AS out_agmt_src_cd,
                      exp_pass_from_source.o_periodstart      AS agmt_eff_dttm,
                      exp_pass_from_source.o_cancellationdate AS agmt_plnd_expn_dt,
                      CASE
                                WHEN exp_pass_from_source.o_cancellationdate IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                ELSE exp_pass_from_source.o_cancellationdate
                      END                                                                    AS out_agmt_plnd_expn_dt,
                      exp_pass_from_source.retired                                           AS retired,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dt,
                      current_timestamp                                                      AS edw_strt_dttm,
                      exp_pass_from_source.updatetime                                        AS updatetime,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_stream_cd lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.invoice_stream_type_cd
            left join lkp_teradata_etl_ref_xlat_stream_cd lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.invoice_stream_type_cd
            left join lkp_teradata_etl_ref_xlat_stmt_cd lkp_3
            ON        lkp_3.src_idntftn_val = exp_pass_from_source.statement_mail_type_cd
            left join lkp_teradata_etl_ref_xlat_stmt_cd lkp_4
            ON        lkp_4.src_idntftn_val = exp_pass_from_source.statement_mail_type_cd
            left join lkp_teradata_etl_ref_xlat_xmitl_mode_type_cd lkp_5
            ON        lkp_5.src_idntftn_val = exp_pass_from_source.transmittal_mode_type_cd
            left join lkp_teradata_etl_ref_xlat_xmitl_mode_type_cd lkp_6
            ON        lkp_6.src_idntftn_val = exp_pass_from_source.transmittal_mode_type_cd
            left join lkp_teradata_etl_ref_xlat_billing_cd lkp_7
            ON        lkp_7.src_idntftn_val = exp_pass_from_source.billing_method_type_cd
            left join lkp_teradata_etl_ref_xlat_billing_cd lkp_8
            ON        lkp_8.src_idntftn_val = exp_pass_from_source.billing_method_type_cd
            left join lkp_teradata_etl_ref_stmt_cd lkp_9
            ON        lkp_9.src_idntftn_val = exp_pass_from_source.stmt_cycl_cd
            left join lkp_teradata_etl_ref_stmt_cd lkp_10
            ON        lkp_10.src_idntftn_val = exp_pass_from_source.stmt_cycl_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_11
            ON        lkp_11.src_idntftn_val = src_cd qualify rnk = 1 );
  -- Component LKP_AGMT_NEW, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_new AS
  (
            SELECT    lkp.agmt_id,
                      lkp.agmt_plnd_expn_dttm,
                      lkp.agmt_type_cd,
                      lkp.stmt_cycl_cd,
                      lkp.stmt_ml_type_cd,
                      lkp.invc_strem_type_cd,
                      lkp.bilg_meth_type_cd,
                      lkp.agmt_pmt_meth_cd,
                      lkp.agmt_eff_dttm,
                      lkp.nk_src_key,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_type_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.bilg_meth_type_cd ASC,lkp.agmt_pmt_meth_cd ASC,lkp.agmt_eff_dttm ASC,lkp.nk_src_key ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   agmt.agmt_id             AS agmt_id,
                                        agmt.agmt_plnd_expn_dttm AS agmt_plnd_expn_dttm,
                                        agmt.stmt_cycl_cd        AS stmt_cycl_cd,
                                        agmt.stmt_ml_type_cd     AS stmt_ml_type_cd,
                                        agmt.invc_strem_type_cd  AS invc_strem_type_cd,
                                        agmt.bilg_meth_type_cd   AS bilg_meth_type_cd,
                                        agmt.agmt_pmt_meth_cd    AS agmt_pmt_meth_cd,
                                        agmt.agmt_eff_dttm       AS agmt_eff_dttm,
                                        agmt.edw_strt_dttm       AS edw_strt_dttm,
                                        agmt.edw_end_dttm        AS edw_end_dttm,
                                        agmt.nk_src_key          AS nk_src_key,
                                        agmt.agmt_type_cd        AS agmt_type_cd
                               FROM     db_t_prod_core.agmt
                               WHERE    agmt_type_cd=''INV'' qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_data_transformation.billingreferencenumber_alfa
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_code qualify rnk = 1 );
  -- Component exp_for_cdc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_for_cdc AS
  (
             SELECT     lkp_agmt_new.agmt_id                                                   AS lkp_agmt_id,
                        lkp_agmt_new.agmt_plnd_expn_dttm                                       AS lkp_agmt_plnd_expn_dt,
                        lkp_agmt_new.agmt_type_cd                                              AS lkp_agmt_type_cd,
                        lkp_agmt_new.stmt_cycl_cd                                              AS lkp_stmt_cycl_cd,
                        lkp_agmt_new.invc_strem_type_cd                                        AS lkp_invc_strem_type_cd,
                        lkp_agmt_new.bilg_meth_type_cd                                         AS lkp_bilg_meth_type_cd,
                        lkp_agmt_new.agmt_eff_dttm                                             AS kp_agmt_eff_dttm,
                        lkp_agmt_new.nk_src_key                                                AS lkp_nk_src_key,
                        lkp_agmt_new.edw_strt_dttm                                             AS lkp_edw_start_dt,
                        lkp_agmt_new.edw_end_dttm                                              AS lkp_edw_end_dttm,
                        exp_data_transformation.billingreferencenumber_alfa                    AS billingreferencenumber_alfa,
                        exp_data_transformation.out_invc_stream_type_cd                        AS out_invc_stream_type_cd,
                        exp_data_transformation.out_stmt_ml_type_cd_type_cd                    AS out_stmt_ml_type_cd_type_cd,
                        exp_data_transformation.out_xmitl_mode_type_cd                         AS out_xmitl_mode_type_cd,
                        exp_data_transformation.out_agmt_type_code                             AS out_agmt_type_code,
                        exp_data_transformation.out_bilg_meth_type_cd                          AS out_bilg_meth_type_cd,
                        exp_data_transformation.out_agmt_cur_sts_cd                            AS out_agmt_cur_sts_cd,
                        exp_data_transformation.out_agmt_obtnd_cd                              AS out_agmt_obtnd_cd,
                        exp_data_transformation.out_agmt_sbtype_cd                             AS out_agmt_sbtype_cd,
                        exp_data_transformation.out_agmt_objtv_type_cd                         AS out_agmt_objtv_type_cd,
                        exp_data_transformation.out_mkt_risk_type_cd                           AS out_mkt_risk_type_cd,
                        exp_data_transformation.out_ntwk_srvr_agmt_type_cd                     AS out_ntwk_srvr_agmt_type_cd,
                        exp_data_transformation.out_frmlty_type_cd                             AS out_frmlty_type_cd,
                        exp_data_transformation.out_agmt_idntftn_cd                            AS out_agmt_idntftn_cd,
                        exp_data_transformation.out_trmtn_type_cd                              AS out_trmtn_type_cd,
                        exp_data_transformation.out_int_pmt_meth_type_cd                       AS out_int_pmt_meth_type_cd,
                        exp_data_transformation.out_prcs_id                                    AS out_prcs_id,
                        exp_data_transformation.out_stmt_cycl_cd                               AS out_stmt_cycl_cd,
                        exp_data_transformation.agmt_eff_dttm                                  AS in_agmt_eff_dttm,
                        exp_data_transformation.out_agmt_plnd_expn_dt                          AS in_agmt_plnd_expn_dt,
                        exp_data_transformation.edw_strt_dttm                                  AS edw_start_dt,
                        exp_data_transformation.edw_end_dt                                     AS edw_end_dt,
                        exp_data_transformation.out_agmt_src_cd                                AS out_agmt_src_cd,
                        exp_data_transformation.retired                                        AS retired,
                        to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS businessdatedefault,
                        to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS businessenddatedefault,
                        dateadd (second,-1, exp_data_transformation.edw_strt_dttm  )     AS edw_end_dt_exp,
                        md5 ( ltrim ( rtrim ( lkp_agmt_new.invc_strem_type_cd ) )
                                   || ltrim ( rtrim ( lkp_agmt_new.stmt_cycl_cd ) )
                                   || ltrim ( rtrim ( lkp_agmt_new.stmt_ml_type_cd ) )
                                   || ltrim ( rtrim ( lkp_agmt_new.agmt_eff_dttm ) )
                                   || ltrim ( rtrim ( lkp_agmt_new.agmt_plnd_expn_dttm ) )
                                   || lkp_agmt_new.agmt_pmt_meth_cd ) AS v_md5_lkp,
                        md5 ( ltrim ( rtrim ( exp_data_transformation.out_invc_stream_type_cd ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_stmt_cycl_cd ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_stmt_ml_type_cd_type_cd ) )
                                   || ltrim ( rtrim ( exp_data_transformation.agmt_eff_dttm ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_agmt_plnd_expn_dt ) )
                                   || exp_data_transformation.out_xmitl_mode_type_cd ) AS v_md5_src,
                        CASE
                                   WHEN lkp_agmt_new.agmt_id IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_md5_lkp != v_md5_src THEN ''U''
                                                         ELSE ''R''
                                              END
                        END AS o_ins_upd,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DATA_SRC */
                                                           AS agmt_src_cd,
                        exp_data_transformation.updatetime AS updatetime,
                        exp_data_transformation.source_record_id,
                        row_number() over (PARTITION BY exp_data_transformation.source_record_id ORDER BY exp_data_transformation.source_record_id) AS rnk
             FROM       exp_data_transformation
             inner join lkp_agmt_new
             ON         exp_data_transformation.source_record_id = lkp_agmt_new.source_record_id
             left join  lkp_teradata_etl_ref_xlat_data_src lkp_1
             ON         lkp_1.src_idntftn_val = ''DATA_SRC_TYPE2'' 
			 qualify row_number() over (PARTITION BY exp_data_transformation.source_record_id ORDER BY exp_data_transformation.source_record_id) 
			 = 1 );
  -- Component rtr_agmt_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_agmt_INSERT as
  SELECT exp_for_cdc.lkp_agmt_id                 AS agmt_id,
         exp_for_cdc.billingreferencenumber_alfa AS billingreferencenumber_alfa,
         exp_for_cdc.out_invc_stream_type_cd     AS out_invc_stream_type_cd,
         exp_for_cdc.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
         exp_for_cdc.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd,
         exp_for_cdc.out_agmt_type_code          AS out_agmt_type_code,
         exp_for_cdc.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd,
         exp_for_cdc.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd,
         exp_for_cdc.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd,
         exp_for_cdc.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd,
         exp_for_cdc.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd,
         exp_for_cdc.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd,
         exp_for_cdc.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd,
         exp_for_cdc.out_frmlty_type_cd          AS out_frmlty_type_cd,
         exp_for_cdc.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd,
         exp_for_cdc.out_trmtn_type_cd           AS out_trmtn_type_cd,
         exp_for_cdc.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd,
         exp_for_cdc.out_prcs_id                 AS out_prcs_id,
         exp_for_cdc.out_stmt_cycl_cd            AS out_stmt_cycl_cd,
         exp_for_cdc.o_ins_upd                   AS o_ins_upd,
         exp_for_cdc.lkp_edw_start_dt            AS lkp_edw_start_dt,
         exp_for_cdc.edw_start_dt                AS edw_start_dt,
         exp_for_cdc.edw_end_dt                  AS edw_end_dt,
         exp_for_cdc.edw_end_dt_exp              AS edw_end_dt_exp,
         exp_for_cdc.out_agmt_src_cd             AS out_agmt_src_cd,
         exp_for_cdc.businessdatedefault         AS businessdatedefault,
         exp_for_cdc.in_agmt_eff_dttm            AS in_agmt_eff_dttm,
         exp_for_cdc.in_agmt_plnd_expn_dt        AS in_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_nk_src_key              AS lkp_nk_src_key,
         exp_for_cdc.lkp_agmt_type_cd            AS lkp_agmt_type_cd,
         exp_for_cdc.lkp_stmt_cycl_cd            AS lkp_stmt_cycl_cd,
         exp_for_cdc.kp_agmt_eff_dttm            AS kp_agmt_eff_dttm,
         exp_for_cdc.lkp_agmt_plnd_expn_dt       AS lkp_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_bilg_meth_type_cd       AS lkp_bilg_meth_type_cd,
         exp_for_cdc.lkp_invc_strem_type_cd      AS lkp_invc_strem_type_cd,
         NULL                                    AS in_agmt_id_dummy,
         exp_for_cdc.retired                     AS retired,
         exp_for_cdc.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         NULL                                    AS flag,
         exp_for_cdc.agmt_src_cd                 AS agmt_src_cd,
         exp_for_cdc.updatetime                  AS updatetime,
         exp_for_cdc.businessenddatedefault      AS businessenddatedefault,
         exp_for_cdc.source_record_id
  FROM   exp_for_cdc
  WHERE  exp_for_cdc.o_ins_upd = ''I''
  OR     (
                exp_for_cdc.retired = 0
         AND    exp_for_cdc.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- exp_for_cdc.o_ins_upd = ''I''
		 ;
  
  -- Component rtr_agmt_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_agmt_RETIRED as
  SELECT exp_for_cdc.lkp_agmt_id                 AS agmt_id,
         exp_for_cdc.billingreferencenumber_alfa AS billingreferencenumber_alfa,
         exp_for_cdc.out_invc_stream_type_cd     AS out_invc_stream_type_cd,
         exp_for_cdc.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
         exp_for_cdc.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd,
         exp_for_cdc.out_agmt_type_code          AS out_agmt_type_code,
         exp_for_cdc.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd,
         exp_for_cdc.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd,
         exp_for_cdc.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd,
         exp_for_cdc.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd,
         exp_for_cdc.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd,
         exp_for_cdc.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd,
         exp_for_cdc.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd,
         exp_for_cdc.out_frmlty_type_cd          AS out_frmlty_type_cd,
         exp_for_cdc.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd,
         exp_for_cdc.out_trmtn_type_cd           AS out_trmtn_type_cd,
         exp_for_cdc.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd,
         exp_for_cdc.out_prcs_id                 AS out_prcs_id,
         exp_for_cdc.out_stmt_cycl_cd            AS out_stmt_cycl_cd,
         exp_for_cdc.o_ins_upd                   AS o_ins_upd,
         exp_for_cdc.lkp_edw_start_dt            AS lkp_edw_start_dt,
         exp_for_cdc.edw_start_dt                AS edw_start_dt,
         exp_for_cdc.edw_end_dt                  AS edw_end_dt,
         exp_for_cdc.edw_end_dt_exp              AS edw_end_dt_exp,
         exp_for_cdc.out_agmt_src_cd             AS out_agmt_src_cd,
         exp_for_cdc.businessdatedefault         AS businessdatedefault,
         exp_for_cdc.in_agmt_eff_dttm            AS in_agmt_eff_dttm,
         exp_for_cdc.in_agmt_plnd_expn_dt        AS in_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_nk_src_key              AS lkp_nk_src_key,
         exp_for_cdc.lkp_agmt_type_cd            AS lkp_agmt_type_cd,
         exp_for_cdc.lkp_stmt_cycl_cd            AS lkp_stmt_cycl_cd,
         exp_for_cdc.kp_agmt_eff_dttm            AS kp_agmt_eff_dttm,
         exp_for_cdc.lkp_agmt_plnd_expn_dt       AS lkp_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_bilg_meth_type_cd       AS lkp_bilg_meth_type_cd,
         exp_for_cdc.lkp_invc_strem_type_cd      AS lkp_invc_strem_type_cd,
         NULL                                    AS in_agmt_id_dummy,
         exp_for_cdc.retired                     AS retired,
         exp_for_cdc.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         NULL                                    AS flag,
         exp_for_cdc.agmt_src_cd                 AS agmt_src_cd,
         exp_for_cdc.updatetime                  AS updatetime,
         exp_for_cdc.businessenddatedefault      AS businessenddatedefault,
         exp_for_cdc.source_record_id
  FROM   exp_for_cdc
  WHERE  exp_for_cdc.o_ins_upd = ''R''
  AND    exp_for_cdc.retired != 0
  AND    exp_for_cdc.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_agmt_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_agmt_UPDATE as
  SELECT exp_for_cdc.lkp_agmt_id                 AS agmt_id,
         exp_for_cdc.billingreferencenumber_alfa AS billingreferencenumber_alfa,
         exp_for_cdc.out_invc_stream_type_cd     AS out_invc_stream_type_cd,
         exp_for_cdc.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
         exp_for_cdc.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd,
         exp_for_cdc.out_agmt_type_code          AS out_agmt_type_code,
         exp_for_cdc.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd,
         exp_for_cdc.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd,
         exp_for_cdc.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd,
         exp_for_cdc.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd,
         exp_for_cdc.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd,
         exp_for_cdc.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd,
         exp_for_cdc.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd,
         exp_for_cdc.out_frmlty_type_cd          AS out_frmlty_type_cd,
         exp_for_cdc.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd,
         exp_for_cdc.out_trmtn_type_cd           AS out_trmtn_type_cd,
         exp_for_cdc.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd,
         exp_for_cdc.out_prcs_id                 AS out_prcs_id,
         exp_for_cdc.out_stmt_cycl_cd            AS out_stmt_cycl_cd,
         exp_for_cdc.o_ins_upd                   AS o_ins_upd,
         exp_for_cdc.lkp_edw_start_dt            AS lkp_edw_start_dt,
         exp_for_cdc.edw_start_dt                AS edw_start_dt,
         exp_for_cdc.edw_end_dt                  AS edw_end_dt,
         exp_for_cdc.edw_end_dt_exp              AS edw_end_dt_exp,
         exp_for_cdc.out_agmt_src_cd             AS out_agmt_src_cd,
         exp_for_cdc.businessdatedefault         AS businessdatedefault,
         exp_for_cdc.in_agmt_eff_dttm            AS in_agmt_eff_dttm,
         exp_for_cdc.in_agmt_plnd_expn_dt        AS in_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_nk_src_key              AS lkp_nk_src_key,
         exp_for_cdc.lkp_agmt_type_cd            AS lkp_agmt_type_cd,
         exp_for_cdc.lkp_stmt_cycl_cd            AS lkp_stmt_cycl_cd,
         exp_for_cdc.kp_agmt_eff_dttm            AS kp_agmt_eff_dttm,
         exp_for_cdc.lkp_agmt_plnd_expn_dt       AS lkp_agmt_plnd_expn_dt,
         exp_for_cdc.lkp_bilg_meth_type_cd       AS lkp_bilg_meth_type_cd,
         exp_for_cdc.lkp_invc_strem_type_cd      AS lkp_invc_strem_type_cd,
         NULL                                    AS in_agmt_id_dummy,
         exp_for_cdc.retired                     AS retired,
         exp_for_cdc.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         NULL                                    AS flag,
         exp_for_cdc.agmt_src_cd                 AS agmt_src_cd,
         exp_for_cdc.updatetime                  AS updatetime,
         exp_for_cdc.businessenddatedefault      AS businessenddatedefault,
         exp_for_cdc.source_record_id
  FROM   exp_for_cdc
  WHERE  exp_for_cdc.o_ins_upd = ''U''
  AND    exp_for_cdc.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) -- exp_for_cdc.o_ins_upd = ''U''
  ;
  
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_insert.billingreferencenumber_alfa AS billingreferencenumber_alfa1,
                rtr_agmt_insert.out_invc_stream_type_cd     AS out_invc_stream_type_cd1,
                rtr_agmt_insert.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
                rtr_agmt_insert.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd1,
                rtr_agmt_insert.out_agmt_type_code          AS out_agmt_type_code1,
                rtr_agmt_insert.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd1,
                rtr_agmt_insert.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd1,
                rtr_agmt_insert.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd1,
                rtr_agmt_insert.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd1,
                rtr_agmt_insert.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd1,
                rtr_agmt_insert.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd1,
                rtr_agmt_insert.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd1,
                rtr_agmt_insert.out_frmlty_type_cd          AS out_frmlty_type_cd1,
                rtr_agmt_insert.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd1,
                rtr_agmt_insert.out_trmtn_type_cd           AS out_trmtn_type_cd1,
                rtr_agmt_insert.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd1,
                rtr_agmt_insert.out_prcs_id                 AS out_prcs_id1,
                rtr_agmt_insert.out_stmt_cycl_cd            AS out_stmt_cycl_cd1,
                rtr_agmt_insert.lkp_edw_start_dt            AS lkp_edw_start_dt1,
                rtr_agmt_insert.edw_start_dt                AS edw_start_dt1,
                rtr_agmt_insert.edw_end_dt                  AS edw_end_dt1,
                rtr_agmt_insert.edw_end_dt_exp              AS edw_end_dt_exp1,
                rtr_agmt_insert.out_agmt_src_cd             AS out_agmt_src_cd1,
                rtr_agmt_insert.businessdatedefault         AS businessdatedefault,
                rtr_agmt_insert.in_agmt_eff_dttm            AS in_agmt_eff_dttm1,
                rtr_agmt_insert.in_agmt_plnd_expn_dt        AS in_agmt_plnd_expn_dt1,
                rtr_agmt_insert.in_agmt_id_dummy            AS in_agmt_id_dummy1,
                rtr_agmt_insert.agmt_src_cd                 AS agmt_src_cd1,
                rtr_agmt_insert.updatetime                  AS updatetime1,
                rtr_agmt_insert.retired                     AS retired1,
                rtr_agmt_insert.businessenddatedefault      AS businessenddatedefault1,
                0                                           AS update_strategy_action,
				source_record_id
         FROM   rtr_agmt_insert );
  -- Component upd_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_update.agmt_id                     AS agmt_id,
                rtr_agmt_update.billingreferencenumber_alfa AS billingreferencenumber_alfa1,
                rtr_agmt_update.out_invc_stream_type_cd     AS out_invc_stream_type_cd1,
                rtr_agmt_update.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
                rtr_agmt_update.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd1,
                rtr_agmt_update.out_agmt_type_code          AS out_agmt_type_code1,
                rtr_agmt_update.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd1,
                rtr_agmt_update.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd1,
                rtr_agmt_update.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd1,
                rtr_agmt_update.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd1,
                rtr_agmt_update.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd1,
                rtr_agmt_update.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd1,
                rtr_agmt_update.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd1,
                rtr_agmt_update.out_frmlty_type_cd          AS out_frmlty_type_cd1,
                rtr_agmt_update.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd1,
                rtr_agmt_update.out_trmtn_type_cd           AS out_trmtn_type_cd1,
                rtr_agmt_update.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd1,
                rtr_agmt_update.out_prcs_id                 AS out_prcs_id1,
                rtr_agmt_update.out_stmt_cycl_cd            AS out_stmt_cycl_cd1,
                rtr_agmt_update.lkp_edw_start_dt            AS lkp_edw_start_dt3,
                rtr_agmt_update.edw_start_dt                AS edw_start_dt3,
                rtr_agmt_update.edw_end_dt                  AS edw_end_dt3,
                rtr_agmt_update.edw_end_dt_exp              AS edw_end_dt_exp3,
                rtr_agmt_update.out_agmt_src_cd             AS out_agmt_src_cd3,
                rtr_agmt_update.businessdatedefault         AS businessdatedefault,
                rtr_agmt_update.in_agmt_eff_dttm            AS in_agmt_eff_dttm3,
                rtr_agmt_update.in_agmt_plnd_expn_dt        AS in_agmt_plnd_expn_dt3,
                rtr_agmt_update.retired                     AS retired3,
                rtr_agmt_update.lkp_edw_end_dttm            AS lkp_edw_end_dttm3,
                rtr_agmt_update.agmt_src_cd                 AS agmt_src_cd3,
                rtr_agmt_update.updatetime                  AS updatetime3,
                rtr_agmt_update.businessenddatedefault      AS businessenddatedefault3,
                0                                           AS update_strategy_action,
				source_record_id
         FROM   rtr_agmt_update );
  -- Component upd_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_update.agmt_id                     AS agmt_id,
                rtr_agmt_update.billingreferencenumber_alfa AS billingreferencenumber_alfa1,
                rtr_agmt_update.out_invc_stream_type_cd     AS out_invc_stream_type_cd3,
                rtr_agmt_update.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
                rtr_agmt_update.out_xmitl_mode_type_cd      AS out_xmitl_mode_type_cd3,
                rtr_agmt_update.out_agmt_type_code          AS out_agmt_type_code3,
                rtr_agmt_update.out_bilg_meth_type_cd       AS out_bilg_meth_type_cd3,
                rtr_agmt_update.out_agmt_cur_sts_cd         AS out_agmt_cur_sts_cd3,
                rtr_agmt_update.out_agmt_obtnd_cd           AS out_agmt_obtnd_cd3,
                rtr_agmt_update.out_agmt_sbtype_cd          AS out_agmt_sbtype_cd3,
                rtr_agmt_update.out_agmt_objtv_type_cd      AS out_agmt_objtv_type_cd3,
                rtr_agmt_update.out_mkt_risk_type_cd        AS out_mkt_risk_type_cd3,
                rtr_agmt_update.out_ntwk_srvr_agmt_type_cd  AS out_ntwk_srvr_agmt_type_cd3,
                rtr_agmt_update.out_frmlty_type_cd          AS out_frmlty_type_cd3,
                rtr_agmt_update.out_agmt_idntftn_cd         AS out_agmt_idntftn_cd3,
                rtr_agmt_update.out_trmtn_type_cd           AS out_trmtn_type_cd3,
                rtr_agmt_update.out_int_pmt_meth_type_cd    AS out_int_pmt_meth_type_cd3,
                rtr_agmt_update.out_prcs_id                 AS out_prcs_id3,
                rtr_agmt_update.out_stmt_cycl_cd            AS out_stmt_cycl_cd3,
                rtr_agmt_update.lkp_edw_start_dt            AS lkp_edw_start_dt3,
                rtr_agmt_update.edw_start_dt                AS edw_start_dt3,
                rtr_agmt_update.edw_end_dt                  AS edw_end_dt3,
                rtr_agmt_update.edw_end_dt_exp              AS edw_end_dt_exp3,
                rtr_agmt_update.out_agmt_src_cd             AS out_agmt_src_cd3,
                rtr_agmt_update.lkp_nk_src_key              AS lkp_nk_src_key3,
                rtr_agmt_update.lkp_agmt_type_cd            AS lkp_agmt_type_cd3,
                rtr_agmt_update.lkp_stmt_cycl_cd            AS lkp_stmt_cycl_cd3,
                rtr_agmt_update.kp_agmt_eff_dttm            AS kp_agmt_eff_dttm3,
                rtr_agmt_update.lkp_agmt_plnd_expn_dt       AS lkp_agmt_plnd_expn_dt3,
                rtr_agmt_update.lkp_bilg_meth_type_cd       AS lkp_bilg_meth_type_cd,
                rtr_agmt_update.lkp_invc_strem_type_cd      AS lkp_invc_strem_type_cd3,
                rtr_agmt_update.retired                     AS retired3,
                rtr_agmt_update.lkp_edw_end_dttm            AS lkp_edw_end_dttm3,
                rtr_agmt_update.updatetime                  AS updatetime3,
                1                                           AS update_strategy_action,
				source_record_id
         FROM   rtr_agmt_update );
  -- Component UPDTRANS, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updtrans AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_retired.agmt_id          AS agmt_id,
                rtr_agmt_retired.lkp_edw_start_dt AS lkp_edw_start_dt4,
                rtr_agmt_retired.lkp_nk_src_key   AS lkp_nk_src_key4,
                rtr_agmt_retired.updatetime       AS updatetime4,
                1                                 AS update_strategy_action,
				source_record_id
         FROM   rtr_agmt_retired );
  -- Component exp_pass_through_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through_tgt_ins AS
  (
            SELECT    upd_ins.billingreferencenumber_alfa1 AS billingreferencenumber_alfa1,
                      lkp_1.agmt_id
                      /* replaced lookup LKP_XREF_AGMNT */
                                                          AS o_agmt_id,
                      upd_ins.out_invc_stream_type_cd1    AS out_invc_stream_type_cd1,
                      upd_ins.out_stmt_ml_type_cd_type_cd AS out_stmt_ml_type_cd_type_cd,
                      upd_ins.out_xmitl_mode_type_cd1     AS out_xmitl_mode_type_cd1,
                      upd_ins.out_agmt_type_code1         AS out_agmt_type_code1,
                      upd_ins.out_agmt_cur_sts_cd1        AS out_agmt_cur_sts_cd1,
                      upd_ins.out_agmt_obtnd_cd1          AS out_agmt_obtnd_cd1,
                      upd_ins.out_agmt_sbtype_cd1         AS out_agmt_sbtype_cd1,
                      upd_ins.out_agmt_objtv_type_cd1     AS out_agmt_objtv_type_cd1,
                      upd_ins.out_mkt_risk_type_cd1       AS out_mkt_risk_type_cd1,
                      upd_ins.out_ntwk_srvr_agmt_type_cd1 AS out_ntwk_srvr_agmt_type_cd1,
                      upd_ins.out_frmlty_type_cd1         AS out_frmlty_type_cd1,
                      upd_ins.out_agmt_idntftn_cd1        AS out_agmt_idntftn_cd1,
                      upd_ins.out_trmtn_type_cd1          AS out_trmtn_type_cd1,
                      upd_ins.out_int_pmt_meth_type_cd1   AS out_int_pmt_meth_type_cd1,
                      upd_ins.out_prcs_id1                AS out_prcs_id1,
                      upd_ins.out_stmt_cycl_cd1           AS out_stmt_cycl_cd1,
                      upd_ins.edw_start_dt1               AS edw_start_dt1,
                      CASE
                                WHEN upd_ins.retired1 <> 0 THEN current_timestamp
                                ELSE to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                      END                           AS edw_end_dt1,
                      upd_ins.out_agmt_src_cd1      AS out_agmt_src_cd1,
                      upd_ins.businessdatedefault   AS businessdatedefault,
                      upd_ins.in_agmt_eff_dttm1     AS in_agmt_eff_dttm1,
                      upd_ins.in_agmt_plnd_expn_dt1 AS in_agmt_plnd_expn_dt1,
                      upd_ins.agmt_src_cd1          AS agmt_src_cd1,
                      upd_ins.updatetime1           AS updatetime1,
                      CASE
                                WHEN upd_ins.retired1 <> 0 THEN upd_ins.updatetime1
                                ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                      END                             AS trans_end_dttm,
                      upd_ins.businessenddatedefault1 AS businessenddatedefault1,
                      upd_ins.source_record_id,
                      row_number() over (PARTITION BY upd_ins.source_record_id ORDER BY upd_ins.source_record_id) AS rnk
            FROM      upd_ins
            left join lkp_xref_agmnt lkp_1
            ON        lkp_1.nk_src_key = ltrim ( rtrim ( upd_ins.billingreferencenumber_alfa1 ) )
            AND       lkp_1.term_num = NULL
            AND       lkp_1.agmt_type_cd = ltrim ( rtrim ( upd_ins.out_agmt_type_code1 ) ) qualify rnk = 1 );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
         SELECT updtrans.agmt_id           AS agmt_id,
                updtrans.lkp_edw_start_dt4 AS lkp_edw_start_dt4,
                current_timestamp          AS edw_end_dttm,
                updtrans.lkp_nk_src_key4   AS lkp_nk_src_key4,
                updtrans.updatetime4       AS updatetime4,
                updtrans.source_record_id
         FROM   updtrans );
  -- Component exp_pass_through_tgt_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through_tgt_update AS
  (
         SELECT upd_update.agmt_id                                  AS agmt_id,
                upd_update.lkp_edw_start_dt3                        AS lkp_edw_start_dt3,
                upd_update.edw_end_dt_exp3                          AS edw_end_dt_exp3,
                upd_update.lkp_nk_src_key3                          AS lkp_nk_src_key3,
                upd_update.lkp_agmt_type_cd3                        AS lkp_agmt_type_cd3,
                upd_update.lkp_stmt_cycl_cd3                        AS lkp_stmt_cycl_cd3,
                upd_update.kp_agmt_eff_dttm3                        AS kp_agmt_eff_dttm3,
                upd_update.lkp_agmt_plnd_expn_dt3                   AS lkp_agmt_plnd_expn_dt3,
                upd_update.lkp_bilg_meth_type_cd                    AS lkp_bilg_meth_type_cd,
                upd_update.lkp_invc_strem_type_cd3                  AS lkp_invc_strem_type_cd3,
                upd_update.retired3                                 AS retired3,
                upd_update.lkp_edw_end_dttm3                        AS lkp_edw_end_dttm3,
                dateadd (second,-1, upd_update.updatetime3  ) AS trans_end_dttm,
                upd_update.source_record_id
         FROM   upd_update );
  -- Component exp_pass_through_tgt_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through_tgt_ins_upd AS
  (
         SELECT upd_ins_upd.billingreferencenumber_alfa1 AS billingreferencenumber_alfa1,
                upd_ins_upd.agmt_id                      AS NEXTVAL,
                upd_ins_upd.out_invc_stream_type_cd1     AS out_invc_stream_type_cd1,
                upd_ins_upd.out_stmt_ml_type_cd_type_cd  AS out_stmt_ml_type_cd_type_cd,
                upd_ins_upd.out_xmitl_mode_type_cd1      AS out_xmitl_mode_type_cd1,
                upd_ins_upd.out_agmt_type_code1          AS out_agmt_type_code1,
                upd_ins_upd.out_bilg_meth_type_cd1       AS out_bilg_meth_type_cd1,
                upd_ins_upd.out_agmt_cur_sts_cd1         AS out_agmt_cur_sts_cd1,
                upd_ins_upd.out_agmt_obtnd_cd1           AS out_agmt_obtnd_cd1,
                upd_ins_upd.out_agmt_sbtype_cd1          AS out_agmt_sbtype_cd1,
                upd_ins_upd.out_agmt_objtv_type_cd1      AS out_agmt_objtv_type_cd1,
                upd_ins_upd.out_mkt_risk_type_cd1        AS out_mkt_risk_type_cd1,
                upd_ins_upd.out_ntwk_srvr_agmt_type_cd1  AS out_ntwk_srvr_agmt_type_cd1,
                upd_ins_upd.out_frmlty_type_cd1          AS out_frmlty_type_cd1,
                upd_ins_upd.out_agmt_idntftn_cd1         AS out_agmt_idntftn_cd1,
                upd_ins_upd.out_trmtn_type_cd1           AS out_trmtn_type_cd1,
                upd_ins_upd.out_int_pmt_meth_type_cd1    AS out_int_pmt_meth_type_cd1,
                upd_ins_upd.out_prcs_id1                 AS out_prcs_id1,
                upd_ins_upd.out_stmt_cycl_cd1            AS out_stmt_cycl_cd1,
                upd_ins_upd.edw_start_dt3                AS edw_start_dt3,
                upd_ins_upd.edw_end_dt3                  AS edw_end_dt3,
                upd_ins_upd.out_agmt_src_cd3             AS out_agmt_src_cd3,
                upd_ins_upd.businessdatedefault          AS businessdatedefault,
                upd_ins_upd.in_agmt_eff_dttm3            AS in_agmt_eff_dttm3,
                upd_ins_upd.in_agmt_plnd_expn_dt3        AS in_agmt_plnd_expn_dt3,
                upd_ins_upd.retired3                     AS retired3,
                upd_ins_upd.lkp_edw_end_dttm3            AS lkp_edw_end_dttm3,
                upd_ins_upd.agmt_src_cd3                 AS agmt_src_cd3,
                upd_ins_upd.updatetime3                  AS updatetime3,
                upd_ins_upd.businessenddatedefault3      AS businessenddatedefault3,
                upd_ins_upd.source_record_id
         FROM   upd_ins_upd );
  -- Component AGMT_INS, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          stmt_cycl_cd,
                          stmt_ml_type_cd,
                          agmt_objtv_type_cd,
                          mkt_risk_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          invc_strem_type_cd,
                          agmt_eff_dttm,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          nk_src_key,
                          agmt_pmt_meth_cd,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_through_tgt_ins.o_agmt_id                    AS agmt_id,
         exp_pass_through_tgt_ins.billingreferencenumber_alfa1 AS host_agmt_num,
         exp_pass_through_tgt_ins.in_agmt_plnd_expn_dt1        AS agmt_plnd_expn_dttm,
         exp_pass_through_tgt_ins.businessdatedefault          AS agmt_signd_dttm,
         exp_pass_through_tgt_ins.out_agmt_type_code1          AS agmt_type_cd,
         exp_pass_through_tgt_ins.agmt_src_cd1                 AS agmt_src_cd,
         exp_pass_through_tgt_ins.out_agmt_cur_sts_cd1         AS agmt_cur_sts_cd,
         exp_pass_through_tgt_ins.out_agmt_obtnd_cd1           AS agmt_obtnd_cd,
         exp_pass_through_tgt_ins.out_agmt_sbtype_cd1          AS agmt_sbtype_cd,
         exp_pass_through_tgt_ins.businessdatedefault          AS agmt_prcsg_dttm,
         exp_pass_through_tgt_ins.out_stmt_cycl_cd1            AS stmt_cycl_cd,
         exp_pass_through_tgt_ins.out_stmt_ml_type_cd_type_cd  AS stmt_ml_type_cd,
         exp_pass_through_tgt_ins.out_agmt_objtv_type_cd1      AS agmt_objtv_type_cd,
         exp_pass_through_tgt_ins.out_mkt_risk_type_cd1        AS mkt_risk_type_cd,
         exp_pass_through_tgt_ins.out_ntwk_srvr_agmt_type_cd1  AS ntwk_srvc_agmt_type_cd,
         exp_pass_through_tgt_ins.out_frmlty_type_cd1          AS frmlty_type_cd,
         exp_pass_through_tgt_ins.out_agmt_idntftn_cd1         AS agmt_idntftn_cd,
         exp_pass_through_tgt_ins.out_trmtn_type_cd1           AS trmtn_type_cd,
         exp_pass_through_tgt_ins.out_int_pmt_meth_type_cd1    AS int_pmt_meth_cd,
         exp_pass_through_tgt_ins.out_invc_stream_type_cd1     AS invc_strem_type_cd,
         exp_pass_through_tgt_ins.in_agmt_eff_dttm1            AS agmt_eff_dttm,
         exp_pass_through_tgt_ins.businessdatedefault          AS modl_eff_dttm,
         exp_pass_through_tgt_ins.out_prcs_id1                 AS prcs_id,
         exp_pass_through_tgt_ins.businessenddatedefault1      AS modl_actl_end_dttm,
         exp_pass_through_tgt_ins.businessdatedefault          AS cntnus_srvc_dttm,
         exp_pass_through_tgt_ins.billingreferencenumber_alfa1 AS nk_src_key,
         exp_pass_through_tgt_ins.out_xmitl_mode_type_cd1      AS agmt_pmt_meth_cd,
         exp_pass_through_tgt_ins.out_agmt_src_cd1             AS src_sys_cd,
         exp_pass_through_tgt_ins.edw_start_dt1                AS edw_strt_dttm,
         exp_pass_through_tgt_ins.edw_end_dt1                  AS edw_end_dttm,
         exp_pass_through_tgt_ins.updatetime1                  AS trans_strt_dttm,
         exp_pass_through_tgt_ins.trans_end_dttm               AS trans_end_dttm
  FROM   exp_pass_through_tgt_ins;
  
  -- Component AGMT_UPD_Retire, Type TARGET
  merge
  INTO         db_t_prod_core.agmt
  USING        exptrans
  ON (
                            agmt.agmt_id = exptrans.agmt_id
               AND          agmt.nk_src_key = exptrans.lkp_nk_src_key4
               AND          agmt.edw_strt_dttm = exptrans.lkp_edw_start_dt4)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exptrans.agmt_id,
         nk_src_key = exptrans.lkp_nk_src_key4,
         edw_strt_dttm = exptrans.lkp_edw_start_dt4,
         edw_end_dttm = exptrans.edw_end_dttm,
         trans_end_dttm = exptrans.updatetime4;
  
  -- Component FILTRANS, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans AS
  (
         SELECT exp_pass_through_tgt_update.lkp_agmt_plnd_expn_dt3  AS lkp_agmt_plnd_expn_dt3,
                exp_pass_through_tgt_update.lkp_agmt_type_cd3       AS lkp_agmt_type_cd3,
                exp_pass_through_tgt_update.lkp_stmt_cycl_cd3       AS lkp_stmt_cycl_cd3,
                exp_pass_through_tgt_update.lkp_invc_strem_type_cd3 AS lkp_invc_strem_type_cd3,
                exp_pass_through_tgt_update.lkp_bilg_meth_type_cd   AS lkp_bilg_meth_type_cd,
                exp_pass_through_tgt_update.kp_agmt_eff_dttm3       AS kp_agmt_eff_dttm3,
                exp_pass_through_tgt_update.edw_end_dt_exp3         AS edw_end_dt_exp3,
                exp_pass_through_tgt_update.lkp_edw_start_dt3       AS lkp_edw_start_dt3,
                exp_pass_through_tgt_update.lkp_nk_src_key3         AS lkp_nk_src_key3,
                exp_pass_through_tgt_update.retired3                AS retired3,
                exp_pass_through_tgt_update.lkp_edw_end_dttm3       AS lkp_edw_end_dttm3,
                exp_pass_through_tgt_update.agmt_id                 AS agmt_id,
                exp_pass_through_tgt_update.trans_end_dttm          AS trans_end_dttm,
                exp_pass_through_tgt_update.source_record_id
         FROM   exp_pass_through_tgt_update
         WHERE  exp_pass_through_tgt_update.lkp_edw_end_dttm3 = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  -- Component FILTRANS1, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans1 AS
  (
         SELECT exp_pass_through_tgt_ins_upd.NEXTVAL                      AS NEXTVAL,
                exp_pass_through_tgt_ins_upd.billingreferencenumber_alfa1 AS billingreferencenumber_alfa1,
                exp_pass_through_tgt_ins_upd.in_agmt_plnd_expn_dt3        AS in_agmt_plnd_expn_dt3,
                exp_pass_through_tgt_ins_upd.out_agmt_type_code1          AS out_agmt_type_code1,
                exp_pass_through_tgt_ins_upd.out_agmt_src_cd3             AS out_agmt_src_cd3,
                exp_pass_through_tgt_ins_upd.out_agmt_cur_sts_cd1         AS out_agmt_cur_sts_cd1,
                exp_pass_through_tgt_ins_upd.out_agmt_obtnd_cd1           AS out_agmt_obtnd_cd1,
                exp_pass_through_tgt_ins_upd.out_agmt_sbtype_cd1          AS out_agmt_sbtype_cd1,
                exp_pass_through_tgt_ins_upd.out_stmt_cycl_cd1            AS out_stmt_cycl_cd1,
                exp_pass_through_tgt_ins_upd.out_stmt_ml_type_cd_type_cd  AS out_stmt_ml_type_cd_type_cd,
                exp_pass_through_tgt_ins_upd.out_agmt_objtv_type_cd1      AS out_agmt_objtv_type_cd1,
                exp_pass_through_tgt_ins_upd.out_mkt_risk_type_cd1        AS out_mkt_risk_type_cd1,
                exp_pass_through_tgt_ins_upd.out_ntwk_srvr_agmt_type_cd1  AS out_ntwk_srvr_agmt_type_cd1,
                exp_pass_through_tgt_ins_upd.out_frmlty_type_cd1          AS out_frmlty_type_cd1,
                exp_pass_through_tgt_ins_upd.out_agmt_idntftn_cd1         AS out_agmt_idntftn_cd1,
                exp_pass_through_tgt_ins_upd.out_trmtn_type_cd1           AS out_trmtn_type_cd1,
                exp_pass_through_tgt_ins_upd.out_int_pmt_meth_type_cd1    AS out_int_pmt_meth_type_cd1,
                exp_pass_through_tgt_ins_upd.out_invc_stream_type_cd1     AS out_invc_stream_type_cd1,
                exp_pass_through_tgt_ins_upd.out_bilg_meth_type_cd1       AS out_bilg_meth_type_cd1,
                exp_pass_through_tgt_ins_upd.in_agmt_eff_dttm3            AS in_agmt_eff_dttm3,
                exp_pass_through_tgt_ins_upd.businessdatedefault          AS businessdatedefault,
                exp_pass_through_tgt_ins_upd.out_xmitl_mode_type_cd1      AS out_xmitl_mode_type_cd1,
                exp_pass_through_tgt_ins_upd.out_prcs_id1                 AS out_prcs_id1,
                exp_pass_through_tgt_ins_upd.businessdatedefault          AS businessdatedefault1,
                exp_pass_through_tgt_ins_upd.edw_start_dt3                AS edw_start_dt3,
                exp_pass_through_tgt_ins_upd.edw_end_dt3                  AS edw_end_dt3,
                exp_pass_through_tgt_ins_upd.billingreferencenumber_alfa1 AS billingreferencenumber_alfa11,
                exp_pass_through_tgt_ins_upd.out_agmt_src_cd3             AS out_agmt_src_cd31,
                exp_pass_through_tgt_ins_upd.retired3                     AS retired3,
                exp_pass_through_tgt_ins_upd.lkp_edw_end_dttm3            AS lkp_edw_end_dttm3,
                exp_pass_through_tgt_ins_upd.agmt_src_cd3                 AS agmt_src_cd3,
                exp_pass_through_tgt_ins_upd.updatetime3                  AS updatetime3,
                exp_pass_through_tgt_ins_upd.businessenddatedefault3      AS businessenddatedefault3,
                exp_pass_through_tgt_ins_upd.source_record_id
         FROM   exp_pass_through_tgt_ins_upd
         WHERE  exp_pass_through_tgt_ins_upd.retired3 = 0 );
  -- Component AGMT_UPD_CDC, Type TARGET
  merge
  INTO         db_t_prod_core.agmt
  USING        filtrans
  ON (
                            agmt.agmt_id = filtrans.agmt_id
               AND          agmt.nk_src_key = filtrans.lkp_nk_src_key3
               AND          agmt.edw_strt_dttm = filtrans.lkp_edw_start_dt3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = filtrans.agmt_id,
         nk_src_key = filtrans.lkp_nk_src_key3,
         edw_strt_dttm = filtrans.lkp_edw_start_dt3,
         edw_end_dttm = filtrans.edw_end_dt_exp3,
         trans_end_dttm = filtrans.trans_end_dttm;
  
  -- Component AGMT_INS_Retire, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          stmt_cycl_cd,
                          stmt_ml_type_cd,
                          agmt_objtv_type_cd,
                          mkt_risk_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          invc_strem_type_cd,
                          agmt_eff_dttm,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          nk_src_key,
                          agmt_pmt_meth_cd,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT   row_number() over (ORDER BY 1)          AS agmt_id,
           filtrans1.billingreferencenumber_alfa1  AS host_agmt_num,
           filtrans1.in_agmt_plnd_expn_dt3         AS agmt_plnd_expn_dttm,
           filtrans1.businessdatedefault1          AS agmt_signd_dttm,
           filtrans1.out_agmt_type_code1           AS agmt_type_cd,
           filtrans1.agmt_src_cd3                  AS agmt_src_cd,
           filtrans1.out_agmt_cur_sts_cd1          AS agmt_cur_sts_cd,
           filtrans1.out_agmt_obtnd_cd1            AS agmt_obtnd_cd,
           filtrans1.out_agmt_sbtype_cd1           AS agmt_sbtype_cd,
           filtrans1.businessdatedefault1          AS agmt_prcsg_dttm,
           filtrans1.out_stmt_cycl_cd1             AS stmt_cycl_cd,
           filtrans1.out_stmt_ml_type_cd_type_cd   AS stmt_ml_type_cd,
           filtrans1.out_agmt_objtv_type_cd1       AS agmt_objtv_type_cd,
           filtrans1.out_mkt_risk_type_cd1         AS mkt_risk_type_cd,
           filtrans1.out_ntwk_srvr_agmt_type_cd1   AS ntwk_srvc_agmt_type_cd,
           filtrans1.out_frmlty_type_cd1           AS frmlty_type_cd,
           filtrans1.out_agmt_idntftn_cd1          AS agmt_idntftn_cd,
           filtrans1.out_trmtn_type_cd1            AS trmtn_type_cd,
           filtrans1.out_int_pmt_meth_type_cd1     AS int_pmt_meth_cd,
           filtrans1.out_invc_stream_type_cd1      AS invc_strem_type_cd,
           filtrans1.in_agmt_eff_dttm3             AS agmt_eff_dttm,
           filtrans1.businessdatedefault           AS modl_eff_dttm,
           filtrans1.out_prcs_id1                  AS prcs_id,
           filtrans1.businessenddatedefault3       AS modl_actl_end_dttm,
           filtrans1.businessdatedefault1          AS cntnus_srvc_dttm,
           filtrans1.billingreferencenumber_alfa11 AS nk_src_key,
           filtrans1.out_xmitl_mode_type_cd1       AS agmt_pmt_meth_cd,
           filtrans1.out_agmt_src_cd31             AS src_sys_cd,
           filtrans1.edw_start_dt3                 AS edw_strt_dttm,
           filtrans1.edw_end_dt3                   AS edw_end_dttm,
           filtrans1.updatetime3                   AS trans_strt_dttm
  FROM     filtrans1;

END;
';