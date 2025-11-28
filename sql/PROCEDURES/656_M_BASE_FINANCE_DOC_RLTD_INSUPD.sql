-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_DOC_RLTD_INSUPD("WORKLET_NAME" VARCHAR)
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


  -- Component sq_cc_check, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_check AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS doc_id_t,
                $2  AS doc_rltd_type_cd,
                $3  AS rltd_doc_id,
                $4  AS doc_rltd_strt_dt,
                $5  AS doc_rltd_end_dt,
                $6  AS edw_strt_dttm,
                $7  AS edw_end_dttm,
                $8  AS trans_strt_dttm,
                $9  AS trans_end_dttm,
                $10 AS doc_id,
                $11 AS out_cmb_doc_type,
                $12 AS createtime,
                $13 AS doc_id1,
                $14 AS updatetime,
                $15 AS edw_strt_dttm1,
                $16 AS edw_end_dttm1,
                $17 AS out_update_flag,
                $18 AS out_src_cd,
                $19 AS retired,
                $20 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT doc_id_t ,
                                                doc_rltd_type_cd,
                                                rltd_doc_id,
                                                doc_rltd_strt_dt,
                                                doc_rltd_end_dt,
                                                edw_strt_dttm,
                                                edw_end_dttm ,
                                                trans_strt_dttm,
                                                trans_end_dttm,
                                                doc_id,
                                                out_cmb_doc_type,
                                                createtime,
                                                doc_id1,
                                                updatetime ,
                                                current_timestamp AS edw_strt_dttm,
                                                edw_end_dttm1,
                                                out_update_flag,
                                                out_src_cd ,
                                                retired
                                         FROM  (
                                                (
                                                          SELECT    scr.cmb_ref_key,
                                                                    scr.cmb_doc_type,
                                                                    scr.cmb_doc_category,
                                                                    scr.ck_ref_key,
                                                                    scr.ck_doc_type,
                                                                    scr.ck_doc_category,
                                                                    CASE
                                                                              WHEN xlat.tgt_idntftn_val5 IS NULL THEN ''UNK''
                                                                              ELSE xlat.tgt_idntftn_val5
                                                                    END AS doc_status,
                                                                    scr.src_cd,
                                                                    CASE
                                                                              WHEN length(trim(scr.cmb_doc_type)) = 0
                                                                              OR        scr.cmb_doc_type IS NULL
                                                                              OR        length (scr.cmb_doc_type) = 0 THEN ''UNK''
                                                                              ELSE doc_lkp.tgt_idntftn_val1
                                                                    END out_cmb_doc_type,
                                                                    CASE
                                                                              WHEN length(trim(scr.cmb_doc_category)) = 0
                                                                              OR        scr.cmb_doc_category IS NULL
                                                                              OR        length (scr.cmb_doc_category) = 0 THEN ''UNK''
                                                                              ELSE doc_cttype_lkp.tgt_idntftn_val2
                                                                    END out_cmb_doc_category ,
                                                                    CASE
                                                                              WHEN length(trim(scr.ck_doc_type)) = 0
                                                                              OR        scr.ck_doc_type IS NULL
                                                                              OR        length (scr.ck_doc_type) = 0 THEN ''UNK''
                                                                              ELSE doc1_lkp.tgt_idntftn_val3
                                                                    END out_ck_doc_type,
                                                                    CASE
                                                                              WHEN length(trim(scr.ck_doc_category)) = 0
                                                                              OR        scr.ck_doc_category IS NULL
                                                                              OR        length (scr.ck_doc_category) = 0 THEN ''UNK''
                                                                              ELSE doc_cktype_lkp.tgt_idntftn_val4
                                                                    END out_ck_doc_category,
                                                                    xlat.tgt_idntftn_val5,
                                                                    src_cd.tgt_idntftn_val6 AS out_src_cd ,
                                                                    tgt.doc_rltd_strt_dt,
                                                                    tgt.doc_rltd_end_dt,
                                                                    tgt.edw_strt_dttm,
                                                                    tgt.edw_end_dttm ,
                                                                    tgt.trans_strt_dttm,
                                                                    tgt.trans_end_dttm,
                                                                    tgt. doc_id_t,
                                                                    tgt.doc_rltd_type_cd,
                                                                    tgt.rltd_doc_id,
                                                                    doc1.doc_id,
                                                                    scr.createtime,
                                                                    scr.updatetime,
                                                                    cast(''9999-12-31 23:59:59.999999'' AS timestamp ) AS edw_end_dttm1,
                                                                    doc.doc_id                                       AS doc_id1,
                                                                    cast(trim(tgt.doc_id_t )
                                                                              ||trim(tgt.doc_rltd_type_cd )
                                                                              ||trim(tgt.rltd_doc_id)
                                                                              ||trim(tgt.doc_rltd_strt_dt) AS VARCHAR(100) ) AS target_data,
                                                                    cast(trim(doc1.doc_id )
                                                                              ||trim(out_cmb_doc_type )
                                                                              ||trim(doc.doc_id)
                                                                              ||trim(scr.createtime) AS VARCHAR(100) ) AS soruce_data,
                                                                    CASE
                                                                              WHEN target_data IS NULL THEN ''I''
                                                                              WHEN target_data<>soruce_data THEN ''U''
                                                                              ELSE ''R''
                                                                    END AS out_update_flag,
                                                                    scr.retired
                                                          FROM     (
                                                                    (
                                                                                    SELECT DISTINCT cast(cc_check.combinedchecknumber_alfa_stg AS VARCHAR(1000)) AS cmb_ref_key,
                                                                                                    cast(''DOC_TYPE6'' AS                           VARCHAR(50))   AS cmb_doc_type,
                                                                                                    cast(''DOC_CTGY_TYPE1'' AS                      VARCHAR(50))   AS cmb_doc_category,
                                                                                                    cast(cc_check.publicid_stg AS                 VARCHAR(1000)) AS ck_ref_key,
                                                                                                    cast(''DOC_TYPE1'' AS                           VARCHAR(50))   AS ck_doc_type,
                                                                                                    cast(''DOC_CTGY_TYPE1'' AS                      VARCHAR(50))   AS ck_doc_category,
                                                                                                    cctl_transactionstatus.typecode_stg                          AS doc_status,
                                                                                                    cast(''SRC_SYS6'' AS VARCHAR(50))                              AS src_cd,
                                                                                                    date_trunc(day, cc_check.createtime_stg)                     AS createtime,
                                                                                                    cc_check.retired_stg                                         AS retired,
                                                                                                    cc_check.updatetime_stg                                         updatetime
                                                                                    FROM            db_t_prod_stag.cc_claim
                                                                                    inner join      db_t_prod_stag.cctl_claimstate
                                                                                    ON              cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                    join            db_t_prod_stag.cc_check
                                                                                    ON              cc_claim.id_stg = cc_check.claimid_stg
                                                                                    join            db_t_prod_stag.cc_transaction
                                                                                    ON              cc_check.id_stg =cc_transaction.checkid_stg
                                                                                    join            db_t_prod_stag.cc_transactionlineitem
                                                                                    ON              cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg
                                                                                    join            db_t_prod_stag.cctl_transactionstatus
                                                                                    ON              cc_check.status_stg = cctl_transactionstatus.id_stg
                                                                                    join            db_t_prod_stag.cctl_paymentmethod
                                                                                    ON              cc_check.paymentmethod_stg = cctl_paymentmethod.id_stg
                                                                                    WHERE           cctl_claimstate.name_stg <> ''Draft''
                                                                                    AND             cc_check.updatetime_stg>(:START_DTTM)
                                                                                    AND             cc_check.updatetime_stg <= (:END_DTTM)
                                                                                    AND             cctl_paymentmethod.typecode_stg <> ''expenseWithheld_alfa''
                                                                                    AND             cctl_transactionstatus.typecode_stg <> ''voided''
                                                                                    AND             cc_check.combinedchecknumber_alfa_stg IS NOT NULL
                                                                                    AND             claimnumber_stg IS NOT NULL qualify row_number() over (PARTITION BY cmb_ref_key,ck_ref_key,doc_status,cc_check.retired_stg ORDER BY date_trunc(day, cc_check.createtime_stg) DESC)=1 )scr )
                                                          left join
                                                                    (
                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val1 ,
                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val1
                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')doc_lkp
                                                          ON        doc_lkp.src_idntftn_val1=scr.cmb_doc_type
                                                          left join
                                                                    (
                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val2 ,
                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val2
                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')doc_cttype_lkp
                                                          ON        doc_cttype_lkp.src_idntftn_val2=scr.cmb_doc_category
                                                          left join
                                                                    (
                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val3 ,
                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val3
                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )doc1_lkp
                                                          ON        doc1_lkp.src_idntftn_val3=scr.ck_doc_type
                                                          left join
                                                                    (
                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val4 ,
                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val4
                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')doc_cktype_lkp
                                                          ON        doc_cktype_lkp.src_idntftn_val4=scr.ck_doc_category
                                                          left join
                                                                    (
                                                                                    SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val5 ,
                                                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val5
                                                                                    FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                    WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_STS_TYPE''
                                                                                    AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'')xlat
                                                          ON        xlat.src_idntftn_val5=scr.doc_status
                                                          left join db_t_prod_core.doc
                                                          ON        doc.doc_ctgy_type_cd=out_ck_doc_category
                                                          AND       doc.doc_issur_num=scr.ck_ref_key
                                                          AND       doc.doc_type_cd=out_ck_doc_type
                                                          AND       doc.edw_end_dttm=''9999-12-31 23:59:59.999999''
                                                          left join db_t_prod_core.doc doc1
                                                          ON        doc1.doc_ctgy_type_cd=out_cmb_doc_category
                                                          AND       doc1.doc_issur_num=scr.cmb_ref_key
                                                          AND       doc1.doc_type_cd=out_cmb_doc_type
                                                          AND       doc1.edw_end_dttm=''9999-12-31 23:59:59.999999''
                                                          left join
                                                                    (
                                                                             SELECT   doc_rltd.doc_rltd_strt_dt AS doc_rltd_strt_dt,
                                                                                      doc_rltd.doc_rltd_end_dt  AS doc_rltd_end_dt,
                                                                                      doc_rltd.edw_strt_dttm    AS edw_strt_dttm,
                                                                                      doc_rltd.edw_end_dttm     AS edw_end_dttm,
                                                                                      doc_rltd.trans_strt_dttm  AS trans_strt_dttm,
                                                                                      doc_rltd.trans_end_dttm   AS trans_end_dttm,
                                                                                      doc_rltd.doc_id           AS doc_id_t,
                                                                                      doc_rltd.doc_rltd_type_cd AS doc_rltd_type_cd,
                                                                                      doc_rltd.rltd_doc_id      AS rltd_doc_id
                                                                             FROM     db_t_prod_core.doc_rltd qualify row_number () over ( PARTITION BY doc_id,doc_rltd_type_cd, rltd_doc_id ORDER BY edw_end_dttm DESC)=1)tgt
                                                          ON        tgt.doc_id_t=doc1.doc_id
                                                          AND       upper(tgt.doc_rltd_type_cd)=upper(out_cmb_doc_type)
                                                          AND       tgt.rltd_doc_id=doc.doc_id
                                                          left join
                                                                    (
                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val6 ,
                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val6
                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')src_cd
                                                          ON        src_cd.src_idntftn_val6=scr.src_cd ) a ) ) src ) );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT sq_cc_check.doc_id_t         AS lkp_doc_id,
                sq_cc_check.doc_rltd_type_cd AS lkp_doc_rltd_type_cd,
                sq_cc_check.rltd_doc_id      AS lkp_rltd_doc_id,
                sq_cc_check.doc_rltd_strt_dt AS lkp_doc_rltd_strt_dt,
                sq_cc_check.doc_rltd_end_dt  AS lkp_doc_rltd_end_dt,
                sq_cc_check.edw_strt_dttm    AS lkp_edw_strt_dttm,
                sq_cc_check.edw_end_dttm     AS lkp_edw_end_dttm,
                sq_cc_check.trans_strt_dttm  AS lkp_trns_strt_dttm,
                sq_cc_check.trans_end_dttm   AS lkp_trns_end_dttm,
                sq_cc_check.doc_id           AS doc_id1,
                sq_cc_check.out_cmb_doc_type AS cmbn_doc_type,
                sq_cc_check.createtime       AS createtime,
                sq_cc_check.doc_id1          AS doc_id2,
                sq_cc_check.updatetime       AS updatetime,
                sq_cc_check.edw_strt_dttm1   AS edw_strt_dttm,
                sq_cc_check.edw_end_dttm1    AS edw_end_dttm,
                sq_cc_check.out_update_flag  AS out_updateflag,
                :prcs_id                     AS o_process_id,
                sq_cc_check.out_src_cd       AS out_src_cd,
                sq_cc_check.retired          AS retired,
                sq_cc_check.source_record_id
         FROM   sq_cc_check );
  -- Component rtr_check_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_check_ins_upd_INSERT as
  SELECT exp_ins_upd.lkp_doc_id           AS lkp_doc_id,
         exp_ins_upd.lkp_doc_rltd_type_cd AS lkp_doc_rltd_type_cd,
         exp_ins_upd.lkp_rltd_doc_id      AS lkp_rltd_doc_id,
         exp_ins_upd.lkp_doc_rltd_strt_dt AS lkp_doc_rltd_strt_dt,
         exp_ins_upd.lkp_doc_rltd_end_dt  AS lkp_doc_rltd_end_dt,
         exp_ins_upd.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_ins_upd.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_ins_upd.doc_id1              AS doc_id1,
         exp_ins_upd.cmbn_doc_type        AS cmbn_doc_type,
         exp_ins_upd.doc_id2              AS doc_id2,
         exp_ins_upd.updatetime           AS updatetime,
         exp_ins_upd.edw_strt_dttm        AS edw_strt_dttm,
         exp_ins_upd.edw_end_dttm         AS edw_end_dttm,
         exp_ins_upd.out_updateflag       AS out_updateflag,
         exp_ins_upd.o_process_id         AS o_process_id,
         exp_ins_upd.out_src_cd           AS out_src_cd,
         exp_ins_upd.retired              AS retired,
         exp_ins_upd.createtime           AS createtime,
         exp_ins_upd.lkp_trns_strt_dttm   AS lkp_trns_strt_dttm,
         exp_ins_upd.lkp_trns_end_dttm    AS lkp_trns_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  (
                exp_ins_upd.doc_id1 IS NOT NULL
         AND    exp_ins_upd.doc_id2 IS NOT NULL )
  AND    (
                exp_ins_upd.out_updateflag = ''I'' )
  OR     (
                exp_ins_upd.retired = 0
         AND    exp_ins_upd.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_check_ins_upd_Retired, Type ROUTER Output Group Retired
    create or replace temporary table rtr_check_ins_upd_retired as
  SELECT exp_ins_upd.lkp_doc_id           AS lkp_doc_id,
         exp_ins_upd.lkp_doc_rltd_type_cd AS lkp_doc_rltd_type_cd,
         exp_ins_upd.lkp_rltd_doc_id      AS lkp_rltd_doc_id,
         exp_ins_upd.lkp_doc_rltd_strt_dt AS lkp_doc_rltd_strt_dt,
         exp_ins_upd.lkp_doc_rltd_end_dt  AS lkp_doc_rltd_end_dt,
         exp_ins_upd.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_ins_upd.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_ins_upd.doc_id1              AS doc_id1,
         exp_ins_upd.cmbn_doc_type        AS cmbn_doc_type,
         exp_ins_upd.doc_id2              AS doc_id2,
         exp_ins_upd.updatetime           AS updatetime,
         exp_ins_upd.edw_strt_dttm        AS edw_strt_dttm,
         exp_ins_upd.edw_end_dttm         AS edw_end_dttm,
         exp_ins_upd.out_updateflag       AS out_updateflag,
         exp_ins_upd.o_process_id         AS o_process_id,
         exp_ins_upd.out_src_cd           AS out_src_cd,
         exp_ins_upd.retired              AS retired,
         exp_ins_upd.createtime           AS createtime,
         exp_ins_upd.lkp_trns_strt_dttm   AS lkp_trns_strt_dttm,
         exp_ins_upd.lkp_trns_end_dttm    AS lkp_trns_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.out_updateflag = ''R''
  AND    exp_ins_upd.retired != 0
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_check_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
    create or replace temporary table rtr_check_ins_upd_update as
  SELECT exp_ins_upd.lkp_doc_id           AS lkp_doc_id,
         exp_ins_upd.lkp_doc_rltd_type_cd AS lkp_doc_rltd_type_cd,
         exp_ins_upd.lkp_rltd_doc_id      AS lkp_rltd_doc_id,
         exp_ins_upd.lkp_doc_rltd_strt_dt AS lkp_doc_rltd_strt_dt,
         exp_ins_upd.lkp_doc_rltd_end_dt  AS lkp_doc_rltd_end_dt,
         exp_ins_upd.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_ins_upd.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_ins_upd.doc_id1              AS doc_id1,
         exp_ins_upd.cmbn_doc_type        AS cmbn_doc_type,
         exp_ins_upd.doc_id2              AS doc_id2,
         exp_ins_upd.updatetime           AS updatetime,
         exp_ins_upd.edw_strt_dttm        AS edw_strt_dttm,
         exp_ins_upd.edw_end_dttm         AS edw_end_dttm,
         exp_ins_upd.out_updateflag       AS out_updateflag,
         exp_ins_upd.o_process_id         AS o_process_id,
         exp_ins_upd.out_src_cd           AS out_src_cd,
         exp_ins_upd.retired              AS retired,
         exp_ins_upd.createtime           AS createtime,
         exp_ins_upd.lkp_trns_strt_dttm   AS lkp_trns_strt_dttm,
         exp_ins_upd.lkp_trns_end_dttm    AS lkp_trns_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.out_updateflag = ''U''
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_doc_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_doc_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_ins_upd_update.doc_id1            AS doc_id11,
                rtr_check_ins_upd_update.cmbn_doc_type      AS cmbn_doc_type1,
                rtr_check_ins_upd_update.doc_id2            AS doc_id21,
                rtr_check_ins_upd_update.updatetime         AS updatetime1,
                NULL                                        AS lkp_edw_strt_dttm3,
                rtr_check_ins_upd_update.edw_end_dttm       AS edw_end_dttm1,
                rtr_check_ins_upd_update.o_process_id       AS o_process_id1,
                rtr_check_ins_upd_update.out_src_cd         AS out_src_cd1,
                rtr_check_ins_upd_update.retired            AS retired1,
                NULL                                        AS trans_strt_dttm3,
                rtr_check_ins_upd_update.createtime         AS createtime3,
                rtr_check_ins_upd_update.lkp_trns_strt_dttm AS lkp_trns_strt_dttm3,
                rtr_check_ins_upd_update.lkp_trns_end_dttm  AS lkp_trns_end_dttm3,
                rtr_check_ins_upd_update.edw_strt_dttm      AS edw_strt_dttm3,
                0                                           AS update_strategy_action,
                rtr_check_ins_upd_update.source_record_id
         FROM   rtr_check_ins_upd_update );
  -- Component upd_doc_ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_doc_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_ins_upd_insert.doc_id1       AS doc_id11,
                rtr_check_ins_upd_insert.cmbn_doc_type AS cmbn_doc_type1,
                rtr_check_ins_upd_insert.doc_id2       AS doc_id21,
                rtr_check_ins_upd_insert.updatetime    AS updatetime1,
                rtr_check_ins_upd_insert.o_process_id  AS o_process_id1,
                rtr_check_ins_upd_insert.out_src_cd    AS out_src_cd1,
                rtr_check_ins_upd_insert.edw_strt_dttm AS edw_strt_dttm1,
                rtr_check_ins_upd_insert.edw_end_dttm  AS edw_end_dttm1,
                rtr_check_ins_upd_insert.retired       AS retired1,
                rtr_check_ins_upd_insert.createtime    AS createtime1,
                0                                      AS update_strategy_action,
                source_record_id
         FROM   rtr_check_ins_upd_insert );
  -- Component upd_doc_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_doc_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_ins_upd_update.doc_id1            AS doc_id3,
                rtr_check_ins_upd_update.cmbn_doc_type      AS doc_rltd_type_cd3,
                rtr_check_ins_upd_update.doc_id2            AS rltd_doc_id3,
                NULL                                        AS doc_rltd_strt_dt3,
                NULL                                        AS doc_rltd_end_dt3,
                rtr_check_ins_upd_update.lkp_edw_strt_dttm  AS lkp_edw_strt_dttm3,
                NULL                                        AS lkp_edw_end_dttm3,
                NULL                                        AS o_process_id3,
                NULL                                        AS out_src_cd3,
                NULL                                        AS retired3,
                rtr_check_ins_upd_update.lkp_trns_strt_dttm AS lkp_trns_strt_dttm3,
                rtr_check_ins_upd_update.lkp_trns_end_dttm  AS lkp_trns_end_dttm3,
                rtr_check_ins_upd_update.updatetime         AS updatetime3,
                rtr_check_ins_upd_update.edw_strt_dttm      AS edw_strt_dttm3,
                1                                           AS update_strategy_action,
                rtr_check_ins_upd_update.source_record_id
         FROM   rtr_check_ins_upd_update );
  -- Component fil_upd_ins, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_upd_ins AS
  (
         SELECT upd_doc_ins.doc_id11            AS doc_id11,
                upd_doc_ins.cmbn_doc_type1      AS cmbn_doc_type1,
                upd_doc_ins.doc_id21            AS doc_id21,
                upd_doc_ins.updatetime1         AS updatetime1,
                upd_doc_ins.edw_strt_dttm3      AS edw_strt_dttm1,
                upd_doc_ins.edw_end_dttm1       AS edw_end_dttm1,
                upd_doc_ins.o_process_id1       AS o_process_id1,
                upd_doc_ins.out_src_cd1         AS out_src_cd1,
                upd_doc_ins.retired1            AS retired1,
                upd_doc_ins.createtime3         AS createtime3,
                upd_doc_ins.lkp_trns_strt_dttm3 AS lkp_trns_strt_dttm3,
                upd_doc_ins.lkp_trns_end_dttm3  AS lkp_trns_end_dttm3,
                upd_doc_ins.source_record_id
         FROM   upd_doc_ins
         WHERE  upd_doc_ins.retired1 = 0 );
  -- Component upd_doc_upd_Retired_Rejected, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_doc_upd_retired_rejected AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_ins_upd_retired.doc_id1            AS doc_id14,
                rtr_check_ins_upd_retired.cmbn_doc_type      AS cmbn_doc_type4,
                rtr_check_ins_upd_retired.doc_id2            AS doc_id24,
                rtr_check_ins_upd_retired.o_process_id       AS o_process_id4,
                rtr_check_ins_upd_retired.lkp_edw_strt_dttm  AS lkp_edw_strt_dttm4,
                NULL                                         AS trans_strt_dttm4,
                rtr_check_ins_upd_retired.lkp_trns_strt_dttm AS lkp_trns_strt_dttm4,
                1                                            AS update_strategy_action,
                rtr_check_ins_upd_retired.source_record_id
         FROM   rtr_check_ins_upd_retired );
  -- Component exp_ins_new, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_new AS
  (
         SELECT upd_doc_ins_new.doc_id11       AS doc_id11,
                upd_doc_ins_new.cmbn_doc_type1 AS cmbn_doc_type1,
                upd_doc_ins_new.doc_id21       AS doc_id21,
                upd_doc_ins_new.updatetime1    AS updatetime1,
                upd_doc_ins_new.o_process_id1  AS o_process_id1,
                upd_doc_ins_new.out_src_cd1    AS out_src_cd1,
                upd_doc_ins_new.edw_strt_dttm1 AS edw_strt_dttm1,
                CASE
                       WHEN upd_doc_ins_new.retired1 != 0 THEN current_timestamp
                       ELSE upd_doc_ins_new.edw_end_dttm1
                END                         AS out_edw_end_dttm,
                upd_doc_ins_new.createtime1 AS createtime1,
                upd_doc_ins_new.source_record_id
         FROM   upd_doc_ins_new );
  -- Component exp_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_upd AS
  (
         SELECT upd_doc_update.doc_id3                            AS doc_id3,
                upd_doc_update.doc_rltd_type_cd3                  AS doc_rltd_type_cd3,
                upd_doc_update.rltd_doc_id3                       AS rltd_doc_id3,
                upd_doc_update.lkp_edw_strt_dttm3                 AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, upd_doc_update.edw_strt_dttm3) AS out_edw_end_dttm,
                dateadd(''second'', - 1, upd_doc_update.updatetime3)    AS out_trns_end_dttm3,
                upd_doc_update.source_record_id
         FROM   upd_doc_update );
  -- Component tgt_ins_new_DOC_RLTD, Type TARGET
  INSERT INTO db_t_prod_core.doc_rltd
              (
                          doc_id,
                          doc_rltd_type_cd,
                          rltd_doc_id,
                          doc_rltd_strt_dt,
                          prcs_id,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_ins_new.doc_id11         AS doc_id,
         exp_ins_new.cmbn_doc_type1   AS doc_rltd_type_cd,
         exp_ins_new.doc_id21         AS rltd_doc_id,
         exp_ins_new.createtime1      AS doc_rltd_strt_dt,
         exp_ins_new.o_process_id1    AS prcs_id,
         exp_ins_new.out_src_cd1      AS src_sys_cd,
         exp_ins_new.edw_strt_dttm1   AS edw_strt_dttm,
         exp_ins_new.out_edw_end_dttm AS edw_end_dttm,
         exp_ins_new.updatetime1      AS trans_strt_dttm
  FROM   exp_ins_new;
  
  -- Component exp_upd_ret, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_upd_ret AS
  (
         SELECT upd_doc_upd_retired_rejected.doc_id14            AS doc_id,
                upd_doc_upd_retired_rejected.cmbn_doc_type4      AS doc_rltd_type_cd,
                upd_doc_upd_retired_rejected.doc_id24            AS rltd_doc_id,
                upd_doc_upd_retired_rejected.lkp_edw_strt_dttm4  AS lkp2_edw_strt_dttm3,
                dateadd(''second'', - 1, current_timestamp)            AS edw_end_dttm3_exp,
                upd_doc_upd_retired_rejected.lkp_trns_strt_dttm4 AS lkp_trns_strt_dttm4,
                dateadd(''second'', - 1, current_timestamp)            AS trans_end_dttm,
                upd_doc_upd_retired_rejected.source_record_id
         FROM   upd_doc_upd_retired_rejected );
  -- Component exp_upd_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_upd_ins AS
  (
         SELECT fil_upd_ins.doc_id11       AS doc_id11,
                fil_upd_ins.cmbn_doc_type1 AS cmbn_doc_type1,
                fil_upd_ins.doc_id21       AS doc_id21,
                fil_upd_ins.updatetime1    AS updatetime1,
                fil_upd_ins.edw_strt_dttm1 AS edw_strt_dttm1,
                fil_upd_ins.edw_end_dttm1  AS edw_end_dttm1,
                fil_upd_ins.o_process_id1  AS o_process_id1,
                fil_upd_ins.out_src_cd1    AS out_src_cd1,
                fil_upd_ins.createtime3    AS createtime3,
                fil_upd_ins.source_record_id
         FROM   fil_upd_ins );
  -- Component tgt_upd_DOC_RLTD, Type TARGET
  merge
  INTO         db_t_prod_core.doc_rltd
  USING        exp_upd
  ON (
                            doc_rltd.doc_id = exp_upd.doc_id3
               AND          doc_rltd.doc_rltd_type_cd = exp_upd.doc_rltd_type_cd3
               AND          doc_rltd.rltd_doc_id = exp_upd.rltd_doc_id3
               AND          doc_rltd.edw_strt_dttm = exp_upd.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    doc_id = exp_upd.doc_id3,
         doc_rltd_type_cd = exp_upd.doc_rltd_type_cd3,
         rltd_doc_id = exp_upd.rltd_doc_id3,
         edw_strt_dttm = exp_upd.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_upd.out_edw_end_dttm,
         trans_end_dttm = exp_upd.out_trns_end_dttm3;
  
  -- Component tgt_upd_ins_DOC_RLTD, Type TARGET
  INSERT INTO db_t_prod_core.doc_rltd
              (
                          doc_id,
                          doc_rltd_type_cd,
                          rltd_doc_id,
                          doc_rltd_strt_dt,
                          prcs_id,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_upd_ins.doc_id11       AS doc_id,
         exp_upd_ins.cmbn_doc_type1 AS doc_rltd_type_cd,
         exp_upd_ins.doc_id21       AS rltd_doc_id,
         exp_upd_ins.createtime3    AS doc_rltd_strt_dt,
         exp_upd_ins.o_process_id1  AS prcs_id,
         exp_upd_ins.out_src_cd1    AS src_sys_cd,
         exp_upd_ins.edw_strt_dttm1 AS edw_strt_dttm,
         exp_upd_ins.edw_end_dttm1  AS edw_end_dttm,
         exp_upd_ins.updatetime1    AS trans_strt_dttm
  FROM   exp_upd_ins;
  
  -- Component tgt_upd_ret_DOC_RLTD, Type TARGET
  merge
  INTO         db_t_prod_core.doc_rltd
  USING        exp_upd_ret
  ON (
                            doc_rltd.doc_id = exp_upd_ret.doc_id
               AND          doc_rltd.doc_rltd_type_cd = exp_upd_ret.doc_rltd_type_cd
               AND          doc_rltd.rltd_doc_id = exp_upd_ret.rltd_doc_id
               AND          doc_rltd.edw_strt_dttm = exp_upd_ret.lkp2_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    doc_id = exp_upd_ret.doc_id,
         doc_rltd_type_cd = exp_upd_ret.doc_rltd_type_cd,
         rltd_doc_id = exp_upd_ret.rltd_doc_id,
         edw_strt_dttm = exp_upd_ret.lkp2_edw_strt_dttm3,
         edw_end_dttm = exp_upd_ret.edw_end_dttm3_exp,
         trans_strt_dttm = exp_upd_ret.lkp_trns_strt_dttm4,
         trans_end_dttm = exp_upd_ret.trans_end_dttm;

END;
';