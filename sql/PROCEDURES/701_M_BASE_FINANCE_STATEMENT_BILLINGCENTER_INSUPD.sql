-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_STATEMENT_BILLINGCENTER_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;

BEGIN
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


  -- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_doc_ctgy_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_doc_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component src_sq_bc_invoice, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE src_sq_bc_invoice AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS eventdate,
                $2 AS invoicenumber,
                $3 AS createtime,
                $4 AS retired,
                $5 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT bc_invoice.eventdate_stg     AS eventdate,
                                                bc_invoice.invoicenumber_stg AS invoicenumber,
                                                bc_invoice.createtime_stg    AS createtime,
                                                bc_invoice.retired_stg       AS retired
                                         FROM   db_t_prod_stag.bc_invoice
                                         WHERE  bc_invoice.updatetime_stg > (:START_DTTM)
                                         AND    bc_invoice.updatetime_stg <= (:END_DTTM) ) src ) );
  -- Component exp_src_pass, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_pass AS
  (
            SELECT    src_sq_bc_invoice.eventdate     AS eventdate,
                      src_sq_bc_invoice.invoicenumber AS invoicenumber,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */
                      AS typecode,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */
                                                   AS doc_category,
                      src_sq_bc_invoice.createtime AS createtime,
                      src_sq_bc_invoice.retired    AS retired,
                      src_sq_bc_invoice.source_record_id,
                      row_number() over (PARTITION BY src_sq_bc_invoice.source_record_id ORDER BY src_sq_bc_invoice.source_record_id) AS rnk
            FROM      src_sq_bc_invoice
            left join lkp_teradata_etl_ref_xlat_doc_type lkp_1
            ON        lkp_1.src_idntftn_val = ''DOC_TYPE3''
            left join lkp_teradata_etl_ref_xlat_doc_ctgy_type lkp_2
            ON        lkp_2.src_idntftn_val = ''DOC_CTGY_TYPE4'' qualify rnk = 1 );
  -- Component LKP_DOC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_doc AS
  (
            SELECT    lkp.doc_id,
                      exp_src_pass.source_record_id,
                      row_number() over(PARTITION BY exp_src_pass.source_record_id ORDER BY lkp.doc_id DESC) rnk
            FROM      exp_src_pass
            left join
                      (
                               SELECT   doc.doc_id           AS doc_id,
                                        doc.doc_issur_num    AS doc_issur_num,
                                        doc.doc_type_cd      AS doc_type_cd,
                                        doc.doc_ctgy_type_cd AS doc_ctgy_type_cd
                               FROM     db_t_prod_core.doc qualify row_number () over ( PARTITION BY doc_issur_num, doc_type_cd, doc_ctgy_type_cd ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.doc_issur_num = exp_src_pass.invoicenumber
            AND       lkp.doc_type_cd = exp_src_pass.typecode
            AND       lkp.doc_ctgy_type_cd = exp_src_pass.doc_category qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_doc.doc_id          AS doc_id,
                        exp_src_pass.eventdate  AS eventdate,
                        exp_src_pass.createtime AS createtime,
                        :prcs_id                AS o_process_id,
                        exp_src_pass.retired    AS retired,
                        exp_src_pass.source_record_id
             FROM       exp_src_pass
             inner join lkp_doc
             ON         exp_src_pass.source_record_id = lkp_doc.source_record_id );
  -- Component exp_SrcFields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_srcfields AS
  (
         SELECT exp_data_transformation.doc_id       AS in_stmt_doc_id,
                exp_data_transformation.eventdate    AS in_stmt_issu_dt,
                exp_data_transformation.o_process_id AS in_prcs_id,
                exp_data_transformation.createtime   AS createtime,
                exp_data_transformation.retired      AS retired,
                exp_data_transformation.source_record_id
         FROM   exp_data_transformation );
  -- Component LKP_STMT1, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_stmt1 AS
  (
            SELECT    lkp.stmt_doc_id,
                      lkp.stmt_issu_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_srcfields.source_record_id,
                      row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.stmt_doc_id ASC,lkp.stmt_issu_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_srcfields
            left join
                      (
                               SELECT   stmt.stmt_issu_dttm AS stmt_issu_dttm,
                                        stmt.edw_strt_dttm  AS edw_strt_dttm,
                                        stmt.edw_end_dttm   AS edw_end_dttm,
                                        stmt.stmt_doc_id    AS stmt_doc_id
                               FROM     db_t_prod_core.stmt qualify row_number () over (PARTITION BY stmt_doc_id ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.stmt_doc_id = exp_srcfields.in_stmt_doc_id qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_srcfields.in_stmt_doc_id                                          AS in_stmt_doc_id,
                        exp_srcfields.in_stmt_issu_dt                                         AS in_stmt_issu_dt,
                        exp_srcfields.in_prcs_id                                              AS in_prcs_id,
                        lkp_stmt1.stmt_doc_id                                                 AS lkp_stmt_doc_id,
                        lkp_stmt1.edw_strt_dttm                                               AS lkp_edw_strt_dttm,
                        lkp_stmt1.edw_end_dttm                                                AS lkp_edw_end_dttm,
                        exp_srcfields.createtime                                              AS stmt_strt_dt,
                        md5 ( ltrim ( rtrim ( to_char ( exp_srcfields.in_stmt_issu_dt ) ) ) ) AS v_src_md5,
                        md5 ( ltrim ( rtrim ( to_char ( lkp_stmt1.stmt_issu_dttm ) ) ) )      AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''X''
                                                         ELSE ''U''
                                              END
                        END                   AS o_src_tgt,
                        exp_srcfields.retired AS retired,
                        current_timestamp     AS startdate,
                        CASE
                                   WHEN exp_srcfields.retired = 0 THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                   ELSE current_timestamp
                        END AS enddate,
                        exp_srcfields.source_record_id
             FROM       exp_srcfields
             inner join lkp_stmt1
             ON         exp_srcfields.source_record_id = lkp_stmt1.source_record_id );
  -- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_cdc_insert as
  SELECT exp_cdc_check.in_stmt_doc_id    AS in_stmt_doc_id,
         exp_cdc_check.in_stmt_issu_dt   AS in_stmt_issu_dt,
         exp_cdc_check.in_prcs_id        AS in_prcs_id,
         exp_cdc_check.lkp_stmt_doc_id   AS lkp_stmt_doc_id,
         exp_cdc_check.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_cdc_check.o_src_tgt         AS o_src_tgt,
         exp_cdc_check.startdate         AS startdate,
         exp_cdc_check.enddate           AS enddate,
         exp_cdc_check.stmt_strt_dt      AS stmt_strt_dt,
         exp_cdc_check.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_cdc_check.retired           AS retired,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.in_stmt_doc_id IS NOT NULL
  AND    (
                exp_cdc_check.o_src_tgt = ''I''
         OR     exp_cdc_check.o_src_tgt = ''U''
         OR     (
                       exp_cdc_check.retired = 0
                AND    exp_cdc_check.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_CDC_Retire, Type ROUTER Output Group Retire
  create or replace temporary table rtr_cdc_retire as
  SELECT exp_cdc_check.in_stmt_doc_id    AS in_stmt_doc_id,
         exp_cdc_check.in_stmt_issu_dt   AS in_stmt_issu_dt,
         exp_cdc_check.in_prcs_id        AS in_prcs_id,
         exp_cdc_check.lkp_stmt_doc_id   AS lkp_stmt_doc_id,
         exp_cdc_check.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_cdc_check.o_src_tgt         AS o_src_tgt,
         exp_cdc_check.startdate         AS startdate,
         exp_cdc_check.enddate           AS enddate,
         exp_cdc_check.stmt_strt_dt      AS stmt_strt_dt,
         exp_cdc_check.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_cdc_check.retired           AS retired,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''X''
  AND    exp_cdc_check.retired != 0
  AND    exp_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_cdc_retire.lkp_stmt_doc_id   AS lkp_stmt_doc_id3,
                rtr_cdc_retire.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                1                                AS update_strategy_action,
				source_record_id
         FROM   rtr_cdc_retire );
  -- Component upd_strtgy_Ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_strtgy_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_cdc_insert.in_stmt_doc_id  AS in_stmt_doc_id1,
                rtr_cdc_insert.in_stmt_issu_dt AS in_stmt_issu_dt1,
                rtr_cdc_insert.in_prcs_id      AS in_prcs_id1,
                rtr_cdc_insert.startdate       AS startdate1,
                rtr_cdc_insert.enddate         AS enddate1,
                rtr_cdc_insert.stmt_strt_dt    AS stmt_strt_dt1,
                rtr_cdc_insert.retired         AS retired1,
                0                              AS update_strategy_action,
				rtr_cdc_insert.source_record_id
         FROM   rtr_cdc_insert );
  -- Component exp_pass_to_tgt_ins_new, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins_new AS
  (
         SELECT upd_strtgy_ins_new.in_stmt_doc_id1  AS in_stmt_doc_id1,
                upd_strtgy_ins_new.in_stmt_issu_dt1 AS in_stmt_issu_dt1,
                upd_strtgy_ins_new.in_prcs_id1      AS in_prcs_id1,
                upd_strtgy_ins_new.startdate1       AS startdate1,
                upd_strtgy_ins_new.stmt_strt_dt1    AS stmt_strt_dt1,
                CASE
                       WHEN upd_strtgy_ins_new.retired1 <> 0 THEN upd_strtgy_ins_new.startdate1
                       ELSE upd_strtgy_ins_new.enddate1
                END AS out_edw_end_date,
                upd_strtgy_ins_new.source_record_id
         FROM   upd_strtgy_ins_new );
  -- Component exp_retire, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retire AS
  (
         SELECT upd_retire.lkp_stmt_doc_id3   AS lkp_stmt_doc_id3,
                upd_retire.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                current_timestamp             AS o_enddate,
                upd_retire.source_record_id
         FROM   upd_retire );
  -- Component tgt_stmt_NewInsert, Type TARGET
  INSERT INTO db_t_prod_core.stmt
              (
                          stmt_doc_id,
                          stmt_issu_dttm,
                          prcs_id,
                          stmt_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_tgt_ins_new.in_stmt_doc_id1  AS stmt_doc_id,
         exp_pass_to_tgt_ins_new.in_stmt_issu_dt1 AS stmt_issu_dttm,
         exp_pass_to_tgt_ins_new.in_prcs_id1      AS prcs_id,
         exp_pass_to_tgt_ins_new.stmt_strt_dt1    AS stmt_strt_dttm,
         exp_pass_to_tgt_ins_new.startdate1       AS edw_strt_dttm,
         exp_pass_to_tgt_ins_new.out_edw_end_date AS edw_end_dttm
  FROM   exp_pass_to_tgt_ins_new;
  
  -- Component tgt_stmt_retire, Type TARGET
  merge
  INTO         db_t_prod_core.stmt
  USING        exp_retire
  ON (
                            stmt.stmt_doc_id = exp_retire.lkp_stmt_doc_id3
               AND          stmt.edw_strt_dttm = exp_retire.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    stmt_doc_id = exp_retire.lkp_stmt_doc_id3,
         edw_strt_dttm = exp_retire.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_retire.o_enddate;
  
  -- Component tgt_stmt_retire, Type Post SQL
  UPDATE db_t_prod_core.stmt
  SET    edw_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT stmt_doc_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY stmt_doc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.stmt ) a
    WHERE  stmt.edw_strt_dttm = a.edw_strt_dttm
  AND    stmt.stmt_doc_id=a.stmt_doc_id
  AND    cast(stmt.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead IS NOT NULL;

END;
';