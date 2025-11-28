-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AR_INVOICE_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_bc_invoice, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS eventdate,
                $2 AS retired,
                $3 AS invoicenumber,
                $4 AS doc_id,
                $5 AS ar_invc_id,
                $6 AS edw_strt_dttm,
                $7 AS edw_end_dttm,
                $8 AS ar_invc_strt_dttm,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    src.eventdate,
                                                      src.retired,
                                                      src.invoicenumber,
                                                      lkp_doc.doc_id,
                                                      lkp_ar_invc.ar_invc_id,
                                                      lkp_ar_invc.edw_strt_dttm,
                                                      lkp_ar_invc.edw_end_dttm,
                                                      lkp_ar_invc.ar_invc_strt_dttm
                                            FROM      (
                                                                      SELECT DISTINCT bc_invoice.eventdate_stg     AS eventdate,
                                                                                      bc_invoice.retired_stg       AS retired,
                                                                                      bc_invoice.invoicenumber_stg AS invoicenumber,
                                                                                      ''INVOICE''                    AS doc_type,
                                                                                      ''BILL''                       AS doc_ctgy_type
                                                                      FROM            db_t_prod_stag.bc_invoice
                                                                      WHERE           bc_invoice.updatetime_stg > ($start_dttm)
                                                                      AND             bc_invoice.updatetime_stg <= ($end_dttm))src
                                            left join
                                                      (
                                                               SELECT   doc.doc_id        AS doc_id,
                                                                        doc.doc_issur_num AS doc_issur_num
                                                               FROM     db_t_prod_core.doc
                                                               WHERE    doc_type_cd = ''INVOICE''
                                                               AND      doc_ctgy_type_cd =''BILL''
                                                               AND      doc_issur_num IN
                                                                                          (
                                                                                          SELECT DISTINCT invoicenumber_stg
                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                          WHERE           bc_invoice.updatetime_stg > ($start_dttm)
                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)) qualify row_number () over (PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1) lkp_doc
                                            ON        src.invoicenumber=lkp_doc.doc_issur_num
                                            left join
                                                      (
                                                               SELECT   ar_invc.edw_strt_dttm     AS edw_strt_dttm,
                                                                        ar_invc.edw_end_dttm      AS edw_end_dttm,
                                                                        ar_invc.ar_invc_strt_dttm AS ar_invc_strt_dttm,
                                                                        ar_invc.ar_invc_id        AS ar_invc_id
                                                               FROM     db_t_prod_core.ar_invc    AS ar_invc qualify row_number () over (PARTITION BY ar_invc_id ORDER BY edw_end_dttm DESC)=1) lkp_ar_invc
                                            ON        lkp_doc.doc_id=lkp_ar_invc.ar_invc_id ) src ) );
  -- Component exp_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through AS
  (
         SELECT sq_bc_invoice.eventdate         AS eventdate,
                sq_bc_invoice.retired           AS retired,
                sq_bc_invoice.doc_id            AS doc_id,
                sq_bc_invoice.ar_invc_id        AS ar_invc_id,
                sq_bc_invoice.edw_strt_dttm     AS edw_strt_dttm,
                sq_bc_invoice.edw_end_dttm      AS edw_end_dttm,
                sq_bc_invoice.ar_invc_strt_dttm AS ar_invc_strt_dttm,
                sq_bc_invoice.source_record_id
         FROM   sq_bc_invoice );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
         SELECT exp_pass_through.eventdate                                             AS in_eventdate,
                exp_pass_through.retired                                               AS in_retired,
                exp_pass_through.doc_id                                                AS in_doc_id,
                to_char ( exp_pass_through.eventdate , ''YYYY-MM-DD'' )                  AS var_eventdate,
                exp_pass_through.ar_invc_id                                            AS lkp_ar_invc_id,
                exp_pass_through.edw_strt_dttm                                         AS lkp_edw_strt_dttm,
                exp_pass_through.edw_end_dttm                                          AS lkp_edw_end_dttm,
                to_char ( exp_pass_through.ar_invc_strt_dttm , ''YYYY-MM-DD'' )          AS var_ar_invc_strt_dt,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                md5 ( var_ar_invc_strt_dt )                                            AS var_orig_chksm,
                md5 ( to_char ( var_eventdate ) )                                      AS var_calc_chksm,
                CASE
                       WHEN var_orig_chksm IS NULL THEN ''I''
                       ELSE
                              CASE
                                     WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                     ELSE ''R''
                              END
                END      AS out_ins_upd,
                $prcs_id AS out_prcs_id,
                ''UNK''    AS out_undefined_not_null_field,
                exp_pass_through.source_record_id
         FROM   exp_pass_through );
  -- Component rtr_ar_invoice_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_ar_invoice_ins_upd_INSERT as
  SELECT exp_check_flag.in_eventdate                 AS eventdate,
         exp_check_flag.in_retired                   AS retired,
         exp_check_flag.in_doc_id                    AS ar_invc_id,
         exp_check_flag.lkp_ar_invc_id               AS lkp_ar_invc_id,
         exp_check_flag.lkp_edw_strt_dttm            AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm             AS lkp_edw_end_dttm,
         exp_check_flag.out_edw_strt_dttm            AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm             AS out_edw_end_dttm,
         exp_check_flag.out_ins_upd                  AS out_ins_upd,
         exp_check_flag.out_prcs_id                  AS out_prcs_id,
         exp_check_flag.out_undefined_not_null_field AS ar_invc_type_cd,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.in_doc_id IS NOT NULL
  AND    ( (
                       exp_check_flag.out_ins_upd = ''I'' )
         OR     (
                       exp_check_flag.in_retired = 0
                AND    exp_check_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_check_flag.out_ins_upd = ''U''
                AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_ar_invoice_ins_upd_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_ar_invoice_ins_upd_RETIRED as
  SELECT exp_check_flag.in_eventdate                 AS eventdate,
         exp_check_flag.in_retired                   AS retired,
         exp_check_flag.in_doc_id                    AS ar_invc_id,
         exp_check_flag.lkp_ar_invc_id               AS lkp_ar_invc_id,
         exp_check_flag.lkp_edw_strt_dttm            AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm             AS lkp_edw_end_dttm,
         exp_check_flag.out_edw_strt_dttm            AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm             AS out_edw_end_dttm,
         exp_check_flag.out_ins_upd                  AS out_ins_upd,
         exp_check_flag.out_prcs_id                  AS out_prcs_id,
         exp_check_flag.out_undefined_not_null_field AS ar_invc_type_cd,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.out_ins_upd = ''R''
  AND    exp_check_flag.in_retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_ar_invc_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ar_invc_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ar_invoice_ins_upd_retired.lkp_ar_invc_id    AS ar_invc_id,
                NULL                                             AS ar_invc_type_cd,
                rtr_ar_invoice_ins_upd_retired.out_prcs_id       AS out_prcs_id,
                rtr_ar_invoice_ins_upd_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
                rtr_ar_invoice_ins_upd_retired.out_edw_strt_dttm AS out_edw_strt_dttm,
                1                                                AS update_strategy_action,
                source_record_id
         FROM   rtr_ar_invoice_ins_upd_retired );
  -- Component upd_ar_invc_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ar_invc_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ar_invoice_ins_upd_insert.ar_invc_id        AS ar_invc_id,
                rtr_ar_invoice_ins_upd_insert.ar_invc_type_cd   AS ar_invc_type_cd,
                rtr_ar_invoice_ins_upd_insert.out_prcs_id       AS out_prcs_id,
                rtr_ar_invoice_ins_upd_insert.out_edw_strt_dttm AS out_edw_strt_dttm,
                rtr_ar_invoice_ins_upd_insert.out_edw_end_dttm  AS out_edw_end_dttm,
                rtr_ar_invoice_ins_upd_insert.eventdate         AS eventdate,
                rtr_ar_invoice_ins_upd_insert.retired           AS retired1,
                0                                               AS update_strategy_action,
                source_record_id
         FROM   rtr_ar_invoice_ins_upd_insert );
  -- Component exp_pass_to_target_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert AS
  (
         SELECT upd_ar_invc_insert.ar_invc_id        AS ar_invc_id,
                upd_ar_invc_insert.ar_invc_type_cd   AS ar_invc_type_cd,
                upd_ar_invc_insert.out_prcs_id       AS out_prcs_id,
                upd_ar_invc_insert.out_edw_strt_dttm AS out_edw_strt_dttm,
                CASE
                       WHEN upd_ar_invc_insert.retired1 = 0 THEN upd_ar_invc_insert.out_edw_end_dttm
                       ELSE current_timestamp
                END                          AS out_edw_end_dttm1,
                upd_ar_invc_insert.eventdate AS eventdate,
                upd_ar_invc_insert.source_record_id
         FROM   upd_ar_invc_insert );
  -- Component tgt_ar_invc_insert, Type TARGET
  INSERT INTO db_t_prod_core.ar_invc
              (
                          ar_invc_id,
                          ar_invc_type_cd,
                          prcs_id,
                          ar_invc_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_insert.ar_invc_id        AS ar_invc_id,
         exp_pass_to_target_insert.ar_invc_type_cd   AS ar_invc_type_cd,
         exp_pass_to_target_insert.out_prcs_id       AS prcs_id,
         exp_pass_to_target_insert.eventdate         AS ar_invc_strt_dttm,
         exp_pass_to_target_insert.out_edw_strt_dttm AS edw_strt_dttm,
         exp_pass_to_target_insert.out_edw_end_dttm1 AS edw_end_dttm
  FROM   exp_pass_to_target_insert;
  
  -- Component exp_pass_to_target_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_retired AS
  (
         SELECT upd_ar_invc_retired.ar_invc_id                            AS ar_invc_id,
                upd_ar_invc_retired.lkp_edw_strt_dttm                     AS lkp_edw_strt_dttm,
                dateadd(''second'', - 1, upd_ar_invc_retired.out_edw_strt_dttm) AS out_edw_end_dttm1,
                upd_ar_invc_retired.source_record_id
         FROM   upd_ar_invc_retired );
  -- Component tgt_ar_invc_retired, Type TARGET
  merge
  INTO         db_t_prod_core.ar_invc
  USING        exp_pass_to_target_retired
  ON (
                            ar_invc.ar_invc_id = exp_pass_to_target_retired.ar_invc_id
               AND          ar_invc.edw_strt_dttm = exp_pass_to_target_retired.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    ar_invc_id = exp_pass_to_target_retired.ar_invc_id,
         edw_strt_dttm = exp_pass_to_target_retired.lkp_edw_strt_dttm,
         edw_end_dttm = exp_pass_to_target_retired.out_edw_end_dttm1;
  
  -- Component tgt_ar_invc_retired, Type Post SQL
  UPDATE db_t_prod_core.ar_invc b
    SET    edw_end_dttm=a.lead1
  FROM    
         (
                         SELECT DISTINCT ar_invc_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ar_invc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.ar_invc ) a

  WHERE  b.edw_strt_dttm = a.edw_strt_dttm
  AND    b.ar_invc_id=a.ar_invc_id
  AND    cast(b.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';