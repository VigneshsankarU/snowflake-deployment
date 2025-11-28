-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INVC_STS_INSUPD("WORKLET_NAME" VARCHAR)
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

  -- Component sq_bc_invoice, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                lkp_invc_id,
                lkp_invc_sts_type_cd,
                lkp_invc_sts_dttm,
                lkp_edw_strt_dttm,
                lkp_edw_end_dttm,
                src_invc_id,
                src_invc_sts_type_cd,
                src_invc_sts_dttm,
                trans_strt_dttm,
                retired,
                sourcedata,
                targetdata,
                cdc_flag,
                source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH intrm_invc AS
                                  (
                                         SELECT bc_invoice.invoicenumber_stg    AS invoicenumber,
                                                updatetime_stg                  AS updatetime,
                                                createtime_stg                  AS createtime,
                                                bctl_invoicestatus.typecode_stg AS typecode,
                                                bc_invoice.retired_stg          AS retired
                                         FROM   db_t_prod_stag.bc_invoice
                                         join   db_t_prod_stag.bctl_invoicestatus
                                         ON     bctl_invoicestatus.id_stg=bc_invoice.status_stg
                                         WHERE  bc_invoice.updatetime_stg > (:START_DTTM)
                                         AND    bc_invoice.updatetime_stg <= (:END_DTTM) )
                           /* --------------------------Stage Query ends here---------------------------------------- */
                           SELECT          tgt_lkp_invc_sts.invc_id          AS lkp_invc_id,
                                           tgt_lkp_invc_sts.invc_sts_type_cd AS lkp_invc_sts_type_cd,
                                           tgt_lkp_invc_sts.invc_sts_dttm    AS lkp_invc_sts_dttm,
                                           tgt_lkp_invc_sts.edw_strt_dttm    AS lkp_edw_strt_dttm,
                                           tgt_lkp_invc_sts.edw_end_dttm     AS lkp_edw_end_dttm,
                                           xlat_src.invc_id                  AS src_invc_id,
                                           xlat_src.invc_sts_type_cd         AS src_invc_sts_type_cd,
                                           xlat_src.invc_sts_dttm            AS src_invc_sts_dttm,
                                           xlat_src.trans_strt_dttm          AS trans_strt_dttm,
                                           xlat_src.retired                  AS retired,
                                           /* target data */
                                           cast(trim(cast(coalesce(cast(tgt_lkp_invc_sts.invc_sts_dttm AS timestamp),cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))) AS VARCHAR(60)))
                                                           || trim(tgt_lkp_invc_sts.invc_sts_type_cd) AS VARCHAR(1000)) AS targetdata,
                                           /* source data */
                                           cast(trim(cast(coalesce(cast(xlat_src.invc_sts_dttm AS timestamp),cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))) AS VARCHAR(60)))
                                                           || trim(xlat_src.invc_sts_type_cd) AS VARCHAR(1000)) AS sourcedata,
                                           CASE
                                                           WHEN targetdata IS NULL THEN ''I''
                                                           WHEN targetdata IS NOT NULL
                                                           AND             sourcedata = targetdata THEN ''R''
                                                           WHEN targetdata IS NOT NULL
                                                           AND             sourcedata <> targetdata THEN ''U''
                                           END AS cdc_flag
                           FROM
                                           /* --source query with expression */
                                           (
                                                           SELECT          src.invoicenumber,
                                                                           lkp_xlat_typecode.tgt_idntftn_val                                               AS invc_sts_type_cd,
                                                                           to_char(src.createtime , ''YYYY-MM-DD HH24:MI:SS.FF6'')                              AS invc_sts_dttm,
                                                                           src.updatetime                                                                  AS trans_strt_dttm,
                                                                           lkp_doc.doc_id                                                                  AS invc_id,
                                                                           retired
                                                           FROM            intrm_invc                           AS src
                                                           left outer join db_t_prod_core.teradata_etl_ref_xlat AS lkp_xlat_typecode
                                                           ON              lkp_xlat_typecode.src_idntftn_val=src.typecode
                                                           AND             lkp_xlat_typecode.tgt_idntftn_nm= ''INVC_STS_TYPE''
                                                           AND             lkp_xlat_typecode.src_idntftn_nm= ''bctl_invoicestatus.typecode''
                                                           AND             lkp_xlat_typecode.src_idntftn_sys=''GW''
                                                           AND             lkp_xlat_typecode.expn_dt=''9999-12-31''
                                                           left outer join db_t_prod_core.teradata_etl_ref_xlat AS lkp_xlat_doc_type
                                                           ON              lkp_xlat_doc_type.src_idntftn_val=''DOC_TYPE3''
                                                           AND             lkp_xlat_doc_type.tgt_idntftn_nm= ''DOC_TYPE''
                                                           AND             lkp_xlat_doc_type.expn_dt=''9999-12-31''
                                                           left outer join db_t_prod_core.teradata_etl_ref_xlat AS lkp_xlat_ctgy_type
                                                           ON              lkp_xlat_ctgy_type.src_idntftn_val=''DOC_CTGY_TYPE4''
                                                           AND             lkp_xlat_ctgy_type.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                           AND             lkp_xlat_ctgy_type.expn_dt=''9999-12-31''
                                                           left outer join
                                                                           (
                                                                                    SELECT   doc.doc_id           AS doc_id,
                                                                                             doc.doc_issur_num    AS doc_issur_num,
                                                                                             doc.doc_type_cd      AS doc_type_cd,
                                                                                             doc.doc_ctgy_type_cd AS doc_ctgy_type_cd
                                                                                    FROM     db_t_prod_core.doc 
                                                                                    WHERE    doc.doc_issur_num IN
                                                                                                                   (
                                                                                                                   SELECT DISTINCT invoicenumber
                                                                                                                   FROM            intrm_invc) qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS lkp_doc
                                                           ON              lkp_doc.doc_issur_num=invoicenumber
                                                           AND             lkp_doc.doc_type_cd=lkp_xlat_doc_type.tgt_idntftn_val
                                                           AND             lkp_doc.doc_ctgy_type_cd=lkp_xlat_ctgy_type.tgt_idntftn_val ) AS xlat_src
                           left outer join
                                           (
                                                    SELECT   invc_sts.invc_sts_type_cd AS invc_sts_type_cd,
                                                             invc_sts.invc_sts_dttm    AS invc_sts_dttm,
                                                             invc_sts.edw_strt_dttm    AS edw_strt_dttm,
                                                             invc_sts.edw_end_dttm     AS edw_end_dttm,
                                                             invc_sts.invc_id          AS invc_id
                                                    FROM     db_t_prod_core.invc_sts 
                                                    WHERE    invc_sts.invc_id IN
                                                                                  (
                                                                                  SELECT DISTINCT doc_id
                                                                                  FROM            db_t_prod_core.doc
                                                                                  WHERE           doc_issur_num IN
                                                                                                                    (
                                                                                                                    SELECT DISTINCT invoicenumber
                                                                                                                    FROM            intrm_invc)) qualify row_number () over (PARTITION BY invc_id ORDER BY edw_end_dttm DESC)=1 ) AS tgt_lkp_invc_sts
                           ON              tgt_lkp_invc_sts.invc_id=xlat_src.invc_id ) src ) );
  -- Component exp_CDC_Flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_flag AS
  (
         SELECT sq_bc_invoice.lkp_invc_id                                              AS lkp_invc_id,
                sq_bc_invoice.lkp_edw_strt_dttm                                        AS lkp_edw_strt_dttm,
                sq_bc_invoice.lkp_edw_end_dttm                                         AS lkp_edw_end_dttm,
                sq_bc_invoice.src_invc_id                                              AS in_invc_id,
                sq_bc_invoice.src_invc_sts_type_cd                                     AS in_invc_sts_type_cd,
                sq_bc_invoice.src_invc_sts_dttm                                        AS in_invc_sts_dttm,
                :prcs_id                                                               AS prcs_id,
                current_timestamp                                                      AS startdate,
                to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS enddate,
                sq_bc_invoice.cdc_flag                                                 AS cdc_flag,
                sq_bc_invoice.retired                                                  AS retired,
                sq_bc_invoice.trans_strt_dttm                                          AS trans_strt_dttm,
                sq_bc_invoice.source_record_id
         FROM   sq_bc_invoice );
  -- Component rtr_insert_update_flag_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_insert_update_flag_insert AS
  SELECT exp_cdc_flag.in_invc_id          AS in_invc_id,
         exp_cdc_flag.in_invc_sts_type_cd AS in_invc_sts_type_cd,
         exp_cdc_flag.in_invc_sts_dttm    AS in_invc_sts_dttm,
         exp_cdc_flag.prcs_id             AS in_prcs_id,
         exp_cdc_flag.startdate           AS in_edw_strt_dttm,
         exp_cdc_flag.enddate             AS in_edw_end_dttm,
         exp_cdc_flag.lkp_invc_id         AS lkp_invc_id,
         exp_cdc_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_cdc_flag.cdc_flag            AS cdc_flag,
         exp_cdc_flag.retired             AS retired,
         exp_cdc_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_cdc_flag.trans_strt_dttm     AS trans_strt_dttm,
         exp_cdc_flag.source_record_id
  FROM   exp_cdc_flag
  WHERE  exp_cdc_flag.in_invc_id IS NOT NULL
  AND    (
                exp_cdc_flag.cdc_flag = ''I''
         OR     (
                       exp_cdc_flag.cdc_flag = ''U''
                AND    exp_cdc_flag.retired = 0
                AND    exp_cdc_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_cdc_flag.retired = 0
                AND    exp_cdc_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_insert_update_flag_RETIRE, Type ROUTER Output Group RETIRE
  create or replace TEMPORARY TABLE rtr_insert_update_flag_retire AS
  SELECT exp_cdc_flag.in_invc_id          AS in_invc_id,
         exp_cdc_flag.in_invc_sts_type_cd AS in_invc_sts_type_cd,
         exp_cdc_flag.in_invc_sts_dttm    AS in_invc_sts_dttm,
         exp_cdc_flag.prcs_id             AS in_prcs_id,
         exp_cdc_flag.startdate           AS in_edw_strt_dttm,
         exp_cdc_flag.enddate             AS in_edw_end_dttm,
         exp_cdc_flag.lkp_invc_id         AS lkp_invc_id,
         exp_cdc_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_cdc_flag.cdc_flag            AS cdc_flag,
         exp_cdc_flag.retired             AS retired,
         exp_cdc_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_cdc_flag.trans_strt_dttm     AS trans_strt_dttm,
         exp_cdc_flag.source_record_id
  FROM   exp_cdc_flag
  WHERE  exp_cdc_flag.cdc_flag = ''R''
  AND    exp_cdc_flag.retired != 0
  AND    exp_cdc_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_invc_sts_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_invc_sts_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_insert.in_invc_id          AS in_invc_id1,
                rtr_insert_update_flag_insert.in_invc_sts_type_cd AS in_invc_sts_type_cd1,
                rtr_insert_update_flag_insert.in_invc_sts_dttm    AS in_invc_sts_dttm1,
                rtr_insert_update_flag_insert.in_prcs_id          AS in_prcs_id1,
                rtr_insert_update_flag_insert.in_edw_strt_dttm    AS in_edw_strt_dttm1,
                rtr_insert_update_flag_insert.in_edw_end_dttm     AS in_edw_end_dttm1,
                rtr_insert_update_flag_insert.retired             AS retired1,
                rtr_insert_update_flag_insert.trans_strt_dttm     AS trans_strt_dttm1,
                0                                                 AS update_strategy_action,
                source_record_id
         FROM   rtr_insert_update_flag_insert );
  -- Component upd_invc_sts_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_invc_sts_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_retire.lkp_invc_id       AS lkp_invc_id3,
                rtr_insert_update_flag_retire.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                rtr_insert_update_flag_retire.in_invc_sts_dttm  AS in_edw_strt_dttm3,
                rtr_insert_update_flag_retire.trans_strt_dttm   AS trans_strt_dttm4,
                1                                       AS update_strategy_action,
                rtr_insert_update_flag_retire.source_record_id
         FROM   rtr_insert_update_flag_retire );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_invc_sts_ins.in_invc_id1          AS in_invc_id,
                upd_invc_sts_ins.in_invc_sts_type_cd1 AS in_invc_sts_type_cd,
                upd_invc_sts_ins.in_invc_sts_dttm1    AS in_invc_sts_dttm,
                upd_invc_sts_ins.in_prcs_id1          AS in_prcs_id1,
                upd_invc_sts_ins.in_edw_strt_dttm1    AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_invc_sts_ins.retired1 = 0 THEN upd_invc_sts_ins.in_edw_end_dttm1
                       ELSE current_timestamp
                END                               AS in_edw_end_dttm11,
                upd_invc_sts_ins.trans_strt_dttm1 AS trans_strt_dttm1,
                CASE
                       WHEN upd_invc_sts_ins.retired1 = 0 THEN to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                       ELSE upd_invc_sts_ins.trans_strt_dttm1
                END AS out_trns_end_dttm,
                upd_invc_sts_ins.source_record_id
         FROM   upd_invc_sts_ins );
  -- Component tgt_invc_sts_ins, Type TARGET
  INSERT INTO db_t_prod_core.invc_sts
              (
                          invc_id,
                          invc_sts_type_cd,
                          invc_sts_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.in_invc_id          AS invc_id,
         exp_pass_to_target_ins.in_invc_sts_type_cd AS invc_sts_type_cd,
         exp_pass_to_target_ins.in_invc_sts_dttm    AS invc_sts_dttm,
         exp_pass_to_target_ins.in_prcs_id1         AS prcs_id,
         exp_pass_to_target_ins.in_edw_strt_dttm1   AS edw_strt_dttm,
         exp_pass_to_target_ins.in_edw_end_dttm11   AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm1    AS trans_strt_dttm,
         exp_pass_to_target_ins.out_trns_end_dttm   AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component tgt_invc_sts_ins, Type Post SQL
  UPDATE db_t_prod_core.invc_sts
    SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT invc_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY invc_id ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY invc_id ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.invc_sts ) a

  WHERE  invc_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    invc_sts.invc_id=a.invc_id
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;
  
  -- Component exp_pass_to_target_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd1 AS
  (
         SELECT upd_invc_sts_retire.lkp_invc_id3       AS lkp_invc_id3,
                upd_invc_sts_retire.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                current_timestamp                      AS o_edw_end_dttm,
                upd_invc_sts_retire.trans_strt_dttm4   AS trans_strt_dttm4,
                upd_invc_sts_retire.source_record_id
         FROM   upd_invc_sts_retire );
  -- Component tgt_invc_sts_retire, Type TARGET
  merge
  INTO         db_t_prod_core.invc_sts
  USING        exp_pass_to_target_upd1
  ON (
                            invc_sts.invc_id = exp_pass_to_target_upd1.lkp_invc_id3
               AND          invc_sts.edw_strt_dttm = exp_pass_to_target_upd1.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    invc_id = exp_pass_to_target_upd1.lkp_invc_id3,
         edw_strt_dttm = exp_pass_to_target_upd1.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd1.o_edw_end_dttm,
         trans_end_dttm = exp_pass_to_target_upd1.trans_strt_dttm4;

END;
';