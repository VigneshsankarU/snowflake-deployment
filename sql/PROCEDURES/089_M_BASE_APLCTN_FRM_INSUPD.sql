-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_FRM_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_pc_questionsetlookup, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_questionsetlookup AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_doc_id,
                $2  AS lkp_aplctn_frm_strt_dt,
                $3  AS lkp_aplctn_frm_end_dt,
                $4  AS lkp_prod_id,
                $5  AS lkp_prod_grp_id,
                $6  AS lkp_trans_strt_dttm,
                $7  AS lkp_edw_strt_dttm,
                $8  AS lkp_edw_end_dttm,
                $9  AS in_doc_id,
                $10 AS in_aplctn_frm_strt_dttm,
                $11 AS in_aplction_frm_end_dttm,
                $12 AS in_prod_id,
                $13 AS in_prod_grp_id,
                $14 AS in_trans_strt_dttm,
                $15 AS retried,
                $16 AS insert_flag,
                $17 AS update_flag,
                $18 AS rejectflag,
                $19 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT b.doc_id               AS doc_id_lkp,
                                                b.aplctn_frm_strt_dttm AS aplctn_frm_strt_dttm_lkp ,
                                                b.aplctn_frm_end_dttm  AS aplctn_frm_end_dttm_lkp,
                                                b.prod_id              AS prod_id_lkp,
                                                b.prod_grp_id          AS prod_grp_id_lkp,
                                                b.trans_strt_dttm      AS trans_strt_dttm_lkp,
                                                b.edw_strt_dttm        AS edw_strt_dttm_lkp,
                                                b.edw_end_dttm         AS edw_end_dttm_lkp,
                                                b.doc_id1              AS in_doc_id,
                                                b.starteffectivedate   AS in_starteffectivedate ,
                                                b.endeffectivedate_stg,
                                                b.in_prod_id ,
                                                b.in_prod_gr_id,
                                                b.updatetime,
                                                /*b.ref_key,*/
                                                b.retired1,
                                                /*b.TGT_IDNTFTN_VAL, b.TGT_IDNTFTN_VAL_ctgy, */
                                                cast(trim(cast(aplctn_frm_strt_dttm_lkp AS    VARCHAR(100)))
                                                       ||trim(cast(aplctn_frm_end_dttm_lkp AS VARCHAR(100)))
                                                       ||trim(prod_id_lkp)
                                                       ||trim(prod_grp_id_lkp) AS VARCHAR(1000)) AS source_data,
                                                cast(trim(cast(in_starteffectivedate AS    VARCHAR(100)))
                                                       ||trim(cast(endeffectivedate_stg AS VARCHAR(100)))
                                                       ||trim(in_prod_id)
                                                       ||trim(in_prod_gr_id) AS VARCHAR(1000)) AS target_data,
                                                CASE
                                                       WHEN doc_id_lkp IS NULL THEN 1
                                                       ELSE 0
                                                END AS insertflag,
                                                CASE
                                                       WHEN doc_id_lkp IS NOT NULL
                                                       AND    source_data <> target_data THEN 1
                                                       ELSE 0
                                                END AS updateflag,
                                                CASE
                                                       WHEN doc_id_lkp IS NOT NULL
                                                       AND    source_data = target_data THEN 1
                                                       ELSE 0
                                                END AS rejectflag
                                         FROM   (
                                                          SELECT    *
                                                          FROM      (
                                                                              SELECT    sor.retired AS retired1,
                                                                                        CASE
                                                                                                  WHEN sor.starteffectivedate IS NULL THEN cast(''1900-01-01 00:00:00.000000'' AS timestamp )
                                                                                                  ELSE starteffectivedate
                                                                                        END AS starteffectivedate ,
                                                                                        sor.updatetime,
                                                                                        sor.ref_key,
                                                                                        CASE
                                                                                                  WHEN sor.endeffectivedate_stg IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp )
                                                                                                  ELSE endeffectivedate_stg
                                                                                        END AS endeffectivedate_stg,
                                                                                        un_doclkp.src_idntftn_val ,
                                                                                        un_doc_ctgy_lkp.src_idntftn_val_ctgy,
                                                                                        un_doclkp.tgt_idntftn_val,
                                                                                        un_doc_ctgy_lkp.tgt_idntftn_val_ctgy,
                                                                                        9999 AS in_prod_id,
                                                                                        9999 AS in_prod_gr_id
                                                                              FROM      (
                                                                                        (
                                                                                                        SELECT DISTINCT coalesce(src1.retired_stg,src2.retired_stg) AS retired,
                                                                                                                        src2.starteffectivedate,
                                                                                                                        src2.updatetime                                   AS updatetime,
                                                                                                                        coalesce(src1.sourcefile_stg,src2.sourcefile_stg) AS ref_key,
                                                                                                                        src1.endeffectivedate_stg
                                                                                                        FROM
                                                                                                                        /* DB_T_PROD_STAG.pc_questionsetlookup  */
                                                                                                                        (
                                                                                                                               SELECT pc_questionsetlookup.retired_stg,
                                                                                                                                      pc_questionsetlookup.sourcefile_stg,
                                                                                                                                      pc_questionsetlookup.endeffectivedate_stg
                                                                                                                               FROM   db_t_prod_stag.pc_questionsetlookup
                                                                                                                               WHERE  pc_questionsetlookup.updatetime_stg > ($start_dttm)
                                                                                                                               AND    pc_questionsetlookup.updatetime_stg <= ($end_dttm) ) src1
                                                                                                        full outer join
                                                                                                                        /* DB_T_PROD_STAG.pc_questionlookup  */
                                                                                                                        (
                                                                                                                                        SELECT DISTINCT pc_questionlookup.retired_stg,
                                                                                                                                                        coalesce(pc_questionsetlookup.starteffectivedate_stg,pc_questionlookup.starteffectivedate_stg) AS starteffectivedate,
                                                                                                                                                        coalesce(pc_questionsetlookup.updatetime_stg,pc_questionlookup.updatetime_stg)                 AS updatetime,
                                                                                                                                                        pc_questionsetlookup.sourcefile_stg
                                                                                                                                        FROM            db_t_prod_stag.pc_questionlookup
                                                                                                                                        full outer join db_t_prod_stag.pc_questionsetlookup
                                                                                                                                        ON              pc_questionlookup.sourcefile_stg=pc_questionsetlookup.sourcefile_stg
                                                                                                                                        left join       db_t_prod_stag.pcx_etlquestion_alfa
                                                                                                                                        ON              pc_questionlookup.questioncode_stg=pcx_etlquestion_alfa.questioncode_stg
                                                                                                                                        WHERE           pc_questionlookup.updatetime_stg > ($start_dttm)
                                                                                                                                        AND             pc_questionlookup.updatetime_stg <= ($end_dttm) ) src2
                                                                                                        ON              src2.sourcefile_stg=src1.sourcefile_stg qualify row_number() over(PARTITION BY coalesce(src1.sourcefile_stg,src2.sourcefile_stg) ORDER BY src2.updatetime DESC, src2.starteffectivedate ASC) = 1 )sor
                                                                                        /* ----TERADATA_ETL_REF_XLAT_doc_type--- */
                                                                              left join
                                                                                        (
                                                                                                        SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                        teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                        FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                        WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                                                        AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )un_doclkp
                                                                              ON        un_doclkp.src_idntftn_val=''DOC_TYPE5''
                                                                                        /* -------TERADATA_ETL_REF_XLAT_doc_ctgy_type-- */
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val_ctgy ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val_ctgy
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')un_doc_ctgy_lkp
                                                                              ON        un_doc_ctgy_lkp.src_idntftn_val_ctgy=''DOC_CTGY_TYPE5'' ) )srcqualifier
                                                                    /* ------- */
                                                          left join
                                                                    (
                                                                             SELECT   doc.doc_id            AS doc_id1,
                                                                                      doc.tm_prd_cd         AS tm_prd_cd1,
                                                                                      doc.doc_crtn_dttm     AS doc_crtn_dttm,
                                                                                      doc.doc_recpt_dt      AS doc_recpt_dt1,
                                                                                      doc.doc_prd_strt_dttm AS doc_prd_strt_dttm1,
                                                                                      doc.doc_prd_end_dttm  AS doc_prd_end_dttm1,
                                                                                      doc.edw_strt_dttm     AS edw_strt_dttm1,
                                                                                      doc.data_src_type_cd  AS data_src_type_cd1,
                                                                                      doc.doc_desc_txt      AS doc_desc_txt1,
                                                                                      doc.doc_name          AS doc_name1,
                                                                                      doc.doc_host_num      AS doc_host_num1,
                                                                                      doc.doc_host_vers_num AS doc_host_vers_num1,
                                                                                      doc.doc_cycl_cd       AS doc_cycl_cd1,
                                                                                      doc.mm_objt_id        AS mm_objt_id1,
                                                                                      doc.lang_type_cd      AS lang_type_cd1,
                                                                                      doc.prcs_id           AS prcs_id1,
                                                                                      doc.doc_sts_cd        AS doc_sts_cd1,
                                                                                      doc.doc_issur_num     AS doc_issur_num1,
                                                                                      doc.doc_type_cd       AS doc_type_cd1,
                                                                                      doc.doc_ctgy_type_cd  AS doc_ctgy_type_cd1
                                                                             FROM     db_t_prod_core.doc qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1)doclkp
                                                          ON        srcqualifier.ref_key=doclkp.doc_issur_num1
                                                          AND       srcqualifier.tgt_idntftn_val =doclkp.doc_type_cd1
                                                          AND       srcqualifier.tgt_idntftn_val_ctgy =doclkp.doc_ctgy_type_cd1
                                                                    /* -------- */
                                                          left join
                                                                    (
                                                                             SELECT   aplctn_frm.aplctn_frm_strt_dttm AS aplctn_frm_strt_dttm,
                                                                                      aplctn_frm.trans_strt_dttm      AS trans_strt_dttm,
                                                                                      aplctn_frm.aplctn_frm_end_dttm  AS aplctn_frm_end_dttm,
                                                                                      aplctn_frm.prod_id              AS prod_id,
                                                                                      aplctn_frm.prod_grp_id          AS prod_grp_id,
                                                                                      aplctn_frm.prcs_id              AS prcs_id,
                                                                                      aplctn_frm.edw_strt_dttm        AS edw_strt_dttm,
                                                                                      aplctn_frm.edw_end_dttm         AS edw_end_dttm,
                                                                                      aplctn_frm.doc_id               AS doc_id
                                                                             FROM     db_t_prod_core.aplctn_frm qualify row_number () over ( PARTITION BY doc_id ORDER BY edw_end_dttm DESC)=1)tgt_lkp
                                                          ON        doclkp.doc_id1=tgt_lkp.doc_id ) b ) src ) );
  -- Component exp_flag_ins_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_flag_ins_upd1 AS
  (
         SELECT sq_pc_questionsetlookup.lkp_doc_id                                    AS lkp_doc_id,
                sq_pc_questionsetlookup.lkp_edw_strt_dttm                             AS lkp_edw_strt_dttm,
                sq_pc_questionsetlookup.lkp_edw_end_dttm                              AS lkp_edw_end_dttm,
                sq_pc_questionsetlookup.in_doc_id                                     AS in_doc_id,
                sq_pc_questionsetlookup.in_aplctn_frm_strt_dttm                       AS in_aplctn_frm_strt_dt,
                sq_pc_questionsetlookup.in_aplction_frm_end_dttm                      AS in_aplctn_frm_end_dt,
                sq_pc_questionsetlookup.in_prod_id                                    AS in_prod_id,
                sq_pc_questionsetlookup.in_prod_grp_id                                AS in_prod_grp_id,
                sq_pc_questionsetlookup.in_trans_strt_dttm                            AS in_trans_strt_dttm,
                current_timestamp                                                     AS edw_strt_dttm,
                to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                sq_pc_questionsetlookup.retried                                       AS retired,
                dateadd(''second'', - 1, current_timestamp)                                 AS out_trans_end_dttm,
                $prcs_id                                                              AS out_prcs_id,
                out_prcs_id                                                           AS out_prcs_id1,
                sq_pc_questionsetlookup.insert_flag                                   AS insert_flag,
                sq_pc_questionsetlookup.update_flag                                   AS update_flag,
                sq_pc_questionsetlookup.rejectflag                                    AS rejectflag,
                sq_pc_questionsetlookup.source_record_id
         FROM   sq_pc_questionsetlookup );
  -- Component rtr_aplctn_frm_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_aplctn_frm_insert as
  SELECT exp_flag_ins_upd1.lkp_doc_id            AS lkp_doc_id,
         exp_flag_ins_upd1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_flag_ins_upd1.in_doc_id             AS in_doc_id,
         exp_flag_ins_upd1.in_aplctn_frm_strt_dt AS in_aplctn_frm_strt_dt,
         exp_flag_ins_upd1.in_aplctn_frm_end_dt  AS in_aplctn_frm_end_dt,
         exp_flag_ins_upd1.in_prod_id            AS in_prod_id,
         exp_flag_ins_upd1.in_prod_grp_id        AS in_prod_grp_id,
         exp_flag_ins_upd1.in_trans_strt_dttm    AS in_trans_strt_dttm,
         exp_flag_ins_upd1.out_prcs_id1          AS prcs_id,
         exp_flag_ins_upd1.edw_strt_dttm         AS edw_strt_dttm,
         exp_flag_ins_upd1.edw_end_dttm          AS edw_end_dttm,
         exp_flag_ins_upd1.update_flag           AS updateflag,
         exp_flag_ins_upd1.insert_flag           AS insertflag,
         exp_flag_ins_upd1.retired               AS retired,
         exp_flag_ins_upd1.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_flag_ins_upd1.rejectflag            AS rejectflag,
         exp_flag_ins_upd1.out_trans_end_dttm    AS out_trans_end_dttm,
         exp_flag_ins_upd1.source_record_id
  FROM   exp_flag_ins_upd1
  WHERE  exp_flag_ins_upd1.in_doc_id IS NOT NULL
  AND    exp_flag_ins_upd1.insert_flag = 1
  OR     (
                exp_flag_ins_upd1.retired = 0
         AND    exp_flag_ins_upd1.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_aplctn_frm_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_aplctn_frm_retire as
  SELECT exp_flag_ins_upd1.lkp_doc_id            AS lkp_doc_id,
         exp_flag_ins_upd1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_flag_ins_upd1.in_doc_id             AS in_doc_id,
         exp_flag_ins_upd1.in_aplctn_frm_strt_dt AS in_aplctn_frm_strt_dt,
         exp_flag_ins_upd1.in_aplctn_frm_end_dt  AS in_aplctn_frm_end_dt,
         exp_flag_ins_upd1.in_prod_id            AS in_prod_id,
         exp_flag_ins_upd1.in_prod_grp_id        AS in_prod_grp_id,
         exp_flag_ins_upd1.in_trans_strt_dttm    AS in_trans_strt_dttm,
         exp_flag_ins_upd1.out_prcs_id1          AS prcs_id,
         exp_flag_ins_upd1.edw_strt_dttm         AS edw_strt_dttm,
         exp_flag_ins_upd1.edw_end_dttm          AS edw_end_dttm,
         exp_flag_ins_upd1.update_flag           AS updateflag,
         exp_flag_ins_upd1.insert_flag           AS insertflag,
         exp_flag_ins_upd1.retired               AS retired,
         exp_flag_ins_upd1.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_flag_ins_upd1.rejectflag            AS rejectflag,
         exp_flag_ins_upd1.out_trans_end_dttm    AS out_trans_end_dttm,
         exp_flag_ins_upd1.source_record_id
  FROM   exp_flag_ins_upd1
  WHERE  exp_flag_ins_upd1.rejectflag = 1
  AND    exp_flag_ins_upd1.retired != 0
  AND    exp_flag_ins_upd1.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_aplctn_frm_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_aplctn_frm_update as
  SELECT exp_flag_ins_upd1.lkp_doc_id            AS lkp_doc_id,
         exp_flag_ins_upd1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_flag_ins_upd1.in_doc_id             AS in_doc_id,
         exp_flag_ins_upd1.in_aplctn_frm_strt_dt AS in_aplctn_frm_strt_dt,
         exp_flag_ins_upd1.in_aplctn_frm_end_dt  AS in_aplctn_frm_end_dt,
         exp_flag_ins_upd1.in_prod_id            AS in_prod_id,
         exp_flag_ins_upd1.in_prod_grp_id        AS in_prod_grp_id,
         exp_flag_ins_upd1.in_trans_strt_dttm    AS in_trans_strt_dttm,
         exp_flag_ins_upd1.out_prcs_id1          AS prcs_id,
         exp_flag_ins_upd1.edw_strt_dttm         AS edw_strt_dttm,
         exp_flag_ins_upd1.edw_end_dttm          AS edw_end_dttm,
         exp_flag_ins_upd1.update_flag           AS updateflag,
         exp_flag_ins_upd1.insert_flag           AS insertflag,
         exp_flag_ins_upd1.retired               AS retired,
         exp_flag_ins_upd1.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_flag_ins_upd1.rejectflag            AS rejectflag,
         exp_flag_ins_upd1.out_trans_end_dttm    AS out_trans_end_dttm,
         exp_flag_ins_upd1.source_record_id
  FROM   exp_flag_ins_upd1
  WHERE  exp_flag_ins_upd1.update_flag = 1
  AND    exp_flag_ins_upd1.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_insert.in_doc_id             AS doc_id,
                rtr_aplctn_frm_insert.in_aplctn_frm_strt_dt AS aplctn_frm_strt_dt,
                rtr_aplctn_frm_insert.in_aplctn_frm_end_dt  AS aplctn_frm_end_dt,
                rtr_aplctn_frm_insert.in_prod_id            AS prod_id,
                rtr_aplctn_frm_insert.in_prod_grp_id        AS prod_grp_id,
                rtr_aplctn_frm_insert.prcs_id               AS prcs_id,
                rtr_aplctn_frm_insert.edw_strt_dttm         AS edw_strt_dttm,
                rtr_aplctn_frm_insert.edw_end_dttm          AS edw_end_dttm,
                rtr_aplctn_frm_insert.in_trans_strt_dttm    AS trans_strt_dttm,
                rtr_aplctn_frm_insert.retired               AS retired1,
                0                                           AS update_strategy_action,
                source_record_id
         FROM   rtr_aplctn_frm_insert );
  -- Component upd_aplctn_frm_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_retire.lkp_doc_id         AS doc_id,
                rtr_aplctn_frm_retire.lkp_edw_strt_dttm  AS edw_strt_dttm,
                rtr_aplctn_frm_retire.edw_strt_dttm      AS edw_end_dttm,
                rtr_aplctn_frm_retire.prcs_id            AS prcs_id3,
                rtr_aplctn_frm_retire.out_trans_end_dttm AS out_trans_end_dttm4,
                rtr_aplctn_frm_retire.in_trans_strt_dttm AS in_trans_strt_dttm4,
                1                                        AS update_strategy_action,
                rtr_aplctn_frm_retire.source_record_id
         FROM   rtr_aplctn_frm_retire );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_ins.doc_id             AS doc_id,
                upd_ins.aplctn_frm_strt_dt AS aplctn_frm_strt_dt,
                upd_ins.aplctn_frm_end_dt  AS aplctn_frm_end_dt,
                upd_ins.prod_id            AS prod_id,
                upd_ins.prod_grp_id        AS prod_grp_id,
                upd_ins.prcs_id            AS prcs_id,
                upd_ins.edw_strt_dttm      AS edw_strt_dttm,
                upd_ins.trans_strt_dttm    AS trans_strt_dttm,
                CASE
                       WHEN upd_ins.retired1 = 0 THEN upd_ins.edw_end_dttm
                       ELSE current_timestamp
                END AS out_edw_end_ddtm,
                upd_ins.source_record_id
         FROM   upd_ins );
  -- Component upd_insupd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insupd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_update.in_doc_id             AS doc_id,
                rtr_aplctn_frm_update.in_aplctn_frm_strt_dt AS aplctn_frm_strt_dt,
                rtr_aplctn_frm_update.in_aplctn_frm_end_dt  AS aplctn_frm_end_dt,
                rtr_aplctn_frm_update.in_prod_id            AS prod_id,
                rtr_aplctn_frm_update.in_prod_grp_id        AS prod_grp_id,
                rtr_aplctn_frm_update.prcs_id               AS prcs_id,
                rtr_aplctn_frm_update.edw_strt_dttm         AS edw_strt_dttm,
                rtr_aplctn_frm_update.edw_end_dttm          AS edw_end_dttm,
                rtr_aplctn_frm_update.in_trans_strt_dttm    AS trans_strt_dttm,
                rtr_aplctn_frm_update.retired               AS retired3,
                rtr_aplctn_frm_update.lkp_edw_end_dttm      AS lkp_edw_end_dttm3,
                0                                           AS update_strategy_action,
                rtr_aplctn_frm_update.source_record_id
         FROM   rtr_aplctn_frm_update );
  -- Component upd_aplctn_frm_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_update.lkp_doc_id         AS doc_id,
                rtr_aplctn_frm_update.lkp_edw_strt_dttm  AS edw_strt_dttm,
                rtr_aplctn_frm_update.edw_strt_dttm      AS edw_end_dttm,
                rtr_aplctn_frm_update.prcs_id            AS prcs_id3,
                rtr_aplctn_frm_update.lkp_edw_end_dttm   AS lkp_edw_end_dttm3,
                rtr_aplctn_frm_update.out_trans_end_dttm AS out_trans_end_dttm3,
                rtr_aplctn_frm_update.in_trans_strt_dttm AS in_trans_strt_dttm3,
                1                                        AS update_strategy_action,
                rtr_aplctn_frm_update.source_record_id
         FROM   rtr_aplctn_frm_update );
  -- Component tgt_APLCTN_FRM_ins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_frm
              (
                          doc_id,
                          aplctn_frm_strt_dttm,
                          aplctn_frm_end_dttm,
                          prod_id,
                          prod_grp_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins.doc_id             AS doc_id,
         exp_pass_to_tgt_ins.aplctn_frm_strt_dt AS aplctn_frm_strt_dttm,
         exp_pass_to_tgt_ins.aplctn_frm_end_dt  AS aplctn_frm_end_dttm,
         exp_pass_to_tgt_ins.prod_id            AS prod_id,
         exp_pass_to_tgt_ins.prod_grp_id        AS prod_grp_id,
         exp_pass_to_tgt_ins.prcs_id            AS prcs_id,
         exp_pass_to_tgt_ins.edw_strt_dttm      AS edw_strt_dttm,
         exp_pass_to_tgt_ins.out_edw_end_ddtm   AS edw_end_dttm,
         exp_pass_to_tgt_ins.trans_strt_dttm    AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component exp_pass_to_tgt_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
  (
         SELECT upd_aplctn_frm_retire.doc_id                                  AS doc_id,
                upd_aplctn_frm_retire.edw_strt_dttm                           AS edw_strt_dttm,
                current_timestamp                                             AS out_edw_end_dttm,
                dateadd(''second'', - 1, upd_aplctn_frm_retire.in_trans_strt_dttm4) AS out_trans_end_dttm4,
                upd_aplctn_frm_retire.source_record_id
         FROM   upd_aplctn_frm_retire );
  -- Component fil_active_recs, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_active_recs AS
  (
         SELECT upd_insupd.doc_id             AS doc_id,
                upd_insupd.aplctn_frm_strt_dt AS aplctn_frm_strt_dt,
                upd_insupd.aplctn_frm_end_dt  AS aplctn_frm_end_dt,
                upd_insupd.prod_id            AS prod_id,
                upd_insupd.prod_grp_id        AS prod_grp_id,
                upd_insupd.prcs_id            AS prcs_id,
                upd_insupd.edw_strt_dttm      AS edw_strt_dttm,
                upd_insupd.edw_end_dttm       AS edw_end_dttm,
                upd_insupd.trans_strt_dttm    AS trans_strt_dttm,
                upd_insupd.retired3           AS retired3,
                upd_insupd.lkp_edw_end_dttm3  AS lkp_edw_end_dttm3,
                upd_insupd.source_record_id
         FROM   upd_insupd
         WHERE  upd_insupd.retired3 = 0 );
  -- Component fil_active_recs_upd, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_active_recs_upd AS
  (
         SELECT upd_aplctn_frm_upd.doc_id              AS doc_id,
                upd_aplctn_frm_upd.edw_strt_dttm       AS edw_strt_dttm,
                upd_aplctn_frm_upd.edw_end_dttm        AS edw_end_dttm,
                upd_aplctn_frm_upd.prcs_id3            AS prcs_id3,
                upd_aplctn_frm_upd.lkp_edw_end_dttm3   AS lkp_edw_end_dttm3,
                upd_aplctn_frm_upd.out_trans_end_dttm3 AS out_trans_end_dttm3,
                upd_aplctn_frm_upd.in_trans_strt_dttm3 AS in_trans_strt_dttm3,
                upd_aplctn_frm_upd.source_record_id
         FROM   upd_aplctn_frm_upd
         WHERE  upd_aplctn_frm_upd.lkp_edw_end_dttm3 = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  -- Component tgt_APLCTN_FRM_retire, Type TARGET
  merge
  INTO         db_t_prod_core.aplctn_frm
  USING        exp_pass_to_tgt_upd1
  ON (
                            aplctn_frm.doc_id = exp_pass_to_tgt_upd1.doc_id
               AND          aplctn_frm.edw_strt_dttm = exp_pass_to_tgt_upd1.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    doc_id = exp_pass_to_tgt_upd1.doc_id,
         edw_strt_dttm = exp_pass_to_tgt_upd1.edw_strt_dttm,
         edw_end_dttm = exp_pass_to_tgt_upd1.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_upd1.out_trans_end_dttm4;
  
  -- Component exp_pass_to_tgt_ins1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins1 AS
  (
         SELECT fil_active_recs.doc_id             AS doc_id,
                fil_active_recs.aplctn_frm_strt_dt AS aplctn_frm_strt_dt,
                fil_active_recs.aplctn_frm_end_dt  AS aplctn_frm_end_dt,
                fil_active_recs.prod_id            AS prod_id,
                fil_active_recs.prod_grp_id        AS prod_grp_id,
                fil_active_recs.prcs_id            AS prcs_id,
                fil_active_recs.edw_strt_dttm      AS edw_strt_dttm,
                fil_active_recs.edw_end_dttm       AS edw_end_dttm,
                fil_active_recs.trans_strt_dttm    AS trans_strt_dttm,
                fil_active_recs.source_record_id
         FROM   fil_active_recs );
  -- Component tgt_APLCTN_FRM_insupd, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_frm
              (
                          doc_id,
                          aplctn_frm_strt_dttm,
                          aplctn_frm_end_dttm,
                          prod_id,
                          prod_grp_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins1.doc_id             AS doc_id,
         exp_pass_to_tgt_ins1.aplctn_frm_strt_dt AS aplctn_frm_strt_dttm,
         exp_pass_to_tgt_ins1.aplctn_frm_end_dt  AS aplctn_frm_end_dttm,
         exp_pass_to_tgt_ins1.prod_id            AS prod_id,
         exp_pass_to_tgt_ins1.prod_grp_id        AS prod_grp_id,
         exp_pass_to_tgt_ins1.prcs_id            AS prcs_id,
         exp_pass_to_tgt_ins1.edw_strt_dttm      AS edw_strt_dttm,
         exp_pass_to_tgt_ins1.edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_tgt_ins1.trans_strt_dttm    AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins1;
  
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT fil_active_recs_upd.doc_id                                  AS doc_id,
                fil_active_recs_upd.edw_strt_dttm                           AS edw_strt_dttm,
                dateadd(''second'', - 1, fil_active_recs_upd.edw_end_dttm)        AS out_edw_end_dttm,
                dateadd(''second'', - 1, fil_active_recs_upd.in_trans_strt_dttm3) AS out_trans_end_dttm3,
                fil_active_recs_upd.source_record_id
         FROM   fil_active_recs_upd );
  -- Component tgt_APLCTN_FRM_upd_expire, Type TARGET
  merge
  INTO         db_t_prod_core.aplctn_frm
  USING        exp_pass_to_tgt_upd
  ON (
                            aplctn_frm.doc_id = exp_pass_to_tgt_upd.doc_id
               AND          aplctn_frm.edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    doc_id = exp_pass_to_tgt_upd.doc_id,
         edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm,
         edw_end_dttm = exp_pass_to_tgt_upd.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_upd.out_trans_end_dttm3;

END;
';