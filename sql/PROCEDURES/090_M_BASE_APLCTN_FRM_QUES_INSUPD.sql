-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_FRM_QUES_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_pc_questionlookup, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_questionlookup AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS src_questioncode,
                $2  AS src_updatetime,
                $3  AS src_starteffectivedate,
                $4  AS src_endeffectivedate,
                $5  AS src_retired,
                $6  AS src_questionlabel,
                $7  AS src_doc_id,
                $8  AS tgt_doc_id,
                $9  AS tgt_aplctn_frm_ques_strt_dttm,
                $10 AS tgt_aplctn_frm_ques_end_dttm,
                $11 AS tgt_aplctn_frm_ques_num,
                $12 AS tgt_edw_strt_dttm,
                $13 AS tgt_edw_end_dttm,
                $14 AS sourcedata,
                $15 AS targetdata,
                $16 AS ins_upd_flag,
                $17 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          src.questioncode,
                                                                  src.updatetime,
                                                                  src.starteffectivedate,
                                                                  src.endeffectivedate,
                                                                  src.retired,
                                                                  src.questionlabel,
                                                                  src.src_doc_id,
                                                                  tgt_lkup.doc_id,
                                                                  tgt_lkup.aplctn_frm_ques_strt_dttm ,
                                                                  tgt_lkup.aplctn_frm_ques_end_dttm ,
                                                                  tgt_lkup.aplctn_frm_ques_num ,
                                                                  tgt_lkup.edw_strt_dttm ,
                                                                  tgt_lkup.edw_end_dttm,
                                                                  /*Sourcedata*/
                                                                  CAST(
																	TRIM(to_varchar(src.starteffectivedate, ''YYYY-MM-DD HH24:MI:SS'')) ||
																	TRIM(to_varchar(src.endeffectivedate, ''YYYY-MM-DD HH24:MI:SS''))
																	AS VARCHAR(1100)
																	) AS sourcedata,		
                                                                  /*Targetdata*/
																  CAST(
																	TRIM(TO_VARCHAR(tgt_lkup.aplctn_frm_ques_strt_dttm, ''YYYY-MM-DD HH24:MI:SS'')) ||
																	TRIM(TO_VARCHAR(tgt_lkup.aplctn_frm_ques_end_dttm, ''YYYY-MM-DD HH24:MI:SS''))
																	AS VARCHAR(1100)
																	) AS targetdata,


                                                                  CASE
                                                                                  WHEN tgt_lkup.aplctn_frm_ques_num IS NULL THEN ''I''
                                                                                  WHEN tgt_lkup.aplctn_frm_ques_num IS NOT NULL
                                                                                  AND             sourcedata <> targetdata THEN ''U''
                                                                                  WHEN tgt_lkup.aplctn_frm_ques_num IS NOT NULL
                                                                                  AND             sourcedata = targetdata THEN ''R''
                                                                  END AS ins_upd_flag
                                                  FROM            (
                                                                                  SELECT DISTINCT stag.questioncode,
                                                                                                  stag. sourcefile,
                                                                                                  stag.updatetime,
                                                                                                  CASE
                                                                                                                  WHEN starteffectivedate IS NULL THEN to_date(''01/01/1900 00:00:00.000000'',''MM/DD/YYYY HH24:MI:SS.FF6'')
                                                                                                                  ELSE starteffectivedate
                                                                                                  END AS starteffectivedate,
                                                                                                  CASE
                                                                                                                  WHEN endeffectivedate IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))
                                                                                                                  ELSE endeffectivedate
                                                                                                  END AS endeffectivedate,
                                                                                                  stag.retired,
                                                                                                  stag.questionlabel ,
                                                                                                  lkup_doc.doc_id AS src_doc_id
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT pc_questionlookup.questioncode_stg                                                             AS questioncode ,
                                                                                                                                  pc_questionsetlookup.sourcefile_stg                                                            AS sourcefile ,
                                                                                                                                  coalesce(pc_questionsetlookup.updatetime_stg,pc_questionlookup.updatetime_stg)                 AS updatetime ,
                                                                                                                                  coalesce(pc_questionsetlookup.starteffectivedate_stg,pc_questionlookup.starteffectivedate_stg) AS starteffectivedate ,
                                                                                                                                  pc_questionsetlookup.endeffectivedate_stg                                                      AS endeffectivedate ,
                                                                                                                                  pc_questionlookup.retired_stg                                                                  AS retired ,
                                                                                                                                  pcx_etlquestion_alfa.questionlabel_stg                                                         AS questionlabel
                                                                                                                  FROM            db_t_prod_stag.pc_questionlookup
                                                                                                                  full outer join db_t_prod_stag.pc_questionsetlookup
                                                                                                                  ON              pc_questionlookup.sourcefile_stg=pc_questionsetlookup.sourcefile_stg
                                                                                                                  left join       db_t_prod_stag.pcx_etlquestion_alfa
                                                                                                                  ON              pc_questionlookup.questioncode_stg=pcx_etlquestion_alfa.questioncode_stg
                                                                                                                  WHERE           pc_questionlookup.updatetime_stg > ($start_dttm)
                                                                                                                  AND             pc_questionlookup.updatetime_stg <= ($end_dttm)
                                                                                                                  AND             pc_questionsetlookup.sourcefile_stg IS NOT NULL
                                                                                                                  AND             pc_questionlookup.questioncode_stg IS NOT NULL
                                                                                                                                  /*  Added this filter to handle null records in target  */
                                                                                                  )stag
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) xlat_doc_type
                                                                                  ON              xlat_doc_type.src_idntftn_val=''DOC_TYPE5''
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_doc_ctgy_type
                                                                                  ON              xlat_doc_ctgy_type.src_idntftn_val=''DOC_CTGY_TYPE5''
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   doc.doc_id            AS doc_id,
                                                                                                                    doc.tm_prd_cd         AS tm_prd_cd,
                                                                                                                    doc.doc_crtn_dttm     AS doc_crtn_dttm,
                                                                                                                    doc.doc_recpt_dt      AS doc_recpt_dt,
                                                                                                                    doc.doc_prd_strt_dttm AS doc_prd_strt_dttm,
                                                                                                                    doc.doc_prd_end_dttm  AS doc_prd_end_dttm,
                                                                                                                    doc.edw_strt_dttm     AS edw_strt_dttm,
                                                                                                                    doc.data_src_type_cd  AS data_src_type_cd,
                                                                                                                    doc.doc_desc_txt      AS doc_desc_txt,
                                                                                                                    doc.doc_name          AS doc_name,
                                                                                                                    doc.doc_host_num      AS doc_host_num,
                                                                                                                    doc.doc_host_vers_num AS doc_host_vers_num,
                                                                                                                    doc.doc_cycl_cd       AS doc_cycl_cd,
                                                                                                                    doc.mm_objt_id        AS mm_objt_id,
                                                                                                                    doc.lang_type_cd      AS lang_type_cd,
                                                                                                                    doc.prcs_id           AS prcs_id,
                                                                                                                    doc.doc_sts_cd        AS doc_sts_cd,
                                                                                                                    doc.doc_issur_num     AS doc_issur_num,
                                                                                                                    doc.doc_type_cd       AS doc_type_cd,
                                                                                                                    doc.doc_ctgy_type_cd  AS doc_ctgy_type_cd
                                                                                                           FROM     db_t_prod_core.doc  qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 )lkup_doc
                                                                                  ON              lkup_doc.doc_issur_num =stag.sourcefile
                                                                                  AND             lkup_doc.doc_ctgy_type_cd=xlat_doc_ctgy_type.tgt_idntftn_val
                                                                                  AND             lkup_doc.doc_type_cd=xlat_doc_type. tgt_idntftn_val )src
                                                  left outer join
                                                                  (
                                                                           SELECT   aplctn_frm_ques.edw_strt_dttm             AS edw_strt_dttm,
                                                                                    aplctn_frm_ques.aplctn_frm_ques_strt_dttm AS aplctn_frm_ques_strt_dttm,
                                                                                    aplctn_frm_ques.aplctn_frm_ques_end_dttm  AS aplctn_frm_ques_end_dttm,
                                                                                    aplctn_frm_ques.edw_end_dttm              AS edw_end_dttm,
                                                                                    aplctn_frm_ques.aplctn_frm_ques_num       AS aplctn_frm_ques_num,
                                                                                    aplctn_frm_ques.doc_id                    AS doc_id
                                                                           FROM     db_t_prod_core.aplctn_frm_ques            AS aplctn_frm_ques qualify row_number () over ( PARTITION BY aplctn_frm_ques_num,doc_id ORDER BY edw_end_dttm DESC)=1 )tgt_lkup
                                                  ON              tgt_lkup.aplctn_frm_ques_num=src.questioncode
                                                  AND             tgt_lkup.doc_id=src.src_doc_id ) src ) );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
         SELECT sq_pc_questionlookup.src_questioncode                                 AS src_questioncode,
                sq_pc_questionlookup.src_updatetime                                   AS src_updatetime,
                sq_pc_questionlookup.src_starteffectivedate                           AS src_starteffectivedate,
                sq_pc_questionlookup.src_endeffectivedate                             AS src_endeffectivedate,
                sq_pc_questionlookup.src_retired                                      AS src_retired,
                sq_pc_questionlookup.src_questionlabel                                AS src_questionlabel,
                sq_pc_questionlookup.src_doc_id                                       AS src_doc_id,
                sq_pc_questionlookup.tgt_aplctn_frm_ques_num                          AS tgt_aplctn_frm_ques_num,
                sq_pc_questionlookup.tgt_edw_strt_dttm                                AS tgt_edw_strt_dttm,
                sq_pc_questionlookup.tgt_edw_end_dttm                                 AS tgt_edw_end_dttm,
                sq_pc_questionlookup.ins_upd_flag                                     AS ins_upd_flag,
                $prcs_id                                                              AS out_prcs_id,
                current_timestamp                                                     AS edw_strt_dttm,
                to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                sq_pc_questionlookup.source_record_id
         FROM   sq_pc_questionlookup );
  -- Component rtr_aplctn_frm_ques_insupd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_aplctn_frm_ques_insupd_insert as
  SELECT exptrans.tgt_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
         exptrans.tgt_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exptrans.src_doc_id              AS doc_id,
         exptrans.out_prcs_id             AS out_prcs_id,
         exptrans.src_questioncode        AS questioncode,
         exptrans.edw_strt_dttm           AS edw_strt_dttm,
         exptrans.edw_end_dttm            AS edw_end_dttm,
         exptrans.ins_upd_flag            AS ins_upd_flag,
         exptrans.src_starteffectivedate  AS in_aplctn_frm_ques_strt_dt,
         exptrans.src_endeffectivedate    AS in_aplctn_frm_ques_end_dt,
         exptrans.src_updatetime          AS in_trans_strt_dttm,
         exptrans.tgt_edw_end_dttm        AS lkp_edw_end_dttm,
         exptrans.src_retired             AS retired,
         exptrans.src_questionlabel       AS questionlabel,
         exptrans.source_record_id
  FROM   exptrans
  WHERE  exptrans.ins_upd_flag = ''I''
  OR     (
                exptrans.src_retired = 0
         AND    exptrans.tgt_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_aplctn_frm_ques_insupd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_aplctn_frm_ques_insupd_retire as
  SELECT exptrans.tgt_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
         exptrans.tgt_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exptrans.src_doc_id              AS doc_id,
         exptrans.out_prcs_id             AS out_prcs_id,
         exptrans.src_questioncode        AS questioncode,
         exptrans.edw_strt_dttm           AS edw_strt_dttm,
         exptrans.edw_end_dttm            AS edw_end_dttm,
         exptrans.ins_upd_flag            AS ins_upd_flag,
         exptrans.src_starteffectivedate  AS in_aplctn_frm_ques_strt_dt,
         exptrans.src_endeffectivedate    AS in_aplctn_frm_ques_end_dt,
         exptrans.src_updatetime          AS in_trans_strt_dttm,
         exptrans.tgt_edw_end_dttm        AS lkp_edw_end_dttm,
         exptrans.src_retired             AS retired,
         exptrans.src_questionlabel       AS questionlabel,
         exptrans.source_record_id
  FROM   exptrans
  WHERE  exptrans.ins_upd_flag = ''R''
  AND    exptrans.src_retired != 0
  AND    exptrans.tgt_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_aplctn_frm_ques_insupd_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_aplctn_frm_ques_insupd_update as
  SELECT exptrans.tgt_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
         exptrans.tgt_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exptrans.src_doc_id              AS doc_id,
         exptrans.out_prcs_id             AS out_prcs_id,
         exptrans.src_questioncode        AS questioncode,
         exptrans.edw_strt_dttm           AS edw_strt_dttm,
         exptrans.edw_end_dttm            AS edw_end_dttm,
         exptrans.ins_upd_flag            AS ins_upd_flag,
         exptrans.src_starteffectivedate  AS in_aplctn_frm_ques_strt_dt,
         exptrans.src_endeffectivedate    AS in_aplctn_frm_ques_end_dt,
         exptrans.src_updatetime          AS in_trans_strt_dttm,
         exptrans.tgt_edw_end_dttm        AS lkp_edw_end_dttm,
         exptrans.src_retired             AS retired,
         exptrans.src_questionlabel       AS questionlabel,
         exptrans.source_record_id
  FROM   exptrans
  WHERE  exptrans.ins_upd_flag = ''U''
  AND    exptrans.tgt_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_aplctn_frm_ques_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_ques_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_ques_insupd_retire.doc_id                  AS doc_id3,
                rtr_aplctn_frm_ques_insupd_retire.lkp_aplctn_frm_ques_num AS questioncode3,
                rtr_aplctn_frm_ques_insupd_retire.lkp_edw_strt_dttm       AS edw_strt_dttm3,
                1                                                         AS update_strategy_action,
				rtr_aplctn_frm_ques_insupd_retire.source_record_id
         FROM   rtr_aplctn_frm_ques_insupd_retire );
  -- Component upd_aplctn_frm_ques_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_ques_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_ques_insupd_update.doc_id                  AS doc_id3,
                rtr_aplctn_frm_ques_insupd_update.lkp_aplctn_frm_ques_num AS questioncode3,
                rtr_aplctn_frm_ques_insupd_update.lkp_edw_strt_dttm       AS edw_strt_dttm3,
                rtr_aplctn_frm_ques_insupd_update.lkp_edw_end_dttm        AS lkp_edw_end_dttm3,
                rtr_aplctn_frm_ques_insupd_update.in_trans_strt_dttm      AS in_trans_strt_dttm3,
                rtr_aplctn_frm_ques_insupd_update.questionlabel           AS questionlabel3,
                1                                                         AS update_strategy_action,
				rtr_aplctn_frm_ques_insupd_update.source_record_id
         FROM   rtr_aplctn_frm_ques_insupd_update );
  -- Component upd_aplctn_frm_ques_insupd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_ques_insupd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_ques_insupd_update.doc_id                     AS doc_id,
                rtr_aplctn_frm_ques_insupd_update.questioncode               AS questioncode1,
                rtr_aplctn_frm_ques_insupd_update.out_prcs_id                AS out_prcs_id1,
                rtr_aplctn_frm_ques_insupd_update.edw_strt_dttm              AS edw_strt_dttm1,
                rtr_aplctn_frm_ques_insupd_update.edw_end_dttm               AS edw_end_dttm1,
                rtr_aplctn_frm_ques_insupd_update.in_aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dt,
                rtr_aplctn_frm_ques_insupd_update.in_aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dt,
                rtr_aplctn_frm_ques_insupd_update.in_trans_strt_dttm         AS trans_strt_dttm,
                rtr_aplctn_frm_ques_insupd_update.retired                    AS retired3,
                rtr_aplctn_frm_ques_insupd_update.questionlabel              AS questionlabel3,
                0                                                            AS update_strategy_action,
				rtr_aplctn_frm_ques_insupd_update.source_record_id
         FROM   rtr_aplctn_frm_ques_insupd_update );
  -- Component fil_valid_recs, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_valid_recs AS
  (
         SELECT upd_aplctn_frm_ques_upd.doc_id3             AS doc_id3,
                upd_aplctn_frm_ques_upd.questioncode3       AS questioncode3,
                upd_aplctn_frm_ques_upd.edw_strt_dttm3      AS edw_strt_dttm3,
                upd_aplctn_frm_ques_upd.lkp_edw_end_dttm3   AS lkp_edw_end_dttm3,
                upd_aplctn_frm_ques_upd.in_trans_strt_dttm3 AS in_trans_strt_dttm3,
                upd_aplctn_frm_ques_upd.questionlabel3      AS questionlabel3,
                upd_aplctn_frm_ques_upd.source_record_id
         FROM   upd_aplctn_frm_ques_upd
         WHERE  upd_aplctn_frm_ques_upd.lkp_edw_end_dttm3 = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  -- Component upd_aplctn_frm_ques_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_frm_ques_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_frm_ques_insupd_insert.doc_id                     AS doc_id,
                rtr_aplctn_frm_ques_insupd_insert.questioncode               AS questioncode1,
                rtr_aplctn_frm_ques_insupd_insert.out_prcs_id                AS out_prcs_id1,
                rtr_aplctn_frm_ques_insupd_insert.edw_strt_dttm              AS edw_strt_dttm1,
                rtr_aplctn_frm_ques_insupd_insert.edw_end_dttm               AS edw_end_dttm1,
                rtr_aplctn_frm_ques_insupd_insert.in_aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dt,
                rtr_aplctn_frm_ques_insupd_insert.in_aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dt,
                rtr_aplctn_frm_ques_insupd_insert.in_trans_strt_dttm         AS trans_strt_dttm,
                rtr_aplctn_frm_ques_insupd_insert.retired                    AS retired1,
                rtr_aplctn_frm_ques_insupd_insert.questionlabel              AS questionlabel1,
                0                                                            AS update_strategy_action,
				rtr_aplctn_frm_ques_insupd_insert.source_record_id
         FROM   rtr_aplctn_frm_ques_insupd_insert );
  -- Component exp_pass_to_tgt_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
  (
         SELECT upd_aplctn_frm_ques_retire.questioncode3  AS aplctn_frm_ques_num,
                upd_aplctn_frm_ques_retire.doc_id3        AS doc_id,
                upd_aplctn_frm_ques_retire.edw_strt_dttm3 AS edw_strt_dttm3,
                current_timestamp                         AS out_edw_end_dttm,
                current_timestamp                         AS o_trans_end_dttm,
                upd_aplctn_frm_ques_retire.source_record_id
         FROM   upd_aplctn_frm_ques_retire );
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT fil_valid_recs.questioncode3                           AS aplctn_frm_ques_num,
                fil_valid_recs.doc_id3                                 AS doc_id,
                fil_valid_recs.edw_strt_dttm3                          AS edw_strt_dttm3,
                dateadd(''second'', - 1, fil_valid_recs.edw_strt_dttm3)      AS out_edw_end_dttm,
                dateadd(''second'', - 1, fil_valid_recs.in_trans_strt_dttm3) AS o_trans_end_dttm,
                fil_valid_recs.source_record_id
         FROM   fil_valid_recs );
  -- Component fil_active_recs, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_active_recs AS
  (
         SELECT upd_aplctn_frm_ques_insupd.doc_id                  AS doc_id,
                upd_aplctn_frm_ques_insupd.questioncode1           AS questioncode1,
                upd_aplctn_frm_ques_insupd.out_prcs_id1            AS out_prcs_id1,
                upd_aplctn_frm_ques_insupd.edw_strt_dttm1          AS edw_strt_dttm1,
                upd_aplctn_frm_ques_insupd.edw_end_dttm1           AS edw_end_dttm1,
                upd_aplctn_frm_ques_insupd.aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dt,
                upd_aplctn_frm_ques_insupd.aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dt,
                upd_aplctn_frm_ques_insupd.trans_strt_dttm         AS trans_strt_dttm,
                upd_aplctn_frm_ques_insupd.retired3                AS retired3,
                upd_aplctn_frm_ques_insupd.questionlabel3          AS questionlabel3,
                upd_aplctn_frm_ques_insupd.source_record_id
         FROM   upd_aplctn_frm_ques_insupd
         WHERE  upd_aplctn_frm_ques_insupd.retired3 = 0 );
  -- Component tgt_APLCTN_FRM_QUES_retire, Type TARGET
  merge
  INTO         db_t_prod_core.aplctn_frm_ques
  USING        exp_pass_to_tgt_upd1
  ON (
                            aplctn_frm_ques.aplctn_frm_ques_num = exp_pass_to_tgt_upd1.aplctn_frm_ques_num
               AND          aplctn_frm_ques.doc_id = exp_pass_to_tgt_upd1.doc_id
               AND          aplctn_frm_ques.edw_strt_dttm = exp_pass_to_tgt_upd1.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    aplctn_frm_ques_num = exp_pass_to_tgt_upd1.aplctn_frm_ques_num,
         doc_id = exp_pass_to_tgt_upd1.doc_id,
         edw_strt_dttm = exp_pass_to_tgt_upd1.edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_upd1.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_upd1.o_trans_end_dttm;
  
  -- Component exp_pass_to_tgt_ins1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins1 AS
  (
         SELECT fil_active_recs.questioncode1           AS aplctn_frm_ques_num,
                fil_active_recs.doc_id                  AS doc_id,
                fil_active_recs.out_prcs_id1            AS prcs_id,
                fil_active_recs.edw_strt_dttm1          AS edw_strt_dttm1,
                fil_active_recs.edw_end_dttm1           AS edw_end_dttm1,
                fil_active_recs.aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dt,
                fil_active_recs.aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dt,
                fil_active_recs.trans_strt_dttm         AS trans_strt_dttm,
                fil_active_recs.questionlabel3          AS questionlabel3,
                fil_active_recs.source_record_id
         FROM   fil_active_recs );
  -- Component tgt_APLCTN_FRM_QUES_updins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_frm_ques
              (
                          aplctn_frm_ques_num,
                          doc_id,
                          aplctn_frm_ques_txt,
                          prcs_id,
                          aplctn_frm_ques_strt_dttm,
                          aplctn_frm_ques_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins1.aplctn_frm_ques_num     AS aplctn_frm_ques_num,
         exp_pass_to_tgt_ins1.doc_id                  AS doc_id,
         exp_pass_to_tgt_ins1.questionlabel3          AS aplctn_frm_ques_txt,
         exp_pass_to_tgt_ins1.prcs_id                 AS prcs_id,
         exp_pass_to_tgt_ins1.aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dttm,
         exp_pass_to_tgt_ins1.aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dttm,
         exp_pass_to_tgt_ins1.edw_strt_dttm1          AS edw_strt_dttm,
         exp_pass_to_tgt_ins1.edw_end_dttm1           AS edw_end_dttm,
         exp_pass_to_tgt_ins1.trans_strt_dttm         AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins1;
  
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_aplctn_frm_ques_ins.questioncode1           AS aplctn_frm_ques_num,
                upd_aplctn_frm_ques_ins.doc_id                  AS doc_id,
                upd_aplctn_frm_ques_ins.out_prcs_id1            AS prcs_id,
                upd_aplctn_frm_ques_ins.edw_strt_dttm1          AS edw_strt_dttm1,
                upd_aplctn_frm_ques_ins.aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dt,
                upd_aplctn_frm_ques_ins.aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dt,
                upd_aplctn_frm_ques_ins.trans_strt_dttm         AS trans_strt_dttm,
                upd_aplctn_frm_ques_ins.questionlabel1          AS questionlabel1,
                CASE
                       WHEN upd_aplctn_frm_ques_ins.retired1 = 0 THEN upd_aplctn_frm_ques_ins.edw_end_dttm1
                       ELSE upd_aplctn_frm_ques_ins.edw_strt_dttm1
                END AS edw_end_dttm11,
                CASE
                       WHEN upd_aplctn_frm_ques_ins.retired1 = 0 THEN to_date ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd hh24:mi:ss.ff6'' )
                       ELSE upd_aplctn_frm_ques_ins.trans_strt_dttm
                END AS o_trans_end_dttm,
                upd_aplctn_frm_ques_ins.source_record_id
         FROM   upd_aplctn_frm_ques_ins );
  -- Component tgt_APLCTN_FRM_QUES_ins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_frm_ques
              (
                          aplctn_frm_ques_num,
                          doc_id,
                          aplctn_frm_ques_txt,
                          prcs_id,
                          aplctn_frm_ques_strt_dttm,
                          aplctn_frm_ques_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.aplctn_frm_ques_num     AS aplctn_frm_ques_num,
         exp_pass_to_tgt_ins.doc_id                  AS doc_id,
         exp_pass_to_tgt_ins.questionlabel1          AS aplctn_frm_ques_txt,
         exp_pass_to_tgt_ins.prcs_id                 AS prcs_id,
         exp_pass_to_tgt_ins.aplctn_frm_ques_strt_dt AS aplctn_frm_ques_strt_dttm,
         exp_pass_to_tgt_ins.aplctn_frm_ques_end_dt  AS aplctn_frm_ques_end_dttm,
         exp_pass_to_tgt_ins.edw_strt_dttm1          AS edw_strt_dttm,
         exp_pass_to_tgt_ins.edw_end_dttm11          AS edw_end_dttm,
         exp_pass_to_tgt_ins.trans_strt_dttm         AS trans_strt_dttm,
         exp_pass_to_tgt_ins.o_trans_end_dttm        AS trans_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component tgt_APLCTN_FRM_QUES_upd, Type TARGET
  merge
  INTO         db_t_prod_core.aplctn_frm_ques
  USING        exp_pass_to_tgt_upd
  ON (
                            aplctn_frm_ques.aplctn_frm_ques_num = exp_pass_to_tgt_upd.aplctn_frm_ques_num
               AND          aplctn_frm_ques.doc_id = exp_pass_to_tgt_upd.doc_id
               AND          aplctn_frm_ques.edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    aplctn_frm_ques_num = exp_pass_to_tgt_upd.aplctn_frm_ques_num,
         doc_id = exp_pass_to_tgt_upd.doc_id,
         edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_upd.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_upd.o_trans_end_dttm;

END;
';