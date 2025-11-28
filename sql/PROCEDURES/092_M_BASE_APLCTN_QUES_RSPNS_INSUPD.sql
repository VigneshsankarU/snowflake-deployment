-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_QUES_RSPNS_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_pc_periodanswer, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_periodanswer AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_aplctn_id,
                $2  AS lkp_aplctn_frm_ques_num,
                $3  AS lkp_doc_id,
                $4  AS lkp_edw_strt_dttm,
                $5  AS lkp_edw_end_dttm,
                $6  AS lkp_trans_strt_dttm,
                $7  AS questioncode_pc_job,
                $8  AS textanswer,
                $9  AS dateanswer,
                $10 AS booleananswer,
                $11 AS trans_strt_dttm,
                $12 AS retired,
                $13 AS aplctn_id,
                $14 AS doc_id,
                $15 AS sourcedata,
                $16 AS targetdata,
                $17 AS ins_upd_flag,
                $18 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  /*Joe''s Query*/
                                                  SELECT          tgt_aplctn_ques_rspn.aplctn_id           AS lkp_aplctn_id,
                                                                  tgt_aplctn_ques_rspn.aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
                                                                  tgt_aplctn_ques_rspn.doc_id              AS lkp_doc_id,
                                                                  tgt_aplctn_ques_rspn.edw_strt_dttm       AS lkp_edw_strt_dttm,
                                                                  tgt_aplctn_ques_rspn.tg_edw_end_dttm     AS lkp_edw_end_dttm,
                                                                  tgt_aplctn_ques_rspn.trans_strt_dttm     AS lkp_trans_strt_dttm,
                                                                  xlat_src.questioncode_pc_job             AS questioncode_pc_job,
                                                                /*/  CASE
                                                                                  WHEN translate_chk(xlat_src.textanswer USING latin_to_unicode)=''0'' THEN xlat_src.textanswer
                                                                                  ELSE cast(regexp_replace(substr(xlat_src.textanswer,1,translate_chk(xlat_src.textanswer USING latin_to_unicode)-1) , ''[^0-9 a-z]'', '''', 1, 0, ''i'')
                                                                                                                  ||''-modified due to junk data'' AS VARCHAR(1000))
                                                                  END     */
                                                                    CASE
                                                                    WHEN REGEXP_INSTR(xlat_src.textanswer, ''[^[:print:]]'') = 0 
                                                                        THEN xlat_src.textanswer
                                                                    ELSE CAST(
                                                                        REGEXP_REPLACE(
                                                                        SUBSTR(
                                                                            xlat_src.textanswer,
                                                                            1,
                                                                            REGEXP_INSTR(xlat_src.textanswer, ''[^[:print:]]'') - 1
                                                                        ),
                                                                        ''(?i)[^0-9a-z]'',
                                                                        ''''
                                                                        )
                                                                        || ''-modified due to junk data''
                                                                        AS VARCHAR(1000)
                                                                    )
                                                                    END AS  textanswer_new,
                                                                  xlat_src.dateanswer     AS dateanswer,
                                                                  xlat_src.booleananswer  AS booleananswer,
                                                                  xlat_src.src_updatetime AS trans_strt_dttm,
                                                                  xlat_src.retired        AS retired,
                                                                  xlat_src.aplctn_id      AS aplctn_id,
                                                                  xlat_src.doc_id         AS doc_id,
                                                                  /* SourceData */
                                                                  cast(trim(cast(coalesce(textanswer_new ,''~'') AS VARCHAR(1000)))
                                                                                  || trim(to_char((coalesce(dateanswer,to_date(''12/31/9999'',''MM/DD/YYYY'')))))
                                                                                  || trim(cast(coalesce(xlat_src.booleananswer,''~'') AS VARCHAR(3))) AS VARCHAR(1500)) AS sourcedata,
                                                                  /* TargetData */
                                                                  cast((trim(coalesce(tgt_aplctn_ques_rspn.aplctn_rspns_txt,''~''))
                                                                                  || trim(to_char((coalesce(tgt_aplctn_ques_rspn.aplctn_rspns_dttm,to_date(''12/31/9999'',''MM/DD/YYYY'')))))
                                                                                  || trim(coalesce(tgt_aplctn_ques_rspn.aplctn_rspns_ind,''~''))) AS VARCHAR(1500)) AS targetdata,
                                                                  /* Flag */
                                                                  CASE
                                                                                  WHEN tgt_aplctn_ques_rspn.aplctn_frm_ques_num IS NULL
                                                                                  AND             (
                                                                                                                  xlat_src.aplctn_id) IS NOT NULL
                                                                                  AND             (
                                                                                                                  xlat_src.doc_id) IS NOT NULL THEN ''I''
                                                                                  WHEN tgt_aplctn_ques_rspn.aplctn_frm_ques_num IS NOT NULL
                                                                                  AND             sourcedata <> targetdata
                                                                                  AND             (
                                                                                                                  xlat_src.aplctn_id) IS NOT NULL
                                                                                  AND             (
                                                                                                                  xlat_src.doc_id) IS NOT NULL THEN ''U''
                                                                                  WHEN tgt_aplctn_ques_rspn.aplctn_frm_ques_num IS NOT NULL
                                                                                  AND             sourcedata = targetdata
                                                                                  AND             (
                                                                                                                  xlat_src.aplctn_id) IS NOT NULL
                                                                                  AND             (
                                                                                                                  xlat_src.doc_id) IS NOT NULL THEN ''R''
                                                                  END AS ins_upd_flag
                                                  FROM
                                                                  /*source query with Expression*/
                                                                  (
                                                                         SELECT aplctn_lkp.aplctn_id,
                                                                                doc_lkp.doc_id,
                                                                                cast(src.questioncode_pc_job AS VARCHAR(50))AS questioncode_pc_job,
                                                                                src.sourcefile,
                                                                                to_date (coalesce(trim(cast(src.dateanswer AS VARCHAR(10))),''1900-01-01''),''YYYY-MM-DD'') AS dateanswer,
                                                                                trim(src.booleananswer)                                                                             AS booleananswer,
                                                                                src.textanswer                                                                                      AS textanswer,
                                                                                CASE
                                                                                       WHEN updatetime IS NOT NULL THEN cast(updatetime AS VARCHAR(50))
                                                                                       WHEN updatetime IS NULL THEN cast(to_date(''01/01/1900'',''MM/DD/YYYY'' ) AS VARCHAR(20))
                                                                                END AS src_updatetime,
                                                                                /* CAST(CURRENT_DATE AS TIMESTAMP) AS EDW_STRT_DTTM,                                                              */
                                                                                doc_lkp.doc_type_cd AS doc_type_cd,
                                                                                /* ------------------CHANGED FROM XLAT TO MAIN TABLE-------------------------------------------------JOE */
                                                                                doc_lkp.doc_ctgy_type_cd AS doc_ctgy_type_cd,
                                                                                /* ------------------CHANGED FROM XLAT TO MAIN TABLE-------------------------------------------------JOE */
                                                                                xlat_src_sys.tgt_idntftn_val AS sys_src_cd,
                                                                                src.retired                  AS retired,
                                                                                /* CAST(''12/31/9999 23:59:59.999999'' AS TIMESTAMP FORMAT ''MM/DD/YYYYBHH:MI:SS.S(6)'' ) AS EDW_END_DTTM, */
                                                                                src.jobnumber
                                                                         FROM
                                                                                /*source query*/
                                                                                (
                                                                                       SELECT tmp.booleananswer,
                                                                                              tmp.textanswer,
                                                                                              tmp.dateanswer,
                                                                                              tmp.jobnumber,
                                                                                              tmp.typecode,
                                                                                              tmp.questioncode_pc_job,
                                                                                              tmp.sourcefile,
                                                                                              ''SRC_SYS4'' AS sys_src_cd,
                                                                                              tmp.retired,
                                                                                              tmp.updatetime
                                                                                       FROM   (
                                                                                                     SELECT pc_periodanswer.booleananswer       AS booleananswer,
                                                                                                            pc_periodanswer.textanswer          AS textanswer,
                                                                                                            pc_periodanswer.dateanswer          AS dateanswer,
                                                                                                            pc_periodanswer.jobnumber           AS jobnumber,
                                                                                                            pc_periodanswer.typecode            AS typecode,
                                                                                                            pc_periodanswer.questioncode_pc_job AS questioncode_pc_job,
                                                                                                            pc_periodanswer.sourcefile          AS sourcefile,
                                                                                                            ''SRC_SYS4''                          AS sys_src_cd,
                                                                                                            pc_periodanswer.updatetime          AS updatetime,
                                                                                                            pc_periodanswer.retired             AS retired
                                                                                                     FROM   (
                                                                                                                      /* -EIM-41188 Removed Unused INFORMATION_SCHEMA.columns from select list for below unions                                                                                         */
                                                                                                                      SELECT    pc_prd.booleananswer_stg   AS booleananswer,
                                                                                                                                pc_prd.textanswer_stg      AS textanswer,
                                                                                                                                pc_prd.dateanswer_stg      AS dateanswer,
                                                                                                                                pc_jb.jobnumber_stg        AS jobnumber,
                                                                                                                                pctl_jb.typecode_stg       AS typecode,
                                                                                                                                pc_ques.questioncode_stg   AS questioncode_pc_job,
                                                                                                                                pc_ques_set.sourcefile_stg AS sourcefile,
                                                                                                                                pc_prd.updatetime_stg         updatetime,
                                                                                                                                CASE
                                                                                                                                          WHEN pc_jb.retired_stg=0
                                                                                                                                          AND       pc_ques.retired_stg=0 THEN 0
                                                                                                                                          ELSE 1
                                                                                                                                END retired
                                                                                                                      FROM      db_t_prod_stag.pc_periodanswer pc_prd
                                                                                                                      left join db_t_prod_stag.pc_questionlookup pc_ques
                                                                                                                      ON        pc_prd.questioncode_stg=pc_ques.questioncode_stg
                                                                                                                      left join db_t_prod_stag.pc_questionsetlookup pc_ques_set
                                                                                                                      ON        pc_ques.sourcefile_stg=pc_ques_set.sourcefile_stg
                                                                                                                      left join db_t_prod_stag.pc_policyperiod pc_plcyprd
                                                                                                                      ON        pc_plcyprd.id_stg=pc_prd.branchid_stg
                                                                                                                      left join db_t_prod_stag.pc_job pc_jb
                                                                                                                      ON        pc_jb.id_stg=pc_plcyprd.jobid_stg
                                                                                                                      left join db_t_prod_stag.pctl_job pctl_jb
                                                                                                                      ON        pc_jb.subtype_stg=pctl_jb.id_stg
                                                                                                                      WHERE     pc_prd.updatetime_stg> ($start_dttm)
                                                                                                                      AND       pc_prd.updatetime_stg <= ($end_dttm) qualify row_number() over(PARTITION BY jobnumber,typecode,questioncode_pc_job,sourcefile ORDER BY updatetime DESC,booleananswer DESC,dateanswer DESC,textanswer DESC)=1
                                                                                                                      UNION
                                                                                                                      /*EIM-41188 Added the case logic for textanswer column*/
                                                                                                                      SELECT    pcx_dwel.booleananswer_stg AS booleananswer,
                                                                                                                                CASE
                                                                                                                                          WHEN pcx_dwel.textanswer_stg IS NULL THEN pcx_dwel.choiceanswerchoicecode_alfa_stg
                                                                                                                                          ELSE pcx_dwel.textanswer_stg
                                                                                                                                END                        AS textanswer,
                                                                                                                                pcx_dwel.dateanswer_stg    AS dateanswer,
                                                                                                                                pc_jb.jobnumber_stg        AS jobnumber,
                                                                                                                                pctl_jb.typecode_stg       AS typecode,
                                                                                                                                pc_ques.questioncode_stg   AS questioncode_pc_job,
                                                                                                                                pc_ques_set.sourcefile_stg AS sourcefile,
                                                                                                                                pcx_dwel.updatetime_stg    AS updatetime,
                                                                                                                                CASE
                                                                                                                                          WHEN pc_jb.retired_stg=0
                                                                                                                                          AND       pc_ques.retired_stg=0 THEN 0
                                                                                                                                          ELSE 1
                                                                                                                                END retired
                                                                                                                      FROM      db_t_prod_stag.pcx_dwellinganswer_alf pcx_dwel
                                                                                                                      left join db_t_prod_stag.pc_questionlookup pc_ques
                                                                                                                      ON        pcx_dwel.questioncode_stg=pc_ques.questioncode_stg
                                                                                                                      left join db_t_prod_stag.pc_questionsetlookup pc_ques_set
                                                                                                                      ON        pc_ques.sourcefile_stg=pc_ques_set.sourcefile_stg
                                                                                                                      left join db_t_prod_stag.pc_policyperiod pc_plcyprd
                                                                                                                      ON        pc_plcyprd.id_stg=pcx_dwel.branchid_stg
                                                                                                                      left join db_t_prod_stag.pc_job pc_jb
                                                                                                                      ON        pc_jb.id_stg=pc_plcyprd.jobid_stg
                                                                                                                      left join db_t_prod_stag.pctl_job pctl_jb
                                                                                                                      ON        pc_jb.subtype_stg=pctl_jb.id_stg
                                                                                                                      WHERE     pcx_dwel.updatetime_stg> ($start_dttm)
                                                                                                                      AND       pcx_dwel.updatetime_stg <= ($end_dttm) qualify row_number() over(PARTITION BY jobnumber,typecode,questioncode_pc_job,sourcefile ORDER BY updatetime DESC,booleananswer DESC,dateanswer DESC,textanswer DESC)=1
                                                                                                                      UNION
                                                                                                                      SELECT    pcx_veh.booleananswer_stg  AS booleananswer,
                                                                                                                                pcx_veh.textanswer_stg     AS textanswer,
                                                                                                                                pcx_veh.dateanswer_stg     AS dateanswer,
                                                                                                                                pc_jb.jobnumber_stg        AS jobnumber,
                                                                                                                                pctl_jb.typecode_stg       AS typecode,
                                                                                                                                pc_ques.questioncode_stg   AS questioncode_pc_job,
                                                                                                                                pc_ques_set.sourcefile_stg AS sourcefile,
                                                                                                                                pcx_veh.updatetime_stg     AS updatetime,
                                                                                                                                CASE
                                                                                                                                          WHEN pc_jb.retired_stg=0
                                                                                                                                          AND       pc_ques.retired_stg=0 THEN 0
                                                                                                                                          ELSE 1
                                                                                                                                END retired
                                                                                                                      FROM      db_t_prod_stag.pcx_vehicleanswer_alfa pcx_veh
                                                                                                                      left join db_t_prod_stag.pc_questionlookup pc_ques
                                                                                                                      ON        pcx_veh.questioncode_stg=pc_ques.questioncode_stg
                                                                                                                      left join db_t_prod_stag.pc_questionsetlookup pc_ques_set
                                                                                                                      ON        pc_ques.sourcefile_stg=pc_ques_set.sourcefile_stg
                                                                                                                      left join db_t_prod_stag.pc_policyperiod pc_plcyprd
                                                                                                                      ON        pc_plcyprd.id_stg=pcx_veh.branchid_stg
                                                                                                                      left join db_t_prod_stag.pc_job pc_jb
                                                                                                                      ON        pc_jb.id_stg=pc_plcyprd.jobid_stg
                                                                                                                      left join db_t_prod_stag.pctl_job pctl_jb
                                                                                                                      ON        pc_jb.subtype_stg=pctl_jb.id_stg
                                                                                                                      WHERE     pcx_veh.updatetime_stg> ($start_dttm)
                                                                                                                      AND       pcx_veh.updatetime_stg <= ($end_dttm) qualify row_number() over(PARTITION BY jobnumber,typecode,questioncode_pc_job,sourcefile ORDER BY updatetime DESC,booleananswer DESC,dateanswer DESC,textanswer DESC)=1 ) pc_periodanswer
                                                                                                     WHERE  pc_periodanswer.typecode IN (''Submission'',
                                                                                                                                         ''PolicyChange'',
                                                                                                                                         ''Renewal'')
                                                                                                     AND    pc_periodanswer.sourcefile IS NOT NULL ) tmp ) AS src
                                                                         join   db_t_prod_core.teradata_etl_ref_xlat                                       AS xlat_aplctn_type
                                                                         ON     xlat_aplctn_type.src_idntftn_val= src.typecode
                                                                         AND    xlat_aplctn_type.tgt_idntftn_nm= ''APLCTN_TYPE''
                                                                         AND    xlat_aplctn_type.src_idntftn_nm= ''pctl_job.Typecode''
                                                                         AND    xlat_aplctn_type.src_idntftn_sys= ''GW''
                                                                         AND    xlat_aplctn_type.expn_dt=''9999-12-31''
                                                                         join   db_t_prod_core.teradata_etl_ref_xlat AS xlat_src_sys
                                                                         ON     xlat_src_sys.src_idntftn_val = src.sys_src_cd
                                                                         AND    xlat_src_sys.tgt_idntftn_nm= ''SRC_SYS''
                                                                         AND    xlat_src_sys.src_idntftn_nm= ''derived''
                                                                         AND    xlat_src_sys.src_idntftn_sys= ''DS''
                                                                         AND    xlat_src_sys.expn_dt=''9999-12-31''
                                                                         join
                                                                                /*lkp_aplctn*/
                                                                                (
                                                                                         SELECT   aplctn.aplctn_id      AS aplctn_id,
                                                                                                  aplctn.host_aplctn_id AS host_aplctn_id,
                                                                                                  aplctn.src_sys_cd     AS src_sys_cd,
                                                                                                  aplctn.aplctn_type_cd AS aplctn_type_cd
                                                                                         FROM     db_t_prod_core.aplctn  qualify row_number () over (PARTITION BY host_aplctn_id,src_sys_cd ORDER BY edw_end_dttm DESC)=1 ) AS aplctn_lkp
                                                                         ON     aplctn_lkp.src_sys_cd=xlat_src_sys.tgt_idntftn_val
                                                                         AND    aplctn_lkp.host_aplctn_id = src.jobnumber
                                                                         AND    aplctn_lkp.aplctn_type_cd = coalesce(xlat_aplctn_type.tgt_idntftn_val,''UNK'')
                                                                         join
                                                                                /*lkp_doc*/
                                                                                (
                                                                                         SELECT   doc.doc_id           AS doc_id,
                                                                                                  doc.doc_issur_num    AS doc_issur_num,
                                                                                                  doc.doc_type_cd      AS doc_type_cd,
                                                                                                  doc.doc_ctgy_type_cd AS doc_ctgy_type_cd
                                                                                         FROM     db_t_prod_core.doc 
                                                                                         WHERE    doc.doc_type_cd= ''QSTNSET''
                                                                                         AND      doc.doc_ctgy_type_cd= ''APLCTN'' qualify row_number () over (PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS doc_lkp
                                                                         ON     doc_lkp.doc_issur_num=src.sourcefile ) AS xlat_src
                                                  left outer join
                                                                  (
                                                                           SELECT   aplctn_ques_rspns.aplctn_rspns_txt,
                                                                                    aplctn_ques_rspns.aplctn_rspns_dttm,
                                                                                    aplctn_ques_rspns.aplctn_rspns_ind,
                                                                                    aplctn_ques_rspns.edw_strt_dttm,
                                                                                    aplctn_ques_rspns.edw_end_dttm AS tg_edw_end_dttm,
                                                                                    aplctn_ques_rspns.trans_strt_dttm,
                                                                                    aplctn_ques_rspns.aplctn_id,
                                                                                    aplctn_ques_rspns.doc_id,
                                                                                    aplctn_ques_rspns.aplctn_frm_ques_num
                                                                           FROM     db_t_prod_core.aplctn_ques_rspns  qualify row_number() over(PARTITION BY aplctn_id,doc_id,aplctn_frm_ques_num ORDER BY edw_end_dttm DESC) = 1 ) AS tgt_aplctn_ques_rspn
                                                  ON              xlat_src.questioncode_pc_job=tgt_aplctn_ques_rspn.aplctn_frm_ques_num
                                                  AND             tgt_aplctn_ques_rspn.aplctn_id=xlat_src.aplctn_id
                                                  AND             tgt_aplctn_ques_rspn.doc_id=xlat_src.doc_id ) src ) );
  -- Component exp_data_transaformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transaformation AS
  (
         SELECT current_timestamp                                                      AS o_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS o_edw_end_dttm,
                $prcs_id                                                               AS prcs_id,
                to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )                                AS o_default_date,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS o_default_enddate,
                sq_pc_periodanswer.lkp_aplctn_id                                       AS lkp_aplctn_id,
                sq_pc_periodanswer.lkp_aplctn_frm_ques_num                             AS lkp_aplctn_frm_ques_num,
                sq_pc_periodanswer.lkp_doc_id                                          AS lkp_doc_id,
                sq_pc_periodanswer.lkp_edw_strt_dttm                                   AS lkp_edw_strt_dttm,
                sq_pc_periodanswer.lkp_edw_end_dttm                                    AS lkp_edw_end_dttm,
                sq_pc_periodanswer.lkp_trans_strt_dttm                                 AS lkp_trans_strt_dttm,
                sq_pc_periodanswer.questioncode_pc_job                                 AS questioncode_pc_job,
                sq_pc_periodanswer.textanswer                                          AS textanswer,
                sq_pc_periodanswer.dateanswer                                          AS dateanswer,
                sq_pc_periodanswer.booleananswer                                       AS booleananswer,
                sq_pc_periodanswer.trans_strt_dttm                                     AS trans_strt_dttm,
                sq_pc_periodanswer.retired                                             AS retired,
                sq_pc_periodanswer.aplctn_id                                           AS aplctn_id,
                sq_pc_periodanswer.doc_id                                              AS doc_id,
                sq_pc_periodanswer.ins_upd_flag                                        AS ins_upd_flag,
                sq_pc_periodanswer.source_record_id
         FROM   sq_pc_periodanswer );
  -- Component rtr_aplctn_ques_rspns_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_aplctn_ques_rspns_insert AS
  SELECT exp_data_transaformation.ins_upd_flag            AS out_ins_upd,
         exp_data_transaformation.lkp_aplctn_id           AS lkp_aplctn_id,
         exp_data_transaformation.questioncode_pc_job     AS in_questioncode_pc_job,
         exp_data_transaformation.lkp_doc_id              AS lkp_doc_id,
         exp_data_transaformation.textanswer              AS in_textanswer,
         exp_data_transaformation.prcs_id                 AS prcs_id,
         exp_data_transaformation.dateanswer              AS in_dateanswer,
         exp_data_transaformation.booleananswer           AS in_booleananswer,
         exp_data_transaformation.lkp_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
         exp_data_transaformation.o_edw_strt_dttm         AS edw_strt_dttm,
         exp_data_transaformation.o_edw_end_dttm          AS edw_end_dttm,
         exp_data_transaformation.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_data_transaformation.o_default_date          AS o_default_date,
         exp_data_transaformation.o_default_enddate       AS o_default_enddate,
         exp_data_transaformation.trans_strt_dttm         AS trans_strt_dttm,
         exp_data_transaformation.lkp_trans_strt_dttm     AS lkp_trans_strt_dttm,
         exp_data_transaformation.aplctn_id               AS in_aplctn_id,
         exp_data_transaformation.doc_id                  AS in_doc_id,
         exp_data_transaformation.retired                 AS retired,
         exp_data_transaformation.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_data_transaformation.source_record_id
  FROM   exp_data_transaformation
  WHERE  (
                exp_data_transaformation.aplctn_id IS NOT NULL
         AND    exp_data_transaformation.doc_id IS NOT NULL
         AND    (
                       exp_data_transaformation.ins_upd_flag = ''I''
                OR     (
                              exp_data_transaformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       AND    exp_data_transaformation.retired = 0 ) ) )
  OR     (
                exp_data_transaformation.aplctn_id IS NOT NULL
         AND    exp_data_transaformation.doc_id IS NOT NULL
         AND    (
                       exp_data_transaformation.ins_upd_flag = ''U''
                AND    exp_data_transaformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_aplctn_ques_rspns_RETIRED, Type ROUTER Output Group RETIRED
    create or replace temporary table rtr_aplctn_ques_rspns_retired AS
  SELECT exp_data_transaformation.ins_upd_flag            AS out_ins_upd,
         exp_data_transaformation.lkp_aplctn_id           AS lkp_aplctn_id,
         exp_data_transaformation.questioncode_pc_job     AS in_questioncode_pc_job,
         exp_data_transaformation.lkp_doc_id              AS lkp_doc_id,
         exp_data_transaformation.textanswer              AS in_textanswer,
         exp_data_transaformation.prcs_id                 AS prcs_id,
         exp_data_transaformation.dateanswer              AS in_dateanswer,
         exp_data_transaformation.booleananswer           AS in_booleananswer,
         exp_data_transaformation.lkp_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num,
         exp_data_transaformation.o_edw_strt_dttm         AS edw_strt_dttm,
         exp_data_transaformation.o_edw_end_dttm          AS edw_end_dttm,
         exp_data_transaformation.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_data_transaformation.o_default_date          AS o_default_date,
         exp_data_transaformation.o_default_enddate       AS o_default_enddate,
         exp_data_transaformation.trans_strt_dttm         AS trans_strt_dttm,
         exp_data_transaformation.lkp_trans_strt_dttm     AS lkp_trans_strt_dttm,
         exp_data_transaformation.aplctn_id               AS in_aplctn_id,
         exp_data_transaformation.doc_id                  AS in_doc_id,
         exp_data_transaformation.retired                 AS retired,
         exp_data_transaformation.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_data_transaformation.source_record_id
  FROM   exp_data_transaformation
  WHERE  exp_data_transaformation.ins_upd_flag = ''R''
  AND    exp_data_transaformation.retired != 0
  AND    exp_data_transaformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_aplctn_ques_rspns_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_ques_rspns_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_ques_rspns_retired.lkp_aplctn_id           AS lkp_aplctn_id3,
                rtr_aplctn_ques_rspns_retired.lkp_aplctn_frm_ques_num AS lkp_aplctn_frm_ques_num3,
                rtr_aplctn_ques_rspns_retired.lkp_doc_id              AS lkp_doc_id3,
                rtr_aplctn_ques_rspns_retired.prcs_id                 AS prcs_id3,
                rtr_aplctn_ques_rspns_retired.lkp_edw_strt_dttm       AS edw_strt_dttm3,
                rtr_aplctn_ques_rspns_retired.trans_strt_dttm         AS trans_strt_dttm4,
                1                                                     AS update_strategy_action,
                rtr_aplctn_ques_rspns_retired.source_record_id
         FROM   rtr_aplctn_ques_rspns_retired );
  -- Component upd_aplctn_ques_rspns_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_ques_rspns_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_aplctn_ques_rspns_insert.in_aplctn_id           AS lkp_aplctn_id1,
                rtr_aplctn_ques_rspns_insert.in_questioncode_pc_job AS in_questioncode_pc_job1,
                rtr_aplctn_ques_rspns_insert.in_doc_id              AS lkp_doc_id1,
                rtr_aplctn_ques_rspns_insert.in_textanswer          AS in_textanswer1,
                rtr_aplctn_ques_rspns_insert.prcs_id                AS prcs_id1,
                rtr_aplctn_ques_rspns_insert.in_dateanswer          AS in_dateanswer1,
                rtr_aplctn_ques_rspns_insert.in_booleananswer       AS in_booleananswer1,
                rtr_aplctn_ques_rspns_insert.edw_strt_dttm          AS edw_strt_dttm1,
                rtr_aplctn_ques_rspns_insert.edw_end_dttm           AS edw_end_dttm1,
                rtr_aplctn_ques_rspns_insert.o_default_date         AS o_default_date1,
                rtr_aplctn_ques_rspns_insert.o_default_enddate      AS o_default_enddate1,
                rtr_aplctn_ques_rspns_insert.trans_strt_dttm        AS trans_strt_dttm1,
                rtr_aplctn_ques_rspns_insert.retired                AS retired1,
                0                                                   AS update_strategy_action,
                rtr_aplctn_ques_rspns_insert.source_record_id
         FROM   rtr_aplctn_ques_rspns_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_aplctn_ques_rspns_ins.lkp_aplctn_id1          AS aplctn_id,
                upd_aplctn_ques_rspns_ins.in_questioncode_pc_job1 AS aplctn_frm_ques_num,
                upd_aplctn_ques_rspns_ins.lkp_doc_id1             AS doc_id,
                upd_aplctn_ques_rspns_ins.in_textanswer1          AS aplctn_rspns_txt,
                upd_aplctn_ques_rspns_ins.prcs_id1                AS prcs_id,
                upd_aplctn_ques_rspns_ins.in_dateanswer1          AS aplctn_rspns_dt,
                upd_aplctn_ques_rspns_ins.in_booleananswer1       AS aplctn_rspns_ind,
                upd_aplctn_ques_rspns_ins.edw_strt_dttm1          AS edw_strt_dttm1,
                upd_aplctn_ques_rspns_ins.trans_strt_dttm1        AS trans_strt_dttm1,
                CASE
                       WHEN upd_aplctn_ques_rspns_ins.retired1 <> 0 THEN upd_aplctn_ques_rspns_ins.trans_strt_dttm1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm11,
                CASE
                       WHEN upd_aplctn_ques_rspns_ins.retired1 = 0 THEN upd_aplctn_ques_rspns_ins.o_default_enddate1
                       ELSE upd_aplctn_ques_rspns_ins.edw_strt_dttm1
                END AS edw_end_dttm,
                upd_aplctn_ques_rspns_ins.source_record_id
         FROM   upd_aplctn_ques_rspns_ins );
  -- Component exp_pass_to_tgt_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_retired AS
  (
         SELECT upd_aplctn_ques_rspns_retired.lkp_aplctn_id3           AS aplctn_id,
                upd_aplctn_ques_rspns_retired.lkp_aplctn_frm_ques_num3 AS aplctn_frm_ques_num,
                upd_aplctn_ques_rspns_retired.lkp_doc_id3              AS doc_id,
                upd_aplctn_ques_rspns_retired.edw_strt_dttm3           AS edw_strt_dttm3,
                dateadd(''second'', - 1, current_timestamp)                  AS out_edw_end_dttm,
                upd_aplctn_ques_rspns_retired.trans_strt_dttm4         AS trans_strt_dttm4,
                upd_aplctn_ques_rspns_retired.source_record_id
         FROM   upd_aplctn_ques_rspns_retired );
  -- Component tgt_APLCTN_QUES_RSPNS_ins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_ques_rspns
              (
                          aplctn_id,
                          aplctn_frm_ques_num,
                          doc_id,
                          aplctn_rspns_txt,
                          prcs_id,
                          aplctn_rspns_dttm,
                          aplctn_rspns_ind,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.aplctn_id           AS aplctn_id,
         exp_pass_to_tgt_ins.aplctn_frm_ques_num AS aplctn_frm_ques_num,
         exp_pass_to_tgt_ins.doc_id              AS doc_id,
         exp_pass_to_tgt_ins.aplctn_rspns_txt    AS aplctn_rspns_txt,
         exp_pass_to_tgt_ins.prcs_id             AS prcs_id,
         exp_pass_to_tgt_ins.aplctn_rspns_dt     AS aplctn_rspns_dttm,
         exp_pass_to_tgt_ins.aplctn_rspns_ind    AS aplctn_rspns_ind,
         exp_pass_to_tgt_ins.edw_strt_dttm1      AS edw_strt_dttm,
         exp_pass_to_tgt_ins.edw_end_dttm        AS edw_end_dttm,
         exp_pass_to_tgt_ins.trans_strt_dttm1    AS trans_strt_dttm,
         exp_pass_to_tgt_ins.trans_end_dttm11    AS trans_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component tgt_APLCTN_QUES_RSPNS_retired, Type TARGET
  merge
  INTO         db_t_prod_core.aplctn_ques_rspns
  USING        exp_pass_to_tgt_retired
  ON (
                            aplctn_ques_rspns.aplctn_id = exp_pass_to_tgt_retired.aplctn_id
               AND          aplctn_ques_rspns.aplctn_frm_ques_num = exp_pass_to_tgt_retired.aplctn_frm_ques_num
               AND          aplctn_ques_rspns.doc_id = exp_pass_to_tgt_retired.doc_id
               AND          aplctn_ques_rspns.edw_strt_dttm = exp_pass_to_tgt_retired.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    aplctn_id = exp_pass_to_tgt_retired.aplctn_id,
         aplctn_frm_ques_num = exp_pass_to_tgt_retired.aplctn_frm_ques_num,
         doc_id = exp_pass_to_tgt_retired.doc_id,
         edw_strt_dttm = exp_pass_to_tgt_retired.edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_retired.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_retired.trans_strt_dttm4;
  
  -- Component tgt_APLCTN_QUES_RSPNS_retired, Type Post SQL
  UPDATE db_t_prod_core.aplctn_ques_rspns
    SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT aplctn_id,
                                         aplctn_frm_ques_num,
                                         doc_id,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY aplctn_id,aplctn_frm_ques_num,doc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY aplctn_id,aplctn_frm_ques_num,doc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.aplctn_ques_rspns
                         GROUP BY        aplctn_id,
                                         aplctn_frm_ques_num,
                                         doc_id,
                                         edw_strt_dttm,
                                         trans_strt_dttm ) a

  WHERE  aplctn_ques_rspns.edw_strt_dttm = a.edw_strt_dttm
  AND    aplctn_ques_rspns.trans_strt_dttm = a.trans_strt_dttm
  AND    aplctn_ques_rspns.aplctn_id = a.aplctn_id
  AND    aplctn_ques_rspns.aplctn_frm_ques_num = a.aplctn_frm_ques_num
  AND    aplctn_ques_rspns.doc_id = a.doc_id
  AND    cast(edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;

END;
';