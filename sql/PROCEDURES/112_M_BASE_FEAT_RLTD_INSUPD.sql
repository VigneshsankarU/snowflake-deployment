-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FEAT_RLTD_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_RLTD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_feat_rltd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''FEAT_RLTNSHP_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_PARENTSUBTPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_parentsubtpe AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''FEAT_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_pcx_etlclausepattern, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_etlclausepattern AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS parentpatternid,
                $2 AS parentsubtype,
                $3 AS childpatternid,
                $4 AS childtsubtype,
                $5 AS rltnshp_type_cd,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                             SELECT     pcp.patternid_stg                   AS parentpatternid,
                                                        cast(''FEAT_SBTYPE7'' AS VARCHAR(50)) AS parentsubtype,
                                                        pcv.patternid_stg                   AS childpatternid,
                                                        cast(''FEAT_SBTYPE6'' AS        VARCHAR(50)) AS childtsubtype,
                                                        cast( ''FEAT_RLTNSHP_TYPE5'' AS VARCHAR(50)) AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlclausepattern pcp
                                             ON         pcp.id_stg=pcv.clausepatternid_stg
                                             UNION
                                             SELECT     pcp.patternid_stg    AS parentpatternid,
                                                        ''FEAT_SBTYPE7''       AS parentsubtype,
                                                        pco.patternid_stg    AS childpatternid,
                                                        ''FEAT_SBTYPE8''       AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE3'' AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlclausepattern pcp
                                             ON         pcp.id_stg=pcv.clausepatternid_stg
                                             inner join db_t_prod_stag.pc_etlcovtermoption pco
                                             ON         pco.coveragetermpatternid_stg=pcv.id_stg
                                             UNION
                                             SELECT     pcp.patternid_stg    AS parentpatternid,
                                                        ''FEAT_SBTYPE7''       AS parentsubtype,
                                                        pct.patternid_stg    AS childpatternid,
                                                        ''FEAT_SBTYPE9''       AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE4'' AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlclausepattern pcp
                                             ON         pcp.id_stg=pcv.clausepatternid_stg
                                             inner join db_t_prod_stag.pc_etlcovtermpackage pct
                                             ON         pct.coveragetermpatternid_stg=pcv.id_stg
                                             UNION
                                             SELECT     pct.patternid_stg    AS parentpatternid,
                                                        ''FEAT_SBTYPE9''       AS parentsubtype,
                                                        pp.patternid_stg     AS childpatternid,
                                                        ''FEAT_SBTYPE10''      AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE6'' AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlclausepattern pcp
                                             ON         pcp.id_stg=pcv.clausepatternid_stg
                                             inner join db_t_prod_stag.pc_etlcovtermpackage pct
                                             ON         pct.coveragetermpatternid_stg=pcv.id_stg
                                             inner join db_t_prod_stag.pc_etlpackterm pp
                                             ON         pp.covtermpackid_stg=pct.id_stg
                                             UNION
                                             SELECT     pcv.patternid_stg    AS parentpatternid,
                                                        ''FEAT_SBTYPE6''       AS parentsubtype,
                                                        pct.patternid_stg    AS childpatternid,
                                                        ''FEAT_SBTYPE9''       AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE9'' AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlcovtermpackage pct
                                             ON         pct.coveragetermpatternid_stg=pcv.id_stg
                                             UNION
                                             SELECT     pcv.patternid_stg    AS parentpatternid,
                                                        ''FEAT_SBTYPE6''       AS parentsubtype,
                                                        pco.patternid_stg    AS childpatternid,
                                                        ''FEAT_SBTYPE8''       AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE8'' AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                             inner join db_t_prod_stag.pc_etlcovtermoption pco
                                             ON         pco.coveragetermpatternid_stg=pcv.id_stg
                                             UNION
                                             SELECT     pf.code_stg              AS parentpatternid,
                                                        ''FEAT_SBTYPE15''          AS parentsubtype,
                                                        pf.clausepatterncode_stg AS childpatternid,
                                                        ''FEAT_SBTYPE7''           AS childtsubtype,
                                                        ''FEAT_RLTNSHP_TYPE10''    AS rltnshp_type_cd
                                             FROM       db_t_prod_stag.pc_formpattern pf
                                             inner join db_t_prod_stag.pctl_documenttype pd
                                             ON         pd.id_stg= pf.documenttype_stg
                                             WHERE      typecode_stg = ''endorsement_alfa''
                                             AND        clausepatterncode_stg IS NOT NULL
                                             AND        pf.retired_stg = 0
                                             AND        pf.updatetime_stg > ($start_dttm)
                                             AND        pf.updatetime_stg <= ($end_dttm) ) src ) );
  -- Component exp_pass_to_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_src AS
  (
            SELECT    sq_pcx_etlclausepattern.parentpatternid AS parentpatternid,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PARENTSUBTPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PARENTSUBTPE */
                      END                                    AS out_parentsubtype,
                      sq_pcx_etlclausepattern.childpatternid AS childpatternid,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PARENTSUBTPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PARENTSUBTPE */
                      END AS out_childsubtype,
                      CASE
                                WHEN lkp_5.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_RLTD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_6.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_RLTD */
                      END AS out_rltnshp_type_cd,
                      sq_pcx_etlclausepattern.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_etlclausepattern.source_record_id ORDER BY sq_pcx_etlclausepattern.source_record_id) AS rnk
            FROM      sq_pcx_etlclausepattern
            left join lkp_teradata_etl_ref_xlat_parentsubtpe lkp_1
            ON        lkp_1.src_idntftn_val = sq_pcx_etlclausepattern.parentsubtype
            left join lkp_teradata_etl_ref_xlat_parentsubtpe lkp_2
            ON        lkp_2.src_idntftn_val = sq_pcx_etlclausepattern.parentsubtype
            left join lkp_teradata_etl_ref_xlat_parentsubtpe lkp_3
            ON        lkp_3.src_idntftn_val = sq_pcx_etlclausepattern.childtsubtype
            left join lkp_teradata_etl_ref_xlat_parentsubtpe lkp_4
            ON        lkp_4.src_idntftn_val = sq_pcx_etlclausepattern.childtsubtype
            left join lkp_teradata_etl_ref_xlat_feat_rltd lkp_5
            ON        lkp_5.src_idntftn_val = sq_pcx_etlclausepattern.rltnshp_type_cd
            left join lkp_teradata_etl_ref_xlat_feat_rltd lkp_6
            ON        lkp_6.src_idntftn_val = sq_pcx_etlclausepattern.rltnshp_type_cd qualify rnk = 1 );
  -- Component LKP_FEAT_PARENT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat_parent AS
  (
            SELECT    lkp.feat_id,
                      exp_pass_to_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_to_src.source_record_id ORDER BY lkp.feat_id DESC,lkp.feat_sbtype_cd DESC,lkp.nk_src_key DESC,lkp.feat_insrnc_sbtype_cd DESC,lkp.feat_clasfcn_cd DESC,lkp.feat_desc DESC,lkp.feat_name DESC,lkp.comn_feat_name DESC,lkp.feat_lvl_sbtype_cnt DESC,lkp.insrnc_cvge_type_cd DESC,lkp.insrnc_lob_type_cd DESC,lkp.prcs_id DESC) rnk
            FROM      exp_pass_to_src
            left join
                      (
                               SELECT   feat.feat_id               AS feat_id,
                                        feat.feat_insrnc_sbtype_cd AS feat_insrnc_sbtype_cd,
                                        feat.feat_clasfcn_cd       AS feat_clasfcn_cd,
                                        feat.feat_desc             AS feat_desc,
                                        feat.feat_name             AS feat_name,
                                        feat.comn_feat_name        AS comn_feat_name,
                                        feat.feat_lvl_sbtype_cnt   AS feat_lvl_sbtype_cnt,
                                        feat.insrnc_cvge_type_cd   AS insrnc_cvge_type_cd,
                                        feat.insrnc_lob_type_cd    AS insrnc_lob_type_cd,
                                        feat.prcs_id               AS prcs_id,
                                        feat.feat_sbtype_cd        AS feat_sbtype_cd,
                                        feat.nk_src_key            AS nk_src_key
                               FROM     db_t_prod_core.feat qualify row_number () over (PARTITION BY nk_src_key,feat_sbtype_cd ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.feat_sbtype_cd = exp_pass_to_src.out_parentsubtype
            AND       lkp.nk_src_key = exp_pass_to_src.parentpatternid qualify rnk = 1 );
  -- Component LKP_FEAT_CHILD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat_child AS
  (
            SELECT    lkp.feat_id,
                      exp_pass_to_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_to_src.source_record_id ORDER BY lkp.feat_id DESC,lkp.feat_sbtype_cd DESC,lkp.nk_src_key DESC,lkp.feat_insrnc_sbtype_cd DESC,lkp.feat_clasfcn_cd DESC,lkp.feat_desc DESC,lkp.feat_name DESC,lkp.comn_feat_name DESC,lkp.feat_lvl_sbtype_cnt DESC,lkp.insrnc_cvge_type_cd DESC,lkp.insrnc_lob_type_cd DESC,lkp.prcs_id DESC) rnk
            FROM      exp_pass_to_src
            left join
                      (
                               SELECT   feat.feat_id               AS feat_id,
                                        feat.feat_insrnc_sbtype_cd AS feat_insrnc_sbtype_cd,
                                        feat.feat_clasfcn_cd       AS feat_clasfcn_cd,
                                        feat.feat_desc             AS feat_desc,
                                        feat.feat_name             AS feat_name,
                                        feat.comn_feat_name        AS comn_feat_name,
                                        feat.feat_lvl_sbtype_cnt   AS feat_lvl_sbtype_cnt,
                                        feat.insrnc_cvge_type_cd   AS insrnc_cvge_type_cd,
                                        feat.insrnc_lob_type_cd    AS insrnc_lob_type_cd,
                                        feat.prcs_id               AS prcs_id,
                                        feat.feat_sbtype_cd        AS feat_sbtype_cd,
                                        feat.nk_src_key            AS nk_src_key
                               FROM     db_t_prod_core.feat qualify row_number () over (PARTITION BY nk_src_key,feat_sbtype_cd ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.feat_sbtype_cd = exp_pass_to_src.out_childsubtype
            AND       lkp.nk_src_key = exp_pass_to_src.childpatternid qualify rnk = 1 );
  -- Component exp_feat_lkp, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_feat_lkp AS
  (
             SELECT     lkp_feat_parent.feat_id                                                AS feat_id,
                        lkp_feat_child.feat_id                                                 AS rltd_feat_id,
                        exp_pass_to_src.out_rltnshp_type_cd                                    AS feat_rltnshp_type_cd,
                        to_date ( ''1900/01/01'' , ''YYYY/MM/DD'' )                                AS feat_rltd_strt_dt,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS feat_rltd_end_dt,
                        $prcs_id                                                               AS prcs_id,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        exp_pass_to_src.source_record_id
             FROM       exp_pass_to_src
             inner join lkp_feat_parent
             ON         exp_pass_to_src.source_record_id = lkp_feat_parent.source_record_id
             inner join lkp_feat_child
             ON         lkp_feat_parent.source_record_id = lkp_feat_child.source_record_id );
  -- Component exp_SRC_Fields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_fields AS
  (
         SELECT exp_feat_lkp.feat_id                                                   AS in_feat_id,
                exp_feat_lkp.rltd_feat_id                                              AS in_rltd_feat_id,
                exp_feat_lkp.feat_rltnshp_type_cd                                      AS in_feat_rltnshp_type_cd,
                exp_feat_lkp.feat_rltd_strt_dt                                         AS in_feat_rltd_strt_dt,
                exp_feat_lkp.feat_rltd_end_dt                                          AS in_feat_rltd_end_dt,
                exp_feat_lkp.prcs_id                                                   AS in_prcs_id,
                exp_feat_lkp.edw_strt_dttm                                             AS in_edw_strt_dttm,
                exp_feat_lkp.edw_end_dttm                                              AS in_edw_end_dttm,
                current_timestamp                                                      AS in_trans_strt_dttm,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS in_trans_end_dttm,
                exp_feat_lkp.source_record_id
         FROM   exp_feat_lkp );
  -- Component LKP_FEAT_RLTD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat_rltd AS
  (
            SELECT    lkp.feat_id,
                      lkp.rltd_feat_id,
                      lkp.feat_rltnshp_type_cd,
                      lkp.feat_rltd_strt_dt,
                      lkp.feat_rltd_end_dt,
                      exp_src_fields.in_feat_id              AS in_feat_id,
                      exp_src_fields.in_rltd_feat_id         AS in_rltd_feat_id,
                      exp_src_fields.in_feat_rltnshp_type_cd AS in_feat_rltnshp_type_cd,
                      exp_src_fields.source_record_id,
                      row_number() over(PARTITION BY exp_src_fields.source_record_id ORDER BY lkp.feat_id ASC,lkp.rltd_feat_id ASC,lkp.feat_rltnshp_type_cd ASC,lkp.feat_rltd_strt_dt ASC,lkp.feat_rltd_end_dt ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_src_fields
            left join
                      (
                               SELECT   feat_rltd.feat_rltd_strt_dt    AS feat_rltd_strt_dt,
                                        feat_rltd.feat_rltd_end_dt     AS feat_rltd_end_dt,
                                        feat_rltd.edw_end_dttm         AS edw_end_dttm,
                                        feat_rltd.feat_id              AS feat_id,
                                        feat_rltd.rltd_feat_id         AS rltd_feat_id,
                                        feat_rltd.feat_rltnshp_type_cd AS feat_rltnshp_type_cd
                               FROM     db_t_prod_core.feat_rltd qualify row_number() over(PARTITION BY feat_id,rltd_feat_id,feat_rltnshp_type_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.feat_id = exp_src_fields.in_feat_id
            AND       lkp.rltd_feat_id = exp_src_fields.in_rltd_feat_id
            AND       lkp.feat_rltnshp_type_cd = exp_src_fields.in_feat_rltnshp_type_cd qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_src_fields.in_feat_id              AS in_feat_id,
                        exp_src_fields.in_rltd_feat_id         AS in_rltd_feat_id,
                        exp_src_fields.in_feat_rltnshp_type_cd AS in_feat_rltnshp_type_cd,
                        exp_src_fields.in_feat_rltd_strt_dt    AS in_feat_rltd_strt_dt,
                        exp_src_fields.in_feat_rltd_end_dt     AS in_feat_rltd_end_dt,
                        exp_src_fields.in_prcs_id              AS in_prcs_id,
                        exp_src_fields.in_edw_strt_dttm        AS in_edw_strt_dttm,
                        exp_src_fields.in_edw_end_dttm         AS in_edw_end_dttm,
                        exp_src_fields.in_trans_strt_dttm      AS in_trans_strt_dttm,
                        exp_src_fields.in_trans_end_dttm       AS in_trans_end_dttm,
                        CASE
                                   WHEN lkp_feat_rltd.feat_id IS NULL THEN ''I''
                                   ELSE ''R''
                        END AS o_src_tgt,
                        exp_src_fields.source_record_id
             FROM       exp_src_fields
             inner join lkp_feat_rltd
             ON         exp_src_fields.source_record_id = lkp_feat_rltd.source_record_id );
  -- Component fltr_Insert_reject, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fltr_insert_reject AS
  (
         SELECT exp_cdc_check.in_feat_id              AS in_feat_id,
                exp_cdc_check.in_rltd_feat_id         AS in_rltd_feat_id,
                exp_cdc_check.in_feat_rltnshp_type_cd AS in_feat_rltnshp_type_cd,
                exp_cdc_check.in_feat_rltd_strt_dt    AS in_feat_rltd_strt_dt,
                exp_cdc_check.in_feat_rltd_end_dt     AS in_feat_rltd_end_dt,
                exp_cdc_check.in_prcs_id              AS in_prcs_id,
                exp_cdc_check.in_edw_strt_dttm        AS in_edw_strt_dttm,
                exp_cdc_check.in_edw_end_dttm         AS in_edw_end_dttm,
                exp_cdc_check.in_trans_strt_dttm      AS in_trans_strt_dttm,
                exp_cdc_check.in_trans_end_dttm       AS in_trans_end_dttm,
                exp_cdc_check.o_src_tgt               AS o_src_tgt,
                exp_cdc_check.source_record_id
         FROM   exp_cdc_check
         WHERE  exp_cdc_check.o_src_tgt = ''I'' );
  -- Component TGT_FEAT_RLTD_ins, Type TARGET
  INSERT INTO db_t_prod_core.feat_rltd
              (
                          feat_id,
                          rltd_feat_id,
                          feat_rltnshp_type_cd,
                          feat_rltd_strt_dt,
                          feat_rltd_end_dt,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT fltr_insert_reject.in_feat_id              AS feat_id,
         fltr_insert_reject.in_rltd_feat_id         AS rltd_feat_id,
         fltr_insert_reject.in_feat_rltnshp_type_cd AS feat_rltnshp_type_cd,
         fltr_insert_reject.in_feat_rltd_strt_dt    AS feat_rltd_strt_dt,
         fltr_insert_reject.in_feat_rltd_end_dt     AS feat_rltd_end_dt,
         fltr_insert_reject.in_prcs_id              AS prcs_id,
         fltr_insert_reject.in_edw_strt_dttm        AS edw_strt_dttm,
         fltr_insert_reject.in_edw_end_dttm         AS edw_end_dttm,
         fltr_insert_reject.in_trans_strt_dttm      AS trans_strt_dttm,
         fltr_insert_reject.in_trans_end_dttm       AS trans_end_dttm
  FROM   fltr_insert_reject;

END;
';