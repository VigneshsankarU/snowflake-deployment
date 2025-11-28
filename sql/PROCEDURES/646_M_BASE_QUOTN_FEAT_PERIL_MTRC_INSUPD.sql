-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_FEAT_PERIL_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       PRCS_ID STRING;

BEGIN

       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);

  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_insrnc_mtrc AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm=''derived''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_plcy_section_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PLCY_SECTN_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_rtg_peril AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm=''RTG_PERIL_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_agmtquotn_ast_ft_prl_mtr_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS jobnumber,
                $2  AS nk_branchnum,
                $3  AS feat_nksrckey,
                $4  AS earnings_as_of_dt,
                $5  AS inscrn_mtrc_type_cd,
                $6  AS peril_type,
                $7  AS cury_cd,
                $8  AS trans_strt_dttm,
                $9  AS amount,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    pc_job.jobnumber_stg                  AS jobnumber_stg,
                                                      pc_policyperiod.branchnumber_stg      AS branchnumber_stg,
                                                      pc_etlclausepattern.patternid_stg     AS patternid_stg,
                                                      pc_policyperiod.editeffectivedate_stg AS editeffectivedate_stg,
                                                      ''INSRNC_MTRC_TYPE16''                  AS inscrn_mtrc_type_cd,
                                                      pctl_periltype_alfa.typecode_stg      AS typecode_stg,
                                                      ''USD''                                 AS currency_code,
                                                      pc_policyperiod.updatetime_stg        AS transaction_start_date,
                                                      SUM(pcx_hotransaction_hoe.amount_stg) AS amt
                                            FROM      db_t_prod_stag.pc_policyperiod
                                            join      db_t_prod_stag.pc_job
                                            ON        pc_job.id_stg = pc_policyperiod.jobid_stg
                                            join      db_t_prod_stag.pcx_hotransaction_hoe
                                            ON        pcx_hotransaction_hoe.branchid_stg = pc_policyperiod.id_stg
                                            AND       (
                                                                pcx_hotransaction_hoe.expirationdate_stg IS NULL
                                                      OR        pcx_hotransaction_hoe.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                            join      db_t_prod_stag.pcx_homeownerscost_hoe
                                            ON        pcx_homeownerscost_hoe.id_stg = pcx_hotransaction_hoe.homeownerscost_stg
                                            join      db_t_prod_stag.pctl_periltype_alfa
                                            ON        pctl_periltype_alfa.id_stg = pcx_homeownerscost_hoe.periltype_alfa_stg
                                            join      db_t_prod_stag.pcx_homeownerslinecov_hoe
                                            ON        pcx_homeownerslinecov_hoe.id_stg = pcx_homeownerscost_hoe.homeownerslinecov_stg
                                            AND       (
                                                                pcx_homeownerslinecov_hoe.expirationdate_stg IS NULL
                                                      OR        pcx_homeownerslinecov_hoe.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                            left join db_t_prod_stag.pc_policyline
                                            ON        pc_policyline.id_stg = pcx_homeownerslinecov_hoe.holine_stg
                                            left join db_t_prod_stag.pc_etlclausepattern
                                            ON        pc_etlclausepattern.patternid_stg = pcx_homeownerslinecov_hoe.patterncode_stg
                                            WHERE     pc_policyperiod.updatetime_stg > (:start_dttm)
                                            AND       pc_policyperiod.updatetime_stg <= (:end_dttm )
                                            GROUP BY  jobnumber_stg,
                                                      branchnumber_stg,
                                                      patternid_stg,
                                                      editeffectivedate_stg,
                                                      inscrn_mtrc_type_cd,
                                                      pctl_periltype_alfa.typecode_stg,
                                                      currency_code,
                                                      transaction_start_date ) src ) );
  -- Component exp_pass_frm_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_src AS
  (
            SELECT    sq_pc_agmtquotn_ast_ft_prl_mtr_x.jobnumber         AS jobnumber,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.feat_nksrckey     AS feat_nksrckey,
                      ''QUOTE''                                            AS asset_cntrct_role_sbtype_cd,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.earnings_as_of_dt AS earnings_as_of_dt1,
                      NULL as section_type,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */
                      END AS section_type1,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC */
                                                              AS o_inscrn_mtrc_type_cd,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.amount AS amount,
                      CASE
                                WHEN sq_pc_agmtquotn_ast_ft_prl_mtr_x.trans_strt_dttm IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                ELSE sq_pc_agmtquotn_ast_ft_prl_mtr_x.trans_strt_dttm
                      END                                                                    AS trans_strt_dttm1,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm1,
                      CASE
                                WHEN lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_5.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */
                      END                                                AS in_rtg_peril_type_cd,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.earnings_as_of_dt AS earnings_as_of_dt,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.nk_branchnum      AS nk_branchnum,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.cury_cd           AS cury_cd,
                      sq_pc_agmtquotn_ast_ft_prl_mtr_x.source_record_id,
                      row_number() over (PARTITION BY sq_pc_agmtquotn_ast_ft_prl_mtr_x.source_record_id ORDER BY sq_pc_agmtquotn_ast_ft_prl_mtr_x.source_record_id) AS rnk1
            FROM      sq_pc_agmtquotn_ast_ft_prl_mtr_x
            left join lkp_teradata_etl_ref_xlat_plcy_section_type lkp_1
            ON        lkp_1.src_idntftn_val = section_type
            left join lkp_teradata_etl_ref_xlat_plcy_section_type lkp_2
            ON        lkp_2.src_idntftn_val = section_type
            left join lkp_teradata_etl_ref_xlat_insrnc_mtrc lkp_3
            ON        lkp_3.src_idntftn_val = sq_pc_agmtquotn_ast_ft_prl_mtr_x.inscrn_mtrc_type_cd
            left join lkp_teradata_etl_ref_xlat_rtg_peril lkp_4
            ON        lkp_4.src_idntftn_val = sq_pc_agmtquotn_ast_ft_prl_mtr_x.peril_type
            left join lkp_teradata_etl_ref_xlat_rtg_peril lkp_5
            ON        lkp_5.src_idntftn_val = sq_pc_agmtquotn_ast_ft_prl_mtr_x.peril_type qualify rnk1 = 1 );
  -- Component LKP_INSRNC_QUOTN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_insrnc_quotn AS
  (
            SELECT    lkp.quotn_id,
                      exp_pass_frm_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.quotn_id ASC) rnk2
            FROM      exp_pass_frm_src
            left join
                      (
                               SELECT   insrnc_quotn.quotn_id   AS quotn_id,
                                        insrnc_quotn.nk_job_nbr AS nk_job_nbr,
                                        insrnc_quotn.vers_nbr   AS vers_nbr
                               FROM     db_t_prod_core.insrnc_quotn qualify row_number() over(PARTITION BY insrnc_quotn.nk_job_nbr, insrnc_quotn.vers_nbr, insrnc_quotn.src_sys_cd ORDER BY insrnc_quotn.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_job_nbr = exp_pass_frm_src.jobnumber
            AND       lkp.vers_nbr = exp_pass_frm_src.nk_branchnum qualify rnk2 = 1 );
  -- Component LKP_FEAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat AS
  (
            SELECT    lkp.feat_id,
                      exp_pass_frm_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.feat_id DESC,lkp.feat_sbtype_cd DESC,lkp.nk_src_key DESC,lkp.feat_insrnc_sbtype_cd DESC,lkp.feat_clasfcn_cd DESC,lkp.feat_desc DESC,lkp.feat_name DESC,lkp.comn_feat_name DESC,lkp.feat_lvl_sbtype_cnt DESC,lkp.insrnc_cvge_type_cd DESC,lkp.insrnc_lob_type_cd DESC,lkp.prcs_id DESC) rnk3
            FROM      exp_pass_frm_src
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
            ON        
            -- lkp.feat_sbtype_cd = in_feat_sbtype_cd AND
            lkp.nk_src_key = exp_pass_frm_src.feat_nksrckey qualify rnk3 = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_CURY_CD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_cury_cd AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_frm_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk4
            FROM      exp_pass_frm_src
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''ISO_CURY_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm=''derived''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_frm_src.cury_cd qualify rnk4 = 1 );
  -- Component LKP_QUONT_FEAT_PERIL_MTRC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_quont_feat_peril_mtrc AS
  (
             SELECT     lkp.quotn_id,
                        lkp.feat_id,
                        lkp.quotn_feat_strt_dttm,
                        lkp.rtg_peril_type_cd,
                        lkp.insrnc_mtrc_type_cd,
                        lkp.qf_peril_mtrc_strt_dttm,
                        lkp.qf_peril_mtrc_amt,
                        lkp.edw_strt_dttm,
                        exp_pass_frm_src.in_rtg_peril_type_cd AS in_rtg_peril_type_cd,
                        exp_pass_frm_src.source_record_id,
                        row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.quotn_id ASC,lkp.feat_id ASC,lkp.quotn_feat_strt_dttm ASC,lkp.rtg_peril_type_cd ASC,lkp.insrnc_mtrc_type_cd ASC,lkp.qf_peril_mtrc_strt_dttm ASC,lkp.qf_peril_mtrc_amt ASC,lkp.edw_strt_dttm ASC) rnk5
             FROM       exp_pass_frm_src
             inner join lkp_insrnc_quotn
             ON         exp_pass_frm_src.source_record_id = lkp_insrnc_quotn.source_record_id
             inner join lkp_feat
             ON         lkp_insrnc_quotn.source_record_id = lkp_feat.source_record_id
             left join
                        (
                                 SELECT   quotn_feat_peril_mtrc.quotn_feat_strt_dttm       AS quotn_feat_strt_dttm,
                                          quotn_feat_peril_mtrc.quotn_feat_peril_strt_dttm AS quotn_feat_peril_strt_dttm,
                                          quotn_feat_peril_mtrc.qf_peril_mtrc_strt_dttm    AS qf_peril_mtrc_strt_dttm,
                                          quotn_feat_peril_mtrc.qf_peril_mtrc_amt          AS qf_peril_mtrc_amt,
                                          quotn_feat_peril_mtrc.edw_strt_dttm              AS edw_strt_dttm,
                                          quotn_feat_peril_mtrc.quotn_id                   AS quotn_id,
                                          quotn_feat_peril_mtrc.feat_id                    AS feat_id,
                                          quotn_feat_peril_mtrc.insrnc_mtrc_type_cd        AS insrnc_mtrc_type_cd,
                                          quotn_feat_peril_mtrc.rtg_peril_type_cd          AS rtg_peril_type_cd
                                 FROM     db_t_prod_core.quotn_feat_peril_mtrc qualify row_number() over(PARTITION BY insrnc_mtrc_type_cd,rtg_peril_type_cd,feat_id,quotn_id ORDER BY quotn_feat_peril_mtrc.edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.quotn_id = lkp_insrnc_quotn.quotn_id
             AND        lkp.feat_id = lkp_feat.feat_id
             AND        lkp.insrnc_mtrc_type_cd = exp_pass_frm_src.o_inscrn_mtrc_type_cd
             AND        lkp.rtg_peril_type_cd = exp_pass_frm_src.in_rtg_peril_type_cd qualify rnk5 = 1 );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
             SELECT     lkp_quont_feat_peril_mtrc.quotn_id                AS lkp_quotn_id,
                        lkp_quont_feat_peril_mtrc.qf_peril_mtrc_strt_dttm AS lkp_qaf_peril_mtrc_dttm,
                        lkp_quont_feat_peril_mtrc.edw_strt_dttm           AS lkp_edw_strt_dttm,
                        lkp_quont_feat_peril_mtrc.feat_id                 AS lkp_feat_id,
                        lkp_quont_feat_peril_mtrc.rtg_peril_type_cd       AS lkp_rtg_peril_type_cd,
                        lkp_quont_feat_peril_mtrc.insrnc_mtrc_type_cd     AS lkp_insrnc_mtrc_type_cd,
                        lkp_quont_feat_peril_mtrc.quotn_feat_strt_dttm    AS lkp_quotn_asset_feat_strt_dttm,
                        md5 ( lkp_quont_feat_peril_mtrc.qf_peril_mtrc_amt
                                   || to_char ( lkp_quont_feat_peril_mtrc.quotn_feat_strt_dttm ) ) AS lkp_checksum,
                        lkp_feat.feat_id                                                           AS in_feat_id,
                        lkp_insrnc_quotn.quotn_id                                                  AS quotn_id,
                        exp_pass_frm_src.o_inscrn_mtrc_type_cd                                     AS in_inscrn_mtrc_type_cd,
                        exp_pass_frm_src.in_rtg_peril_type_cd                                      AS in_rtg_peril_type_cd,
                        exp_pass_frm_src.asset_cntrct_role_sbtype_cd                               AS in_asset_cntrct_role_sbtype_cd,
                        exp_pass_frm_src.earnings_as_of_dt1                                        AS earnings_as_of_dt1,
                        exp_pass_frm_src.section_type1                                             AS section_type,
                        exp_pass_frm_src.earnings_as_of_dt                                         AS earnings_as_of_dt,
                        exp_pass_frm_src.amount                                                    AS amount,
                        md5 ( exp_pass_frm_src.amount
                                   || to_char ( exp_pass_frm_src.earnings_as_of_dt ) ) AS in_checksum,
                        exp_pass_frm_src.trans_strt_dttm1                              AS trans_strt_dttm,
                        exp_pass_frm_src.trans_end_dttm1                               AS trans_end_dttm,
                        CASE
                                   WHEN lkp_checksum IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN lkp_checksum <> in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END )
                        END                                                                    AS ins_upd_flag,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS agmt_asset_strt_dttm,
                        :prcs_id                                                               AS prcs_id,
                        lkp_teradata_etl_ref_xlat_cury_cd.tgt_idntftn_val                      AS tgt_idntftn_val,
                        exp_pass_frm_src.source_record_id
             FROM       exp_pass_frm_src
             inner join lkp_insrnc_quotn
             ON         exp_pass_frm_src.source_record_id = lkp_insrnc_quotn.source_record_id
             inner join lkp_feat
             ON         lkp_insrnc_quotn.source_record_id = lkp_feat.source_record_id
             inner join lkp_teradata_etl_ref_xlat_cury_cd
             ON         lkp_feat.source_record_id = lkp_teradata_etl_ref_xlat_cury_cd.source_record_id
             inner join lkp_quont_feat_peril_mtrc
             ON         lkp_teradata_etl_ref_xlat_cury_cd.source_record_id = lkp_quont_feat_peril_mtrc.source_record_id );
  -- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
  CREATE or replace TEMPORARY table RTRTRANS_INSERT as
  SELECT exp_ins_upd.lkp_quotn_id                   AS lkp_quotn_id,
         exp_ins_upd.lkp_feat_id                    AS lkp_feat_id,
         exp_ins_upd.lkp_rtg_peril_type_cd          AS lkp_rtg_peril_type_cd,
         exp_ins_upd.lkp_insrnc_mtrc_type_cd        AS lkp_insrnc_mtrc_type_cd,
         exp_ins_upd.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_ins_upd.in_feat_id                     AS in_feat_id,
         exp_ins_upd.quotn_id                       AS quotn_id,
         exp_ins_upd.in_inscrn_mtrc_type_cd         AS in_inscrn_mtrc_type_cd,
         exp_ins_upd.in_rtg_peril_type_cd           AS in_rtg_peril_type_cd,
         exp_ins_upd.in_asset_cntrct_role_sbtype_cd AS in_asset_cntrct_role_sbtype_cd,
         exp_ins_upd.earnings_as_of_dt1             AS earnings_as_of_dt4,
         exp_ins_upd.section_type                   AS section_type,
         exp_ins_upd.earnings_as_of_dt              AS earnings_as_of_dt,
         exp_ins_upd.amount                         AS amount,
         exp_ins_upd.trans_strt_dttm                AS trans_strt_dttm,
         exp_ins_upd.trans_end_dttm                 AS trans_end_dttm,
         exp_ins_upd.ins_upd_flag                   AS ins_upd_flag,
         exp_ins_upd.edw_strt_dttm                  AS edw_strt_dttm,
         exp_ins_upd.edw_end_dttm                   AS edw_end_dttm,
         exp_ins_upd.prcs_id                        AS prcs_id,
         exp_ins_upd.lkp_qaf_peril_mtrc_dttm        AS lkp_qaf_peril_mtrc_dttm,
         exp_ins_upd.lkp_quotn_asset_feat_strt_dttm AS lkp_quotn_asset_feat_strt_dttm,
         exp_ins_upd.agmt_asset_strt_dttm           AS agmt_asset_strt_dttm,
         exp_ins_upd.tgt_idntftn_val                AS tgt_idntftn_val,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flag = ''I''
  AND    exp_ins_upd.quotn_id IS NOT NULL
  AND    exp_ins_upd.in_feat_id IS NOT NULL
;
  
  -- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
  create or replace TEMPORARY table RTRTRANS_UPDATE as 
  SELECT exp_ins_upd.lkp_quotn_id                   AS lkp_quotn_id,
         exp_ins_upd.lkp_feat_id                    AS lkp_feat_id,
         exp_ins_upd.lkp_rtg_peril_type_cd          AS lkp_rtg_peril_type_cd,
         exp_ins_upd.lkp_insrnc_mtrc_type_cd        AS lkp_insrnc_mtrc_type_cd,
         exp_ins_upd.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_ins_upd.in_feat_id                     AS in_feat_id,
         exp_ins_upd.quotn_id                       AS quotn_id,
         exp_ins_upd.in_inscrn_mtrc_type_cd         AS in_inscrn_mtrc_type_cd,
         exp_ins_upd.in_rtg_peril_type_cd           AS in_rtg_peril_type_cd,
         exp_ins_upd.in_asset_cntrct_role_sbtype_cd AS in_asset_cntrct_role_sbtype_cd,
         exp_ins_upd.earnings_as_of_dt1             AS earnings_as_of_dt4,
         exp_ins_upd.section_type                   AS section_type,
         exp_ins_upd.earnings_as_of_dt              AS earnings_as_of_dt,
         exp_ins_upd.amount                         AS amount,
         exp_ins_upd.trans_strt_dttm                AS trans_strt_dttm,
         exp_ins_upd.trans_end_dttm                 AS trans_end_dttm,
         exp_ins_upd.ins_upd_flag                   AS ins_upd_flag,
         exp_ins_upd.edw_strt_dttm                  AS edw_strt_dttm,
         exp_ins_upd.edw_end_dttm                   AS edw_end_dttm,
         exp_ins_upd.prcs_id                        AS prcs_id,
         exp_ins_upd.lkp_qaf_peril_mtrc_dttm        AS lkp_qaf_peril_mtrc_dttm,
         exp_ins_upd.lkp_quotn_asset_feat_strt_dttm AS lkp_quotn_asset_feat_strt_dttm,
         exp_ins_upd.agmt_asset_strt_dttm           AS agmt_asset_strt_dttm,
         exp_ins_upd.tgt_idntftn_val                AS tgt_idntftn_val,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flag = ''U''
  AND    exp_ins_upd.quotn_id IS NOT NULL
  AND    exp_ins_upd.in_feat_id IS NOT NULL;
  
  -- Component exp_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_upd AS
  (
         SELECT rtrtrans_update.lkp_quotn_id                               AS lkp_quotn_id,
                rtrtrans_update.lkp_feat_id                                AS lkp_feat_id3,
                rtrtrans_update.lkp_rtg_peril_type_cd                      AS lkp_rtg_peril_type_cd3,
                rtrtrans_update.lkp_insrnc_mtrc_type_cd                    AS lkp_insrnc_mtrc_type_cd3,
                rtrtrans_update.lkp_edw_strt_dttm                          AS lkp_edw_strt_dttm3,
                dateadd ( second,-1, rtrtrans_update.edw_strt_dttm ) AS edw_end_dttm,
                dateadd ( second, -1, rtrtrans_update.trans_strt_dttm ) AS trans_end_dttm,
                rtrtrans_update.lkp_qaf_peril_mtrc_dttm                      AS lkp_qaf_peril_mtrc_dttm,
                rtrtrans_update.lkp_quotn_asset_feat_strt_dttm               AS lkp_quotn_asset_feat_strt_dttm,
                rtrtrans_update.source_record_id
         FROM   rtrtrans_update );
  -- Component QUOTN_FEAT_PERIL_MTRC_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.quotn_feat_peril_mtrc
              (
                          quotn_id,
                          feat_id,
                          quotn_feat_role_cd,
                          quotn_feat_strt_dttm,
                          rtg_peril_type_cd,
                          quotn_feat_peril_strt_dttm,
                          insrnc_mtrc_type_cd,
                          qf_peril_mtrc_strt_dttm,
                          qf_peril_mtrc_end_dttm,
                          qf_peril_mtrc_amt,
                          cury_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT rtrtrans_insert.quotn_id                       AS quotn_id,
         rtrtrans_insert.in_feat_id                     AS feat_id,
         rtrtrans_insert.in_asset_cntrct_role_sbtype_cd AS quotn_feat_role_cd,
         rtrtrans_insert.earnings_as_of_dt4             AS quotn_feat_strt_dttm,
         rtrtrans_insert.in_rtg_peril_type_cd           AS rtg_peril_type_cd,
         rtrtrans_insert.agmt_asset_strt_dttm           AS quotn_feat_peril_strt_dttm,
         rtrtrans_insert.in_inscrn_mtrc_type_cd         AS insrnc_mtrc_type_cd,
         rtrtrans_insert.agmt_asset_strt_dttm           AS qf_peril_mtrc_strt_dttm,
         rtrtrans_insert.edw_end_dttm                   AS qf_peril_mtrc_end_dttm,
         rtrtrans_insert.amount                         AS qf_peril_mtrc_amt,
         rtrtrans_insert.tgt_idntftn_val                AS cury_cd,
         rtrtrans_insert.prcs_id                        AS prcs_id,
         rtrtrans_insert.edw_strt_dttm                  AS edw_strt_dttm,
         rtrtrans_insert.edw_end_dttm                   AS edw_end_dttm,
         rtrtrans_insert.trans_strt_dttm                AS trans_strt_dttm,
         rtrtrans_insert.trans_end_dttm                 AS trans_end_dttm
  FROM   rtrtrans_insert;
  
  -- Component exp_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins AS
  (
         SELECT rtrtrans_update.quotn_id                       AS quotn_id,
                rtrtrans_update.in_feat_id                     AS feat_id,
                rtrtrans_update.in_asset_cntrct_role_sbtype_cd AS asset_cntrct_role_sbtype_cd,
                rtrtrans_update.earnings_as_of_dt4             AS earnings_as_of_dt,
                rtrtrans_update.in_rtg_peril_type_cd           AS rtg_peril_type_cd,
                rtrtrans_update.earnings_as_of_dt4             AS earnings_as_of_dt1,
                rtrtrans_update.earnings_as_of_dt4             AS earnings_as_of_dt2,
                rtrtrans_update.section_type                   AS agmt_sectn_cd,
                rtrtrans_update.in_inscrn_mtrc_type_cd         AS insrnc_mtrc_type_cd,
                rtrtrans_update.earnings_as_of_dt              AS earnings_as_of_dt3,
                rtrtrans_update.amount                         AS agmt_asset_feat_peril_amt,
                rtrtrans_update.prcs_id                        AS prcs_id,
                rtrtrans_update.edw_strt_dttm                  AS edw_strt_dttm,
                rtrtrans_update.edw_end_dttm                   AS edw_end_dttm,
                rtrtrans_update.trans_strt_dttm                AS trans_strt_dttm,
                rtrtrans_update.trans_end_dttm                 AS trans_end_dttm,
                rtrtrans_update.agmt_asset_strt_dttm           AS agmt_asset_strt_dttm3,
                rtrtrans_update.tgt_idntftn_val                AS tgt_idntftn_val3,
                rtrtrans_update.source_record_id
         FROM   rtrtrans_update );
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_ins.quotn_id                    AS quotn_id,
                exp_ins.feat_id                     AS feat_id,
                exp_ins.asset_cntrct_role_sbtype_cd AS asset_cntrct_role_sbtype_cd,
                exp_ins.earnings_as_of_dt           AS earnings_as_of_dt,
                exp_ins.rtg_peril_type_cd           AS rtg_peril_type_cd,
                exp_ins.earnings_as_of_dt1          AS earnings_as_of_dt1,
                exp_ins.earnings_as_of_dt2          AS earnings_as_of_dt2,
                exp_ins.agmt_sectn_cd               AS agmt_sectn_cd,
                exp_ins.insrnc_mtrc_type_cd         AS insrnc_mtrc_type_cd,
                exp_ins.earnings_as_of_dt3          AS earnings_as_of_dt3,
                exp_ins.agmt_asset_feat_peril_amt   AS agmt_asset_feat_peril_amt,
                exp_ins.prcs_id                     AS prcs_id,
                exp_ins.edw_strt_dttm               AS edw_strt_dttm,
                exp_ins.edw_end_dttm                AS edw_end_dttm,
                exp_ins.trans_strt_dttm             AS trans_strt_dttm,
                exp_ins.trans_end_dttm              AS trans_end_dttm,
                exp_ins.agmt_asset_strt_dttm3       AS agmt_asset_strt_dttm3,
                exp_ins.tgt_idntftn_val3            AS tgt_idntftn_val3,
                0                                   AS update_strategy_action,
				source_record_id
         FROM   exp_ins 
         WHERE trans_end_dttm IS NOT NULL
  );
  -- Component QUOTN_FEAT_PERIL_MTRC_ins, Type TARGET
  INSERT INTO db_t_prod_core.quotn_feat_peril_mtrc
              (
                          quotn_id,
                          feat_id,
                          quotn_feat_role_cd,
                          quotn_feat_strt_dttm,
                          rtg_peril_type_cd,
                          quotn_feat_peril_strt_dttm,
                          insrnc_mtrc_type_cd,
                          qf_peril_mtrc_strt_dttm,
                          qf_peril_mtrc_end_dttm,
                          qf_peril_mtrc_amt,
                          cury_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_ins.quotn_id                    AS quotn_id,
         upd_ins.feat_id                     AS feat_id,
         upd_ins.asset_cntrct_role_sbtype_cd AS quotn_feat_role_cd,
         upd_ins.earnings_as_of_dt           AS quotn_feat_strt_dttm,
         upd_ins.rtg_peril_type_cd           AS rtg_peril_type_cd,
         upd_ins.agmt_asset_strt_dttm3       AS quotn_feat_peril_strt_dttm,
         upd_ins.insrnc_mtrc_type_cd         AS insrnc_mtrc_type_cd,
         upd_ins.agmt_asset_strt_dttm3       AS qf_peril_mtrc_strt_dttm,
         upd_ins.edw_end_dttm                AS qf_peril_mtrc_end_dttm,
         upd_ins.agmt_asset_feat_peril_amt   AS qf_peril_mtrc_amt,
         upd_ins.tgt_idntftn_val3            AS cury_cd,
         upd_ins.prcs_id                     AS prcs_id,
         upd_ins.edw_strt_dttm               AS edw_strt_dttm,
         upd_ins.edw_end_dttm                AS edw_end_dttm,
         upd_ins.trans_strt_dttm             AS trans_strt_dttm,
         upd_ins.trans_end_dttm              AS trans_end_dttm
  FROM   upd_ins;
  
  -- Component update_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE update_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_upd.lkp_quotn_id                   AS lkp_quotn_id,
                exp_upd.lkp_feat_id3                   AS lkp_feat_id3,
                exp_upd.lkp_rtg_peril_type_cd3         AS lkp_rtg_peril_type_cd3,
                exp_upd.lkp_insrnc_mtrc_type_cd3       AS lkp_insrnc_mtrc_type_cd3,
                exp_upd.lkp_edw_strt_dttm3             AS lkp_edw_strt_dttm3,
                exp_upd.edw_end_dttm                   AS edw_end_dttm,
                exp_upd.trans_end_dttm                 AS trans_end_dttm,
                exp_upd.lkp_qaf_peril_mtrc_dttm        AS lkp_qaf_peril_mtrc_dttm,
                exp_upd.lkp_quotn_asset_feat_strt_dttm AS lkp_quotn_asset_feat_strt_dttm,
                1                                      AS update_strategy_action,
				source_record_id
         FROM   exp_upd
         WHERE exp_upd.trans_end_dttm IS NOT NULL );
  -- Component QUOTN_FEAT_PERIL_MTRC_upd, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.quotn_feat_peril_mtrc
  USING        update_upd
  ON (
                            update_strategy_action = 1
               AND          quotn_feat_peril_mtrc.quotn_id = update_upd.lkp_quotn_id
               AND          quotn_feat_peril_mtrc.feat_id = update_upd.lkp_feat_id3
               AND          quotn_feat_peril_mtrc.rtg_peril_type_cd = update_upd.lkp_rtg_peril_type_cd3
               AND          quotn_feat_peril_mtrc.insrnc_mtrc_type_cd = update_upd.lkp_insrnc_mtrc_type_cd3
               AND          quotn_feat_peril_mtrc.edw_strt_dttm = update_upd.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = update_upd.edw_end_dttm,
         trans_end_dttm = update_upd.trans_end_dttm ;

END;
';