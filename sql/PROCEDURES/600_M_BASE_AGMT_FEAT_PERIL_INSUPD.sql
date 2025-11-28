-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_FEAT_PERIL_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  IN_FEAT_SBTYPE_CD STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  IN_FEAT_SBTYPE_CD := public.func_get_scoped_param(:run_id, ''in_feat_sbtype_cd'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL, Type Prerequisite Lookup Object
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL AS
  (
         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
         FROM   db_t_prod_core.TERADATA_ETL_REF_XLAT
         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''RTG_PERIL_TYPE''
         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' );
  -- Component SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS PublicID,
                $2 AS PatternID,
                $3 AS AGMT_FEAT_STRT_DTTM,
                $4 AS RTG_PERIL_TYPE_CD,
                $5 AS AGMT_FEAT_PERIL_STRT_DTTM,
                $6 AS AGMT_FEAT_PERIL_END_DTTM,
                $7 AS TRANS_STRT_DTTM,
                $8 AS TRANS_END_DTTM,
                $9 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pc_policyperiod.publicid_stg                                                     AS publicID,
                                                                  pc_etlclausepattern.PatternID_stg                                                AS PatternID,
                                                                  pc_policyperiod.EditEffectiveDate_stg                                            AS AGMT_FEAT_STRT_DTTM,
                                                                  pctl_periltype_alfa.TypeCode_stg                                                 AS RTG_PERIL_TYPECODE_CD,
                                                                  to_timestamp_ntz(''1900-01-01 00:00:00.00000'' , ''YYYY-MM-DDBHH:MI:SS.FF6'') AS AGMT_FEAT_PERIL_STRT_DTTM,
                                                                  to_timestamp_ntz(''9999-12-31 23:59:59.99999'',''YYYY-MM-DDBHH:MI:SS.FF6'') AS AGMT_FEAT_PERIL_END_DTTM,
                                                                  pc_policyperiod.updatetime_stg                                                   AS TRANS_STRT_DTTM,
                                                                  CAST(NULL AS TIMESTAMP)                                                          AS TRANS_END_DTTM
                                                  FROM            DB_T_PROD_STAG.pc_policyperiod
                                                                  /* -Added as part of EIM-41200 */
                                                  INNER JOIN      DB_T_PROD_STAG.pc_policy
                                                  ON              pc_policy.ID_stg=pc_policyperiod.PolicyID_stg
                                                  JOIN            DB_T_PROD_STAG.pcx_hotransaction_hoe
                                                  ON              pcx_hotransaction_hoe.BranchID_stg = pc_policyperiod.id_stg
                                                  AND             (
                                                                                  pcx_hotransaction_hoe.ExpirationDate_stg IS NULL
                                                                  OR              pcx_hotransaction_hoe.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg)
                                                  JOIN            DB_T_PROD_STAG.pcx_homeownerscost_hoe
                                                  ON              pcx_homeownerscost_hoe.id_stg = pcx_hotransaction_hoe.HomeownersCost_stg
                                                  JOIN            DB_T_PROD_STAG.pctl_PerilType_alfa
                                                  ON              pctl_PerilType_alfa.id_stg = pcx_homeownerscost_hoe.PerilType_alfa_stg
                                                  JOIN            DB_T_PROD_STAG.pcx_homeownerslinecov_hoe
                                                  ON              pcx_homeownerslinecov_hoe.id_stg = pcx_homeownerscost_hoe.HomeownersLineCov_stg
                                                  AND             (
                                                                                  pcx_homeownerslinecov_hoe.ExpirationDate_stg IS NULL
                                                                  OR              pcx_homeownerslinecov_hoe.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg)
                                                  LEFT JOIN       DB_T_PROD_STAG.pc_policyline
                                                  ON              pc_policyline.id_stg = pcx_homeownerslinecov_hoe.HOLine_stg
                                                  LEFT JOIN       DB_T_PROD_STAG.pc_etlclausepattern
                                                  ON              pc_etlclausepattern.PatternID_stg = pcx_homeownerslinecov_hoe.PatternCode_stg
                                                  JOIN            DB_T_PROD_STAG.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                  WHERE           pctl_policyperiodstatus.TypeCode_stg = ''Bound''
                                                  AND             pcx_homeownerscost_hoe.HomeownersLineCov_stg IS NOT NULL
                                                  AND             pc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                  AND             pc_policyperiod.UpdateTime_stg <= (:end_dttm) ) SRC ) );
  -- Component exp_pass_frm_src, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_frm_src AS
  (
            SELECT    SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.PublicID            AS PublicID,
                      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.PatternID           AS Feat_NKsrckey,
                      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.AGMT_FEAT_STRT_DTTM AS AGMT_FEAT_STRT_DTTM,
                      CASE
                                WHEN LKP_1.TGT_IDNTFTN_VAL
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */
                                          IS NULL THEN ''UNK''
                                ELSE LKP_2.TGT_IDNTFTN_VAL
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */
                      END                                                        AS out_RTG_PERIL_TYPE_CD,
                      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
                      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM,
                      CASE
                                WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM
                      END AS out_TRANS_STRT_DTTM,
                      CASE
                                WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM
                      END   AS out_TRANS_END_DTTM,
                      ''PPV'' AS AGMT_TYPE_CD,
                      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id,
                      row_number() over (PARTITION BY SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id ORDER BY SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id) AS RNK
            FROM      SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL LKP_1
            ON        LKP_1.SRC_IDNTFTN_VAL = SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.RTG_PERIL_TYPE_CD
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL LKP_2
            ON        LKP_2.SRC_IDNTFTN_VAL = SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.RTG_PERIL_TYPE_CD 
			QUALIFY RNK = 1 );
  -- Component LKP_DIR_AGMT, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_DIR_AGMT AS
  (
            SELECT    LKP.AGMT_ID,
                      LKP.AGMT_TYPE_CD,
                      exp_pass_frm_src.source_record_id,
                      ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.NK_SRC_KEY ASC,LKP.TERM_NUM ASC,LKP.AGMT_TYPE_CD ASC,LKP.SRC_SYS_CD ASC,LKP.LOAD_DTTM ASC) RNK
            FROM      exp_pass_frm_src
            LEFT JOIN
                      (
                             SELECT AGMT_ID,
                                    NK_SRC_KEY,
                                    TERM_NUM,
                                    AGMT_TYPE_CD,
                                    SRC_SYS_CD,
                                    LOAD_DTTM
                             FROM   db_t_prod_core.DIR_AGMT
                             WHERE  AGMT_TYPE_CD=''PPV'' ) LKP
            ON        LKP.NK_SRC_KEY = exp_pass_frm_src.PublicID
            AND       LKP.AGMT_TYPE_CD = exp_pass_frm_src.AGMT_TYPE_CD 
			QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.NK_SRC_KEY ASC,LKP.TERM_NUM ASC,LKP.AGMT_TYPE_CD ASC,LKP.SRC_SYS_CD ASC,LKP.LOAD_DTTM ASC) = 1 );
  -- Component LKP_FEAT, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_FEAT AS
  (
            SELECT    LKP.FEAT_ID,
                      exp_pass_frm_src.source_record_id,
                      ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.FEAT_ID DESC,LKP.FEAT_SBTYPE_CD DESC,LKP.NK_SRC_KEY DESC,LKP.FEAT_INSRNC_SBTYPE_CD DESC,LKP.FEAT_CLASFCN_CD DESC,LKP.FEAT_DESC DESC,LKP.FEAT_NAME DESC,LKP.COMN_FEAT_NAME DESC,LKP.FEAT_LVL_SBTYPE_CNT DESC,LKP.INSRNC_CVGE_TYPE_CD DESC,LKP.INSRNC_LOB_TYPE_CD DESC,LKP.PRCS_ID DESC) RNK
            FROM      exp_pass_frm_src
            LEFT JOIN
                      (
                               SELECT   FEAT.FEAT_ID               AS FEAT_ID,
                                        FEAT.FEAT_INSRNC_SBTYPE_CD AS FEAT_INSRNC_SBTYPE_CD,
                                        FEAT.FEAT_CLASFCN_CD       AS FEAT_CLASFCN_CD,
                                        FEAT.FEAT_DESC             AS FEAT_DESC,
                                        FEAT.FEAT_NAME             AS FEAT_NAME,
                                        FEAT.COMN_FEAT_NAME        AS COMN_FEAT_NAME,
                                        FEAT.FEAT_LVL_SBTYPE_CNT   AS FEAT_LVL_SBTYPE_CNT,
                                        FEAT.INSRNC_CVGE_TYPE_CD   AS INSRNC_CVGE_TYPE_CD,
                                        FEAT.INSRNC_LOB_TYPE_CD    AS INSRNC_LOB_TYPE_CD,
                                        FEAT.PRCS_ID               AS PRCS_ID,
                                        FEAT.FEAT_SBTYPE_CD        AS FEAT_SBTYPE_CD,
                                        FEAT.NK_SRC_KEY            AS NK_SRC_KEY
                               FROM     db_t_prod_core.FEAT 
							   QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD ORDER BY edw_end_dttm DESC)=1 
							   ) LKP
            ON        LKP.FEAT_SBTYPE_CD = :in_FEAT_SBTYPE_CD
            AND       LKP.NK_SRC_KEY = exp_pass_frm_src.Feat_NKsrckey 
			QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.FEAT_ID DESC,LKP.FEAT_SBTYPE_CD DESC,LKP.NK_SRC_KEY DESC,LKP.FEAT_INSRNC_SBTYPE_CD DESC,LKP.FEAT_CLASFCN_CD DESC,LKP.FEAT_DESC DESC,LKP.FEAT_NAME DESC,LKP.COMN_FEAT_NAME DESC,LKP.FEAT_LVL_SBTYPE_CNT DESC,LKP.INSRNC_CVGE_TYPE_CD DESC,LKP.INSRNC_LOB_TYPE_CD DESC,LKP.PRCS_ID DESC) = 1 );
  -- Component LKP_AGMT_FEAT_PERIL, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_AGMT_FEAT_PERIL AS
  (
             SELECT     LKP.AGMT_ID,
                        LKP.FEAT_ID,
                        LKP.AGMT_FEAT_STRT_DTTM,
                        LKP.RTG_PERIL_TYPE_CD,
                        LKP.EDW_STRT_DTTM,
                        exp_pass_frm_src.source_record_id,
                        ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.FEAT_ID ASC,LKP.AGMT_FEAT_STRT_DTTM ASC,LKP.RTG_PERIL_TYPE_CD ASC,LKP.EDW_STRT_DTTM ASC) RNK
             FROM       exp_pass_frm_src
             INNER JOIN LKP_DIR_AGMT
             ON         exp_pass_frm_src.source_record_id = LKP_DIR_AGMT.source_record_id
             INNER JOIN LKP_FEAT
             ON         LKP_DIR_AGMT.source_record_id = LKP_FEAT.source_record_id
             LEFT JOIN
                        (
                               SELECT AGMT_ID,
                                      FEAT_ID,
                                      AGMT_FEAT_STRT_DTTM,
                                      RTG_PERIL_TYPE_CD,
                                      EDW_STRT_DTTM
                               FROM   db_t_prod_core.AGMT_FEAT_PERIL ) LKP
             ON         LKP.FEAT_ID = LKP_FEAT.FEAT_ID
             AND        LKP.AGMT_ID = LKP_DIR_AGMT.AGMT_ID
             AND        LKP.RTG_PERIL_TYPE_CD = exp_pass_frm_src.out_RTG_PERIL_TYPE_CD 
			 QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.FEAT_ID ASC,LKP.AGMT_FEAT_STRT_DTTM ASC,LKP.RTG_PERIL_TYPE_CD ASC,LKP.EDW_STRT_DTTM ASC) = 1 );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_ins_upd AS
  (
             SELECT     LKP_AGMT_FEAT_PERIL.AGMT_ID                                 AS lkp_AGMT_ID,
                        LKP_AGMT_FEAT_PERIL.FEAT_ID                                 AS lkp_FEAT_ID,
                        LKP_AGMT_FEAT_PERIL.AGMT_FEAT_STRT_DTTM                     AS lkp_AGMT_FEAT_STRT_DTTM,
                        LKP_AGMT_FEAT_PERIL.RTG_PERIL_TYPE_CD                       AS lkp_RTG_PERIL_TYPE_CD,
                        LKP_AGMT_FEAT_PERIL.EDW_STRT_DTTM                           AS lkp_EDW_STRT_DTTM,
                        MD5 ( TO_CHAR ( LKP_AGMT_FEAT_PERIL.AGMT_FEAT_STRT_DTTM ) ) AS lkp_checksum,
                        LKP_FEAT.FEAT_ID                                            AS in_FEAT_ID,
                        LKP_DIR_AGMT.AGMT_ID                                        AS in_AGMT_ID,
                        exp_pass_frm_src.out_RTG_PERIL_TYPE_CD                      AS in_RTG_PERIL_TYPE_CD,
                        exp_pass_frm_src.AGMT_FEAT_STRT_DTTM                        AS in_AGMT_FEAT_STRT_DTTM,
                        MD5 ( TO_CHAR ( exp_pass_frm_src.AGMT_FEAT_STRT_DTTM ) )    AS in_checksum,
                        exp_pass_frm_src.out_TRANS_STRT_DTTM                        AS TRANS_STRT_DTTM,
                        exp_pass_frm_src.out_TRANS_END_DTTM                         AS TRANS_END_DTTM,
                        CASE
                                   WHEN LKP_AGMT_FEAT_PERIL.AGMT_ID IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN lkp_checksum <> in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END )
                        END                                                                   AS ins_upd_flag,
                        CURRENT_TIMESTAMP                                                     AS EDW_STRT_DTTM,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS EDW_END_DTTM,
                        :PRCS_ID                                                              AS PRCS_ID,
                        CASE
                                   WHEN LKP_DIR_AGMT.AGMT_TYPE_CD IS NULL THEN ''UNK''
                                   ELSE LKP_DIR_AGMT.AGMT_TYPE_CD
                        END                                        AS AGMT_FEAT_ROLE_CD_out,
                        exp_pass_frm_src.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
                        exp_pass_frm_src.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM,
                        exp_pass_frm_src.source_record_id
             FROM       exp_pass_frm_src
             INNER JOIN LKP_DIR_AGMT
             ON         exp_pass_frm_src.source_record_id = LKP_DIR_AGMT.source_record_id
             INNER JOIN LKP_FEAT
             ON         LKP_DIR_AGMT.source_record_id = LKP_FEAT.source_record_id
             INNER JOIN LKP_AGMT_FEAT_PERIL
             ON         LKP_FEAT.source_record_id = LKP_AGMT_FEAT_PERIL.source_record_id );
  
  -- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table RTRTRANS_INSERT as
    SELECT exp_ins_upd.lkp_AGMT_ID               AS lkp_AGMT_ID,
         exp_ins_upd.lkp_FEAT_ID               AS lkp_FEAT_ID,
         exp_ins_upd.lkp_AGMT_FEAT_STRT_DTTM   AS lkp_AGMT_FEAT_STRT_DTTM,
         exp_ins_upd.lkp_RTG_PERIL_TYPE_CD     AS lkp_RTG_PERIL_TYPE_CD,
         exp_ins_upd.lkp_EDW_STRT_DTTM         AS lkp_EDW_STRT_DTTM,
         exp_ins_upd.in_FEAT_ID                AS in_FEAT_ID,
         exp_ins_upd.in_AGMT_ID                AS in_AGMT_ID,
         exp_ins_upd.in_RTG_PERIL_TYPE_CD      AS in_RTG_PERIL_TYPE_CD,
         exp_ins_upd.in_AGMT_FEAT_STRT_DTTM    AS in_AGMT_FEAT_STRT_DTTM,
         exp_ins_upd.TRANS_STRT_DTTM           AS TRANS_STRT_DTTM,
         exp_ins_upd.TRANS_END_DTTM            AS TRANS_END_DTTM,
         exp_ins_upd.EDW_STRT_DTTM             AS EDW_STRT_DTTM,
         exp_ins_upd.EDW_END_DTTM              AS EDW_END_DTTM,
         exp_ins_upd.PRCS_ID                   AS PRCS_ID,
         exp_ins_upd.AGMT_FEAT_ROLE_CD_out     AS AGMT_FEAT_ROLE_CD,
         exp_ins_upd.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
         exp_ins_upd.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM,
         exp_ins_upd.ins_upd_flag              AS ins_upd_flag,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flag = ''I''
  AND    exp_ins_upd.in_FEAT_ID IS NOT NULL;
  
  -- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table RTRTRANS_UPDATE as
  SELECT exp_ins_upd.lkp_AGMT_ID               AS lkp_AGMT_ID,
         exp_ins_upd.lkp_FEAT_ID               AS lkp_FEAT_ID,
         exp_ins_upd.lkp_AGMT_FEAT_STRT_DTTM   AS lkp_AGMT_FEAT_STRT_DTTM,
         exp_ins_upd.lkp_RTG_PERIL_TYPE_CD     AS lkp_RTG_PERIL_TYPE_CD,
         exp_ins_upd.lkp_EDW_STRT_DTTM         AS lkp_EDW_STRT_DTTM,
         exp_ins_upd.in_FEAT_ID                AS in_FEAT_ID,
         exp_ins_upd.in_AGMT_ID                AS in_AGMT_ID,
         exp_ins_upd.in_RTG_PERIL_TYPE_CD      AS in_RTG_PERIL_TYPE_CD,
         exp_ins_upd.in_AGMT_FEAT_STRT_DTTM    AS in_AGMT_FEAT_STRT_DTTM,
         exp_ins_upd.TRANS_STRT_DTTM           AS TRANS_STRT_DTTM,
         exp_ins_upd.TRANS_END_DTTM            AS TRANS_END_DTTM,
         exp_ins_upd.EDW_STRT_DTTM             AS EDW_STRT_DTTM,
         exp_ins_upd.EDW_END_DTTM              AS EDW_END_DTTM,
         exp_ins_upd.PRCS_ID                   AS PRCS_ID,
         exp_ins_upd.AGMT_FEAT_ROLE_CD_out     AS AGMT_FEAT_ROLE_CD,
         exp_ins_upd.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
         exp_ins_upd.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM,
         exp_ins_upd.ins_upd_flag              AS ins_upd_flag,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flag = ''U''
  AND    exp_ins_upd.in_FEAT_ID IS NOT NULL;
  
  -- Component exp_upd, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_upd AS
  (
         SELECT RTRTRANS_UPDATE.lkp_AGMT_ID                         AS lkp_AGMT_ID3,
                RTRTRANS_UPDATE.lkp_FEAT_ID                         AS lkp_FEAT_ID3,
                RTRTRANS_UPDATE.lkp_RTG_PERIL_TYPE_CD               AS lkp_RTG_PERIL_TYPE_CD3,
                RTRTRANS_UPDATE.lkp_EDW_STRT_DTTM                   AS lkp_EDW_STRT_DTTM3,
                RTRTRANS_UPDATE.lkp_AGMT_FEAT_STRT_DTTM             AS lkp_AGMT_FEAT_STRT_DTTM,
                DATEADD(''second'', - 1, RTRTRANS_UPDATE.EDW_STRT_DTTM)   AS EDW_END_DTTM,
                DATEADD(''second'', - 1, RTRTRANS_UPDATE.TRANS_STRT_DTTM) AS TRANS_END_DTTM,
                RTRTRANS_UPDATE.source_record_id
         FROM   RTRTRANS_UPDATE );
  -- Component AGMT_FEAT_PERIL_ins_new, Type TARGET
  INSERT INTO DB_T_PROD_CORE.AGMT_FEAT_PERIL
              (
                          AGMT_ID,
                          FEAT_ID,
                          AGMT_FEAT_ROLE_CD,
                          AGMT_FEAT_STRT_DTTM,
                          RTG_PERIL_TYPE_CD,
                          AGMT_FEAT_PERIL_STRT_DTTM,
                          AGMT_FEAT_PERIL_END_DTTM,
                          PRCS_ID,
                          EDW_STRT_DTTM,
                          EDW_END_DTTM,
                          TRANS_STRT_DTTM,
                          TRANS_END_DTTM
              )
  SELECT RTRTRANS_INSERT.in_AGMT_ID                AS AGMT_ID,
         RTRTRANS_INSERT.in_FEAT_ID                AS FEAT_ID,
         RTRTRANS_INSERT.AGMT_FEAT_ROLE_CD         AS AGMT_FEAT_ROLE_CD,
         RTRTRANS_INSERT.in_AGMT_FEAT_STRT_DTTM    AS AGMT_FEAT_STRT_DTTM,
         RTRTRANS_INSERT.in_RTG_PERIL_TYPE_CD      AS RTG_PERIL_TYPE_CD,
         RTRTRANS_INSERT.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
         RTRTRANS_INSERT.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM,
         RTRTRANS_INSERT.PRCS_ID                   AS PRCS_ID,
         RTRTRANS_INSERT.EDW_STRT_DTTM             AS EDW_STRT_DTTM,
         RTRTRANS_INSERT.EDW_END_DTTM              AS EDW_END_DTTM,
         RTRTRANS_INSERT.TRANS_STRT_DTTM           AS TRANS_STRT_DTTM,
         RTRTRANS_INSERT.TRANS_END_DTTM            AS TRANS_END_DTTM
  FROM   RTRTRANS_INSERT;
  
  -- Component exp_ins, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_ins AS
  (
         SELECT RTRTRANS_UPDATE.in_AGMT_ID                AS AGMT_ID,
                RTRTRANS_UPDATE.in_FEAT_ID                AS FEAT_ID,
                RTRTRANS_UPDATE.in_RTG_PERIL_TYPE_CD      AS RTG_PERIL_TYPE_CD,
                RTRTRANS_UPDATE.PRCS_ID                   AS PRCS_ID,
                RTRTRANS_UPDATE.EDW_STRT_DTTM             AS EDW_STRT_DTTM,
                RTRTRANS_UPDATE.EDW_END_DTTM              AS EDW_END_DTTM,
                RTRTRANS_UPDATE.TRANS_STRT_DTTM           AS TRANS_STRT_DTTM,
                RTRTRANS_UPDATE.TRANS_END_DTTM            AS TRANS_END_DTTM,
                RTRTRANS_UPDATE.in_AGMT_FEAT_STRT_DTTM    AS AGMT_FEAT_STRT_DTTM3,
                RTRTRANS_UPDATE.AGMT_FEAT_ROLE_CD         AS AGMT_FEAT_ROLE_CD3,
                RTRTRANS_UPDATE.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM3,
                RTRTRANS_UPDATE.AGMT_FEAT_PERIL_END_DTTM  AS AGMT_FEAT_PERIL_END_DTTM3,
                RTRTRANS_UPDATE.source_record_id
         FROM   RTRTRANS_UPDATE );
  -- Component update_upd, Type UPDATE
  CREATE
  OR
  REPLACE TEMPORARY TABLE update_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_upd.lkp_AGMT_ID3            AS lkp_AGMT_ID3,
                exp_upd.lkp_FEAT_ID3            AS lkp_FEAT_ID3,
                exp_upd.lkp_RTG_PERIL_TYPE_CD3  AS lkp_RTG_PERIL_TYPE_CD3,
                exp_upd.lkp_AGMT_FEAT_STRT_DTTM AS lkp_AGMT_FEAT_STRT_DTTM,
                exp_upd.lkp_EDW_STRT_DTTM3      AS lkp_EDW_STRT_DTTM3,
                exp_upd.EDW_END_DTTM            AS EDW_END_DTTM,
                exp_upd.TRANS_END_DTTM          AS TRANS_END_DTTM,
                1                               AS UPDATE_STRATEGY_ACTION
         FROM   exp_upd );
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  REPLACE TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_ins.AGMT_ID                    AS AGMT_ID,
                exp_ins.FEAT_ID                    AS FEAT_ID,
                exp_ins.RTG_PERIL_TYPE_CD          AS RTG_PERIL_TYPE_CD,
                exp_ins.PRCS_ID                    AS PRCS_ID,
                exp_ins.EDW_STRT_DTTM              AS EDW_STRT_DTTM,
                exp_ins.EDW_END_DTTM               AS EDW_END_DTTM,
                exp_ins.TRANS_STRT_DTTM            AS TRANS_STRT_DTTM,
                exp_ins.TRANS_END_DTTM             AS TRANS_END_DTTM,
                exp_ins.AGMT_FEAT_STRT_DTTM3       AS AGMT_FEAT_STRT_DTTM,
                exp_ins.AGMT_FEAT_ROLE_CD3         AS AGMT_FEAT_ROLE_CD,
                exp_ins.AGMT_FEAT_PERIL_STRT_DTTM3 AS AGMT_FEAT_PERIL_STRT_DTTM,
                exp_ins.AGMT_FEAT_PERIL_END_DTTM3  AS AGMT_FEAT_PERIL_END_DTTM,
                0                                  AS UPDATE_STRATEGY_ACTION
         FROM   exp_ins );
  -- Component AGMT_FEAT_PERIL_upd_ins, Type TARGET
  INSERT INTO DB_T_PROD_CORE.AGMT_FEAT_PERIL
              (
                          AGMT_ID,
                          FEAT_ID,
                          AGMT_FEAT_ROLE_CD,
                          AGMT_FEAT_STRT_DTTM,
                          RTG_PERIL_TYPE_CD,
                          AGMT_FEAT_PERIL_STRT_DTTM,
                          PRCS_ID,
                          EDW_STRT_DTTM,
                          EDW_END_DTTM,
                          TRANS_STRT_DTTM
              )
  SELECT upd_ins.AGMT_ID                   AS AGMT_ID,
         upd_ins.FEAT_ID                   AS FEAT_ID,
         upd_ins.AGMT_FEAT_ROLE_CD         AS AGMT_FEAT_ROLE_CD,
         upd_ins.AGMT_FEAT_STRT_DTTM       AS AGMT_FEAT_STRT_DTTM,
         upd_ins.RTG_PERIL_TYPE_CD         AS RTG_PERIL_TYPE_CD,
         upd_ins.AGMT_FEAT_PERIL_STRT_DTTM AS AGMT_FEAT_PERIL_STRT_DTTM,
         upd_ins.PRCS_ID                   AS PRCS_ID,
         upd_ins.EDW_STRT_DTTM             AS EDW_STRT_DTTM,
         upd_ins.EDW_END_DTTM              AS EDW_END_DTTM,
         upd_ins.TRANS_STRT_DTTM           AS TRANS_STRT_DTTM
  FROM   upd_ins;
  
  -- Component AGMT_FEAT_PERIL_upd, Type TARGET
  /* Perform Updates */
  MERGE
  INTO         DB_T_PROD_CORE.AGMT_FEAT_PERIL
  USING        update_upd
  ON (
                            UPDATE_STRATEGY_ACTION = 1
               AND          AGMT_FEAT_PERIL.AGMT_ID = update_upd.lkp_AGMT_ID3
               AND          AGMT_FEAT_PERIL.FEAT_ID = update_upd.lkp_FEAT_ID3
               AND          AGMT_FEAT_PERIL.RTG_PERIL_TYPE_CD = update_upd.lkp_RTG_PERIL_TYPE_CD3
               AND          AGMT_FEAT_PERIL.EDW_STRT_DTTM = update_upd.EDW_END_DTTM)
  WHEN MATCHED THEN
  UPDATE
  SET    EDW_END_DTTM = update_upd.EDW_END_DTTM,
         TRANS_END_DTTM = update_upd.TRANS_END_DTTM ;

END;
';