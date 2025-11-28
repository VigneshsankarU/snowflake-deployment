-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_LOCTR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  P_AGMT_TYPE_CD_POLICY_VERSION := public.func_get_scoped_param(:run_id, ''p_agmt_type_cd_policy_version'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_ROLE, Type Prerequisite Lookup Object
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_AGMT_ROLE AS
  (
         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
         FROM   db_t_prod_core.TERADATA_ETL_REF_XLAT
         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''AGMT_LOCTR_ROLE''
         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' );
  -- Component SQ_agmt_loctr, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_agmt_loctr AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS STATE,
                $2 AS COUNTRY,
                $3 AS PUBLICID,
                $4 AS AGMT_LOCTR_ROLE_TYPE_CD,
                $5 AS EFF_DT,
                $6 AS END_DT,
                $7 AS UPDATETIME,
                $8 AS CODE,
                $9 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT agmt_loctr.state,
                                                                  agmt_loctr.COUNTRY,
                                                                  agmt_loctr.PUBLICID,
                                                                  agmt_loctr.AGMT_LOCTR_ROLE_TYPE_CD,
                                                                  agmt_loctr.EFF_DT,
                                                                  agmt_loctr.END_DT,
                                                                  agmt_loctr.UPDATETIME,
                                                                  agmt_loctr.CD_TYPE
                                                  FROM            (
                                                                             SELECT     cctl_jurisdiction.TYPECODE_stg                AS state,
                                                                                        ''US''                                          AS Country,
                                                                                        CAST(cc_policy.id_stg AS VARCHAR(64))         AS publicid,
                                                                                        ''AGMT_LOCTR_ROLE2''                            AS AGMT_LOCTR_ROLE_TYPE_CD,
                                                                                        CAST(CAST(''1900/01/01'' AS DATE) AS TIMESTAMP) AS eff_dt,
                                                                                        CAST(CAST(''9999/12/31'' AS DATE) AS TIMESTAMP) AS end_dt,
                                                                                        cc_policy.UpdateTime_stg                      AS UpdateTime,
                                                                                        CAST(''SRC_SYS6'' AS VARCHAR(60))               AS CD_TYPE
                                                                             FROM       DB_T_PROD_STAG.cc_policy
                                                                             INNER JOIN DB_T_PROD_STAG.cctl_jurisdiction
                                                                             ON         cctl_jurisdiction.id_stg=cc_policy.basesate_alfa_stg
                                                                             WHERE      (
                                                                                                   cc_policy.Verified_stg = 0
                                                                                        AND        COALESCE(cc_policy.legacypolind_alfa_stg,0)<>1)
                                                                             AND        cc_policy.UpdateTime_stg > (:Start_dttm)
                                                                             AND        cc_policy.UpdateTime_stg <= (:End_dttm)
                                                                             UNION
                                                                             SELECT     cctl_jurisdiction.TYPECODE_stg                AS state,
                                                                                        ''US''                                          AS Country,
                                                                                        CAST(cc_policy.id_stg AS VARCHAR(64))         AS publicid,
                                                                                        ''AGMT_LOCTR_ROLE7''                            AS AGMT_LOCTR_ROLE_TYPE_CD,
                                                                                        CAST(CAST(''1900/01/01'' AS DATE) AS TIMESTAMP) AS eff_dt,
                                                                                        CAST(CAST(''9999/12/31'' AS DATE) AS TIMESTAMP) AS end_dt,
                                                                                        cc_policy.UpdateTime_stg                      AS UpdateTime,
                                                                                        CAST(''SRC_SYS6'' AS VARCHAR(60))               AS CD_TYPE
                                                                             FROM       DB_T_PROD_STAG.cc_policy
                                                                             INNER JOIN DB_T_PROD_STAG.cctl_jurisdiction
                                                                             ON         cctl_jurisdiction.id_stg=cc_policy.basesate_alfa_stg
                                                                             WHERE      COALESCE(cc_policy.legacypolind_alfa_stg,0)=1
                                                                             AND        cc_policy.UpdateTime_stg > (:Start_dttm)
                                                                             AND        cc_policy.UpdateTime_stg <= (:End_dttm)
                                                                             UNION
                                                                             /* ---Agreement written in DB_T_SHRD_PROD.state */
                                                                             SELECT DISTINCT pctl_jurisdiction.TYPECODE_stg        AS state,
                                                                                             ''US''                                  AS Country,
                                                                                             pc_policyperiod.Publicid_stg          AS PUBLICID,
                                                                                             ''AGMT_LOCTR_ROLE6''                    AS AGMT_LOCTR_ROLE_TYPE_CD,
                                                                                             pc_policyperiod.EditeffectiveDate_stg AS eff_dt,
                                                                                             CAST(NULL AS TIMESTAMP)               AS end_dt,
                                                                                             pc_policyperiod.UpdateTime_stg        AS UPDATETIME,
                                                                                             CAST(''AGRM''AS VARCHAR(60))            AS CD_TYPE
                                                                             FROM            DB_T_PROD_STAG.pc_policyperiod
                                                                             JOIN            DB_T_PROD_STAG.pctl_jurisdiction
                                                                             ON              pc_policyperiod.BaseState_stg = pctl_jurisdiction.id_stg
                                                                             JOIN            DB_T_PROD_STAG.pc_policycontactrole
                                                                             ON              pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg
                                                                             JOIN            DB_T_PROD_STAG.pctl_policycontactrole
                                                                             ON              pc_policycontactrole.subtype_stg = pctl_policycontactrole.id_stg
                                                                             INNER JOIN      DB_T_PROD_STAG.pc_contact
                                                                             ON              pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg
                                                                             INNER JOIN      DB_T_PROD_STAG.pc_job
                                                                             ON              pc_job.id_stg=pc_policyperiod.JobID_stg
                                                                             INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                                             ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                                             INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus
                                                                             ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg
                                                                             AND             pc_policyperiod.PolicyNumber_stg IS NOT NULL
                                                                             WHERE           pctl_policyperiodstatus.TYPECODE_stg=''Bound''
                                                                             AND             pc_policyperiod.updatetime_stg >(:Start_dttm)
                                                                             AND             pc_policyperiod.updatetime_stg <= (:End_dttm) ) agmt_loctr ) SRC ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    SQ_agmt_loctr.PUBLICID AS PUBLICID,
                      SQ_agmt_loctr.STATE    AS STATE,
                      SQ_agmt_loctr.COUNTRY  AS COUNTRY,
                      LKP_1.TGT_IDNTFTN_VAL
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_ROLE */
                                                     AS out_AGMT_LOCTR_ROLE_TYPE_CD,
                      SQ_agmt_loctr.EFF_DT           AS EFF_DT,
                      :p_agmt_type_cd_policy_version AS out_AGMT_TYPE_CD,
                      ''SRC_SYS4''                     AS v_AGMT_SRC_CD,
                      SQ_agmt_loctr.CODE             AS out_AGMT_SRC_CD,
                      SQ_agmt_loctr.UPDATETIME       AS UPDATETIME,
                      SQ_agmt_loctr.source_record_id,
                      row_number() over (PARTITION BY SQ_agmt_loctr.source_record_id ORDER BY SQ_agmt_loctr.source_record_id) AS RNK
            FROM      SQ_agmt_loctr
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_ROLE LKP_1
            ON        LKP_1.SRC_IDNTFTN_VAL = SQ_agmt_loctr.AGMT_LOCTR_ROLE_TYPE_CD QUALIFY RNK = 1 );
  -- Component LKP_AGMT_POL, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_AGMT_POL AS
  (
            SELECT    LKP.AGMT_ID,
                      exp_pass_from_source.source_record_id,
                      ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.HOST_AGMT_NUM ASC,LKP.AGMT_NAME ASC,LKP.AGMT_OPN_DTTM ASC,LKP.AGMT_CLS_DTTM ASC,LKP.AGMT_PLND_EXPN_DTTM ASC,LKP.AGMT_SIGND_DTTM ASC,LKP.AGMT_TYPE_CD ASC,LKP.AGMT_LEGLY_BINDG_IND ASC,LKP.AGMT_SRC_CD ASC,LKP.AGMT_CUR_STS_CD ASC,LKP.AGMT_CUR_STS_RSN_CD ASC,LKP.AGMT_OBTND_CD ASC,LKP.AGMT_SBTYPE_CD ASC,LKP.AGMT_PRCSG_DTTM ASC,LKP.ALT_AGMT_NAME ASC,LKP.ASSET_LIABTY_CD ASC,LKP.BAL_SHET_CD ASC,LKP.STMT_CYCL_CD ASC,LKP.STMT_ML_TYPE_CD ASC,LKP.PRPOSL_ID ASC,LKP.AGMT_OBJTV_TYPE_CD ASC,LKP.FINCL_AGMT_SBTYPE_CD ASC,LKP.MKT_RISK_TYPE_CD ASC,LKP.ORIGNL_MATURTY_DT ASC,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD ASC,LKP.BNK_TRD_BK_CD ASC,LKP.PRCG_METH_SBTYPE_CD ASC,LKP.FINCL_AGMT_TYPE_CD ASC,LKP.DY_CNT_BSS_CD ASC,LKP.FRST_PREM_DUE_DT ASC,LKP.INSRNC_AGMT_SBTYPE_CD ASC,LKP.INSRNC_AGMT_TYPE_CD ASC,LKP.NTWK_SRVC_AGMT_TYPE_CD ASC,LKP.FRMLTY_TYPE_CD ASC,LKP.CNTRCT_TERM_NUM ASC,LKP.RATE_RPRCG_CYCL_MTH_NUM ASC,LKP.CMPND_INT_CYCL_MTH_NUM ASC,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM ASC,LKP.PREV_MDTERM_INT_PMT_DT ASC,LKP.NXT_MDTERM_INT_PMT_DT ASC,LKP.PREV_INT_RATE_RVSD_DT ASC,LKP.NXT_INT_RATE_RVSD_DT ASC,LKP.PREV_REF_DT_INT_RATE ASC,LKP.NXT_REF_DT_FOR_INT_RATE ASC,LKP.MDTERM_CNCLTN_DT ASC,LKP.STK_FLOW_CLAS_IN_MTH_IND ASC,LKP.STK_FLOW_CLAS_IN_TERM_IND ASC,LKP.LGCY_DSCNT_IND ASC,LKP.AGMT_IDNTFTN_CD ASC,LKP.TRMTN_TYPE_CD ASC,LKP.INT_PMT_METH_CD ASC,LKP.LBR_AGMT_DESC ASC,LKP.GUARTD_IMPRSNS_CNT ASC,LKP.COST_PER_IMPRSN_AMT ASC,LKP.GUARTD_CLKTHRU_CNT ASC,LKP.COST_PER_CLKTHRU_AMT ASC,LKP.BUSN_PRTY_ID ASC,LKP.PMT_PLN_TYPE_CD ASC,LKP.INVC_STREM_TYPE_CD ASC,LKP.MODL_CRTN_DTTM ASC,LKP.CNTNUS_SRVC_DTTM ASC,LKP.BILG_METH_TYPE_CD ASC,LKP.SRC_SYS_CD ASC,LKP.AGMT_EFF_DTTM ASC,LKP.MODL_EFF_DTTM ASC,LKP.PRCS_ID ASC,LKP.MODL_ACTL_END_DTTM ASC,LKP.TIER_TYPE_CD ASC,LKP.EDW_STRT_DTTM ASC,LKP.EDW_END_DTTM ASC,LKP.VFYD_PLCY_IND ASC,LKP.SRC_OF_BUSN_CD ASC,LKP.NK_SRC_KEY ASC,LKP.OVRD_COMS_TYPE_CD ASC,LKP.LGCY_PLCY_IND ASC,LKP.TRANS_STRT_DTTM ASC) RNK
            FROM      exp_pass_from_source
            LEFT JOIN
                      (
                               SELECT   AGMT.AGMT_ID                     AS AGMT_ID,
                                        AGMT.HOST_AGMT_NUM               AS HOST_AGMT_NUM,
                                        AGMT.AGMT_NAME                   AS AGMT_NAME,
                                        AGMT.AGMT_OPN_DTTM               AS AGMT_OPN_DTTM,
                                        AGMT.AGMT_CLS_DTTM               AS AGMT_CLS_DTTM,
                                        AGMT.AGMT_PLND_EXPN_DTTM         AS AGMT_PLND_EXPN_DTTM,
                                        AGMT.AGMT_SIGND_DTTM             AS AGMT_SIGND_DTTM,
                                        AGMT.AGMT_LEGLY_BINDG_IND        AS AGMT_LEGLY_BINDG_IND,
                                        AGMT.AGMT_SRC_CD                 AS AGMT_SRC_CD,
                                        AGMT.AGMT_CUR_STS_CD             AS AGMT_CUR_STS_CD,
                                        AGMT.AGMT_CUR_STS_RSN_CD         AS AGMT_CUR_STS_RSN_CD,
                                        AGMT.AGMT_OBTND_CD               AS AGMT_OBTND_CD,
                                        AGMT.AGMT_SBTYPE_CD              AS AGMT_SBTYPE_CD,
                                        AGMT.AGMT_PRCSG_DTTM             AS AGMT_PRCSG_DTTM,
                                        AGMT.ALT_AGMT_NAME               AS ALT_AGMT_NAME,
                                        AGMT.ASSET_LIABTY_CD             AS ASSET_LIABTY_CD,
                                        AGMT.BAL_SHET_CD                 AS BAL_SHET_CD,
                                        AGMT.STMT_CYCL_CD                AS STMT_CYCL_CD,
                                        AGMT.STMT_ML_TYPE_CD             AS STMT_ML_TYPE_CD,
                                        AGMT.PRPOSL_ID                   AS PRPOSL_ID,
                                        AGMT.AGMT_OBJTV_TYPE_CD          AS AGMT_OBJTV_TYPE_CD,
                                        AGMT.FINCL_AGMT_SBTYPE_CD        AS FINCL_AGMT_SBTYPE_CD,
                                        AGMT.MKT_RISK_TYPE_CD            AS MKT_RISK_TYPE_CD,
                                        AGMT.ORIGNL_MATURTY_DT           AS ORIGNL_MATURTY_DT,
                                        AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD  AS RISK_EXPSR_MTGNT_SBTYPE_CD,
                                        AGMT.BNK_TRD_BK_CD               AS BNK_TRD_BK_CD,
                                        AGMT.PRCG_METH_SBTYPE_CD         AS PRCG_METH_SBTYPE_CD,
                                        AGMT.FINCL_AGMT_TYPE_CD          AS FINCL_AGMT_TYPE_CD,
                                        AGMT.DY_CNT_BSS_CD               AS DY_CNT_BSS_CD,
                                        AGMT.FRST_PREM_DUE_DT            AS FRST_PREM_DUE_DT,
                                        AGMT.INSRNC_AGMT_SBTYPE_CD       AS INSRNC_AGMT_SBTYPE_CD,
                                        AGMT.INSRNC_AGMT_TYPE_CD         AS INSRNC_AGMT_TYPE_CD,
                                        AGMT.NTWK_SRVC_AGMT_TYPE_CD      AS NTWK_SRVC_AGMT_TYPE_CD,
                                        AGMT.FRMLTY_TYPE_CD              AS FRMLTY_TYPE_CD,
                                        AGMT.CNTRCT_TERM_NUM             AS CNTRCT_TERM_NUM,
                                        AGMT.RATE_RPRCG_CYCL_MTH_NUM     AS RATE_RPRCG_CYCL_MTH_NUM,
                                        AGMT.CMPND_INT_CYCL_MTH_NUM      AS CMPND_INT_CYCL_MTH_NUM,
                                        AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM AS MDTERM_INT_PMT_CYCL_MTH_NUM,
                                        AGMT.PREV_MDTERM_INT_PMT_DT      AS PREV_MDTERM_INT_PMT_DT,
                                        AGMT.NXT_MDTERM_INT_PMT_DT       AS NXT_MDTERM_INT_PMT_DT,
                                        AGMT.PREV_INT_RATE_RVSD_DT       AS PREV_INT_RATE_RVSD_DT,
                                        AGMT.NXT_INT_RATE_RVSD_DT        AS NXT_INT_RATE_RVSD_DT,
                                        AGMT.PREV_REF_DT_INT_RATE        AS PREV_REF_DT_INT_RATE,
                                        AGMT.NXT_REF_DT_FOR_INT_RATE     AS NXT_REF_DT_FOR_INT_RATE,
                                        AGMT.MDTERM_CNCLTN_DT            AS MDTERM_CNCLTN_DT,
                                        AGMT.STK_FLOW_CLAS_IN_MTH_IND    AS STK_FLOW_CLAS_IN_MTH_IND,
                                        AGMT.STK_FLOW_CLAS_IN_TERM_IND   AS STK_FLOW_CLAS_IN_TERM_IND,
                                        AGMT.LGCY_DSCNT_IND              AS LGCY_DSCNT_IND,
                                        AGMT.AGMT_IDNTFTN_CD             AS AGMT_IDNTFTN_CD,
                                        AGMT.TRMTN_TYPE_CD               AS TRMTN_TYPE_CD,
                                        AGMT.INT_PMT_METH_CD             AS INT_PMT_METH_CD,
                                        AGMT.LBR_AGMT_DESC               AS LBR_AGMT_DESC,
                                        AGMT.GUARTD_IMPRSNS_CNT          AS GUARTD_IMPRSNS_CNT,
                                        AGMT.COST_PER_IMPRSN_AMT         AS COST_PER_IMPRSN_AMT,
                                        AGMT.GUARTD_CLKTHRU_CNT          AS GUARTD_CLKTHRU_CNT,
                                        AGMT.COST_PER_CLKTHRU_AMT        AS COST_PER_CLKTHRU_AMT,
                                        AGMT.BUSN_PRTY_ID                AS BUSN_PRTY_ID,
                                        AGMT.PMT_PLN_TYPE_CD             AS PMT_PLN_TYPE_CD,
                                        AGMT.INVC_STREM_TYPE_CD          AS INVC_STREM_TYPE_CD,
                                        AGMT.MODL_CRTN_DTTM              AS MODL_CRTN_DTTM,
                                        AGMT.CNTNUS_SRVC_DTTM            AS CNTNUS_SRVC_DTTM,
                                        AGMT.BILG_METH_TYPE_CD           AS BILG_METH_TYPE_CD,
                                        AGMT.SRC_SYS_CD                  AS SRC_SYS_CD,
                                        AGMT.AGMT_EFF_DTTM               AS AGMT_EFF_DTTM,
                                        AGMT.MODL_EFF_DTTM               AS MODL_EFF_DTTM,
                                        AGMT.PRCS_ID                     AS PRCS_ID,
                                        AGMT.MODL_ACTL_END_DTTM          AS MODL_ACTL_END_DTTM,
                                        AGMT.TIER_TYPE_CD                AS TIER_TYPE_CD,
                                        AGMT.EDW_STRT_DTTM               AS EDW_STRT_DTTM,
                                        AGMT.EDW_END_DTTM                AS EDW_END_DTTM,
                                        AGMT.VFYD_PLCY_IND               AS VFYD_PLCY_IND,
                                        AGMT.SRC_OF_BUSN_CD              AS SRC_OF_BUSN_CD,
                                        AGMT.OVRD_COMS_TYPE_CD           AS OVRD_COMS_TYPE_CD,
                                        AGMT.LGCY_PLCY_IND               AS LGCY_PLCY_IND,
                                        AGMT.TRANS_STRT_DTTM             AS TRANS_STRT_DTTM,
                                        AGMT.NK_SRC_KEY                  AS NK_SRC_KEY,
                                        AGMT.AGMT_TYPE_CD                AS AGMT_TYPE_CD
                               FROM     db_t_prod_core.AGMT 
                               QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM ORDER BY AGMT.EDW_END_DTTM DESC) = 1 ) LKP
            ON        LKP.NK_SRC_KEY = exp_pass_from_source.PUBLICID
            AND       LKP.AGMT_TYPE_CD = exp_pass_from_source.out_AGMT_TYPE_CD 
            QUALIFY RNK = 1 );
  -- Component LKP_CTRY, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_CTRY AS
  (
            SELECT    LKP.CTRY_ID,
                      exp_pass_from_source.source_record_id,
                      ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.CTRY_ID ASC,LKP.CAL_TYPE_CD ASC,LKP.ISO_3166_CTRY_NUM ASC,LKP.GEOGRCL_AREA_SHRT_NAME ASC,LKP.GEOGRCL_AREA_NAME ASC,LKP.GEOGRCL_AREA_DESC ASC,LKP.CURY_CD ASC,LKP.GEOGRCL_AREA_STRT_DTTM ASC,LKP.GEOGRCL_AREA_END_DTTM ASC,LKP.PRCS_ID ASC) RNK
            FROM      exp_pass_from_source
            LEFT JOIN
                      (
                             SELECT CTRY_ID,
                                    CAL_TYPE_CD,
                                    ISO_3166_CTRY_NUM,
                                    GEOGRCL_AREA_SHRT_NAME,
                                    GEOGRCL_AREA_NAME,
                                    GEOGRCL_AREA_DESC,
                                    CURY_CD,
                                    GEOGRCL_AREA_STRT_DTTM,
                                    GEOGRCL_AREA_END_DTTM,
                                    PRCS_ID
                             FROM   db_t_prod_core.CTRY ) LKP
            ON        LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source.COUNTRY 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.CTRY_ID ASC,LKP.CAL_TYPE_CD ASC,LKP.ISO_3166_CTRY_NUM ASC,LKP.GEOGRCL_AREA_SHRT_NAME ASC,LKP.GEOGRCL_AREA_NAME ASC,LKP.GEOGRCL_AREA_DESC ASC,LKP.CURY_CD ASC,LKP.GEOGRCL_AREA_STRT_DTTM ASC,LKP.GEOGRCL_AREA_END_DTTM ASC,LKP.PRCS_ID ASC) = 1 );
  -- Component LKP_TERR, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_TERR AS
  (
             SELECT     LKP.TERR_ID,
                        exp_pass_from_source.source_record_id,
                        ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.TERR_ID ASC,LKP.TERR_TYPE_CD ASC,LKP.CTRY_ID ASC,LKP.RGN_ID ASC,LKP.GEOGRCL_AREA_SHRT_NAME ASC,LKP.GEOGRCL_AREA_NAME ASC,LKP.GEOGRCL_AREA_DESC ASC,LKP.CURY_CD ASC,LKP.GEOGRCL_AREA_STRT_DTTM ASC,LKP.GEOGRCL_AREA_END_DTTM ASC,LKP.PRCS_ID ASC) RNK
             FROM       exp_pass_from_source
             INNER JOIN LKP_CTRY
             ON         exp_pass_from_source.source_record_id = LKP_CTRY.source_record_id
             LEFT JOIN
                        (
                               SELECT TERR_ID,
                                      TERR_TYPE_CD,
                                      CTRY_ID,
                                      RGN_ID,
                                      GEOGRCL_AREA_SHRT_NAME,
                                      GEOGRCL_AREA_NAME,
                                      GEOGRCL_AREA_DESC,
                                      CURY_CD,
                                      GEOGRCL_AREA_STRT_DTTM,
                                      GEOGRCL_AREA_END_DTTM,
                                      PRCS_ID
                               FROM   db_t_prod_core.TERR ) LKP
             ON         LKP.CTRY_ID = LKP_CTRY.CTRY_ID
             AND        LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source.STATE 
             QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.TERR_ID ASC,LKP.TERR_TYPE_CD ASC,LKP.CTRY_ID ASC,LKP.RGN_ID ASC,LKP.GEOGRCL_AREA_SHRT_NAME ASC,LKP.GEOGRCL_AREA_NAME ASC,LKP.GEOGRCL_AREA_DESC ASC,LKP.CURY_CD ASC,LKP.GEOGRCL_AREA_STRT_DTTM ASC,LKP.GEOGRCL_AREA_END_DTTM ASC,LKP.PRCS_ID ASC) = 1 );
  -- Component exp_trans, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_trans AS
  (
             SELECT     LKP_AGMT_POL.AGMT_ID                                                  AS AGMT_ID,
                        LKP_TERR.TERR_ID                                                      AS TERR_ID,
                        exp_pass_from_source.out_AGMT_LOCTR_ROLE_TYPE_CD                      AS out_AGMT_LOCTR_ROLE_TYPE_CD,
                        exp_pass_from_source.EFF_DT                                           AS EFF_DT,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS END_DT1,
                        :PRCS_ID                                                              AS out_PRCS_ID,
                        CURRENT_TIMESTAMP                                                     AS EDW_STRT_DTTM,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS EDW_END_DTTM,
                        exp_pass_from_source.UPDATETIME                                       AS UPDATETIME,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             INNER JOIN LKP_AGMT_POL
             ON         exp_pass_from_source.source_record_id = LKP_AGMT_POL.source_record_id
             INNER JOIN LKP_TERR
             ON         LKP_AGMT_POL.source_record_id = LKP_TERR.source_record_id );
  -- Component LKP_AGMT_LOCTR1, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_AGMT_LOCTR1 AS
  (
             SELECT     LKP.AGMT_ID,
                        LKP.AGMT_LOCTR_ROLE_TYPE_CD,
                        LKP.AGMT_LOCTR_STRT_DTTM,
                        LKP.LOC_ID,
                        LKP.AGMT_LOCTR_END_DTTM,
                        LKP.EDW_STRT_DTTM,
                        LKP_AGMT_POL.source_record_id,
                        ROW_NUMBER() OVER(PARTITION BY LKP_AGMT_POL.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.AGMT_LOCTR_ROLE_TYPE_CD ASC,LKP.AGMT_LOCTR_STRT_DTTM ASC,LKP.LOC_ID ASC,LKP.AGMT_LOCTR_END_DTTM ASC,LKP.EDW_STRT_DTTM ASC) RNK
             FROM       LKP_AGMT_POL
             INNER JOIN exp_trans
             ON         LKP_AGMT_POL.source_record_id = exp_trans.source_record_id
             LEFT JOIN
                        (
                                 SELECT   AGMT_LOCTR.AGMT_LOCTR_STRT_DTTM    AS AGMT_LOCTR_STRT_DTTM,
                                          AGMT_LOCTR.AGMT_LOCTR_END_DTTM     AS AGMT_LOCTR_END_DTTM,
                                          AGMT_LOCTR.EDW_STRT_DTTM           AS EDW_STRT_DTTM,
                                          AGMT_LOCTR.AGMT_ID                 AS AGMT_ID,
                                          AGMT_LOCTR.AGMT_LOCTR_ROLE_TYPE_CD AS AGMT_LOCTR_ROLE_TYPE_CD,
                                          AGMT_LOCTR.LOC_ID                  AS LOC_ID
                                 FROM     DB_T_PROD_CORE.AGMT_LOCTR 
                                 QUALIFY ROW_NUMBER() OVER( PARTITION BY AGMT_LOCTR.AGMT_LOCTR_ROLE_TYPE_CD,AGMT_LOCTR.AGMT_ID ORDER BY EDW_END_DTTM DESC) = 1 ) LKP
             ON         LKP.AGMT_ID = LKP_AGMT_POL.AGMT_ID
             AND        LKP.AGMT_LOCTR_ROLE_TYPE_CD = exp_trans.out_AGMT_LOCTR_ROLE_TYPE_CD 
             QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_AGMT_POL.source_record_id ORDER BY LKP.AGMT_ID ASC,LKP.AGMT_LOCTR_ROLE_TYPE_CD ASC,LKP.AGMT_LOCTR_STRT_DTTM ASC,LKP.LOC_ID ASC,LKP.AGMT_LOCTR_END_DTTM ASC,LKP.EDW_STRT_DTTM ASC) = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_CDC_Check AS
  (
             SELECT     exp_trans.AGMT_ID                       AS in_AGMT_ID,
                        exp_trans.out_AGMT_LOCTR_ROLE_TYPE_CD   AS in_AGMT_LOCTR_ROLE_TYPE_CD,
                        exp_trans.EFF_DT                        AS in_AGMT_LOCTR_STRT_DT,
                        exp_trans.TERR_ID                       AS in_LOC_ID,
                        exp_trans.END_DT1                       AS in_AGMT_LOCTR_END_DT,
                        exp_trans.out_PRCS_ID                   AS in_PRCS_ID,
                        exp_trans.EDW_STRT_DTTM                 AS in_EDW_STRT_DTTM,
                        exp_trans.EDW_END_DTTM                  AS in_EDW_END_DTTM,
                        exp_trans.UPDATETIME                    AS in_TRANS_STRT_DTTM,
                        LKP_AGMT_LOCTR1.AGMT_ID                 AS lkp_AGMT_ID,
                        LKP_AGMT_LOCTR1.AGMT_LOCTR_ROLE_TYPE_CD AS lkp_AGMT_LOCTR_ROLE_TYPE_CD,
                        LKP_AGMT_LOCTR1.AGMT_LOCTR_STRT_DTTM    AS lkp_AGMT_LOCTR_STRT_DT,
                        LKP_AGMT_LOCTR1.EDW_STRT_DTTM           AS lkp_EDW_STRT_DTTM,
                        MD5 ( TO_CHAR ( exp_trans.EFF_DT )
                                   || TO_CHAR ( exp_trans.END_DT1 )
                                   || TO_CHAR ( exp_trans.TERR_ID ) ) AS v_SRC_MD5,
                        MD5 ( TO_CHAR ( LKP_AGMT_LOCTR1.AGMT_LOCTR_STRT_DTTM )
                                   || TO_CHAR ( LKP_AGMT_LOCTR1.AGMT_LOCTR_END_DTTM )
                                   || TO_CHAR ( LKP_AGMT_LOCTR1.LOC_ID ) ) AS v_TGT_MD5,
                        CASE
                                   WHEN v_TGT_MD5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS OUT_INS_UPD,
                        exp_trans.source_record_id
             FROM       exp_trans
             INNER JOIN LKP_AGMT_LOCTR1
             ON         exp_trans.source_record_id = LKP_AGMT_LOCTR1.source_record_id );

  -- Component RTR_AGMT_LOCTR_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table RTR_AGMT_LOCTR_INSERT as
    SELECT exp_CDC_Check.in_AGMT_ID                  AS in_AGMT_ID,
         exp_CDC_Check.in_AGMT_LOCTR_ROLE_TYPE_CD  AS in_AGMT_LOCTR_ROLE_TYPE_CD,
         exp_CDC_Check.in_AGMT_LOCTR_STRT_DT       AS in_AGMT_LOCTR_STRT_DT,
         exp_CDC_Check.in_LOC_ID                   AS in_LOC_ID,
         exp_CDC_Check.in_AGMT_LOCTR_END_DT        AS in_AGMT_LOCTR_END_DT,
         exp_CDC_Check.in_PRCS_ID                  AS in_PRCS_ID,
         exp_CDC_Check.in_EDW_STRT_DTTM            AS in_EDW_STRT_DTTM,
         exp_CDC_Check.in_EDW_END_DTTM             AS in_EDW_END_DTTM,
         exp_CDC_Check.in_TRANS_STRT_DTTM          AS in_TRANS_STRT_DTTM,
         NULL                                      AS in_TRANS_END_DTTM,
         exp_CDC_Check.lkp_AGMT_ID                 AS lkp_AGMT_ID,
         exp_CDC_Check.lkp_AGMT_LOCTR_ROLE_TYPE_CD AS lkp_AGMT_LOCTR_ROLE_TYPE_CD,
         exp_CDC_Check.lkp_EDW_STRT_DTTM           AS lkp_EDW_STRT_DTTM,
         exp_CDC_Check.OUT_INS_UPD                 AS OUT_INS_UPD,
         NULL                                      AS out_trans_end_dttm,
         NULL                                      AS RANKINDEX,
         exp_CDC_Check.lkp_AGMT_LOCTR_STRT_DT      AS lkp_AGMT_LOCTR_STRT_DT,
         exp_CDC_Check.source_record_id
  FROM   exp_CDC_Check
  WHERE  exp_CDC_Check.OUT_INS_UPD = ''I''
  AND    exp_CDC_Check.in_LOC_ID IS NOT NULL
  AND    exp_CDC_Check.in_AGMT_ID IS NOT NULL;
  
  -- Component RTR_AGMT_LOCTR_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table RTR_AGMT_LOCTR_UPDATE as
  SELECT exp_CDC_Check.in_AGMT_ID                  AS in_AGMT_ID,
         exp_CDC_Check.in_AGMT_LOCTR_ROLE_TYPE_CD  AS in_AGMT_LOCTR_ROLE_TYPE_CD,
         exp_CDC_Check.in_AGMT_LOCTR_STRT_DT       AS in_AGMT_LOCTR_STRT_DT,
         exp_CDC_Check.in_LOC_ID                   AS in_LOC_ID,
         exp_CDC_Check.in_AGMT_LOCTR_END_DT        AS in_AGMT_LOCTR_END_DT,
         exp_CDC_Check.in_PRCS_ID                  AS in_PRCS_ID,
         exp_CDC_Check.in_EDW_STRT_DTTM            AS in_EDW_STRT_DTTM,
         exp_CDC_Check.in_EDW_END_DTTM             AS in_EDW_END_DTTM,
         exp_CDC_Check.in_TRANS_STRT_DTTM          AS in_TRANS_STRT_DTTM,
         NULL                                      AS in_TRANS_END_DTTM,
         exp_CDC_Check.lkp_AGMT_ID                 AS lkp_AGMT_ID,
         exp_CDC_Check.lkp_AGMT_LOCTR_ROLE_TYPE_CD AS lkp_AGMT_LOCTR_ROLE_TYPE_CD,
         exp_CDC_Check.lkp_EDW_STRT_DTTM           AS lkp_EDW_STRT_DTTM,
         exp_CDC_Check.OUT_INS_UPD                 AS OUT_INS_UPD,
         NULL                                      AS out_trans_end_dttm,
         NULL                                      AS RANKINDEX,
         exp_CDC_Check.lkp_AGMT_LOCTR_STRT_DT      AS lkp_AGMT_LOCTR_STRT_DT,
         exp_CDC_Check.source_record_id
  FROM   exp_CDC_Check
  WHERE
         CASE
                WHEN exp_CDC_Check.OUT_INS_UPD = ''U''
                AND    exp_CDC_Check.in_LOC_ID IS NOT NULL
                AND    (
                              exp_CDC_Check.in_AGMT_LOCTR_STRT_DT >= exp_CDC_Check.lkp_AGMT_LOCTR_STRT_DT ) THEN 1
                ELSE NULL
         END -- EIM - 18970 - -
        --  CASE
        --         WHEN exp_CDC_Check.OUT_INS_UPD = ''U''
        --         AND    (
        --                       exp_CDC_Check.in_AGMT_LOCTR_STRT_DT > exp_CDC_Check.lkp_AGMT_LOCTR_STRT_DT ) THEN 1
        --         ELSE $3
        -- END -- exp_CDC_Check.OUT_INS_UPD = ''U''
  AND    exp_CDC_Check.in_LOC_ID IS NOT NULL;
  
  -- Component exp_ins, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_ins AS
  (
         SELECT RTR_AGMT_LOCTR_UPDATE.in_AGMT_ID                 AS in_AGMT_ID3,
                RTR_AGMT_LOCTR_UPDATE.in_AGMT_LOCTR_ROLE_TYPE_CD AS in_AGMT_LOCTR_ROLE_TYPE_CD3,
                RTR_AGMT_LOCTR_UPDATE.in_AGMT_LOCTR_STRT_DT      AS in_AGMT_LOCTR_STRT_DT3,
                RTR_AGMT_LOCTR_UPDATE.in_LOC_ID                  AS in_LOC_ID3,
                RTR_AGMT_LOCTR_UPDATE.in_AGMT_LOCTR_END_DT       AS in_AGMT_LOCTR_END_DT3,
                RTR_AGMT_LOCTR_UPDATE.in_PRCS_ID                 AS in_PRCS_ID3,
                RTR_AGMT_LOCTR_UPDATE.in_EDW_STRT_DTTM           AS in_EDW_STRT_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.in_EDW_END_DTTM            AS in_EDW_END_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.in_TRANS_STRT_DTTM         AS in_TRANS_STRT_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.source_record_id
         FROM   RTR_AGMT_LOCTR_UPDATE );
 
  -- Component AGMT_LOCTR_ins, Type TARGET
  INSERT INTO DB_T_PROD_CORE.AGMT_LOCTR
              (
                          AGMT_ID,
                          AGMT_LOCTR_ROLE_TYPE_CD,
                          AGMT_LOCTR_STRT_DTTM,
                          LOC_ID,
                          AGMT_LOCTR_END_DTTM,
                          PRCS_ID,
                          EDW_STRT_DTTM,
                          EDW_END_DTTM,
                          TRANS_STRT_DTTM
              )
  SELECT exp_ins.in_AGMT_ID3                 AS AGMT_ID,
         exp_ins.in_AGMT_LOCTR_ROLE_TYPE_CD3 AS AGMT_LOCTR_ROLE_TYPE_CD,
         exp_ins.in_AGMT_LOCTR_STRT_DT3      AS AGMT_LOCTR_STRT_DTTM,
         exp_ins.in_LOC_ID3                  AS LOC_ID,
         exp_ins.in_AGMT_LOCTR_END_DT3       AS AGMT_LOCTR_END_DTTM,
         exp_ins.in_PRCS_ID3                 AS PRCS_ID,
         exp_ins.in_EDW_STRT_DTTM3           AS EDW_STRT_DTTM,
         exp_ins.in_EDW_END_DTTM3            AS EDW_END_DTTM,
         exp_ins.in_TRANS_STRT_DTTM3         AS TRANS_STRT_DTTM
  FROM   exp_ins;
  
  -- Component exp_pass_to_tgt, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
  (
         SELECT RTR_AGMT_LOCTR_INSERT.in_AGMT_ID                                      AS in_AGMT_ID1,
                RTR_AGMT_LOCTR_INSERT.in_AGMT_LOCTR_ROLE_TYPE_CD                      AS in_AGMT_LOCTR_ROLE_TYPE_CD1,
                RTR_AGMT_LOCTR_INSERT.in_AGMT_LOCTR_STRT_DT                           AS in_AGMT_LOCTR_STRT_DT1,
                RTR_AGMT_LOCTR_INSERT.in_LOC_ID                                       AS in_LOC_ID1,
                RTR_AGMT_LOCTR_INSERT.in_AGMT_LOCTR_END_DT                            AS in_AGMT_LOCTR_END_DT1,
                RTR_AGMT_LOCTR_INSERT.in_PRCS_ID                                      AS in_PRCS_ID1,
                RTR_AGMT_LOCTR_INSERT.in_EDW_STRT_DTTM                                AS in_EDW_STRT_DTTM1,
                RTR_AGMT_LOCTR_INSERT.in_TRANS_STRT_DTTM                              AS in_TRANS_STRT_DTTM1,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_EDW_END_DATE,
                RTR_AGMT_LOCTR_INSERT.source_record_id
         FROM   RTR_AGMT_LOCTR_INSERT );
  -- Component upd_update, Type UPDATE
  CREATE
  OR
  REPLACE TEMPORARY TABLE upd_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT RTR_AGMT_LOCTR_UPDATE.lkp_AGMT_ID                 AS lkp_AGMT_ID3,
                RTR_AGMT_LOCTR_UPDATE.lkp_AGMT_LOCTR_ROLE_TYPE_CD AS lkp_AGMT_LOCTR_ROLE_TYPE_CD3,
                RTR_AGMT_LOCTR_UPDATE.lkp_EDW_STRT_DTTM           AS lkp_EDW_STRT_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.in_EDW_STRT_DTTM            AS in_EDW_STRT_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.in_TRANS_STRT_DTTM          AS in_TRANS_STRT_DTTM3,
                RTR_AGMT_LOCTR_UPDATE.source_record_id,
                1                                                 AS UPDATE_STRATEGY_ACTION
         FROM   RTR_AGMT_LOCTR_UPDATE );
  -- Component exp_upd, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_upd AS
  (
         SELECT upd_update.lkp_AGMT_ID3                            AS lkp_AGMT_ID3,
                upd_update.lkp_AGMT_LOCTR_ROLE_TYPE_CD3            AS lkp_AGMT_LOCTR_ROLE_TYPE_CD3,
                upd_update.lkp_EDW_STRT_DTTM3                      AS lkp_EDW_STRT_DTTM3,
                DATEADD(''second'', - 1, upd_update.in_EDW_STRT_DTTM3)   AS EDW_END_DTTM,
                DATEADD(''second'', - 1, upd_update.in_TRANS_STRT_DTTM3) AS TRANS_END_DTTM,
                upd_update.source_record_id
         FROM   upd_update );
  -- Component AGMT_LOCTR_ins_new, Type TARGET
  INSERT INTO DB_T_PROD_CORE.AGMT_LOCTR
              (
                          AGMT_ID,
                          AGMT_LOCTR_ROLE_TYPE_CD,
                          AGMT_LOCTR_STRT_DTTM,
                          LOC_ID,
                          AGMT_LOCTR_END_DTTM,
                          PRCS_ID,
                          EDW_STRT_DTTM,
                          EDW_END_DTTM,
                          TRANS_STRT_DTTM
              )
  SELECT exp_pass_to_tgt.in_AGMT_ID1                 AS AGMT_ID,
         exp_pass_to_tgt.in_AGMT_LOCTR_ROLE_TYPE_CD1 AS AGMT_LOCTR_ROLE_TYPE_CD,
         exp_pass_to_tgt.in_AGMT_LOCTR_STRT_DT1      AS AGMT_LOCTR_STRT_DTTM,
         exp_pass_to_tgt.in_LOC_ID1                  AS LOC_ID,
         exp_pass_to_tgt.in_AGMT_LOCTR_END_DT1       AS AGMT_LOCTR_END_DTTM,
         exp_pass_to_tgt.in_PRCS_ID1                 AS PRCS_ID,
         exp_pass_to_tgt.in_EDW_STRT_DTTM1           AS EDW_STRT_DTTM,
         exp_pass_to_tgt.out_EDW_END_DATE            AS EDW_END_DTTM,
         exp_pass_to_tgt.in_TRANS_STRT_DTTM1         AS TRANS_STRT_DTTM
  FROM   exp_pass_to_tgt;
  
  -- Component AGMT_LOCTR_upd, Type TARGET
  MERGE
  INTO         DB_T_PROD_CORE.AGMT_LOCTR
  USING        exp_upd
  ON (
                            AGMT_LOCTR.AGMT_ID = exp_upd.lkp_AGMT_ID3
               AND          AGMT_LOCTR.AGMT_LOCTR_ROLE_TYPE_CD = exp_upd.lkp_AGMT_LOCTR_ROLE_TYPE_CD3
               AND          AGMT_LOCTR.EDW_STRT_DTTM = exp_upd.lkp_EDW_STRT_DTTM3)
  WHEN MATCHED THEN
  UPDATE
  SET    AGMT_ID = exp_upd.lkp_AGMT_ID3,
         AGMT_LOCTR_ROLE_TYPE_CD = exp_upd.lkp_AGMT_LOCTR_ROLE_TYPE_CD3,
         EDW_STRT_DTTM = exp_upd.lkp_EDW_STRT_DTTM3,
         EDW_END_DTTM = exp_upd.EDW_END_DTTM,
         TRANS_END_DTTM = exp_upd.TRANS_END_DTTM;

END;
';