-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_INSU_ASST_FEAT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
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
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component SQ_pc_agmt_insrd_asset_feat_x, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_pc_agmt_insrd_asset_feat_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_AGMT_ASSET_FEAT_STRT_DTTM,
                $2  AS lkp_AGMT_ASSET_STRT_DTTM,
                $3  AS lkp_AGMT_ASSET_FEAT_END_DTTM,
                $4  AS lkp_RATE_SYMB_CD,
                $5  AS lkp_AGMT_ASSET_FEAT_AMT,
                $6  AS lkp_AGMT_ASSET_FEAT_DT,
                $7  AS lkp_AGMT_ASSET_FEAT_TXT,
                $8  AS lkp_AGMT_ASSET_FEAT_IND,
                $9  AS lkp_FEAT_EFECT_TYPE_CD,
                $10 AS lkp_AGMT_ID,
                $11 AS lkp_FEAT_ID,
                $12 AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
                $13 AS lkp_PRTY_ASSET_ID,
                $14 AS lkp_EDW_STRT_DTTM,
                $15 AS lkp_EDW_END_DTTM,
                $16 AS feature_strt_dt,
                $17 AS asset_strt_dt,
                $18 AS feature_end_dt,
                $19 AS AGMT_ID,
                $20 AS FEAT_ID,
                $21 AS PRTY_ASSET_ID,
                $22 AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
                $23 AS INSRNC_CVGE_TYPE_CD,
                $24 AS Ratesymbol_alfa,
                $25 AS RateSymbolCollision_alfa,
                $26 AS RATE_SYMB_CD,
                $27 AS AGMT_ASSET_FEAT_AMT,
                $28 AS AGMT_ASSET_FEAT_DT,
                $29 AS AGMT_ASSET_FEAT_TXT,
                $30 AS AGMT_ASSET_FEAT_IND,
                $31 AS DiscountSurcharge_alfa_typecd,
                $32 AS TRANS_STRT_DTTM,
                $33 AS Retired,
                $34 AS polcov_RateModifier,
                $35 AS polcov_Eligible,
                $36 AS sourcedata,
                $37 AS targetdata,
                $38 AS ins_upd_flag,
                $39 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                  /*  EIM-33116 Added policyperiod join condition in all inner query and created seperate with clause to handle source part--- */
                                  WITH
                                  /*EIM-48971- FARM CHANGES BEGINS*/
                                  FARM_TEMP AS
                                  (
                                                  /* Coverages */
                                                  SELECT DISTINCT pp.publicid_stg,
                                                                  pp.PeriodStart_stg AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate_stg IS NULL THEN pp.PeriodStart_stg
                                                                                  ELSE polcov.EffectiveDate_stg
                                                                  END feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate_stg IS NULL THEN pp.PeriodEnd_stg
                                                                                  ELSE polcov.ExpirationDate_stg
                                                                  END                                                 feature_end_dt,
                                                                  CAST(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS Cntrct_role,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType = ''Package'' THEN PACKAGE.packagePatternID
                                                                                  WHEN covterm.CovTermType = ''Option''
                                                                                  AND             polcov.val IS NOT NULL THEN optn.optionPatternID
                                                                                  WHEN covterm.CovTermType = ''Clause'' THEN covterm.clausePatternID
                                                                                  ELSE covterm.covtermPatternID
                                                                  END AS nk_public_id,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType = ''Package'' THEN CAST (''PACKAGE'' AS VARCHAR (50))
                                                                                  WHEN covterm.CovTermType = ''Option''
                                                                                  AND             polcov.val IS NOT NULL THEN CAST (''OPTIONS'' AS VARCHAR(50))
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN CAST(''CLAUSE'' AS        VARCHAR(50))
                                                                                  ELSE CAST (''COVTERM'' AS                                        VARCHAR (50))
                                                                  END                                                   FEAT_SBTYPE_CD,
                                                                  polcov.assettype_stg                                  AS assettype,
                                                                  polcov.classification_code                            AS classification_code,
                                                                  polcov.assetkey                                       AS fixedid,
                                                                  COALESCE(polcov.EffectiveDate_stg,pp.periodstart_stg) AS asset_start_dt,
                                                                  polcov.updatetime_stg                                 AS updatetime_stg,
                                                                  ''SRC_SYS4''                                            AS SRC_CD,
                                                                  CAST(NULL AS VARCHAR(60))                             AS RateSymbolCollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))                             AS RateSymbol_alfa_stg,
                                                                  pp.Retired_stg                                        AS Retired,
                                                                  polcov.val                                               feat_val,
                                                                  CAST(
                                                                  CASE
                                                                                  WHEN optn.ValueType=''Percent'' THEN optn.Value1
                                                                  END AS       DECIMAL(14,4))    feat_rate,
                                                                  CAST(NULL AS VARCHAR(10))   AS Eligible,
                                                                  covterm.CovTermType            feat_CovTermType,
                                                                  CAST(NULL AS VARCHAR(50))   AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                           /* DB_T_PROD_STAG.pcx_fopdwellingcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm2_stg IS NULL THEN CAST(DateTerm2_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm2_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingcov dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DateTerm2Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm1Avl_stg IS NULL ) AS fopdwell 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopoutbuildingcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm2_stg IS NULL THEN CAST(DateTerm2_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm2_stg ,''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DateTerm2Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL ) AS fopoutbldg 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_Stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopfeedandseedcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FeedAndSeed_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfeedandseedcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FeedAndSeed_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfeedandseedcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FeedAndSeed_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfeedandseedcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FeedAndSeed_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfeedandseedcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL ) AS fopfdsd 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopmachinerycov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Machinery_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopmachinerycov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Machinery_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopmachinerycov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Machinery_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopmachinerycov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Machinery_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopmachinerycov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Machinery_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopmachinerycov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL ) AS fopmch 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_foplivestockcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Livestock_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_foplivestockcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Livestock_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_foplivestockcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Livestock_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_foplivestockcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    Livestock_stg                              AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_foplivestockcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL ) AS fopliv 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPDwellingScheduleCovItem_stg             AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             BooleanTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm4Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm1Avl_stg IS NULL
                                                                                                    AND             StringTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm3Avl_stg IS NULL ) AS dwellsch 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm2_stg IS NULL THEN CAST(DateTerm2_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm2_stg ,''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPFarmownersLiScheduleCovItem_stg         AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             BooleanTerm1Avl_stg IS NULL
                                                                                                    AND             BooleanTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm4Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DateTerm2Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm1Avl_stg IS NULL
                                                                                                    AND             StringTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm3Avl_stg IS NULL
                                                                                                    AND             StringTerm4Avl_stg IS NULL ) AS fopfarmsch 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm5_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm5Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm6_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm6Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm7_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm7Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm8'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm8_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm8Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm9'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm9_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm9Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm10'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm10_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS      VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm10Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm11'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm11_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS      VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm11Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm12'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm12_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS      VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm12Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm13'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm13_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS      VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm13Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm5_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm5Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm6_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm6Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm7_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm7Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm8'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm8_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm8Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm5_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm5Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm6_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm6Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm7_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm7Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    fop.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    fop.updatetime_stg,
                                                                                                                    FOPLiabilityScheduleCovItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov fop
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = fop.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             BooleanTerm1Avl_stg IS NULL
                                                                                                    AND             BooleanTerm2Avl_stg IS NULL
                                                                                                    AND             BooleanTerm3Avl_stg IS NULL
                                                                                                    AND             BooleanTerm4Avl_stg IS NULL
                                                                                                    AND             BooleanTerm5Avl_stg IS NULL
                                                                                                    AND             BooleanTerm6Avl_stg IS NULL
                                                                                                    AND             BooleanTerm7Avl_stg IS NULL
                                                                                                    AND             BooleanTerm8Avl_stg IS NULL
                                                                                                    AND             BooleanTerm9Avl_stg IS NULL
                                                                                                    AND             BooleanTerm10Avl_stg IS NULL
                                                                                                    AND             BooleanTerm11Avl_stg IS NULL
                                                                                                    AND             BooleanTerm12Avl_stg IS NULL
                                                                                                    AND             BooleanTerm13Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm4Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL
                                                                                                    AND             DirectTerm3Avl_stg IS NULL
                                                                                                    AND             DirectTerm4Avl_stg IS NULL
                                                                                                    AND             DirectTerm5Avl_stg IS NULL
                                                                                                    AND             DirectTerm6Avl_stg IS NULL
                                                                                                    AND             DirectTerm7Avl_stg IS NULL
                                                                                                    AND             StringTerm1Avl_stg IS NULL
                                                                                                    AND             StringTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm3Avl_stg IS NULL
                                                                                                    AND             StringTerm4Avl_stg IS NULL
                                                                                                    AND             StringTerm5Avl_stg IS NULL
                                                                                                    AND             StringTerm6Avl_stg IS NULL
                                                                                                    AND             StringTerm7Avl_stg IS NULL
                                                                                                    AND             StringTerm8Avl_stg IS NULL ) AS fopliabsch 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1 ) polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id,
                                                                                PolicyNumber_stg,
                                                                                PeriodStart_stg,
                                                                                PeriodEnd_stg,
                                                                                MostRecentModel_stg,
                                                                                Status_stg,
                                                                                JobID_stg,
                                                                                PublicID_stg,
                                                                                EditEffectiveDate_stg,
                                                                                Createtime_stg,
                                                                                updatetime_stg,
                                                                                Retired_stg
                                                                         FROM   db_t_prod_stag.pc_policyperiod) pp
                                                  ON              pp.id = polcov.BranchID
                                                  LEFT JOIN
                                                                  (
                                                                         SELECT pcl.PatternID_stg     clausePatternID,
                                                                                pcv.PatternID_stg     covtermPatternID,
                                                                                pcv.ColumnName_stg  AS columnname,
                                                                                pcv.CovTermType_stg AS covtermtype,
                                                                                pcl.name_stg           clausename
                                                                         FROM   DB_T_PROD_STAG.pc_etlclausepattern pcl
                                                                         JOIN   DB_T_PROD_STAG.pc_etlcovtermpattern pcv
                                                                         ON     pcl.id_stg = pcv.ClausePatternID_stg
                                                                         UNION
                                                                         SELECT    pcl.PatternID_stg                       clausePatternID,
                                                                                   pcv.PatternID_stg                       covtermPatternID,
                                                                                   COALESCE(pcv.ColumnName_stg,''Clause'')   columnname,
                                                                                   COALESCE(pcv.CovTermType_stg, ''Clause'') covtermtype,
                                                                                   pcl.name_stg                            clausename
                                                                         FROM      DB_T_PROD_STAG.pc_etlclausepattern pcl
                                                                         LEFT JOIN
                                                                                   (
                                                                                          SELECT *
                                                                                          FROM   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                                          WHERE  Name_stg NOT LIKE ''ZZ%'' ) pcv
                                                                         ON        pcv.ClausePatternID_stg = pcl.ID_stg
                                                                         WHERE     pcl.Name_stg NOT LIKE ''ZZ%''
                                                                         AND       pcv.Name_stg IS NULL
                                                                         AND       OwningEntityType_stg IN (''FOPBlanket'',
                                                                                                            ''FOPDwelling'',
                                                                                                            ''FOPDwellingScheduleCovItem'',
                                                                                                            ''FOPDwellingScheduleExclItem'',
                                                                                                            ''FOPFarmownersLine'',
                                                                                                            ''FOPFarmownersLineScheduleCovItem '',
                                                                                                            ''FOPFeedAndSeed'',
                                                                                                            ''FOPLiability'',
                                                                                                            ''FOPLiabilityScheduleCovItem'',
                                                                                                            ''FOPLiabilityScheduleExclItem'',
                                                                                                            ''FOPLivestock'',
                                                                                                            ''FOPMachinery'',
                                                                                                            ''FOPOutbuilding'') ) covterm
                                                  ON              covterm.clausePatternID = polcov.PatternCode_stg
                                                  AND             covterm.ColumnName = polcov.columnname
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT pcv.PatternID_stg   packagePatternID,
                                                                                pcv.PackageCode_stg cov_id,
                                                                                pcv.PackageCode_stg name1
                                                                         FROM   DB_T_PROD_STAG.pc_etlcovtermpackage pcv) PACKAGE
                                                  ON              PACKAGE.packagePatternID = polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                             SELECT     pct.PatternID_stg                      optionPatternID,
                                                                                        pct.optioncode_stg                     name1,
                                                                                        CAST(pct.value_stg AS VARCHAR(255)) AS value1,
                                                                                        pcv.ValueType_stg                   AS ValueType
                                                                             FROM       DB_T_PROD_STAG.pc_etlcovtermpattern pcv
                                                                             INNER JOIN DB_T_PROD_STAG.pc_etlcovtermoption pct
                                                                             ON         pcv.id_stg = pct.CoverageTermPatternID_stg ) optn
                                                  ON              optn.optionPatternID = polcov.val
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus pps
                                                  ON              pps.id_stg = pp.Status_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pc_job pj
                                                  ON              pj.id_stg = pp.JobID_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job pcj
                                                  ON              pcj.id_stg = pj.Subtype_stg
                                                  WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                  AND             pps.TYPECODE_stg = ''Bound''
                                                  AND             pp.updatetime_stg > (:start_dttm)
                                                  AND             pp.updatetime_stg <= (:end_dttm)
                                                  UNION
                                                  /* Modifiers */
                                                  SELECT DISTINCT pp.publicid_stg,
                                                                  pp.PeriodStart_stg AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate_stg IS NULL THEN pp.PeriodStart_stg
                                                                                  ELSE polcov.EffectiveDate_stg
                                                                  END feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate_stg IS NULL THEN pp.PeriodEnd_stg
                                                                                  ELSE polcov.ExpirationDate_stg
                                                                  END                                                   feature_end_dt,
                                                                  CAST(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))      AS Cntrct_role,
                                                                  polcov.PatternCode_stg                                AS nk_public_id,
                                                                  polcov.typ                                            AS FEAT_SBTYPE_CD,
                                                                  polcov.assettype_stg                                  AS assettype,
                                                                  polcov.classification_code                            AS classification_code,
                                                                  polcov.assetkey                                       AS fixedid,
                                                                  COALESCE(polcov.effectivedate_stg,pp.periodstart_stg) AS asset_start_dt,
                                                                  pp.updatetime_stg                                     AS updatetime_stg,
                                                                  ''SRC_SYS4''                                            AS SRC_CD,
                                                                  CAST(NULL AS VARCHAR(60))                             AS RateSymbolCollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))                             AS RateSymbol_alfa_stg,
                                                                  pp.Retired_stg                                        AS Retired,
                                                                  CAST(NULL AS             VARCHAR(255))                            AS feat_val,
                                                                  CAST(polcov.feat_rate AS DECIMAL(14,4))                           AS feat_rate,
                                                                  CAST(polcov.Eligible AS  VARCHAR(10))                             AS Eligible ,
                                                                  CAST(NULL AS             VARCHAR(255))                            AS feat_CovTermType,
                                                                  CAST(pda.typecode_stg AS VARCHAR(50))                             AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                             SELECT     patterncode_stg,
                                                                                        CAST(BranchID_stg AS VARCHAR(255)) AS BranchID,
                                                                                        ''MODIFIER''                         AS typ,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ratemodifier_stg AS           VARCHAR(255)) AS ratemodifier,
                                                                                        CAST(DiscountSurcharge_alfa_stg AS VARCHAR(255)) AS DiscountSurcharge_alfa,
                                                                                        CAST(
                                                                                        CASE
                                                                                                   WHEN fop.Eligible_stg= 1 THEN fop.RateModifier_stg
                                                                                                   ELSE 0
                                                                                        END AS               VARCHAR(255))         AS feat_rate,
                                                                                        CAST(Eligible_stg AS VARCHAR(10))          AS Eligible,
                                                                                        FOPDwelling_stg                            AS assetkey,
                                                                                        CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                        CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                             FROM       DB_T_PROD_STAG.pcx_fopdwellingmodifier fop
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg = fop.BranchID_stg
                                                                             WHERE      (
                                                                                                   ExpirationDate_stg IS NULL
                                                                                        OR         ExpirationDate_stg > modeldate_stg)
                                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                                             AND        pp.updatetime_stg <= (:end_dttm) 
                                                                             qualify row_number() over (PARTITION BY BranchID_stg,assetkey,patterncode_stg ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,fop.updatetime_stg DESC,fop.createtime_stg DESC)=1
                                                                             UNION
                                                                             SELECT     patterncode_stg,
                                                                                        CAST(BranchID_stg AS VARCHAR(255)) AS BranchID,
                                                                                        ''MODIFIER''                         AS typ,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ratemodifier_stg AS           VARCHAR(255)) AS ratemodifier,
                                                                                        CAST(DiscountSurcharge_alfa_stg AS VARCHAR(255)) AS DiscountSurcharge_alfa,
                                                                                        CAST(
                                                                                        CASE
                                                                                                   WHEN fop.Eligible_stg= 1 THEN fop.RateModifier_stg
                                                                                                   ELSE 0
                                                                                        END AS               VARCHAR(255))         AS feat_rate,
                                                                                        CAST(Eligible_stg AS VARCHAR(10))          AS Eligible,
                                                                                        FOPMachinery_stg                           AS assetkey,
                                                                                        CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                        CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                             FROM       DB_T_PROD_STAG.pcx_fopmachinerymodifier fop
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg = fop.BranchID_stg
                                                                             WHERE      (
                                                                                                   ExpirationDate_stg IS NULL
                                                                                        OR         ExpirationDate_stg > modeldate_stg)
                                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                                             AND        pp.updatetime_stg <= (:end_dttm) 
                                                                             qualify row_number() over (PARTITION BY BranchID_stg,assetkey,patterncode_stg ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,fop.updatetime_stg DESC,fop.createtime_stg DESC)=1 ) polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id,
                                                                                PolicyNumber_stg,
                                                                                PeriodStart_stg,
                                                                                EditEffectiveDate_stg,
                                                                                PeriodEnd_stg,
                                                                                MostRecentModel_stg,
                                                                                Status_stg,
                                                                                JobID_stg,
                                                                                PublicID_stg,
                                                                                createtime_stg,
                                                                                updatetime_stg,
                                                                                Retired_stg
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pp
                                                  ON              pp.id = polcov.BranchID
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus pps
                                                  ON              pps.id_stg = pp.Status_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pc_job pj
                                                  ON              pj.id_stg = pp.JobID_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job pcj
                                                  ON              pcj.id_stg=pj.Subtype_stg
                                                  LEFT JOIN       DB_T_PROD_STAG.pctl_discountsurcharge_alfa pda
                                                  ON              polcov.DiscountSurcharge_alfa = pda.ID_stg
                                                  WHERE           pps.TYPECODE_stg = ''Bound''
                                                  AND             pp.updatetime_stg > (:start_dttm)
                                                  AND             pp.updatetime_stg <= (:end_dttm)
                                                  UNION
                                                  /* Exclusions */
                                                  SELECT DISTINCT pp.PUBLICID_stg,
                                                                  pp.PeriodStart_stg AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate_stg IS NULL THEN pp.PeriodStart_stg
                                                                                  ELSE polcov.EffectiveDate_stg
                                                                  END AS feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate_stg IS NULL THEN pp.PeriodEnd_stg
                                                                                  ELSE polcov.ExpirationDate_stg
                                                                  END                                               AS feature_end_dt,
                                                                  CAST (''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN PACKAGE.packagePatternID
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN optn.optionPatternID
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN covterm.clausePatternID
                                                                                  ELSE covterm.covtermPatternID
                                                                  END AS nk_public_id,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN CAST (''PACKAGE'' AS VARCHAR (50))
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN CAST (''OPTIONS'' AS VARCHAR(50))
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN CAST(''CLAUSE'' AS        VARCHAR(50))
                                                                                  ELSE CAST ( ''COVTERM'' AS                                       VARCHAR (50))
                                                                  END                                                   AS FEAT_SBTYPE_CD,
                                                                  polcov.assettype_stg                                  AS assettype ,
                                                                  polcov.classification_code                            AS classification_code,
                                                                  polcov.assetkey                                       AS fixedid,
                                                                  COALESCE(polcov.EffectiveDate_stg,pp.PeriodStart_stg) AS asset_start_dt,
                                                                  polcov.updatetime_stg,
                                                                  ''SRC_SYS4''                        AS src_cd,
                                                                  CAST(NULL AS VARCHAR(60))         AS RateSymbolCollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))         AS RateSymbol_alfa_stg,
                                                                  pp.Retired_stg                    AS Retired,
                                                                  CAST(polcov.val AS VARCHAR(255))  AS feat_val,
                                                                  CAST(NULL AS       DECIMAL(14,4)) AS feat_rate,
                                                                  CAST(NULL AS       VARCHAR(5))    AS Eligible,
                                                                  covterm.CovTermType               AS feat_CovTermType,
                                                                  CAST(NULL AS VARCHAR(50))         AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                           /* DB_T_PROD_STAG.pcx_fopdwellingexcl */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Dwelling_stg                               AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             BooleanTerm1Avl_stg IS NULL
                                                                                                    AND             BooleanTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL ) AS fopdwell 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl */
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm3_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm3Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(ChoiceTerm4_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           ChoiceTerm4Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS     VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           BooleanTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                                                    CASE
                                                                                                                                    WHEN DateTerm1_stg IS NULL THEN CAST(DateTerm1_stg AS VARCHAR(255))
                                                                                                                                    ELSE CAST(to_char(DateTerm1_stg , ''mm/dd/yyyy'') AS VARCHAR(255))
                                                                                                                    END                                   AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DateTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           DirectTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm2_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm2Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    FOPDwellingScheduleExclItem_stg            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopdwellingschexclitemexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             BooleanTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm1Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm2Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm3Avl_stg IS NULL
                                                                                                    AND             ChoiceTerm4Avl_stg IS NULL
                                                                                                    AND             DateTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm1Avl_stg IS NULL
                                                                                                    AND             DirectTerm2Avl_stg IS NULL
                                                                                                    AND             StringTerm1Avl_stg IS NULL
                                                                                                    AND             StringTerm2Avl_stg IS NULL ) AS fopdwellsch 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                                                           UNION
                                                                           /* DB_T_PROD_STAG.pcx_fopoutbuildingexcl */
                                                                           SELECT   *
                                                                           FROM    (
                                                                                                    SELECT DISTINCT CAST(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                    CAST(StringTerm1_stg AS VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           StringTerm1Avl_stg = 1
                                                                                                    AND             (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    UNION
                                                                                                    SELECT DISTINCT CAST(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                                                    CAST(NULL AS            VARCHAR(255)) AS val,
                                                                                                                    CAST(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                                                    CAST(BranchID_stg AS    VARCHAR(255)) AS BranchId,
                                                                                                                    dwell.createtime_stg,
                                                                                                                    EffectiveDate_stg,
                                                                                                                    ExpirationDate_stg,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                    CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                    dwell.updatetime_stg,
                                                                                                                    Outbuilding_stg                            AS assetkey,
                                                                                                                    CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                                                    CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                                                    FROM            DB_T_PROD_STAG.pcx_fopoutbuildingexcl dwell
                                                                                                    INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                                                                    ON              pp.id_stg = dwell.BranchID_stg
                                                                                                    AND             pp.updatetime_stg > (:start_dttm)
                                                                                                    AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                    WHERE           (
                                                                                                                                    ExpirationDate_stg IS NULL
                                                                                                                    OR              ExpirationDate_stg > modeldate_stg)
                                                                                                    AND             StringTerm1Avl_stg IS NULL) AS foput 
                                                                                                    qualify row_number() over (PARTITION BY BranchID,assetkey,patterncode_stg,columnname ORDER BY COALESCE(ExpirationDate_stg,CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1 ) polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id,
                                                                                PolicyNumber_stg,
                                                                                PeriodStart_stg,
                                                                                PeriodEnd_stg,
                                                                                MostRecentModel_stg,
                                                                                Status_stg,
                                                                                JOBID_stg,
                                                                                PUBLICID_stg,
                                                                                updatetime_stg,
                                                                                Retired_stg
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pp
                                                  ON              pp.id = polcov.BranchID
                                                  LEFT JOIN
                                                                  (
                                                                         SELECT CAST(cls.PatternID_stg AS   VARCHAR(255)) AS clausePatternID,
                                                                                CAST(cov.PatternID_stg AS   VARCHAR(255)) AS covtermPatternID,
                                                                                CAST(cov.ColumnName_stg AS  VARCHAR(255)) AS columnname,
                                                                                CAST(cov.CovTermType_stg AS VARCHAR(100)) AS covtermtype,
                                                                                CAST(cls.name_stg AS        VARCHAR(255)) AS clausename
                                                                         FROM   DB_T_PROD_STAG.pc_etlclausepattern cls
                                                                         JOIN   DB_T_PROD_STAG.pc_etlcovtermpattern cov
                                                                         ON     cls.id_stg=cov.ClausePatternID_stg
                                                                         UNION
                                                                         SELECT    cls.PatternID_stg                      AS clausePatternID,
                                                                                   cov.PatternID_stg                      AS covtermPatternID,
                                                                                   COALESCE(cov.ColumnName_stg,''Clause'')  AS columnname,
                                                                                   COALESCE(cov.CovTermType_stg,''Clause'') AS covtermtype,
                                                                                   cls.name_stg                           AS clausename
                                                                         FROM      DB_T_PROD_STAG.pc_etlclausepattern cls
                                                                         LEFT JOIN
                                                                                   (
                                                                                          SELECT *
                                                                                          FROM   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                                          WHERE  Name_stg NOT LIKE ''ZZ%'') cov
                                                                         ON        cov.ClausePatternID_stg=cls.ID_stg
                                                                         WHERE     cls.Name_stg NOT LIKE ''ZZ%''
                                                                         AND       cov.Name_stg IS NULL
                                                                         AND       OwningEntityType_stg IN (''FOPBlanket'',
                                                                                                            ''FOPDwelling'',
                                                                                                            ''FOPDwellingScheduleCovItem'',
                                                                                                            ''FOPDwellingScheduleExclItem'',
                                                                                                            ''FOPFarmownersLine'',
                                                                                                            ''FOPFarmownersLineScheduleCovItem '',
                                                                                                            ''FOPFeedAndSeed'',
                                                                                                            ''FOPLiability'',
                                                                                                            ''FOPLiabilityScheduleCovItem'',
                                                                                                            ''FOPLiabilityScheduleExclItem'',
                                                                                                            ''FOPLivestock'',
                                                                                                            ''FOPMachinery'',
                                                                                                            ''FOPOutbuilding'') ) covterm
                                                  ON              covterm.clausePatternID=polcov.PatternCode_stg
                                                  AND             covterm.ColumnName=polcov.columnname
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT pkg.PatternID_stg   AS packagePatternID,
                                                                                pkg.PackageCode_stg AS cov_id,
                                                                                pkg.PackageCode_stg AS name1
                                                                         FROM   DB_T_PROD_STAG.pc_etlcovtermpackage pkg) PACKAGE
                                                  ON              PACKAGE.packagePatternID=polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                             SELECT     opt.PatternID_stg  AS optionPatternID,
                                                                                        opt.optioncode_stg AS name1,
                                                                                        opt.Value_stg,
                                                                                        cov.ValueType_stg
                                                                             FROM       DB_T_PROD_STAG.pc_etlcovtermpattern cov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_etlcovtermoption opt
                                                                             ON         cov.id_stg=opt.CoverageTermPatternID_stg ) optn
                                                  ON              optn.optionPatternID=polcov.val
                                                  JOIN            DB_T_PROD_STAG.pctl_policyperiodstatus pps
                                                  ON              pps.id_stg=pp.Status_stg
                                                  JOIN            DB_T_PROD_STAG.pc_job pj
                                                  ON              pj.id_stg=pp.JobID_stg
                                                  JOIN            DB_T_PROD_STAG.pctl_job pcj
                                                  ON              pcj.id_stg=pj.Subtype_stg
                                                  WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                  AND             pps.TYPECODE_stg=''Bound''
                                                  AND             pp.UpdateTime_stg > (:start_dttm)
                                                  AND             pp.UpdateTime_stg <= (:end_dttm)
                                                  UNION
                                                  /* DB_T_CORE_DM_PROD.ENDORSEMENT */
                                                  SELECT DISTINCT pp.PUBLICID    AS PublicID_stg,
                                                                  PP.PeriodStart AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate IS NULL THEN PP.PeriodStart
                                                                                  ELSE polcov.EffectiveDate
                                                                  END AS feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate IS NULL THEN pp.PeriodEnd
                                                                                  ELSE polcov.ExpirationDate
                                                                  END                                              AS feature_end_dt,
                                                                  CAST(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS Cntrct_role,
                                                                  nk_public_id,
                                                                  CAST(''FEAT_SBTYPE15'' AS VARCHAR(50)) AS FEAT_SBTYPE_CD,
                                                                  polcov.assettype_stg                 AS assettype,
                                                                  polcov.classification_code           AS classification_code,
                                                                  polcov.assetkey                      AS fixedid,
                                                                  pp.EditEffectiveDate                 AS asset_start_dt,
                                                                  polcov.updatetime                    AS updatetime_stg,
                                                                  ''SRC_SYS4''                           AS SRC_CD,
                                                                  CAST(NULL AS VARCHAR(60))            AS RateSymbolCollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))            AS RateSymbol_alfa_stg,
                                                                  pp.Retired                           AS Retired,
                                                                  CAST(NULL AS VARCHAR(255))           AS feat_val,
                                                                  CAST(NULL AS DECIMAL(14,4))          AS feat_rate,
                                                                  CAST(NULL AS VARCHAR(5))             AS Eligible,
                                                                  CAST(NULL AS VARCHAR(255))           AS FEAT_COVTERMTYPE,
                                                                  CAST(NULL AS VARCHAR(50))            AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                                  /*  Dwelling DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50))  AS classification_code,
                                                                                                  Dwelling_stg                                 AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopdwellingcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  outbldng DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50))  AS classification_code,
                                                                                                  Outbuilding_stg                              AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopoutbuildingcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  fdsd DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50))  AS classification_code,
                                                                                                  FeedAndSeed_stg                              AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopfeedandseedcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  mch DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50))  AS classification_code,
                                                                                                  Machinery_stg                                AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopmachinerycov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  lvstk DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50))  AS classification_code,
                                                                                                  Livestock_stg                                AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_foplivestockcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  dwellschcov DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50))  AS classification_code,
                                                                                                  FOPDwellingScheduleCovItem_stg               AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  farmschcov DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50))  AS classification_code,
                                                                                                  FOPFarmownersLiScheduleCovItem_stg           AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /*  liabsch DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS  VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                                  CAST(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                  CAST(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50))  AS classification_code,
                                                                                                  FOPLiabilityScheduleCovItem_stg              AS assetkey ,
                                                                                                  a.createtime_stg                             AS createtime ,
                                                                                                  a.EffectiveDate_stg                          AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                         AS ExpirationDate ,
                                                                                                  a.updatetime_stg                             AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm) )polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id ,
                                                                                PolicyNumber_stg             AS PolicyNumber ,
                                                                                PeriodStart_stg              AS PeriodStart ,
                                                                                PeriodEnd_stg                AS PeriodEnd,
                                                                                branchnumber_stg ,
                                                                                STATUS_stg            AS STATUS ,
                                                                                JobID_stg             AS JobID ,
                                                                                PublicID_stg          AS PublicID ,
                                                                                Createtime_stg        AS Createtime ,
                                                                                updatetime_stg        AS updatetime ,
                                                                                Retired_stg           AS Retired ,
                                                                                PolicyID_stg          AS PolicyID ,
                                                                                EditEffectiveDate_stg AS EditEffectiveDate
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pp
                                                  ON              pp.id = polcov.BranchID
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus pps
                                                  ON              pps.id_stg = pp.STATUS
                                                  INNER JOIN      DB_T_PROD_STAG.pc_job pj
                                                  ON              pj.id_stg=pp.JobID
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job pcj
                                                  ON              pcj.id_stg=pj.Subtype_stg
                                                  WHERE           pps.typecode_stg = ''Bound''
                                                  AND             pp.updatetime > (:start_dttm)
                                                  AND             pp.updatetime <= (:end_dttm))
                                  /*EIM-48971- FARM CHANGES ENDS*/
                                  ,TEMP_FORM AS
                                  (
                                                  /*  EIM-34747 Dwelling DB_T_CORE_DM_PROD.ENDORSEMENT & personalvehicle DB_T_CORE_DM_PROD.ENDORSEMENT & holinesched DB_T_CORE_DM_PROD.ENDORSEMENT */
                                                  SELECT DISTINCT pc_policyperiod.PUBLICID    AS PublicID_stg,
                                                                  pc_policyperiod.PeriodStart AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate IS NULL THEN pc_policyperiod.PeriodStart
                                                                                  ELSE polcov.EffectiveDate
                                                                  END AS feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate IS NULL THEN pc_policyperiod.PeriodEnd
                                                                                  ELSE polcov.ExpirationDate
                                                                  END                                              AS feature_end_dt,
                                                                  CAST(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS Cntrct_role,
                                                                  nk_public_id,
                                                                  CAST(''FEAT_SBTYPE15'' AS VARCHAR(50)) AS FEAT_SBTYPE_CD,
                                                                  CASE
                                                                                  WHEN polcov.assettype_stg IN (''dwelling_hoe'') THEN CAST(''PRTY_ASSET_SBTYPE5'' AS            VARCHAR(100))
                                                                                  WHEN polcov.assettype_stg IN ( ''personalvehicle'') THEN CAST(''PRTY_ASSET_SBTYPE4'' AS        VARCHAR(100))
                                                                                  WHEN polcov.assettype_stg IN (''holineschedcovitem_alfa'') THEN CAST(''PRTY_ASSET_SBTYPE5'' AS VARCHAR(100))
                                                                  END AS assettype,
                                                                  CASE
                                                                                  WHEN polcov.assettype_stg IN (''dwelling_hoe'') THEN CAST(''PRTY_ASSET_CLASFCN1'' AS        VARCHAR(255))
                                                                                  WHEN polcov.assettype_stg IN ( ''personalvehicle'' ) THEN CAST(''PRTY_ASSET_CLASFCN3'' AS   VARCHAR(255))
                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN CAST(polcov.choiceterm1_stg AS VARCHAR(255))
                                                                  END AS classification_code,
                                                                  CASE
                                                                                  WHEN polcov.assettype_stg=''dwelling_hoe'' THEN pol.fixedid_stg
                                                                                  WHEN polcov.assettype_stg=''personalvehicle'' THEN pol.fixedid_stg
                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN pol.fixedid_stg
                                                                  END AS fixedid,
                                                                  CASE
                                                                                  WHEN polcov.assettype_stg=''dwelling_hoe'' THEN pc_policyperiod.EditEffectiveDate
                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN pc_policyperiod.EditEffectiveDate
                                                                                  WHEN polcov.assettype_stg=''personalvehicle'' THEN pc_policyperiod.EditEffectiveDate
                                                                  END                         AS asset_start_dt,
                                                                  polcov.updatetime           AS updatetime_stg,
                                                                  ''SRC_SYS4''                  AS SRC_CD,
                                                                  CAST(NULL AS VARCHAR(60))   AS RateSymbolCollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))   AS RateSymbol_alfa_stg,
                                                                  pc_policyperiod.Retired     AS Retired,
                                                                  CAST(NULL AS VARCHAR(255))  AS feat_val,
                                                                  CAST(NULL AS DECIMAL(14,4)) AS feat_rate,
                                                                  CAST(NULL AS VARCHAR(5))    AS Eligible,
                                                                  CAST(NULL AS VARCHAR(255))  AS FEAT_COVTERMTYPE,
                                                                  CAST(NULL AS VARCHAR(50))   AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                                  /*  Dwelling DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS        VARCHAR(255)) AS BranchId,
                                                                                                  a.Dwelling_stg                              AS assetkey,
                                                                                                  CAST( ''dwelling_hoe'' AS VARCHAR(255))          assettype_stg ,
                                                                                                  a.createtime_stg                            AS createtime ,
                                                                                                  a.EffectiveDate_stg                         AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                        AS ExpirationDate ,
                                                                                                  a.updatetime_stg                            AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_dwellingcov_hoe a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  /* Personalvehicle DB_T_CORE_DM_PROD.Coverage */
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS        VARCHAR(255)) AS BranchId,
                                                                                                  a.PersonalVehicle_stg                       AS assetkey,
                                                                                                  CAST( ''personalvehicle'' AS VARCHAR(255))       assettype_stg ,
                                                                                                  a.createtime_stg                            AS createtime ,
                                                                                                  a.EffectiveDate_stg                         AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                        AS ExpirationDate ,
                                                                                                  a.updatetime_stg                            AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(NULL AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pc_personalvehiclecov a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                  WHERE           ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm)
                                                                                  /* holineschcovitemcov_alfa DB_T_CORE_DM_PROD.ENDORSEMENT */
                                                                                  UNION
                                                                                  SELECT DISTINCT CAST(d.formpatterncode_stg AS VARCHAR(255))      AS nk_public_id,
                                                                                                  CAST(a.BranchID_stg AS        VARCHAR(255))      AS BranchId,
                                                                                                  a.HOLineSchCovItem_stg                           AS assetkey,
                                                                                                  CAST( ''holineschedcovitem_alfa'' AS VARCHAR(255))    assettype_stg ,
                                                                                                  a.createtime_stg                                 AS createtime ,
                                                                                                  a.EffectiveDate_stg                              AS EffectiveDate ,
                                                                                                  a.ExpirationDate_stg                             AS ExpirationDate ,
                                                                                                  a.updatetime_stg                                 AS updatetime,
                                                                                                  a.patterncode_stg,
                                                                                                  e.CoverageSubtype_stg,
                                                                                                  CAST(g.choiceterm1_stg AS VARCHAR(255)) AS Choiceterm1_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa a
                                                                                  JOIN            DB_T_PROD_STAG.pc_policyperiod b
                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus pps
                                                                                  ON              b.status_stg = pps.id_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_formpattern c
                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_form d
                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                  JOIN            DB_T_PROD_STAG.pc_etlclausepattern e
                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                  JOIN            DB_T_PROD_STAG.pctl_documenttype pd
                                                                                  ON              pd.id_stg = c.DocumentType_stg
                                                                                  JOIN            DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa g
                                                                                  ON              g.HOLineSchCovItem_stg = a.HOLineSchCovItem_stg
                                                                                  AND             g.branchid_stg = a.branchid_stg
                                                                                  AND             g.expirationdate_stg IS NULL
                                                                                  AND             g.ChoiceTerm1_stg IS NOT NULL
                                                                                  WHERE           pd.typecode_stg = ''endorsement_alfa''
                                                                                  AND             ( (
                                                                                                                                  a.EffectiveDate_stg IS NULL)
                                                                                                  OR             (
                                                                                                                                  a.EffectiveDate_stg > b.ModelDate_stg
                                                                                                                  AND             COALESCE( a.EffectiveDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) <> COALESCE(a.ExpirationDate_stg,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))))
                                                                                  AND             c.Retired_stg = 0
                                                                                  AND             d.RemovedorSuperseded_stg IS NULL
                                                                                  AND             b.UpdateTime_stg > (:start_dttm)
                                                                                  AND             b.UpdateTime_stg <= (:end_dttm) )polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id ,
                                                                                PolicyNumber_stg             AS PolicyNumber ,
                                                                                PeriodStart_stg              AS PeriodStart ,
                                                                                PeriodEnd_stg                AS PeriodEnd,
                                                                                branchnumber_stg ,
                                                                                STATUS_stg            AS STATUS ,
                                                                                JobID_stg             AS JobID ,
                                                                                PublicID_stg          AS PublicID ,
                                                                                Createtime_stg        AS Createtime ,
                                                                                updatetime_stg        AS updatetime ,
                                                                                Retired_stg           AS Retired ,
                                                                                PolicyID_stg          AS PolicyID ,
                                                                                EditEffectiveDate_stg AS EditEffectiveDate
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod
                                                  ON              pc_policyperiod.id = polcov.BranchID
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.STATUS
                                                  LEFT OUTER JOIN
                                                                  (
                                                                                  SELECT DISTINCT FixedID_stg                                     AS Fixedid_stg,
                                                                                                  CAST(''holineschedcovitem_alfa'' AS VARCHAR(255))    assettype_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_holineschedcovitem_alfa
                                                                                  WHERE           ExpirationDate_stg IS NULL
                                                                                  UNION
                                                                                  SELECT DISTINCT Fixedid_stg                          AS Fixedid_stg,
                                                                                                  CAST(''dwelling_hoe'' AS VARCHAR(255))    assettype_stg
                                                                                  FROM            DB_T_PROD_STAG.pcx_dwelling_hoe
                                                                                  WHERE           ExpirationDate_stg IS NULL
                                                                                  UNION
                                                                                  SELECT DISTINCT FixedID_stg                             AS Fixedid_stg,
                                                                                                  CAST(''personalvehicle'' AS VARCHAR(255))    assettype_stg
                                                                                  FROM            DB_T_PROD_STAG.pc_personalvehicle
                                                                                  WHERE           ExpirationDate_stg IS NULL )pol
                                                  ON              polcov.assetkey=pol.Fixedid_stg
                                                  AND             polcov.assettype_stg=pol.assettype_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pc_job
                                                  ON              pc_job.id_stg=pc_policyperiod.JobID
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                  WHERE           pctl_policyperiodstatus.typecode_stg = ''Bound''
                                                  AND             pc_policyperiod.updatetime > (:start_dttm)
                                                  AND             pc_policyperiod.updatetime <= (:end_dttm) ), TEMP_MODIFIER AS
                                  (
                                                  /*  SQ DB_T_CORE_DM_PROD.policy period modifier */
                                                  SELECT DISTINCT pp.publicid_stg,
                                                                  pp.periodstart_stg                               AS pol_start_dt,
                                                                  COALESCE(t.effectivedate_stg,pp.periodstart_stg) AS feature_start_dt,
                                                                  pp.periodend_stg                                 AS feature_end_dt,
                                                                  CAST(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                  t.patterncode_stg                                AS nk_public_id,
                                                                  CAST(''MODIFIER'' AS VARCHAR(50))                  AS feat_sbtype_cd,
                                                                  CASE
                                                                                  WHEN t.assettype IN (''dwelling_hoe'') THEN ''PRTY_ASSET_SBTYPE5''
                                                                                  WHEN t.assettype IN (''personalvehicle'') THEN ''PRTY_ASSET_SBTYPE4''
                                                                  END AS assettype,
                                                                  CASE
                                                                                  WHEN t.assettype=''dwelling_hoe'' THEN ''PRTY_ASSET_CLASFCN1''
                                                                                  WHEN t.assettype=''personalvehicle'' THEN ''PRTY_ASSET_CLASFCN3''
                                                                  END        AS classification_code,
                                                                  t.assetkey AS fixedid,
                                                                  CASE
                                                                                  WHEN t.assettype=''dwelling_hoe'' THEN COALESCE(d.effectivedate_stg,pp.periodstart_stg)
                                                                                  WHEN t.assettype=''personalvehicle'' THEN COALESCE(e.effectivedate_stg,pp.periodstart_stg)
                                                                  END AS asset_start_dt,
                                                                  t.updatetime_stg ,
                                                                  ''SRC_SYS4''                                AS src_cd,
                                                                  CAST(NULL AS VARCHAR(60))                 AS ratesymbolcollision_alfa_stg,
                                                                  CAST(NULL AS VARCHAR(60))                 AS ratesymbol_alfa_stg,
                                                                  pp.retired_stg                            AS retired,
                                                                  CAST(NULL AS               VARCHAR(255))  AS feat_val,
                                                                  CAST(t.ratemodifier_stg AS DECIMAL(14,4)) AS feat_rate,
                                                                  CAST(t.eligible_stg AS     VARCHAR(5))    AS eligible,
                                                                  CAST(NULL AS               VARCHAR(255))  AS feat_covtermtype,
                                                                  CAST(c.typecode_stg AS     VARCHAR(50))   AS discountsurcharge_alfa_typecd
                                                  FROM            (
                                                                             SELECT     a.branchid_stg,
                                                                                        a.patterncode_stg,
                                                                                        CAST(''dwelling_hoe'' AS VARCHAR(250))AS assettype,
                                                                                        dwelling_stg                        AS assetkey,
                                                                                        a.effectivedate_stg,
                                                                                        a.expirationdate_stg,
                                                                                        a.updatetime_stg,
                                                                                        a.ratemodifier_stg,
                                                                                        a.discountsurcharge_alfa_stg,
                                                                                        a.eligible_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingmodifier_hoe a
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=a.branchid_stg
                                                                             WHERE      expirationdate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION ALL
                                                                             SELECT     a.branchid_stg,
                                                                                        a.patterncode_stg,
                                                                                        ''personalvehicle'' AS assettype,
                                                                                        a.pavehicle_stg   AS assetkey,
                                                                                        a.effectivedate_stg,
                                                                                        a.expirationdate_stg,
                                                                                        a.updatetime_stg,
                                                                                        a.ratemodifier_stg,
                                                                                        a.discountsurcharge_alfa_stg,
                                                                                        a.eligible_stg
                                                                             FROM       DB_T_PROD_STAG.pc_pavehmodifier a
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=a.branchid_stg
                                                                             WHERE      expirationdate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm) ) t
                                                  INNER JOIN      DB_T_PROD_STAG.pc_policyperiod pp
                                                  ON              pp.id_stg=t.branchid_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus b
                                                  ON              b.id_stg=pp.status_stg
                                                  AND             b.typecode_stg=''Bound''
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_discountsurcharge_alfa c
                                                  ON              c.id_stg=t.discountsurcharge_alfa_stg
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT branchid_stg,
                                                                                effectivedate_stg,
                                                                                id_stg,
                                                                                fixedid_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_dwelling_hoe
                                                                         WHERE  expirationdate_stg IS NULL) d
                                                  ON              t.assettype=''dwelling_hoe''
                                                  AND             t.branchid_stg=d.branchid_stg
                                                  AND             t.assetkey=d.fixedid_stg
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT branchid_stg,
                                                                                effectivedate_stg,
                                                                                id_stg,
                                                                                fixedid_stg
                                                                         FROM   DB_T_PROD_STAG.pc_personalvehicle
                                                                         WHERE  expirationdate_stg IS NULL) e
                                                  ON              t.assettype=''personalvehicle''
                                                  AND             t.branchid_stg=e.branchid_stg
                                                  AND             t.assetkey=e.fixedid_stg
                                                  WHERE           (
                                                                                  d.id_stg IS NOT NULL
                                                                  OR              e.id_stg IS NOT NULL)
                                                  AND             pp.UpdateTime_stg > (:start_dttm)
                                                  AND             pp.UpdateTime_stg <= (:end_dttm) ), TEMP_EXCL AS
                                  (
                                                  /* sq_policy_period_exclusions */
                                                  SELECT DISTINCT pc_policyperiod.PUBLICID_stg,
                                                                  pc_policyperiod.PeriodStart_stg AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate_stg IS NULL THEN pc_policyperiod.PeriodStart_stg
                                                                                  ELSE polcov.EffectiveDate_stg
                                                                  END AS feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate_stg IS NULL THEN pc_policyperiod.PeriodEnd_stg
                                                                                  ELSE polcov.ExpirationDate_stg
                                                                  END                                               AS feature_end_dt,
                                                                  CAST (''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN PACKAGE.packagePatternID
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN optn.optionPatternID
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN covterm.clausePatternID
                                                                                  ELSE covterm.covtermPatternID
                                                                  END AS nk_public_id,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN CAST (''PACKAGE'' AS VARCHAR (50))
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN CAST (''OPTIONS'' AS VARCHAR(50))
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN CAST(''CLAUSE'' AS        VARCHAR(50))
                                                                                  ELSE CAST ( ''COVTERM'' AS                                       VARCHAR (50))
                                                                  END AS FEAT_SBTYPE_CD,
                                                                  /*Party Asset Key*/
                                                                  CASE
                                                                                  WHEN polcov.assettype            IN (''dwelling_hoe'') THEN ''PRTY_ASSET_SBTYPE5''
                                                                                  WHEN polcov.assettype            IN (''HOLineSchExclItem_alfa'')
                                                                                  AND             polcov.PatternID IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                       ''HOSI_SpecificOtherStructureExclItem_alfa'') THEN ''PRTY_ASSET_SBTYPE5''
                                                                                  WHEN polcov.assettype IN (''HOLineSchExclItem_alfa'')
                                                                                  AND             polcov.PatternID =''HOSI_ScheduledPropertyItem_alfa'' THEN ''PRTY_ASSET_SBTYPE7''
                                                                                                  /*''REALSP-PP''*/
                                                                                  WHEN polcov.assettype IN (''personalvehicle'',
                                                                                                            ''pawatercraftmotor_alfa'',
                                                                                                            ''pawatercrafttrailer_alfa'') THEN ''PRTY_ASSET_SBTYPE4''
                                                                  END AS assettype ,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN ''PRTY_ASSET_CLASFCN1''
                                                                                  WHEN polcov.assettype=''HOLineSchExclItem_alfa'' THEN polcov.choiceterm1_stg
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN ''PRTY_ASSET_CLASFCN3''
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN ''PRTY_ASSET_CLASFCN4''
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN ''PRTY_ASSET_CLASFCN5''
                                                                  END AS classification_code,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN pcx_dwelling_hoe.fixedid_stg
                                                                                  WHEN polcov.assettype=''HOLineSchExclItem_alfa'' THEN pcx_holineschexclitemexc_alfa.fixedid_stg
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN pc_personalvehicle.fixedid_stg
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN pcx_pawatercraftmotor_alfa.fixedid_stg
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN pcx_pawatercrafttrailer_alfa.fixedid_stg
                                                                  END AS fixedid,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN COALESCE(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''HOLineSchExclItem_alfa'' THEN COALESCE(pcx_holineschexclitemexc_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN COALESCE(pc_personalvehicle.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN COALESCE(pcx_pawatercraftmotor_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN COALESCE(pcx_pawatercrafttrailer_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                  END AS asset_start_dt,
                                                                  polcov.updatetime_stg,
                                                                  ''SRC_SYS4'' AS src_cd,
                                                                  RateSymbolCollision_alfa_stg,
                                                                  RateSymbol_alfa_stg,
                                                                  pc_policyperiod.Retired_stg AS Retired,
                                                                  polcov.val                  AS feat_val,
                                                                  CAST(NULL AS DECIMAL(14,4)) AS feat_rate,
                                                                  CAST(NULL AS VARCHAR(5))    AS Eligible,
                                                                  covterm.CovTermType         AS feat_CovTermType,
                                                                  CAST(NULL AS VARCHAR(50))   AS DiscountSurcharge_alfa_typecd
                                                                  /***feature keys***/
                                                  FROM            (
                                                                             /*pcx_holineschexclitemexc_alfa*/
                                                                             SELECT     CAST(''ChoiceTerm1'' AS   VARCHAR(100)) AS columnname,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS             VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS   VARCHAR(255)) AS assetkey,
                                                                                        CAST(''HOLineSchExclItem_alfa'' AS VARCHAR(255)) AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                        CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm3''                        AS columnname,
                                                                                        CAST(ChoiceTerm3_stg AS VARCHAR(255))AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm4''                         AS columnname,
                                                                                        CAST(ChoiceTerm4_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm1''                         AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''holineschexclitem''                          AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DirectTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''holineschexclitem''                          AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DirectTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm1''                         AS columnname,
                                                                                        CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm1''                        AS columnname,
                                                                                        CAST(StringTerm1_stg AS VARCHAR(255))AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm2''                         AS columnname,
                                                                                        CAST(StringTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm3''                         AS columnname,
                                                                                        CAST(StringTerm3_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DateTerm1''                         AS columnname,
                                                                                        CAST(DateTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid,
                                                                                        pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DateTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255))    val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS           VARCHAR(255)) AS BranchId,
                                                                                        CAST (HOLineSchExclItem_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''HOLineSchExclItem_alfa''                     AS assettype,
                                                                                        pcx_holineschexclitemexc_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg     AS patternid ,
                                                                                        DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschexclitemexc_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschexclitemexc_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm1Avl_stg IS NULL
                                                                             AND        ChoiceTerm2Avl_stg IS NULL
                                                                             AND        ChoiceTerm3Avl_stg IS NULL
                                                                             AND        ChoiceTerm4Avl_stg IS NULL
                                                                             AND        DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        BooleanTerm1Avl_stg IS NULL
                                                                             AND        StringTerm1Avl_stg IS NULL
                                                                             AND        StringTerm2Avl_stg IS NULL
                                                                             AND        StringTerm3Avl_stg IS NULL
                                                                             AND        DateTerm1Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255))    val,
                                                                                        patterncode_stg,
                                                                                        CAST(BranchID_stg AS         VARCHAR(255)) AS BranchId,
                                                                                        CAST (personalvehicle_stg AS VARCHAR(255)) AS assetkey,
                                                                                        ''personalvehicle''                          AS assettype,
                                                                                        pcx_pavehicleexclusion_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS VARCHAR(255))        AS ChoiceTerm1_stg ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_pavehicleexclusion_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pavehicleexclusion_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pavehicleexclusion_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_pavehicleexclusion_alfa.PatternCode_stg
                                                                             WHERE      pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                                        /***/
                                                                  ) polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT CAST(id_stg AS VARCHAR(255)) AS id,
                                                                                PolicyNumber_stg,
                                                                                PeriodStart_stg,
                                                                                PeriodEnd_stg,
                                                                                MostRecentModel_stg,
                                                                                Status_stg,
                                                                                JOBID_stg,
                                                                                PUBLICID_stg,
                                                                                updatetime_stg,
                                                                                Retired_stg
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod
                                                  ON              pc_policyperiod.id = polcov.BranchID
                                                  LEFT JOIN
                                                                  (
                                                                         SELECT CAST(pc_etlclausepattern.PatternID_stg AS    VARCHAR(255)) AS clausePatternID,
                                                                                CAST(pc_etlcovtermpattern.PatternID_stg AS   VARCHAR(255)) AS covtermPatternID,
                                                                                CAST(pc_etlcovtermpattern.ColumnName_stg AS  VARCHAR(255)) AS columnname,
                                                                                CAST(pc_etlcovtermpattern.CovTermType_stg AS VARCHAR(100)) AS covtermtype,
                                                                                CAST(pc_etlclausepattern.name_stg AS         VARCHAR(255)) AS clausename
                                                                         FROM   DB_T_PROD_STAG.pc_etlclausepattern
                                                                         JOIN   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                         ON     pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg
                                                                         UNION
                                                                         SELECT    pc_etlclausepattern.PatternID_stg                       AS clausePatternID,
                                                                                   pc_etlcovtermpattern.PatternID_stg                      AS covtermPatternID,
                                                                                   COALESCE(pc_etlcovtermpattern.ColumnName_stg,''Clause'')  AS columnname,
                                                                                   COALESCE(pc_etlcovtermpattern.CovTermType_stg,''Clause'') AS covtermtype,
                                                                                   pc_etlclausepattern.name_stg                            AS clausename
                                                                         FROM      DB_T_PROD_STAG.pc_etlclausepattern
                                                                         LEFT JOIN
                                                                                   (
                                                                                          SELECT *
                                                                                          FROM   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                                          WHERE  Name_stg NOT LIKE ''ZZ%'') pc_etlcovtermpattern
                                                                         ON        pc_etlcovtermpattern.ClausePatternID_stg=pc_etlclausepattern.ID_stg
                                                                         WHERE     pc_etlclausepattern.Name_stg NOT LIKE ''ZZ%''
                                                                         AND       pc_etlcovtermpattern.Name_stg IS NULL
                                                                         AND       OwningEntityType_stg IN (''HOLineSchCovItem_alfa'',
                                                                                                            ''HomeownersLine_HOE'',
                                                                                                            ''Dwelling_HOE'',
                                                                                                            ''PersonalVehicle'',
                                                                                                            ''PersonalAutoLine'',
                                                                                                            ''HOLineSchExclItem_alfa'' ) ) covterm
                                                  ON              covterm.clausePatternID=polcov.PatternCode_stg
                                                  AND             covterm.ColumnName=polcov.columnname
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT pc_etlcovtermpackage.PatternID_stg   AS packagePatternID,
                                                                                pc_etlcovtermpackage.PackageCode_stg AS cov_id,
                                                                                pc_etlcovtermpackage.PackageCode_stg AS name
                                                                         FROM   DB_T_PROD_STAG.pc_etlcovtermpackage ) PACKAGE
                                                  ON              PACKAGE.packagePatternID=polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                             SELECT     pc_etlcovtermoption.PatternID_stg  AS optionPatternID,
                                                                                        pc_etlcovtermoption.optioncode_stg AS name,
                                                                                        pc_etlcovtermoption.Value_stg,
                                                                                        pc_etlcovtermpattern.ValueType_stg
                                                                             FROM       DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                             INNER JOIN DB_T_PROD_STAG.pc_etlcovtermoption
                                                                             ON         pc_etlcovtermpattern.id_stg=pc_etlcovtermoption.CoverageTermPatternID_stg ) optn
                                                  ON              optn.optionPatternID=polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT Fixedid_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_dwelling_hoe
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_dwelling_hoe
                                                                  /* replace pcx_dwelling_hoe.id with pcx_dwelling_hoe.FixedId for defect 21470 */
                                                  ON              CAST( pcx_dwelling_hoe.Fixedid_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''dwelling_hoe''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                Fixedid_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_holineschedcovitem_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_holineschedcovitem_alfa
                                                  ON              CAST (pcx_holineschedcovitem_alfa.id_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''holineschedcovitem_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT HOLineSchCovItem_stg,
                                                                                Fixedid_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_holineschcovitemcov_alfa
                                                  ON              CAST (pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''holineschedcovitem_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT HOLineSchExclItem_stg,
                                                                                Fixedid_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_holineschexclitemexc_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_holineschexclitemexc_alfa
                                                  ON              CAST (pcx_holineschexclitemexc_alfa.HOLineSchExclItem_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''HOLineSchExclItem_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT FixedID_stg,
                                                                                EffectiveDate_stg,
                                                                                RateSymbolCollision_alfa_stg ,
                                                                                RateSymbol_alfa_stg
                                                                         FROM   DB_T_PROD_STAG.pc_personalvehicle
                                                                         WHERE  ExpirationDate_stg IS NULL) pc_personalvehicle
                                                  ON              CAST(pc_personalvehicle.FixedID_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''personalvehicle''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                FixedID_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_pawatercraftmotor_alfa
                                                  ON              CAST (pcx_pawatercraftmotor_alfa.id_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''pawatercraftmotor_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                FixedID_stg,
                                                                                EffectiveDate_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_pawatercrafttrailer_alfa
                                                  ON              CAST( pcx_pawatercrafttrailer_alfa.id_stg AS VARCHAR(50)) =polcov.assetkey
                                                  AND             assettype=''pawatercrafttrailer_alfa''
                                                  JOIN            DB_T_PROD_STAG.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg
                                                  JOIN            DB_T_PROD_STAG.pc_job
                                                  ON              pc_job.id_stg=pc_policyperiod.JobID_stg
                                                  JOIN            DB_T_PROD_STAG.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                  WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                  AND             pctl_policyperiodstatus.TYPECODE_stg=''Bound''
                                                  AND             pc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                  AND             pc_policyperiod.UpdateTime_stg <= (:end_dttm) ), TEMP_COV AS
                                  (
                                                  /* sq_policyperiod_coverage */
                                                  SELECT DISTINCT pc_policyperiod.PUBLICID_stg,
                                                                  pc_policyperiod.PeriodStart_stg AS pol_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.EffectiveDate_stg IS NULL THEN pc_policyperiod.PeriodStart_stg
                                                                                  ELSE polcov.EffectiveDate_stg
                                                                  END AS feature_start_dt,
                                                                  CASE
                                                                                  WHEN polcov.ExpirationDate_stg IS NULL THEN pc_policyperiod.PeriodEnd_stg
                                                                                  ELSE polcov.ExpirationDate_stg
                                                                  END                                               AS feature_end_dt,
                                                                  CAST (''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                  /***feature keys***/
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN PACKAGE.packagePatternID
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN optn.optionPatternID
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN covterm.clausePatternID
                                                                                  ELSE covterm.covtermPatternID
                                                                  END AS nk_public_id,
                                                                  CASE
                                                                                  WHEN covterm.CovTermType=''package'' THEN CAST (''PACKAGE'' AS VARCHAR (50))
                                                                                  WHEN covterm.CovTermType=''option''
                                                                                  AND             polcov.val IS NOT NULL THEN CAST (''OPTIONS'' AS VARCHAR(50))
                                                                                  WHEN covterm.CovTermType=''Clause'' THEN CAST(''CLAUSE'' AS        VARCHAR(50))
                                                                                  ELSE CAST ( ''COVTERM'' AS                                       VARCHAR (50))
                                                                  END AS FEAT_SBTYPE_CD,
                                                                  /*Party Asset Key*/
                                                                  CASE
                                                                                  WHEN polcov.assettype            IN (''dwelling_hoe'') THEN ''PRTY_ASSET_SBTYPE5''
                                                                                  WHEN polcov.assettype            IN (''holineschedcovitem_alfa'')
                                                                                  AND             polcov.PatternID IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                       ''HOSI_SpecificOtherStructureExclItem_alfa'') THEN CAST(''PRTY_ASSET_SBTYPE5'' AS VARCHAR(100))
                                                                                  WHEN polcov.assettype IN (''holineschedcovitem_alfa'')
                                                                                  AND             polcov.PatternID =''HOSI_ScheduledPropertyItem_alfa'' THEN CAST(''PRTY_ASSET_SBTYPE7'' AS VARCHAR(100))
                                                                                                  /*''REALSP-PP''*/
                                                                                  WHEN polcov.assettype IN (''personalvehicle'',
                                                                                                            ''pawatercraftmotor_alfa'',
                                                                                                            ''pawatercrafttrailer_alfa'') THEN CAST(''PRTY_ASSET_SBTYPE4'' AS VARCHAR(100))
                                                                  END AS assettype,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN CAST(''PRTY_ASSET_CLASFCN1'' AS             VARCHAR(255))
                                                                                  WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN CAST(polcov.choiceterm1 AS     VARCHAR(255))
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN CAST(''PRTY_ASSET_CLASFCN3'' AS          VARCHAR(255))
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN CAST(''PRTY_ASSET_CLASFCN4'' AS   VARCHAR(255))
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN CAST(''PRTY_ASSET_CLASFCN5'' AS VARCHAR(255))
                                                                  END AS classification_code,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN pcx_dwelling_hoe.fixedid_stg
                                                                                  WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN pcx_holineschedcovitem_alfa.fixedid_stg
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN pc_personalvehicle.fixedid_stg
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN pcx_pawatercraftmotor_alfa.fixedid_stg
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN pcx_pawatercrafttrailer_alfa.fixedid_stg
                                                                  END AS fixedid,
                                                                  CASE
                                                                                  WHEN polcov.assettype=''dwelling_hoe'' THEN COALESCE(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN COALESCE(pcx_holineschedcovitem_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''personalvehicle'' THEN COALESCE(pc_personalvehicle.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN COALESCE(pcx_pawatercraftmotor_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                                  WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN COALESCE(pcx_pawatercrafttrailer_alfa.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)
                                                                  END AS asset_start_dt,
                                                                  polcov.updatetime_stg,
                                                                  ''SRC_SYS4'' AS src_cd,
                                                                  RateSymbolCollision_alfa_stg,
                                                                  RateSymbol_alfa_stg,
                                                                  pc_policyperiod.Retired_stg AS Retired,
                                                                  polcov.val                  AS feat_val,
                                                                  CAST(NULL AS DECIMAL(14,4)) AS feat_rate,
                                                                  CAST(NULL AS VARCHAR(5))    AS Eligible,
                                                                  covterm.CovTermType         AS feat_CovTermType ,
                                                                  CAST(NULL AS VARCHAR(50))   AS DiscountSurcharge_alfa_typecd
                                                  FROM            (
                                                                             /*pcx_dwellingcov_hoe*/
                                                                             SELECT     CAST(''ChoiceTerm1'' AS   VARCHAR(100)) AS columnname,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        CAST(patterncode_stg AS VARCHAR(255)) AS patterncode_stg ,
                                                                                        BranchID_stg                          AS BranchId,
                                                                                        Dwelling_stg                          AS assetkey,
                                                                                        CAST(''dwelling_hoe'' AS VARCHAR(100))  AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''   AS columnname,
                                                                                        ChoiceTerm2_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm3''   AS columnname,
                                                                                        ChoiceTerm3_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm4''   AS columnname,
                                                                                        ChoiceTerm4_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm5''   AS columnname,
                                                                                        ChoiceTerm5_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm5Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm1''                         AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      DirectTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm3''                         AS columnname,
                                                                                        CAST(DirectTerm3_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      DirectTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm4''                         AS columnname,
                                                                                        CAST(DirectTerm4_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      DirectTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm1''                         AS columnname,
                                                                                        CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      BooleanTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm2''                         AS columnname,
                                                                                        CAST(BooleanTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      BooleanTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg   AS BranchId,
                                                                                        Dwelling_stg   AS assetkey,
                                                                                        ''dwelling_hoe'' AS assettype,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_dwellingcov_hoe
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_dwellingcov_hoe.branchid_stg
                                                                             WHERE      ChoiceTerm1Avl_stg IS NULL
                                                                             AND        ChoiceTerm2Avl_stg IS NULL
                                                                             AND        ChoiceTerm3Avl_stg IS NULL
                                                                             AND        ChoiceTerm4Avl_stg IS NULL
                                                                             AND        ChoiceTerm5Avl_stg IS NULL
                                                                             AND        DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        DirectTerm3Avl_stg IS NULL
                                                                             AND        DirectTerm4Avl_stg IS NULL
                                                                             AND        BooleanTerm2Avl_stg IS NULL
                                                                             AND        BooleanTerm1Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             /*pcx_holineschcovitemcov_alfa*/
                                                                             SELECT     CAST(''ChoiceTerm1'' AS    VARCHAR(100))          AS columnname,
                                                                                        CAST( ChoiceTerm1_stg AS VARCHAR(255))          AS val,
                                                                                        CAST(patterncode_stg AS  VARCHAR(255))          AS patterncode_stg ,
                                                                                        BranchID_stg                                    AS BranchId,
                                                                                        HOLineSchCovItem_stg                            AS assetkey,
                                                                                        CAST(''holineschedcovitem_alfa'' AS VARCHAR(100)) AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''   AS columnname,
                                                                                        ChoiceTerm2_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm3''   AS columnname,
                                                                                        ChoiceTerm3_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm4''   AS columnname,
                                                                                        ChoiceTerm4_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm1''                         AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DirectTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DirectTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm1''                         AS columnname,
                                                                                        CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm2''                         AS columnname,
                                                                                        CAST(BooleanTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm3''                         AS columnname,
                                                                                        CAST(BooleanTerm3_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm4''                         AS columnname,
                                                                                        CAST(BooleanTerm4_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm5''                         AS columnname,
                                                                                        CAST(BooleanTerm5_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        BooleanTerm5Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm1''   AS columnname,
                                                                                        StringTerm1_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm2''   AS columnname,
                                                                                        StringTerm2_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm3''   AS columnname,
                                                                                        StringTerm3_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm3Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''StringTerm4''   AS columnname,
                                                                                        StringTerm4_stg AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        StringTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DateTerm1''                         AS columnname,
                                                                                        CAST(DateTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DateTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DateTerm4''                         AS columnname,
                                                                                        CAST(DateTerm4_stg AS VARCHAR(255))    val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        DateTerm4Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255))    val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg              AS BranchId,
                                                                                        HOLineSchCovItem_stg      AS assetkey,
                                                                                        ''holineschedcovitem_alfa'' AS assettype,
                                                                                        pcx_holineschcovitemcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        ChoiceTerm1_stg                   AS choiceterm1 ,
                                                                                        pc_etlclausepattern.PatternID_stg AS patternid ,
                                                                                        pcx_holineschcovitemcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_holineschcovitemcov_alfa.branchid_stg
                                                                             LEFT JOIN  DB_T_PROD_STAG.pc_etlclausepattern
                                                                             ON         pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg
                                                                             WHERE      pc_etlclausepattern.PatternID_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                              ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                              ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                             AND        ChoiceTerm1Avl_stg IS NULL
                                                                             AND        ChoiceTerm2Avl_stg IS NULL
                                                                             AND        ChoiceTerm3Avl_stg IS NULL
                                                                             AND        ChoiceTerm4Avl_stg IS NULL
                                                                             AND        ChoiceTerm5Avl_stg IS NULL
                                                                             AND        ChoiceTerm6Avl_stg IS NULL
                                                                             AND        DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        BooleanTerm1Avl_stg IS NULL
                                                                             AND        BooleanTerm2Avl_stg IS NULL
                                                                             AND        BooleanTerm3Avl_stg IS NULL
                                                                             AND        BooleanTerm4Avl_stg IS NULL
                                                                             AND        BooleanTerm5Avl_stg IS NULL
                                                                             AND        StringTerm1Avl_stg IS NULL
                                                                             AND        StringTerm2Avl_stg IS NULL
                                                                             AND        StringTerm3Avl_stg IS NULL
                                                                             AND        StringTerm4Avl_stg IS NULL
                                                                             AND        DateTerm1Avl_stg IS NULL
                                                                             AND        DateTerm4Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             /**pc_personalvehiclecov***/
                                                                             SELECT     CAST(''ChoiceTerm1'' AS   VARCHAR(100))   AS columnname,
                                                                                        CAST(ChoiceTerm1_stg AS VARCHAR(255))   AS val,
                                                                                        CAST(patterncode_stg AS VARCHAR(255))   AS patterncode_stg ,
                                                                                        BranchID_stg                            AS BranchId,
                                                                                        PersonalVehicle_stg                     AS assetkey,
                                                                                        CAST(''personalvehicle'' AS VARCHAR(100)) AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      ChoiceTerm1Avl_stg =1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                        CAST(ChoiceTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg        AS BranchId,
                                                                                        PersonalVehicle_stg AS assetkey,
                                                                                        ''personalvehicle''   AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      ChoiceTerm2Avl_stg =1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm1''                         AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg        AS BranchId,
                                                                                        PersonalVehicle_stg AS assetkey,
                                                                                        ''personalvehicle''   AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg =1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg        AS BranchId,
                                                                                        PersonalVehicle_stg AS assetkey,
                                                                                        ''personalvehicle''   AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST( NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS  VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      DirectTerm2Avl_stg =1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''BooleanTerm1''                         AS columnname,
                                                                                        CAST(BooleanTerm1_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg        AS BranchId,
                                                                                        PersonalVehicle_stg AS assetkey,
                                                                                        ''personalvehicle''   AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      BooleanTerm1Avl_stg =1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg        AS BranchId,
                                                                                        PersonalVehicle_stg AS assetkey,
                                                                                        ''personalvehicle''   AS assettype,
                                                                                        pc_personalvehiclecov.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST( NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS  VARCHAR(255)) AS patternid ,
                                                                                        pc_personalvehiclecov.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pc_personalvehiclecov
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pc_personalvehiclecov.branchid_stg
                                                                             WHERE      ChoiceTerm1Avl_stg IS NULL
                                                                             AND        ChoiceTerm2Avl_stg IS NULL
                                                                             AND        DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        BooleanTerm1Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             /*pcx_pawatercraftmotorcov_alfa***/
                                                                             SELECT     CAST(''DirectTerm1'' AS   VARCHAR(100))          AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255))          AS val,
                                                                                        CAST(patterncode_stg AS VARCHAR(255))          AS patterncode_stg ,
                                                                                        BranchID_stg                                   AS BranchId,
                                                                                        PAWatercraftMotor_alfa_stg                     AS assetkey,
                                                                                        CAST(''pawatercraftmotor_alfa'' AS VARCHAR(100)) AS assettype,
                                                                                        pcx_pawatercraftmotorcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_pawatercraftmotorcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawatercraftmotorcov_alfa.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg               AS BranchId,
                                                                                        PAWatercraftMotor_alfa_stg AS assetkey,
                                                                                        ''pawatercraftmotor_alfa''   AS assettype,
                                                                                        pcx_pawatercraftmotorcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_pawatercraftmotorcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawatercraftmotorcov_alfa.branchid_stg
                                                                             WHERE      DirectTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg               AS BranchId,
                                                                                        PAWatercraftMotor_alfa_stg AS assetkey,
                                                                                        ''pawatercraftmotor_alfa''   AS assettype,
                                                                                        pcx_pawatercraftmotorcov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        CAST(NULL AS VARCHAR(255))    patternid ,
                                                                                        pcx_pawatercraftmotorcov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawatercraftmotorcov_alfa.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             /*pcx_pawctrailercov_alfa*/
                                                                             SELECT     CAST(''DirectTerm1'' AS   VARCHAR(100))            AS columnname,
                                                                                        CAST(DirectTerm1_stg AS VARCHAR(255))            AS val,
                                                                                        CAST(patterncode_stg AS VARCHAR(255))            AS patterncode_stg ,
                                                                                        BranchID_stg                                     AS BranchId,
                                                                                        PAWatercraftTrailer_alfa_stg                     AS assetkey,
                                                                                        CAST(''pawatercrafttrailer_alfa'' AS VARCHAR(100)) AS assettype,
                                                                                        pcx_pawctrailercov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg,
                                                                                        CAST(NULL AS  VARCHAR(255)) AS choiceterm1,
                                                                                        CAST( NULL AS VARCHAR(255)) AS patternid ,
                                                                                        pcx_pawctrailercov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawctrailercov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawctrailercov_alfa.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        CAST(DirectTerm2_stg AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg                 AS BranchId,
                                                                                        PAWatercraftTrailer_alfa_stg AS assetkey,
                                                                                        ''pawatercrafttrailer_alfa''   AS assettype,
                                                                                        pcx_pawctrailercov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS  VARCHAR(255)) AS choiceterm1,
                                                                                        CAST( NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_pawctrailercov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawctrailercov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawctrailercov_alfa.branchid_stg
                                                                             WHERE      DirectTerm2Avl_stg=1
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm)
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        CAST(NULL AS VARCHAR(255)) AS val,
                                                                                        patterncode_stg,
                                                                                        BranchID_stg                 AS BranchId,
                                                                                        PAWatercraftTrailer_alfa_stg AS assetkey,
                                                                                        ''pawatercrafttrailer_alfa''   AS assettype,
                                                                                        pcx_pawctrailercov_alfa.createtime_stg,
                                                                                        EffectiveDate_stg,
                                                                                        ExpirationDate_stg ,
                                                                                        CAST(NULL AS  VARCHAR(255)) AS choiceterm1,
                                                                                        CAST( NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_pawctrailercov_alfa.updatetime_stg
                                                                             FROM       DB_T_PROD_STAG.pcx_pawctrailercov_alfa
                                                                             INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp
                                                                             ON         pp.id_stg=pcx_pawctrailercov_alfa.branchid_stg
                                                                             WHERE      DirectTerm1Avl_stg IS NULL
                                                                             AND        DirectTerm2Avl_stg IS NULL
                                                                             AND        ExpirationDate_stg IS NULL
                                                                             AND        pp.UpdateTime_stg > (:start_dttm)
                                                                             AND        pp.UpdateTime_stg <= (:end_dttm) ) polcov
                                                  INNER JOIN
                                                                  (
                                                                         SELECT id_stg AS id,
                                                                                PolicyNumber_stg,
                                                                                PeriodStart_stg,
                                                                                PeriodEnd_stg,
                                                                                MostRecentModel_stg,
                                                                                Status_stg,
                                                                                JOBID_stg,
                                                                                PUBLICID_stg,
                                                                                updatetime_stg,
                                                                                Retired_stg
                                                                         FROM   DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod
                                                  ON              pc_policyperiod.id = polcov.BranchID
                                                  LEFT JOIN
                                                                  (
                                                                         SELECT CAST(pc_etlclausepattern.PatternID_stg AS    VARCHAR(255)) AS clausePatternID,
                                                                                CAST(pc_etlcovtermpattern.PatternID_stg AS   VARCHAR(255)) AS covtermPatternID,
                                                                                CAST(pc_etlcovtermpattern.ColumnName_stg AS  VARCHAR(255)) AS ColumnName ,
                                                                                CAST(pc_etlcovtermpattern.CovTermType_stg AS VARCHAR(255)) AS CovTermType,
                                                                                CAST(pc_etlclausepattern.name_stg AS         VARCHAR(255)) AS clausename
                                                                         FROM   DB_T_PROD_STAG.pc_etlclausepattern
                                                                         JOIN   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                         ON     pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg
                                                                         UNION
                                                                         SELECT    pc_etlclausepattern.PatternID_stg                       AS clausePatternID,
                                                                                   pc_etlcovtermpattern.PatternID_stg                      AS covtermPatternID,
                                                                                   COALESCE(pc_etlcovtermpattern.ColumnName_stg,''Clause'')  AS columnname,
                                                                                   COALESCE(pc_etlcovtermpattern.CovTermType_stg,''Clause'') AS covtermtype,
                                                                                   pc_etlclausepattern.name_stg                            AS clausename
                                                                         FROM      DB_T_PROD_STAG.pc_etlclausepattern
                                                                         LEFT JOIN
                                                                                   (
                                                                                          SELECT *
                                                                                          FROM   DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                                          WHERE  Name_stg NOT LIKE ''ZZ%'') pc_etlcovtermpattern
                                                                         ON        pc_etlcovtermpattern.ClausePatternID_stg=pc_etlclausepattern.ID_stg
                                                                         WHERE     pc_etlclausepattern.Name_stg NOT LIKE ''ZZ%''
                                                                         AND       pc_etlcovtermpattern.Name_stg IS NULL
                                                                         AND       OwningEntityType_stg IN (''HOLineSchCovItem_alfa'',
                                                                                                            ''HomeownersLine_HOE'',
                                                                                                            ''Dwelling_HOE'',
                                                                                                            ''PersonalVehicle'',
                                                                                                            ''PersonalAutoLine'' ) ) covterm
                                                  ON              covterm.clausePatternID=polcov.PatternCode_stg
                                                  AND             covterm.ColumnName=polcov.columnname
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT pc_etlcovtermpackage.PatternID_stg   AS packagePatternID,
                                                                                pc_etlcovtermpackage.PackageCode_stg AS cov_id,
                                                                                pc_etlcovtermpackage.PackageCode_stg AS name_stg
                                                                         FROM   DB_T_PROD_STAG.pc_etlcovtermpackage ) PACKAGE
                                                  ON              PACKAGE.packagePatternID=polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                             SELECT     pc_etlcovtermoption.PatternID_stg  AS optionPatternID,
                                                                                        pc_etlcovtermoption.optioncode_stg AS name_stg,
                                                                                        pc_etlcovtermoption.Value_stg      AS value_stg,
                                                                                        pc_etlcovtermpattern.ValueType_stg AS valuetype
                                                                             FROM       DB_T_PROD_STAG.pc_etlcovtermpattern
                                                                             INNER JOIN DB_T_PROD_STAG.pc_etlcovtermoption
                                                                             ON         pc_etlcovtermpattern.id_stg=pc_etlcovtermoption.CoverageTermPatternID_stg ) optn
                                                  ON              optn.optionPatternID=polcov.val
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT Fixedid_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_dwelling_hoe
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_dwelling_hoe
                                                                  /* replace pcx_dwelling_hoe.id with pcx_dwelling_hoe.FixedId for defect 21470 */
                                                  ON              pcx_dwelling_hoe.Fixedid_stg =polcov.assetkey
                                                  AND             assettype=''dwelling_hoe''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                Fixedid_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_holineschedcovitem_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_holineschedcovitem_alfa
                                                  ON              pcx_holineschedcovitem_alfa.id_stg =polcov.assetkey
                                                  AND             assettype=''holineschedcovitem_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT HOLineSchCovItem_stg,
                                                                                Fixedid_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_holineschcovitemcov_alfa
                                                  ON              pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg =polcov.assetkey
                                                  AND             assettype=''holineschedcovitem_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT FixedID_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg,
                                                                                RateSymbolCollision_alfa_stg ,
                                                                                RateSymbol_alfa_stg
                                                                         FROM   DB_T_PROD_STAG.pc_personalvehicle
                                                                         WHERE  ExpirationDate_stg IS NULL) pc_personalvehicle
                                                  ON              pc_personalvehicle.FixedID_stg =polcov.assetkey
                                                  AND             assettype=''personalvehicle''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                Fixedid_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_pawatercraftmotor_alfa
                                                  ON              pcx_pawatercraftmotor_alfa.id_stg =polcov.assetkey
                                                  AND             assettype=''pawatercraftmotor_alfa''
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT id_stg,
                                                                                Fixedid_stg,
                                                                                ExpirationDate_stg,
                                                                                EffectiveDate_stg,
                                                                                BranchID_stg
                                                                         FROM   DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa
                                                                         WHERE  ExpirationDate_stg IS NULL) pcx_pawatercrafttrailer_alfa
                                                  ON              pcx_pawatercrafttrailer_alfa.id_stg=polcov.assetkey
                                                  AND             assettype=''pawatercrafttrailer_alfa''
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pc_job
                                                  ON              pc_job.id_stg=pc_policyperiod.JobID_stg
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                  WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                  AND             pctl_policyperiodstatus.TYPECODE_stg=''Bound''
                                                  AND             (
                                                                                  pc_personalvehicle.BranchID_stg=pc_policyperiod.id
                                                                  OR              pcx_dwelling_hoe.Branchid_stg=pc_policyperiod.id
                                                                  OR              pcx_pawatercraftmotor_alfa.BranchId_stg=pc_policyperiod.id
                                                                  OR              pcx_pawatercrafttrailer_alfa.BranchId_stg=pc_policyperiod.id
                                                                  OR              pcx_holineschcovitemcov_alfa.BranchID_stg=pc_policyperiod.id)
                                                  AND             pc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                  AND             pc_policyperiod.UpdateTime_stg <= (:end_dttm) ),
                                  /* EIM-33116-- Seperate with clause for source Query---- */
                                  AGMT_INS_FEAT AS
                                  (
                                                  SELECT          agmt_lkp.AGMT_ID                                          AS Src_AGMT_ID,
                                                                  feat_lkp.FEAT_ID                                          AS Src_FEAT_ID,
                                                                  PRTY_ASSET_lkp.PRTY_ASSET_ID                              AS Src_PRTY_ASSET_ID,
                                                                  feat_lkp.INSRNC_CVGE_TYPE_CD                              AS Src_INSRNC_CVGE_TYPE_CD,
                                                                  src.feature_start_dt                                      AS Src_feature_strt_dt,
                                                                  src.feature_end_dt                                        AS Src_feature_end_dt,
                                                                  COALESCE(xlat_feat_cntrct_role.TGT_IDNTFTN_VAL,''UNK'')     AS Src_OUT_cntrct_role,
                                                                  src.asset_start_dt                                        AS Src_asset_strt_dt,
                                                                  COALESCE(SRC.UPDATETIME_stg, CAST(''1900-01-01'' AS DATE )) AS Src_UPDTAETIME,
                                                                  src.RateSymbolCollision_alfa_stg                          AS Src_RateSymbolCollision_alfa,
                                                                  src.Retired                                               AS Src_Retired,
                                                                  src.RateSymbol_alfa_stg                                   AS Src_RateSymbol_alfa,
                                                                  COALESCE(xlat_feat_effect_type_cd.TGT_IDNTFTN_VAL,''UNK'')  AS Src_o_DiscountSurcharge_alfa_typecd,
                                                                  src.feat_rate                                             AS Src_polcov_RateModifier,
                                                                  src.eligible                                              AS Src_polcov_Eligible,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  lower(trim(src.FEAT_COVTERMTYPE))) = ''bit''THEN trim(src.FEAT_VAL)
                                                                                  ELSE CAST(NULL AS VARCHAR(20))
                                                                  END AS Src_AGMT_ASSET_FEAT_IND,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  LOWER(TRIM(SRC.FEAT_COVTERMTYPE)) = ''shorttext'')
                                                                                  OR              (
                                                                                                                  LOWER(TRIM(SRC.FEAT_COVTERMTYPE)) =''typekey'') THEN TRIM(SRC.FEAT_VAL)
                                                                  END AS Src_AGMT_ASSET_FEAT_TXT,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''direct'' THEN CAST(SRC.FEAT_VAL AS DECIMAL(18,4))
                                                                                  ELSE NULL
                                                                  END AS Src_AGMT_ASSET_FEAT_AMT,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) =''datetime'' THEN to_date(TRIM(SRC.FEAT_VAL), ''mm/dd/yyyy'')
                                                                                  ELSE NULL
                                                                  END AS Src_AGMT_ASSET_FEAT_DT
                                                  FROM
                                                                  /* -src query start */
                                                                  (
                                                                         SELECT *
                                                                         FROM   (
                                                                                         SELECT   pc_agmt_insrd_asset_feat_x.PublicID_stg,
                                                                                                  pc_agmt_insrd_asset_feat_x.pol_start_dt,
                                                                                                  pc_agmt_insrd_asset_feat_x.feature_start_dt,
                                                                                                  pc_agmt_insrd_asset_feat_x.feature_end_dt,
                                                                                                  pc_agmt_insrd_asset_feat_x.Cntrct_role,
                                                                                                  pc_agmt_insrd_asset_feat_x.nk_public_id,
                                                                                                  pc_agmt_insrd_asset_feat_x.feat_sbtype_cd,
                                                                                                  pc_agmt_insrd_asset_feat_x.assettype,
                                                                                                  pc_agmt_insrd_asset_feat_x.classification_code,
                                                                                                  pc_agmt_insrd_asset_feat_x.fixedid,
                                                                                                  pc_agmt_insrd_asset_feat_x.asset_start_dt,
                                                                                                  pc_agmt_insrd_asset_feat_x.UPDATETIME_stg,
                                                                                                  pc_agmt_insrd_asset_feat_x.SRC_CD,
                                                                                                  pc_agmt_insrd_asset_feat_x.RateSymbolCollision_alfa_stg,
                                                                                                  pc_agmt_insrd_asset_feat_x.RateSymbol_alfa_stg,
                                                                                                  pc_agmt_insrd_asset_feat_x.Retired,
                                                                                                  pc_agmt_insrd_asset_feat_x.FEAT_VAL,
                                                                                                  pc_agmt_insrd_asset_feat_x.FEAT_COVTERMTYPE,
                                                                                                  pc_agmt_insrd_asset_feat_x.feat_rate,
                                                                                                  SUBSTR (pc_agmt_insrd_asset_feat_x.Eligible,1,1) AS eligible,
                                                                                                  DiscountSurcharge_alfa_typecd,
                                                                                                  ROW_NUMBER() OVER(PARTITION BY PublicID_stg,nk_public_id,feat_sbtype_cd,assettype,classification_code,fixedid,Cntrct_role ORDER BY pol_start_dt DESC) AS rankid
                                                                                         FROM     (
                                                                                                         SELECT *
                                                                                                         FROM   TEMP_FORM
                                                                                                         UNION ALL
                                                                                                         SELECT *
                                                                                                         FROM   TEMP_MODIFIER
                                                                                                         UNION ALL
                                                                                                         SELECT *
                                                                                                         FROM   TEMP_EXCL
                                                                                                         UNION ALL
                                                                                                         SELECT *
                                                                                                         FROM   TEMP_COV
                                                                                                         UNION ALL
                                                                                                         SELECT *
                                                                                                         FROM   FARM_TEMP ) pc_agmt_insrd_asset_feat_x )tmp
                                                                         WHERE  rankid=1
                                                                         AND    fixedid IS NOT NULL ) AS SRC
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_CNTRCT_ROLE_SBTYPE''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_feat_cntrct_role
                                                  ON              xlat_feat_cntrct_role.SRC_IDNTFTN_VAL=src.Cntrct_role
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_src_cd
                                                  ON              xlat_src_cd.SRC_IDNTFTN_VAL=src.SRC_CD
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL        AS TGT_IDNTFTN_VAL ,
                                                                                UPPER(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL) AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',
                                                                                                                          ''GW'')
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_asset_clasfcn
                                                  ON              xlat_asset_clasfcn.SRC_IDNTFTN_VAL=UPPER(TRIM(src.classification_code))
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )xlat_asset_sbtype
                                                  ON              xlat_asset_sbtype.SRC_IDNTFTN_VAL=src.assettype
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_SBTYPE''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_feat_sbtype_cd
                                                  ON              xlat_feat_sbtype_cd.SRC_IDNTFTN_VAL= (
                                                                  CASE
                                                                                  WHEN SRC.feat_sbtype_cd = ''MODIFIER'' THEN ''FEAT_SBTYPE11''
                                                                                  WHEN SRC.feat_sbtype_cd = ''OPTIONS'' THEN ''FEAT_SBTYPE8''
                                                                                  WHEN SRC.feat_sbtype_cd = ''COVTERM'' THEN ''FEAT_SBTYPE6''
                                                                                  WHEN SRC.feat_sbtype_cd = ''CLAUSE'' THEN ''FEAT_SBTYPE7''
                                                                                  WHEN SRC.feat_sbtype_cd = ''PACKAGE'' THEN ''FEAT_SBTYPE9''
                                                                                  WHEN SRC.feat_sbtype_cd = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15''
                                                                  END )
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_EFECT_TYPE''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_discountsurcharge_alfa.typecode''
                                                                         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW''
                                                                         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_feat_effect_type_cd
                                                  ON              xlat_feat_effect_type_cd.SRC_IDNTFTN_VAL=src.DiscountSurcharge_alfa_typecd
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT FEAT.FEAT_ID               AS FEAT_ID,
                                                                                FEAT.FEAT_INSRNC_SBTYPE_CD AS FEAT_INSRNC_SBTYPE_CD,
                                                                                FEAT.INSRNC_CVGE_TYPE_CD   AS INSRNC_CVGE_TYPE_CD,
                                                                                FEAT.FEAT_SBTYPE_CD        AS FEAT_SBTYPE_CD,
                                                                                FEAT.NK_SRC_KEY            AS NK_SRC_KEY
                                                                         FROM   DB_T_PROD_CORE.FEAT 
                                                                         WHERE  CAST(FEAT.EDW_END_DTTM AS DATE)=''9999-12-31'' ) AS feat_lkp
                                                  ON              feat_lkp.FEAT_SBTYPE_CD=COALESCE( xlat_feat_sbtype_cd.TGT_IDNTFTN_VAL,''UNK'')
                                                  AND             CAST(feat_lkp.NK_SRC_KEY AS VARCHAR(100))=CAST(src.nk_public_id AS VARCHAR(100))
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT PRTY_ASSET.PRTY_ASSET_ID         AS PRTY_ASSET_ID,
                                                                                PRTY_ASSET.ASSET_HOST_ID_VAL     AS ASSET_HOST_ID_VAL,
                                                                                PRTY_ASSET.PRTY_ASSET_SBTYPE_CD  AS PRTY_ASSET_SBTYPE_CD,
                                                                                PRTY_ASSET.PRTY_ASSET_CLASFCN_CD AS PRTY_ASSET_CLASFCN_CD
                                                                         FROM   DB_T_PROD_CORE.PRTY_ASSET 
                                                                         WHERE  CAST(PRTY_ASSET.EDW_END_DTTM AS DATE)=''9999-12-31'' ) AS PRTY_ASSET_lkp
                                                  ON              CAST(PRTY_ASSET_lkp.ASSET_HOST_ID_VAL AS VARCHAR(100))= CAST(src.fixedid AS VARCHAR(100))
                                                  AND             PRTY_ASSET_lkp.PRTY_ASSET_SBTYPE_CD=xlat_asset_sbtype.TGT_IDNTFTN_VAL
                                                  AND             PRTY_ASSET_lkp.PRTY_ASSET_CLASFCN_CD=COALESCE(xlat_asset_clasfcn.TGT_IDNTFTN_VAL,''UNK'')
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT AGMT.AGMT_ID       AS AGMT_ID,
                                                                                AGMT.HOST_AGMT_NUM AS HOST_AGMT_NUM,
                                                                                AGMT.NK_SRC_KEY    AS NK_SRC_KEY,
                                                                                AGMT.AGMT_TYPE_CD  AS AGMT_TYPE_CD
                                                                         FROM   DB_T_PROD_CORE.AGMT 
                                                                         WHERE  AGMT_TYPE_CD=''PPV''
                                                                         AND    CAST(AGMT.EDW_END_DTTM AS DATE)=''9999-12-31'' ) AS agmt_lkp
                                                  ON              CAST(agmt_lkp.NK_SRC_KEY AS VARCHAR(100))=CAST(src.PublicID_stg AS VARCHAR(100))
                                                  AND             agmt_lkp.AGMT_TYPE_CD=''PPV'' )
                  SELECT DISTINCT TG_agmt.AGMT_ASSET_FEAT_STRT_DTTM     AS lkp_AGMT_ASSET_FEAT_STRT_DTTM,
                                  TG_agmt.AGMT_ASSET_STRT_DTTM          AS lkp_AGMT_ASSET_STRT_DTTM,
                                  TG_agmt.AGMT_ASSET_FEAT_END_DTTM      AS lkp_AGMT_ASSET_FEAT_END_DTTM,
                                  TG_agmt.RATE_SYMB_CD                  AS lkp_RATE_SYMB_CD,
                                  TG_agmt.AGMT_ASSET_FEAT_AMT           AS lkp_AGMT_ASSET_FEAT_AMT,
                                  TG_agmt.AGMT_ASSET_FEAT_DT            AS lkp_AGMT_ASSET_FEAT_DT,
                                  TG_agmt.AGMT_ASSET_FEAT_TXT           AS lkp_AGMT_ASSET_FEAT_TXT,
                                  TG_agmt.AGMT_ASSET_FEAT_IND           AS lkp_AGMT_ASSET_FEAT_IND,
                                  TG_agmt.FEAT_EFECT_TYPE_CD            AS lkp_FEAT_EFECT_TYPE_CD,
                                  TG_agmt.AGMT_ID                       AS lkp_AGMT_ID,
                                  TG_agmt.FEAT_ID                       AS lkp_FEAT_ID,
                                  TG_agmt.ASSET_CNTRCT_ROLE_SBTYPE_CD   AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
                                  TG_agmt.PRTY_ASSET_ID                 AS lkp_PRTY_ASSET_ID,
                                  TG_agmt.EDW_STRT_DTTM                 AS lkp_EDW_STRT_DTTM,
                                  TG_agmt.EDW_END_DTTM                  AS lkp_EDW_END_DTTM,
                                  xlat_src.Src_feature_strt_dt          AS Src_feature_strt_dt,
                                  xlat_src.Src_asset_strt_dt            AS Src_asset_strt_dt,
                                  xlat_src.Src_feature_end_dt           AS Src_feature_end_dt,
                                  xlat_src.Src_AGMT_ID                  AS Src_AGMT_ID,
                                  xlat_src.Src_FEAT_ID                  AS Src_FEAT_ID,
                                  xlat_src.Src_PRTY_ASSET_ID            AS Src_PRTY_ASSET_ID,
                                  xlat_src.Src_OUT_cntrct_role          AS Src_ASSET_CNTRCT_ROLE_SBTYPE_CD,
                                  xlat_src.Src_INSRNC_CVGE_TYPE_CD      AS Src_INSRNC_CVGE_TYPE_CD,
                                  xlat_src.Src_RateSymbol_alfa          AS Src_RateSymbol_alfa,
                                  xlat_src.Src_RateSymbolCollision_alfa AS Src_RateSymbolCollision_alfa,
                                  CASE
                                                  WHEN xlat_src.Src_INSRNC_CVGE_TYPE_CD= ''COMP'' THEN Src_RateSymbol_alfa
                                                  WHEN xlat_src.Src_INSRNC_CVGE_TYPE_CD=''COLL'' THEN Src_RateSymbolCollision_alfa
                                                  ELSE ''''
                                  END                                          AS Src_RATE_SYMB_CD,
                                  xlat_src.Src_AGMT_ASSET_FEAT_AMT             AS Src_AGMT_ASSET_FEAT_AMT,
                                  xlat_src.Src_AGMT_ASSET_FEAT_DT              AS Src_AGMT_ASSET_FEAT_DT,
                                  xlat_src.Src_AGMT_ASSET_FEAT_TXT             AS Src_AGMT_ASSET_FEAT_TXT,
                                  xlat_src.Src_AGMT_ASSET_FEAT_IND             AS Src_AGMT_ASSET_FEAT_IND,
                                  xlat_src.Src_o_DiscountSurcharge_alfa_typecd AS Src_DiscountSurcharge_alfa_typecd,
                                  xlat_src.Src_UPDTAETIME                      AS Src_TRANS_STRT_DTTM,
                                  xlat_src.Src_Retired                         AS Src_Retired,
                                  xlat_src.Src_polcov_RateModifier             AS Src_polcov_RateModifier,
                                  CASE
                                                  WHEN xlat_src.Src_polcov_Eligible=1 THEN ''T''
                                                  WHEN xlat_src.Src_polcov_Eligible=0 THEN ''F''
                                  END AS Src_polcov_Eligible,
                                  /*sourcedata*/
                                  CAST(trim(TO_CHAR(to_char(XLAT_SRC.Src_feature_strt_dt , ''mm/dd/yyyy'' )))
                                                  || trim(TO_CHAR(to_char(XLAT_SRC.Src_asset_strt_dt, ''mm/dd/yyyy'')))
                                                  || trim(TO_CHAR(to_char(XLAT_SRC.Src_feature_end_dt , ''mm/dd/yyyy'' )))
                                                  || trim(COALESCE(
                                  CASE
                                                  WHEN COALESCE(RATE_SYMB_CD,'''')='''' THEN ''0''
                                                  ELSE RATE_SYMB_CD
                                  END ,0))
                                                  || trim(COALESCE(CAST(xlat_src.Src_AGMT_ASSET_FEAT_AMT AS DECIMAL(18,4)),0))
                                                  || COALESCE(CAST(to_char(XLAT_SRC.src_agmt_asset_feat_dt , ''mm/dd/yyyy'') AS VARCHAR(15)),CAST(to_date(''01/01/1900'' , ''mm/dd/yyyy'') AS VARCHAR(15)))
                                                  || trim(COALESCE(
                                  CASE
                                                  WHEN COALESCE(xlat_src.Src_AGMT_ASSET_FEAT_TXT,'''') = '''' THEN ''0''
                                                  ELSE xlat_src.Src_AGMT_ASSET_FEAT_TXT
                                  END ,0))
                                                  || trim(COALESCE(xlat_src.Src_AGMT_ASSET_FEAT_IND,0))
                                                  || trim(COALESCE(xlat_src.Src_o_DiscountSurcharge_alfa_typecd,0)) AS VARCHAR(1100)) AS sourcedata,
                                  /*Targetdata*/
                                  CAST(trim(TO_CHAR(to_char(TG_agmt.AGMT_ASSET_FEAT_STRT_DTTM , ''mm/dd/yyyy'' )))
                                                  || trim(TO_CHAR(to_char(TG_agmt.AGMT_ASSET_STRT_DTTM , ''mm/dd/yyyy'' )))
                                                  || trim(TO_CHAR(to_char(TG_agmt.AGMT_ASSET_FEAT_END_DTTM , ''mm/dd/yyyy'' )))
                                                  || trim(COALESCE(
                                  CASE
                                                  WHEN TG_agmt.RATE_SYMB_CD = '''' THEN ''0''
                                                  ELSE TG_agmt.RATE_SYMB_CD
                                  END ,0))
                                                  || trim(COALESCE(CAST(TG_agmt.AGMT_ASSET_FEAT_AMT AS DECIMAL(18,4)),0))
                                                  || COALESCE(CAST(to_char(agmt_asset_feat_dt , ''mm/dd/yyyy'') AS VARCHAR(15)),CAST(to_date(''01/01/1900'' , ''mm/dd/yyyy'') AS VARCHAR(15)))
                                                  || trim(COALESCE(
                                  CASE
                                                  WHEN TG_agmt.AGMT_ASSET_FEAT_TXT='''' THEN ''0''
                                                  ELSE TG_agmt.AGMT_ASSET_FEAT_TXT
                                  END ,0))
                                                  || trim(COALESCE(TG_agmt.AGMT_ASSET_FEAT_IND,0))
                                                  || trim(COALESCE(TG_agmt.FEAT_EFECT_TYPE_CD,0)) AS VARCHAR(1100)) AS targetdata,
                                  /*Flag*/
                                  CASE
                                                  WHEN targetdata IS NULL THEN ''I''
                                                  WHEN targetdata IS NOT NULL
                                                  AND             SourceData <> TargetData THEN ''U''
                                                  WHEN targetdata IS NOT NULL
                                                  AND             SourceData = TargetData THEN ''R''
                                  END AS ins_upd_flag
                  FROM            (
                                         SELECT Src_AGMT_ID,
                                                Src_FEAT_ID,
                                                Src_PRTY_ASSET_ID,
                                                Src_INSRNC_CVGE_TYPE_CD,
                                                Src_feature_strt_dt,
                                                Src_feature_end_dt,
                                                Src_OUT_cntrct_role,
                                                Src_asset_strt_dt,
                                                Src_UPDTAETIME,
                                                Src_RateSymbolCollision_alfa,
                                                Src_Retired,
                                                Src_RateSymbol_alfa,
                                                Src_o_DiscountSurcharge_alfa_typecd,
                                                Src_polcov_RateModifier,
                                                Src_polcov_Eligible,
                                                Src_AGMT_ASSET_FEAT_IND,
                                                Src_AGMT_ASSET_FEAT_TXT,
                                                Src_AGMT_ASSET_FEAT_AMT,
                                                Src_AGMT_ASSET_FEAT_DT
                                         FROM   AGMT_INS_FEAT) AS XLAT_SRC
                  LEFT OUTER JOIN
                                  (
                                           SELECT   AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_STRT_DTTM   AS AGMT_ASSET_FEAT_STRT_DTTM,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_AMT         AS AGMT_ASSET_FEAT_AMT,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_DT          AS AGMT_ASSET_FEAT_DT,
                                                    AGMT_INSRD_ASSET_FEAT.FEAT_EFECT_TYPE_CD          AS FEAT_EFECT_TYPE_CD,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_TXT         AS AGMT_ASSET_FEAT_TXT,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_IND         AS AGMT_ASSET_FEAT_IND,
                                                    AGMT_INSRD_ASSET_FEAT.PRTY_CNTCT_ID               AS PRTY_CNTCT_ID,
                                                    AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM               AS EDW_STRT_DTTM,
                                                    AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM                AS EDW_END_DTTM,
                                                    AGMT_INSRD_ASSET_FEAT.RATE_SYMB_CD                AS RATE_SYMB_CD,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ID                     AS AGMT_ID,
                                                    AGMT_INSRD_ASSET_FEAT.FEAT_ID                     AS FEAT_ID,
                                                    AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID               AS PRTY_ASSET_ID,
                                                    AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_STRT_DTTM        AS AGMT_ASSET_STRT_DTTM,
                                                    AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_END_DTTM    AS AGMT_ASSET_FEAT_END_DTTM
                                           FROM     DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT 
                                           JOIN     DB_T_PROD_CORE.AGMT_PROD 
                                           ON       AGMT_INSRD_ASSET_FEAT.AGMT_ID=AGMT_PROD.AGMT_ID
                                           JOIN     DB_T_PROD_CORE.PROD 
                                           ON       AGMT_PROD.PROD_ID=PROD.PROD_ID
                                           WHERE    (
                                                             AGMT_INSRD_ASSET_FEAT.AGMT_ID, AGMT_INSRD_ASSET_FEAT.FEAT_ID,AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID) IN
                                                                                                                                                                   (
                                                                                                                                                                   SELECT DISTINCT Src_AGMT_ID,
                                                                                                                                                                                   Src_FEAT_ID,
                                                                                                                                                                                   Src_PRTY_ASSET_ID
                                                                                                                                                                   FROM            AGMT_INS_FEAT)
                                           AND      PROD.INSRNC_LOB_TYPE_CD<>''BO'' 
                                           QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_INSRD_ASSET_FEAT.AGMT_ID, AGMT_INSRD_ASSET_FEAT.FEAT_ID,AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID, AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM DESC) = 1 ) AS TG_agmt
                  ON              TG_agmt.AGMT_ID=XLAT_SRC.Src_AGMT_ID
                  AND             TG_agmt.FEAT_ID=XLAT_SRC.Src_FEAT_ID
                  AND             TG_agmt.PRTY_ASSET_ID=XLAT_SRC.Src_PRTY_ASSET_ID
                  AND             TG_agmt.ASSET_CNTRCT_ROLE_SBTYPE_CD=XLAT_SRC.Src_OUT_cntrct_role
                  WHERE           (
                                                  xlat_src.Src_AGMT_ID IS NOT NULL
                                  AND             xlat_src.Src_FEAT_ID IS NOT NULL
                                  AND             xlat_src.Src_PRTY_ASSET_ID IS NOT NULL)
                  AND             (
                                                  ins_upd_flag IN (''I'',
                                                                   ''U'')
                                  OR              (
                                                                  ins_upd_flag=''R''
                                                  AND             CAST(TG_agmt.EDW_END_DTTM AS DATE)<>''9999-12-31'')) ) SRC ) );
  -- Component exp_isn_upd, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_isn_upd AS
  (
         SELECT SQ_pc_agmt_insrd_asset_feat_x.lkp_AGMT_ID                     AS lkp_AGMT_ID,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_FEAT_ID                     AS lkp_FEAT_ID,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_AGMT_ASSET_FEAT_STRT_DTTM   AS lkp_AGMT_ASSET_FEAT_STRT_DT,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_PRTY_ASSET_ID               AS lkp_PRTY_ASSET_ID,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_AGMT_ASSET_STRT_DTTM        AS lkp_AGMT_ASSET_STRT_DT,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_AGMT_ASSET_FEAT_END_DTTM    AS lkp_AGMT_ASSET_FEAT_END_DT,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_EDW_STRT_DTTM               AS lkp_EDW_STRT_DTTM,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_EDW_END_DTTM                AS lkp_EDW_END_DTTM,
                SQ_pc_agmt_insrd_asset_feat_x.lkp_FEAT_EFECT_TYPE_CD          AS lkp_FEAT_EFECT_TYPE_CD,
                SQ_pc_agmt_insrd_asset_feat_x.AGMT_ID                         AS AGMT_ID,
                SQ_pc_agmt_insrd_asset_feat_x.FEAT_ID                         AS FEAT_ID,
                SQ_pc_agmt_insrd_asset_feat_x.PRTY_ASSET_ID                   AS PRTY_ASSET_ID,
                SQ_pc_agmt_insrd_asset_feat_x.ASSET_CNTRCT_ROLE_SBTYPE_CD     AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
                SQ_pc_agmt_insrd_asset_feat_x.asset_strt_dt                   AS AGMT_ASSET_STRT_DT,
                SQ_pc_agmt_insrd_asset_feat_x.feature_strt_dt                 AS AGMT_ASSET_FEAT_STRT_DT,
                SQ_pc_agmt_insrd_asset_feat_x.feature_end_dt                  AS AGMT_ASSET_FEAT_END_DT,
                SQ_pc_agmt_insrd_asset_feat_x.TRANS_STRT_DTTM                 AS in_TRANS_STRT_DTTM,
                :PRCS_ID                                                      AS PRCS_ID,
                SQ_pc_agmt_insrd_asset_feat_x.AGMT_ASSET_FEAT_AMT             AS AGMT_ASSET_FEAT_AMT,
                SQ_pc_agmt_insrd_asset_feat_x.AGMT_ASSET_FEAT_DT              AS AGMT_ASSET_FEAT_DT,
                CASE
                       WHEN SQ_pc_agmt_insrd_asset_feat_x.AGMT_ASSET_FEAT_TXT IS NULL THEN ''''
                       ELSE SQ_pc_agmt_insrd_asset_feat_x.AGMT_ASSET_FEAT_TXT
                END                                                                   AS o_AGMT_ASSET_FEAT_TXT,
                SQ_pc_agmt_insrd_asset_feat_x.AGMT_ASSET_FEAT_IND                     AS AGMT_ASSET_FEAT_IND,
                CURRENT_TIMESTAMP                                                     AS EDW_STRT_DTTM,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS EDW_END_DTTM,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS o_Default_EndDate,
                SQ_pc_agmt_insrd_asset_feat_x.Retired                                 AS Retired,
                SQ_pc_agmt_insrd_asset_feat_x.polcov_RateModifier                     AS polcov_RateModifier,
                SQ_pc_agmt_insrd_asset_feat_x.polcov_Eligible                         AS polcov_Eligible,
                SQ_pc_agmt_insrd_asset_feat_x.DiscountSurcharge_alfa_typecd           AS o_DiscountSurcharge_alfa_typecd,
                SQ_pc_agmt_insrd_asset_feat_x.RATE_SYMB_CD                            AS RATE_SYMB_CD,
                SQ_pc_agmt_insrd_asset_feat_x.ins_upd_flag                            AS ins_upd_flag,
                SQ_pc_agmt_insrd_asset_feat_x.source_record_id
         FROM   SQ_pc_agmt_insrd_asset_feat_x );
  
  
  -- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_ins_upd_INSERT as
    SELECT exp_isn_upd.lkp_AGMT_ID                     AS lkp_AGMT_ID,
         exp_isn_upd.lkp_FEAT_ID                     AS lkp_FEAT_ID,
         exp_isn_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
         exp_isn_upd.lkp_AGMT_ASSET_STRT_DT          AS lkp_AGMT_ASSET_STRT_DT,
         exp_isn_upd.lkp_PRTY_ASSET_ID               AS lkp_PRTY_ASSET_ID,
         exp_isn_upd.lkp_AGMT_ASSET_FEAT_STRT_DT     AS lkp_AGMT_ASSET_FEAT_STRT_DT,
         exp_isn_upd.lkp_FEAT_EFECT_TYPE_CD          AS FEAT_EFECT_TYPE_CD,
         exp_isn_upd.AGMT_ID                         AS AGMT_ID,
         exp_isn_upd.FEAT_ID                         AS FEAT_ID,
         exp_isn_upd.PRTY_ASSET_ID                   AS PRTY_ASSET_ID,
         exp_isn_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD     AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
         exp_isn_upd.AGMT_ASSET_STRT_DT              AS AGMT_ASSET_STRT_DT,
         exp_isn_upd.AGMT_ASSET_FEAT_STRT_DT         AS AGMT_ASSET_FEAT_STRT_DT,
         exp_isn_upd.AGMT_ASSET_FEAT_END_DT          AS AGMT_ASSET_FEAT_END_DT,
         exp_isn_upd.PRCS_ID                         AS PRCS_ID,
         NULL                                        AS OVRDN_FEAT_ID,
         exp_isn_upd.ins_upd_flag                    AS out_ins_upd,
         exp_isn_upd.EDW_STRT_DTTM                   AS EDW_STRT_DTTM,
         exp_isn_upd.EDW_END_DTTM                    AS EDW_END_DTTM,
         exp_isn_upd.lkp_EDW_STRT_DTTM               AS lkp_EDW_STRT_DTTM,
         exp_isn_upd.lkp_AGMT_ASSET_FEAT_END_DT      AS lkp_AGMT_ASSET_FEAT_END_DT,
         exp_isn_upd.in_TRANS_STRT_DTTM              AS o_Default_Date,
         exp_isn_upd.o_Default_EndDate               AS o_Default_EndDate,
         exp_isn_upd.RATE_SYMB_CD                    AS out_RATE_SYMB_CD,
         exp_isn_upd.Retired                         AS Retired,
         exp_isn_upd.lkp_EDW_END_DTTM                AS lkp_EDW_END_DTTM,
         NULL                                        AS out_trans_end_dttm,
         exp_isn_upd.AGMT_ASSET_FEAT_AMT             AS AGMT_ASSET_FEAT_AMT,
         exp_isn_upd.AGMT_ASSET_FEAT_DT              AS AGMT_ASSET_FEAT_DT,
         exp_isn_upd.o_AGMT_ASSET_FEAT_TXT           AS AGMT_ASSET_FEAT_TXT,
         exp_isn_upd.AGMT_ASSET_FEAT_IND             AS AGMT_ASSET_FEAT_IND,
         exp_isn_upd.polcov_RateModifier             AS polcov_RateModifier,
         exp_isn_upd.polcov_Eligible                 AS polcov_Eligible,
         exp_isn_upd.o_DiscountSurcharge_alfa_typecd AS o_DiscountSurcharge_alfa_typecd,
         exp_isn_upd.source_record_id
  FROM   exp_isn_upd
  WHERE  exp_isn_upd.AGMT_ID IS NOT NULL
  AND    exp_isn_upd.FEAT_ID IS NOT NULL
  AND    exp_isn_upd.PRTY_ASSET_ID IS NOT NULL
  AND    (
                exp_isn_upd.ins_upd_flag = ''I'' )
  OR     (
                exp_isn_upd.Retired = 0
         AND    exp_isn_upd.lkp_EDW_END_DTTM != to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) )
  --OR     - - exp_isn_upd.AGMT_ID IS NOT NULL
  AND    exp_isn_upd.ins_upd_flag = ''U''
  AND    exp_isn_upd.lkp_EDW_END_DTTM = to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) 
  --exp_isn_upd.AGMT_ID IS NOT NULL
  AND    exp_isn_upd.FEAT_ID IS NOT NULL
  AND    exp_isn_upd.PRTY_ASSET_ID IS NOT NULL
  AND    (
                exp_isn_upd.ins_upd_flag = ''U'' )
  AND    exp_isn_upd.lkp_EDW_END_DTTM = to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );
  
  -- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_ins_upd_RETIRE as
    SELECT exp_isn_upd.lkp_AGMT_ID                     AS lkp_AGMT_ID,
         exp_isn_upd.lkp_FEAT_ID                     AS lkp_FEAT_ID,
         exp_isn_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
         exp_isn_upd.lkp_AGMT_ASSET_STRT_DT          AS lkp_AGMT_ASSET_STRT_DT,
         exp_isn_upd.lkp_PRTY_ASSET_ID               AS lkp_PRTY_ASSET_ID,
         exp_isn_upd.lkp_AGMT_ASSET_FEAT_STRT_DT     AS lkp_AGMT_ASSET_FEAT_STRT_DT,
         exp_isn_upd.lkp_FEAT_EFECT_TYPE_CD          AS FEAT_EFECT_TYPE_CD,
         exp_isn_upd.AGMT_ID                         AS AGMT_ID,
         exp_isn_upd.FEAT_ID                         AS FEAT_ID,
         exp_isn_upd.PRTY_ASSET_ID                   AS PRTY_ASSET_ID,
         exp_isn_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD     AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
         exp_isn_upd.AGMT_ASSET_STRT_DT              AS AGMT_ASSET_STRT_DT,
         exp_isn_upd.AGMT_ASSET_FEAT_STRT_DT         AS AGMT_ASSET_FEAT_STRT_DT,
         exp_isn_upd.AGMT_ASSET_FEAT_END_DT          AS AGMT_ASSET_FEAT_END_DT,
         exp_isn_upd.PRCS_ID                         AS PRCS_ID,
         NULL                                        AS OVRDN_FEAT_ID,
         exp_isn_upd.ins_upd_flag                    AS out_ins_upd,
         exp_isn_upd.EDW_STRT_DTTM                   AS EDW_STRT_DTTM,
         exp_isn_upd.EDW_END_DTTM                    AS EDW_END_DTTM,
         exp_isn_upd.lkp_EDW_STRT_DTTM               AS lkp_EDW_STRT_DTTM,
         exp_isn_upd.lkp_AGMT_ASSET_FEAT_END_DT      AS lkp_AGMT_ASSET_FEAT_END_DT,
         exp_isn_upd.in_TRANS_STRT_DTTM              AS o_Default_Date,
         exp_isn_upd.o_Default_EndDate               AS o_Default_EndDate,
         exp_isn_upd.RATE_SYMB_CD                    AS out_RATE_SYMB_CD,
         exp_isn_upd.Retired                         AS Retired,
         exp_isn_upd.lkp_EDW_END_DTTM                AS lkp_EDW_END_DTTM,
         NULL                                        AS out_trans_end_dttm,
         exp_isn_upd.AGMT_ASSET_FEAT_AMT             AS AGMT_ASSET_FEAT_AMT,
         exp_isn_upd.AGMT_ASSET_FEAT_DT              AS AGMT_ASSET_FEAT_DT,
         exp_isn_upd.o_AGMT_ASSET_FEAT_TXT           AS AGMT_ASSET_FEAT_TXT,
         exp_isn_upd.AGMT_ASSET_FEAT_IND             AS AGMT_ASSET_FEAT_IND,
         exp_isn_upd.polcov_RateModifier             AS polcov_RateModifier,
         exp_isn_upd.polcov_Eligible                 AS polcov_Eligible,
         exp_isn_upd.o_DiscountSurcharge_alfa_typecd AS o_DiscountSurcharge_alfa_typecd,
         exp_isn_upd.source_record_id
  FROM   exp_isn_upd
  WHERE  exp_isn_upd.AGMT_ID IS NOT NULL
  AND    exp_isn_upd.FEAT_ID IS NOT NULL
  AND    exp_isn_upd.PRTY_ASSET_ID IS NOT NULL
  AND    exp_isn_upd.ins_upd_flag = ''R''
  AND    exp_isn_upd.Retired != 0
  AND    exp_isn_upd.lkp_EDW_END_DTTM = to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );
  
  -- Component upd_update1, Type UPDATE
  CREATE
  OR
  REPLACE TEMPORARY TABLE upd_update1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_RETIRE.lkp_AGMT_ID                     AS lkp_AGMT_ID3,
                rtr_ins_upd_RETIRE.lkp_FEAT_ID                     AS lkp_FEAT_ID3,
                rtr_ins_upd_RETIRE.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
                rtr_ins_upd_RETIRE.lkp_PRTY_ASSET_ID               AS lkp_PRTY_ASSET_ID3,
                rtr_ins_upd_RETIRE.lkp_EDW_STRT_DTTM               AS lkp_EDW_STRT_DTTM3,
                rtr_ins_upd_RETIRE.lkp_AGMT_ASSET_STRT_DT          AS lkp_AGMT_ASSET_STRT_DT3,
                rtr_ins_upd_RETIRE.lkp_AGMT_ASSET_FEAT_STRT_DT     AS lkp_AGMT_ASSET_FEAT_STRT_DT3,
                rtr_ins_upd_RETIRE.PRCS_ID                         AS PRCS_ID3,
                rtr_ins_upd_RETIRE.o_Default_Date                  AS o_Default_Date4,
                rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_AMT             AS AGMT_ASSET_FEAT_AMT4,
                rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_DT              AS AGMT_ASSET_FEAT_DT4,
                rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_TXT             AS AGMT_ASSET_FEAT_TXT4,
                rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_IND             AS AGMT_ASSET_FEAT_IND4,
                rtr_ins_upd_RETIRE.polcov_RateModifier             AS polcov_RateModifier,
                rtr_ins_upd_RETIRE.polcov_Eligible                 AS polcov_Eligible,
                rtr_ins_upd_RETIRE.o_DiscountSurcharge_alfa_typecd AS o_DiscountSurcharge_alfa_typecd4,
                1                                           AS UPDATE_STRATEGY_ACTION,
                rtr_ins_upd_RETIRE.source_record_id
         FROM   rtr_ins_upd_RETIRE );
  -- Component upd_ins_new, Type UPDATE
  CREATE
  OR
  REPLACE TEMPORARY TABLE upd_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_INSERT.AGMT_ID                         AS AGMT_ID,
                rtr_ins_upd_INSERT.FEAT_ID                         AS FEAT_ID,
                rtr_ins_upd_INSERT.PRTY_ASSET_ID                   AS PRTY_ASSET_ID,
                rtr_ins_upd_INSERT.ASSET_CNTRCT_ROLE_SBTYPE_CD     AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
                rtr_ins_upd_INSERT.AGMT_ASSET_STRT_DT              AS AGMT_ASSET_STRT_DT,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_STRT_DT         AS AGMT_ASSET_FEAT_STRT_DT,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_END_DT          AS AGMT_ASSET_FEAT_END_DT,
                NULL                                               AS OVRDN_FEAT_ID,
                rtr_ins_upd_INSERT.PRCS_ID                         AS PRCS_ID,
                rtr_ins_upd_INSERT.EDW_STRT_DTTM                   AS EDW_STRT_DTTM1,
                rtr_ins_upd_INSERT.EDW_END_DTTM                    AS EDW_END_DTTM1,
                rtr_ins_upd_INSERT.o_Default_Date                  AS o_Default_Date3,
                rtr_ins_upd_INSERT.o_Default_EndDate               AS o_Default_EndDate1,
                rtr_ins_upd_INSERT.out_RATE_SYMB_CD                AS out_RATE_SYMB_CD,
                rtr_ins_upd_INSERT.Retired                         AS Retired1,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_AMT             AS AGMT_ASSET_FEAT_AMT1,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_DT              AS AGMT_ASSET_FEAT_DT1,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_TXT             AS AGMT_ASSET_FEAT_TXT1,
                rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_IND             AS AGMT_ASSET_FEAT_IND1,
                rtr_ins_upd_INSERT.polcov_RateModifier             AS polcov_RateModifier,
                rtr_ins_upd_INSERT.polcov_Eligible                 AS polcov_Eligible,
                rtr_ins_upd_INSERT.o_DiscountSurcharge_alfa_typecd AS o_DiscountSurcharge_alfa_typecd1,
                0                                                  AS UPDATE_STRATEGY_ACTION,
                rtr_ins_upd_INSERT.source_record_id
         FROM   rtr_ins_upd_INSERT );
  -- Component exp_pass_to_tgt_upd1, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
  (
         SELECT upd_update1.lkp_AGMT_ID3                     AS lkp_AGMT_ID3,
                upd_update1.lkp_FEAT_ID3                     AS lkp_FEAT_ID3,
                upd_update1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3 AS lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
                upd_update1.lkp_PRTY_ASSET_ID3               AS lkp_PRTY_ASSET_ID3,
                upd_update1.lkp_EDW_STRT_DTTM3               AS lkp_EDW_STRT_DTTM3,
                CURRENT_TIMESTAMP                            AS EDW_END_DTTM,
                upd_update1.o_Default_Date4                  AS o_Default_Date4,
                upd_update1.o_Default_Date4                  AS out_trans_end_dttm4,
                upd_update1.AGMT_ASSET_FEAT_AMT4             AS AGMT_ASSET_FEAT_AMT4,
                upd_update1.AGMT_ASSET_FEAT_DT4              AS AGMT_ASSET_FEAT_DT4,
                upd_update1.AGMT_ASSET_FEAT_TXT4             AS AGMT_ASSET_FEAT_TXT4,
                upd_update1.AGMT_ASSET_FEAT_IND4             AS AGMT_ASSET_FEAT_IND4,
                upd_update1.polcov_RateModifier              AS polcov_RateModifier,
                upd_update1.polcov_Eligible                  AS polcov_Eligible,
                upd_update1.o_DiscountSurcharge_alfa_typecd4 AS o_DiscountSurcharge_alfa_typecd4,
                upd_update1.source_record_id
         FROM   upd_update1 );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_ins_new.AGMT_ID                     AS AGMT_ID,
                upd_ins_new.FEAT_ID                     AS FEAT_ID,
                upd_ins_new.PRTY_ASSET_ID               AS PRTY_ASSET_ID,
                upd_ins_new.ASSET_CNTRCT_ROLE_SBTYPE_CD AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
                upd_ins_new.AGMT_ASSET_STRT_DT          AS AGMT_ASSET_STRT_DT,
                upd_ins_new.AGMT_ASSET_FEAT_STRT_DT     AS AGMT_ASSET_FEAT_STRT_DT,
                upd_ins_new.AGMT_ASSET_FEAT_END_DT      AS AGMT_ASSET_FEAT_END_DT,
                upd_ins_new.PRCS_ID                     AS PRCS_ID,
                upd_ins_new.EDW_STRT_DTTM1              AS EDW_STRT_DTTM1,
                CASE
                       WHEN upd_ins_new.Retired1 != 0 THEN CURRENT_TIMESTAMP
                       ELSE upd_ins_new.EDW_END_DTTM1
                END                              AS o_EDW_END_DTTM,
                upd_ins_new.o_Default_Date3      AS o_Default_Date3,
                upd_ins_new.out_RATE_SYMB_CD     AS out_RATE_SYMB_CD,
                upd_ins_new.AGMT_ASSET_FEAT_AMT1 AS AGMT_ASSET_FEAT_AMT1,
                upd_ins_new.AGMT_ASSET_FEAT_DT1  AS AGMT_ASSET_FEAT_DT1,
                upd_ins_new.AGMT_ASSET_FEAT_TXT1 AS AGMT_ASSET_FEAT_TXT1,
                upd_ins_new.AGMT_ASSET_FEAT_IND1 AS AGMT_ASSET_FEAT_IND1,
                CASE
                       WHEN upd_ins_new.Retired1 = 0 THEN to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                       ELSE upd_ins_new.o_Default_Date3
                END                                          AS TRNS_END_DTTM,
                upd_ins_new.polcov_RateModifier              AS polcov_RateModifier,
                upd_ins_new.polcov_Eligible                  AS polcov_Eligible,
                upd_ins_new.o_DiscountSurcharge_alfa_typecd1 AS o_DiscountSurcharge_alfa_typecd1,
                upd_ins_new.source_record_id
         FROM   upd_ins_new );
  -- Component AGMT_INSRD_ASSET_FEAT_insert_new, Type TARGET
  INSERT INTO DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
              (
                          AGMT_ID,
                          FEAT_ID,
                          PRTY_ASSET_ID,
                          ASSET_CNTRCT_ROLE_SBTYPE_CD,
                          AGMT_ASSET_STRT_DTTM,
                          AGMT_ASSET_FEAT_STRT_DTTM,
                          AGMT_ASSET_FEAT_END_DTTM,
                          RATE_SYMB_CD,
                          AGMT_ASSET_FEAT_AMT,
                          AGMT_ASSET_FEAT_RATE,
                          AGMT_ASSET_FEAT_DT,
                          FEAT_EFECT_TYPE_CD,
                          AGMT_ASSET_FEAT_TXT,
                          AGMT_ASSET_FEAT_IND,
                          FEAT_ELGBL_IND,
                          PRCS_ID,
                          EDW_STRT_DTTM,
                          EDW_END_DTTM,
                          TRANS_STRT_DTTM,
                          TRANS_END_DTTM
              )
  SELECT exp_pass_to_target_ins.AGMT_ID                          AS AGMT_ID,
         exp_pass_to_target_ins.FEAT_ID                          AS FEAT_ID,
         exp_pass_to_target_ins.PRTY_ASSET_ID                    AS PRTY_ASSET_ID,
         exp_pass_to_target_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD      AS ASSET_CNTRCT_ROLE_SBTYPE_CD,
         exp_pass_to_target_ins.AGMT_ASSET_STRT_DT               AS AGMT_ASSET_STRT_DTTM,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_STRT_DT          AS AGMT_ASSET_FEAT_STRT_DTTM,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_END_DT           AS AGMT_ASSET_FEAT_END_DTTM,
         exp_pass_to_target_ins.out_RATE_SYMB_CD                 AS RATE_SYMB_CD,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_AMT1             AS AGMT_ASSET_FEAT_AMT,
         exp_pass_to_target_ins.polcov_RateModifier              AS AGMT_ASSET_FEAT_RATE,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_DT1              AS AGMT_ASSET_FEAT_DT,
         exp_pass_to_target_ins.o_DiscountSurcharge_alfa_typecd1 AS FEAT_EFECT_TYPE_CD,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_TXT1             AS AGMT_ASSET_FEAT_TXT,
         exp_pass_to_target_ins.AGMT_ASSET_FEAT_IND1             AS AGMT_ASSET_FEAT_IND,
         exp_pass_to_target_ins.polcov_Eligible                  AS FEAT_ELGBL_IND,
         exp_pass_to_target_ins.PRCS_ID                          AS PRCS_ID,
         exp_pass_to_target_ins.EDW_STRT_DTTM1                   AS EDW_STRT_DTTM,
         exp_pass_to_target_ins.o_EDW_END_DTTM                   AS EDW_END_DTTM,
         exp_pass_to_target_ins.o_Default_Date3                  AS TRANS_STRT_DTTM,
         exp_pass_to_target_ins.TRNS_END_DTTM                    AS TRANS_END_DTTM
  FROM   exp_pass_to_target_ins;
  
  -- Component AGMT_INSRD_ASSET_FEAT_upd_retired, Type TARGET
  MERGE
  INTO         DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
  USING        exp_pass_to_tgt_upd1
  ON (
                            AGMT_INSRD_ASSET_FEAT.AGMT_ID = exp_pass_to_tgt_upd1.lkp_AGMT_ID3
               AND          AGMT_INSRD_ASSET_FEAT.FEAT_ID = exp_pass_to_tgt_upd1.lkp_FEAT_ID3
               AND          AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID = exp_pass_to_tgt_upd1.lkp_PRTY_ASSET_ID3
               AND          AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3
               AND          AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = exp_pass_to_tgt_upd1.lkp_EDW_STRT_DTTM3)
  WHEN MATCHED THEN
  UPDATE
  SET    AGMT_ID = exp_pass_to_tgt_upd1.lkp_AGMT_ID3,
         FEAT_ID = exp_pass_to_tgt_upd1.lkp_FEAT_ID3,
         PRTY_ASSET_ID = exp_pass_to_tgt_upd1.lkp_PRTY_ASSET_ID3,
         ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
         AGMT_ASSET_FEAT_AMT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_AMT4,
         AGMT_ASSET_FEAT_RATE = exp_pass_to_tgt_upd1.polcov_RateModifier,
         AGMT_ASSET_FEAT_DT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_DT4,
         FEAT_EFECT_TYPE_CD = exp_pass_to_tgt_upd1.o_DiscountSurcharge_alfa_typecd4,
         AGMT_ASSET_FEAT_TXT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_TXT4,
         AGMT_ASSET_FEAT_IND = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_IND4,
         FEAT_ELGBL_IND = exp_pass_to_tgt_upd1.polcov_Eligible,
         EDW_STRT_DTTM = exp_pass_to_tgt_upd1.lkp_EDW_STRT_DTTM3,
         EDW_END_DTTM = exp_pass_to_tgt_upd1.EDW_END_DTTM,
         TRANS_STRT_DTTM = exp_pass_to_tgt_upd1.o_Default_Date4,
         TRANS_END_DTTM = exp_pass_to_tgt_upd1.out_trans_end_dttm4;
  
  -- Component AGMT_INSRD_ASSET_FEAT_upd_retired, Type Post SQL
  UPDATE db_t_prod_core.AGMT_INSRD_ASSET_FEAT
  SET    EDW_END_DTTM=A.lead1,
         TRANS_END_DTTM=A.lead2
  FROM   (
                         SELECT DISTINCT AGMT_ID,
                                         FEAT_ID,
                                         PRTY_ASSET_ID,
                                         ASSET_CNTRCT_ROLE_SBTYPE_CD,
                                         EDW_STRT_DTTM,
                                         TRANS_STRT_DTTM,
                                         MAX(EDW_STRT_DTTM) over (PARTITION BY AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY EDW_STRT_DTTM ASC ROWS BETWEEN 1 following AND             1 following)     - INTERVAL ''1 SECOND'' AS lead1 ,
                                         MAX(TRANS_STRT_DTTM) over (PARTITION BY AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY TRANS_STRT_DTTM ASC ROWS BETWEEN 1 following AND             1 following) - INTERVAL ''1 SECOND'' AS lead2
                         FROM            db_t_prod_core.AGMT_INSRD_ASSET_FEAT
                         GROUP BY        AGMT_ID,
                                         FEAT_ID,
                                         PRTY_ASSET_ID,
                                         ASSET_CNTRCT_ROLE_SBTYPE_CD,
                                         EDW_STRT_DTTM,
                                         TRANS_STRT_DTTM ) A

  WHERE  AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM
  AND    AGMT_INSRD_ASSET_FEAT.AGMT_ID=A.AGMT_ID
  AND    AGMT_INSRD_ASSET_FEAT.FEAT_ID=A.FEAT_ID
  AND    AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID=A.PRTY_ASSET_ID
  AND    AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD
  AND    CAST(AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;

END;
';