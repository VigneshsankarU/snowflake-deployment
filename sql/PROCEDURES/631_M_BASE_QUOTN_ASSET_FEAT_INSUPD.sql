-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_ASSET_FEAT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
  DECLARE
    prcs_id INTEGER;
    start_dttm timestamp;
    end_dttm timestamp;
    run_id STRING;
    workflow_name STRING;
    session_name STRING;
  BEGIN
    run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
    workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
    session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
    prcs_id := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
    start_dttm := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
    -- PIPELINE START FOR 1
    -- ******** No handler defined for type SOURCE, node SQ_pc_quotn_asset_feat_x *******
    CREATE
    OR
    replace TEMPORARY TABLE sq_pc_quotn_asset_feat_x AS ( WITH
    /*EIM-48974- FARM CHANGES BEGINS*/
    farm_temp AS
    (--Coverages
                    SELECT DISTINCT pp.policynumber_stg,
                                    pp.periodstart_stg AS pol_start_dt,
                                    CASE
                                                    WHEN polcov.effectivedate_stg IS NULL THEN pp.periodstart_stg
                                                    ELSE polcov.effectivedate_stg
                                    END feature_start_dt,
                                    CASE
                                                    WHEN polcov.expirationdate_stg IS NULL THEN pp.periodend_stg
                                                    ELSE polcov.expirationdate_stg
                                    END                                                    feature_end_dt,
                                    cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))       AS cntrct_role,
                                    coalesce(polcov.expirationdate_stg,pp.periodstart_stg) AS asset_start_dt,
                                    CASE
                                                    WHEN covterm.covtermtype = ''Package'' THEN PACKAGE.packagepatternid
                                                    WHEN covterm.covtermtype = ''Option''
                                                    AND             polcov.val IS NOT NULL THEN optn.optionpatternid
                                                    WHEN covterm.covtermtype = ''Clause'' THEN covterm.clausepatternid
                                                    ELSE covterm.covtermpatternid
                                    END AS nk_public_id,
                                    CASE
                                                    WHEN covterm.covtermtype = ''Package'' THEN cast (''FEAT_SBTYPE9'' AS VARCHAR (50))
                                                    WHEN covterm.covtermtype = ''Option''
                                                    AND             polcov.val IS NOT NULL THEN cast (''FEAT_SBTYPE8'' AS VARCHAR(50))
                                                    WHEN covterm.covtermtype=''Clause'' THEN cast(''FEAT_SBTYPE7'' AS       VARCHAR(50))
                                                    ELSE cast (''FEAT_SBTYPE6'' AS                                        VARCHAR (50))
                                    END                        feat_sbtype_cd,
                                    polcov.assettype_stg       AS assettype,
                                    polcov.classification_code AS classification_code,
                                    polcov.assetkey            AS fixedid,
                                    cast(NULL AS VARCHAR(60))  AS ratesymbolcollision_alfa_stg,
                                    cast(NULL AS VARCHAR(60))  AS ratesymbol_alfa_stg,
                                    jobnumber_stg,
                                    branchnumber_stg,
                                    pp.updatetime_stg           AS trans_strt_dt,
                                    cast( ''9999-12-31'' AS DATE )AS trans_end_dt,
                                    polcov.val                     feat_val,
                                    cast(
                                    CASE
                                                    WHEN optn.valuetype=''Percent'' THEN optn.value1
                                    END AS       DECIMAL(14,4))    feat_rate,
                                    cast(NULL AS VARCHAR(10))   AS eligible,
                                    covterm.covtermtype            feat_covtermtype,
                                    cast(NULL AS VARCHAR(50))   AS discountsurcharge_alfa_typecd
                    FROM            (--pcx_fopdwellingcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm2_stg IS NULL THEN cast(dateterm2_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm2_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             dateterm2avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL
                                                                      AND             stringterm1avl_stg IS NULL ) AS fopdwell qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopoutbuildingcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm2_stg IS NULL THEN cast(dateterm2_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm2_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             dateterm2avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL ) AS fopoutbldg qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopfeedandseedcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      feedandseed_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfeedandseedcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      feedandseed_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfeedandseedcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      feedandseed_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfeedandseedcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      feedandseed_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfeedandseedcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL ) AS fopfdsd qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopmachinerycov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      machinery_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      machinery_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      machinery_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      machinery_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      machinery_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL ) AS fopmch qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_foplivestockcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      livestock_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_foplivestockcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      livestock_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_foplivestockcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      livestock_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_foplivestockcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      livestock_stg                              AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_foplivestockcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL ) AS fopliv qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopdwellingschcovitemcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopdwellingschedulecovitem_stg             AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             booleanterm1avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             choiceterm4avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL
                                                                      AND             stringterm1avl_stg IS NULL
                                                                      AND             stringterm2avl_stg IS NULL
                                                                      AND             stringterm3avl_stg IS NULL ) AS dwellsch qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopfarmownersschcovitemcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm2'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm2_stg IS NULL THEN cast(dateterm2_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm2_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopfarmownerslischedulecovitem_stg         AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             booleanterm1avl_stg IS NULL
                                                                      AND             booleanterm2avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             choiceterm4avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             dateterm2avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL
                                                                      AND             stringterm1avl_stg IS NULL
                                                                      AND             stringterm2avl_stg IS NULL
                                                                      AND             stringterm3avl_stg IS NULL
                                                                      AND             stringterm4avl_stg IS NULL ) AS fopfarmsch qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopliabilityschcovitemcov
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm5_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm5avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm6_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm6avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm7_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm7avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm8'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm8_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm8avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm9'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm9_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm9avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm10'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm10_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS      VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm10avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm11'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm11_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS      VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm11avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm12'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm12_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS      VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm12avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm13'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm13_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS   VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS      VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm13avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm5_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm5avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm6_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm6avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm7_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm7avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm8'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm8_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm8avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm5'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm5_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm5avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm6'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm6_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm6avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm7'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm7_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm7avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      fop.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      fop.updatetime_stg,
                                                                                      fopliabilityschedulecovitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov fop
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = fop.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             booleanterm1avl_stg IS NULL
                                                                      AND             booleanterm2avl_stg IS NULL
                                                                      AND             booleanterm3avl_stg IS NULL
                                                                      AND             booleanterm4avl_stg IS NULL
                                                                      AND             booleanterm5avl_stg IS NULL
                                                                      AND             booleanterm6avl_stg IS NULL
                                                                      AND             booleanterm7avl_stg IS NULL
                                                                      AND             booleanterm8avl_stg IS NULL
                                                                      AND             booleanterm9avl_stg IS NULL
                                                                      AND             booleanterm10avl_stg IS NULL
                                                                      AND             booleanterm11avl_stg IS NULL
                                                                      AND             booleanterm12avl_stg IS NULL
                                                                      AND             booleanterm13avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             choiceterm4avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL
                                                                      AND             directterm3avl_stg IS NULL
                                                                      AND             directterm4avl_stg IS NULL
                                                                      AND             directterm5avl_stg IS NULL
                                                                      AND             directterm6avl_stg IS NULL
                                                                      AND             directterm7avl_stg IS NULL
                                                                      AND             stringterm1avl_stg IS NULL
                                                                      AND             stringterm2avl_stg IS NULL
                                                                      AND             stringterm3avl_stg IS NULL
                                                                      AND             stringterm4avl_stg IS NULL
                                                                      AND             stringterm5avl_stg IS NULL
                                                                      AND             stringterm6avl_stg IS NULL
                                                                      AND             stringterm7avl_stg IS NULL
                                                                      AND             stringterm8avl_stg IS NULL ) AS fopliabsch qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1 ) polcov
                    inner join
                                    (
                                           SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                  policynumber_stg,
                                                  periodstart_stg,
                                                  branchnumber_stg,
                                                  periodend_stg,
                                                  mostrecentmodel_stg,
                                                  status_stg,
                                                  jobid_stg,
                                                  publicid_stg,
                                                  editeffectivedate_stg,
                                                  createtime_stg,
                                                  updatetime_stg,
                                                  retired_stg
                                           FROM   db_t_prod_stag.pc_policyperiod) pp
                    ON              pp.id = polcov.branchid
                    left join
                                    (
                                           SELECT pcl.patternid_stg     clausepatternid,
                                                  pcv.patternid_stg     covtermpatternid,
                                                  pcv.columnname_stg  AS columnname,
                                                  pcv.covtermtype_stg AS covtermtype,
                                                  pcl.name_stg           clausename
                                           FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                           join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                           ON     pcl.id_stg = pcv.clausepatternid_stg
                                           UNION
                                           SELECT    pcl.patternid_stg                       clausepatternid,
                                                     pcv.patternid_stg                       covtermpatternid,
                                                     coalesce(pcv.columnname_stg,''Clause'')   columnname,
                                                     coalesce(pcv.covtermtype_stg, ''Clause'') covtermtype,
                                                     pcl.name_stg                            clausename
                                           FROM      db_t_prod_stag.pc_etlclausepattern pcl
                                           left join
                                                     (
                                                            SELECT *
                                                            FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                            WHERE  name_stg NOT LIKE ''ZZ%'' ) pcv
                                           ON        pcv.clausepatternid_stg = pcl.id_stg
                                           WHERE     pcl.name_stg NOT LIKE ''ZZ%''
                                           AND       pcv.name_stg IS NULL
                                           AND       owningentitytype_stg IN (''FOPBlanket'',
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
                    ON              covterm.clausepatternid = polcov.patterncode_stg
                    AND             covterm.columnname = polcov.columnname
                    left outer join
                                    (
                                           SELECT pcv.patternid_stg   packagepatternid,
                                                  pcv.packagecode_stg cov_id,
                                                  pcv.packagecode_stg name1
                                           FROM   db_t_prod_stag.pc_etlcovtermpackage pcv) PACKAGE
                    ON              PACKAGE.packagepatternid = polcov.val
                    left outer join
                                    (
                                               SELECT     pct.patternid_stg                      optionpatternid,
                                                          pct.optioncode_stg                     name1,
                                                          cast(pct.value_stg AS VARCHAR(255)) AS value1,
                                                          pcv.valuetype_stg                   AS valuetype
                                               FROM       db_t_prod_stag.pc_etlcovtermpattern pcv
                                               inner join db_t_prod_stag.pc_etlcovtermoption pct
                                               ON         pcv.id_stg = pct.coveragetermpatternid_stg ) optn
                    ON              optn.optionpatternid = polcov.val
                    inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                    ON              pps.id_stg = pp.status_stg
                    inner join      db_t_prod_stag.pc_job pj
                    ON              pj.id_stg = pp.jobid_stg
                    inner join      db_t_prod_stag.pctl_job pcj
                    ON              pcj.id_stg = pj.subtype_stg
                    WHERE           covterm.clausename NOT LIKE''%ZZ%''
                    AND             pps.typecode_stg <> ''Temporary''
                    AND             pcj.typecode_stg IN (''Submission'',
                                                         ''PolicyChange'',
                                                         ''Renewal'')
                    AND             pp.updatetime_stg > (:start_dttm)
                    AND             pp.updatetime_stg <= (:end_dttm)
                    UNION
                    --Modifiers
                    SELECT DISTINCT pp.policynumber_stg,
                                    pp.periodstart_stg AS pol_start_dt,
                                    CASE
                                                    WHEN polcov.effectivedate_stg IS NULL THEN pp.periodstart_stg
                                                    ELSE polcov.effectivedate_stg
                                    END feature_start_dt,
                                    CASE
                                                    WHEN polcov.expirationdate_stg IS NULL THEN pp.periodend_stg
                                                    ELSE polcov.expirationdate_stg
                                    END                                                   feature_end_dt,
                                    cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))      AS cntrct_role,
                                    coalesce(polcov.effectivedate_stg,pp.periodstart_stg) AS asset_start_dt,
                                    polcov.patterncode_stg                                AS nk_public_id,
                                    polcov.typ                                            AS feat_sbtype_cd,
                                    polcov.assettype_stg                                  AS assettype,
                                    polcov.classification_code                            AS classification_code,
                                    polcov.assetkey                                       AS fixedid,
                                    cast(NULL AS VARCHAR(60))                             AS ratesymbolcollision_alfa_stg,
                                    cast(NULL AS VARCHAR(60))                             AS ratesymbol_alfa_stg,
                                    jobnumber_stg                                         AS jobnumber,
                                    branchnumber_stg                                      AS branchnumber,
                                    pp.updatetime_stg                                     AS trans_strt_dt,
                                    cast(''9999-12-31'' AS     DATE)                            AS trans_end_dt,
                                    cast(NULL AS             VARCHAR(255))                    AS feat_val,
                                    cast(polcov.feat_rate AS DECIMAL(14,4))                   AS feat_rate,
                                    cast(polcov.eligible AS  VARCHAR(10))                     AS eligible ,
                                    cast(NULL AS             VARCHAR(255))                    AS feat_covtermtype,
                                    cast(pda.typecode_stg AS VARCHAR(50))                     AS discountsurcharge_alfa_typecd
                    FROM            (
                                               SELECT     patterncode_stg,
                                                          cast(branchid_stg AS VARCHAR(255)) AS branchid,
                                                          ''FEAT_SBTYPE11''                    AS typ,
                                                          effectivedate_stg,
                                                          expirationdate_stg,
                                                          cast(ratemodifier_stg AS           VARCHAR(255)) AS ratemodifier,
                                                          cast(discountsurcharge_alfa_stg AS VARCHAR(255)) AS discountsurcharge_alfa,
                                                          cast(
                                                          CASE
                                                                     WHEN fop.eligible_stg= 1 THEN fop.ratemodifier_stg
                                                                     ELSE 0
                                                          END AS               VARCHAR(255))         AS feat_rate,
                                                          cast(eligible_stg AS VARCHAR(10))          AS eligible,
                                                          fopdwelling_stg                            AS assetkey,
                                                          cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                          cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                               FROM       db_t_prod_stag.pcx_fopdwellingmodifier fop
                                               inner join db_t_prod_stag.pc_policyperiod pp
                                               ON         pp.id_stg = fop.branchid_stg
                                               WHERE      (
                                                                     expirationdate_stg IS NULL
                                                          OR         expirationdate_stg > editeffectivedate_stg)
                                               AND        pp.updatetime_stg > (:start_dttm)
                                               AND        pp.updatetime_stg <= (:end_dttm) qualify row_number() over (PARTITION BY branchid_stg,assetkey,patterncode_stg ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,fop.updatetime_stg DESC,fop.createtime_stg DESC)=1
                                               UNION
                                               SELECT     patterncode_stg,
                                                          cast(branchid_stg AS VARCHAR(255)) AS branchid,
                                                          ''FEAT_SBTYPE11''                    AS typ,
                                                          effectivedate_stg,
                                                          expirationdate_stg,
                                                          cast(ratemodifier_stg AS           VARCHAR(255)) AS ratemodifier,
                                                          cast(discountsurcharge_alfa_stg AS VARCHAR(255)) AS discountsurcharge_alfa,
                                                          cast(
                                                          CASE
                                                                     WHEN fop.eligible_stg= 1 THEN fop.ratemodifier_stg
                                                                     ELSE 0
                                                          END AS               VARCHAR(255))         AS feat_rate,
                                                          cast(eligible_stg AS VARCHAR(10))          AS eligible,
                                                          fopmachinery_stg                           AS assetkey,
                                                          cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50)) AS assettype_stg,
                                                          cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50)) AS classification_code
                                               FROM       db_t_prod_stag.pcx_fopmachinerymodifier fop
                                               inner join db_t_prod_stag.pc_policyperiod pp
                                               ON         pp.id_stg = fop.branchid_stg
                                               WHERE      (
                                                                     expirationdate_stg IS NULL
                                                          OR         expirationdate_stg > editeffectivedate_stg)
                                               AND        pp.updatetime_stg > (:start_dttm)
                                               AND        pp.updatetime_stg <= (:end_dttm) qualify row_number() over (PARTITION BY branchid_stg,assetkey,patterncode_stg ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,fop.updatetime_stg DESC,fop.createtime_stg DESC)=1 ) polcov
                    inner join
                                    (
                                           SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                  policynumber_stg,
                                                  periodstart_stg,
                                                  editeffectivedate_stg,
                                                  branchnumber_stg,
                                                  periodend_stg,
                                                  mostrecentmodel_stg,
                                                  status_stg,
                                                  jobid_stg,
                                                  publicid_stg,
                                                  createtime_stg,
                                                  updatetime_stg,
                                                  retired_stg
                                           FROM   db_t_prod_stag.pc_policyperiod ) pp
                    ON              pp.id = polcov.branchid
                    inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                    ON              pps.id_stg = pp.status_stg
                    inner join      db_t_prod_stag.pc_job pj
                    ON              pj.id_stg = pp.jobid_stg
                    inner join      db_t_prod_stag.pctl_job pcj
                    ON              pcj.id_stg=pj.subtype_stg
                    left join       db_t_prod_stag.pctl_discountsurcharge_alfa pda
                    ON              polcov.discountsurcharge_alfa = pda.id_stg
                    WHERE           pps.typecode_stg <> ''Temporary''
                    AND             pcj.typecode_stg IN (''Submission'',
                                                         ''PolicyChange'',
                                                         ''Renewal'')
                    AND             pp.updatetime_stg > (:start_dttm)
                    AND             pp.updatetime_stg <= (:end_dttm)
                    UNION
                    --Exclusions
                    SELECT DISTINCT pp.policynumber_stg,
                                    pp.periodstart_stg AS pol_start_dt,
                                    CASE
                                                    WHEN polcov.effectivedate_stg IS NULL THEN pp.periodstart_stg
                                                    ELSE polcov.effectivedate_stg
                                    END AS feature_start_dt,
                                    CASE
                                                    WHEN polcov.expirationdate_stg IS NULL THEN pp.periodend_stg
                                                    ELSE polcov.expirationdate_stg
                                    END                                                   AS feature_end_dt,
                                    cast (''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))     AS cntrct_role,
                                    coalesce(polcov.effectivedate_stg,pp.periodstart_stg) AS asset_start_dt,
                                    CASE
                                                    WHEN covterm.covtermtype=''package'' THEN PACKAGE.packagepatternid
                                                    WHEN covterm.covtermtype=''option''
                                                    AND             polcov.val IS NOT NULL THEN optn.optionpatternid
                                                    WHEN covterm.covtermtype=''Clause'' THEN covterm.clausepatternid
                                                    ELSE covterm.covtermpatternid
                                    END AS nk_public_id,
                                    CASE
                                                    WHEN covterm.covtermtype=''package'' THEN cast (''FEAT_SBTYPE9'' AS VARCHAR (50))
                                                    WHEN covterm.covtermtype=''option''
                                                    AND             polcov.val IS NOT NULL THEN cast (''FEAT_SBTYPE8'' AS VARCHAR(50))
                                                    WHEN covterm.covtermtype=''Clause'' THEN cast(''FEAT_SBTYPE7'' AS       VARCHAR(50))
                                                    ELSE cast ( ''FEAT_SBTYPE6'' AS                                       VARCHAR (50))
                                    END                        AS feat_sbtype_cd,
                                    polcov.assettype_stg       AS assettype ,
                                    polcov.classification_code AS classification_code,
                                    polcov.assetkey            AS fixedid,
                                    cast(NULL AS VARCHAR(60))  AS ratesymbolcollision_alfa_stg,
                                    cast(NULL AS VARCHAR(60))  AS ratesymbol_alfa_stg,
                                    jobnumber_stg,
                                    branchnumber_stg,
                                    pp.updatetime_stg                    AS trans_strt_dt,
                                    cast( ''9999-12-31'' AS DATE )         AS trans_end_dt,
                                    cast(polcov.val AS    VARCHAR(255))  AS feat_val,
                                    cast(NULL AS          DECIMAL(14,4)) AS feat_rate,
                                    cast(NULL AS          VARCHAR(5))    AS eligible,
                                    covterm.covtermtype                  AS feat_covtermtype,
                                    cast(NULL AS VARCHAR(50))            AS discountsurcharge_alfa_typecd
                    FROM            (
                                             --pcx_fopdwellingexcl
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      dwelling_stg                               AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             booleanterm1avl_stg IS NULL
                                                                      AND             booleanterm2avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL ) AS fopdwell qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopdwellingschexclitemexcl
                                             SELECT   *
                                             FROM     (
                                                                      SELECT DISTINCT cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm3'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm3avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''ChoiceTerm4'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(choiceterm4_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           choiceterm4avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''BooleanTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS  VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS     VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           booleanterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DateTerm1'' AS VARCHAR(250)) AS columnname,
                                                                                      CASE
                                                                                                      WHEN dateterm1_stg IS NULL THEN cast(dateterm1_stg AS VARCHAR(255))
                                                                                                      ELSE cast(to_date(to_varchar(dateterm1_stg), ''MM/DD/YYYY'') AS     VARCHAR(255))
                                                                                      END                                   AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           dateterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''DirectTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           directterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''StringTerm2'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm2_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm2avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      fopdwellingscheduleexclitem_stg            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE40'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN18'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschexclitemexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             booleanterm1avl_stg IS NULL
                                                                      AND             choiceterm1avl_stg IS NULL
                                                                      AND             choiceterm2avl_stg IS NULL
                                                                      AND             choiceterm3avl_stg IS NULL
                                                                      AND             choiceterm4avl_stg IS NULL
                                                                      AND             dateterm1avl_stg IS NULL
                                                                      AND             directterm1avl_stg IS NULL
                                                                      AND             directterm2avl_stg IS NULL
                                                                      AND             stringterm1avl_stg IS NULL
                                                                      AND             stringterm2avl_stg IS NULL ) AS fopdwellsch qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1
                                             UNION
                                             --pcx_fopoutbuildingexcl
                                             SELECT   *
                                             FROM    (
                                                                      SELECT DISTINCT cast(''StringTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                      cast(stringterm1_stg AS VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           stringterm1avl_stg = 1
                                                                      AND             (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      UNION
                                                                      SELECT DISTINCT cast(''Clause'' AS        VARCHAR(250)) AS columnname,
                                                                                      cast(NULL AS            VARCHAR(255)) AS val,
                                                                                      cast(patterncode_stg AS VARCHAR(250))    patterncode_stg,
                                                                                      cast(branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                      dwell.createtime_stg,
                                                                                      effectivedate_stg,
                                                                                      expirationdate_stg,
                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                      dwell.updatetime_stg,
                                                                                      outbuilding_stg                            AS assetkey,
                                                                                      cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50)) AS assettype_stg,
                                                                                      cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50)) AS classification_code
                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingexcl dwell
                                                                      inner join      db_t_prod_stag.pc_policyperiod pp
                                                                      ON              pp.id_stg = dwell.branchid_stg
                                                                      AND             pp.updatetime_stg > (:start_dttm)
                                                                      AND             pp.updatetime_stg <= (:end_dttm)
                                                                      WHERE           (
                                                                                                      expirationdate_stg IS NULL
                                                                                      OR              expirationdate_stg > editeffectivedate_stg)
                                                                      AND             stringterm1avl_stg IS NULL ) AS foput qualify row_number() over (PARTITION BY branchid,assetkey,patterncode_stg,columnname ORDER BY coalesce(expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,updatetime_stg DESC,createtime_stg DESC)=1 ) polcov
                    inner join
                                    (
                                           SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                  policynumber_stg,
                                                  periodstart_stg,
                                                  periodend_stg,
                                                  mostrecentmodel_stg,
                                                  status_stg,
                                                  branchnumber_stg,
                                                  jobid_stg,
                                                  publicid_stg,
                                                  updatetime_stg,
                                                  retired_stg
                                           FROM   db_t_prod_stag.pc_policyperiod ) pp
                    ON              pp.id = polcov.branchid
                    left join
                                    (
                                           SELECT cast(cls.patternid_stg AS   VARCHAR(255)) AS clausepatternid,
                                                  cast(cov.patternid_stg AS   VARCHAR(255)) AS covtermpatternid,
                                                  cast(cov.columnname_stg AS  VARCHAR(255)) AS columnname,
                                                  cast(cov.covtermtype_stg AS VARCHAR(100)) AS covtermtype,
                                                  cast(cls.name_stg AS        VARCHAR(255)) AS clausename
                                           FROM   db_t_prod_stag.pc_etlclausepattern cls
                                           join   db_t_prod_stag.pc_etlcovtermpattern cov
                                           ON     cls.id_stg=cov.clausepatternid_stg
                                           UNION
                                           SELECT    cls.patternid_stg                      AS clausepatternid,
                                                     cov.patternid_stg                      AS covtermpatternid,
                                                     coalesce(cov.columnname_stg,''Clause'')  AS columnname,
                                                     coalesce(cov.covtermtype_stg,''Clause'') AS covtermtype,
                                                     cls.name_stg                           AS clausename
                                           FROM      db_t_prod_stag.pc_etlclausepattern cls
                                           left join
                                                     (
                                                            SELECT *
                                                            FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                            WHERE  name_stg NOT LIKE ''ZZ%'') cov
                                           ON        cov.clausepatternid_stg=cls.id_stg
                                           WHERE     cls.name_stg NOT LIKE ''ZZ%''
                                           AND       cov.name_stg IS NULL
                                           AND       owningentitytype_stg IN (''FOPBlanket'',
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
                    ON              covterm.clausepatternid=polcov.patterncode_stg
                    AND             covterm.columnname=polcov.columnname
                    left outer join
                                    (
                                           SELECT pkg.patternid_stg   AS packagepatternid,
                                                  pkg.packagecode_stg AS cov_id,
                                                  pkg.packagecode_stg AS name1
                                           FROM   db_t_prod_stag.pc_etlcovtermpackage pkg) PACKAGE
                    ON              PACKAGE.packagepatternid=polcov.val
                    left outer join
                                    (
                                               SELECT     opt.patternid_stg  AS optionpatternid,
                                                          opt.optioncode_stg AS name1,
                                                          opt.value_stg,
                                                          cov.valuetype_stg
                                               FROM       db_t_prod_stag.pc_etlcovtermpattern cov
                                               inner join db_t_prod_stag.pc_etlcovtermoption opt
                                               ON         cov.id_stg=opt.coveragetermpatternid_stg ) optn
                    ON              optn.optionpatternid=polcov.val
                    join            db_t_prod_stag.pctl_policyperiodstatus pps
                    ON              pps.id_stg=pp.status_stg
                    join            db_t_prod_stag.pc_job pj
                    ON              pj.id_stg=pp.jobid_stg
                    join            db_t_prod_stag.pctl_job pcj
                    ON              pcj.id_stg=pj.subtype_stg
                    WHERE           covterm.clausename NOT LIKE''%ZZ%''
                    AND             pps.typecode_stg <> ''Temporary''
                    AND             pcj.typecode_stg IN (''Submission'',
                                                         ''PolicyChange'',
                                                         ''Renewal'')
                    AND             pp.updatetime_stg > (:start_dttm)
                    AND             pp.updatetime_stg <= (:end_dttm)
                    UNION
                    --ENDORSEMENTS
                    SELECT policynumber,
                           pol_strt_dt,
                           feature_strt_dt,
                           feature_end_dt,
                           cntrct_role,
                           asset_strt_dt,
                           nk_public_id,
                           feat_sbtype_cd,
                           cast(assettype AS           VARCHAR(100)) AS typecode,
                           cast(classification_code AS VARCHAR(255)) AS classification_code,
                           fixedid,
                           ratesymbolcollision_alfa ,
                           ratesymbol_alfa,
                           jobnumber,
                           branchnumber,
                           trans_strt_dt,
                           trans_end_dt,
                           feat_val,
                           feat_rate,
                           eligible,
                           feat_covtermtype,
                           discountsurcharge_alfa_typecd
                    FROM   (
                                           SELECT DISTINCT FORM.policynumber,
                                                           FORM.pol_strt_dt,
                                                           FORM.feature_strt_dt,
                                                           FORM.feature_end_dt,
                                                           FORM.cntrct_role,
                                                           FORM.asset_strt_dt,
                                                           FORM.nk_public_id,
                                                           FORM.feat_sbtype_cd,
                                                           FORM.assettype,
                                                           FORM.classification_code,
                                                           FORM.fixedid,
                                                           ratesymbolcollision_alfa,
                                                           ratesymbol_alfa,
                                                           jobnumber,
                                                           branchnumber,
                                                           FORM.updatetime AS trans_strt_dt,
                                                           FORM.src_cd,
                                                           retired,
                                                           FORM.feat_val,
                                                           cast(''9999-12-31'' AS DATE) AS trans_end_dt,
                                                           FORM.feat_covtermtype,
                                                           FORM.feat_rate             AS feat_rate,
                                                           substr (FORM.eligible,1,1) AS eligible,
                                                           discountsurcharge_alfa_typecd,
                                                           addressbookuid,
                                                           row_number() over( PARTITION BY publicid,nk_public_id,feat_sbtype_cd,assettype,classification_code, fixedid,cntrct_role ORDER BY pol_strt_dt DESC) AS rankid
                                           FROM            (
                                                                           SELECT DISTINCT pp.publicid AS publicid,
                                                                                           pp.policynumber,
                                                                                           pp.periodstart AS pol_strt_dt,
                                                                                           CASE
                                                                                                           WHEN polcov.effectivedate IS NULL THEN pp.periodstart
                                                                                                           ELSE polcov.effectivedate
                                                                                           END AS feature_strt_dt,
                                                                                           CASE
                                                                                                           WHEN polcov.expirationdate IS NULL THEN pp.periodend
                                                                                                           ELSE polcov.expirationdate
                                                                                           END                                              AS feature_end_dt,
                                                                                           cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                                           nk_public_id,
                                                                                           cast(''FEAT_SBTYPE15'' AS VARCHAR(50)) AS feat_sbtype_cd,
                                                                                           polcov.assettype_stg                 AS assettype,
                                                                                           polcov.classification_code           AS classification_code,
                                                                                           assetkey                             AS fixedid,
                                                                                           pp.editeffectivedate                 AS asset_strt_dt,
                                                                                           polcov.updatetime                    AS updatetime,
                                                                                           ''SRC_SYS4''                           AS src_cd,
                                                                                           pj.jobnumber_stg                     AS jobnumber,
                                                                                           pp.branchnumber_stg                  AS branchnumber,
                                                                                           cast(NULL AS VARCHAR(100))           AS ratesymbolcollision_alfa,
                                                                                           cast(NULL AS VARCHAR(100))           AS ratesymbol_alfa,
                                                                                           pp.retired                           AS retired,
                                                                                           cast(NULL AS VARCHAR(255))           AS feat_val,
                                                                                           cast(NULL AS DECIMAL(14,4))          AS feat_rate,
                                                                                           cast(NULL AS VARCHAR(5))             AS eligible,
                                                                                           cast(NULL AS VARCHAR(255))           AS feat_covtermtype,
                                                                                           cast(NULL AS VARCHAR(50))            AS discountsurcharge_alfa_typecd,
                                                                                           NULL                                 AS addressbookuid
                                                                           FROM            (-- pcx_fopdwellingcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           dwelling_stg                                AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE37'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopdwellingcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopoutbuildingcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           outbuilding_stg                             AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE36'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN13'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopoutbuildingcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopfeedandseedcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           feedandseed_stg                             AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopfeedandseedcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopmachinerycov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           machinery_stg                               AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopmachinerycov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_foplivestockcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           livestock_stg                               AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE35'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN14'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_foplivestockcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopdwellingschcovitemcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           fopdwellingschedulecovitem_stg              AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE38'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN16'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopfarmownersschcovitemcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           fopfarmownerslischedulecovitem_stg          AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE41'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN19'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm)
                                                                                                           UNION
                                                                                                           -- pcx_fopliabilityschcovitemcov
                                                                                                           SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                           cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                           fopliabilityschedulecovitem_stg             AS assetkey,
                                                                                                                           cast(''PRTY_ASSET_SBTYPE42'' AS  VARCHAR(50))  AS assettype_stg,
                                                                                                                           cast(''PRTY_ASSET_CLASFCN20'' AS VARCHAR(50))  AS classification_code ,
                                                                                                                           a.createtime_stg                             AS createtime ,
                                                                                                                           a.effectivedate_stg                          AS effectivedate ,
                                                                                                                           a.expirationdate_stg                         AS expirationdate ,
                                                                                                                           a.updatetime_stg                             AS updatetime,
                                                                                                                           a.patterncode_stg,
                                                                                                                           e.coveragesubtype_stg,
                                                                                                                           cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                           FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov a
                                                                                                           join            db_t_prod_stag.pc_policyperiod b
                                                                                                           ON              b.id_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_formpattern c
                                                                                                           ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pc_form d
                                                                                                           ON              d.formpatterncode_stg = c.code_stg
                                                                                                           AND             d.branchid_stg = a.branchid_stg
                                                                                                           join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                           ON              e.patternid_stg = a.patterncode_stg
                                                                                                           join            db_t_prod_stag.pctl_documenttype pd
                                                                                                           ON              pd.id_stg = c.documenttype_stg
                                                                                                           AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                           WHERE           ( (
                                                                                                                                                           a.effectivedate_stg IS NULL)
                                                                                                                           OR             (
                                                                                                                                                           a.effectivedate_stg > b.editeffectivedate_stg
                                                                                                                                           AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                           AND             c.retired_stg = 0
                                                                                                           AND             d.removedorsuperseded_stg IS NULL
                                                                                                           AND             b.updatetime_stg > (:start_dttm)
                                                                                                           AND             b.updatetime_stg <= (:end_dttm) )polcov
                                                                           inner join
                                                                                           (
                                                                                                  SELECT cast(id_stg AS VARCHAR(255)) AS id ,
                                                                                                         policynumber_stg             AS policynumber ,
                                                                                                         periodstart_stg              AS periodstart ,
                                                                                                         periodend_stg                AS periodend,
                                                                                                         branchnumber_stg ,
                                                                                                         status_stg            AS status ,
                                                                                                         jobid_stg             AS jobid ,
                                                                                                         publicid_stg          AS publicid ,
                                                                                                         createtime_stg        AS createtime ,
                                                                                                         updatetime_stg        AS updatetime ,
                                                                                                         retired_stg           AS retired ,
                                                                                                         policyid_stg          AS policyid ,
                                                                                                         editeffectivedate_stg AS editeffectivedate
                                                                                                  FROM   db_t_prod_stag.pc_policyperiod ) pp
                                                                           ON              pp.id = polcov.branchid
                                                                           inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                                                                           ON              pps.id_stg = pp.status
                                                                           inner join      db_t_prod_stag.pc_job pj
                                                                           ON              pj.id_stg=pp.jobid
                                                                           inner join      db_t_prod_stag.pctl_job pcj
                                                                           ON              pcj.id_stg=pj.subtype_stg
                                                                           WHERE           pcj.typecode_stg IN (''Submission'',
                                                                                                                ''PolicyChange'',
                                                                                                                ''Renewal'')
                                                                           AND             pps.typecode_stg NOT IN (''Temporary'')
                                                                           AND             pp.updatetime > (:start_dttm)
                                                                           AND             pp.updatetime <= (:end_dttm) ) FORM )tmp
                    WHERE  rankid=1
                    AND    fixedid IS NOT NULL)
    /*EIM-48974- FARM CHANGES ENDS*/
    --Union1
    ,quotn_asset_feat_1 AS
    (
                    SELECT DISTINCT pc_policyperiod.policynumber_stg AS policynumber ,
                                    pc_policyperiod.periodstart_stg  AS pol_start_dt ,
                                    CASE
                                                    WHEN polcov.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                    WHEN polcov.effectivedate_stg IS NOT NULL THEN polcov.effectivedate_stg
                                    END AS feature_start_dt ,
                                    CASE
                                                    WHEN polcov.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                    WHEN polcov.expirationdate_stg IS NOT NULL THEN polcov.expirationdate_stg
                                    END                         AS feature_end_dt ,
                                    ''ASSET_CNTRCT_ROLE_SBTYPE1'' AS cntrct_role ,
                                    CASE
                                                    WHEN polcov.assettype IN (''bp7bldgschedcovitem'',
                                                                              ''bp7classification'') THEN coalesce(pcx_bp7classification.effectivedate_stg,pc_policyperiod.periodstart_stg)
                                    END AS asset_start_dt
                                    /***feature keys***/
                                    ,
                                    CASE
                                                    WHEN covterm.covtermtype = ''package'' THEN PACKAGE.packagepatternid
                                                    WHEN covterm.covtermtype = ''option''
                                                    AND             polcov.val IS NOT NULL THEN optn.optionpatternid
                                                    WHEN covterm.covtermtype = ''Clause'' THEN covterm.clausepatternid
                                                    ELSE covterm.covtermpatternid
                                    END AS nk_public_id ,
                                    CASE
                                                    WHEN covterm.covtermtype = ''package'' THEN ''FEAT_SBTYPE9''
                                                    WHEN covterm.covtermtype = ''option''
                                                    AND             polcov.val IS NOT NULL THEN ''FEAT_SBTYPE8''
                                                    WHEN covterm.covtermtype = ''Clause'' THEN cast( ''FEAT_SBTYPE7'' AS VARCHAR(50))
                                                    ELSE ''FEAT_SBTYPE6''
                                    END AS feat_sbtype_cd
                                    /*******feature keys****/
                                    /*******Party Asset Key*****************************/
                                    ,
                                    CASE
                                                    WHEN polcov.assettype = ''bp7classification'' THEN ''PRTY_ASSET_SBTYPE13''
                                    END AS assettype ,
                                    CASE
                                                    WHEN polcov.assettype = ''bp7classification'' THEN pctl_bp7classificationproperty.typecode_stg
                                    END AS classification_code ,
                                    CASE
                                                    WHEN polcov.assettype = ''bp7classification'' THEN pcx_bp7classification.fixedid_stg
                                    END                        AS fixedid ,
                                    cast(NULL AS VARCHAR(100))    ratesymbolcollision_alfa ,
                                    cast(NULL AS VARCHAR(100))    ratesymbol_alfa ,
                                    jobnumber_stg ,
                                    branchnumber_stg ,
                                    pc_policyperiod.updatetime_stg       AS trans_strt_dt ,
                                    cast( ''9999-12-31'' AS DATE )         AS trans_end_dt ,
                                    cast(polcov.val AS    VARCHAR(255))  AS feat_val ,
                                    cast(NULL AS          DECIMAL(14,4)) AS feat_rate ,
                                    cast(NULL AS          VARCHAR(5))    AS eligible ,
                                    covterm.covtermtype                  AS feat_covtermtype ,
                                    cast(NULL AS VARCHAR(50))            AS discountsurcharge_alfa_typecd
                    FROM
                                    /*******Party Asset Key*****************************/
                                    (
                                           /*pcx_bp7classificationcov*/
                                           SELECT cast(''ChoiceTerm1'' AS   VARCHAR(100))    AS columnname,
                                                  cast(choiceterm1_stg AS VARCHAR(255))    AS val,
                                                  cast(patterncode_stg AS VARCHAR(255))    AS patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS  VARCHAR(255)) AS assetkey,
                                                  cast(''bp7classification'' AS VARCHAR(100)) AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm1avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm2''   AS columnname,
                                                  choiceterm2_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm2avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm3''   AS columnname,
                                                  choiceterm3_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm3avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm4''   AS columnname,
                                                  choiceterm4_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm4avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm5''   AS columnname,
                                                  choiceterm5_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm5avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm1''                         AS columnname,
                                                  cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  directterm1avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm2''                         AS columnname,
                                                  cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  directterm2avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm3''                         AS columnname,
                                                  cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  directterm3avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm1''                         AS columnname,
                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  booleanterm1avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm2''                         AS columnname,
                                                  cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  booleanterm2avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm3''                         AS columnname,
                                                  cast(booleanterm3_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  booleanterm3avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''StringTerm1''   AS columnname,
                                                  stringterm1_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  stringterm1avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''StringTerm2''   AS columnname,
                                                  stringterm2_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  stringterm2avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''StringTerm3''   AS columnname,
                                                  stringterm3_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  stringterm3avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''StringTerm4''   AS columnname,
                                                  stringterm4_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  stringterm4avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''StringTerm5''   AS columnname,
                                                  stringterm5_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  stringterm5avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DateTerm1''                         AS columnname,
                                                  cast(dateterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  dateterm1avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DateTerm2''                         AS columnname,
                                                  cast(dateterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  dateterm2avl_stg = 1
                                           AND    expirationdate_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''Clause''                   AS columnname,
                                                  cast(NULL AS VARCHAR(255))    val,
                                                  patterncode_stg,
                                                  branchid_stg                             AS branchid,
                                                  cast(classification_stg AS VARCHAR(255)) AS assetkey,
                                                  ''bp7classification''                      AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                           WHERE  choiceterm1avl_stg IS NULL
                                           AND    updatetime_stg>(:start_dttm)
                                           AND    updatetime_stg <= (:end_dttm)
                                           AND    choiceterm2avl_stg IS NULL
                                           AND    choiceterm3avl_stg IS NULL
                                           AND    choiceterm4avl_stg IS NULL
                                           AND    choiceterm5avl_stg IS NULL
                                           AND    directterm1avl_stg IS NULL
                                           AND    directterm2avl_stg IS NULL
                                           AND    directterm3avl_stg IS NULL
                                           AND    booleanterm1avl_stg IS NULL
                                           AND    booleanterm2avl_stg IS NULL
                                           AND    booleanterm3avl_stg IS NULL
                                           AND    stringterm1avl_stg IS NULL
                                           AND    stringterm2avl_stg IS NULL
                                           AND    stringterm3avl_stg IS NULL
                                           AND    stringterm4avl_stg IS NULL
                                           AND    stringterm5avl_stg IS NULL
                                           AND    dateterm1avl_stg IS NULL
                                           AND    dateterm2avl_stg IS NULL
                                           AND    expirationdate_stg IS NULL
                                           UNION
                                           /*pcx_bp7bldgschedcovitemcov*/
                                           SELECT    cast(''ChoiceTerm1'' AS   VARCHAR(100))      AS columnname,
                                                     cast(choiceterm1_stg AS VARCHAR(255))      AS val,
                                                     cast(patterncode_stg AS VARCHAR(255))      AS patterncode_stg ,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS  VARCHAR(255)) AS assetkey,
                                                     cast(''bp7bldgschedcovitem'' AS VARCHAR(100)) AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       choiceterm1avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm2''   AS columnname,
                                                     choiceterm2_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       choiceterm2avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm3''   AS columnname,
                                                     choiceterm3_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       choiceterm3avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm4''   AS columnname,
                                                     choiceterm4_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       choiceterm4avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm5''   AS columnname,
                                                     choiceterm5_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       choiceterm5avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DirectTerm1''                         AS columnname,
                                                     cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       directterm1avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DirectTerm2''                         AS columnname,
                                                     cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       directterm2avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DirectTerm3''                         AS columnname,
                                                     cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       directterm3avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm1''                         AS columnname,
                                                     cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       booleanterm1avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm2''                         AS columnname,
                                                     cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       booleanterm2avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm3''                         AS columnname,
                                                     cast(booleanterm3_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       booleanterm3avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm1''   AS columnname,
                                                     stringterm1_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       stringterm1avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm2''   AS columnname,
                                                     stringterm2_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       stringterm2avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DateTerm1''                         AS columnname,
                                                     cast(dateterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       dateterm1avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DateTerm2''                         AS columnname,
                                                     cast(dateterm2_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       dateterm2avl_stg = 1
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''Clause''                  AS columnname,
                                                     cast(NULL AS VARCHAR(255))AS val,
                                                     patterncode_stg,
                                                     branchid_stg                               AS branchid,
                                                     cast(bldgschedcovitem_stg AS VARCHAR(255)) AS assetkey,
                                                     ''bp7bldgschedcovitem''                      AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg,
                                                     pc_etlclausepattern.patternid_stg AS patternid,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_bp7bldgschedcovitemcov
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg = pcx_bp7bldgschedcovitemcov.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg = ''BP7LossPayableItem''
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg>(:start_dttm)
                                           AND       pcx_bp7bldgschedcovitemcov.updatetime_stg <= (:end_dttm)
                                           AND       choiceterm1avl_stg IS NULL
                                           AND       choiceterm2avl_stg IS NULL
                                           AND       choiceterm3avl_stg IS NULL
                                           AND       choiceterm4avl_stg IS NULL
                                           AND       choiceterm5avl_stg IS NULL
                                           AND       directterm1avl_stg IS NULL
                                           AND       directterm2avl_stg IS NULL
                                           AND       directterm3avl_stg IS NULL
                                           AND       booleanterm1avl_stg IS NULL
                                           AND       booleanterm2avl_stg IS NULL
                                           AND       booleanterm3avl_stg IS NULL
                                           AND       stringterm1avl_stg IS NULL
                                           AND       stringterm2avl_stg IS NULL
                                           AND       dateterm1avl_stg IS NULL
                                           AND       dateterm2avl_stg IS NULL
                                           AND       expirationdate_stg IS NULL ) polcov
                    inner join
                                    (
                                           SELECT id_stg AS id ,
                                                  policynumber_stg ,
                                                  periodstart_stg ,
                                                  periodend_stg ,
                                                  mostrecentmodel_stg ,
                                                  status_stg ,
                                                  jobid_stg ,
                                                  publicid_stg ,
                                                  createtime_stg ,
                                                  updatetime_stg ,
                                                  branchnumber_stg ,
                                                  editeffectivedate_stg
                                           FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                    ON              pc_policyperiod.id = polcov.branchid
                    left join
                                    (
                                           SELECT cast(pc_etlclausepattern.patternid_stg AS    VARCHAR(255)) AS clausepatternid ,
                                                  cast(pc_etlcovtermpattern.patternid_stg AS   VARCHAR(255)) AS covtermpatternid ,
                                                  cast(pc_etlcovtermpattern.columnname_stg AS  VARCHAR(100)) AS columnname ,
                                                  cast(pc_etlcovtermpattern.covtermtype_stg AS VARCHAR(100)) AS covtermtype ,
                                                  cast(pc_etlclausepattern.name_stg AS         VARCHAR(255)) AS clausename
                                           FROM   db_t_prod_stag.pc_etlclausepattern
                                           join   db_t_prod_stag.pc_etlcovtermpattern
                                           ON     pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausepatternid_stg
                                           UNION
                                           SELECT    pc_etlclausepattern.patternid_stg                          AS clausepatternid ,
                                                     pc_etlcovtermpattern.patternid_stg                         AS covtermpatternid ,
                                                     coalesce( pc_etlcovtermpattern.columnname_stg, ''Clause'' )  AS columnname ,
                                                     coalesce( pc_etlcovtermpattern.covtermtype_stg, ''Clause'' ) AS covtermtype ,
                                                     pc_etlclausepattern.name_stg                               AS clausename
                                           FROM      db_t_prod_stag.pc_etlclausepattern
                                           left join
                                                     (
                                                            SELECT *
                                                            FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                            WHERE  name_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern
                                           ON        pc_etlcovtermpattern.clausepatternid_stg = pc_etlclausepattern.id_stg
                                           WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                           AND       pc_etlcovtermpattern.name_stg IS NULL
                                           AND       owningentitytype_stg IN ( ''BP7ClassificationCov'',
                                                                              ''BP7BldgSchedCovItemCov'') ) covterm
                    ON              covterm.clausepatternid = polcov.patterncode_stg
                    AND             covterm.columnname = polcov.columnname
                    left outer join
                                    (
                                           SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid ,
                                                  pc_etlcovtermpackage.packagecode_stg AS cov_id ,
                                                  pc_etlcovtermpackage.packagecode_stg AS name_stg
                                           FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                    ON              PACKAGE.packagepatternid = polcov.val
                    left outer join
                                    (
                                               SELECT     pc_etlcovtermoption.patternid_stg  AS optionpatternid ,
                                                          pc_etlcovtermoption.optioncode_stg AS name_stg ,
                                                          pc_etlcovtermoption.value_stg ,
                                                          pc_etlcovtermpattern.valuetype_stg
                                               FROM       db_t_prod_stag.pc_etlcovtermpattern
                                               inner join db_t_prod_stag.pc_etlcovtermoption
                                               ON         pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                    ON              optn.optionpatternid = polcov.val
                    inner join      db_t_prod_stag.pctl_policyperiodstatus
                    ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                    inner join      db_t_prod_stag.pc_job
                    ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                    inner join      db_t_prod_stag.pctl_job
                    ON              pctl_job.id_stg = pc_job.subtype_stg
                    left outer join
                                    (
                                             SELECT   fixedid_stg,
                                                      effectivedate_stg,
                                                      expirationdate_stg,
                                                      bp7classpropertytype_stg,
                                                      branchid_stg,
                                                      rank() over (PARTITION BY fixedid_stg ORDER BY updatetime_stg DESC) r
                                             FROM     db_t_prod_stag.pcx_bp7classification) pcx_bp7classification
                    ON              cast(pcx_bp7classification.fixedid_stg AS VARCHAR(255)) = cast(polcov.assetkey AS VARCHAR(255))
                    AND             polcov.assettype = ''bp7classification''
                    AND             pcx_bp7classification.expirationdate_stg IS NULL
                    AND             pcx_bp7classification.r = 1
                    left outer join db_t_prod_stag.pctl_bp7classificationproperty
                    ON              pctl_bp7classificationproperty.id_stg = pcx_bp7classification.bp7classpropertytype_stg
                    WHERE           covterm.clausename NOT LIKE''%ZZ%''
                    AND             pctl_job.typecode_stg IN ( ''Submission'',
                                                              ''PolicyChange'',
                                                              ''Renewal'' )
                    AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                    AND             (
                                                    pcx_bp7classification.branchid_stg = pc_policyperiod.id)) ,
    --Union2
    quotn_asset_feat_2 AS
    (
               SELECT     pc_policyperiod.policynumber_stg AS policynumber,
                          pc_policyperiod.periodstart_stg  AS pol_start_dt,
                          CASE
                                     WHEN polcov.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                     ELSE polcov.effectivedate_stg
                          END AS feature_start_dt,
                          CASE
                                     WHEN polcov.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                     ELSE polcov.expirationdate_stg
                          END                         AS feature_end_dt,
                          ''ASSET_CNTRCT_ROLE_SBTYPE1'' AS cntrct_role,
                          CASE
                                     WHEN polcov.assettype=''dwelling_hoe'' THEN coalesce(polcov.effectivedate_stg,pc_policyperiod.periodstart_stg)
                                     WHEN polcov.assettype=''personalvehicle'' THEN coalesce(polcov.effectivedate_stg,pc_policyperiod.periodstart_stg)
                          END                    AS asset_start_dt,
                          polcov.patterncode_stg AS nk_public_id,
                          ''FEAT_SBTYPE11''        AS feat_sbtype_cd,
                          CASE
                                     WHEN polcov.assettype IN (''dwelling_hoe'') THEN ''PRTY_ASSET_SBTYPE5''
                                     WHEN polcov.assettype IN (''personalvehicle'') THEN ''PRTY_ASSET_SBTYPE4''
                          END AS assettype,
                          CASE
                                     WHEN polcov.assettype=''dwelling_hoe'' THEN ''PRTY_ASSET_CLASFCN1''
                                     WHEN polcov.assettype=''personalvehicle'' THEN ''PRTY_ASSET_CLASFCN3''
                          END AS classification_code,
                          CASE
                                     WHEN polcov.assettype=''dwelling_hoe'' THEN polcov.fixedid_stg
                                     WHEN polcov.assettype=''personalvehicle'' THEN polcov.fixedid_stg
                          END                                            AS fixedid,
                          cast(NULL AS VARCHAR(100))                     AS ratesymbolcollision_alfa,
                          cast(NULL AS VARCHAR(100))                     AS ratesymbol_alfa,
                          jobnumber_stg                                  AS jobnumber,
                          branchnumber_stg                               AS branchnumber,
                          pc_policyperiod.updatetime_stg                 AS trans_strt_dt,
                          cast(''9999-12-31'' AS            DATE)          AS trans_end_dt,
                          cast(NULL AS                    VARCHAR(255))  AS feat_val,
                          cast(polcov.ratemodifier_stg AS DECIMAL(14,4)) AS feat_rate,
                          cast(polcov.eligible_stg AS     VARCHAR(5))    AS eligible,
                          cast(NULL AS                    VARCHAR(60))   AS feat_covtermtype,
                          pctl_discountsurcharge_alfa.typecode_stg       AS discountsurcharge_alfa_typecd
               FROM       (
                                    --For Property
                                    SELECT    branchid_stg,
                                              updatetime_stg,
                                              patterncode_stg,
                                              cast(dwelling_stg AS   VARCHAR(100)) AS coverableid,
                                              cast(''dwelling_hoe'' AS VARCHAR(100)) AS assettype,
                                              fixedid_stg,
                                              effectivedate_stg,
                                              expirationdate_stg,
                                              dwelling_stg AS unitid,
                                              ratemodifier_stg,
                                              discountsurcharge_alfa_stg,
                                              eligible_stg,
                                              pctl_discountsurcharge_alfa.typecode_stg
                                    FROM      db_t_prod_stag.pcx_dwellingmodifier_hoe
                                    left join db_t_prod_stag.pctl_discountsurcharge_alfa
                                    ON        pcx_dwellingmodifier_hoe.discountsurcharge_alfa_stg = pctl_discountsurcharge_alfa.id_stg
                                    WHERE     pcx_dwellingmodifier_hoe.updatetime_stg>(:start_dttm)
                                    AND       pcx_dwellingmodifier_hoe.updatetime_stg <= (:end_dttm)
                                    UNION ALL
                                    --For Auto
                                    SELECT    branchid_stg,
                                              updatetime_stg,
                                              patterncode_stg,
                                              cast(pavehicle_stg AS     VARCHAR(100))    coverableid,
                                              cast(''personalvehicle'' AS VARCHAR(100)) AS assettype,
                                              fixedid_stg,
                                              effectivedate_stg,
                                              expirationdate_stg,
                                              pavehicle_stg,
                                              ratemodifier_stg,
                                              discountsurcharge_alfa_stg,
                                              eligible_stg,
                                              pctl_discountsurcharge_alfa.typecode_stg
                                    FROM      db_t_prod_stag.pc_pavehmodifier
                                    left join db_t_prod_stag.pctl_discountsurcharge_alfa
                                    ON        pc_pavehmodifier.discountsurcharge_alfa_stg = pctl_discountsurcharge_alfa.id_stg
                                    WHERE     pc_pavehmodifier.updatetime_stg>(:start_dttm)
                                    AND       pc_pavehmodifier.updatetime_stg <= (:end_dttm) ) polcov
               inner join
                          (
                                 SELECT id_stg AS id,
                                        policynumber_stg,
                                        branchnumber_stg,
                                        periodstart_stg,
                                        periodend_stg,
                                        mostrecentmodel_stg,
                                        status_stg,
                                        jobid_stg,
                                        publicid_stg,
                                        updatetime_stg,
                                        retired_stg
                                 FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
               ON         pc_policyperiod.id = polcov.branchid_stg
               inner join db_t_prod_stag.pc_etlmodifierpattern
               ON         polcov.patterncode_stg=pc_etlmodifierpattern.patternid_stg
               inner join db_t_prod_stag.pctl_discountsurcharge_alfa
               ON         pctl_discountsurcharge_alfa.id_stg=polcov.discountsurcharge_alfa_stg
               inner join db_t_prod_stag.pctl_policyperiodstatus
               ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
               inner join db_t_prod_stag.pc_job
               ON         pc_job.id_stg=pc_policyperiod.jobid_stg
               inner join db_t_prod_stag.pctl_job
               ON         pctl_job.id_stg=pc_job.subtype_stg
               WHERE      (
                                     polcov.expirationdate_stg>(current_date)
                          OR         polcov.expirationdate_stg IS NULL)
               AND        pctl_policyperiodstatus.typecode_stg <>''Bound'' ),
    --Union3
    -- EIM-36312 BOP endorsements
    quotn_asset_feat_3 AS
    (
           SELECT policynumber,
                  pol_strt_dt,
                  feature_strt_dt,
                  feature_end_dt,
                  cntrct_role,
                  asset_strt_dt,
                  nk_public_id,
                  feat_sbtype_cd,
                  cast(typecode AS            VARCHAR(100)) AS typecode,
                  cast(classification_code AS VARCHAR(255)) AS classification_code,
                  fixed_id,
                  ratesymbolcollision_alfa ,
                  ratesymbol_alfa,
                  jobnumber,
                  branchnumber,
                  trans_strt_dt,
                  trans_end_dt,
                  feat_val,
                  feat_rate,
                  eligible,
                  feat_covtermtype,
                  discountsurcharge_alfa_typecd
           FROM   (
                                  SELECT DISTINCT pc_quotn_asset_feat_x.policynumber,
                                                  pc_quotn_asset_feat_x.pol_strt_dt,
                                                  pc_quotn_asset_feat_x.feature_strt_dt,
                                                  pc_quotn_asset_feat_x.feature_end_dt,
                                                  pc_quotn_asset_feat_x.cntrct_role,
                                                  pc_quotn_asset_feat_x.asset_strt_dt,
                                                  pc_quotn_asset_feat_x.nk_public_id,
                                                  pc_quotn_asset_feat_x.feat_sbtype_cd,
                                                  pc_quotn_asset_feat_x.typecode,
                                                  pc_quotn_asset_feat_x.classification_code,
                                                  pc_quotn_asset_feat_x.fixed_id,
                                                  ratesymbolcollision_alfa,
                                                  ratesymbol_alfa,
                                                  jobnumber,
                                                  branchnumber,
                                                  pc_quotn_asset_feat_x.updatetime AS trans_strt_dt,
                                                  pc_quotn_asset_feat_x.src_cd,
                                                  retired,
                                                  pc_quotn_asset_feat_x.feat_val,
                                                  cast(''9999-12-31'' AS DATE) AS trans_end_dt,
                                                  pc_quotn_asset_feat_x.feat_covtermtype,
                                                  pc_quotn_asset_feat_x.polcov_ratemodifier          AS feat_rate,
                                                  substr (pc_quotn_asset_feat_x.polcov_eligible,1,1) AS eligible,
                                                  discountsurcharge_alfa_typecd,
                                                  addressbookuid,
                                                  row_number() over( PARTITION BY publicid,nk_public_id,feat_sbtype_cd,typecode,classification_code, fixed_id,cntrct_role ORDER BY pol_strt_dt DESC) AS rankid
                                  FROM            (
                                                                  SELECT DISTINCT pc_policyperiod.publicid     AS publicid,
                                                                                  pc_policyperiod.policynumber AS policynumber,
                                                                                  pc_policyperiod.periodstart  AS pol_strt_dt,
                                                                                  CASE
                                                                                                  WHEN polcov.effectivedate IS NULL THEN pc_policyperiod.periodstart
                                                                                                  ELSE polcov.effectivedate
                                                                                  END AS feature_strt_dt,
                                                                                  CASE
                                                                                                  WHEN polcov.expirationdate IS NULL THEN pc_policyperiod.periodend
                                                                                                  ELSE polcov.expirationdate
                                                                                  END                         AS feature_end_dt,
                                                                                  ''ASSET_CNTRCT_ROLE_SBTYPE1'' AS cntrct_role,
                                                                                  nk_public_id,
                                                                                  ''FEAT_SBTYPE15'' AS feat_sbtype_cd,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg IN ( ''BP7Classification'') THEN ''PRTY_ASSET_SBTYPE13''
                                                                                                  WHEN polcov.assettype_stg IN ( ''BP7Building'') THEN ''PRTY_ASSET_SBTYPE32''
                                                                                  END AS typecode,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg IN ( ''BP7Classification'' ) THEN pctl_bp7classificationproperty.typecode_stg
                                                                                                  WHEN polcov.assettype_stg IN (''BP7Building'') THEN ''PRTY_ASSET_CLASFCN10''
                                                                                  END                                                                       AS classification_code,
                                                                                  pol.fixedid_stg                                                           AS fixed_id,
                                                                                  coalesce(pol.effectivedate_stg,pc_policyperiod.periodstart)               AS asset_strt_dt,
                                                                                  polcov.updatetime                                                         AS updatetime,
                                                                                  ''SRC_SYS4''                                                                AS src_cd,
                                                                                  cast(NULL AS VARCHAR(100))                                                AS ratesymbolcollision_alfa,
                                                                                  cast(NULL AS VARCHAR(100))                                                AS ratesymbol_alfa,
                                                                                  pc_policyperiod.retired                                                   AS retired,
                                                                                  pc_job.jobnumber_stg                                                      AS jobnumber,
                                                                                  pc_policyperiod.branchnumber_stg                                          AS branchnumber,
                                                                                  cast(NULL AS VARCHAR(255))                                                AS feat_val,
                                                                                  cast(NULL AS DECIMAL(14,4))                                               AS polcov_ratemodifier,
                                                                                  cast(NULL AS VARCHAR(5))                                                  AS polcov_eligible,
                                                                                  cast(NULL AS VARCHAR(255))                                                AS feat_covtermtype,
                                                                                  cast(NULL AS VARCHAR(50))                                                 AS discountsurcharge_alfa_typecd,
                                                                                  coalesce(pc_con_ins.addressbookuid_ins_stg, pc_con.addressbookuid_int_stg)   addressbookuid
                                                                  FROM            (-- BP7Classification
                                                                                                  SELECT DISTINCT d.formpatterncode_stg                AS nk_public_id,
                                                                                                                  cast(a.branchid_stg AS       VARCHAR(255)) AS branchid,
                                                                                                                  cast(a.classification_stg AS VARCHAR(255)) AS assetkey,
                                                                                                                  cast( ''BP7Classification'' AS VARCHAR(255))    assettype_stg ,
                                                                                                                  a.createtime_stg                           AS createtime ,
                                                                                                                  a.effectivedate_stg                        AS effectivedate ,
                                                                                                                  a.expirationdate_stg                       AS expirationdate ,
                                                                                                                  a.updatetime_stg                           AS updatetime,
                                                                                                                  a.patterncode_stg,
                                                                                                                  e.coveragesubtype_stg
                                                                                                  FROM            db_t_prod_stag.pcx_bp7classificationcov a
                                                                                                  join            db_t_prod_stag.pc_policyperiod b
                                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_formpattern c
                                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pc_form d
                                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pctl_documenttype pd
                                                                                                  ON              pd.id_stg = c.documenttype_stg
                                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                  WHERE           ( (
                                                                                                                                                  a.effectivedate_stg IS NULL)
                                                                                                                  OR             (
                                                                                                                                                  a.effectivedate_stg > b.modeldate_stg
                                                                                                                                  AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                  AND             c.retired_stg = 0
                                                                                                  AND             d.removedorsuperseded_stg IS NULL
                                                                                                  AND             b.updatetime_stg > (:start_dttm)
                                                                                                  AND             b.updatetime_stg <= (:end_dttm)
                                                                                                  UNION
                                                                                                  --BP7Building
                                                                                                  SELECT DISTINCT d.formpatterncode_stg                AS nk_public_id,
                                                                                                                  cast(a.branchid_stg AS VARCHAR(255)) AS branchid,
                                                                                                                  cast(a.building_stg AS VARCHAR(255)) AS assetkey,
                                                                                                                  cast( ''BP7Building'' AS VARCHAR(255))    assettype_stg ,
                                                                                                                  a.createtime_stg                     AS createtime ,
                                                                                                                  a.effectivedate_stg                  AS effectivedate ,
                                                                                                                  a.expirationdate_stg                 AS expirationdate ,
                                                                                                                  a.updatetime_stg                     AS updatetime,
                                                                                                                  a.patterncode_stg,
                                                                                                                  e.coveragesubtype_stg
                                                                                                  FROM            db_t_prod_stag.pcx_bp7buildingcov a
                                                                                                  join            db_t_prod_stag.pc_policyperiod b
                                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_formpattern c
                                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pc_form d
                                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pctl_documenttype pd
                                                                                                  ON              pd.id_stg = c.documenttype_stg
                                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                  WHERE           ( (
                                                                                                                                                  a.effectivedate_stg IS NULL)
                                                                                                                  OR             (
                                                                                                                                                  a.effectivedate_stg > b.modeldate_stg
                                                                                                                                  AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                  AND             c.retired_stg = 0
                                                                                                  AND             d.removedorsuperseded_stg IS NULL
                                                                                                  AND             b.updatetime_stg > (:start_dttm)
                                                                                                  AND             b.updatetime_stg <= (:end_dttm) )polcov
                                                                  inner join
                                                                                  (
                                                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id ,
                                                                                                policynumber_stg             AS policynumber ,
                                                                                                periodstart_stg              AS periodstart ,
                                                                                                periodend_stg                AS periodend,
                                                                                                branchnumber_stg ,
                                                                                                status_stg     AS status ,
                                                                                                jobid_stg      AS jobid ,
                                                                                                publicid_stg   AS publicid ,
                                                                                                createtime_stg AS createtime ,
                                                                                                updatetime_stg AS updatetime ,
                                                                                                retired_stg    AS retired ,
                                                                                                policyid_stg   AS policyid
                                                                                         FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                                                  ON              pc_policyperiod.id = polcov.branchid
                                                                  inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status
                                                                  left outer join
                                                                                  (
                                                                                                  SELECT DISTINCT fixedid_stg                        AS fixedid_stg,
                                                                                                                  cast(branchid_stg AS         VARCHAR(250))    branchid_stg,
                                                                                                                  cast( ''BP7Classification'' AS VARCHAR(255))    assettype_stg ,
                                                                                                                  effectivedate_stg,
                                                                                                                  expirationdate_stg,
                                                                                                                  cast(bp7classpropertytype_stg AS VARCHAR(255))                                      AS bp7classpropertytype_stg,
                                                                                                                  cast(NULL AS                     VARCHAR(255))                                      AS additionalinterest_stg,
                                                                                                                  cast(NULL AS                     VARCHAR(255))                                      AS additionalinsured_stg,
                                                                                                                  rank() over ( PARTITION BY fixedid_stg , branchid_stg ORDER BY updatetime_stg DESC)    r
                                                                                                  FROM            db_t_prod_stag.pcx_bp7classification
                                                                                                  WHERE           expirationdate_stg IS NULL
                                                                                                  UNION
                                                                                                  SELECT DISTINCT a.fixedid_stg                        AS fixedid_stg,
                                                                                                                  cast(a.branchid_stg AS VARCHAR(250))    branchid_stg,
                                                                                                                  cast( ''BP7Building'' AS VARCHAR(255))    assettype_stg ,
                                                                                                                  a.effectivedate_stg,
                                                                                                                  a.expirationdate_stg,
                                                                                                                  cast(NULL AS VARCHAR(255))                                                                AS bp7classpropertytype_stg,
                                                                                                                  cast(NULL AS VARCHAR(255))                                                                   additionalinterest_stg,
                                                                                                                  cast(NULL AS VARCHAR(255))                                                                   additionalinsured_stg ,
                                                                                                                  rank() over ( PARTITION BY a.fixedid_stg , a.branchid_stg ORDER BY a.updatetime_stg DESC)    r
                                                                                                  FROM            db_t_prod_stag.pcx_bp7building a
                                                                                                  join            db_t_prod_stag.pc_building b
                                                                                                  ON              b.fixedid_stg =a.building_stg
                                                                                                  AND             a.branchid_stg=b.branchid_stg
                                                                                                  WHERE           a.expirationdate_stg IS NULL ) pol
                                                                  ON              cast(polcov.assetkey AS VARCHAR(255))=cast(pol.fixedid_stg AS VARCHAR(255))
                                                                  AND             polcov.branchid=pol.branchid_stg
                                                                  AND             polcov.assettype_stg=pol.assettype_stg
                                                                  AND             r=1
                                                                  left outer join db_t_prod_stag.pctl_bp7classificationproperty
                                                                  ON              pctl_bp7classificationproperty.id_stg = pol.bp7classpropertytype_stg
                                                                  join            db_t_prod_stag.pc_job
                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid
                                                                  join            db_t_prod_stag.pctl_job
                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                  left outer join
                                                                                  (
                                                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                                                  ||''-''
                                                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_int_stg,
                                                                                                                  pc_addlinterestdetail.id_stg                   addlinter_id
                                                                                                  FROM            db_t_prod_stag.pc_addlinterestdetail
                                                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                                                  ON              pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg
                                                                                                  join            db_t_prod_stag.pc_contact
                                                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg )pc_con
                                                                  ON              cast(addlinter_id AS INTEGER) =cast(pol.additionalinterest_stg AS INTEGER)
                                                                  left join
                                                                                  (
                                                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                                                  ||''-''
                                                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_ins_stg,
                                                                                                                  pc_policyaddlinsureddetail.id_stg              addinsuredid
                                                                                                  FROM            db_t_prod_stag.pc_policyaddlinsureddetail
                                                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                                                  ON              pc_policyaddlinsureddetail.policyaddlinsured_stg = pc_policycontactrole.id_stg
                                                                                                  join            db_t_prod_stag.pc_contact
                                                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con_ins
                                                                  ON              cast(addinsuredid AS INTEGER) =cast(pol.additionalinsured_stg AS INTEGER)
                                                                  WHERE           pctl_job.typecode_stg IN (''Submission'',
                                                                                                            ''PolicyChange'',
                                                                                                            ''Renewal'')
                                                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                                                                  AND             pc_policyperiod.updatetime > (:start_dttm)
                                                                  AND             pc_policyperiod.updatetime <= (:end_dttm) )pc_quotn_asset_feat_x ) tmp
           WHERE  rankid=1
           AND    fixed_id IS NOT NULL ),
    --Union4
    --EIM-37450 Endorsements
    quotn_asset_feat_4 AS
    (
           SELECT policynumber,
                  pol_strt_dt,
                  feature_strt_dt,
                  feature_end_dt,
                  cntrct_role,
                  asset_strt_dt,
                  nk_public_id,
                  feat_sbtype_cd,
                  cast(assettype AS           VARCHAR(100)) AS typecode,
                  cast(classification_code AS VARCHAR(255)) AS classification_code,
                  fixedid,
                  ratesymbolcollision_alfa ,
                  ratesymbol_alfa,
                  jobnumber,
                  branchnumber,
                  trans_strt_dt,
                  trans_end_dt,
                  feat_val,
                  feat_rate,
                  eligible,
                  feat_covtermtype,
                  discountsurcharge_alfa_typecd
           FROM   (
                                  SELECT DISTINCT FORM.policynumber,
                                                  FORM.pol_strt_dt,
                                                  FORM.feature_strt_dt,
                                                  FORM.feature_end_dt,
                                                  FORM.cntrct_role,
                                                  FORM.asset_strt_dt,
                                                  FORM.nk_public_id,
                                                  FORM.feat_sbtype_cd,
                                                  FORM.assettype,
                                                  FORM.classification_code,
                                                  FORM.fixedid,
                                                  ratesymbolcollision_alfa,
                                                  ratesymbol_alfa,
                                                  jobnumber,
                                                  branchnumber,
                                                  FORM.updatetime AS trans_strt_dt,
                                                  FORM.src_cd,
                                                  retired,
                                                  FORM.feat_val,
                                                  cast(''9999-12-31'' AS DATE) AS trans_end_dt,
                                                  FORM.feat_covtermtype,
                                                  FORM.feat_rate             AS feat_rate,
                                                  substr (FORM.eligible,1,1) AS eligible,
                                                  discountsurcharge_alfa_typecd,
                                                  addressbookuid,
                                                  row_number() over( PARTITION BY publicid,nk_public_id,feat_sbtype_cd,assettype,classification_code, fixedid,cntrct_role ORDER BY pol_strt_dt DESC) AS rankid
                                  FROM            (
                                                                  SELECT DISTINCT pc_policyperiod.publicid AS publicid,
                                                                                  pc_policyperiod.policynumber,
                                                                                  pc_policyperiod.periodstart AS pol_strt_dt,
                                                                                  CASE
                                                                                                  WHEN polcov.effectivedate IS NULL THEN pc_policyperiod.periodstart
                                                                                                  ELSE polcov.effectivedate
                                                                                  END AS feature_strt_dt,
                                                                                  CASE
                                                                                                  WHEN polcov.expirationdate IS NULL THEN pc_policyperiod.periodend
                                                                                                  ELSE polcov.expirationdate
                                                                                  END                                              AS feature_end_dt,
                                                                                  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50)) AS cntrct_role,
                                                                                  nk_public_id,
                                                                                  cast(''FEAT_SBTYPE15'' AS VARCHAR(50)) AS feat_sbtype_cd,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg IN (''dwelling_hoe'') THEN cast(''PRTY_ASSET_SBTYPE5'' AS            VARCHAR(100))
                                                                                                  WHEN polcov.assettype_stg IN ( ''personalvehicle'') THEN cast(''PRTY_ASSET_SBTYPE4'' AS        VARCHAR(100))
                                                                                                  WHEN polcov.assettype_stg IN (''holineschedcovitem_alfa'') THEN cast(''PRTY_ASSET_SBTYPE5'' AS VARCHAR(100))
                                                                                  END AS assettype,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg IN (''dwelling_hoe'') THEN cast(''PRTY_ASSET_CLASFCN1'' AS        VARCHAR(255))
                                                                                                  WHEN polcov.assettype_stg IN ( ''personalvehicle'' ) THEN cast(''PRTY_ASSET_CLASFCN3'' AS   VARCHAR(255))
                                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN cast(polcov.choiceterm1_stg AS VARCHAR(255))
                                                                                  END AS classification_code,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg=''dwelling_hoe'' THEN pol.fixedid_stg
                                                                                                  WHEN polcov.assettype_stg=''personalvehicle'' THEN pol.fixedid_stg
                                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN pol.fixedid_stg
                                                                                  END AS fixedid,
                                                                                  CASE
                                                                                                  WHEN polcov.assettype_stg=''dwelling_hoe'' THEN pc_policyperiod.editeffectivedate
                                                                                                  WHEN polcov.assettype_stg=''holineschedcovitem_alfa'' THEN pc_policyperiod.editeffectivedate
                                                                                                  WHEN polcov.assettype_stg=''personalvehicle'' THEN pc_policyperiod.editeffectivedate
                                                                                  END                                                                       AS asset_strt_dt,
                                                                                  polcov.updatetime                                                         AS updatetime,
                                                                                  ''SRC_SYS4''                                                                AS src_cd,
                                                                                  pc_job.jobnumber_stg                                                      AS jobnumber,
                                                                                  pc_policyperiod.branchnumber_stg                                          AS branchnumber,
                                                                                  cast(NULL AS VARCHAR(100))                                                AS ratesymbolcollision_alfa,
                                                                                  cast(NULL AS VARCHAR(100))                                                AS ratesymbol_alfa,
                                                                                  pc_policyperiod.retired                                                   AS retired,
                                                                                  cast(NULL AS VARCHAR(255))                                                AS feat_val,
                                                                                  cast(NULL AS DECIMAL(14,4))                                               AS feat_rate,
                                                                                  cast(NULL AS VARCHAR(5))                                                  AS eligible,
                                                                                  cast(NULL AS VARCHAR(255))                                                AS feat_covtermtype,
                                                                                  cast(NULL AS VARCHAR(50))                                                 AS discountsurcharge_alfa_typecd,
                                                                                  coalesce(pc_con_ins.addressbookuid_ins_stg, pc_con.addressbookuid_int_stg)   addressbookuid
                                                                  FROM            (-- Dwelling coverage
                                                                                                  SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                  cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                  a.dwelling_stg                              AS assetkey,
                                                                                                                  cast( ''dwelling_hoe'' AS VARCHAR(255))          assettype_stg ,
                                                                                                                  a.createtime_stg                            AS createtime ,
                                                                                                                  a.effectivedate_stg                         AS effectivedate ,
                                                                                                                  a.expirationdate_stg                        AS expirationdate ,
                                                                                                                  a.updatetime_stg                            AS updatetime,
                                                                                                                  a.patterncode_stg,
                                                                                                                  e.coveragesubtype_stg,
                                                                                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                  FROM            db_t_prod_stag.pcx_dwellingcov_hoe a
                                                                                                  join            db_t_prod_stag.pc_policyperiod b
                                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_formpattern c
                                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pc_form d
                                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pctl_documenttype pd
                                                                                                  ON              pd.id_stg = c.documenttype_stg
                                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                  WHERE           ( (
                                                                                                                                                  a.effectivedate_stg IS NULL)
                                                                                                                  OR             (
                                                                                                                                                  a.effectivedate_stg > b.modeldate_stg
                                                                                                                                  AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                  AND             c.retired_stg = 0
                                                                                                  AND             d.removedorsuperseded_stg IS NULL
                                                                                                  AND             b.updatetime_stg > (:start_dttm)
                                                                                                  AND             b.updatetime_stg <= (:end_dttm)
                                                                                                  UNION
                                                                                                  --Personalvehicle Coverage
                                                                                                  SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255)) AS nk_public_id,
                                                                                                                  cast(a.branchid_stg AS        VARCHAR(255)) AS branchid,
                                                                                                                  a.personalvehicle_stg                       AS assetkey,
                                                                                                                  cast( ''personalvehicle'' AS VARCHAR(255))       assettype_stg ,
                                                                                                                  a.createtime_stg                            AS createtime ,
                                                                                                                  a.effectivedate_stg                         AS effectivedate ,
                                                                                                                  a.expirationdate_stg                        AS expirationdate ,
                                                                                                                  a.updatetime_stg                            AS updatetime,
                                                                                                                  a.patterncode_stg,
                                                                                                                  e.coveragesubtype_stg,
                                                                                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                  FROM            db_t_prod_stag.pc_personalvehiclecov a
                                                                                                  join            db_t_prod_stag.pc_policyperiod b
                                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_formpattern c
                                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pc_form d
                                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pctl_documenttype pd
                                                                                                  ON              pd.id_stg = c.documenttype_stg
                                                                                                  AND             pd.typecode_stg = ''endorsement_alfa''
                                                                                                  WHERE           ( (
                                                                                                                                                  a.effectivedate_stg IS NULL)
                                                                                                                  OR             (
                                                                                                                                                  a.effectivedate_stg > b.modeldate_stg
                                                                                                                                  AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                  AND             c.retired_stg = 0
                                                                                                  AND             d.removedorsuperseded_stg IS NULL
                                                                                                  AND             b.updatetime_stg > (:start_dttm)
                                                                                                  AND             b.updatetime_stg <= (:end_dttm)
                                                                                                  --holineschcovitemcov_alfa Endorsement
                                                                                                  UNION
                                                                                                  SELECT DISTINCT cast(d.formpatterncode_stg AS VARCHAR(255))      AS nk_public_id,
                                                                                                                  cast(a.branchid_stg AS        VARCHAR(255))      AS branchid,
                                                                                                                  a.holineschcovitem_stg                           AS assetkey,
                                                                                                                  cast( ''holineschedcovitem_alfa'' AS VARCHAR(255))    assettype_stg ,
                                                                                                                  a.createtime_stg                                 AS createtime ,
                                                                                                                  a.effectivedate_stg                              AS effectivedate ,
                                                                                                                  a.expirationdate_stg                             AS expirationdate ,
                                                                                                                  a.updatetime_stg                                 AS updatetime,
                                                                                                                  a.patterncode_stg,
                                                                                                                  e.coveragesubtype_stg,
                                                                                                                  cast(g.choiceterm1_stg AS VARCHAR(255)) AS choiceterm1_stg
                                                                                                  FROM            db_t_prod_stag.pcx_holineschcovitemcov_alfa a
                                                                                                  join            db_t_prod_stag.pc_policyperiod b
                                                                                                  ON              b.id_stg = a.branchid_stg
                                                                                                  inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                                  ON              b.status_stg = pps.id_stg
                                                                                                  join            db_t_prod_stag.pc_formpattern c
                                                                                                  ON              c.clausepatterncode_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pc_form d
                                                                                                  ON              d.formpatterncode_stg = c.code_stg
                                                                                                  AND             d.branchid_stg = a.branchid_stg
                                                                                                  join            db_t_prod_stag.pc_etlclausepattern e
                                                                                                  ON              e.patternid_stg = a.patterncode_stg
                                                                                                  join            db_t_prod_stag.pctl_documenttype pd
                                                                                                  ON              pd.id_stg = c.documenttype_stg
                                                                                                  join            db_t_prod_stag.pcx_holineschcovitemcov_alfa g
                                                                                                  ON              g.holineschcovitem_stg = a.holineschcovitem_stg
                                                                                                  AND             g.branchid_stg = a.branchid_stg
                                                                                                  AND             g.expirationdate_stg IS NULL
                                                                                                  AND             g.choiceterm1_stg IS NOT NULL
                                                                                                  WHERE           pd.typecode_stg = ''endorsement_alfa''
                                                                                                  AND             ( (
                                                                                                                                                  a.effectivedate_stg IS NULL)
                                                                                                                  OR             (
                                                                                                                                                  a.effectivedate_stg > b.modeldate_stg
                                                                                                                                  AND             coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                  AND             c.retired_stg = 0
                                                                                                  AND             d.removedorsuperseded_stg IS NULL
                                                                                                  AND             b.updatetime_stg > (:start_dttm)
                                                                                                  AND             b.updatetime_stg <= (:end_dttm) )polcov
                                                                  inner join
                                                                                  (
                                                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id ,
                                                                                                policynumber_stg             AS policynumber ,
                                                                                                periodstart_stg              AS periodstart ,
                                                                                                periodend_stg                AS periodend,
                                                                                                branchnumber_stg ,
                                                                                                status_stg            AS status ,
                                                                                                jobid_stg             AS jobid ,
                                                                                                publicid_stg          AS publicid ,
                                                                                                createtime_stg        AS createtime ,
                                                                                                updatetime_stg        AS updatetime ,
                                                                                                retired_stg           AS retired ,
                                                                                                policyid_stg          AS policyid ,
                                                                                                editeffectivedate_stg AS editeffectivedate
                                                                                         FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                                                  ON              pc_policyperiod.id = polcov.branchid
                                                                  inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status
                                                                  left outer join
                                                                                  (
                                                                                                  SELECT DISTINCT fixedid_stg                                     AS fixedid_stg,
                                                                                                                  cast(''holineschedcovitem_alfa'' AS VARCHAR(255))    assettype_stg,
                                                                                                                  cast(NULL AS                      VARCHAR(255)) AS additionalinterest_stg,
                                                                                                                  cast(NULL AS                      VARCHAR(255)) AS additionalinsured_stg
                                                                                                  FROM            db_t_prod_stag.pcx_holineschedcovitem_alfa
                                                                                                  WHERE           expirationdate_stg IS NULL
                                                                                                  UNION
                                                                                                  SELECT DISTINCT fixedid_stg                          AS fixedid_stg,
                                                                                                                  cast(''dwelling_hoe'' AS VARCHAR(255))    assettype_stg,
                                                                                                                  cast(NULL AS           VARCHAR(255)) AS additionalinterest_stg,
                                                                                                                  cast(NULL AS           VARCHAR(255)) AS additionalinsured_stg
                                                                                                  FROM            db_t_prod_stag.pcx_dwelling_hoe
                                                                                                  WHERE           expirationdate_stg IS NULL
                                                                                                  UNION
                                                                                                  SELECT DISTINCT fixedid_stg                             AS fixedid_stg,
                                                                                                                  cast(''personalvehicle'' AS VARCHAR(255))    assettype_stg,
                                                                                                                  cast(NULL AS              VARCHAR(255)) AS additionalinterest_stg,
                                                                                                                  cast(NULL AS              VARCHAR(255)) AS additionalinsured_stg
                                                                                                  FROM            db_t_prod_stag.pc_personalvehicle
                                                                                                  WHERE           expirationdate_stg IS NULL )pol
                                                                  ON              polcov.assetkey=pol.fixedid_stg
                                                                  AND             polcov.assettype_stg=pol.assettype_stg
                                                                  left outer join
                                                                                  (
                                                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                                                  ||''-''
                                                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_int_stg,
                                                                                                                  pc_addlinterestdetail.id_stg                   addlinter_id
                                                                                                  FROM            db_t_prod_stag.pc_addlinterestdetail
                                                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                                                  ON              pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg
                                                                                                  join            db_t_prod_stag.pc_contact
                                                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg )pc_con
                                                                  ON              cast(addlinter_id AS INTEGER) =cast(pol.additionalinterest_stg AS INTEGER)
                                                                  left join
                                                                                  (
                                                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                                                  ||''-''
                                                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_ins_stg,
                                                                                                                  pc_policyaddlinsureddetail.id_stg              addinsuredid
                                                                                                  FROM            db_t_prod_stag.pc_policyaddlinsureddetail
                                                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                                                  ON              pc_policyaddlinsureddetail.policyaddlinsured_stg = pc_policycontactrole.id_stg
                                                                                                  join            db_t_prod_stag.pc_contact
                                                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con_ins
                                                                  ON              cast(addinsuredid AS INTEGER) =cast(pol.additionalinsured_stg AS INTEGER)
                                                                  inner join      db_t_prod_stag.pc_job
                                                                  ON              pc_job.id_stg=pc_policyperiod.jobid
                                                                  inner join      db_t_prod_stag.pctl_job
                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                  WHERE           pctl_job.typecode_stg IN (''Submission'',
                                                                                                            ''PolicyChange'',
                                                                                                            ''Renewal'')
                                                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                                                                  AND             pc_policyperiod.updatetime > (:start_dttm)
                                                                  AND             pc_policyperiod.updatetime <= (:end_dttm) ) FORM )tmp
           WHERE  rankid=1
           AND    fixedid IS NOT NULL ),
    --Union5
    quotn_asset_feat_5 AS
    (
                    SELECT DISTINCT pc_policyperiod.policynumber_stg AS policynumber ,
                                    pc_policyperiod.periodstart_stg  AS pol_start_dt,
                                    CASE
                                                    WHEN polcov.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                    ELSE polcov.effectivedate_stg
                                    END AS feature_start_dt,
                                    CASE
                                                    WHEN polcov.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                    ELSE polcov.expirationdate_stg
                                    END                         AS feature_end_dt ,
                                    ''ASSET_CNTRCT_ROLE_SBTYPE1'' AS cntrct_role ,
                                    CASE
                                                    WHEN polcov.assettype=''dwelling_hoe'' THEN pc_policyperiod.editeffectivedate_stg
                                                    WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN pc_policyperiod.editeffectivedate_stg
                                                    WHEN polcov.assettype=''personalvehicle'' THEN pc_policyperiod.editeffectivedate_stg
                                                    WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN pc_policyperiod.editeffectivedate_stg
                                                    WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN pc_policyperiod.editeffectivedate_stg
                                    END AS asset_start_dt
                                    /***feature keys***/
                                    ,
                                    CASE
                                                    WHEN covterm.covtermtype_stg=''package'' THEN PACKAGE.packagepatternid
                                                    WHEN covterm.covtermtype_stg=''option''
                                                    AND             polcov.val IS NOT NULL THEN optn.optionpatternid
                                                    WHEN covterm.covtermtype_stg=''Clause'' THEN covterm.clausepatternid
                                                    ELSE covterm.covtermpatternid
                                    END AS nk_public_id ,
                                    CASE
                                                    WHEN covterm.covtermtype_stg=''package'' THEN ''FEAT_SBTYPE9''
                                                    WHEN covterm.covtermtype_stg=''option''
                                                    AND             polcov.val IS NOT NULL THEN ''FEAT_SBTYPE8''
                                                    WHEN covterm.covtermtype_stg=''Clause'' THEN cast(''FEAT_SBTYPE7'' AS VARCHAR(50))
                                                    ELSE ''FEAT_SBTYPE6''
                                    END AS feat_sbtype_cd
                                    /*******feature keys****/
                                    /*******Party Asset Key*****************************/
                                    ,
                                    CASE
                                                    WHEN polcov.assettype            IN (''dwelling_hoe'') THEN ''PRTY_ASSET_SBTYPE5''
                                                    WHEN polcov.assettype            IN (''holineschedcovitem_alfa'')
                                                    AND             polcov.patternid IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                         ''HOSI_SpecificOtherStructureExclItem_alfa'') THEN ''PRTY_ASSET_SBTYPE5''
                                                    WHEN polcov.assettype IN (''holineschedcovitem_alfa'')
                                                    AND             polcov.patternid =''HOSI_ScheduledPropertyItem_alfa'' THEN ''PRTY_ASSET_SBTYPE7''
                                                                    /*''REALSP-PP''*/
                                                    WHEN polcov.assettype IN (''personalvehicle'',
                                                                              ''pawatercraftmotor_alfa'',
                                                                              ''pawatercrafttrailer_alfa'') THEN ''PRTY_ASSET_SBTYPE4''
                                    END AS assettype ,
                                    CASE
                                                    WHEN polcov.assettype=''dwelling_hoe'' THEN ''PRTY_ASSET_CLASFCN1''
                                                    WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN polcov.choiceterm1
                                                    WHEN polcov.assettype=''personalvehicle'' THEN ''PRTY_ASSET_CLASFCN3''
                                                    WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN ''PRTY_ASSET_CLASFCN4''
                                                    WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN ''PRTY_ASSET_CLASFCN5''
                                    END AS classification_code,
                                    CASE
                                                    WHEN polcov.assettype=''dwelling_hoe'' THEN pcx_dwelling_hoe.fixedid_stg
                                                    WHEN polcov.assettype=''holineschedcovitem_alfa'' THEN pcx_holineschedcovitem_alfa.fixedid_stg
                                                    WHEN polcov.assettype=''personalvehicle'' THEN pc_personalvehicle.fixedid_stg
                                                    WHEN polcov.assettype=''pawatercraftmotor_alfa'' THEN pcx_pawatercraftmotor_alfa.fixedid_stg
                                                    WHEN polcov.assettype=''pawatercrafttrailer_alfa'' THEN pcx_pawatercrafttrailer_alfa.fixedid_stg
                                    END AS fixedid,
                                    ratesymbolcollision_alfa_stg,
                                    ratesymbol_alfa_stg,
                                    jobnumber_stg,
                                    branchnumber_stg,
                                    pc_policyperiod.updatetime_stg      AS trans_strt_dt,
                                    cast(''9999-12-31'' AS            DATE)          AS trans_end_dt,
                                    cast(polcov.val AS              VARCHAR(255))  AS feat_val,
                                    cast(NULL AS                    DECIMAL(14,4)) AS feat_rate,
                                    cast(NULL AS                    VARCHAR(5))    AS eligible,
                                    cast(covterm.covtermtype_stg AS VARCHAR(255))  AS feat_covtermtype,
                                    cast(NULL AS                    VARCHAR(60))   AS discountsurcharge_alfa_typecd
                    FROM
                                    /*******Party Asset Key*****************************/
                                    (
                                           SELECT cast(''ChoiceTerm1'' AS   VARCHAR(100)) AS columnname,
                                                  cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                  cast(patterncode_stg AS VARCHAR(255)) AS patterncode_stg,
                                                  branchid_stg                          AS branchid,
                                                  dwelling_stg                          AS assetkey,
                                                  cast(''dwelling_hoe'' AS VARCHAR(100))  AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg >(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm2''   AS columnname,
                                                  choiceterm2_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg ,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm3''   AS columnname,
                                                  choiceterm3_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm3_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm4''   AS columnname,
                                                  choiceterm4_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm4_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm5''   AS columnname,
                                                  choiceterm5_stg AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm5_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm1''                         AS columnname,
                                                  cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  directterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm2'' AS columnname,
                                                  /*TRIM(DirectTerm2_stg (FORMAT ''ZZZZZZZZZ9.9999'') (varchar(255))),-- */
                                                  cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  directterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm3''                         AS columnname,
                                                  cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  directterm3_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm4''                         AS columnname,
                                                  cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  directterm4_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm1''                         AS columnname,
                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  booleanterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm2''                         AS columnname,
                                                  cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  booleanterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''Clause''                   AS columnname,
                                                  cast(NULL AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg   AS branchid,
                                                  dwelling_stg   AS assetkey,
                                                  ''dwelling_hoe'' AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) AS patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_dwellingcov_hoe
                                           WHERE  choiceterm1avl_stg IS NULL
                                           AND    choiceterm2avl_stg IS NULL
                                           AND    choiceterm3avl_stg IS NULL
                                           AND    choiceterm4avl_stg IS NULL
                                           AND    choiceterm5avl_stg IS NULL
                                           AND    directterm1avl_stg IS NULL
                                           AND    directterm2avl_stg IS NULL
                                           AND    directterm3avl_stg IS NULL
                                           AND    directterm4avl_stg IS NULL
                                           AND    booleanterm2avl_stg IS NULL
                                           AND    booleanterm1avl_stg IS NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_dwellingcov_hoe.updatetime_stg>(:start_dttm)
                                           AND    pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm)
                                           /********************************************************************************************/
                                           UNION
                                           /*******************************************pcx_holineschcovitemcov_alfa***************/
                                           SELECT    ''ChoiceTerm1''   AS columnname,
                                                     choiceterm1_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       choiceterm1_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm2''   AS columnname,
                                                     choiceterm2_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       choiceterm2_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm3''   AS columnname,
                                                     choiceterm3_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       choiceterm3_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''ChoiceTerm4''   AS columnname,
                                                     choiceterm4_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       choiceterm4_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DirectTerm1''                         AS columnname,
                                                     cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       directterm1_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DirectTerm2'' AS columnname,
                                                     /*TRIM(DirectTerm2_stg (FORMAT ''ZZZZZZZZZ9.9999'') (varchar(255))),--*/
                                                     cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       directterm2_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm1''                         AS columnname,
                                                     cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       booleanterm1_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm2''                         AS columnname,
                                                     cast(booleanterm2_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       booleanterm2_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm3''                         AS columnname,
                                                     cast(booleanterm3_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       booleanterm3_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm4''                         AS columnname,
                                                     cast(booleanterm4_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       booleanterm4_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''BooleanTerm5''                         AS columnname,
                                                     cast(booleanterm5_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       booleanterm5_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm1''   AS columnname,
                                                     stringterm1_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg ,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       stringterm1_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm2''   AS columnname,
                                                     stringterm2_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       stringterm2_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm3''   AS columnname,
                                                     stringterm3_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       stringterm3_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''StringTerm4''   AS columnname,
                                                     stringterm4_stg AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       stringterm4_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DateTerm1''                         AS columnname,
                                                     cast(dateterm1_stg AS VARCHAR(255)) AS val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       dateterm1_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''DateTerm4''                         AS columnname,
                                                     cast(dateterm4_stg AS VARCHAR(255))    val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg ,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       dateterm4_stg IS NOT NULL
                                           AND       expirationdate_stg IS NULL
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT    ''Clause''                   AS columnname,
                                                     cast(NULL AS VARCHAR(255))    val,
                                                     patterncode_stg,
                                                     branchid_stg              AS branchid,
                                                     holineschcovitem_stg      AS assetkey,
                                                     ''holineschedcovitem_alfa'' AS assettype,
                                                     createtime_stg,
                                                     effectivedate_stg,
                                                     expirationdate_stg ,
                                                     choiceterm1_stg ,
                                                     pc_etlclausepattern.patternid_stg AS patternid ,
                                                     updatetime_stg
                                           FROM      db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                           left join db_t_prod_stag.pc_etlclausepattern
                                           ON        pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                           WHERE     pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                           ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                           ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg>(:start_dttm)
                                           AND       pcx_holineschcovitemcov_alfa.updatetime_stg <= (:end_dttm)
                                           AND       choiceterm1avl_stg IS NULL
                                           AND       choiceterm2avl_stg IS NULL
                                           AND       choiceterm3avl_stg IS NULL
                                           AND       choiceterm4avl_stg IS NULL
                                           AND       choiceterm5avl_stg IS NULL
                                           AND       choiceterm6avl_stg IS NULL
                                           AND       directterm1avl_stg IS NULL
                                           AND       directterm2avl_stg IS NULL
                                           AND       booleanterm1avl_stg IS NULL
                                           AND       booleanterm2avl_stg IS NULL
                                           AND       booleanterm3avl_stg IS NULL
                                           AND       booleanterm4avl_stg IS NULL
                                           AND       booleanterm5avl_stg IS NULL
                                           AND       stringterm1avl_stg IS NULL
                                           AND       stringterm2avl_stg IS NULL
                                           AND       stringterm3avl_stg IS NULL
                                           AND       stringterm4avl_stg IS NULL
                                           AND       dateterm1avl_stg IS NULL
                                           AND       dateterm4avl_stg IS NULL
                                           AND       expirationdate_stg IS NULL
                                           /***********************************************************************************************/
                                           UNION
                                           /*****************************pc_personalvehiclecov************************************************/
                                           SELECT ''ChoiceTerm1''                         AS columnname,
                                                  cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  choiceterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''ChoiceTerm2''                         AS columnname,
                                                  cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  choiceterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm1''                         AS columnname,
                                                  cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  directterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm2''                         AS columnname,
                                                  cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  directterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''BooleanTerm1''                         AS columnname,
                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  booleanterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''Clause''                   AS columnname,
                                                  cast(NULL AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg        AS branchid,
                                                  personalvehicle_stg AS assetkey,
                                                  ''personalvehicle''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pc_personalvehiclecov
                                           WHERE  choiceterm1avl_stg IS NULL
                                           AND    choiceterm2avl_stg IS NULL
                                           AND    directterm1avl_stg IS NULL
                                           AND    directterm2avl_stg IS NULL
                                           AND    booleanterm1avl_stg IS NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pc_personalvehiclecov.updatetime_stg>(:start_dttm)
                                           AND    pc_personalvehiclecov.updatetime_stg <= (:end_dttm)
                                           UNION
                                           /*****************************pcx_pawatercraftmotorcov_alfa*********************************************/
                                           SELECT ''DirectTerm1''                         AS columnname,
                                                  cast(directterm1_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg               AS branchid,
                                                  pawatercraftmotor_alfa_stg AS assetkey,
                                                  ''pawatercraftmotor_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawatercraftmotorcov_alfa
                                           WHERE  directterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm2''                         AS columnname,
                                                  cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg               AS branchid,
                                                  pawatercraftmotor_alfa_stg AS assetkey,
                                                  ''pawatercraftmotor_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawatercraftmotorcov_alfa
                                           WHERE  directterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''Clause''                   AS columnname,
                                                  cast(NULL AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg               AS branchid,
                                                  pawatercraftmotor_alfa_stg AS assetkey,
                                                  ''pawatercraftmotor_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawatercraftmotorcov_alfa
                                           WHERE  directterm1avl_stg IS NULL
                                           AND    directterm2avl_stg IS NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawatercraftmotorcov_alfa.updatetime_stg <= (:end_dttm)
                                           /*******************************************************************************************************/
                                           UNION
                                           /*****************************pcx_pawctrailercov_alfa************************************************/
                                           SELECT ''DirectTerm1''                        AS columnname,
                                                  cast(directterm1_stg AS VARCHAR(255))AS val,
                                                  patterncode_stg,
                                                  branchid_stg                 AS branchid,
                                                  pawatercrafttrailer_alfa_stg AS assetkey,
                                                  ''pawatercrafttrailer_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawctrailercov_alfa
                                           WHERE  directterm1_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''DirectTerm2''                         AS columnname,
                                                  cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                 AS branchid,
                                                  pawatercrafttrailer_alfa_stg AS assetkey,
                                                  ''pawatercrafttrailer_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawctrailercov_alfa
                                           WHERE  directterm2_stg IS NOT NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg <= (:end_dttm)
                                           UNION
                                           SELECT ''Clause''                   AS columnname,
                                                  cast(NULL AS VARCHAR(255)) AS val,
                                                  patterncode_stg,
                                                  branchid_stg                 AS branchid,
                                                  pawatercrafttrailer_alfa_stg AS assetkey,
                                                  ''pawatercrafttrailer_alfa''   AS assettype,
                                                  createtime_stg,
                                                  effectivedate_stg,
                                                  expirationdate_stg,
                                                  cast(NULL AS VARCHAR(255)) choiceterm1,
                                                  cast(NULL AS VARCHAR(255)) patternid ,
                                                  updatetime_stg
                                           FROM   db_t_prod_stag.pcx_pawctrailercov_alfa
                                           WHERE  directterm1avl_stg IS NULL
                                           AND    directterm2avl_stg IS NULL
                                           AND    expirationdate_stg IS NULL
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg>(:start_dttm)
                                           AND    pcx_pawctrailercov_alfa.updatetime_stg <= (:end_dttm)
                                                  /***********************************************************************************************/
                                    ) polcov
                    inner join
                                    (
                                           SELECT id_stg AS id,
                                                  policynumber_stg,
                                                  periodstart_stg,
                                                  periodend_stg,
                                                  mostrecentmodel_stg,
                                                  status_stg,
                                                  jobid_stg ,
                                                  publicid_stg,
                                                  createtime_stg,
                                                  updatetime_stg,
                                                  branchnumber_stg,
                                                  editeffectivedate_stg
                                           FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                    ON              pc_policyperiod.id = polcov.branchid
                    left join
                                    (
                                           SELECT cast(pc_etlclausepattern.patternid_stg AS    VARCHAR(255)) AS clausepatternid,
                                                  cast(pc_etlcovtermpattern.patternid_stg AS   VARCHAR(255)) AS covtermpatternid,
                                                  cast(pc_etlcovtermpattern.columnname_stg AS  VARCHAR(255)) AS columnname_stg,
                                                  cast(pc_etlcovtermpattern.covtermtype_stg AS VARCHAR(100)) AS covtermtype_stg,
                                                  cast(pc_etlclausepattern.name_stg AS         VARCHAR(255)) AS clausename
                                           FROM   db_t_prod_stag.pc_etlclausepattern
                                           join   db_t_prod_stag.pc_etlcovtermpattern
                                           ON     pc_etlclausepattern.id_stg=pc_etlcovtermpattern.clausepatternid_stg
                                           UNION
                                           SELECT    pc_etlclausepattern.patternid_stg                       AS clausepatternid,
                                                     pc_etlcovtermpattern.patternid_stg                      AS covtermpatternid,
                                                     coalesce(pc_etlcovtermpattern.columnname_stg,''Clause'')  AS columnname_stg,
                                                     coalesce(pc_etlcovtermpattern.covtermtype_stg,''Clause'') AS covtermtype_stg,
                                                     pc_etlclausepattern.name_stg                            AS clausename
                                           FROM      db_t_prod_stag.pc_etlclausepattern
                                           left join
                                                     (
                                                            SELECT *
                                                            FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                            WHERE  name_stg NOT LIKE ''ZZ%'') pc_etlcovtermpattern
                                           ON        pc_etlcovtermpattern.clausepatternid_stg=pc_etlclausepattern.id_stg
                                           WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                           AND       pc_etlcovtermpattern.name_stg IS NULL
                                           AND       owningentitytype_stg IN (''HOLineSchCovItem_alfa'',
                                                                              ''HomeownersLine_HOE'',
                                                                              ''Dwelling_HOE'',
                                                                              ''PersonalVehicle'',
                                                                              ''PersonalAutoLine'' ) ) covterm
                    ON              covterm.clausepatternid=polcov.patterncode_stg
                    AND             covterm.columnname_stg=polcov.columnname
                    left outer join
                                    (
                                           SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid,
                                                  pc_etlcovtermpackage.packagecode_stg AS cov_id,
                                                  pc_etlcovtermpackage.packagecode_stg AS name
                                           FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                    ON              PACKAGE.packagepatternid=polcov.val
                    left outer join
                                    (
                                               SELECT     pc_etlcovtermoption.patternid_stg  AS optionpatternid,
                                                          pc_etlcovtermoption.optioncode_stg AS name_stg,
                                                          pc_etlcovtermoption.value_stg,
                                                          pc_etlcovtermpattern.valuetype_stg
                                               FROM       db_t_prod_stag.pc_etlcovtermpattern
                                               inner join db_t_prod_stag.pc_etlcovtermoption
                                               ON         pc_etlcovtermpattern.id_stg=pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                    ON              optn.optionpatternid=polcov.val
                    left outer join db_t_prod_stag.pcx_dwelling_hoe
                    ON              pcx_dwelling_hoe.id_stg =polcov.assetkey
                    AND             assettype=''dwelling_hoe''
                    left outer join db_t_prod_stag.pcx_holineschedcovitem_alfa
                    ON              pcx_holineschedcovitem_alfa.id_stg =polcov.assetkey
                    AND             assettype=''holineschedcovitem_alfa''
                    left outer join db_t_prod_stag.pcx_holineschcovitemcov_alfa
                    ON              pcx_holineschcovitemcov_alfa.holineschcovitem_stg =polcov.assetkey
                    AND             assettype=''holineschedcovitem_alfa''
                    left outer join db_t_prod_stag.pc_personalvehicle
                    ON              pc_personalvehicle.id_stg =polcov.assetkey
                    AND             assettype=''personalvehicle''
                    left outer join db_t_prod_stag.pcx_pawatercraftmotor_alfa
                    ON              pcx_pawatercraftmotor_alfa.id_stg =polcov.assetkey
                    AND             assettype=''pawatercraftmotor_alfa''
                    left outer join db_t_prod_stag.pcx_pawatercrafttrailer_alfa
                    ON              pcx_pawatercrafttrailer_alfa.id_stg =polcov.assetkey
                    AND             assettype=''pawatercrafttrailer_alfa''
                    inner join      db_t_prod_stag.pctl_policyperiodstatus
                    ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                    inner join      db_t_prod_stag.pc_job
                    ON              pc_job.id_stg=pc_policyperiod.jobid_stg
                    inner join      db_t_prod_stag.pctl_job
                    ON              pctl_job.id_stg=pc_job.subtype_stg
                    WHERE           covterm.clausename NOT LIKE''%ZZ%''
                    AND             pctl_job.typecode_stg IN (''Submission'',
                                                              ''PolicyChange'',
                                                              ''Renewal'')
                    AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                    AND             (
                                                    pc_personalvehicle.branchid_stg=pc_policyperiod.id
                                    OR              pcx_dwelling_hoe.branchid_stg=pc_policyperiod.id
                                    OR              pcx_pawatercraftmotor_alfa.branchid_stg=pc_policyperiod.id
                                    OR              pcx_pawatercrafttrailer_alfa.branchid_stg=pc_policyperiod.id
                                    OR              pcx_holineschcovitemcov_alfa.branchid_stg=pc_policyperiod.id )) , quotn_asset_feat_6 AS
    (
           SELECT *
           FROM   (
                                  --- Union to bring the building
                                  SELECT DISTINCT pc_policyperiod.policynumber_stg AS policynumber,
                                                  pc_policyperiod.periodstart_stg  AS pol_start_dt,
                                                  CASE
                                                                  WHEN polcov.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                  ELSE polcov.effectivedate_stg
                                                  END AS feature_start_dt,
                                                  CASE
                                                                  WHEN polcov.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                  ELSE polcov.expirationdate_stg
                                                  END                                                                 AS feature_end_dt,
                                                  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))                    AS cntrct_role,
                                                  coalesce(polveh.effectivedate_stg, pc_policyperiod.periodstart_stg) AS asset_start_dt,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS                                             VARCHAR(100))  COLLATE ''en-ci'' =''package'' THEN cast(PACKAGE.packagepatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                             VARCHAR(100))  COLLATE ''en-ci'' =''option''
                                                                  AND             polcov.val_stg IS NOT NULL THEN cast(optn.optionpatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                             VARCHAR(100))  COLLATE ''en-ci'' =''Clause'' THEN cast(covterm.clausepatternid_stg AS VARCHAR(100))
                                                                  ELSE cast(covterm.covtermpatternid_stg AS                                        VARCHAR(100))
                                                  END AS nk_public_id,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''package'' THEN ''FEAT_SBTYPE9''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''option''
                                                                  AND             polcov.val_stg IS NOT NULL THEN ''FEAT_SBTYPE8''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''Clause'' THEN cast( ''FEAT_SBTYPE7'' AS VARCHAR(50))
                                                                  ELSE ''FEAT_SBTYPE6''
                                                  END AS feat_sbtype_cd,
                                                  CASE
                                                                  WHEN polcov.assettype_stg IN ( ''BP7Building'') THEN ''PRTY_ASSET_SBTYPE32''
                                                  END AS assettype,
                                                  CASE
                                                                  WHEN polcov.assettype_stg IN (''BP7Building'') THEN ''PRTY_ASSET_CLASFCN10''
                                                  END                                       AS classification_code,
                                                  cast (polveh.fixedid_stg AS bigint )      AS fixedid,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbolcollision_alfa,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbol_alfa,
                                                  pc_job.jobnumber_stg                      AS jobnumber_stg,
                                                  pc_policyperiod.branchnumber_stg          AS branchnumber_stg,
                                                  pc_policyperiod.updatetime_stg            AS trans_strt_dt ,
                                                  cast( ''9999-12-31'' AS DATE )              AS trans_end_dt,
                                                  cast(
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''datetime'' THEN cast(to_char(cast(polcov.val_stg AS timestamp(6)), ''MM/DD/YYYY'') AS VARCHAR(255))
                                                                  ELSE cast (polcov.val_stg AS         VARCHAR(255))
                                                  END AS VARCHAR(255)) AS feat_val,
                                                  /* CAST( polcov.val_stg as varchar(255))as FEAT_VAL,*/
                                                  cast(NULL AS                    DECIMAL(14,4)) AS feat_rate,
                                                  cast(NULL AS                    VARCHAR(5))    AS eligible,
                                                  cast(covterm.covtermtype_stg AS VARCHAR(255))  AS feat_covtermtype,
                                                  cast(NULL AS                    VARCHAR(50))   AS discountsurcharge_alfa_typecd
                                  FROM            (
                                                             /*pcx_bp7buildingcov*/
                                                             SELECT     ''ChoiceTerm1''   AS columnname_stg,
                                                                        choiceterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        cast(''bp7building'' AS  VARCHAR(255)) AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm2''   AS columnname_stg,
                                                                        choiceterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm3''   AS columnname_stg,
                                                                        choiceterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm4''   AS columnname_stg,
                                                                        choiceterm4_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm4avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm5_stg'' AS columnname_stg,
                                                                        choiceterm5_stg   AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm5avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm6''   AS columnname_stg,
                                                                        choiceterm6_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm6avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm1''                         AS columnname_stg,
                                                                        cast(directterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm2''                         AS columnname_stg,
                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm3''                         AS columnname_stg,
                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm1''                         AS columnname_stg,
                                                                        cast(booleanterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm2''                         AS columnname_stg,
                                                                        cast(booleanterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm3''                         AS columnname_stg,
                                                                        cast(booleanterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm1''   AS columnname_stg,
                                                                        stringterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm2''   AS columnname_stg,
                                                                        stringterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm3''   AS columnname_stg,
                                                                        stringterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm4''   AS columnname_stg,
                                                                        stringterm4_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm4avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm5''   AS columnname_stg,
                                                                        stringterm5_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm5avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm1''                         AS columnname_stg,
                                                                        cast(dateterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm2''                         AS columnname_stg,
                                                                        cast(dateterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''Clause''                   AS columnname_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        NULL AS choiceterm1_stg,
                                                                        NULL AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcov a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg IS NULL
                                                             AND        choiceterm2avl_stg IS NULL
                                                             AND        choiceterm3avl_stg IS NULL
                                                             AND        choiceterm4avl_stg IS NULL
                                                             AND        choiceterm5avl_stg IS NULL
                                                             AND        choiceterm6avl_stg IS NULL
                                                             AND        directterm1avl_stg IS NULL
                                                             AND        directterm2avl_stg IS NULL
                                                             AND        directterm3avl_stg IS NULL
                                                             AND        booleanterm1avl_stg IS NULL
                                                             AND        booleanterm2avl_stg IS NULL
                                                             AND        booleanterm3avl_stg IS NULL
                                                             AND        stringterm1avl_stg IS NULL
                                                             AND        stringterm2avl_stg IS NULL
                                                             AND        stringterm3avl_stg IS NULL
                                                             AND        stringterm4avl_stg IS NULL
                                                             AND        stringterm5avl_stg IS NULL
                                                             AND        dateterm1avl_stg IS NULL
                                                             AND        dateterm2avl_stg IS NULL
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm) )polcov
                                  left join
                                                  (
                                                         SELECT pc_etlclausepattern.patternid_stg  AS clausepatternid_stg,
                                                                pc_etlcovtermpattern.patternid_stg AS covtermpatternid_stg,
                                                                pc_etlcovtermpattern.columnname_stg,
                                                                pc_etlcovtermpattern.covtermtype_stg,
                                                                pc_etlclausepattern.name_stg AS clausename_stg
                                                         FROM   db_t_prod_stag.pc_etlclausepattern
                                                         join   db_t_prod_stag.pc_etlcovtermpattern
                                                         ON     pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausepatternid_stg
                                                         UNION
                                                         SELECT    pc_etlclausepattern.patternid_stg                        AS clausepatternid_stg,
                                                                   pc_etlcovtermpattern.patternid_stg                       AS covtermpatternid_stg,
                                                                   coalesce(pc_etlcovtermpattern.columnname_stg, ''Clause'')  AS columnname_stg,
                                                                   coalesce(pc_etlcovtermpattern.covtermtype_stg, ''Clause'') AS covtermtype_stg,
                                                                   pc_etlclausepattern.name_stg                             AS clausename_stg
                                                         FROM      db_t_prod_stag.pc_etlclausepattern
                                                         left join
                                                                   (
                                                                          SELECT *
                                                                          FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                          WHERE  name_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern
                                                         ON        pc_etlcovtermpattern.clausepatternid_stg = pc_etlclausepattern.id_stg
                                                         WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                                         AND       pc_etlcovtermpattern.name_stg IS NULL
                                                         AND       owningentitytype_stg IN (''BP7Building'') ) covterm
                                  ON              covterm.clausepatternid_stg = polcov.patterncode_stg
                                  AND             covterm.columnname_stg = polcov.columnname_stg
                                  left outer join
                                                  (
                                                         SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid_stg,
                                                                pc_etlcovtermpackage.packagecode_stg AS cov_id,
                                                                pc_etlcovtermpackage.packagecode_stg AS name_stg
                                                         FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                                  ON              PACKAGE.packagepatternid_stg = polcov.val_stg
                                  left outer join
                                                  (
                                                             SELECT     pc_etlcovtermoption.patternid_stg  AS optionpatternid_stg,
                                                                        pc_etlcovtermoption.optioncode_stg AS name_stg,
                                                                        pc_etlcovtermoption.value_stg,
                                                                        pc_etlcovtermpattern.valuetype_stg
                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern
                                                             inner join db_t_prod_stag.pc_etlcovtermoption
                                                             ON         pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                                  ON              optn.optionpatternid_stg = polcov.val_stg
                                  left outer join
                                                  (
                                                           SELECT   cast(a.fixedid_stg AS  VARCHAR(50))  fixedid_stg,
                                                                    cast(a.branchid_stg AS VARCHAR(250)) branchid_stg,
                                                                    cast( ''BP7Building'' AS VARCHAR(255)) assettype_stg ,
                                                                    a.effectivedate_stg,
                                                                    a.expirationdate_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                additionalinterest_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                additionalinsured_stg ,
                                                                    rank() over ( PARTITION BY a.fixedid_stg , a.branchid_stg ORDER BY a.updatetime_stg DESC) r
                                                           FROM     db_t_prod_stag.pcx_bp7building a
                                                           join     db_t_prod_stag.pc_building b
                                                           ON       b.fixedid_stg =a.building_stg
                                                           AND      a.branchid_stg=b.branchid_stg
                                                           WHERE    a.expirationdate_stg IS NULL ) polveh
                                  ON              polcov.assetkey_stg =polveh.fixedid_stg
                                  AND             polcov.branchid_stg=polveh.branchid_stg
                                  AND             polcov.assettype_stg=polveh.assettype_stg
                                  AND             r=1
                                  left outer join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_int_stg,
                                                                                  pc_addlinterestdetail.id_stg                   addlinter_id
                                                                  FROM            db_t_prod_stag.pc_addlinterestdetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg )pc_con
                                  ON              cast(addlinter_id AS INTEGER) =cast(polveh.additionalinterest_stg AS INTEGER)
                                  left join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_ins_stg,
                                                                                  pc_policyaddlinsureddetail.id_stg              addinsuredid
                                                                  FROM            db_t_prod_stag.pc_policyaddlinsureddetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_policyaddlinsureddetail.policyaddlinsured_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con_ins
                                  ON              cast(addinsuredid AS INTEGER) =cast(polveh.additionalinsured_stg AS INTEGER)
                                  inner join
                                                  (
                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id_stg,
                                                                policynumber_stg,
                                                                branchnumber_stg,
                                                                periodstart_stg,
                                                                pnicontactdenorm_stg,
                                                                periodend_stg,
                                                                mostrecentmodel_stg,
                                                                status_stg,
                                                                jobid_stg,
                                                                updatetime_stg,
                                                                retired_stg
                                                         FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                  ON              pc_policyperiod.id_stg = coalesce(polcov.branchid_stg , polveh.branchid_stg)
                                  join            db_t_prod_stag.pctl_policyperiodstatus
                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                  join            db_t_prod_stag.pc_job
                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                  join            db_t_prod_stag.pctl_job
                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                  WHERE           covterm.clausename_stg NOT LIKE''%ZZ%''
                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                            ''PolicyChange'',
                                                                            ''Renewal'')
                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                                  AND             pc_policyperiod.policynumber_stg IS NOT NULL
                                  UNION
                                  --pcx_bp7buildingexcl with pcx_bp7building qry
                                  SELECT DISTINCT pc_policyperiod.policynumber_stg AS policynumber,
                                                  pc_policyperiod.periodstart_stg  AS pol_start_dt,
                                                  CASE
                                                                  WHEN polexcl.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                  ELSE polexcl.effectivedate_stg
                                                  END AS feature_start_dt,
                                                  CASE
                                                                  WHEN polexcl.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                  ELSE polexcl.expirationdate_stg
                                                  END                                                                 AS feature_end_dt,
                                                  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))                    AS cntrct_role,
                                                  coalesce(polveh.effectivedate_stg, pc_policyperiod.periodstart_stg) AS asset_start_dt,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100))  COLLATE ''en-ci''=''package'' THEN cast(PACKAGE.packagepatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100)) COLLATE ''en-ci'' =''option''
                                                                  AND             polexcl.val_stg IS NOT NULL THEN cast(optn.optionpatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100))  COLLATE ''en-ci''=''Clause'' THEN cast(covterm.clausepatternid_stg AS VARCHAR(100))
                                                                  ELSE cast(covterm.covtermpatternid_stg AS                                         VARCHAR(100))
                                                  END AS nk_public_id,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''package'' THEN ''FEAT_SBTYPE9''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''option''
                                                                  AND             polexcl.val_stg IS NOT NULL THEN ''FEAT_SBTYPE8''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''Clause'' THEN cast( ''FEAT_SBTYPE7'' AS VARCHAR(50))
                                                                  ELSE ''FEAT_SBTYPE6''
                                                  END AS feat_sbtype_cd ,
                                                  CASE
                                                                  WHEN polexcl.assettype_stg IN (''BP7Building'') THEN ''PRTY_ASSET_SBTYPE32''
                                                  END AS assettype,
                                                  CASE
                                                                  WHEN polexcl.assettype_stg IN ( ''BP7Building'') THEN ''PRTY_ASSET_CLASFCN10''
                                                  END                                       AS classification_code,
                                                  cast (polveh.fixedid_stg AS bigint)       AS fixedid,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbolcollision_alfa,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbol_alfa,
                                                  pc_job.jobnumber_stg                      AS jobnumber_stg,
                                                  pc_policyperiod.branchnumber_stg          AS branchnumber_stg,
                                                  pc_policyperiod.updatetime_stg            AS trans_strt_dt ,
                                                  cast( ''9999-12-31'' AS DATE )              AS trans_end_dt,
                                                  cast(
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''datetime'' THEN cast(to_char(cast(polexcl.val_stg AS timestamp(6)), ''MM/DD/YYYY'') AS VARCHAR(255))
                                                                  ELSE cast (polexcl.val_stg AS        VARCHAR(255))
                                                  END AS VARCHAR(255)) AS feat_val,
                                                  /* CAST(polexcl.val_stg as varchar(255)) as FEAT_VAL,*/
                                                  cast(NULL AS                    DECIMAL(14,4)) AS feat_rate,
                                                  cast(NULL AS                    VARCHAR(5))    AS eligible,
                                                  cast(covterm.covtermtype_stg AS VARCHAR(255))  AS feat_covtermtype,
                                                  cast(NULL AS                    VARCHAR(50))   AS discountsurcharge_alfa_typecd
                                  FROM            (
                                                             --pcx_bp7buildingexcl
                                                             SELECT     ''ChoiceTerm1''   AS columnname_stg,
                                                                        choiceterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm2''   AS columnname_stg,
                                                                        choiceterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm3''   AS columnname_stg,
                                                                        choiceterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm4''   AS columnname_stg,
                                                                        choiceterm4_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm4avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm5_stg'' AS columnname_stg,
                                                                        choiceterm5_stg   AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm5avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm1''                         AS columnname_stg,
                                                                        cast(directterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm2''                         AS columnname_stg,
                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm3''                         AS columnname_stg,
                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm1''                         AS columnname_stg,
                                                                        cast(booleanterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm2''                         AS columnname_stg,
                                                                        cast(booleanterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm3''                         AS columnname_stg,
                                                                        cast(booleanterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm1''   AS columnname_stg,
                                                                        stringterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm2''   AS columnname_stg,
                                                                        stringterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm3''   AS columnname_stg,
                                                                        stringterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm1''                         AS columnname_stg,
                                                                        cast(dateterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm2''                         AS columnname_stg,
                                                                        cast(dateterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''Clause''                   AS columnname_stg,
                                                                        cast(NULL AS VARCHAR(255))    val,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingexcl a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg IS NULL
                                                             AND        choiceterm2avl_stg IS NULL
                                                             AND        choiceterm3avl_stg IS NULL
                                                             AND        choiceterm4avl_stg IS NULL
                                                             AND        choiceterm5avl_stg IS NULL
                                                             AND        directterm1avl_stg IS NULL
                                                             AND        directterm2avl_stg IS NULL
                                                             AND        directterm3avl_stg IS NULL
                                                             AND        booleanterm1avl_stg IS NULL
                                                             AND        booleanterm2avl_stg IS NULL
                                                             AND        booleanterm3avl_stg IS NULL
                                                             AND        stringterm1avl_stg IS NULL
                                                             AND        stringterm2avl_stg IS NULL
                                                             AND        stringterm3avl_stg IS NULL
                                                             AND        dateterm1avl_stg IS NULL
                                                             AND        dateterm2avl_stg IS NULL
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm) ) polexcl
                                  left join
                                                  (
                                                         SELECT pc_etlclausepattern.patternid_stg  AS clausepatternid_stg,
                                                                pc_etlcovtermpattern.patternid_stg AS covtermpatternid_stg,
                                                                pc_etlcovtermpattern.columnname_stg,
                                                                pc_etlcovtermpattern.covtermtype_stg,
                                                                pc_etlclausepattern.name_stg AS clausename_stg
                                                         FROM   db_t_prod_stag.pc_etlclausepattern
                                                         join   db_t_prod_stag.pc_etlcovtermpattern
                                                         ON     pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausepatternid_stg
                                                         UNION
                                                         SELECT    pc_etlclausepattern.patternid_stg                        AS clausepatternid_stg,
                                                                   pc_etlcovtermpattern.patternid_stg                       AS covtermpatternid_stg,
                                                                   coalesce(pc_etlcovtermpattern.columnname_stg, ''Clause'')  AS columnname_stg,
                                                                   coalesce(pc_etlcovtermpattern.covtermtype_stg, ''Clause'') AS covtermtype_stg,
                                                                   pc_etlclausepattern.name_stg                             AS clausename_stg
                                                         FROM      db_t_prod_stag.pc_etlclausepattern
                                                         left join
                                                                   (
                                                                          SELECT *
                                                                          FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                          WHERE  name_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern
                                                         ON        pc_etlcovtermpattern.clausepatternid_stg = pc_etlclausepattern.id_stg
                                                         WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                                         AND       pc_etlcovtermpattern.name_stg IS NULL
                                                         AND       owningentitytype_stg IN (''BP7Building'') ) covterm
                                  ON              covterm.clausepatternid_stg = polexcl.patterncode_stg
                                  AND             covterm.columnname_stg = polexcl.columnname_stg
                                  left outer join
                                                  (
                                                         SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid_stg,
                                                                pc_etlcovtermpackage.packagecode_stg AS cov_id_stg,
                                                                pc_etlcovtermpackage.packagecode_stg AS name_stg
                                                         FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                                  ON              PACKAGE.packagepatternid_stg = polexcl.val_stg
                                  left outer join
                                                  (
                                                             SELECT     pc_etlcovtermoption.patternid_stg  AS optionpatternid_stg,
                                                                        pc_etlcovtermoption.optioncode_stg AS name_stg,
                                                                        pc_etlcovtermoption.value_stg,
                                                                        pc_etlcovtermpattern.valuetype_stg
                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern
                                                             inner join db_t_prod_stag.pc_etlcovtermoption
                                                             ON         pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                                  ON              optn.optionpatternid_stg = polexcl.val_stg
                                  left outer join
                                                  (
                                                           SELECT   cast(b.fixedid_stg AS  VARCHAR(50))  fixedid_stg,
                                                                    cast(a.branchid_stg AS VARCHAR(250)) branchid_stg,
                                                                    cast( ''BP7Building'' AS VARCHAR(255)) assettype_stg ,
                                                                    a.effectivedate_stg,
                                                                    a.expirationdate_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                AS additionalinterest_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                AS additionalinsured_stg ,
                                                                    rank() over ( PARTITION BY a.fixedid_stg , a.branchid_stg ORDER BY a.updatetime_stg DESC)    r
                                                           FROM     db_t_prod_stag.pcx_bp7building a
                                                           join     db_t_prod_stag.pc_building b
                                                           ON       b.fixedid_stg =a.building_stg
                                                           AND      a.branchid_stg=b.branchid_stg
                                                           WHERE    a.expirationdate_stg IS NULL ) polveh
                                  ON              polexcl.assetkey_stg =polveh.fixedid_stg
                                  AND             polexcl.branchid_stg=polveh.branchid_stg
                                  AND             polexcl.assettype_stg=polveh.assettype_stg
                                  AND             r=1
                                  left outer join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_int_stg,
                                                                                  pc_addlinterestdetail.id_stg                   addlinter_id
                                                                  FROM            db_t_prod_stag.pc_addlinterestdetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con
                                  ON              cast(addlinter_id AS INTEGER) = cast(polveh.additionalinterest_stg AS INTEGER)
                                  left join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_ins_stg,
                                                                                  pc_policyaddlinsureddetail.id_stg              addinsuredid
                                                                  FROM            db_t_prod_stag.pc_policyaddlinsureddetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_policyaddlinsureddetail.policyaddlinsured_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con_ins
                                  ON              cast(addinsuredid AS INTEGER) = cast( polveh.additionalinsured_stg AS INTEGER)
                                  inner join
                                                  (
                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id_stg,
                                                                policynumber_stg,
                                                                branchnumber_stg,
                                                                periodstart_stg,
                                                                pnicontactdenorm_stg,
                                                                periodend_stg,
                                                                mostrecentmodel_stg,
                                                                status_stg,
                                                                jobid_stg,
                                                                updatetime_stg,
                                                                retired_stg
                                                         FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                  ON              pc_policyperiod.id_stg = coalesce(polexcl.branchid_stg , polveh.branchid_stg)
                                  join            db_t_prod_stag.pctl_policyperiodstatus
                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                  join            db_t_prod_stag.pc_job
                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                  join            db_t_prod_stag.pctl_job
                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                  WHERE           covterm.clausename_stg NOT LIKE''%ZZ%''
                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                            ''PolicyChange'',
                                                                            ''Renewal'')
                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                                  AND             pc_policyperiod.policynumber_stg IS NOT NULL
                                  UNION
                                  --PCX_BP7BUILDINGCOND with pcx_bp7building Query
                                  SELECT DISTINCT pc_policyperiod.policynumber_stg AS policynumber,
                                                  pc_policyperiod.periodstart_stg  AS pol_start_dt,
                                                  CASE
                                                                  WHEN polcond.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                  ELSE polcond.effectivedate_stg
                                                  END AS feature_start_dt,
                                                  CASE
                                                                  WHEN polcond.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                  ELSE polcond.expirationdate_stg
                                                  END                                                                 AS feature_end_dt,
                                                  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' AS VARCHAR(50))                    AS cntrct_role,
                                                  coalesce(polveh.effectivedate_stg, pc_policyperiod.periodstart_stg) AS asset_start_dt,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100))  COLLATE ''en-ci''=''package'' THEN cast(PACKAGE.packagepatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100)) COLLATE ''en-ci'' =''option''
                                                                  AND             polcond.val_stg IS NOT NULL THEN cast(optn.optionpatternid_stg AS VARCHAR(100))
                                                                  WHEN cast(covterm.covtermtype_stg AS                                              VARCHAR(100)) COLLATE ''en-ci'' =''Clause'' THEN cast(covterm.clausepatternid_stg AS VARCHAR(100))
                                                                  ELSE cast(covterm.covtermpatternid_stg AS                                         VARCHAR(100))
                                                  END AS nk_public_id,
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''package'' THEN ''FEAT_SBTYPE9''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100))  COLLATE ''en-ci''= ''option''
                                                                  AND             polcond.val_stg IS NOT NULL THEN ''FEAT_SBTYPE8''
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''Clause'' THEN cast( ''FEAT_SBTYPE7'' AS VARCHAR(50))
                                                                  ELSE ''FEAT_SBTYPE6''
                                                  END AS feat_sbtype_cd,
                                                  CASE
                                                                  WHEN polcond.assettype_stg IN (''BP7Building'') THEN ''PRTY_ASSET_SBTYPE32''
                                                  END AS assettype,
                                                  CASE
                                                                  WHEN polcond.assettype_stg IN (''BP7Building'') THEN ''PRTY_ASSET_CLASFCN10''
                                                  END                                       AS classification_code,
                                                  cast( polveh.fixedid_stg AS bigint)       AS fixedid,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbolcollision_alfa,
                                                  cast(NULL AS                VARCHAR(100)) AS ratesymbol_alfa,
                                                  pc_job.jobnumber_stg                      AS jobnumber_stg,
                                                  pc_policyperiod.branchnumber_stg          AS branchnumber_stg,
                                                  pc_policyperiod.updatetime_stg            AS trans_strt_dt ,
                                                  cast( ''9999-12-31'' AS DATE )              AS trans_end_dt,
                                                  cast(
                                                  CASE
                                                                  WHEN cast(covterm.covtermtype_stg AS VARCHAR(100)) COLLATE ''en-ci'' = ''datetime'' THEN cast(to_char(cast(polcond.val_stg AS timestamp(6)), ''MM/DD/YYYY'') AS VARCHAR(255))
                                                                  ELSE cast (polcond.val_stg AS        VARCHAR(255))
                                                  END AS VARCHAR(255)) AS feat_val,
                                                  /* CAST( polcond.val_stg as varchar(255))as FEAT_VAL,*/
                                                  cast(NULL AS                    DECIMAL(14,4)) AS feat_rate,
                                                  cast(NULL AS                    VARCHAR(5))    AS eligible,
                                                  cast(covterm.covtermtype_stg AS VARCHAR(255))  AS feat_covtermtype,
                                                  cast(NULL AS                    VARCHAR(50))   AS discountsurcharge_alfa_typecd
                                  FROM            ( --pcx_bp7buildingcond
                                                             SELECT     ''ChoiceTerm1''   AS columnname_stg,
                                                                        choiceterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        cast(''bp7building'' AS  VARCHAR(255)) AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm2''   AS columnname_stg,
                                                                        choiceterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm3''   AS columnname_stg,
                                                                        choiceterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm4''   AS columnname_stg,
                                                                        choiceterm4_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm4avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''ChoiceTerm5_stg'' AS columnname_stg,
                                                                        choiceterm5_stg   AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm5avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm1''                         AS columnname_stg,
                                                                        cast(directterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm2''                         AS columnname_stg,
                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DirectTerm3''                         AS columnname_stg,
                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      directterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm1''                         AS columnname_stg,
                                                                        cast(booleanterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm2''                         AS columnname_stg,
                                                                        cast(booleanterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''BooleanTerm3''                         AS columnname_stg,
                                                                        cast(booleanterm3_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      booleanterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm1''   AS columnname_stg,
                                                                        stringterm1_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm2''   AS columnname_stg,
                                                                        stringterm2_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''StringTerm3''   AS columnname_stg,
                                                                        stringterm3_stg AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      stringterm3avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm1''                         AS columnname_stg,
                                                                        cast(dateterm1_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm1avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''DateTerm2''                         AS columnname_stg,
                                                                        cast(dateterm2_stg AS VARCHAR(255)) AS val_stg,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      dateterm2avl_stg = 1
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm)
                                                             UNION
                                                             SELECT     ''Clause''                                         AS columnname_stg,
                                                                        cast(cast(NULL AS VARCHAR(255)) AS VARCHAR(255))    val,
                                                                        patterncode_stg,
                                                                        cast(a.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                        cast(b.fixedid_stg AS  VARCHAR(255)) AS assetkey_stg,
                                                                        ''bp7building''                        AS assettype_stg,
                                                                        a.createtime_stg,
                                                                        a.effectivedate_stg,
                                                                        a.expirationdate_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1_stg,
                                                                        cast(NULL AS VARCHAR(255)) AS patternid_stg,
                                                                        a.updatetime_stg
                                                             FROM       db_t_prod_stag.pcx_bp7buildingcond a
                                                             join       db_t_prod_stag.pcx_bp7building b
                                                             ON         a.building_stg =b.fixedid_stg
                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                             ON         pp.id_stg=a.branchid_stg
                                                             WHERE      choiceterm1avl_stg IS NULL
                                                             AND        choiceterm2avl_stg IS NULL
                                                             AND        choiceterm3avl_stg IS NULL
                                                             AND        choiceterm4avl_stg IS NULL
                                                             AND        choiceterm5avl_stg IS NULL
                                                             AND        directterm1avl_stg IS NULL
                                                             AND        directterm2avl_stg IS NULL
                                                             AND        directterm3avl_stg IS NULL
                                                             AND        booleanterm1avl_stg IS NULL
                                                             AND        booleanterm2avl_stg IS NULL
                                                             AND        booleanterm3avl_stg IS NULL
                                                             AND        stringterm1avl_stg IS NULL
                                                             AND        stringterm2avl_stg IS NULL
                                                             AND        stringterm3avl_stg IS NULL
                                                             AND        dateterm1avl_stg IS NULL
                                                             AND        dateterm2avl_stg IS NULL
                                                             AND        a.expirationdate_stg IS NULL
                                                             AND        b.expirationdate_stg IS NULL
                                                             AND        pp.updatetime_stg > (:start_dttm)
                                                             AND        pp.updatetime_stg <= (:end_dttm) ) polcond
                                  left join
                                                  (
                                                         SELECT pc_etlclausepattern.patternid_stg  AS clausepatternid_stg,
                                                                pc_etlcovtermpattern.patternid_stg AS covtermpatternid_stg,
                                                                pc_etlcovtermpattern.columnname_stg,
                                                                pc_etlcovtermpattern.covtermtype_stg,
                                                                pc_etlclausepattern.name_stg AS clausename_stg
                                                         FROM   db_t_prod_stag.pc_etlclausepattern
                                                         join   db_t_prod_stag.pc_etlcovtermpattern
                                                         ON     pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausepatternid_stg
                                                         UNION
                                                         SELECT    pc_etlclausepattern.patternid_stg                        AS clausepatternid_stg,
                                                                   pc_etlcovtermpattern.patternid_stg                       AS covtermpatternid_stg,
                                                                   coalesce(pc_etlcovtermpattern.columnname_stg, ''Clause'')  AS columnname_stg,
                                                                   coalesce(pc_etlcovtermpattern.covtermtype_stg, ''Clause'') AS covtermtype,
                                                                   pc_etlclausepattern.name_stg                             AS clausename_stg
                                                         FROM      db_t_prod_stag.pc_etlclausepattern
                                                         left join
                                                                   (
                                                                          SELECT *
                                                                          FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                          WHERE  name_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern
                                                         ON        pc_etlcovtermpattern.clausepatternid_stg = pc_etlclausepattern.id_stg
                                                         WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                                         AND       pc_etlcovtermpattern.name_stg IS NULL
                                                         AND       owningentitytype_stg IN (''BP7Building'') ) covterm
                                  ON              covterm.clausepatternid_stg = polcond.patterncode_stg
                                  AND             covterm.columnname_stg = polcond.columnname_stg
                                  left outer join
                                                  (
                                                         SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid_stg,
                                                                pc_etlcovtermpackage.packagecode_stg AS cov_id,
                                                                pc_etlcovtermpackage.packagecode_stg AS name
                                                         FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                                  ON              PACKAGE.packagepatternid_stg = polcond.val_stg
                                  left outer join
                                                  (
                                                             SELECT     pc_etlcovtermoption.patternid_stg  AS optionpatternid_stg,
                                                                        pc_etlcovtermoption.optioncode_stg AS name,
                                                                        pc_etlcovtermoption.value_stg,
                                                                        pc_etlcovtermpattern.valuetype_stg
                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern
                                                             inner join db_t_prod_stag.pc_etlcovtermoption
                                                             ON         pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                                  ON              optn.optionpatternid_stg = polcond.val_stg
                                  left outer join
                                                  (
                                                           SELECT   cast(b.fixedid_stg AS  VARCHAR(50))  fixedid_stg,
                                                                    cast(a.branchid_stg AS VARCHAR(250)) branchid_stg,
                                                                    cast( ''BP7Building'' AS VARCHAR(255)) assettype_stg ,
                                                                    a.effectivedate_stg,
                                                                    a.expirationdate_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                AS additionalinterest_stg,
                                                                    cast(NULL AS VARCHAR(255))                                                                AS additionalinsured_stg ,
                                                                    rank() over ( PARTITION BY a.fixedid_stg , a.branchid_stg ORDER BY a.updatetime_stg DESC)    r
                                                           FROM     db_t_prod_stag.pcx_bp7building a
                                                           join     db_t_prod_stag.pc_building b
                                                           ON       b.fixedid_stg =a.building_stg
                                                           AND      a.branchid_stg=b.branchid_stg
                                                           WHERE    a.expirationdate_stg IS NULL ) polveh
                                  ON              polcond.assetkey_stg =polveh.fixedid_stg
                                  AND             polcond.branchid_stg=polveh.branchid_stg
                                  AND             polcond.assettype_stg=polveh.assettype_stg
                                  AND             r=1
                                  left outer join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_int_stg,
                                                                                  pc_addlinterestdetail.id_stg                   addlinter_id
                                                                  FROM            db_t_prod_stag.pc_addlinterestdetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con
                                  ON              cast(addlinter_id AS INTEGER) =cast(polveh.additionalinterest_stg AS INTEGER)
                                  left outer join
                                                  (
                                                                  SELECT DISTINCT addressbookuid_stg
                                                                                                  ||''-''
                                                                                                  ||pctl_contact.typecode_stg AS addressbookuid_ins_stg,
                                                                                  pc_policyaddlinsureddetail.id_stg              addinsuredid
                                                                  FROM            db_t_prod_stag.pc_policyaddlinsureddetail
                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                  ON              pc_policyaddlinsureddetail.policyaddlinsured_stg = pc_policycontactrole.id_stg
                                                                  join            db_t_prod_stag.pc_contact
                                                                  ON              pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg
                                                                  inner join      db_t_prod_stag.pctl_contact
                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg ) pc_con_ins
                                  ON              cast(addinsuredid AS INTEGER) = cast(polveh.additionalinsured_stg AS INTEGER)
                                  inner join
                                                  (
                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id_stg,
                                                                policynumber_stg,
                                                                branchnumber_stg,
                                                                periodstart_stg,
                                                                pnicontactdenorm_stg,
                                                                periodend_stg,
                                                                mostrecentmodel_stg,
                                                                status_stg,
                                                                jobid_stg,
                                                                updatetime_stg,
                                                                retired_stg
                                                         FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                  ON              pc_policyperiod.id_stg = coalesce(polcond.branchid_stg , polveh.branchid_stg)
                                  join            db_t_prod_stag.pctl_policyperiodstatus
                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                  join            db_t_prod_stag.pc_job
                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                  join            db_t_prod_stag.pctl_job
                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                  WHERE           covterm.clausename_stg NOT LIKE''%ZZ%''
                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                            ''PolicyChange'',
                                                                            ''Renewal'')
                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN (''Temporary'')
                                  AND             pc_policyperiod.policynumber_stg IS NOT NULL )building )
    --with Cluase ends
    SELECT DISTINCT tgt.prty_asset_id               AS lkp_prty_asset_id,
                    tgt.asset_cntrct_role_sbtype_cd AS lkp_asset_cntrct_role_sbtype_cd,
                    tgt.quotn_asset_strt_dttm       AS lkp_quotn_asset_strt_dttm,
                    tgt.quotn_asset_feat_strt_dttm  AS lkp_quotn_asset_feat_strt_dttm,
                    tgt.quotn_asset_feat_end_dttm   AS lkp_quotn_asset_feat_end_dttm,
                    tgt.quotn_asset_feat_amt        AS lkp_quotn_asset_feat_amt,
                    tgt.quotn_asset_feat_dt         AS lkp_quotn_asset_feat_dt,
                    tgt.quotn_asset_feat_txt        AS lkp_quotn_asset_feat_txt,
                    tgt.quotn_asset_feat_ind        AS lkp_quotn_asset_feat_ind,
                    tgt.feat_efect_type_cd          AS lkp_feat_efect_type_cd,
                    prty.prty_asset_id,
                    qtn.quotn_id,
                    cntrct.tgt_idntftn_val AS asset_cntrct_role_sbtype_cd,
                    src.asset_start_dt AS quotn_asset_strt_dttm,
                    ft.feat_id,
                    src.feature_start_dt AS QUOTN_ASSET_FEAT_STRT_DTTM,
                    src.feature_end_dt AS QUOTN_ASSET_FEAT_END_DTTM,
                    src.trans_start_date AS TRANS_STRT_DTTM,
                    src.trans_end_date AS TRANS_END_DTTM,
                    CASE
                                    WHEN insrnc_cvge_type_cd=''CMP'' THEN src.ratesymbol_alfa
                                    WHEN insrnc_cvge_type_cd=''COL'' THEN src.ratesymbolcollision_alfa
                                    ELSE ''''
                    END AS rate_symb_cd,
                    src.polcov_ratemodifier,
                    src.polcov_eligible,
                    CASE
                                    WHEN eff.tgt_idntftn_val IS NULL THEN ''UNK''
                                    ELSE eff.tgt_idntftn_val
                    END AS O_DISCOUNTSURCHARGE_ALFA_TYPECD,
                    src.feat_val,
                    src.feat_covtermtype,
                    row_number() over (order by (select NULL)) as SOURCE_RECORD_ID
    FROM            (
                                    SELECT DISTINCT pc_quotn_asset_feat_x.policynumber,
                                                    pc_quotn_asset_feat_x.pol_start_dt,
                                                    pc_quotn_asset_feat_x.feature_start_dt,
                                                    pc_quotn_asset_feat_x.feature_end_dt,
                                                    pc_quotn_asset_feat_x.cntrct_role,
                                                    pc_quotn_asset_feat_x.asset_start_dt,
                                                    pc_quotn_asset_feat_x.nk_public_id,
                                                    pc_quotn_asset_feat_x.feat_sbtype_cd,
                                                    pc_quotn_asset_feat_x.assettype                      AS prty_asset_sbtype_cd,
                                                    pc_quotn_asset_feat_x.classification_code               clasfctn_cd,
                                                    cast (pc_quotn_asset_feat_x.fixedid AS VARCHAR(100))    prtyasset_fixedid,
                                                    pc_quotn_asset_feat_x.ratesymbolcollision_alfa,
                                                    pc_quotn_asset_feat_x.ratesymbol_alfa,
                                                    pc_quotn_asset_feat_x.jobnumber_stg    jobnumber,
                                                    pc_quotn_asset_feat_x.branchnumber_stg branchnumber ,
                                                    pc_quotn_asset_feat_x.feat_rate        polcov_ratemodifier,
                                                    CASE
                                                                    WHEN pc_quotn_asset_feat_x.eligible=1 THEN ''T''
                                                                    WHEN pc_quotn_asset_feat_x.eligible=0 THEN ''F''
                                                    END                                                                                                                                                   AS polcov_eligible,
                                                    --cast(pc_quotn_asset_feat_x.trans_strt_dt as timestamp FORMAT ''YYYY-MM-DD-hh:mi:ss'')(char(20)) Trans_Start_Date, pc_quotn_asset_feat_x.trans_strt_dt AS Trans_Start_Date,
                                                    pc_quotn_asset_feat_x.trans_strt_dt as Trans_Start_Date,
                                                    pc_quotn_asset_feat_x.trans_end_dt                                                                                                                       trans_end_date,
                                                    --pc_quotn_asset_feat_x.FEAT_VAL, 
													CAST((
                                                    CASE
                                                                    WHEN pc_quotn_asset_feat_x.feat_val LIKE ''..%'' THEN pc_quotn_asset_feat_x.feat_val
                                                                    WHEN (
                                                                                                    pc_quotn_asset_feat_x.feat_val LIKE ''.%a%''
                                                                                    OR              pc_quotn_asset_feat_x.feat_val LIKE ''.%e%''
                                                                                    OR              pc_quotn_asset_feat_x.feat_val LIKE ''.%i%''
                                                                                    OR              pc_quotn_asset_feat_x.feat_val LIKE ''.%o%''
                                                                                    OR              pc_quotn_asset_feat_x.feat_val LIKE ''.%u%'') THEN pc_quotn_asset_feat_x.feat_val
                                                                    WHEN pc_quotn_asset_feat_x.feat_val LIKE ''.%'' THEN ''0''
                                                                                                    ||pc_quotn_asset_feat_x.feat_val
                                                                    ELSE pc_quotn_asset_feat_x.feat_val
                                                    END) AS VARCHAR(255)) AS feat_val,
                                                    pc_quotn_asset_feat_x.feat_covtermtype,
                                                    pc_quotn_asset_feat_x.discountsurcharge_alfa_typecd
                                    FROM            ( select * FROM quotn_asset_feat_1
                                    UNION ALL
                                              select *
                                    FROM      quotn_asset_feat_2
                                    UNION ALL
                                              select *
                                    FROM      quotn_asset_feat_3
                                    UNION ALL
                                              select *
                                    FROM      quotn_asset_feat_4
                                    UNION ALL
                                              select *
                                    FROM      quotn_asset_feat_5
                                    UNION ALL
                                              select *
                                    FROM      quotn_asset_feat_6
                                    /*EIM-48974- FARM CHANGES BEGINS*/
                                    UNION ALL
                                              select *
                                    FROM      farm_temp )pc_quotn_asset_feat_x
                                          WHERE     prtyasset_fixedid IS NOT NULL qualify row_number() over(PARTITION BY cntrct_role, nk_public_id, feat_sbtype_cd, prty_asset_sbtype_cd, clasfctn_cd, prtyasset_fixedid, jobnumber ORDER BY trans_start_date DESC) =1) AS src
    left outer join
                    (
                           SELECT tgt_idntftn_val,
                                  src_idntftn_val
                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                           WHERE  tgt_idntftn_nm=''ASSET_CNTRCT_ROLE_SBTYPE''
                           AND    src_idntftn_nm=''derived''
                           AND    src_idntftn_sys=''DS''
                           AND    expn_dt=''9999-12-31'') cntrct
    ON              cntrct.src_idntftn_val=src.cntrct_role
    left outer join
                    (
                           SELECT tgt_idntftn_val,
                                  src_idntftn_val
                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                           WHERE  tgt_idntftn_nm= ''FEAT_SBTYPE''
                           AND    src_idntftn_nm= ''derived''
                           AND    src_idntftn_sys=''DS''
                           AND    expn_dt=''9999-12-31'') feat_sbtp
    ON              feat_sbtp.src_idntftn_val=src.feat_sbtype_cd
    left outer join
                    (
                           SELECT tgt_idntftn_val,
                                  src_idntftn_val
                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                           WHERE  tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
                           AND    src_idntftn_nm= ''derived''
                           AND    src_idntftn_sys=''DS''
                           AND    expn_dt=''9999-12-31'') asset_sbtp
    ON              asset_sbtp.src_idntftn_val=src.prty_asset_sbtype_cd
    left outer join
                    (
                           SELECT tgt_idntftn_val,
                                  src_idntftn_val
                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                           WHERE  tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                           AND    src_idntftn_sys IN (''DS'',
                                                      ''GW'')
                           AND    expn_dt=''9999-12-31'') clsfctn
    ON              clsfctn.src_idntftn_val=src.clasfctn_cd
    left outer join
                    (
                           SELECT tgt_idntftn_val,
                                  src_idntftn_val
                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                           WHERE  tgt_idntftn_nm= ''FEAT_EFECT_TYPE''
                           AND    src_idntftn_nm= ''pctl_discountsurcharge_alfa.typecode''
                           AND    src_idntftn_sys=''GW''
                           AND    expn_dt=''9999-12-31'') eff
    ON              eff.src_idntftn_val=src.discountsurcharge_alfa_typecd
    left outer join
                    (
                             SELECT   feat_id,
                                      feat_insrnc_sbtype_cd,
                                      feat_clasfcn_cd,
                                      feat_desc,
                                      feat_name,
                                      comn_feat_name,
                                      feat_lvl_sbtype_cnt,
                                      insrnc_cvge_type_cd,
                                      insrnc_lob_type_cd,
                                      feat_sbtype_cd,
                                      nk_src_key
                             FROM     db_t_prod_core.feat qualify row_number () over (PARTITION BY nk_src_key,feat_sbtype_cd ORDER BY edw_end_dttm DESC)=1) ft
    ON              ft.nk_src_key=src.nk_public_id
    AND             ft.feat_sbtype_cd=feat_sbtp.tgt_idntftn_val
    left outer join
                    (
                             SELECT   quotn_id,
                                      nk_job_nbr,
                                      vers_nbr
                             FROM     db_t_prod_core.insrnc_quotn qualify row_number() over(PARTITION BY nk_job_nbr, vers_nbr, src_sys_cd ORDER BY edw_end_dttm DESC) = 1) qtn
    ON              qtn.nk_job_nbr=src.jobnumber
    AND             qtn.vers_nbr=src.branchnumber
    left outer join
                    (
                                    SELECT DISTINCT prty_asset_id,
                                                    asset_insrnc_hist_type_cd,
                                                    asset_desc,
                                                    prty_asset_name,
                                                    prty_asset_strt_dttm,
                                                    prty_asset_end_dttm,
                                                    src_sys_cd,
                                                    asset_host_id_val,
                                                    prty_asset_sbtype_cd,
                                                    prty_asset_clasfcn_cd
                                    FROM            db_t_prod_core.prty_asset
                                    WHERE           cast(edw_end_dttm AS DATE) = ''9999-12-31'')prty
    ON              prty.asset_host_id_val = src.prtyasset_fixedid
    AND             prty.prty_asset_sbtype_cd = asset_sbtp.tgt_idntftn_val
    AND             prty.prty_asset_clasfcn_cd = clsfctn.tgt_idntftn_val
    left outer join
                    (
                                    SELECT DISTINCT asset_cntrct_role_sbtype_cd,
                                                    quotn_asset_strt_dttm,
                                                    quotn_asset_feat_strt_dttm,
                                                    quotn_asset_feat_end_dttm,
                                                    quotn_asset_feat_amt,
                                                    quotn_asset_feat_dt,
                                                    feat_efect_type_cd,
                                                    quotn_asset_feat_txt,
                                                    quotn_asset_feat_ind,
                                                    prty_asset_id,
                                                    quotn_id,
                                                    feat_id
                                    FROM            db_t_prod_core.quotn_asset_feat
                                    WHERE           cast(edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE)) tgt
    ON              tgt.prty_asset_id = prty.prty_asset_id
    AND             tgt.quotn_id = qtn.quotn_id
    AND             tgt.feat_id = ft.feat_id );
    -- Component exp_data_transformation, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_data_transformation AS
    (
           SELECT ltrim ( rtrim ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_ind ) )   AS lkp_quotn_asset_feat_ind1,
                  lower ( ltrim ( rtrim ( sq_pc_quotn_asset_feat_x.feat_covtermtype ) ) ) AS v_feat_covtermtype,
                  ltrim ( rtrim ( sq_pc_quotn_asset_feat_x.feat_val ) )                   AS v_feat_val,
                  CASE
                         WHEN v_feat_covtermtype = ''direct'' THEN ifnull(try_to_decimal(sq_pc_quotn_asset_feat_x.feat_val), 0)
                         ELSE NULL
                  END                    AS v_quotn_asset_feat_amt,
                  v_quotn_asset_feat_amt AS out_quotn_asset_feat_amt,
                  CASE
                         WHEN v_feat_covtermtype = ''datetime'' THEN to_date(to_varchar(v_feat_val ), ''MM/DD/YYYY'')
                         ELSE NULL
                  END                   AS v_quotn_asset_feat_dt,
                  v_quotn_asset_feat_dt AS out_quotn_asset_feat_dt,
                  CASE
                         WHEN (
                                       v_feat_covtermtype = ''shorttext'' )
                         OR     (
                                       v_feat_covtermtype = ''typekey'' ) THEN sq_pc_quotn_asset_feat_x.feat_val
                         ELSE NULL
                  END                    AS v_quotn_asset_feat_txt,
                  v_quotn_asset_feat_txt AS quotn_asset_feat_txt,
                  CASE
                         WHEN v_feat_covtermtype = ''bit'' THEN v_feat_val
                         ELSE NULL
                  END                                                                    AS v_quotn_asset_feat_ind,
                  v_quotn_asset_feat_ind                                                 AS quotn_asset_feat_ind,
                  sq_pc_quotn_asset_feat_x.prty_asset_id                                 AS prty_asset_id,
                  sq_pc_quotn_asset_feat_x.quotn_id                                      AS quotn_id,
                  sq_pc_quotn_asset_feat_x.asset_cntrct_role_sbtype_cd                   AS asset_cntrct_role_sbtype_cd,
                  sq_pc_quotn_asset_feat_x.quotn_asset_strt_dttm                         AS quotn_asset_strt_dttm,
                  sq_pc_quotn_asset_feat_x.feat_id                                       AS feat_id,
                  sq_pc_quotn_asset_feat_x.quotn_asset_feat_strt_dttm                    AS quotn_asset_feat_strt_dttm,
                  sq_pc_quotn_asset_feat_x.quotn_asset_feat_end_dttm                     AS quotn_asset_feat_end_dttm,
                  sq_pc_quotn_asset_feat_x.trans_strt_dttm                               AS trans_strt_dttm,
                  to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                  sq_pc_quotn_asset_feat_x.rate_symb_cd                                  AS rate_symb_cd,
                  sq_pc_quotn_asset_feat_x.polcov_ratemodifier                           AS polcov_ratemodifier,
                  sq_pc_quotn_asset_feat_x.polcov_eligible                               AS polcov_eligible,
                  sq_pc_quotn_asset_feat_x.o_discountsurcharge_alfa_typecd               AS o_discountsurcharge_alfa_typecd,
                  current_timestamp                                                      AS edw_strt_dttm,
                  to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                  md5 ( sq_pc_quotn_asset_feat_x.lkp_asset_cntrct_role_sbtype_cd
                         || to_char ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_strt_dttm )
                         || to_char ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_strt_dttm )
                         || to_char ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_end_dttm )
                         || to_char ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_amt )
                         || sq_pc_quotn_asset_feat_x.lkp_feat_efect_type_cd
                         || to_char ( sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_dt )
                         || sq_pc_quotn_asset_feat_x.lkp_quotn_asset_feat_txt
                         || lkp_quotn_asset_feat_ind1 ) AS lkp_checksum,
                  md5 ( sq_pc_quotn_asset_feat_x.asset_cntrct_role_sbtype_cd
                         || to_char ( sq_pc_quotn_asset_feat_x.quotn_asset_strt_dttm )
                         || to_char ( sq_pc_quotn_asset_feat_x.quotn_asset_feat_strt_dttm )
                         || to_char ( sq_pc_quotn_asset_feat_x.quotn_asset_feat_end_dttm )
                         || to_char ( v_quotn_asset_feat_amt )
                         || sq_pc_quotn_asset_feat_x.o_discountsurcharge_alfa_typecd
                         || to_char ( v_quotn_asset_feat_dt )
                         || v_quotn_asset_feat_txt
                         || v_quotn_asset_feat_ind ) AS in_checksum,
                  :PRCS_ID                           AS prcs_id,
                  CASE
                         WHEN sq_pc_quotn_asset_feat_x.lkp_prty_asset_id IS NULL THEN ''I''
                         ELSE (
                                CASE
                                       WHEN lkp_checksum <> in_checksum THEN ''U''
                                       ELSE ''R''
                                END )
                  END AS flag_ins_upd,
                  sq_pc_quotn_asset_feat_x.source_record_id
           FROM   sq_pc_quotn_asset_feat_x );
    -- PIPELINE START FOR 2
    -- Component SQ_pc_quotn_asset_feat_x1, Type SOURCE
    CREATE
    OR
    replace TEMPORARY TABLE sq_pc_quotn_asset_feat_x1 AS
    (
           SELECT
                  /* adding column aliases to ensure proper downstream column references */
                  $1 AS jobnumber,
                  $2 AS source_record_id
           FROM   (
                           SELECT   src.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                           FROM     (
                                                    SELECT DISTINCT jobnumber_stg
                                                    FROM            db_t_prod_stag.pc_job
                                                    WHERE           1=2 ) src ) );
    -- Component rtr_quotn_asset_feat_upd_ins_INSERT, Type ROUTER Output Group INSERT
    CREATE
    OR
    replace TEMPORARY TABLE rtr_quotn_asset_feat_upd_ins_insert AS
    SELECT exp_data_transformation.flag_ins_upd                    AS flag_ins_upd,
           exp_data_transformation.prty_asset_id                   AS prty_asset_id,
           exp_data_transformation.quotn_id                        AS quotn_id,
           exp_data_transformation.asset_cntrct_role_sbtype_cd     AS asset_cntrct_role_sbtype_cd,
           exp_data_transformation.quotn_asset_strt_dttm           AS quotn_asset_strt_dttm,
           exp_data_transformation.feat_id                         AS feat_id,
           exp_data_transformation.quotn_asset_feat_strt_dttm      AS quotn_asset_feat_strt_dttm,
           exp_data_transformation.quotn_asset_feat_end_dttm       AS quotn_asset_feat_end_dttm,
           exp_data_transformation.edw_strt_dttm                   AS edw_strt_dttm,
           exp_data_transformation.edw_end_dttm                    AS edw_end_dttm,
           exp_data_transformation.trans_strt_dttm                 AS trans_strt_dttm,
           exp_data_transformation.trans_end_dttm                  AS trans_end_dttm,
           exp_data_transformation.rate_symb_cd                    AS rate_symb_cd,
           exp_data_transformation.prcs_id                         AS prcs_id,
           exp_data_transformation.out_quotn_asset_feat_amt        AS out_quotn_asset_feat_amt,
           exp_data_transformation.out_quotn_asset_feat_dt         AS out_quotn_asset_feat_dt,
           exp_data_transformation.quotn_asset_feat_txt            AS quotn_asset_feat_txt,
           exp_data_transformation.quotn_asset_feat_ind            AS quotn_asset_feat_ind,
           exp_data_transformation.polcov_ratemodifier             AS polcov_ratemodifier,
           exp_data_transformation.polcov_eligible                 AS polcov_eligible,
           exp_data_transformation.o_discountsurcharge_alfa_typecd AS o_discountsurcharge_alfa_typecd,
           exp_data_transformation.source_record_id
    FROM   exp_data_transformation
    WHERE  exp_data_transformation.prty_asset_id IS NOT NULL
    AND    exp_data_transformation.quotn_id IS NOT NULL
    AND    exp_data_transformation.feat_id IS NOT NULL
    AND    (
                  exp_data_transformation.flag_ins_upd = ''I''
           OR     exp_data_transformation.flag_ins_upd = ''U'' );
    
    -- Component upd_quotn_asset_feat_ins_upd, Type UPDATE
    CREATE
    OR
    replace TEMPORARY TABLE upd_quotn_asset_feat_ins_upd AS
    (
           /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
           SELECT rtr_quotn_asset_feat_upd_ins_insert.prty_asset_id                   AS prty_asset_id1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_id                        AS quotn_id1,
                  rtr_quotn_asset_feat_upd_ins_insert.asset_cntrct_role_sbtype_cd     AS asset_cntrct_role_sbtype_cd1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_asset_strt_dttm           AS quotn_asset_strt_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.feat_id                         AS feat_id1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_asset_feat_strt_dttm      AS quotn_asset_feat_strt_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_asset_feat_end_dttm       AS quotn_asset_feat_end_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.edw_strt_dttm                   AS edw_strt_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.edw_end_dttm                    AS edw_end_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.trans_strt_dttm                 AS trans_strt_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.trans_end_dttm                  AS trans_end_dttm1,
                  rtr_quotn_asset_feat_upd_ins_insert.rate_symb_cd                    AS rate_symb_cd1,
                  rtr_quotn_asset_feat_upd_ins_insert.prcs_id                         AS prcs_id1,
                  rtr_quotn_asset_feat_upd_ins_insert.out_quotn_asset_feat_amt        AS out_quotn_asset_feat_amt1,
                  rtr_quotn_asset_feat_upd_ins_insert.out_quotn_asset_feat_dt         AS out_quotn_asset_feat_dt1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_asset_feat_txt            AS quotn_asset_feat_txt1,
                  rtr_quotn_asset_feat_upd_ins_insert.quotn_asset_feat_ind            AS quotn_asset_feat_ind1,
                  rtr_quotn_asset_feat_upd_ins_insert.polcov_ratemodifier             AS polcov_ratemodifier1,
                  rtr_quotn_asset_feat_upd_ins_insert.polcov_eligible                 AS polcov_eligible1,
                  rtr_quotn_asset_feat_upd_ins_insert.o_discountsurcharge_alfa_typecd AS o_discountsurcharge_alfa_typecd1,
                  0                                                                   AS update_strategy_action,
                  rtr_quotn_asset_feat_upd_ins_insert.source_record_id                AS source_record_id
           FROM   rtr_quotn_asset_feat_upd_ins_insert );
    -- Component QUOTN_ASSET_FEAT_ins1, Type TARGET
    INSERT INTO db_t_prod_core.quotn_asset_feat
                (
                            prty_asset_id
                )
    SELECT sq_pc_quotn_asset_feat_x1.jobnumber AS prty_asset_id
    FROM   sq_pc_quotn_asset_feat_x1;
    
    -- PIPELINE END FOR 2
    -- Component QUOTN_ASSET_FEAT_ins1, Type Post SQL
    UPDATE db_t_prod_core.quotn_asset_feat
    SET    edw_end_dttm=a.lead1 ,
           trans_end_dttm=a.lead
    FROM   (
                           SELECT DISTINCT prty_asset_id,
                                           quotn_id,
                                           feat_id,
                                           edw_strt_dttm ,
                                           trans_strt_dttm,
                                           max(edw_strt_dttm) over (PARTITION BY prty_asset_id,quotn_id,feat_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 SECOND'' AS lead1 ,
                                           max(trans_strt_dttm) over (PARTITION BY prty_asset_id,quotn_id,feat_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 SECOND'' AS lead
                           FROM            db_t_prod_core.quotn_asset_feat ) a
    WHERE  quotn_asset_feat.edw_strt_dttm = a.edw_strt_dttm
    AND    quotn_asset_feat.trans_strt_dttm = a.trans_strt_dttm
    AND    quotn_asset_feat.prty_asset_id=a.prty_asset_id
    AND    quotn_asset_feat.quotn_id=a.quotn_id
    AND    quotn_asset_feat.feat_id=a.feat_id
    AND    cast(quotn_asset_feat.edw_end_dttm AS   DATE)=''9999-12-31''
    AND    cast(quotn_asset_feat.trans_end_dttm AS DATE)=''9999-12-31''
    AND    lead1 IS NOT NULL
    AND    lead IS NOT NULL;
    
    -- Component exp_quotn_asset_feat_ins_upd, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_quotn_asset_feat_ins_upd AS
    (
           SELECT upd_quotn_asset_feat_ins_upd.prty_asset_id1                   AS prty_asset_id1,
                  upd_quotn_asset_feat_ins_upd.quotn_id1                        AS quotn_id1,
                  upd_quotn_asset_feat_ins_upd.asset_cntrct_role_sbtype_cd1     AS asset_cntrct_role_sbtype_cd1,
                  upd_quotn_asset_feat_ins_upd.quotn_asset_strt_dttm1           AS quotn_asset_strt_dttm1,
                  upd_quotn_asset_feat_ins_upd.feat_id1                         AS feat_id1,
                  upd_quotn_asset_feat_ins_upd.quotn_asset_feat_strt_dttm1      AS quotn_asset_feat_strt_dttm1,
                  upd_quotn_asset_feat_ins_upd.quotn_asset_feat_end_dttm1       AS quotn_asset_feat_end_dttm1,
                  upd_quotn_asset_feat_ins_upd.edw_strt_dttm1                   AS edw_strt_dttm1,
                  upd_quotn_asset_feat_ins_upd.edw_end_dttm1                    AS edw_end_dttm1,
                  upd_quotn_asset_feat_ins_upd.trans_strt_dttm1                 AS trans_strt_dttm1,
                  upd_quotn_asset_feat_ins_upd.trans_end_dttm1                  AS trans_end_dttm1,
                  upd_quotn_asset_feat_ins_upd.rate_symb_cd1                    AS rate_symb_cd1,
                  upd_quotn_asset_feat_ins_upd.prcs_id1                         AS prcs_id1,
                  upd_quotn_asset_feat_ins_upd.out_quotn_asset_feat_amt1        AS out_quotn_asset_feat_amt1,
                  upd_quotn_asset_feat_ins_upd.out_quotn_asset_feat_dt1         AS out_quotn_asset_feat_dt1,
                  upd_quotn_asset_feat_ins_upd.quotn_asset_feat_txt1            AS quotn_asset_feat_txt1,
                  upd_quotn_asset_feat_ins_upd.quotn_asset_feat_ind1            AS quotn_asset_feat_ind1,
                  upd_quotn_asset_feat_ins_upd.polcov_ratemodifier1             AS polcov_ratemodifier1,
                  upd_quotn_asset_feat_ins_upd.polcov_eligible1                 AS polcov_eligible1,
                  upd_quotn_asset_feat_ins_upd.o_discountsurcharge_alfa_typecd1 AS o_discountsurcharge_alfa_typecd1,
                  upd_quotn_asset_feat_ins_upd.source_record_id
           FROM   upd_quotn_asset_feat_ins_upd );
    -- Component QUOTN_ASSET_FEAT_ins, Type TARGET
    INSERT INTO db_t_prod_core.quotn_asset_feat
                (
                            prty_asset_id,
                            quotn_id,
                            asset_cntrct_role_sbtype_cd,
                            quotn_asset_strt_dttm,
                            feat_id,
                            quotn_asset_feat_strt_dttm,
                            quotn_asset_feat_end_dttm,
                            rate_symb_cd,
                            quotn_asset_feat_amt,
                            quotn_asset_feat_rate,
                            quotn_asset_feat_dt,
                            feat_efect_type_cd,
                            quotn_asset_feat_txt,
                            quotn_asset_feat_ind,
                            feat_elgbl_ind,
                            prcs_id,
                            edw_strt_dttm,
                            edw_end_dttm,
                            trans_strt_dttm,
                            trans_end_dttm
                )
    SELECT exp_quotn_asset_feat_ins_upd.prty_asset_id1                   AS prty_asset_id,
           exp_quotn_asset_feat_ins_upd.quotn_id1                        AS quotn_id,
           exp_quotn_asset_feat_ins_upd.asset_cntrct_role_sbtype_cd1     AS asset_cntrct_role_sbtype_cd,
           exp_quotn_asset_feat_ins_upd.quotn_asset_strt_dttm1           AS quotn_asset_strt_dttm,
           exp_quotn_asset_feat_ins_upd.feat_id1                         AS feat_id,
           exp_quotn_asset_feat_ins_upd.quotn_asset_feat_strt_dttm1      AS quotn_asset_feat_strt_dttm,
           exp_quotn_asset_feat_ins_upd.quotn_asset_feat_end_dttm1       AS quotn_asset_feat_end_dttm,
           exp_quotn_asset_feat_ins_upd.rate_symb_cd1                    AS rate_symb_cd,
           exp_quotn_asset_feat_ins_upd.out_quotn_asset_feat_amt1        AS quotn_asset_feat_amt,
           exp_quotn_asset_feat_ins_upd.polcov_ratemodifier1             AS quotn_asset_feat_rate,
           exp_quotn_asset_feat_ins_upd.out_quotn_asset_feat_dt1         AS quotn_asset_feat_dt,
           exp_quotn_asset_feat_ins_upd.o_discountsurcharge_alfa_typecd1 AS feat_efect_type_cd,
           exp_quotn_asset_feat_ins_upd.quotn_asset_feat_txt1            AS quotn_asset_feat_txt,
           exp_quotn_asset_feat_ins_upd.quotn_asset_feat_ind1            AS quotn_asset_feat_ind,
           exp_quotn_asset_feat_ins_upd.polcov_eligible1                 AS feat_elgbl_ind,
           exp_quotn_asset_feat_ins_upd.prcs_id1                         AS prcs_id,
           exp_quotn_asset_feat_ins_upd.edw_strt_dttm1                   AS edw_strt_dttm,
           exp_quotn_asset_feat_ins_upd.edw_end_dttm1                    AS edw_end_dttm,
           exp_quotn_asset_feat_ins_upd.trans_strt_dttm1                 AS trans_strt_dttm,
           exp_quotn_asset_feat_ins_upd.trans_end_dttm1                  AS trans_end_dttm
    FROM   exp_quotn_asset_feat_ins_upd;
    
    -- PIPELINE END FOR 1
    -- Component QUOTN_ASSET_FEAT_ins, Type Post SQL
    UPDATE db_t_prod_core.quotn_asset_feat
    SET    edw_end_dttm=a.lead1 ,
           trans_end_dttm=a.lead
    FROM   (
                           SELECT DISTINCT prty_asset_id,
                                           quotn_id,
                                           feat_id,
                                           edw_strt_dttm ,
                                           trans_strt_dttm,
                                           max(edw_strt_dttm) over (PARTITION BY prty_asset_id,quotn_id,feat_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 SECOND'' AS lead1 ,
                                           max(trans_strt_dttm) over (PARTITION BY prty_asset_id,quotn_id,feat_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 SECOND'' AS lead
                           FROM            db_t_prod_core.quotn_asset_feat ) a
    WHERE  quotn_asset_feat.edw_strt_dttm = a.edw_strt_dttm
    AND    quotn_asset_feat.trans_strt_dttm = a.trans_strt_dttm
    AND    quotn_asset_feat.prty_asset_id=a.prty_asset_id
    AND    quotn_asset_feat.quotn_id=a.quotn_id
    AND    quotn_asset_feat.feat_id=a.feat_id
    AND    cast(quotn_asset_feat.edw_end_dttm AS   DATE)=''9999-12-31''
    AND    cast(quotn_asset_feat.trans_end_dttm AS DATE)=''9999-12-31''
    AND    lead1 IS NOT NULL
    AND    lead IS NOT NULL;
  
  END;
  ';