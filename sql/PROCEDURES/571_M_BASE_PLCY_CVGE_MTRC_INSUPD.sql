-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_CVGE_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    run_id STRING;
    workflow_name STRING;
    session_name STRING;
    start_dttm TIMESTAMP;
    end_dttm TIMESTAMP;
    PRCS_ID STRING;
    v_start_time TIMESTAMP;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_agmt_asset_insupd'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
    v_start_time := CURRENT_TIMESTAMP();


  -- PIPELINE START FOR 1
  -- Component SQ_GW_PREMIUM_TRANS, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_premium_trans AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS tgt_plcy_cvge_mtrc_strt_dttm,
                $2  AS src_agmt_id,
                $3  AS src_feat_id,
                $4  AS src_agmt_feat_strt_dttm,
                $5  AS src_agmt_feat_role_cd,
                $6  AS src_insrnc_mtrc_type_cd,
                $7  AS src_plcy_cvge_mtrc_strt_dttm,
                $8  AS src_plcy_cvge_mtrc_end_dttm,
                $9  AS src_tm_prd_cd,
                $10 AS src_plcy_asset_cvge_amt,
                $11 AS src_uom_cd,
                $12 AS src_cury_cd,
                $13 AS src_uom_type_cd,
                $14 AS src_trans_strt_dttm,
                $15 AS src_md5,
                $16 AS tgt_md5,
                $17 AS ins_upd_flag,
                $18 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH non_poltrm_temp1 AS
                                  (
                                            SELECT    pc_policyperiod.publicid_stg                             AS publicid,
                                                      expandedcosttable.coverable_or_policyline_covpattern_stg AS cov_type_cd,
                                                      pc_policyperiod.updatetime_stg                           AS updatetime,
                                                      pc_policyperiod.editeffectivedate_stg                    AS editeffectivedate,
                                                      pc_patransaction.amount_stg                              AS amount,
                                                      pctl_policyperiodstatus.typecode_stg                     AS status
                                            FROM      db_t_prod_stag.pc_patransaction
                                            join
                                                      (
                                                                      SELECT DISTINCT pc_policyperiod.policynumber_stg,
                                                                                      pc_personalvehiclecov.personalvehicle_stg personalvehicleid,
                                                                                      CASE
                                                                                                      WHEN pc_pacost.personalvehiclecov_stg IS NOT NULL THEN ''pc_personalvehicle''
                                                                                                      WHEN pc_pacost.personalautocov_alfa_stg IS NOT NULL THEN ''pc_policyline''
                                                                                                      WHEN pc_pacost.personalautocov_stg IS NOT NULL THEN ''pc_policyline''
                                                                                      END AS table_name_for_fixedid_stg,
                                                                                      CASE
                                                                                                      WHEN pc_pacost.personalvehiclecov_stg IS NOT NULL THEN pc_personalvehiclecov.patterncode_stg
                                                                                                      WHEN pc_pacost.personalautocov_alfa_stg IS NOT NULL THEN pacov_alfa.patterncode_stg
                                                                                      END AS coverable_or_policyline_covpattern_stg,
                                                                                      CASE
                                                                                                      WHEN pc_pacost.personalvehiclecov_stg IS NOT NULL THEN pc_personalvehiclecov.patterncode_stg
                                                                                                      WHEN pc_pacost.personalautocov_alfa_stg IS NOT NULL THEN pacov_alfa.patterncode_stg
                                                                                      END AS coverable_or_policyline_covname_stg,
                                                                                      pc_pacost.*
                                                                      FROM            db_t_prod_stag.pc_pacost
                                                                      join            db_t_prod_stag.pc_policyperiod
                                                                      ON              pc_pacost.branchid_stg=pc_policyperiod.id_stg
                                                                                      /*Add unit-level coverages for auto*/
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pc_personalvehiclecov.patterncode_stg,
                                                                                                                      pc_personalvehiclecov.fixedid_stg,
                                                                                                                      pc_personalvehiclecov.branchid_stg,
                                                                                                                      /*  EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pc_personalvehiclecov.personalvehicle_stg
                                                                                                      FROM            db_t_prod_stag.pc_personalvehiclecov,
                                                                                                                      db_t_prod_stag.pc_policyperiod
                                                                                                      WHERE           pc_personalvehiclecov.branchid_stg=pc_policyperiod.id_stg
                                                                                                      AND             (
                                                                                                                                      pc_personalvehiclecov.expirationdate_stg IS NULL
                                                                                                                      OR              pc_personalvehiclecov.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                      /*  EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                                      ) as pc_personalvehiclecov
                                                                      ON              pc_pacost.personalvehiclecov_stg = pc_personalvehiclecov.fixedid_stg
                                                                      AND             pc_personalvehiclecov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                      /*Add policy-level coverages for auto*/
                                                                      AND             pc_pacost.branchid_stg = pc_personalvehiclecov.branchid_stg
                                                                                      /*  EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                      left join       db_t_prod_stag.pc_personalautocov pacov_alfa
                                                                      ON              pc_pacost.personalautocov_alfa_stg = pacov_alfa.fixedid_stg
                                                                      AND             pacov_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                      AND             pacov_alfa.expirationdate_stg IS NULL
                                                                      left join       db_t_prod_stag.pc_policyline paline_unit_alfa
                                                                      ON              pacov_alfa.paline_stg = paline_unit_alfa.id_stg ) expandedcosttable
                                            ON        pc_patransaction.pacost_stg = expandedcosttable.id_stg
                                            AND       expandedcosttable.coverable_or_policyline_covpattern_stg LIKE ''PA%''
                                            join      db_t_prod_stag.pctl_chargepattern
                                            ON        expandedcosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                            left join db_t_prod_stag.pctl_pacost
                                            ON        expandedcosttable.subtype_stg = pctl_pacost.id_stg
                                            left join db_t_prod_stag.pctl_periltype_alfa autoperiltype
                                            ON        expandedcosttable.periltype_alfa_stg = autoperiltype.id_stg
                                            join      db_t_prod_stag.pc_policyperiod
                                            ON        pc_patransaction.branchid_stg = pc_policyperiod.id_stg
                                            AND       expandedcosttable.policynumber_stg=pc_policyperiod.policynumber_stg
                                            AND       pc_patransaction.branchid_stg=pc_policyperiod.id_stg
                                            left join db_t_prod_stag.pctl_policyperiodstatus
                                            ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                            join      db_t_prod_stag.pc_job
                                            ON        pc_policyperiod.jobid_stg = pc_job.id_stg
                                            left join db_t_prod_stag.pctl_job
                                            ON        pc_job.subtype_stg = pctl_job.id_stg
                                            left join db_t_prod_stag.pc_policyline
                                            ON        pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                            AND       pc_policyline.expirationdate_stg IS NULL
                                            left join db_t_prod_stag.pctl_papolicytype_alfa
                                            ON        pc_policyline.papolicytype_alfa_stg = pctl_papolicytype_alfa.id_stg
                                            left join db_t_prod_stag.pc_policy
                                            ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                            left join db_t_prod_stag.pc_account
                                            ON        pc_policy.accountid_stg = pc_account.id_stg
                                            WHERE     pctl_chargepattern.name_stg = ''Premium''
                                            AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                            AND       pc_policyperiod.updatetime_stg <= (:end_dttm) ), non_poltrm_temp2 AS
                                  (
                                            SELECT    pc_policyperiod.publicid_stg                                  publicid,
                                                      expandedhocosttable.coverable_or_policyline_covpattern_stg AS cov_type_cd,
                                                      pc_policyperiod.updatetime_stg                             AS updatetime,
                                                      pc_policyperiod.editeffectivedate_stg                      AS editeffectivedate,
                                                      SUM(pcx_hotransaction_hoe.amount_stg)                      AS amount,
                                                      pctl_policyperiodstatus.typecode_stg                       AS status
                                            FROM      db_t_prod_stag.pcx_hotransaction_hoe
                                            join
                                                      (
                                                                      SELECT DISTINCT pc_policyperiod.policynumber_stg,
                                                                                      pcx_dwellingcov_hoe.dwelling_stg                  AS dwellingid_stg,
                                                                                      pcx_holineschcovitemcov_alfa.holineschcovitem_stg    scheduleditemid_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_homeownerscost_hoe.dwellingcov_stg IS NOT NULL THEN ''pcx_dwelling_hoe''
                                                                                                      WHEN pcx_homeownerscost_hoe.scheditemcov_stg IS NOT NULL THEN ''pcx_holineschedcovitem_alfa''
                                                                                                      WHEN pcx_homeownerscost_hoe.homeownerslinecov_stg IS NOT NULL THEN ''pc_policyline''
                                                                                      END AS table_name_for_fixedid_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_homeownerscost_hoe.dwellingcov_stg IS NOT NULL THEN pcx_dwellingcov_hoe.patterncode_stg
                                                                                                      WHEN pcx_homeownerscost_hoe.scheditemcov_stg IS NOT NULL THEN pcx_holineschcovitemcov_alfa.patterncode_stg
                                                                                                      WHEN pcx_homeownerscost_hoe.homeownerslinecov_stg IS NOT NULL THEN pcx_homeownerslinecov_hoe.patterncode_stg
                                                                                      END AS coverable_or_policyline_covpattern_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_homeownerscost_hoe.dwellingcov_stg IS NOT NULL THEN pcx_dwellingcov_hoe.patterncode_stg
                                                                                                      WHEN pcx_homeownerscost_hoe.scheditemcov_stg IS NOT NULL THEN pcx_holineschcovitemcov_alfa.patterncode_stg
                                                                                                      WHEN pcx_homeownerscost_hoe.homeownerslinecov_stg IS NOT NULL THEN pcx_homeownerslinecov_hoe.patterncode_stg
                                                                                      END AS coverable_or_policyline_covname_stg,
                                                                                      pcx_homeownerscost_hoe.*
                                                                      FROM            db_t_prod_stag.pcx_homeownerscost_hoe
                                                                      join            db_t_prod_stag.pc_policyperiod
                                                                      ON              pcx_homeownerscost_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                      /*Add unit-level coverages for homeowners*/
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                      pcx_dwellingcov_hoe.fixedid_stg,
                                                                                                                      pcx_dwellingcov_hoe.branchid_stg,
                                                                                                                      /*  EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_dwellingcov_hoe.dwelling_stg
                                                                                                      FROM            db_t_prod_stag.pcx_dwellingcov_hoe,
                                                                                                                      db_t_prod_stag.pc_policyperiod
                                                                                                      WHERE           pcx_dwellingcov_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                                                      /* and (pcx_dwellingcov_hoe.ExpirationDate_stg is null OR pcx_dwellingcov_hoe.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg)-- EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                                                                      qualify row_number() over(PARTITION BY pcx_dwellingcov_hoe.branchid_stg , pcx_dwellingcov_hoe.fixedid_stg ORDER BY coalesce(pcx_dwellingcov_hoe.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC)=1
                                                                                                                      /*  EIM-47362 - FEAT_ID-9999 */
                                                                                      ) as pcx_dwellingcov_hoe
                                                                      ON              pcx_homeownerscost_hoe.dwellingcov_stg = pcx_dwellingcov_hoe.fixedid_stg
                                                                      AND             pcx_dwellingcov_hoe.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      AND             pcx_homeownerscost_hoe.branchid_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                      /*  EIM-45223 DB_T_PROD_CORE.PLCY_CVGE_MTRC duplicate DB_T_PROD_COMN.premium            */
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_holineschcovitemcov_alfa.patterncode_stg,
                                                                                                                      pcx_holineschcovitemcov_alfa.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_holineschcovitemcov_alfa.holineschcovitem_stg
                                                                                                      FROM            db_t_prod_stag.pcx_holineschcovitemcov_alfa,
                                                                                                                      db_t_prod_stag.pc_policyperiod
                                                                                                      WHERE           pcx_holineschcovitemcov_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                      AND             pcx_holineschcovitemcov_alfa.expirationdate_stg IS NULL) as pcx_holineschcovitemcov_alfa
                                                                      ON              pcx_homeownerscost_hoe.scheditemcov_stg = pcx_holineschcovitemcov_alfa.fixedid_stg
                                                                      AND             pcx_holineschcovitemcov_alfa.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                      /*Add policy-level coverages for homeowners*/
                                                                      left join       db_t_prod_stag.pcx_homeownerslinecov_hoe
                                                                      ON              pcx_homeownerscost_hoe.homeownerslinecov_stg = pcx_homeownerslinecov_hoe.fixedid_stg
                                                                                      /*added fixedid_stg insetead of id_stg*/
                                                                      AND             pcx_homeownerslinecov_hoe.expirationdate_stg IS NULL) expandedhocosttable
                                            ON        pcx_hotransaction_hoe.homeownerscost_stg = expandedhocosttable.id_stg
                                            left join db_t_prod_stag.pctl_chargepattern
                                            ON        expandedhocosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                            left join db_t_prod_stag.pctl_homeownerscost_hoe
                                            ON        expandedhocosttable.subtype_stg = pctl_homeownerscost_hoe.id_stg
                                            left join db_t_prod_stag.pctl_periltype_alfa hoperiltype
                                            ON        expandedhocosttable.periltype_alfa_stg = hoperiltype.id_stg
                                            join      db_t_prod_stag.pc_policyperiod
                                            ON        pcx_hotransaction_hoe.branchid_stg = pc_policyperiod.id_stg
                                            AND       expandedhocosttable.policynumber_stg=pc_policyperiod.policynumber_stg
                                            join      db_t_prod_stag.pc_job
                                            ON        pc_policyperiod.jobid_stg = pc_job.id_stg
                                            left join db_t_prod_stag.pctl_job
                                            ON        pc_job.subtype_stg = pctl_job.id_stg
                                            left join db_t_prod_stag.pc_policyline
                                            ON        pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                            AND       pc_policyline.expirationdate_stg IS NULL
                                            left join db_t_prod_stag.pctl_hopolicytype_hoe
                                            ON        pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                            left join db_t_prod_stag.pc_policy
                                            ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                            left join db_t_prod_stag.pc_account
                                            ON        pc_policy.accountid_stg = pc_account.id_stg
                                            left join db_t_prod_stag.pctl_sectiontype_alfa
                                            ON        expandedhocosttable.sectiontype_alfa_stg=pctl_sectiontype_alfa.id_stg
                                            join      db_t_prod_stag.pctl_policyperiodstatus
                                            ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                            WHERE     pctl_chargepattern.name_stg = ''Premium''
                                            AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                            AND       pc_policyperiod.updatetime_stg <= (:end_dttm)
                                            GROUP BY  pc_policyperiod.publicid_stg ,
                                                      CASE
                                                                WHEN expandedhocosttable.dwellingid_stg IS NULL THEN expandedhocosttable.scheduleditemid_stg
                                                                ELSE expandedhocosttable.dwellingid_stg
                                                      END ,
                                                      expandedhocosttable.coverable_or_policyline_covpattern_stg,
                                                      pctl_policyperiodstatus.typecode_stg,
                                                      pc_job.jobnumber_stg,
                                                      pc_policyperiod.branchnumber_stg,
                                                      pc_policyperiod.updatetime_stg,
                                                      pc_policyperiod.editeffectivedate_stg,
                                                      pctl_job.typecode_stg ), non_poltrm_temp3 AS
                                  (
                                            SELECT    pc_policyperiod.publicid_stg                             publicid,
                                                      expandedcosttable.coverable_or_policyline_covpattern_stg cov_type_cd,
                                                      pc_policyperiod.updatetime_stg                           updatetime,
                                                      pc_policyperiod.editeffectivedate_stg                    editeffectivedate,
                                                      SUM(tr.amount_stg)                                       amount,
                                                      pctl_policyperiodstatus.typecode_stg                     status
                                            FROM      db_t_prod_stag.pcx_bp7transaction tr
                                            left join
                                                      (
                                                                      SELECT DISTINCT
                                                                                      CASE
                                                                                                      WHEN cost.buildingcov_stg IS NOT NULL THEN ''pcx_bp7building''
                                                                                                      WHEN cost.locationcov_stg IS NOT NULL THEN ''pcx_bp7location''
                                                                                                      WHEN cost.classificationcov_stg IS NOT NULL THEN ''pcx_bp7classification''
                                                                                                      WHEN cost.linecoverage_stg IS NOT NULL THEN ''pc_policyline''
                                                                                                      WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7locschedcovitemcov''
                                                                                                      WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7bldgschedcovitemcov''
                                                                                                      WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7lineschedcovitemcov''
                                                                                                      WHEN cost.buildingcond_stg IS NOT NULL THEN ''pcx_bp7buildingcond''
                                                                                                      WHEN cost.lineexcl_stg IS NOT NULL THEN ''pcx_bp7lineexcl''
                                                                                                      WHEN cost.classificationexcl_stg IS NOT NULL THEN ''pcx_bp7classificationexcl''
                                                                                      END AS table_name_for_fixedid_stg,
                                                                                      CASE
                                                                                                      WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.building_stg
                                                                                                      WHEN cost.locationcov_stg IS NOT NULL THEN lcov.location_stg
                                                                                                      WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.classification_stg
                                                                                                      WHEN cost.linecoverage_stg IS NOT NULL THEN licov.bp7line_stg
                                                                                                      WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.locschedcovitem_stg
                                                                                                      WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.bldgschedcovitem_stg
                                                                                                      WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.lineschedcovitem_stg
                                                                                                      WHEN cost.buildingcond_stg IS NOT NULL THEN bcond.building_stg
                                                                                                      WHEN cost.lineexcl_stg IS NOT NULL THEN liexcl.bp7line_stg
                                                                                                      WHEN cost.classificationexcl_stg IS NOT NULL THEN cexcl.classification_stg
                                                                                      END AS coverable_or_policyline_partyassetid_stg,
                                                                                      CASE
                                                                                                      WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.patterncode_stg
                                                                                                      WHEN cost.locationcov_stg IS NOT NULL THEN lcov.patterncode_stg
                                                                                                      WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.patterncode_stg
                                                                                                      WHEN cost.linecoverage_stg IS NOT NULL THEN licov.patterncode_stg
                                                                                                      WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.patterncode_stg
                                                                                                      WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.patterncode_stg
                                                                                                      WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.patterncode_stg
                                                                                                      WHEN cost.buildingcond_stg IS NOT NULL THEN bcond.patterncode_stg
                                                                                                      WHEN cost.lineexcl_stg IS NOT NULL THEN liexcl.patterncode_stg
                                                                                                      WHEN cost.classificationexcl_stg IS NOT NULL THEN cexcl.patterncode_stg
                                                                                      END AS coverable_or_policyline_covpattern_stg,
                                                                                      cost.id_stg,
                                                                                      cost.chargepattern_stg,
                                                                                      cp.typecode_stg class_stg
                                                                      FROM            db_t_prod_stag.pcx_bp7cost cost
                                                                                      /*Building Coverage*/
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT fixedid_stg,
                                                                                                                      building_stg,
                                                                                                                      patterncode_stg
                                                                                                      FROM            db_t_prod_stag.pcx_bp7buildingcov ) bcov
                                                                      ON              cost.buildingcov_stg = bcov.fixedid_stg
                                                                                      /*Location Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7locationcov lcov
                                                                      ON              cost.locationcov_stg = lcov.fixedid_stg
                                                                                      /* added fixedid_stg instead of id_stg in location coverage*/
                                                                                      /*Classification Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7classificationcov ccov
                                                                      ON              cost.classificationcov_stg = ccov.fixedid_stg
                                                                      left join       db_t_prod_stag.pcx_bp7classification c
                                                                      ON              ccov.classification_stg = c.id_stg
                                                                      left join       db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                      ON              c.bp7classpropertytype_stg = cp.id_stg
                                                                                      /*Line Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7linecov licov
                                                                      ON              cost.linecoverage_stg = licov.fixedid_stg
                                                                                      /* added fixedid_stg instead of id_stg in line coverage*/
                                                                                      /*Line cond*/
                                                                      left join       db_t_prod_stag.pcx_bp7buildingcond bcond
                                                                      ON              cost.buildingcond_stg = bcond.id_stg
                                                                                      /*LineExcl Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7lineexcl liexcl
                                                                      ON              cost.lineexcl_stg = liexcl.id_stg
                                                                                      /*Classfication Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7classificationexcl cexcl
                                                                      ON              cost.classificationexcl_stg= cexcl.id_stg
                                                                                      /*Location Scheduled Item Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7locschedcovitemcov lscov
                                                                      ON              cost.locschedcovitemcov_stg = lscov.id_stg
                                                                                      /*Building Scheduled Item Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7bldgschedcovitemcov bscov
                                                                      ON              cost.bldgschedcovitemcov_stg = bscov.id_stg
                                                                                      /*Line Scheduled Item Coverage*/
                                                                      left join       db_t_prod_stag.pcx_bp7lineschedcovitemcov liscov
                                                                      ON              cost.lineschedcovitemcov_stg = liscov.id_stg ) expandedcosttable
                                            ON        tr.bp7cost_stg = expandedcosttable.id_stg
                                            left join db_t_prod_stag.pctl_chargepattern
                                            ON        expandedcosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                            left join db_t_prod_stag.pc_policyperiod
                                            ON        tr.branchid_stg = pc_policyperiod.id_stg
                                            left join db_t_prod_stag.pctl_policyperiodstatus
                                            ON        pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                            left join db_t_prod_stag.pc_job
                                            ON        pc_policyperiod.jobid_stg = pc_job.id_stg
                                            left join db_t_prod_stag.pctl_job
                                            ON        pc_job.subtype_stg = pctl_job.id_stg
                                            WHERE     pctl_chargepattern.name_stg = ''Premium''
                                            AND       pctl_policyperiodstatus.typecode_stg=''Bound''
                                            AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                            AND       pc_policyperiod.updatetime_stg <= (:end_dttm)
                                            AND       expandedcosttable.coverable_or_policyline_covpattern_stg IS NOT NULL
                                            GROUP BY  pc_policyperiod.publicid_stg,
                                                      expandedcosttable.coverable_or_policyline_partyassetid_stg,
                                                      expandedcosttable.coverable_or_policyline_covpattern_stg,
                                                      pc_policyperiod.updatetime_stg,
                                                      pc_policyperiod.editeffectivedate_stg,
                                                      pctl_policyperiodstatus.typecode_stg,
                                                      pc_job.jobnumber_stg,
                                                      pc_policyperiod.branchnumber_stg,
                                                      pctl_job.typecode_stg
                                                     /*--------------------------------------------------------------------------------------------------------*/
                                  ), 
                                  non_poltrm_temp4 AS
                                  (
                                            /* PMOP-54876--Added as part of DB_T_STAG_MEMBXREF_PROD.Umbrella            */
                                            SELECT    pc_policyperiod.publicid_stg                               AS publicid,
                                                      expandedhocosttable.coverable_or_policyline_covpattern_stg AS cov_type_cd,
                                                      pcx_puptransaction.updatetime_stg                          AS updatetime,
                                                      pc_policyperiod.editeffectivedate_stg                      AS editeffectivedate,
                                                      SUM(pcx_puptransaction.amount_stg)                         AS amount,
                                                      pctl_policyperiodstatus.typecode_stg                       AS status
                                            FROM      db_t_prod_stag.pcx_puptransaction
                                            join
                                                      (
                                                                      SELECT DISTINCT pc_policyperiod.policynumber_stg,
                                                                                      pcx_puppersonalumbrellalinecov.personalumbrellaline_stg AS pupid_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_pupcost.puppersonalumbrellalinecov_stg IS NOT NULL THEN ''PC_POLICYLINE''
                                                                                      END AS table_name_for_fixedid_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_pupcost.puppersonalumbrellalinecov_stg IS NOT NULL THEN pcx_puppersonalumbrellalinecov.patterncode_stg
                                                                                      END AS coverable_or_policyline_covpattern_stg,
                                                                                      CASE
                                                                                                      WHEN pcx_pupcost.puppersonalumbrellalinecov_stg IS NOT NULL THEN pcx_puppersonalumbrellalinecov.patterncode_stg
                                                                                      END AS coverable_or_policyline_covname_stg,
                                                                                      pcx_pupcost.*
                                                                      FROM            db_t_prod_stag.pcx_pupcost
                                                                      join            db_t_prod_stag.pc_policyperiod
                                                                      ON              pcx_pupcost.branchid_stg=pc_policyperiod.id_stg
                                                                                      /*ADD POLICY-LEVEL COVERAGES FOR UMBRELLA*/
                                                                      left join       db_t_prod_stag.pcx_puppersonalumbrellalinecov
                                                                      ON              pcx_pupcost.puppersonalumbrellalinecov_stg = pcx_puppersonalumbrellalinecov.fixedid_stg
                                                                                      /*ADDED FIXEDID INSETEAD OF ID_STG*/
                                                                      AND             pcx_puppersonalumbrellalinecov.branchid_stg = pc_policyperiod.id_stg
                                                                      AND             pcx_puppersonalumbrellalinecov.expirationdate_stg IS NULL ) expandedhocosttable
                                            ON        pcx_puptransaction.cost_stg = expandedhocosttable.id_stg
                                            left join db_t_prod_stag.pctl_chargepattern
                                            ON        expandedhocosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                            left join db_t_prod_stag.pctl_pupcost
                                            ON        expandedhocosttable.subtype_stg = pctl_pupcost.id_stg
                                            join      db_t_prod_stag.pc_policyperiod
                                            ON        pcx_puptransaction.branchid_stg = pc_policyperiod.id_stg
                                            AND       expandedhocosttable.policynumber_stg =pc_policyperiod.policynumber_stg
                                            join      db_t_prod_stag.pc_job
                                            ON        pc_policyperiod.jobid_stg = pc_job.id_stg
                                            left join db_t_prod_stag.pctl_job
                                            ON        pc_job.subtype_stg = pctl_job.id_stg
                                            left join db_t_prod_stag.pc_policyline
                                            ON        pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                            AND       pc_policyline.expirationdate_stg IS NULL
                                            left join db_t_prod_stag.pctl_puppolicytype
                                            ON        pc_policyline.puppolicytype_stg = pctl_puppolicytype.id_stg
                                            left join db_t_prod_stag.pc_policy
                                            ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                            left join db_t_prod_stag.pc_account
                                            ON        pc_policy.accountid_stg = pc_account.id_stg
                                            join      db_t_prod_stag.pctl_policyperiodstatus
                                            ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                            WHERE     pctl_chargepattern.name_stg = ''Premium''
                                            AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                            AND       pc_policyperiod.updatetime_stg <= (:end_dttm)
                                            GROUP BY  pc_policyperiod.publicid_stg,
                                                      expandedhocosttable.coverable_or_policyline_covpattern_stg,
                                                      pctl_policyperiodstatus.typecode_stg,
                                                      pc_job.jobnumber_stg,
                                                      pc_policyperiod.branchnumber_stg,
                                                      pcx_puptransaction.updatetime_stg,
                                                      pc_policyperiod.editeffectivedate_stg,
                                                      pctl_job.typecode_stg ), non_poltrm_temp5 AS
                                  (
                                            SELECT    pc_policyperiod.publicid_stg                                publicid,
                                                      expandedfarmcosttable.coverable_or_policyline_covpattern AS cov_type_cd,
                                                      pcx_foptransaction.updatetime_stg                        AS updatetime,
                                                      /* EIM-49943                                        */
                                                      pc_policyperiod.editeffectivedate_stg AS editeffectivedate,
                                                      SUM(pcx_foptransaction.amount_stg)    AS amount ,
                                                      pctl_policyperiodstatus.typecode_stg  AS status
                                            FROM      db_t_prod_stag.pcx_foptransaction 
                                            join
                                                      (
                                                                      SELECT DISTINCT pc_policyperiod.policynumber_stg,
                                                                                      /***feat_id ****/
                                                                                      CASE
                                                                                                      WHEN pcx_fopcost.fopdwellingcov_stg IS NOT NULL THEN pcx_fopdwellingcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopdwellingschcovitemcov_stg IS NOT NULL THEN pcx_fopdwellingschcovitemcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopoutbuildingcov_stg IS NOT NULL THEN pcx_fopoutbuildingcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.foplivestockcov_stg IS NOT NULL THEN pcx_foplivestockcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopmachinerycov_stg IS NOT NULL THEN pcx_fopmachinerycov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopfeedandseedcov_stg IS NOT NULL THEN pcx_fopfeedandseedcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopblanketcov_stg IS NOT NULL THEN pcx_fopblanketcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopliabilitycov_stg IS NOT NULL THEN pcx_fopliabilitycov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopfarmownerslineschcovitemcov_stg IS NOT NULL THEN pcx_fopfarmownersschcovitemcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopliabilityschcovitemcov_stg IS NOT NULL THEN pcx_fopliabilityschcovitemcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopfarmownerslinecov_stg IS NOT NULL THEN pcx_fopfarmownerslinecov.patterncode_stg
                                                                                      END AS coverable_or_policyline_covpattern,
                                                                                      pcx_fopcost.chargepattern_stg,
                                                                                      pcx_fopcost.subtype_stg,
                                                                                      pcx_fopcost.id_stg
                                                                      FROM            db_t_prod_stag.pcx_fopcost 
                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                      ON              pcx_fopcost.branchid_stg=pc_policyperiod.id_stg
                                                                                      /*Add unit-level coverages for Farm*/
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopdwellingcov.patterncode_stg,
                                                                                                                      pcx_fopdwellingcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopdwellingcov.dwelling_stg,
                                                                                                                      pcx_fopdwellingcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopdwellingcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopdwellingcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopdwellingcov.branchid_stg , pcx_fopdwellingcov.fixedid_stg ORDER BY coalesce(pcx_fopdwellingcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopdwellingcov.updatetime_stg DESC,pcx_fopdwellingcov.createtime_stg DESC)=1 ) AS pcx_fopdwellingcov
                                                                      ON              pcx_fopcost.fopdwellingcov_stg = pcx_fopdwellingcov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopdwellingcov.branchid_stg
                                                                      AND             pcx_fopdwellingcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopoutbuildingcov.patterncode_stg,
                                                                                                                      pcx_fopoutbuildingcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopoutbuildingcov.outbuilding_stg,
                                                                                                                      pcx_fopoutbuildingcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopoutbuildingcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopoutbuildingcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopoutbuildingcov.branchid_stg , pcx_fopoutbuildingcov.fixedid_stg ORDER BY coalesce(pcx_fopoutbuildingcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopoutbuildingcov.updatetime_stg,pcx_fopoutbuildingcov.createtime_stg DESC)=1 ) AS pcx_fopoutbuildingcov
                                                                      ON              pcx_fopcost.fopoutbuildingcov_stg = pcx_fopoutbuildingcov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopoutbuildingcov.branchid_stg
                                                                      AND             pcx_fopoutbuildingcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_foplivestockcov.patterncode_stg,
                                                                                                                      pcx_foplivestockcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_foplivestockcov.livestock_stg,
                                                                                                                      pcx_foplivestockcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_foplivestockcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_foplivestockcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_foplivestockcov.branchid_stg , pcx_foplivestockcov.fixedid_stg ORDER BY coalesce(pcx_foplivestockcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_foplivestockcov.updatetime_stg DESC,pcx_foplivestockcov.createtime_stg DESC)=1 ) AS pcx_foplivestockcov
                                                                      ON              pcx_fopcost.foplivestockcov_stg = pcx_foplivestockcov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_foplivestockcov.branchid_stg
                                                                      AND             pcx_foplivestockcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopmachinerycov.patterncode_stg,
                                                                                                                      pcx_fopmachinerycov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopmachinerycov.machinery_stg,
                                                                                                                      pcx_fopmachinerycov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopmachinerycov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopmachinerycov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopmachinerycov.branchid_stg , pcx_fopmachinerycov.fixedid_stg ORDER BY coalesce(pcx_fopmachinerycov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopmachinerycov.updatetime_stg DESC,pcx_fopmachinerycov.createtime_stg DESC)=1 ) as pcx_fopmachinerycov
                                                                      ON              pcx_fopcost.fopmachinerycov_stg = pcx_fopmachinerycov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopmachinerycov.branchid_stg
                                                                      AND             pcx_fopmachinerycov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopfeedandseedcov.patterncode_stg,
                                                                                                                      pcx_fopfeedandseedcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopfeedandseedcov.feedandseed_stg,
                                                                                                                      pcx_fopfeedandseedcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopfeedandseedcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopfeedandseedcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopfeedandseedcov.branchid_stg , pcx_fopfeedandseedcov.fixedid_stg ORDER BY coalesce(pcx_fopfeedandseedcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopfeedandseedcov.updatetime_stg DESC,pcx_fopfeedandseedcov.createtime_stg DESC)=1 ) as pcx_fopfeedandseedcov
                                                                      ON              pcx_fopcost.fopfeedandseedcov_stg = pcx_fopfeedandseedcov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopfeedandseedcov.branchid_stg
                                                                      AND             pcx_fopfeedandseedcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopblanketcov.patterncode_stg,
                                                                                                                      pcx_fopblanketcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopblanketcov.blanket_stg,
                                                                                                                      pcx_fopblanketcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopblanketcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopblanketcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopblanketcov.branchid_stg , pcx_fopblanketcov.fixedid_stg ORDER BY coalesce(pcx_fopblanketcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopblanketcov.updatetime_stg DESC,pcx_fopblanketcov.createtime_stg DESC)=1 ) as pcx_fopblanketcov
                                                                      ON              pcx_fopcost.fopblanketcov_stg = pcx_fopblanketcov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopblanketcov.branchid_stg
                                                                      AND             pcx_fopblanketcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopliabilitycov.patterncode_stg,
                                                                                                                      pcx_fopliabilitycov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopliabilitycov.liability_stg,
                                                                                                                      pcx_fopliabilitycov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopliabilitycov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopliabilitycov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopliabilitycov.branchid_stg , pcx_fopliabilitycov.fixedid_stg ORDER BY coalesce(pcx_fopliabilitycov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopliabilitycov.updatetime_stg DESC,pcx_fopliabilitycov.createtime_stg DESC)=1 ) as pcx_fopliabilitycov
                                                                      ON              pcx_fopcost.fopliabilitycov_stg = pcx_fopliabilitycov.fixedid_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopliabilitycov.branchid_stg
                                                                      AND             pcx_fopliabilitycov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopdwellingschcovitemcov.patterncode_stg,
                                                                                                                      pcx_fopdwellingschcovitemcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopdwellingschcovitemcov.fopdwellingschedulecovitem_stg,
                                                                                                                      pcx_fopdwellingschcovitemcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopdwellingschcovitemcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopdwellingschcovitemcov.branchid_stg , pcx_fopdwellingschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopdwellingschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopdwellingschcovitemcov.updatetime_stg DESC,pcx_fopdwellingschcovitemcov.createtime_stg DESC)=1 ) as pcx_fopdwellingschcovitemcov
                                                                      ON              pcx_fopcost.fopdwellingschcovitemcov_stg = pcx_fopdwellingschcovitemcov.fixedid_stg
                                                                      AND             pcx_fopdwellingschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopdwellingschcovitemcov.branchid_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopliabilityschcovitemcov.patterncode_stg,
                                                                                                                      pcx_fopliabilityschcovitemcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopliabilityschcovitemcov.fopliabilityschedulecovitem_stg,
                                                                                                                      pcx_fopliabilityschcovitemcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              db_t_prod_stag.pcx_fopliabilityschcovitemcov .branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopliabilityschcovitemcov.branchid_stg , pcx_fopliabilityschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopliabilityschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopliabilityschcovitemcov.updatetime_stg DESC, pcx_fopliabilityschcovitemcov.createtime_stg DESC)=1 ) as pcx_fopliabilityschcovitemcov
                                                                      ON              pcx_fopcost.fopliabilityschcovitemcov_stg = pcx_fopliabilityschcovitemcov.fixedid_stg
                                                                      AND             pcx_fopliabilityschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopliabilityschcovitemcov.branchid_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopfarmownersschcovitemcov.patterncode_stg,
                                                                                                                      pcx_fopfarmownersschcovitemcov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopfarmownersschcovitemcov.fopfarmownerslischedulecovitem_stg,
                                                                                                                      pcx_fopfarmownersschcovitemcov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopfarmownersschcovitemcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopfarmownersschcovitemcov.branchid_stg , pcx_fopfarmownersschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopfarmownersschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopfarmownersschcovitemcov.updatetime_stg DESC, pcx_fopfarmownersschcovitemcov.createtime_stg DESC)=1 ) as pcx_fopfarmownersschcovitemcov
                                                                      ON              pcx_fopcost.fopfarmownerslineschcovitemcov_stg = pcx_fopfarmownersschcovitemcov.fixedid_stg
                                                                      AND             pcx_fopfarmownersschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopfarmownersschcovitemcov.branchid_stg
                                                                      left join
                                                                                      (
                                                                                                      SELECT DISTINCT pcx_fopfarmownerslinecov.patterncode_stg,
                                                                                                                      pcx_fopfarmownerslinecov.fixedid_stg,
                                                                                                                      pc_policyperiod.policynumber_stg,
                                                                                                                      pcx_fopfarmownerslinecov.farmownersline_stg,
                                                                                                                      pcx_fopfarmownerslinecov.branchid_stg
                                                                                                      FROM            db_t_prod_stag.pcx_fopfarmownerslinecov 
                                                                                                      join            db_t_prod_stag.pc_policyperiod 
                                                                                                      ON              pcx_fopfarmownerslinecov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopfarmownerslinecov.branchid_stg , pcx_fopfarmownerslinecov.fixedid_stg ORDER BY coalesce(pcx_fopfarmownerslinecov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopfarmownerslinecov.updatetime_stg DESC, pcx_fopfarmownerslinecov.createtime_stg DESC)=1 ) as pcx_fopfarmownerslinecov
                                                                      ON              pcx_fopcost.fopfarmownerslinecov_stg = pcx_fopfarmownerslinecov.fixedid_stg
                                                                      AND             pcx_fopfarmownerslinecov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                      AND             pcx_fopcost.branchid_stg = pcx_fopfarmownerslinecov.branchid_stg ) expandedfarmcosttable
                                            ON        pcx_foptransaction.cost_stg = expandedfarmcosttable.id_stg
                                            left join db_t_prod_stag.pctl_chargepattern 
                                            ON        expandedfarmcosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                            left join db_t_prod_stag.pctl_fopcost 
                                            ON        expandedfarmcosttable.subtype_stg = pctl_fopcost.id_stg
                                            join      db_t_prod_stag.pc_policyperiod 
                                            ON        pcx_foptransaction.branchid_stg = pc_policyperiod.id_stg
                                            AND       expandedfarmcosttable.policynumber_stg=pc_policyperiod.policynumber_stg
                                            join      db_t_prod_stag.pc_job 
                                            ON        pc_policyperiod.jobid_stg = pc_job.id_stg
                                            left join db_t_prod_stag.pctl_job 
                                            ON        pc_job.subtype_stg = pctl_job.id_stg
                                            left join db_t_prod_stag.pc_policyline 
                                            ON        pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                            AND       pc_policyline.expirationdate_stg IS NULL
                                            left join db_t_prod_stag.pctl_foppolicytype 
                                            ON        pc_policyline.foppolicytype_stg = pctl_foppolicytype.id_stg
                                            left join db_t_prod_stag.pc_policy 
                                            ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                            left join db_t_prod_stag.pc_account 
                                            ON        pc_policy.accountid_stg = pc_account.id_stg
                                            join      db_t_prod_stag.pctl_policyperiodstatus 
                                            ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                            WHERE     pctl_chargepattern.name_stg = ''Premium''
                                            AND       pctl_policyperiodstatus.typecode_stg=''Bound''
                                            AND       ((
                                                                          pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                AND       pc_policyperiod.updatetime_stg <= (:end_dttm))
                                                      OR        (
                                                                          pcx_foptransaction.updatetime_stg > (:start_dttm)
                                                                AND       pcx_foptransaction.updatetime_stg <= (:end_dttm)))
                                                      /* EIM-49943 */
                                            GROUP BY  pc_policyperiod.publicid_stg ,
                                                      expandedfarmcosttable.coverable_or_policyline_covpattern,
                                                      pctl_policyperiodstatus.typecode_stg,
                                                      pc_job.jobnumber_stg,
                                                      pc_policyperiod.branchnumber_stg,
                                                      pcx_foptransaction.updatetime_stg,
                                                      /* EIM-49943 */
                                                      pc_policyperiod.editeffectivedate_stg,
                                                      pctl_job.typecode_stg )
                        SELECT          tgt_plcy_cvge.plcy_cvge_mtrc_strt_dttm AS tgt_plcy_cvge_mtrc_strt_dttm,
                                        xlat_src.src_agmt_id                   AS src_agmt_id,
                                        xlat_src.src_feat_id                   AS src_feat_id,
                                        xlat_src.src_agmt_feat_strt_dttm       AS src_agmt_feat_strt_dttm,
                                        xlat_src.src_agmt_feat_role_cd         AS src_agmt_feat_role_cd,
                                        xlat_src.src_insrnc_mtrc_type_cd       AS src_insrnc_mtrc_type_cd,
                                        xlat_src.src_plcy_cvge_mtrc_strt_dttm  AS src_plcy_cvge_mtrc_strt_dttm,
                                        xlat_src.src_plcy_cvge_mtrc_end_dttm   AS src_plcy_cvge_mtrc_end_dttm,
                                        xlat_src.src_tm_prd_cd                 AS src_tm_prd_cd,
                                        xlat_src.src_plcy_asset_cvge_amt       AS src_plcy_asset_cvge_amt,
                                        xlat_src.src_uom_cd                    AS src_uom_cd,
                                        xlat_src.src_cury_cd                   AS src_cury_cd,
                                        xlat_src.src_uom_type_cd               AS src_uom_type_cd,
                                        xlat_src.src_trans_strt_dttm           AS src_trans_strt_dttm,
                                        /*Source MD5*/
                                        cast(trim(to_char(xlat_src.src_plcy_cvge_mtrc_strt_dttm , ''yyyy-mm-dd''))
                                                        || trim(coalesce(xlat_src.src_plcy_asset_cvge_amt,0)) AS VARCHAR(1100)) AS src_md5,
                                        /*Target MD5*/
                                        cast(trim(to_char(tgt_plcy_cvge_mtrc_strt_dttm , ''yyyy-mm-dd''))
                                                        || trim(coalesce(tgt_plcy_cvge.plcy_cvge_amt,0)) AS VARCHAR(1100)) AS tgt_md5,
                                        /*Flag*/
                                        CASE
                                                        WHEN tgt_plcy_cvge.feat_id IS NULL
                                                        AND             tgt_plcy_cvge. agmt_id IS NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_strt_dttm IS NULL
                                                        AND             tgt_plcy_cvge.insrnc_mtrc_type_cd IS NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_role_cd IS NULL
                                                        AND             xlat_src.src_feat_id IS NOT NULL
                                                        AND             xlat_src.src_agmt_id IS NOT NULL
                                                        AND             xlat_src.src_agmt_feat_strt_dttm IS NOT NULL
                                                        AND             xlat_src.src_insrnc_mtrc_type_cd IS NOT NULL
                                                        AND             src_agmt_feat_role_cd IS NOT NULL THEN ''I''
                                                        WHEN tgt_plcy_cvge.feat_id IS NOT NULL
                                                        AND             tgt_plcy_cvge. agmt_id IS NOT NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_strt_dttm IS NOT NULL
                                                        AND             tgt_plcy_cvge.insrnc_mtrc_type_cd IS NOT NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_role_cd IS NOT NULL
                                                        AND             src_md5 <> tgt_md5 THEN ''U''
                                                        WHEN tgt_plcy_cvge.feat_id IS NOT NULL
                                                        AND             tgt_plcy_cvge. agmt_id IS NOT NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_strt_dttm IS NOT NULL
                                                        AND             tgt_plcy_cvge.insrnc_mtrc_type_cd IS NOT NULL
                                                        AND             tgt_plcy_cvge.agmt_feat_role_cd IS NOT NULL
                                                        AND             src_md5 =tgt_md5 THEN ''R''
                                        END AS ins_upd_flag
                        FROM            (
                                                        /* Source query*/
                                                        SELECT          publicid,
                                                                        agmt_type,
                                                                        CASE
                                                                                        WHEN fe.feat_id IS NULL THEN ''9999''
                                                                                        ELSE fe.feat_id
                                                                        END                                                                                                  AS src_feat_id,
                                                                        cast(''1900-01-01'' AS DATE )                                                                          AS src_agmt_feat_strt_dttm,
                                                                        lkp_agmt_ppv.agmt_id                                                                                 AS src_agmt_id,
                                                                        coalesce(xlat_insrnc.tgt_idntftn_val,''UNK'')                                                          AS src_insrnc_mtrc_type_cd,
                                                                        coalesce(busn_dt,to_timestamp_ntz(''1900-01-01 00:00:00.000001'', ''YYYY-MM-DDBHH:MI:SS.FF6'' )) AS src_plcy_cvge_mtrc_strt_dttm,
                                                                        to_timestamp_ntz(''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'' )                   AS src_plcy_cvge_mtrc_end_dttm,
                                                                        cast(''UNK'' AS               VARCHAR(10))                                                                           AS src_tm_prd_cd,
                                                                        cast(plcy_asset_cvge_amt AS DECIMAL(18,4))                                                                         AS src_plcy_asset_cvge_amt,
                                                                        trans_dt                                                                                                           AS src_trans_strt_dttm ,
                                                                        cast(''UNK'' AS VARCHAR(10))                                                                                         AS src_uom_cd,
                                                                        cast(''UNK'' AS VARCHAR(10))                                                                                         AS src_cury_cd,
                                                                        cast(''UNK'' AS VARCHAR(10))                                                                                         AS src_uom_type_cd,
                                                                        cast(''UNK'' AS VARCHAR(10))                                                                                         AS src_agmt_feat_role_cd
                                                        FROM            (
                                                                                        SELECT DISTINCT rtrim(ltrim(cov_type_cd )) AS cov_type_cd ,
                                                                                                        publicid ,
                                                                                                        termnumber ,
                                                                                                        agmt_type ,
                                                                                                        insrnc_mtrc_type_cd ,
                                                                                                        plcy_asset_cvge_amt,
                                                                                                        busn_dt  AS busn_dt,
                                                                                                        trans_dt AS trans_dt
                                                                                        FROM            (
                                                                                                                 /* Added the select statemnt to ignore group by clause */
                                                                                                                 SELECT   cov_type_cd ,
                                                                                                                          publicid ,
                                                                                                                          termnumber ,
                                                                                                                          agmt_type ,
                                                                                                                          insrnc_mtrc_type_cd ,
                                                                                                                          SUM(amount) AS plcy_asset_cvge_amt,
                                                                                                                          busn_dt ,
                                                                                                                          trans_dt
                                                                                                                 FROM
                                                                                                                          /* ENd of select statament*/
                                                                                                                          (
                                                                                                                                 SELECT cov_type_cd,
                                                                                                                                        publicid,
                                                                                                                                        cast(NULL AS                 VARCHAR(64)) AS termnumber,
                                                                                                                                        cast(''PPV'' AS                VARCHAR(64)) as agmt_type,
                                                                                                                                        cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(64)) AS insrnc_mtrc_type_cd,
                                                                                                                                        pc_plcy_writtn_prem_x1.amount,
                                                                                                                                        editeffectivedate AS busn_dt,
                                                                                                                                        updatetime        AS trans_dt,
                                                                                                                                        /*added DB_T_CORE_DM_PROD.policy status*/
                                                                                                                                        status AS policyperiodstatus
                                                                                                                                 FROM   (
                                                                                                                                               SELECT *
                                                                                                                                               FROM   non_poltrm_temp1
                                                                                                                                               UNION ALL
                                                                                                                                               SELECT *
                                                                                                                                               FROM   non_poltrm_temp2
                                                                                                                                               UNION ALL
                                                                                                                                               SELECT *
                                                                                                                                               FROM   non_poltrm_temp3 ) pc_plcy_writtn_prem_x1
                                                                                                                                        /*closed the select statemt*/
                                                                                                                                 WHERE  policyperiodstatus=''Bound'' ) AS pc_plcy_writtn_prem_x
                                                                                                                          /* renamed select statement*/
                                                                                                                 GROUP BY cov_type_cd,
                                                                                                                          publicid,
                                                                                                                          termnumber ,
                                                                                                                          agmt_type ,
                                                                                                                          insrnc_mtrc_type_cd,
                                                                                                                          busn_dt,
                                                                                                                          trans_dt )a qualify row_number() over( PARTITION BY cov_type_cd, publicid,termnumber,agmt_type,insrnc_mtrc_type_cd ORDER BY trans_dt, busn_dt DESC) = 1
                                                                                        UNION ALL
                                                                                        /**** PMOP-54876 Added as part of Umbrella******/
                                                                                        SELECT DISTINCT rtrim(ltrim(cov_type_cd )) AS cov_type_cd ,
                                                                                                        publicid ,
                                                                                                        termnumber ,
                                                                                                        agmt_type ,
                                                                                                        insrnc_mtrc_type_cd ,
                                                                                                        plcy_asset_cvge_amt,
                                                                                                        busn_dt  AS busn_dt,
                                                                                                        trans_dt AS trans_dt
                                                                                        FROM            (
                                                                                                                 /* Added the select statemnt to ignore group by clause */
                                                                                                                 SELECT   cov_type_cd ,
                                                                                                                          publicid ,
                                                                                                                          termnumber ,
                                                                                                                          agmt_type ,
                                                                                                                          insrnc_mtrc_type_cd ,
                                                                                                                          SUM(amount) AS plcy_asset_cvge_amt,
                                                                                                                          busn_dt ,
                                                                                                                          trans_dt
                                                                                                                 FROM
                                                                                                                          /* ENd of select statament*/
                                                                                                                          (
                                                                                                                                 SELECT cov_type_cd,
                                                                                                                                        publicid,
                                                                                                                                        cast(NULL AS                 VARCHAR(64)) AS termnumber,
                                                                                                                                        cast(''PPV'' AS                VARCHAR(64)) as agmt_type,
                                                                                                                                        cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(64)) AS insrnc_mtrc_type_cd,
                                                                                                                                        pc_plcy_writtn_prem_x1.amount,
                                                                                                                                        editeffectivedate AS busn_dt,
                                                                                                                                        updatetime        AS trans_dt,
                                                                                                                                        /*added DB_T_CORE_DM_PROD.policy status*/
                                                                                                                                        status AS policyperiodstatus
                                                                                                                                 FROM   (
                                                                                                                                               SELECT *
                                                                                                                                               FROM   non_poltrm_temp4
                                                                                                                                               UNION ALL
                                                                                                                                               SELECT *
                                                                                                                                               FROM   non_poltrm_temp5 ) pc_plcy_writtn_prem_x1
                                                                                                                                        /*closed the select statemt*/
                                                                                                                                 WHERE  policyperiodstatus=''Bound'' ) AS pc_plcy_writtn_prem_x
                                                                                                                          /* renamed select statement*/
                                                                                                                 GROUP BY cov_type_cd,
                                                                                                                          publicid,
                                                                                                                          termnumber ,
                                                                                                                          agmt_type ,
                                                                                                                          insrnc_mtrc_type_cd,
                                                                                                                          busn_dt,
                                                                                                                          trans_dt )a qualify row_number() over( PARTITION BY cov_type_cd, publicid,termnumber,agmt_type,insrnc_mtrc_type_cd ORDER BY trans_dt DESC, busn_dt DESC) = 1 ) src
                                                                        /*LKP_AGMT_PPV*/
                                                        left outer join
                                                                        (
                                                                               SELECT agmt.agmt_id       AS agmt_id,
                                                                                      agmt.host_agmt_num AS host_agmt_num,
                                                                                      agmt.nk_src_key    AS nk_src_key,
                                                                                      agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                      agmt.edw_end_dttm
                                                                               FROM   db_t_prod_core.agmt as agmt
                                                                               WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31''
                                                                               AND    agmt_type_cd=''PPV'' ) lkp_agmt_ppv
                                                        ON              lkp_agmt_ppv.nk_src_key=src.publicid
                                                        AND             lkp_agmt_ppv.agmt_type_cd=agmt_type
                                                                        /*LKP_FEAT*/
                                                        left outer join
                                                                        (
                                                                               SELECT feat_id ,
                                                                                      nk_src_key
                                                                               FROM   db_t_prod_core.feat as feat
                                                                               WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'' ) fe
                                                        ON              fe.nk_src_key=src.cov_type_cd
                                                        left outer join
                                                                        (
                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat  as teradata_etl_ref_xlat
                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                                                                               AND    teradata_etl_ref_xlat.src_idntftn_nm= ''DERIVED''
                                                                               AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_insrnc
                                                        ON              xlat_insrnc.src_idntftn_val=src.insrnc_mtrc_type_cd )xlat_src
                                        /* Target Lookup PLCY_CVGE_MTRC*/
                        left outer join
                                        (
                                               SELECT agmt_id                  AS agmt_id,
                                                      feat_id                  AS feat_id,
                                                      agmt_feat_strt_dttm      AS agmt_feat_strt_dttm,
                                                      agmt_feat_role_cd        AS agmt_feat_role_cd,
                                                      insrnc_mtrc_type_cd      AS insrnc_mtrc_type_cd,
                                                      plcy_cvge_mtrc_strt_dttm AS plcy_cvge_mtrc_strt_dttm,
                                                      plcy_cvge_amt            AS plcy_cvge_amt,
                                                      edw_strt_dttm            AS edw_strt_dttm
                                               FROM   db_t_prod_core.plcy_cvge_mtrc
                                               WHERE  insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                                                  ''GLTRANPREM'',
                                                                                  ''GLUNERNPREM'')
                                               AND    cast(edw_end_dttm AS DATE) = cast(''9999-12-31'' AS DATE) ) tgt_plcy_cvge
                        ON              xlat_src.src_agmt_id=tgt_plcy_cvge.agmt_id
                        AND             xlat_src.src_agmt_feat_strt_dttm=tgt_plcy_cvge.agmt_feat_strt_dttm
                        AND             cast(xlat_src.src_feat_id AS DECIMAL(19,0))=cast(tgt_plcy_cvge.feat_id AS DECIMAL(19,0))
                        AND             xlat_src.src_insrnc_mtrc_type_cd=tgt_plcy_cvge.insrnc_mtrc_type_cd
                        AND             xlat_src.src_agmt_feat_role_cd=tgt_plcy_cvge.agmt_feat_role_cd
                        WHERE           ins_upd_flag IN (''I'',
                                                         ''U'') ) src ) );
  -- Component exp_pass_frm_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_source AS
  (
         SELECT sq_gw_premium_trans.tgt_plcy_cvge_mtrc_strt_dttm                       AS tgt_plcy_cvge_mtrc_strt_dttm,
                sq_gw_premium_trans.src_agmt_id                                        AS src_agmt_id,
                sq_gw_premium_trans.src_feat_id                                        AS src_feat_id,
                sq_gw_premium_trans.src_agmt_feat_strt_dttm                            AS src_agmt_feat_strt_dttm,
                sq_gw_premium_trans.src_agmt_feat_role_cd                              AS src_agmt_feat_role_cd,
                sq_gw_premium_trans.src_insrnc_mtrc_type_cd                            AS src_insrnc_mtrc_type_cd,
                sq_gw_premium_trans.src_plcy_cvge_mtrc_strt_dttm                       AS src_plcy_cvge_mtrc_strt_dttm,
                sq_gw_premium_trans.src_plcy_cvge_mtrc_end_dttm                        AS src_plcy_cvge_mtrc_end_dttm,
                sq_gw_premium_trans.src_tm_prd_cd                                      AS src_tm_prd_cd,
                sq_gw_premium_trans.src_plcy_asset_cvge_amt                            AS src_plcy_asset_cvge_amt,
                sq_gw_premium_trans.src_uom_cd                                         AS src_uom_cd,
                sq_gw_premium_trans.src_cury_cd                                        AS src_cury_cd,
                sq_gw_premium_trans.src_uom_type_cd                                    AS src_uom_type_cd,
                sq_gw_premium_trans.src_trans_strt_dttm                                AS src_trans_strt_dttm,
                sq_gw_premium_trans.ins_upd_flag                                       AS ins_upd_flag,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                :prcs_id                                                               AS out_prcs_id,
                sq_gw_premium_trans.source_record_id
         FROM   sq_gw_premium_trans );
  -- Component rtr_insupd_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_insupd_insert as
  SELECT exp_pass_frm_source.tgt_plcy_cvge_mtrc_strt_dttm AS tgt_plcy_cvge_mtrc_strt_dttm,
         exp_pass_frm_source.src_agmt_id                  AS src_agmt_id,
         exp_pass_frm_source.src_feat_id                  AS src_feat_id,
         exp_pass_frm_source.src_agmt_feat_strt_dttm      AS src_agmt_feat_strt_dttm,
         exp_pass_frm_source.src_agmt_feat_role_cd        AS src_agmt_feat_role_cd,
         exp_pass_frm_source.src_insrnc_mtrc_type_cd      AS src_insrnc_mtrc_type_cd,
         exp_pass_frm_source.src_plcy_cvge_mtrc_strt_dttm AS src_plcy_cvge_mtrc_strt_dttm,
         exp_pass_frm_source.src_plcy_cvge_mtrc_end_dttm  AS src_plcy_cvge_mtrc_end_dttm,
         exp_pass_frm_source.src_tm_prd_cd                AS src_tm_prd_cd,
         exp_pass_frm_source.src_plcy_asset_cvge_amt      AS src_plcy_asset_cvge_amt,
         exp_pass_frm_source.src_uom_cd                   AS src_uom_cd,
         exp_pass_frm_source.src_cury_cd                  AS src_cury_cd,
         exp_pass_frm_source.src_uom_type_cd              AS src_uom_type_cd,
         exp_pass_frm_source.src_trans_strt_dttm          AS src_trans_strt_dttm,
         exp_pass_frm_source.ins_upd_flag                 AS ins_upd_flag,
         exp_pass_frm_source.out_edw_end_dttm             AS out_edw_end_dttm,
         exp_pass_frm_source.out_prcs_id                  AS out_prcs_id,
         exp_pass_frm_source.source_record_id
  FROM   exp_pass_frm_source
  WHERE  (
                exp_pass_frm_source.ins_upd_flag = ''I''
         OR     (
                       exp_pass_frm_source.ins_upd_flag = ''U'' ) )
  AND    exp_pass_frm_source.src_feat_id > 0
  AND    exp_pass_frm_source.src_agmt_id > 0;
  
  -- Component upd_insupd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insupd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insupd_insert.src_agmt_id                  AS src_agmt_id1,
                rtr_insupd_insert.src_feat_id                  AS src_feat_id1,
                rtr_insupd_insert.src_agmt_feat_strt_dttm      AS src_agmt_feat_strt_dttm1,
                rtr_insupd_insert.src_agmt_feat_role_cd        AS src_agmt_feat_role_cd1,
                rtr_insupd_insert.src_insrnc_mtrc_type_cd      AS src_insrnc_mtrc_type_cd1,
                rtr_insupd_insert.src_plcy_cvge_mtrc_strt_dttm AS src_plcy_cvge_mtrc_strt_dttm1,
                rtr_insupd_insert.src_plcy_cvge_mtrc_end_dttm  AS src_plcy_cvge_mtrc_end_dttm1,
                rtr_insupd_insert.src_tm_prd_cd                AS src_tm_prd_cd1,
                rtr_insupd_insert.src_plcy_asset_cvge_amt      AS src_plcy_asset_cvge_amt1,
                rtr_insupd_insert.src_uom_cd                   AS src_uom_cd1,
                rtr_insupd_insert.src_cury_cd                  AS src_cury_cd1,
                rtr_insupd_insert.src_uom_type_cd              AS src_uom_type_cd1,
                rtr_insupd_insert.src_trans_strt_dttm          AS src_trans_strt_dttm1,
                rtr_insupd_insert.out_edw_end_dttm             AS out_edw_end_dttm1,
                rtr_insupd_insert.out_prcs_id                  AS out_prcs_id1,
                rtr_insupd_insert.source_record_id
         FROM   rtr_insupd_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_insupd.src_agmt_id1                  AS src_agmt_id1,
                upd_insupd.src_feat_id1                  AS src_feat_id1,
                upd_insupd.src_agmt_feat_strt_dttm1      AS src_agmt_feat_strt_dttm1,
                upd_insupd.src_agmt_feat_role_cd1        AS src_agmt_feat_role_cd1,
                upd_insupd.src_insrnc_mtrc_type_cd1      AS src_insrnc_mtrc_type_cd1,
                upd_insupd.src_plcy_cvge_mtrc_strt_dttm1 AS src_plcy_cvge_mtrc_strt_dttm1,
                upd_insupd.src_plcy_cvge_mtrc_end_dttm1  AS src_plcy_cvge_mtrc_end_dttm1,
                upd_insupd.src_tm_prd_cd1                AS src_tm_prd_cd1,
                upd_insupd.src_plcy_asset_cvge_amt1      AS src_plcy_asset_cvge_amt1,
                upd_insupd.src_uom_cd1                   AS src_uom_cd1,
                upd_insupd.src_cury_cd1                  AS src_cury_cd1,
                upd_insupd.src_uom_type_cd1              AS src_uom_type_cd1,
                upd_insupd.src_trans_strt_dttm1          AS src_trans_strt_dttm1,
                upd_insupd.out_edw_end_dttm1             AS out_edw_end_dttm1,
                upd_insupd.out_prcs_id1                  AS out_prcs_id1,
                current_timestamp                        AS edw_strt_dttm ,
                upd_insupd.source_record_id
         FROM   upd_insupd );
  -- Component PLCY_CVGE_MTRC, Type TARGET
  INSERT INTO db_t_prod_core.plcy_cvge_mtrc
              (
                          agmt_id,
                          feat_id,
                          agmt_feat_strt_dttm,
                          agmt_feat_role_cd,
                          insrnc_mtrc_type_cd,
                          plcy_cvge_mtrc_strt_dttm,
                          plcy_cvge_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_cvge_amt,
                          uom_cd,
                          cury_cd,
                          uom_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins.src_agmt_id1                  AS agmt_id,
         exp_pass_to_tgt_ins.src_feat_id1                  AS feat_id,
         exp_pass_to_tgt_ins.src_agmt_feat_strt_dttm1      AS agmt_feat_strt_dttm,
         exp_pass_to_tgt_ins.src_agmt_feat_role_cd1        AS agmt_feat_role_cd,
         exp_pass_to_tgt_ins.src_insrnc_mtrc_type_cd1      AS insrnc_mtrc_type_cd,
         exp_pass_to_tgt_ins.src_plcy_cvge_mtrc_strt_dttm1 AS plcy_cvge_mtrc_strt_dttm,
         exp_pass_to_tgt_ins.src_plcy_cvge_mtrc_end_dttm1  AS plcy_cvge_mtrc_end_dttm,
         exp_pass_to_tgt_ins.src_tm_prd_cd1                AS tm_prd_cd,
         exp_pass_to_tgt_ins.src_plcy_asset_cvge_amt1      AS plcy_cvge_amt,
         exp_pass_to_tgt_ins.src_uom_cd1                   AS uom_cd,
         exp_pass_to_tgt_ins.src_cury_cd1                  AS cury_cd,
         exp_pass_to_tgt_ins.src_uom_type_cd1              AS uom_type_cd,
         exp_pass_to_tgt_ins.out_prcs_id1                  AS prcs_id,
         exp_pass_to_tgt_ins.edw_strt_dttm                 AS edw_strt_dttm,
         exp_pass_to_tgt_ins.out_edw_end_dttm1             AS edw_end_dttm,
         exp_pass_to_tgt_ins.src_trans_strt_dttm1          AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_GW_PREMIUM_TRANS1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_premium_trans1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS cov_type_cd,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT typecode_stg
                                         FROM   db_t_prod_stag.pctl_policyperiodstatus
                                         WHERE  1=2
                                                /* Changed the sq as part of the eim-23128 code */
                                  ) src ) );
  -- Component exp_default, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default AS
  (
         SELECT 0                                                                      AS agmt_id,
                0                                                                      AS feat_id,
                current_timestamp                                                      AS agmt_feat_strt_dttm,
                ''N''                                                                    AS agmt_feat_role_cd,
                sq_gw_premium_trans1.cov_type_cd                                       AS insrnc_mtrc_type_cd,
                current_timestamp                                                      AS plcy_cvge_mtrc_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS plcy_cvge_mtrc_end_dttm,
                NULL                                                                   AS plcy_cvge_amt,
                :prcs_id                                                               AS prcs_id,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                current_timestamp                                                      AS trans_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                sq_gw_premium_trans1.source_record_id
         FROM   sq_gw_premium_trans1 );
  -- Component PLCY_CVGE_MTRC1, Type TARGET
  INSERT INTO db_t_prod_core.plcy_cvge_mtrc
              (
                          agmt_id,
                          feat_id,
                          agmt_feat_strt_dttm,
                          agmt_feat_role_cd,
                          insrnc_mtrc_type_cd,
                          plcy_cvge_mtrc_strt_dttm,
                          plcy_cvge_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_cvge_amt,
                          uom_cd,
                          uom_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_default.agmt_id                  AS agmt_id,
         exp_default.feat_id                  AS feat_id,
         exp_default.agmt_feat_strt_dttm      AS agmt_feat_strt_dttm,
         exp_default.agmt_feat_role_cd        AS agmt_feat_role_cd,
         exp_default.insrnc_mtrc_type_cd      AS insrnc_mtrc_type_cd,
         exp_default.plcy_cvge_mtrc_strt_dttm AS plcy_cvge_mtrc_strt_dttm,
         exp_default.plcy_cvge_mtrc_end_dttm  AS plcy_cvge_mtrc_end_dttm,
         exp_default.agmt_feat_role_cd        AS tm_prd_cd,
         exp_default.plcy_cvge_amt            AS plcy_cvge_amt,
         exp_default.agmt_feat_role_cd        AS uom_cd,
         exp_default.insrnc_mtrc_type_cd      AS uom_type_cd,
         exp_default.prcs_id                  AS prcs_id,
         exp_default.edw_strt_dttm            AS edw_strt_dttm,
         exp_default.edw_end_dttm             AS edw_end_dttm,
         exp_default.trans_strt_dttm          AS trans_strt_dttm,
         exp_default.trans_end_dttm           AS trans_end_dttm
  FROM   exp_default;
  
  -- PIPELINE END FOR 2
  -- Component PLCY_CVGE_MTRC1, Type Post SQL
  UPDATE db_t_prod_core.plcy_cvge_mtrc
    SET    trans_end_dttm = a.lead,
         edw_end_dttm = a.lead1
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         feat_id,
                                         agmt_feat_strt_dttm,
                                         agmt_feat_role_cd,
                                         insrnc_mtrc_type_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id,feat_id,agmt_feat_strt_dttm,agmt_feat_role_cd,insrnc_mtrc_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' lead1,
                                         max(trans_strt_dttm) over (PARTITION BY agmt_id,feat_id,agmt_feat_strt_dttm,agmt_feat_role_cd,insrnc_mtrc_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' lead
                         FROM            db_t_prod_core.plcy_cvge_mtrc
                         WHERE           plcy_cvge_mtrc.insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                                                    ''GLTRANPREM'',
                                                                                    ''GLUNERNPREM'')
                         AND             --cast(edw_end_dttm AS DATE)=''9999-12-31'' 
                                        to_char(edw_end_dttm ,''yyyy-mm-dd'')=''9999-12-31''
  ) a

  WHERE  plcy_cvge_mtrc.agmt_id=a.agmt_id
  AND    plcy_cvge_mtrc.feat_id=a.feat_id
  AND    plcy_cvge_mtrc.agmt_feat_strt_dttm=a.agmt_feat_strt_dttm
  AND    plcy_cvge_mtrc.agmt_feat_role_cd=a.agmt_feat_role_cd
  AND    plcy_cvge_mtrc.insrnc_mtrc_type_cd=a.insrnc_mtrc_type_cd
  AND    plcy_cvge_mtrc.edw_strt_dttm=a.edw_strt_dttm
  AND    a.lead IS NOT NULL
  AND    plcy_cvge_mtrc.insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                    ''GLTRANPREM'',
                                                    ''GLUNERNPREM'');


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_agmt_asset_insupd'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_agmt_asset_insupd'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );

END;
';