-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_ASSET_CVGE_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  PRCS_ID STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
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

  -- PIPELINE START FOR 1
  -- Component SQ_pc_plcy_asset_cvge_mtrc_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_plcy_asset_cvge_mtrc_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS src_feat_id,
                $2  AS src_agmt_asset_feat_strt_dttm,
                $3  AS src_asset_cntrct_role_sbtype_cd,
                $4  AS src_prty_asset_id,
                $5  AS src_agmt_asset_strt_dttm,
                $6  AS src_agmt_id,
                $7  AS src_insrnc_mtrc_type_cd,
                $8  AS src_plcy_asset_cvge_mtrc_strt_dttm,
                $9  AS src_plcy_asset_cvge_mtrc_end_dttm,
                $10 AS src_tm_prd_cd,
                $11 AS src_plcy_asset_cvge_amt,
                $12 AS src_plcy_asset_cvge_cnt,
                $13 AS src_uom_cd,
                $14 AS src_cury_cd,
                $15 AS src_uom_type_cd,
                $16 AS src_nk_src_key,
                $17 AS src_trans_strt_dttm,
                $18 AS src_rnk,
                $19 AS edw_strt_dttm,
                $20 AS src_md5,
                $21 AS tgt_md5,
                $22 AS ins_upd_flag,
                $23 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH plcy_asset_cvge_mtrc_temp AS
                                  (
                                         SELECT *
                                         FROM   (
                                                          SELECT    pc_policyperiod.publicid_stg                         publicid,
                                                                    expandedcosttable.personalvehicleid                  AS fixedid,
                                                                    cast(''PRTY_ASSET_SBTYPE4'' AS  VARCHAR(50))           AS asset_sbtype_cd,
                                                                    cast(''PRTY_ASSET_CLASFCN3'' AS VARCHAR(100))          AS asset_clasfcn_cd,
                                                                    ''GWPC''                                               AS asset_src_cd,
                                                                    expandedcosttable.coverable_or_policyline_covpattern AS cov_type_cd,
                                                                    pc_policyperiod.updatetime_stg                       AS updatetime,
                                                                    pc_policyperiod.editeffectivedate_stg                AS editeffectivedate,
                                                                    cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(100))           AS insrnc_mtrc_type_cd,
                                                                    pc_patransaction.amount_stg                          AS amount,
                                                                    0                                                    AS cvge_count,
                                                                    pctl_policyperiodstatus.typecode_stg                 AS policyperiodstatus,
                                                                    pc_job.jobnumber_stg                                 AS jobnumber,
                                                                    pc_policyperiod.branchnumber_stg                     AS branchnumber,
                                                                    (:start_dttm)                                        AS start_dttm,
                                                                    (:end_dttm)                                          AS end_dttm,
                                                                    pctl_job.typecode_stg                                AS jobtype
                                                          FROM      db_t_prod_stag.pc_patransaction 
                                                          join
                                                                    (
                                                                              SELECT    pc_policyperiod.policynumber_stg,
                                                                                        pc_personalvehiclecov.personalvehicle_stg personalvehicleid,
                                                                                        CASE
                                                                                                  WHEN pc_pacost.personalvehiclecov_stg IS NOT NULL THEN pc_personalvehiclecov.patterncode_stg
                                                                                                  WHEN pc_pacost.personalautocov_alfa_stg IS NOT NULL THEN pacov_alfa.patterncode_stg
                                                                                        END AS coverable_or_policyline_covpattern,
                                                                                        pc_pacost.chargepattern_stg,
                                                                                        pc_pacost.subtype_stg,
                                                                                        pc_pacost.periltype_alfa_stg,
                                                                                        pc_pacost.id_stg
                                                                              FROM      db_t_prod_stag.pc_pacost 
                                                                              join      db_t_prod_stag.pc_policyperiod 
                                                                              ON        pc_pacost.branchid_stg=pc_policyperiod.id_stg
                                                                                        /*Add unit-level coverages for auto*/
                                                                              left join
                                                                                        (
                                                                                                        SELECT DISTINCT pc_personalvehiclecov.patterncode_stg,
                                                                                                                        pc_personalvehiclecov.fixedid_stg,
                                                                                                                        pc_policyperiod.policynumber_stg,
                                                                                                                        pc_personalvehiclecov.personalvehicle_stg,
                                                                                                                        pc_personalvehiclecov.branchid_stg
                                                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row                                                  */
                                                                                                        FROM            db_t_prod_stag.pc_personalvehiclecov ,
                                                                                                                        db_t_prod_stag.pc_policyperiod 
                                                                                                        WHERE           pc_personalvehiclecov.branchid_stg=pc_policyperiod.id_stg
                                                                                                        AND             (
                                                                                                                                        pc_personalvehiclecov.expirationdate_stg IS NULL
                                                                                                                        OR              pc_personalvehiclecov.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row */
                                                                                        ) AS pc_personalvehiclecov
                                                                              ON        pc_pacost.personalvehiclecov_stg = pc_personalvehiclecov.fixedid_stg
                                                                              AND       pc_personalvehiclecov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                              AND       pc_pacost.branchid_stg = pc_personalvehiclecov.branchid_stg
                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row           */
                                                                                        /*Add policy-level coverages for auto*/
                                                                              left join db_t_prod_stag.pc_personalautocov pacov_alfa
                                                                              ON        pc_pacost.personalautocov_alfa_stg = pacov_alfa.fixedid_stg
                                                                              AND       pacov_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                              AND       pacov_alfa.expirationdate_stg IS NULL
                                                                              left join db_t_prod_stag.pc_policyline paline_unit_alfa
                                                                              ON        pacov_alfa.paline_stg= paline_unit_alfa.id_stg ) expandedcosttable
                                                          ON        pc_patransaction.pacost_stg = expandedcosttable.id_stg
                                                          left join db_t_prod_stag.pctl_chargepattern 
                                                          ON        expandedcosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                                          left join db_t_prod_stag.pctl_pacost 
                                                          ON        expandedcosttable.subtype_stg = pctl_pacost.id_stg
                                                          left join db_t_prod_stag.pctl_periltype_alfa autoperiltype
                                                          ON        expandedcosttable.periltype_alfa_stg= autoperiltype.id_stg
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
                                                          left join db_t_prod_stag.pc_policy 
                                                          ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                                          WHERE     pctl_chargepattern.name_stg = ''Premium''
                                                          AND       expandedcosttable.coverable_or_policyline_covpattern LIKE ''PA%''
                                                          AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                                          AND       pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                          UNION
                                                          SELECT    pc_policyperiod.publicid_stg                                                     policyperiodid,
                                                                    coalesce(expandedhocosttable.dwellingid, expandedhocosttable.scheduleditemid) AS prty_asset_id,
                                                                    ''PRTY_ASSET_SBTYPE5''                                                          AS asset_type,
                                                                    ''PRTY_ASSET_CLASFCN1''                                                         AS classification_code,
                                                                    ''GWPC''                                                                        AS asset_src_cd,
                                                                    expandedhocosttable.coverable_or_policyline_covpattern                        AS cov_type_cd,
                                                                    /* featid            */
                                                                    pc_policyperiod.updatetime_stg             AS updatetime,
                                                                    pc_policyperiod.editeffectivedate_stg      AS editeffectivedate,
                                                                    cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(100)) AS inscrn_mtrc_type_cd,
                                                                    SUM(pcx_hotransaction_hoe.amount_stg)      AS premium_trans_amt,
                                                                    0                                          AS cvge_cnt,
                                                                    pctl_policyperiodstatus.typecode_stg,
                                                                    pc_job.jobnumber_stg,
                                                                    pc_policyperiod.branchnumber_stg ,
                                                                    (:start_dttm)         AS start_dttm,
                                                                    (:end_dttm)           AS end_dttm,
                                                                    pctl_job.typecode_stg AS jobtype
                                                          FROM      db_t_prod_stag.pcx_hotransaction_hoe 
                                                          join
                                                                    (
                                                                              SELECT    pc_policyperiod.policynumber_stg,
                                                                                        pcx_dwellingcov_hoe.dwelling_stg                  AS dwellingid,
                                                                                        pcx_holineschcovitemcov_alfa.holineschcovitem_stg    scheduleditemid,
                                                                                        CASE
                                                                                                  WHEN pcx_homeownerscost_hoe.dwellingcov_stg IS NOT NULL THEN pcx_dwellingcov_hoe.patterncode_stg
                                                                                                  WHEN pcx_homeownerscost_hoe.scheditemcov_stg IS NOT NULL THEN pcx_holineschcovitemcov_alfa.patterncode_stg
                                                                                                  WHEN pcx_homeownerscost_hoe.homeownerslinecov_stg IS NOT NULL THEN pcx_homeownerslinecov_hoe.patterncode_stg
                                                                                        END AS coverable_or_policyline_covpattern,
                                                                                        pcx_homeownerscost_hoe.chargepattern_stg,
                                                                                        pcx_homeownerscost_hoe.subtype_stg,
                                                                                        pcx_homeownerscost_hoe.periltype_alfa_stg,
                                                                                        pcx_homeownerscost_hoe.id_stg,
                                                                                        pcx_homeownerscost_hoe.sectiontype_alfa_stg
                                                                              FROM      db_t_prod_stag.pcx_homeownerscost_hoe 
                                                                              join      db_t_prod_stag.pc_policyperiod 
                                                                              ON        pcx_homeownerscost_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                        /*  and  pc_policyperiod.PublicID=''sitpcnew:100184''            */
                                                                                        /*Add unit-level coverages for homeowners*/
                                                                              left join
                                                                                        (
                                                                                                        SELECT DISTINCT pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        pcx_dwellingcov_hoe.fixedid_stg,
                                                                                                                        pc_policyperiod.policynumber_stg,
                                                                                                                        pcx_dwellingcov_hoe.dwelling_stg,
                                                                                                                        pcx_dwellingcov_hoe.branchid_stg
                                                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row           */
                                                                                                        FROM            db_t_prod_stag.pcx_dwellingcov_hoe ,
                                                                                                                        db_t_prod_stag.pc_policyperiod 
                                                                                                        WHERE           pcx_dwellingcov_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                                        AND             (
                                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                        OR              pcx_dwellingcov_hoe.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row */
                                                                                        ) AS pcx_dwellingcov_hoe
                                                                              ON        pcx_homeownerscost_hoe.dwellingcov_stg = pcx_dwellingcov_hoe.fixedid_stg
                                                                              AND       pcx_dwellingcov_hoe.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                              AND       pcx_homeownerscost_hoe.branchid_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                        /* -EIM-43914 DB_T_PROD_CORE.PLCY_ASSET_CVGE_MTRC duplicate DB_T_PROD_COMN.premium row */
                                                                              left join
                                                                                        (
                                                                                                        SELECT DISTINCT pcx_holineschcovitemcov_alfa.patterncode_stg,
                                                                                                                        pcx_holineschcovitemcov_alfa.fixedid_stg,
                                                                                                                        pc_policyperiod.policynumber_stg,
                                                                                                                        pcx_holineschcovitemcov_alfa.holineschcovitem_stg
                                                                                                        FROM            db_t_prod_stag.pcx_holineschcovitemcov_alfa ,
                                                                                                                        db_t_prod_stag.pc_policyperiod 
                                                                                                        WHERE           pcx_holineschcovitemcov_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                        AND             pcx_holineschcovitemcov_alfa.expirationdate_stg IS NULL) AS pcx_holineschcovitemcov_alfa
                                                                              ON        pcx_homeownerscost_hoe.scheditemcov_stg = pcx_holineschcovitemcov_alfa.fixedid_stg
                                                                              AND       pcx_holineschcovitemcov_alfa.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                        /*Add policy-level coverages for homeowners*/
                                                                              left join db_t_prod_stag.pcx_homeownerslinecov_hoe 
                                                                              ON        pcx_homeownerscost_hoe.homeownerslinecov_stg = pcx_homeownerslinecov_hoe.id_stg
                                                                              AND       pcx_homeownerslinecov_hoe.expirationdate_stg IS NULL ) expandedhocosttable
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
                                                          left join db_t_prod_stag.pc_policy 
                                                          ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                                          join      db_t_prod_stag.pctl_policyperiodstatus 
                                                          ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                                          WHERE     pctl_chargepattern.name_stg = ''Premium''
                                                          AND       pc_policyperiod.updatetime_stg > (:start_dttm)
                                                          AND       pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                          GROUP BY  pc_policyperiod.publicid_stg ,
                                                                    coalesce(expandedhocosttable.dwellingid,expandedhocosttable.scheduleditemid),
                                                                    expandedhocosttable.coverable_or_policyline_covpattern,
                                                                    pctl_policyperiodstatus.typecode_stg,
                                                                    pc_job.jobnumber_stg,
                                                                    pc_policyperiod.branchnumber_stg,
                                                                    pc_policyperiod.updatetime_stg,
                                                                    pc_policyperiod.editeffectivedate_stg,
                                                                    pctl_job.typecode_stg
                                                          UNION
                                                          /* ------------------------------POLTRM------------------            */
                                                          SELECT    pc_policyperiod.publicid_stg                              policyperiodid,
                                                                    expandedcosttable.coverable_or_policyline_partyassetid AS prty_asset_id,
                                                                    max(
                                                                    CASE
                                                                              WHEN expandedcosttable.table_name_for_fixedid = ''pcx_bp7building'' THEN ''PRTY_ASSET_SBTYPE32''
                                                                              WHEN expandedcosttable.table_name_for_fixedid = ''pcx_bp7classification'' THEN ''PRTY_ASSET_SBTYPE13''
                                                                              ELSE NULL
                                                                    END) AS asset_type,
                                                                    max(
                                                                    CASE
                                                                              WHEN expandedcosttable.table_name_for_fixedid =''pcx_bp7classification'' THEN expandedcosttable.class_stg
                                                                              WHEN expandedcosttable.table_name_for_fixedid = ''pcx_bp7building'' THEN ''PRTY_ASSET_CLASFCN10''
                                                                              ELSE NULL
                                                                    END)                                                 AS classification_code,
                                                                    ''GWPC''                                               AS asset_src_cd,
                                                                    expandedcosttable.coverable_or_policyline_covpattern AS cov_type_cd,
                                                                    pc_policyperiod.updatetime_stg                       AS updatetime,
                                                                    pc_policyperiod.editeffectivedate_stg                AS editeffectivedate,
                                                                    cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(100))           AS inscrn_mtrc_type_cd,
                                                                    SUM(tr.amount_stg)                                   AS premium_trans_amt,
                                                                    0                                                    AS cvge_cnt,
                                                                    pctl_policyperiodstatus.typecode_stg,
                                                                    pc_job.jobnumber_stg,
                                                                    pc_policyperiod.branchnumber_stg,
                                                                    (:start_dttm)         AS start_dttm,
                                                                    (:end_dttm)           AS end_dttm,
                                                                    pctl_job.typecode_stg AS jobtype
                                                          FROM      db_t_prod_stag.pcx_bp7transaction tr
                                                          left join
                                                                    (
                                                                              SELECT
                                                                                        CASE
                                                                                                  WHEN cost.buildingcov_stg IS NOT NULL THEN ''pcx_bp7building''
                                                                                                  WHEN cost.locationcov_stg IS NOT NULL THEN ''pcx_bp7location''
                                                                                                  WHEN cost.classificationcov_stg IS NOT NULL THEN ''pcx_bp7classification''
                                                                                                  WHEN cost.linecoverage_stg IS NOT NULL THEN ''pc_policyline''
                                                                                                  WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7locschedcovitemcov''
                                                                                                  WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7bldgschedcovitemcov''
                                                                                                  WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7lineschedcovitemcov''
                                                                                        END AS table_name_for_fixedid,
                                                                                        CASE
                                                                                                  WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.building_stg
                                                                                                  WHEN cost.locationcov_stg IS NOT NULL THEN lcov.location_stg
                                                                                                  WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.classification_stg
                                                                                                  WHEN cost.linecoverage_stg IS NOT NULL THEN licov.bp7line_stg
                                                                                                  WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.locschedcovitem_stg
                                                                                                  WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.bldgschedcovitem_stg
                                                                                                  WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.lineschedcovitem_stg
                                                                                        END AS coverable_or_policyline_partyassetid,
                                                                                        CASE
                                                                                                  WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.patterncode_stg
                                                                                                  WHEN cost.locationcov_stg IS NOT NULL THEN lcov.patterncode_stg
                                                                                                  WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.patterncode_stg
                                                                                                  WHEN cost.linecoverage_stg IS NOT NULL THEN licov.patterncode_stg
                                                                                                  WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.patterncode_stg
                                                                                                  WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.patterncode_stg
                                                                                                  WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.patterncode_stg
                                                                                        END AS coverable_or_policyline_covpattern,
                                                                                        cost.id_stg,
                                                                                        cost.chargepattern_stg,
                                                                                        cp.typecode_stg AS class_stg
                                                                              FROM      db_t_prod_stag.pcx_bp7cost cost
                                                                                        /* Building DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7buildingcov bcov
                                                                              ON        cost.buildingcov_stg = bcov.id_stg
                                                                                        /* Location DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7locationcov lcov
                                                                              ON        cost.locationcov_stg = lcov.id_stg
                                                                                        /* Classification DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7classificationcov ccov
                                                                              ON        cost.classificationcov_stg = ccov.id_stg
                                                                              left join db_t_prod_stag.pcx_bp7classification c
                                                                              ON        ccov.classification_stg = c.id_stg
                                                                              left join db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                              ON        c.bp7classpropertytype_stg = cp.id_stg
                                                                                        /* Line DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7linecov licov
                                                                              ON        cost.linecoverage_stg = licov.id_stg
                                                                                        /* Location Scheduled Item DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7locschedcovitemcov lscov
                                                                              ON        cost.locschedcovitemcov_stg = lscov.id_stg
                                                                                        /* Building Scheduled Item DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7bldgschedcovitemcov bscov
                                                                              ON        cost.bldgschedcovitemcov_stg = bscov.id_stg
                                                                                        /* Line Scheduled Item DB_T_CORE_DM_PROD.Coverage            */
                                                                              left join db_t_prod_stag.pcx_bp7lineschedcovitemcov liscov
                                                                              ON        cost.lineschedcovitemcov_stg = liscov.id_stg ) expandedcosttable
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
                                                          AND       expandedcosttable.coverable_or_policyline_covpattern IS NOT NULL
                                                          GROUP BY  pc_policyperiod.publicid_stg,
                                                                    expandedcosttable.coverable_or_policyline_partyassetid,
                                                                    expandedcosttable.coverable_or_policyline_covpattern,
                                                                    pc_policyperiod.updatetime_stg,
                                                                    pc_policyperiod.editeffectivedate_stg,
                                                                    pctl_policyperiodstatus.typecode_stg,
                                                                    pc_job.jobnumber_stg,
                                                                    pc_policyperiod.branchnumber_stg,
                                                                    pctl_job.typecode_stg )pc_plcy_writtn_prem_x
                                         WHERE  policyperiodstatus=''Bound''
                                         AND    fixedid IS NOT NULL 
                                    ), 
                                  plcy_asset_cvge_mtrc_farm AS
                                  (
                                           SELECT   *
                                           FROM     (
                                                              /**EIM-48785 FARM Changes**/
                                                              SELECT    pc_policyperiod.publicid_stg                                                                                                                                                                                                        publicid,
                                                                        coalesce(expandedfarmcosttable.dwellingid,expandedfarmcosttable.outbuildingid,expandedfarmcosttable.livestockid,expandedfarmcosttable.machineryid, expandedfarmcosttable.feedandseedid,expandedfarmcosttable.dwellscheduleditemid,expandedfarmcosttable.farmscheduleditemid,expandedfarmcosttable.liabscheduleditemid ) AS fixedid,
                                                                        max(
                                                                        CASE
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopdwellingcov'' THEN ''PRTY_ASSET_SBTYPE37''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopdwellingschcovitemcov'' THEN ''PRTY_ASSET_SBTYPE38''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopoutbuildingcov'' THEN ''PRTY_ASSET_SBTYPE36''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_foplivestockcov'' THEN ''PRTY_ASSET_SBTYPE35''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopmachinerycov'' THEN ''PRTY_ASSET_SBTYPE34''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopfeedandseedcov'' THEN ''PRTY_ASSET_SBTYPE33''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopfarmownersschcovitemcov'' THEN ''PRTY_ASSET_SBTYPE41''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopliabilityschcovitemcov'' THEN ''PRTY_ASSET_SBTYPE42''
                                                                                  ELSE NULL
                                                                        END) AS asset_sbtype_cd,
                                                                        max(
                                                                        CASE
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopdwellingcov'' THEN ''PRTY_ASSET_CLASFCN15''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopdwellingschcovitemcov'' THEN ''PRTY_ASSET_CLASFCN16''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopoutbuildingcov'' THEN ''PRTY_ASSET_CLASFCN13''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_foplivestockcov'' THEN ''PRTY_ASSET_CLASFCN14''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopmachinerycov'' THEN ''PRTY_ASSET_CLASFCN12''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopfeedandseedcov'' THEN ''PRTY_ASSET_CLASFCN11''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid =''pcx_fopfarmownersschcovitemcov'' THEN ''PRTY_ASSET_CLASFCN19''
                                                                                  WHEN expandedfarmcosttable.table_name_for_fixedid = ''pcx_fopliabilityschcovitemcov'' THEN ''PRTY_ASSET_CLASFCN20''
                                                                                  ELSE NULL
                                                                        END)                                                     AS asset_clasfcn_cd,
                                                                        ''GWPC''                                                   AS asset_src_cd,
                                                                        expandedfarmcosttable.coverable_or_policyline_covpattern AS cov_type_cd,
                                                                        pcx_foptransaction.updatetime_stg                        AS updatetime,
                                                                        /* EIM-49942                                        */
                                                                        pc_policyperiod.editeffectivedate_stg      AS editeffectivedate,
                                                                        cast(''INSRNC_MTRC_TYPE16'' AS VARCHAR(100)) AS insrnc_mtrc_type_cd,
                                                                        SUM(pcx_foptransaction.amount_stg)         AS amount ,
                                                                        0                                          AS cvge_count,
                                                                        pctl_policyperiodstatus.typecode_stg       AS policyperiodstatus,
                                                                        pc_job.jobnumber_stg                       AS jobnumber,
                                                                        pc_policyperiod.branchnumber_stg           AS branchnumber,
                                                                        (:start_dttm)                              AS start_dttm,
                                                                        (:end_dttm)                                AS end_dttm,
                                                                        pctl_job.typecode_stg                      AS jobtype
                                                              FROM      db_t_prod_stag.pcx_foptransaction 
                                                              join
                                                                        (
                                                                                  SELECT    pc_policyperiod.policynumber_stg,
                                                                                            /***prty_asset_id****/
                                                                                            pcx_fopdwellingcov.dwelling_stg                                   AS dwellingid,
                                                                                            pcx_fopoutbuildingcov.outbuilding_stg                             AS outbuildingid,
                                                                                            pcx_foplivestockcov.livestock_stg                                 AS livestockid,
                                                                                            pcx_fopmachinerycov.machinery_stg                                 AS machineryid,
                                                                                            pcx_fopfeedandseedcov.feedandseed_stg                             AS feedandseedid,
                                                                                            pcx_fopdwellingschcovitemcov.fopdwellingschedulecovitem_stg       AS dwellscheduleditemid,
                                                                                            pcx_fopfarmownersschcovitemcov.fopfarmownerslischedulecovitem_stg AS farmscheduleditemid,
                                                                                            pcx_fopliabilityschcovitemcov.fopliabilityschedulecovitem_stg     AS liabscheduleditemid,
                                                                                            /***feat_id ****/
                                                                                            CASE
                                                                                                      WHEN pcx_fopcost.fopdwellingcov_stg IS NOT NULL THEN pcx_fopdwellingcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopdwellingschcovitemcov_stg IS NOT NULL THEN pcx_fopdwellingschcovitemcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopoutbuildingcov_stg IS NOT NULL THEN pcx_fopoutbuildingcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.foplivestockcov_stg IS NOT NULL THEN pcx_foplivestockcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopmachinerycov_stg IS NOT NULL THEN pcx_fopmachinerycov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopfeedandseedcov_stg IS NOT NULL THEN pcx_fopfeedandseedcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopfarmownerslineschcovitemcov_stg IS NOT NULL THEN pcx_fopfarmownersschcovitemcov.patterncode_stg
                                                                                                      WHEN pcx_fopcost.fopliabilityschcovitemcov_stg IS NOT NULL THEN pcx_fopliabilityschcovitemcov.patterncode_stg
                                                                                            END AS coverable_or_policyline_covpattern,
                                                                                            /** Fixed id *****/
                                                                                            CASE
                                                                                                      WHEN pcx_fopcost.fopdwellingcov_stg IS NOT NULL THEN ''pcx_fopdwellingcov''
                                                                                                      WHEN pcx_fopcost.fopdwellingschcovitemcov_stg IS NOT NULL THEN ''pcx_fopdwellingschcovitemcov''
                                                                                                      WHEN pcx_fopcost.fopoutbuildingcov_stg IS NOT NULL THEN ''pcx_fopoutbuildingcov''
                                                                                                      WHEN pcx_fopcost.foplivestockcov_stg IS NOT NULL THEN ''pcx_foplivestockcov''
                                                                                                      WHEN pcx_fopcost.fopmachinerycov_stg IS NOT NULL THEN ''pcx_fopmachinerycov''
                                                                                                      WHEN pcx_fopcost.fopfeedandseedcov_stg IS NOT NULL THEN ''pcx_fopfeedandseedcov''
                                                                                                      WHEN pcx_fopcost.fopfarmownerslineschcovitemcov_stg IS NOT NULL THEN ''pcx_fopfarmownersschcovitemcov''
                                                                                                      WHEN pcx_fopcost.fopliabilityschcovitemcov_stg IS NOT NULL THEN ''pcx_fopliabilityschcovitemcov''
                                                                                            END AS table_name_for_fixedid,
                                                                                            pcx_fopcost.chargepattern_stg,
                                                                                            pcx_fopcost.subtype_stg,
                                                                                            pcx_fopcost.id_stg
                                                                                  FROM      db_t_prod_stag.pcx_fopcost 
                                                                                  join      db_t_prod_stag.pc_policyperiod 
                                                                                  ON        pcx_fopcost.branchid_stg=pc_policyperiod.id_stg
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
                                                                                  ON        pcx_fopcost.fopdwellingcov_stg = pcx_fopdwellingcov.fixedid_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopdwellingcov.branchid_stg
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
                                                                                  ON        pcx_fopcost.fopoutbuildingcov_stg = pcx_fopoutbuildingcov.fixedid_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopoutbuildingcov.branchid_stg
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
                                                                                  ON        pcx_fopcost.foplivestockcov_stg = pcx_foplivestockcov.fixedid_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_foplivestockcov.branchid_stg
                                                                                  left join
                                                                                            (
                                                                                                            SELECT DISTINCT pcx_fopmachinerycov.patterncode_stg,
                                                                                                                            pcx_fopmachinerycov.fixedid_stg,
                                                                                                                            pc_policyperiod.policynumber_stg,
                                                                                                                            pcx_fopmachinerycov.machinery_stg,
                                                                                                                            pcx_fopmachinerycov.branchid_stg
                                                                                                            FROM            db_t_prod_stag.pcx_fopmachinerycov 
                                                                                                            join            db_t_prod_stag.pc_policyperiod 
                                                                                                            ON              pcx_fopmachinerycov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopmachinerycov.branchid_stg , pcx_fopmachinerycov.fixedid_stg ORDER BY coalesce(pcx_fopmachinerycov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopmachinerycov.updatetime_stg DESC,pcx_fopmachinerycov.createtime_stg DESC)=1 ) AS pcx_fopmachinerycov
                                                                                  ON        pcx_fopcost.fopmachinerycov_stg = pcx_fopmachinerycov.fixedid_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopmachinerycov.branchid_stg
                                                                                  left join
                                                                                            (
                                                                                                            SELECT DISTINCT pcx_fopfeedandseedcov.patterncode_stg,
                                                                                                                            pcx_fopfeedandseedcov.fixedid_stg,
                                                                                                                            pc_policyperiod.policynumber_stg,
                                                                                                                            pcx_fopfeedandseedcov.feedandseed_stg,
                                                                                                                            pcx_fopfeedandseedcov.branchid_stg
                                                                                                            FROM            db_t_prod_stag.pcx_fopfeedandseedcov 
                                                                                                            join            db_t_prod_stag.pc_policyperiod 
                                                                                                            ON              pcx_fopfeedandseedcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopfeedandseedcov.branchid_stg , pcx_fopfeedandseedcov.fixedid_stg ORDER BY coalesce(pcx_fopfeedandseedcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopfeedandseedcov.updatetime_stg DESC,pcx_fopfeedandseedcov.createtime_stg DESC)=1 ) AS pcx_fopfeedandseedcov
                                                                                  ON        pcx_fopcost.fopfeedandseedcov_stg = pcx_fopfeedandseedcov.fixedid_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopfeedandseedcov.branchid_stg
                                                                                  left join
                                                                                            (
                                                                                                            SELECT DISTINCT pcx_fopdwellingschcovitemcov.patterncode_stg,
                                                                                                                            pcx_fopdwellingschcovitemcov.fixedid_stg,
                                                                                                                            pc_policyperiod.policynumber_stg,
                                                                                                                            pcx_fopdwellingschcovitemcov.fopdwellingschedulecovitem_stg,
                                                                                                                            pcx_fopdwellingschcovitemcov.branchid_stg
                                                                                                            FROM            db_t_prod_stag.pcx_fopdwellingschcovitemcov 
                                                                                                            join            db_t_prod_stag.pc_policyperiod 
                                                                                                            ON              pcx_fopdwellingschcovitemcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopdwellingschcovitemcov.branchid_stg , pcx_fopdwellingschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopdwellingschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopdwellingschcovitemcov.updatetime_stg DESC,pcx_fopdwellingschcovitemcov.createtime_stg DESC)=1 ) AS pcx_fopdwellingschcovitemcov
                                                                                  ON        pcx_fopcost.fopdwellingschcovitemcov_stg = pcx_fopdwellingschcovitemcov.fixedid_stg
                                                                                  AND       pcx_fopdwellingschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopdwellingschcovitemcov.branchid_stg
                                                                                  left join
                                                                                            (
                                                                                                            SELECT DISTINCT pcx_fopliabilityschcovitemcov.patterncode_stg,
                                                                                                                            pcx_fopliabilityschcovitemcov.fixedid_stg,
                                                                                                                            pc_policyperiod.policynumber_stg,
                                                                                                                            pcx_fopliabilityschcovitemcov.fopliabilityschedulecovitem_stg,
                                                                                                                            pcx_fopliabilityschcovitemcov.branchid_stg
                                                                                                            FROM            db_t_prod_stag.pcx_fopliabilityschcovitemcov 
                                                                                                            join            db_t_prod_stag.pc_policyperiod 
                                                                                                            ON              db_t_prod_stag.pcx_fopliabilityschcovitemcov .branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopliabilityschcovitemcov.branchid_stg , pcx_fopliabilityschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopliabilityschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopliabilityschcovitemcov.updatetime_stg DESC, pcx_fopliabilityschcovitemcov.createtime_stg DESC)=1 ) AS pcx_fopliabilityschcovitemcov
                                                                                  ON        pcx_fopcost.fopliabilityschcovitemcov_stg = pcx_fopliabilityschcovitemcov.fixedid_stg
                                                                                  AND       pcx_fopliabilityschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopliabilityschcovitemcov.branchid_stg
                                                                                  left join
                                                                                            (
                                                                                                            SELECT DISTINCT pcx_fopfarmownersschcovitemcov.patterncode_stg,
                                                                                                                            pcx_fopfarmownersschcovitemcov.fixedid_stg,
                                                                                                                            pc_policyperiod.policynumber_stg,
                                                                                                                            pcx_fopfarmownersschcovitemcov.fopfarmownerslischedulecovitem_stg,
                                                                                                                            pcx_fopfarmownersschcovitemcov.branchid_stg
                                                                                                            FROM            db_t_prod_stag.pcx_fopfarmownersschcovitemcov 
                                                                                                            join            db_t_prod_stag.pc_policyperiod 
                                                                                                            ON              pcx_fopfarmownersschcovitemcov.branchid_stg=pc_policyperiod.id_stg qualify row_number() over(PARTITION BY pcx_fopfarmownersschcovitemcov.branchid_stg , pcx_fopfarmownersschcovitemcov.fixedid_stg ORDER BY coalesce(pcx_fopfarmownersschcovitemcov.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC,pcx_fopfarmownersschcovitemcov.updatetime_stg DESC, pcx_fopfarmownersschcovitemcov.createtime_stg DESC)=1 ) AS pcx_fopfarmownersschcovitemcov
                                                                                  ON        pcx_fopcost.fopfarmownerslineschcovitemcov_stg = pcx_fopfarmownersschcovitemcov.fixedid_stg
                                                                                  AND       pcx_fopfarmownersschcovitemcov.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                  AND       pcx_fopcost.branchid_stg = pcx_fopfarmownersschcovitemcov.branchid_stg ) expandedfarmcosttable
                                                              ON        pcx_foptransaction.cost_stg = expandedfarmcosttable.id_stg
                                                              left join db_t_prod_stag.pctl_chargepattern 
                                                              ON        expandedfarmcosttable.chargepattern_stg = pctl_chargepattern.id_stg
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
                                                              left join db_t_prod_stag.pc_policy 
                                                              ON        pc_policyperiod.policyid_stg = pc_policy.id_stg
                                                              join      db_t_prod_stag.pctl_policyperiodstatus 
                                                              ON        pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                                              WHERE     pctl_chargepattern.name_stg = ''Premium''
                                                              AND       ((
                                                                                            pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                  AND       pc_policyperiod.updatetime_stg <= (:end_dttm))
                                                                        OR        (
                                                                                            pcx_foptransaction.updatetime_stg > (:start_dttm)
                                                                                  AND       pcx_foptransaction.updatetime_stg <= (:end_dttm)))
                                                                        /* EIM-49942    */
                                                              GROUP BY  pc_policyperiod.publicid_stg ,
                                                                        coalesce(expandedfarmcosttable.dwellingid,expandedfarmcosttable.outbuildingid,expandedfarmcosttable.livestockid,expandedfarmcosttable.machineryid, expandedfarmcosttable.feedandseedid,expandedfarmcosttable.dwellscheduleditemid,expandedfarmcosttable.farmscheduleditemid,expandedfarmcosttable.liabscheduleditemid),
                                                                        expandedfarmcosttable.coverable_or_policyline_covpattern,
                                                                        pctl_policyperiodstatus.typecode_stg,
                                                                        pc_job.jobnumber_stg,
                                                                        pc_policyperiod.branchnumber_stg,
                                                                        pcx_foptransaction.updatetime_stg,
                                                                        /* EIM-49942    */
                                                                        pc_policyperiod.editeffectivedate_stg,
                                                                        pctl_job.typecode_stg )farm_temp
                                           WHERE    policyperiodstatus=''Bound''
                                           AND      fixedid IS NOT NULL qualify row_number() over( PARTITION BY publicid,fixedid,cov_type_cd,policyperiodstatus,jobnumber,branchnumber,jobtype ORDER BY updatetime DESC,editeffectivedate DESC) = 1
                                                    /*  --EIM-49942     */
                                  ), 
                                  plcy_src AS
                                  (
                                         SELECT *
                                         FROM   (
                                                       SELECT xlat_src.src_feat_id                        AS src_feat_id,
                                                              xlat_src.src_agmt_asset_feat_strt_dttm      AS src_agmt_asset_feat_strt_dttm,
                                                              xlat_src.src_asset_cntrct_role_sbtype_cd    AS src_asset_cntrct_role_sbtype_cd,
                                                              xlat_src.src_prty_asset_id                  AS src_prty_asset_id,
                                                              xlat_src.src_agmt_asset_strt_dttm           AS src_agmt_asset_strt_dttm,
                                                              xlat_src.src_agmt_id                        AS src_agmt_id,
                                                              xlat_src.src_insrnc_mtrc_type_cd            AS src_insrnc_mtrc_type_cd,
                                                              xlat_src.src_plcy_asset_cvge_mtrc_strt_dttm AS src_plcy_asset_cvge_mtrc_strt_dttm,
                                                              xlat_src.src_plcy_asset_cvge_mtrc_end_dttm  AS src_plcy_asset_cvge_mtrc_end_dttm,
                                                              xlat_src.src_tm_prd_cd                      AS src_tm_prd_cd,
                                                              xlat_src.src_plcy_asset_cvge_amt            AS src_plcy_asset_cvge_amt,
                                                              xlat_src.src_plcy_asset_cvge_cnt            AS src_plcy_asset_cvge_cnt,
                                                              xlat_src.src_uom_cd                         AS src_uom_cd,
                                                              xlat_src.src_cury_cd                        AS src_cury_cd,
                                                              xlat_src.src_uom_type_cd                    AS src_uom_type_cd,
                                                              xlat_src.src_nk_src_key                     AS src_nk_src_key,
                                                              xlat_src.src_trans_strt_dttm                AS src_trans_strt_dttm,
                                                              xlat_src.rnk                                AS src_rnk
                                                       FROM   (
                                                                              /*  Source query            */
                                                                              SELECT          publicid                 AS publicid,
                                                                                              agmt_type AS agmt_type,
                                                                                              CASE
                                                                                                              WHEN fe.feat_id IS NULL THEN ''9999''
                                                                                                              ELSE fe.feat_id
                                                                                              END                               AS src_feat_id,
                                                                                              cast(''1900-01-01'' AS DATE )       AS src_agmt_asset_feat_strt_dttm,
                                                                                              cast(''UNK'' AS        VARCHAR(10)) AS src_asset_cntrct_role_sbtype_cd,
                                                                                              cast(''1900-01-01'' AS DATE )       AS src_agmt_asset_strt_dttm,
                                                                                              CASE
                                                                                                              WHEN pa.prty_asset_id IS NULL THEN ''9999''
                                                                                                              ELSE pa.prty_asset_id
                                                                                              END AS src_prty_asset_id,
                                                                                              CASE
                                                                                                              WHEN agmt_type = ''POLTRM'' THEN lkp_agmt_poltrm.agmt_id
                                                                                                              WHEN agmt_type=''PPV'' THEN lkp_agmt_ppv.agmt_id
                                                                                              END                                                                                AS src_agmt_id,
                                                                                              coalesce(xlat_insrnc.tgt_idntftn_val,''UNK'')                                        AS src_insrnc_mtrc_type_cd,
                                                                                              earnings_as_of_dttm                                                                AS src_plcy_asset_cvge_mtrc_strt_dttm,
                                                                                              to_timestamp_ntz(''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'' ) AS src_plcy_asset_cvge_mtrc_end_dttm,
                                                                                              cast(''UNK'' AS    VARCHAR(10))                                                         AS src_tm_prd_cd,
                                                                                              cast(amount AS   DECIMAL(18,4))                                                       AS src_plcy_asset_cvge_amt,
                                                                                              cast(cvge_amt AS INTEGER)                                                             AS src_plcy_asset_cvge_cnt,
                                                                                              updatetime                                                                            AS src_trans_strt_dttm ,
                                                                                              rnk,
                                                                                              coalesce(xlat_asset.tgt_idntftn_val, ''UNK'')   AS src_asset_sbtype_cd,
                                                                                              coalesce(xlat_clasfcn. tgt_idntftn_val,''UNK'') AS src_asset_clasfcn_cd,
                                                                                              cast(''UNK'' AS VARCHAR(10))                    AS src_uom_cd,
                                                                                              cast(''UNK'' AS VARCHAR(10))                    AS src_cury_cd,
                                                                                              cast(''UNK'' AS VARCHAR(10))                    AS src_uom_type_cd,
                                                                                              cast(''UNK'' AS VARCHAR(10))                    AS src_nk_src_key
                                                                              FROM           (
                                                                                                       SELECT   publicid_t                  AS publicid,
                                                                                                                termnumber_t                AS termnumber,
                                                                                                                agmt_type_t                 AS agmt_type,
                                                                                                                fixedid_t                   AS fixedid,
                                                                                                                asset_sbtype_cd_t           AS asset_sbtype_cd,
                                                                                                                asset_clasfcn_cd_t          AS asset_clasfcn_cd,
                                                                                                                rtrim(ltrim(cov_type_cd_t)) AS cov_type_cd,
                                                                                                                earnings_as_of_dttm_t       AS earnings_as_of_dttm,
                                                                                                                amount,
                                                                                                                cvge_amt_t                                                                                                                                                 AS cvge_amt,
                                                                                                                insrnc_mtrc_type_cd_t                                                                                                                                      AS insrnc_mtrc_type_cd,
                                                                                                                updatetime_t                                                                                                                                               AS updatetime,
                                                                                                                rank() over(PARTITION BY publicid_t,cov_type_cd_t,fixedid_t,insrnc_mtrc_type_cd_t,asset_sbtype_cd_t,asset_clasfcn_cd_t,termnumber_t ORDER BY updatetime_t) AS rnk
                                                                                                       FROM     (
                                                                                                                                SELECT DISTINCT (
                                                                                                                                                CASE
                                                                                                                                                                WHEN publicid<>''POLTRM'' THEN (cast(publicid AS VARCHAR(64)))
                                                                                                                                                                ELSE (cast(jobnumber AS                        VARCHAR(64)))
                                                                                                                                                END) AS publicid_t, (
                                                                                                                                                CASE
                                                                                                                                                                WHEN publicid <> ''POLTRM'' THEN (cast(0 AS VARCHAR(64)))
                                                                                                                                                                ELSE (cast(branchnumber AS                VARCHAR(64)))
                                                                                                                                                END) AS termnumber_t, (
                                                                                                                                                CASE
                                                                                                                                                                WHEN publicid <> ''POLTRM'' THEN (cast(''PPV'' AS VARCHAR(64)))
                                                                                                                                                                ELSE (cast(publicid AS                        VARCHAR(64)))
                                                                                                                                                END)                              AS agmt_type_t,
                                                                                                                                                cast(fixedid AS VARCHAR(64))      AS fixedid_t,
                                                                                                                                                asset_sbtype_cd                   AS asset_sbtype_cd_t,
                                                                                                                                                asset_clasfcn_cd                  AS asset_clasfcn_cd_t,
                                                                                                                                                cast(cov_type_cd AS VARCHAR(100)) AS cov_type_cd_t,
                                                                                                                                                editeffectivedate                 AS earnings_as_of_dttm_t ,
                                                                                                                                                SUM(amount)                       AS amount,
                                                                                                                                                cvge_count                        AS cvge_amt_t,
                                                                                                                                                insrnc_mtrc_type_cd               AS insrnc_mtrc_type_cd_t,
                                                                                                                                                updatetime                        AS updatetime_t
                                                                                                                                FROM            (
                                                                                                                                                       SELECT *
                                                                                                                                                       FROM   plcy_asset_cvge_mtrc_temp
                                                                                                                                                       UNION ALL
                                                                                                                                                       SELECT *
                                                                                                                                                       FROM   plcy_asset_cvge_mtrc_farm
                                                                                                                                                              /* EIM-49942    */
                                                                                                                                                )b
                                                                                                                                GROUP BY        publicid_t,
                                                                                                                                                termnumber_t,
                                                                                                                                                agmt_type_t,
                                                                                                                                                fixedid_t,
                                                                                                                                                asset_sbtype_cd_t,
                                                                                                                                                asset_clasfcn_cd_t,
                                                                                                                                                cov_type_cd_t,
                                                                                                                                                earnings_as_of_dttm_t,
                                                                                                                                                cvge_amt_t,
                                                                                                                                                insrnc_mtrc_type_cd_t,
                                                                                                                                                updatetime_t ) x )src
                                                                                              /*  lkp_teradata_etl_ref_xlat_asset_subtype             */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                            teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                     FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                     WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
                                                                                                     AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                                     AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                                     AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) xlat_asset
                                                                              ON              src.asset_sbtype_cd=xlat_asset.src_idntftn_val
                                                                                              /* lkp_teradata_etl_ref_xlat_classfication            */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                            teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                     FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                     WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                                                                                                     AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                                                                                                                      ''GW'')
                                                                                                     AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_clasfcn
                                                                              ON              src.asset_clasfcn_cd=xlat_clasfcn. src_idntftn_val
                                                                                              /* lkp_teradata_etl_ref_xlat_insrnc_mtrc_ype            */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                            teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                     FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                     WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                                                                                                     AND    teradata_etl_ref_xlat.src_idntftn_nm= ''DERIVED''
                                                                                                     AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                                     AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_insrnc
                                                                              ON              xlat_insrnc.src_idntftn_val=src.insrnc_mtrc_type_cd
                                                                                              /* lkp_feat             */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT feat_id ,
                                                                                                            nk_src_key
                                                                                                     FROM   db_t_prod_core.feat AS feat
                                                                                                     WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'' ) fe
                                                                              ON              fe.nk_src_key=src.cov_type_cd
                                                                                              /* lkp_agmt_PPV            */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT agmt.agmt_id       AS agmt_id,
                                                                                                            agmt.host_agmt_num AS host_agmt_num,
                                                                                                            agmt.nk_src_key    AS nk_src_key,
                                                                                                            agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                            agmt.edw_end_dttm
                                                                                                     FROM   db_t_prod_core.agmt AS agmt
                                                                                                     WHERE  cast(edw_end_dttm AS DATE) =''9999-12-31''
                                                                                                     AND    agmt_type_cd=''PPV''
                                                                                                            /* QUALIFY   ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER    BY AGMT.EDW_END_DTTM desc) = 1            */
                                                                                              ) lkp_agmt_ppv
                                                                              ON              lkp_agmt_ppv.nk_src_key=src.publicid
                                                                              AND             lkp_agmt_ppv.agmt_type_cd=agmt_type
                                                                                              /* lkp_agmt_POLTRM            */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT agmt.agmt_id       AS agmt_id,
                                                                                                            agmt.host_agmt_num AS host_agmt_num,
                                                                                                            agmt.term_num      AS term_num,
                                                                                                            agmt.nk_src_key    AS nk_src_key,
                                                                                                            agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                            agmt.edw_end_dttm
                                                                                                     FROM   db_t_prod_core.agmt AS agmt
                                                                                                     WHERE  cast(edw_end_dttm AS DATE) =''9999-12-31''
                                                                                                     AND    agmt_type_cd=''POLTRM''
                                                                                                            /* QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.HOST_AGMT_NUM, AGMT.TERM_NUM, AGMT.AGMT_TYPE_CD  ORDER BY AGMT.EDW_END_DTTM desc) = 1            */
                                                                                              )lkp_agmt_poltrm
                                                                              ON              lkp_agmt_poltrm.host_agmt_num=src.publicid
                                                                              AND             lkp_agmt_poltrm.term_num=src.termnumber
                                                                              AND             lkp_agmt_poltrm.agmt_type_cd=agmt_type
                                                                                              /* Lkp_prty_asset            */
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT prty_asset.prty_asset_id         AS prty_asset_id,
                                                                                                            prty_asset.asset_host_id_val     AS asset_host_id_val,
                                                                                                            prty_asset.prty_asset_sbtype_cd  AS prty_asset_sbtype_cd,
                                                                                                            prty_asset.prty_asset_clasfcn_cd AS prty_asset_clasfcn_cd
                                                                                                     FROM   db_t_prod_core.prty_asset        AS prty_asset
                                                                                                     WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'' )pa
                                                                              ON              pa.asset_host_id_val=src.fixedid
                                                                              AND             pa.prty_asset_sbtype_cd=xlat_asset.tgt_idntftn_val
                                                                              AND             pa.prty_asset_clasfcn_cd=xlat_clasfcn.tgt_idntftn_val )xlat_src ) inr 
                                    )
                           SELECT src_feat_id,
                                  src_agmt_asset_feat_strt_dttm,
                                  src_asset_cntrct_role_sbtype_cd,
                                  src_prty_asset_id,
                                  src_agmt_asset_strt_dttm,
                                  src_agmt_id,
                                  src_insrnc_mtrc_type_cd,
                                  src_plcy_asset_cvge_mtrc_strt_dttm,
                                  src_plcy_asset_cvge_mtrc_end_dttm,
                                  src_tm_prd_cd,
                                  src_plcy_asset_cvge_amt,
                                  src_plcy_asset_cvge_cnt,
                                  src_uom_cd,
                                  src_cury_cd,
                                  src_uom_type_cd,
                                  src_nk_src_key,
                                  src_trans_strt_dttm,
                                  src_rnk,
                                  /*  Added for EIM-35770 */
                                  --cast(current_timestamp(0) AS timestamp) + cast( (src_rnk-1)*2 AS interval second) AS edw_strt_dttm,
                                    DATEADD(SECOND, (src_rnk - 1) * 2, current_timestamp(0)) AS edw_strt_dttm,
                                  /* Source MD5            */
                                  cast((trim(coalesce(src_uom_cd,0))
                                         || trim(coalesce(src_cury_cd,0))
                                         || trim(coalesce(src_uom_type_cd,0))
                                         || trim(coalesce(src_nk_src_key,0))
                                         || trim(coalesce(src_tm_prd_cd, 0))
                                         || trim(to_char(src_plcy_asset_cvge_mtrc_end_dttm ,''yyyy-mm-dd''))
                                         || trim(coalesce(src_plcy_asset_cvge_amt,0))
                                         || trim(coalesce(src_plcy_asset_cvge_cnt,0)) ) AS VARCHAR(1100)) AS src_md5,
                                  /* Target MD5            */
                                  cast((trim(coalesce(uom_cd,0))
                                         || trim(coalesce(cury_cd,0))
                                         || trim(coalesce(uom_type_cd,0))
                                         || trim(coalesce(nk_src_key,0))
                                         || trim(coalesce(tm_prd_cd, 0))
                                         || trim(to_char(plcy_asset_cvge_mtrc_end_dttm , ''yyyy-mm-dd''))
                                         || trim(coalesce(plcy_asset_cvge_amt,0))
                                         || trim(coalesce(plcy_asset_cvge_cnt,0)) ) AS VARCHAR(1100)) AS tgt_md5,
                                  /* Flag            */
                                  CASE
                                         WHEN feat_id IS NULL
                                         AND    prty_asset_id IS NULL
                                         AND    agmt_id IS NULL
                                         AND    plcy_asset_cvge_mtrc_strt_dttm IS NULL
                                         AND    insrnc_mtrc_type_cd IS NULL
                                         AND    src_feat_id IS NOT NULL
                                         AND    src_feat_id<>9999
                                         AND    src_prty_asset_id<>9999
                                         AND    src_agmt_id IS NOT NULL
                                         AND    src_plcy_asset_cvge_mtrc_strt_dttm IS NOT NULL
                                         AND    src_insrnc_mtrc_type_cd IS NOT NULL THEN ''I''
                                         WHEN feat_id IS NOT NULL
                                         AND    prty_asset_id IS NOT NULL
                                         AND    agmt_id IS NOT NULL
                                         AND    src_md5 <> tgt_md5 THEN ''U''
                                         WHEN feat_id IS NOT NULL
                                         AND    prty_asset_id IS NOT NULL
                                         AND    agmt_id IS NOT NULL
                                         AND    src_md5 =tgt_md5 THEN ''R''
                                  END AS ins_upd_flag
                           FROM   (
                                                  SELECT          *
                                                  FROM            plcy_src
                                                  left outer join
                                                                  (
                                                                           SELECT   plcy_asset_cvge_mtrc.agmt_asset_feat_strt_dttm      AS agmt_asset_feat_strt_dttm,
                                                                                    plcy_asset_cvge_mtrc.asset_cntrct_role_sbtype_cd    AS asset_cntrct_role_sbtype_cd,
                                                                                    plcy_asset_cvge_mtrc.agmt_asset_strt_dttm           AS agmt_asset_strt_dttm,
                                                                                    plcy_asset_cvge_mtrc.plcy_asset_cvge_mtrc_end_dttm  AS plcy_asset_cvge_mtrc_end_dttm,
                                                                                    plcy_asset_cvge_mtrc.tm_prd_cd                      AS tm_prd_cd,
                                                                                    plcy_asset_cvge_mtrc.plcy_asset_cvge_amt            AS plcy_asset_cvge_amt,
                                                                                    plcy_asset_cvge_mtrc.plcy_asset_cvge_cnt            AS plcy_asset_cvge_cnt,
                                                                                    plcy_asset_cvge_mtrc.uom_cd                         AS uom_cd,
                                                                                    plcy_asset_cvge_mtrc.cury_cd                        AS cury_cd,
                                                                                    plcy_asset_cvge_mtrc.uom_type_cd                    AS uom_type_cd,
                                                                                    plcy_asset_cvge_mtrc.nk_src_key                     AS nk_src_key,
                                                                                    plcy_asset_cvge_mtrc.edw_strt_dttm                  AS edw_strt_dttm,
                                                                                    plcy_asset_cvge_mtrc.edw_end_dttm                   AS edw_end_dttm,
                                                                                    plcy_asset_cvge_mtrc.trans_strt_dttm                AS trans_strt_dttm,
                                                                                    plcy_asset_cvge_mtrc.trans_end_dttm                 AS trans_end_dttm,
                                                                                    plcy_asset_cvge_mtrc.agmt_id                        AS agmt_id,
                                                                                    plcy_asset_cvge_mtrc.prty_asset_id                  AS prty_asset_id,
                                                                                    plcy_asset_cvge_mtrc.plcy_asset_cvge_mtrc_strt_dttm AS plcy_asset_cvge_mtrc_strt_dttm,
                                                                                    plcy_asset_cvge_mtrc.feat_id                        AS feat_id,
                                                                                    plcy_asset_cvge_mtrc.insrnc_mtrc_type_cd            AS insrnc_mtrc_type_cd
                                                                           FROM     db_t_prod_core.plcy_asset_cvge_mtrc                 AS plcy_asset_cvge_mtrc
                                                                           WHERE    (
                                                                                             prty_asset_id,agmt_id,feat_id) IN
                                                                                                                                (
                                                                                                                                SELECT DISTINCT cast(src_prty_asset_id AS DECIMAL(19,0)),
                                                                                                                                                cast(src_agmt_id AS       DECIMAL(19,0)),
                                                                                                                                                cast(src_feat_id AS       DECIMAL(19,0))
                                                                                                                                FROM            plcy_src)
                                                                           AND      insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                                                                                ''GLTRANPREM'',
                                                                                                                ''GLUNERNPREM'') qualify row_number() over(PARTITION BY agmt_id,feat_id,prty_asset_id,plcy_asset_cvge_mtrc_strt_dttm,insrnc_mtrc_type_cd ORDER BY edw_end_dttm DESC) = 1 )tgt_plcy_asset
                                                  ON              plcy_src.src_agmt_id=tgt_plcy_asset.agmt_id
                                                  AND             cast( plcy_src.src_prty_asset_id AS DECIMAL(19,0))=cast(tgt_plcy_asset.prty_asset_id AS DECIMAL(19,0))
                                                  AND             plcy_src.src_plcy_asset_cvge_mtrc_strt_dttm=tgt_plcy_asset.plcy_asset_cvge_mtrc_strt_dttm
                                                  AND             cast(plcy_src.src_feat_id AS DECIMAL(19,0))=cast(tgt_plcy_asset.feat_id AS DECIMAL(19,0))
                                                  AND             plcy_src.src_insrnc_mtrc_type_cd=tgt_plcy_asset.insrnc_mtrc_type_cd )a
                           WHERE  ins_upd_flag=''I''
                           AND    src_agmt_id IS NOT NULL
                           AND    src_prty_asset_id <> 9999
                           AND    src_feat_id IS NOT NULL
                           AND    src_feat_id <>9999 ) src ) );
  -- Component exp_pass_frm_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_source AS
  (
         SELECT sq_pc_plcy_asset_cvge_mtrc_x.src_feat_id                               AS src_feat_id,
                sq_pc_plcy_asset_cvge_mtrc_x.src_agmt_asset_feat_strt_dttm             AS src_agmt_asset_feat_strt_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.src_asset_cntrct_role_sbtype_cd           AS src_asset_cntrct_role_sbtype_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_prty_asset_id                         AS src_prty_asset_id,
                sq_pc_plcy_asset_cvge_mtrc_x.src_agmt_asset_strt_dttm                  AS src_agmt_asset_strt_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.src_agmt_id                               AS src_agmt_id,
                sq_pc_plcy_asset_cvge_mtrc_x.src_insrnc_mtrc_type_cd                   AS src_insrnc_mtrc_type_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_plcy_asset_cvge_mtrc_strt_dttm        AS src_plcy_asset_cvge_mtrc_strt_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.src_plcy_asset_cvge_mtrc_end_dttm         AS src_plcy_asset_cvge_mtrc_end_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.src_tm_prd_cd                             AS src_tm_prd_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_plcy_asset_cvge_amt                   AS src_plcy_asset_cvge_amt,
                sq_pc_plcy_asset_cvge_mtrc_x.src_plcy_asset_cvge_cnt                   AS src_plcy_asset_cvge_cnt,
                sq_pc_plcy_asset_cvge_mtrc_x.src_uom_cd                                AS src_uom_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_cury_cd                               AS src_cury_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_uom_type_cd                           AS src_uom_type_cd,
                sq_pc_plcy_asset_cvge_mtrc_x.src_nk_src_key                            AS src_nk_src_key,
                sq_pc_plcy_asset_cvge_mtrc_x.src_trans_strt_dttm                       AS src_trans_strt_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.src_rnk                                   AS src_rnk,
                sq_pc_plcy_asset_cvge_mtrc_x.edw_strt_dttm                             AS edw_strt_dttm,
                sq_pc_plcy_asset_cvge_mtrc_x.ins_upd_flag                              AS ins_upd_flag,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                :prcs_id                                                               AS out_prcs_id,
                sq_pc_plcy_asset_cvge_mtrc_x.source_record_id
         FROM   sq_pc_plcy_asset_cvge_mtrc_x );
  -- Component rtr_insupd_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_insupd_insert as
  SELECT exp_pass_frm_source.src_feat_id                        AS src_feat_id,
         exp_pass_frm_source.src_agmt_asset_feat_strt_dttm      AS src_agmt_asset_feat_strt_dttm,
         exp_pass_frm_source.src_asset_cntrct_role_sbtype_cd    AS src_asset_cntrct_role_sbtype_cd,
         exp_pass_frm_source.src_prty_asset_id                  AS src_prty_asset_id,
         exp_pass_frm_source.src_agmt_asset_strt_dttm           AS src_agmt_asset_strt_dttm,
         exp_pass_frm_source.src_agmt_id                        AS src_agmt_id,
         exp_pass_frm_source.src_insrnc_mtrc_type_cd            AS src_insrnc_mtrc_type_cd,
         exp_pass_frm_source.src_plcy_asset_cvge_mtrc_strt_dttm AS src_plcy_asset_cvge_mtrc_strt_dttm,
         exp_pass_frm_source.src_plcy_asset_cvge_mtrc_end_dttm  AS src_plcy_asset_cvge_mtrc_end_dttm,
         exp_pass_frm_source.src_tm_prd_cd                      AS src_tm_prd_cd,
         exp_pass_frm_source.src_plcy_asset_cvge_amt            AS src_plcy_asset_cvge_amt,
         exp_pass_frm_source.src_plcy_asset_cvge_cnt            AS src_plcy_asset_cvge_cnt,
         exp_pass_frm_source.src_uom_cd                         AS src_uom_cd,
         exp_pass_frm_source.src_cury_cd                        AS src_cury_cd,
         exp_pass_frm_source.src_uom_type_cd                    AS src_uom_type_cd,
         exp_pass_frm_source.src_nk_src_key                     AS src_nk_src_key,
         exp_pass_frm_source.src_trans_strt_dttm                AS src_trans_strt_dttm,
         exp_pass_frm_source.src_rnk                            AS src_rnk,
         exp_pass_frm_source.edw_strt_dttm                      AS edw_strt_dttm,
         exp_pass_frm_source.ins_upd_flag                       AS ins_upd_flag,
         exp_pass_frm_source.out_edw_end_dttm                   AS out_edw_end_dttm,
         exp_pass_frm_source.out_prcs_id                        AS out_prcs_id,
         exp_pass_frm_source.source_record_id
  FROM   exp_pass_frm_source
  WHERE  exp_pass_frm_source.ins_upd_flag = ''I''
  AND    exp_pass_frm_source.src_agmt_id IS NOT NULL
  AND    exp_pass_frm_source.src_prty_asset_id <> 9999
  AND    exp_pass_frm_source.src_feat_id IS NOT NULL
  AND    exp_pass_frm_source.src_feat_id <> 9999;
  
  -- Component upd_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insupd_insert.src_feat_id                        AS src_feat_id1,
                rtr_insupd_insert.src_agmt_asset_feat_strt_dttm      AS src_agmt_asset_feat_strt_dttm1,
                rtr_insupd_insert.src_asset_cntrct_role_sbtype_cd    AS src_asset_cntrct_role_sbtype_cd1,
                rtr_insupd_insert.src_prty_asset_id                  AS src_prty_asset_id1,
                rtr_insupd_insert.src_agmt_asset_strt_dttm           AS src_agmt_asset_strt_dttm1,
                rtr_insupd_insert.src_agmt_id                        AS src_agmt_id1,
                rtr_insupd_insert.src_insrnc_mtrc_type_cd            AS src_insrnc_mtrc_type_cd1,
                rtr_insupd_insert.src_plcy_asset_cvge_mtrc_strt_dttm AS src_plcy_asset_cvge_mtrc_strt_dttm1,
                rtr_insupd_insert.src_plcy_asset_cvge_mtrc_end_dttm  AS src_plcy_asset_cvge_mtrc_end_dttm1,
                rtr_insupd_insert.src_tm_prd_cd                      AS src_tm_prd_cd1,
                rtr_insupd_insert.src_plcy_asset_cvge_amt            AS src_plcy_asset_cvge_amt1,
                rtr_insupd_insert.src_plcy_asset_cvge_cnt            AS src_plcy_asset_cvge_cnt1,
                rtr_insupd_insert.src_uom_cd                         AS src_uom_cd1,
                rtr_insupd_insert.src_cury_cd                        AS src_cury_cd1,
                rtr_insupd_insert.src_uom_type_cd                    AS src_uom_type_cd1,
                rtr_insupd_insert.src_nk_src_key                     AS src_nk_src_key1,
                rtr_insupd_insert.src_trans_strt_dttm                AS src_trans_strt_dttm1,
                rtr_insupd_insert.src_rnk                            AS src_rnk1,
                rtr_insupd_insert.edw_strt_dttm                      AS edw_strt_dttm,
                rtr_insupd_insert.ins_upd_flag                       AS ins_upd_flag1,
                rtr_insupd_insert.out_edw_end_dttm                   AS out_edw_end_dttm1,
                rtr_insupd_insert.out_prcs_id                        AS out_prcs_id1,
                0                                                    AS update_strategy_action,
                rtr_insupd_insert.source_record_id
         FROM   rtr_insupd_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_insert.src_feat_id1                        AS feat_id,
                upd_insert.src_agmt_asset_feat_strt_dttm1      AS agmt_asset_feat_strt_dt1,
                upd_insert.src_asset_cntrct_role_sbtype_cd1    AS asset_cntrct_role_sbtype_cd1,
                upd_insert.src_prty_asset_id1                  AS prty_asset_id,
                upd_insert.src_agmt_asset_strt_dttm1           AS agmt_asset_strt_dttm,
                upd_insert.src_agmt_id1                        AS agmt_id,
                upd_insert.src_insrnc_mtrc_type_cd1            AS insrnc_mtrc_type_cd,
                upd_insert.src_plcy_asset_cvge_mtrc_strt_dttm1 AS plcy_asset_cvge_mtrc_strt_dt,
                upd_insert.src_plcy_asset_cvge_mtrc_end_dttm1  AS plcy_asset_cvge_mtrc_end_dt,
                upd_insert.src_tm_prd_cd1                      AS tm_prd_cd,
                upd_insert.src_plcy_asset_cvge_amt1            AS plcy_asset_cvge_amt,
                upd_insert.src_plcy_asset_cvge_cnt1            AS plcy_asset_cvge_cnt,
                upd_insert.src_uom_cd1                         AS uom_cd,
                upd_insert.src_cury_cd1                        AS cury_cd,
                upd_insert.src_uom_type_cd1                    AS uom_type_cd,
                upd_insert.src_nk_src_key1                     AS nk_src_key,
                upd_insert.out_prcs_id1                        AS prcs_id,
                upd_insert.edw_strt_dttm                       AS edw_strt_dttm,
                upd_insert.out_edw_end_dttm1                   AS edw_end_dttm,
                upd_insert.src_trans_strt_dttm1                AS trans_strt_dttm,
                upd_insert.source_record_id
         FROM   upd_insert );
  -- Component PLCY_ASSET_CVGE_MTRC_ins, Type TARGET
  INSERT INTO db_t_prod_core.plcy_asset_cvge_mtrc
              (
                          feat_id,
                          agmt_asset_feat_strt_dttm,
                          asset_cntrct_role_sbtype_cd,
                          prty_asset_id,
                          agmt_asset_strt_dttm,
                          agmt_id,
                          insrnc_mtrc_type_cd,
                          plcy_asset_cvge_mtrc_strt_dttm,
                          plcy_asset_cvge_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_asset_cvge_amt,
                          plcy_asset_cvge_cnt,
                          uom_cd,
                          cury_cd,
                          uom_type_cd,
                          nk_src_key,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins.feat_id                      AS feat_id,
         exp_pass_to_tgt_ins.agmt_asset_feat_strt_dt1     AS agmt_asset_feat_strt_dttm,
         exp_pass_to_tgt_ins.asset_cntrct_role_sbtype_cd1 AS asset_cntrct_role_sbtype_cd,
         exp_pass_to_tgt_ins.prty_asset_id                AS prty_asset_id,
         exp_pass_to_tgt_ins.agmt_asset_strt_dttm         AS agmt_asset_strt_dttm,
         exp_pass_to_tgt_ins.agmt_id                      AS agmt_id,
         exp_pass_to_tgt_ins.insrnc_mtrc_type_cd          AS insrnc_mtrc_type_cd,
         exp_pass_to_tgt_ins.plcy_asset_cvge_mtrc_strt_dt AS plcy_asset_cvge_mtrc_strt_dttm,
         exp_pass_to_tgt_ins.plcy_asset_cvge_mtrc_end_dt  AS plcy_asset_cvge_mtrc_end_dttm,
         exp_pass_to_tgt_ins.tm_prd_cd                    AS tm_prd_cd,
         exp_pass_to_tgt_ins.plcy_asset_cvge_amt          AS plcy_asset_cvge_amt,
         exp_pass_to_tgt_ins.plcy_asset_cvge_cnt          AS plcy_asset_cvge_cnt,
         exp_pass_to_tgt_ins.uom_cd                       AS uom_cd,
         exp_pass_to_tgt_ins.cury_cd                      AS cury_cd,
         exp_pass_to_tgt_ins.uom_type_cd                  AS uom_type_cd,
         exp_pass_to_tgt_ins.nk_src_key                   AS nk_src_key,
         exp_pass_to_tgt_ins.prcs_id                      AS prcs_id,
         exp_pass_to_tgt_ins.edw_strt_dttm                AS edw_strt_dttm,
         exp_pass_to_tgt_ins.edw_end_dttm                 AS edw_end_dttm,
         exp_pass_to_tgt_ins.trans_strt_dttm              AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_pc_plcy_asset_cvge_mtrc_x1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_plcy_asset_cvge_mtrc_x1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS public_id,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT ''''   AS public_id,
                                                                  NULL AS termnumber,
                                                                  NULL AS agmt_type,
                                                                  NULL AS fixedid,
                                                                  NULL AS asset_sbtype_cd,
                                                                  NULL AS asset_clasfcn_cd,
                                                                  NULL AS cov_type_cd,
                                                                  NULL AS earnings_as_of_dttm,
                                                                  NULL AS amount,
                                                                  NULL AS cvge_amt,
                                                                  NULL AS insrnc_mtrc_type_cd,
                                                                  NULL AS updatetime,
                                                                  1    AS rnk
                                                  FROM            db_t_prod_stag.ab_user
                                                  WHERE           1=2 ) src ) );
  -- Component PLCY_ASSET_CVGE_MTRC_upd_post_sql, Type TARGET
  INSERT INTO db_t_prod_core.plcy_asset_cvge_mtrc
              (
                          asset_cntrct_role_sbtype_cd
              )
  SELECT sq_pc_plcy_asset_cvge_mtrc_x1.public_id AS asset_cntrct_role_sbtype_cd
  FROM   sq_pc_plcy_asset_cvge_mtrc_x1;
  
  -- PIPELINE END FOR 2
  -- Component PLCY_ASSET_CVGE_MTRC_upd_post_sql, Type Post SQL
  /*UPDATE  PLCY_ASSET_CVGE_MTRC  FROM
(
SELECT  DISTINCT             AGMT_ID,FEAT_ID,AGMT_ASSET_FEAT_STRT_DTTM,ASSET_CNTRCT_ROLE_SBTYPE_CD,PRTY_ASSET_ID,AGMT_ASSET_STRT_DTTM,INSRNC_MTRC_TYPE_CD,EDW_STRT_DTTM,
EDW_STRT_DTTM -  CAST( (RNK-1)*2  AS INTERVAL SECOND) AS  NEW_EDW_STRT_DTTM,TRANS_STRT_DTTM,
ROW_NUMBER() OVER(PARTITION BY AGMT_ID,FEAT_ID,AGMT_ASSET_FEAT_STRT_DTTM,ASSET_CNTRCT_ROLE_SBTYPE_CD,PRTY_ASSET_ID,AGMT_ASSET_STRT_DTTM,INSRNC_MTRC_TYPE_CD, EDW_STRT_DTTM  ORDER BY TRANS_STRT_DTTM DESC )  AS RNK
FROM   PLCY_ASSET_CVGE_MTRC
WHERE EDW_END_DTTM=TO_DATE(''9999/31/12'',''YYYY/DD/MM'')
AND  PLCY_ASSET_CVGE_MTRC.INSRNC_MTRC_TYPE_CD not in (''GLERNDPREM'',''GLTRANPREM'',''GLUNERNPREM'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_ID,FEAT_ID,AGMT_ASSET_FEAT_STRT_DTTM,ASSET_CNTRCT_ROLE_SBTYPE_CD,PRTY_ASSET_ID,AGMT_ASSET_STRT_DTTM,INSRNC_MTRC_TYPE_CD,EDW_STRT_DTTM   ORDER BY TRANS_STRT_DTTM DESC ) >1
)  A
SET EDW_STRT_DTTM= A.NEW_EDW_STRT_DTTM
WHERE
PLCY_ASSET_CVGE_MTRC.AGMT_ID=A.AGMT_ID
AND PLCY_ASSET_CVGE_MTRC.FEAT_ID=A.FEAT_ID
AND PLCY_ASSET_CVGE_MTRC.AGMT_ASSET_FEAT_STRT_DTTM=A.AGMT_ASSET_FEAT_STRT_DTTM
AND PLCY_ASSET_CVGE_MTRC.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD
AND PLCY_ASSET_CVGE_MTRC.PRTY_ASSET_ID=A.PRTY_ASSET_ID
AND PLCY_ASSET_CVGE_MTRC.AGMT_ASSET_STRT_DTTM=A.AGMT_ASSET_STRT_DTTM
AND PLCY_ASSET_CVGE_MTRC.INSRNC_MTRC_TYPE_CD=A.INSRNC_MTRC_TYPE_CD
AND  PLCY_ASSET_CVGE_MTRC.TRANS_STRT_DTTM=A.TRANS_STRT_DTTM
AND  PLCY_ASSET_CVGE_MTRC.EDW_END_DTTM=TO_DATE(''9999/31/12'',''YYYY/DD/MM'')
AND  PLCY_ASSET_CVGE_MTRC.INSRNC_MTRC_TYPE_CD not in (''GLERNDPREM'',''GLTRANPREM'',''GLUNERNPREM'');
*/
  UPDATE db_t_prod_core.plcy_asset_cvge_mtrc
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         feat_id,
                                         agmt_asset_feat_strt_dttm,
                                         asset_cntrct_role_sbtype_cd,
                                         prty_asset_id,
                                         agmt_asset_strt_dttm,
                                         insrnc_mtrc_type_cd,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY agmt_id,feat_id,agmt_asset_feat_strt_dttm,asset_cntrct_role_sbtype_cd,prty_asset_id,agmt_asset_strt_dttm,insrnc_mtrc_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id,feat_id,agmt_asset_feat_strt_dttm,asset_cntrct_role_sbtype_cd,prty_asset_id,agmt_asset_strt_dttm,insrnc_mtrc_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.plcy_asset_cvge_mtrc
                         WHERE           plcy_asset_cvge_mtrc.insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                                                          ''GLTRANPREM'',
                                                                                          ''GLUNERNPREM'') ) a

  WHERE  plcy_asset_cvge_mtrc.edw_strt_dttm=a.edw_strt_dttm
  AND    plcy_asset_cvge_mtrc.agmt_id=a.agmt_id
  AND    plcy_asset_cvge_mtrc.feat_id=a.feat_id
  AND    plcy_asset_cvge_mtrc.agmt_asset_feat_strt_dttm=a.agmt_asset_feat_strt_dttm
  AND    plcy_asset_cvge_mtrc.asset_cntrct_role_sbtype_cd=a.asset_cntrct_role_sbtype_cd
  AND    plcy_asset_cvge_mtrc.prty_asset_id=a.prty_asset_id
  AND    plcy_asset_cvge_mtrc.agmt_asset_strt_dttm=a.agmt_asset_strt_dttm
  AND    plcy_asset_cvge_mtrc.insrnc_mtrc_type_cd=a.insrnc_mtrc_type_cd
  AND    plcy_asset_cvge_mtrc.trans_strt_dttm <>plcy_asset_cvge_mtrc.trans_end_dttm
  AND    plcy_asset_cvge_mtrc.insrnc_mtrc_type_cd NOT IN (''GLERNDPREM'',
                                                          ''GLTRANPREM'',
                                                          ''GLUNERNPREM'')
  AND    lead IS NOT NULL
  AND    lead1 IS NOT NULL;

END;
';