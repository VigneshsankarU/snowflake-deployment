-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_FEAT_LOCTR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
  P_LOAD_USER STRING;
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
  P_LOAD_USER := public.func_get_scoped_param(:run_id, ''p_load_user'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component SQ_pc_agmt_feat_loctr_x1, Type SOURCE
  CREATE OR replace TEMPORARY TABLE sq_pc_agmt_feat_loctr_x1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_agmt_id,
                $2  AS lkp_feat_id,
                $3  AS lkp_agmt_feat_loctr_role_cd,
                $4  AS lkp_agmt_feat_loctr_strt_dttm,
                $5  AS lkp_loc_id,
                $6  AS lkp_agmt_feat_loctr_amt,
                $7  AS lkp_agmt_feat_loctr_end_dttm,
                $8  AS lkp_edw_strt_dttm,
                $9  AS lkp_edw_end_dttm,
                $10 AS in_agmt_id,
                $11 AS in_feat_id,
                $12 AS in_loc_id,
                $13 AS in_agmt_feat_loctr_amt,
                $14 AS in_agmt_feat_loctr_strt_dttm,
                $15 AS in_agmt_feat_loctr_end_dttm,
                $16 AS o_prcs_id,
                $17 AS in_updatetime,
                $18 AS agmt_feat_loctr_role_type_cd,
                $19 AS v_lkp_chksum,
                $20 AS v_input_chksum,
                $21 AS o_flag,
                $22 AS in_retired,
                $23 AS source_record_id
         FROM   (
                            SELECT    src.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                                FROM      (
                                          SELECT lkp_target.agmt_id                   AS lkp_agmt_id,
                                                  lkp_target.feat_id                   AS lkp_feat_id,
                                                  lkp_target.agmt_feat_loctr_role_cd   AS lkp_agmt_feat_loctr_role_cd,
                                                  lkp_target.agmt_feat_loctr_strt_dttm AS lkp_agmt_feat_loctr_strt_dttm,
                                                  lkp_target.loc_id                    AS lkp_loc_id,
                                                  lkp_target.agmt_feat_loctr_amt       AS lkp_agmt_feat_loctr_amt,
                                                  lkp_target.agmt_feat_loctr_end_dttm  AS lkp_agmt_feat_loctr_end_dttm,
                                                  lkp_target.edw_strt_dttm             AS lkp_edw_strt_dttm,
                                                  lkp_target.edw_end_dttm              AS lkp_edw_end_dttm,
                                                  sq2.agmt_id                          AS in_agmt_id,
                                                  sq2.feat_id                          AS in_feat_id,
                                                  o_street_address_id                  AS in_loc_id,
                                                  feat_amt                             AS in_agmt_feat_loctr_amt,
                                                  o_agmt_feat_loctr_strt_dttm          AS in_agmt_feat_loctr_strt_dttm,
                                                  o_agmt_feat_loctr_end_dttm           AS in_agmt_feat_loctr_end_dttm,
                                                  o_prcs_id,
                                                  updatetime                     AS in_updatetime,
                                                  o_agmt_feat_loctr_role_type_cd AS agmt_feat_loctr_role_type_cd,
                                                  /*flag*/
                                                  /*  MD5(ltrim(rtrim(lkp_AGMT_FEAT_LOCTR_ROLE_CD))||ltrim(rtrim(lkp_AGMT_FEAT_LOCTR_STRT_DTTM))||ltrim(rtrim(lkp_AGMT_FEAT_LOCTR_END_DTTM))||ltrim(rtrim(lkp_AGMT_FEAT_LOCTR_AMT)))*/
                                                  cast(trim(cast(lkp_agmt_feat_loctr_role_cd AS        VARCHAR(100)))
                                                         || trim(cast(lkp_agmt_feat_loctr_strt_dttm AS VARCHAR(100)))
                                                         || trim(cast(lkp_agmt_feat_loctr_end_dttm AS  VARCHAR(100)))
                                                         || trim(cast(lkp_agmt_feat_loctr_amt AS       VARCHAR(100))) AS VARCHAR(100)) AS v_lkp_chksum,
                                                  /*MD5(ltrim(rtrim(AGMT_FEAT_LOCTR_ROLE_TYPE_CD))||ltrim(rtrim(in_AGMT_FEAT_LOCTR_STRT_DTTM))||ltrim(rtrim(in_AGMT_FEAT_LOCTR_END_DTTM))||ltrim(rtrim(in_AGMT_FEAT_LOCTR_AMT)))*/
                                                  cast(trim(cast(agmt_feat_loctr_role_type_cd AS      VARCHAR(100)))
                                                         || trim(cast(in_agmt_feat_loctr_strt_dttm AS VARCHAR(100)))
                                                         || trim(cast(in_agmt_feat_loctr_end_dttm AS  VARCHAR(100)))
                                                         || trim(cast(in_agmt_feat_loctr_amt AS       VARCHAR(100))) AS VARCHAR(100)) AS v_input_chksum,
                                                  /*CASE WHEN (lkp_AGMT_ID IS NULL or lkp_FEAT_ID IS NULL or lkp_LOC_ID IS NULL) THEN ''I'' ELSE (CASE WHEN v_input_CHKSUM<>v_lkp_CHKSUM THEN ''U'' ELSE ''R'' END) END*/
                                                  CASE
                                                         WHEN lkp_agmt_id IS NULL
                                                         OR     lkp_feat_id IS NULL
                                                         OR     lkp_loc_id IS NULL THEN ''I''
                                                         WHEN v_input_chksum<>v_lkp_chksum THEN ''U''
                                                         ELSE ''R''
                                                  END     AS o_flag,
                                                  retired AS in_retired
                                           FROM   (

                                                            SELECT    feat_amt,
                                                                      o_feat_sbtype_cd,
                                                                      nk_public_id,
                                                                      agmt_feat_loctr_role_type_cd AS o_agmt_feat_loctr_role_type_cd,
                                                                      public_id ,
                                                                      createtime,
                                                                      o_src_cd,
                                                                      o_val_typ_cd,
                                                                      cast(updatetime AS timestamp) AS updatetime,
                                                                      retired,
                                                                      cast(agmt_feat_loctr_strt_dt AS timestamp) AS o_agmt_feat_loctr_strt_dttm,
                                                                      cast(agmt_feat_loctr_end_dt AS timestamp)  AS o_agmt_feat_loctr_end_dttm,
                                                                      o_country_id,
                                                                      o_terr_id,
                                                                      o_cnty_id,
                                                                      o_pstl_cd_id,
                                                                      o_city_id,
                                                                      o_street_address_id,
                                                                      o_agmt_type_cd,
                                                                      lkp_agmt.agmt_id AS agmt_id,
                                                                      lkp_feat.feat_id AS feat_id,
                                                                      :prcs_id         AS o_prcs_id
                                                            FROM      (
      SELECT policynumber,
                                                                                    CASE
                                                                                           WHEN feat_sbtype_cd=''MODIFIER'' THEN ''FEAT_SBTYPE11''
                                                                                           WHEN feat_sbtype_cd=''OPTIONS'' THEN ''FEAT_SBTYPE8''
                                                                                           WHEN feat_sbtype_cd=''COVTERM'' THEN ''FEAT_SBTYPE6''
                                                                                           WHEN feat_sbtype_cd=''CLAUSE'' THEN ''FEAT_SBTYPE7''
                                                                                           WHEN feat_sbtype_cd=''PACKAGE'' THEN ''FEAT_SBTYPE9''
                                                                                           WHEN feat_sbtype_cd=''CL'' THEN ''FEAT_SBTYPE7''
                                                                                           WHEN feat_sbtype_cd=''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15''
                                                                                    END AS v_feat_sbtype_cd,
                                                                                    CASE
                                                                                           WHEN lkp_feat_sbtype_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                           ELSE lkp_feat_sbtype_cd.tgt_idntftn_val
                                                                                    END AS o_feat_sbtype_cd,
                                                                                    CASE
                                                                                                  /*  CASE WHEN IS_INTEGER(FEAT_AMT) THEN IFNULL(TRY_TO_DECIMAL(FEAT_AMT), 0) ELSE IFNULL(TRY_TO_DECIMAL(0.0000), 0) END*/
                                                                                           WHEN to_number(feat_amt) IS NOT NULL THEN cast(feat_amt AS DECIMAL(15, 4))
                                                                                           ELSE cast(''0.0000'' AS                                      DECIMAL(15, 4))
                                                                                    END         AS feat_amt,
                                                                                    nk_publicid AS nk_public_id,
                                                                                    publicid    AS public_id,
                                                                                    createtime,
                                                                                    src_cd,
                                                                                    lkp_src_cd.tgt_idntftn_val AS o_src_cd,
                                                                                    val_typ_cd,
                                                                                    lkp_val_type_cd.tgt_idntftn_val AS o_val_typ_cd,
                                                                                    CASE
                                                                                                  /*  CASE WHEN UPDATETIME IS NULL THEN to_date(''1900-01-01'',''YYYY-MM-DD'') ELSE UPDATETIME END*/
                                                                                           WHEN updatetime IS NULL THEN cast(cast(''01-01-1900'' AS DATE ) AS timestamp)
                                                                                           ELSE updatetime
                                                                                    END AS updatetime,
                                                                                    retired,
                                                                                    agmt_feat_loctr_role_type_cd,
                                                                                    trim(agmt_feat_loctr_role_type_cd) AS v_agmt_feat_role_cd,
                                                                                    agmt_feat_loctr_strt_dt,
                                                                                    agmt_feat_loctr_end_dt,
                                                                                    pl_addressline1,
                                                                                    pl_addressline2,
                                                                                    pl_addressline3,
                                                                                    pl_county,
                                                                                    pl_city,
                                                                                    pl_state,
                                                                                    pl_country,
                                                                                    pl_postalcode,
                                                                                    ctry.ctry_id                   AS o_country_id,
                                                                                    terr.terr_id                   AS o_terr_id,
                                                                                    cnty.cnty_id                   AS o_cnty_id,
                                                                                    lkp_postl_cd.postl_cd_id       AS o_pstl_cd_id,
                                                                                    lkp_city.city_id               AS o_city_id,
                                                                                    lkp_street_addr.street_addr_id AS o_street_address_id,
                                                                                    :p_agmt_type_cd_policy_version AS o_agmt_type_cd
                                                                             FROM   (
                                                                                          /* ---------------SQ Query Starts Here---------------- */
                                                                                                    SELECT DISTINCT policynumber,
                                                                                                                    feat_sbtype_cd,
                                                                                                                    feat_amt,
                                                                                                                    addressbookuid,
                                                                                                                    addressline1,
                                                                                                                    addressline2,
                                                                                                                    addressline3,
                                                                                                                    county,
                                                                                                                    city,
                                                                                                                    state ,
                                                                                                                    country,
                                                                                                                    postalcode,
                                                                                                                    pl_addressline1,
                                                                                                                    pl_addressline2,
                                                                                                                    pl_addressline3,
                                                                                                                    pl_county,
                                                                                                                    pl_city,
                                                                                                                    pl_state,
                                                                                                                    pl_country,
                                                                                                                    pl_postalcode,
                                                                                                                    tax_city,
                                                                                                                    publicid,
                                                                                                                    loc_publicid,
                                                                                                                    nk_publicid,
                                                                                                                    agmt_feat_loctr_role_type_cd,
                                                                                                                    agmt_feat_loctr_strt_dt,
                                                                                                                    agmt_feat_loctr_end_dt,
                                                                                                                    cast(retired AS VARCHAR(60))retired,
                                                                                                                    createtime,
                                                                                                                    cast(ctl_id AS SMALLINT)ctl_id,
                                                                                                                    load_user,
                                                                                                                    cast(start_dttm AS timestamp(6)) start_dttm,
                                                                                                                    load_dttm,
                                                                                                                    updatetime,
                                                                                                                    cast(end_dttm AS timestamp(6))   end_dttm,
                                                                                                                    ''SRC_SYS4''                    AS src_cd,
                                                                                                                    NULL                          AS val_typ_cd 
                    FROM            (
                    SELECT DISTINCT pc_policyperiod.policynumber,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN covterm.covtermtype = ''Package'' THEN cast(''PACKAGE'' AS VARCHAR(50))
                                                                                                                                                                    WHEN covterm.covtermtype = ''Option''
                                                                                                                                                                    AND             polcov.val IS NOT NULL THEN cast(''OPTIONS'' AS VARCHAR(50))
                                                                                                                                                                    WHEN covterm.covtermtype = ''Clause'' THEN cast(''CLAUSE'' AS     VARCHAR(50))
                                                                                                                                                                    ELSE cast(''COVTERM'' AS                                        VARCHAR(50))
                                                                                                                                                    END feat_sbtype_cd, (
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN covterm.covtermtype = ''Option''
                                                                                                                                                                    AND             optn.valuetype = ''money'' THEN optn.value_stg
                                                                                                                                                                    WHEN covterm.covtermtype <> ''Option'' THEN polcov.val
                                                                                                                                                    END )                                      feat_amt ,
                                                                                                                                                    (:start_dttm)                              AS start_dttm ,
                                                                                                                                                    (:end_dttm)                                AS end_dttm ,
                                                                                                                                                    polcov.patterncode                         AS loc_publicid ,
                                                                                                                                                    pc_address.addressbookuid_stg              AS addressbookuid ,
                                                                                                                                                    pc_address.county_stg                      AS county ,
                                                                                                                                                    pc_address.postalcode_stg                  AS postalcode ,
                                                                                                                                                    pc_address.city_stg                        AS city ,
                                                                                                                                                    pc_address.addressline1_stg                AS addressline1 ,
                                                                                                                                                    pc_address.addressline2_stg                AS addressline2 ,
                                                                                                                                                    pc_address.addressline3_stg                AS addressline3 ,
                                                                                                                                                    pctl_country.typecode_stg                  AS country ,
                                                                                                                                                    pc_taxlocation.city_stg                    AS tax_city ,
                                                                                                                                                    cast(''UNK'' AS VARCHAR(50))                 AS agmt_feat_loctr_role_type_cd ,
                                                                                                                                                    pctl_jurisdiction.typecode_stg             AS state ,
                                                                                                                                                    pc_policylocation.countyinternal_stg       AS pl_county ,
                                                                                                                                                    pc_policylocation.postalcodeinternal_stg   AS pl_postalcode ,
                                                                                                                                                    pc_policylocation.cityinternal_stg         AS pl_city ,
                                                                                                                                                    pc_policylocation.addressline1internal_stg AS pl_addressline1 ,
                                                                                                                                                    pc_policylocation.addressline2internal_stg AS pl_addressline2 ,
                                                                                                                                                    pc_policylocation.addressline3internal_stg AS pl_addressline3 ,
                                                                                                                                                    pc_policylocation.stateinternal_stg        AS pl_state1 ,
                                                                                                                                                    pc_policylocation.countryinternal_stg      AS pl_country1 ,
                                                                                                                                                    pctl_state.typecode_stg                    AS pl_state ,
                                                                                                                                                    pctl_country.typecode_stg                  AS pl_country,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN polcov.effectivedate IS NULL THEN pc_policyperiod.periodstart
                                                                                                                                                                    ELSE polcov.effectivedate
                                                                                                                                                    END agmt_feat_loctr_strt_dt,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN polcov.expirationdate IS NULL THEN pc_policyperiod.periodend
                                                                                                                                                                    ELSE polcov.expirationdate
                                                                                                                                                    END agmt_feat_loctr_end_dt,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN covterm.covtermtype = ''Package'' THEN PACKAGE.packagepatternid
                                                                                                                                                                    WHEN covterm.covtermtype = ''Option''
                                                                                                                                                                    AND             polcov.val IS NOT NULL THEN optn.optionpatternid
                                                                                                                                                                    WHEN covterm.covtermtype = ''Clause'' THEN covterm.clausepatternid
                                                                                                                                                                    ELSE covterm.covtermpatternid
                                                                                                                                                    END nk_publicid ,
                                                                                                                                                    pc_policyperiod.publicid ,
                                                                                                                                                    pc_policyperiod.createtime ,
                                                                                                                                                    polcov.updatetime ,
                                                                                                                                                    pc_policyperiod.retired                AS retired ,
                                                                                                                                                    cast(''1'' AS VARCHAR(10))               AS ctl_id ,
                                                                                                                                                    (:p_load_user)                         AS load_user ,
                                                                                                                                                    cast(current_timestamp AS timestamp(6))AS load_dttm
                                                                                                                                    FROM            (
                                                                                                                                                           /*pcx_bp7locationcov*/
                                                                                                                                                           SELECT ''ChoiceTerm1''                      AS columnname ,
                                                                                                                                                                  choiceterm1_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  choiceterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm2''                      AS columnname ,
                                                                                                                                                                  choiceterm2_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  choiceterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm3''                      AS columnname ,
                                                                                                                                                                  choiceterm3_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  choiceterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm4''                      AS columnname ,
                                                                                                                                                                  choiceterm4_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  choiceterm4avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm5''                      AS columnname ,
                                                                                                                                                                  choiceterm5_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  choiceterm5avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm1''                         AS columnname ,
                                                                                                                                                                  cast(directterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  directterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm2''                         AS columnname ,
                                                                                                                                                                  cast(directterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  directterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm3''                         AS columnname ,
                                                                                                                                                                  cast(directterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  directterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm1''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  booleanterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm2''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  booleanterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm3''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  booleanterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm1''                      AS columnname ,
                                                                                                                                                                  stringterm1_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  stringterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm2''                      AS columnname ,
                                                                                                                                                                  stringterm2_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  stringterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm3''                      AS columnname ,
                                                                                                                                                                  stringterm3_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  stringterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''PositiveIntTerm1''                         AS columnname ,
                                                                                                                                                                  cast(positiveintterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                            AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))         AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))         AS assetkey ,
                                                                                                                                                                  createtime_stg                             AS createtime ,
                                                                                                                                                                  effectivedate_stg                          AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                         AS expirationdate ,
                                                                                                                                                                  updatetime_stg                             AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  positiveintterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''PositiveIntTerm2''                         AS columnname ,
                                                                                                                                                                  cast(positiveintterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                            AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))         AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))         AS assetkey ,
                                                                                                                                                                  createtime_stg                             AS createtime ,
                                                                                                                                                                  effectivedate_stg                          AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                         AS expirationdate ,
                                                                                                                                                                  updatetime_stg                             AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  positiveintterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm1''                         AS columnname ,
                                                                                                                                                                  cast(dateterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  dateterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm2''                         AS columnname ,
                                                                                                                                                                  cast(dateterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           WHERE  dateterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''Clause''                           AS columnname ,
                                                                                                                                                                  cast(NULL AS VARCHAR(255))            val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcov
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm1''                      AS columnname ,
                                                                                                                                                                  choiceterm1_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  choiceterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm2''                      AS columnname ,
                                                                                                                                                                  choiceterm2_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  choiceterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm3''                      AS columnname ,
                                                                                                                                                                  choiceterm3_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  choiceterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm4''                      AS columnname ,
                                                                                                                                                                  choiceterm4_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  choiceterm4avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm5''                      AS columnname ,
                                                                                                                                                                  choiceterm5_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  choiceterm5avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm1''                         AS columnname ,
                                                                                                                                                                  cast(directterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  directterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm2''                         AS columnname ,
                                                                                                                                                                  cast(directterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  directterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm3''                         AS columnname ,
                                                                                                                                                                  cast(directterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  directterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm1''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  booleanterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm2''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  booleanterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm3''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  booleanterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm1''                         AS columnname ,
                                                                                                                                                                  cast(dateterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  dateterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm2''                         AS columnname ,
                                                                                                                                                                  cast(dateterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           WHERE  dateterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''Clause''                           AS columnname ,
                                                                                                                                                                  cast(NULL AS VARCHAR(255))            val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationcond
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm1''                      AS columnname ,
                                                                                                                                                                  choiceterm1_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  choiceterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm2''                      AS columnname ,
                                                                                                                                                                  choiceterm2_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  choiceterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm3''                      AS columnname ,
                                                                                                                                                                  choiceterm3_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  choiceterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm4''                      AS columnname ,
                                                                                                                                                                  choiceterm4_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  choiceterm4avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''ChoiceTerm5''                      AS columnname ,
                                                                                                                                                                  choiceterm5_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  choiceterm5avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm1''                         AS columnname ,
                                                                                                                                                                  cast(directterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  directterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm2''                         AS columnname ,
                                                                                                                                                                  cast(directterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  directterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DirectTerm3''                         AS columnname ,
                                                                                                                                                                  cast(directterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                       AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))    AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))    AS assetkey ,
                                                                                                                                                                  createtime_stg                        AS createtime ,
                                                                                                                                                                  effectivedate_stg                     AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                    AS expirationdate ,
                                                                                                                                                                  updatetime_stg                        AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  directterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm1''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  booleanterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm2''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  booleanterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''BooleanTerm3''                         AS columnname ,
                                                                                                                                                                  cast(booleanterm3_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                        AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))     AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))     AS assetkey ,
                                                                                                                                                                  createtime_stg                         AS createtime ,
                                                                                                                                                                  effectivedate_stg                      AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                     AS expirationdate ,
                                                                                                                                                                  updatetime_stg                         AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  booleanterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm1''                      AS columnname ,
                                                                                                                                                                  stringterm1_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  stringterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm2''                      AS columnname ,
                                                                                                                                                                  stringterm2_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  stringterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''StringTerm3''                      AS columnname ,
                                                                                                                                                                  stringterm3_stg                    AS val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  stringterm3avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm1''                         AS columnname ,
                                                                                                                                                                  cast(dateterm1_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  dateterm1avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''DateTerm2''                         AS columnname ,
                                                                                                                                                                  cast(dateterm2_stg AS VARCHAR(255)) AS val ,
                                                                                                                                                                  patterncode_stg                     AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255))  AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255))  AS assetkey ,
                                                                                                                                                                  createtime_stg                      AS createtime ,
                                                                                                                                                                  effectivedate_stg                   AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                  AS expirationdate ,
                                                                                                                                                                  updatetime_stg                      AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl
                                                                                                                                                           WHERE  dateterm2avl_stg = 1
                                                                                                                                                           AND    expirationdate IS NULL
                                                                                                                                                           UNION
                                                                                                                                                           SELECT ''Clause''                           AS columnname ,
                                                                                                                                                                  cast(NULL AS VARCHAR(255))            val ,
                                                                                                                                                                  patterncode_stg                    AS patterncode ,
                                                                                                                                                                  cast(branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                  cast(location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                  createtime_stg                     AS createtime ,
                                                                                                                                                                  effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                  expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                  updatetime_stg                     AS updatetime
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7locationexcl) polcov
                                                                                                                                    inner join
                                                                                                                                                    (
                                                                                                                                                           SELECT cast(id_stg AS VARCHAR(255)) AS id ,
                                                                                                                                                                  policynumber_stg             AS policynumber ,
                                                                                                                                                                  periodstart_stg              AS periodstart ,
                                                                                                                                                                  periodend_stg                AS periodend ,
                                                                                                                                                                  status_stg                   AS status ,
                                                                                                                                                                  jobid_stg                    AS jobid ,
                                                                                                                                                                  publicid_stg                 AS publicid ,
                                                                                                                                                                  createtime_stg               AS createtime ,
                                                                                                                                                                  updatetime_stg               AS updatetime ,
                                                                                                                                                                  retired_stg                  AS retired ,
                                                                                                                                                                  policyid_stg                 AS policyid
                                                                                                                                                           FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                                                                                                                    ON              pc_policyperiod.id = polcov.branchid
                                                                                                                                    left join
                                                                                                                                                    (
                                                                                                                                                               SELECT     pc_etlclausepattern.patternid_stg    AS clausepatternid ,
                                                                                                                                                                          pc_etlcovtermpattern.patternid_stg   AS covtermpatternid ,
                                                                                                                                                                          pc_etlcovtermpattern.columnname_stg  AS columnname ,
                                                                                                                                                                          pc_etlcovtermpattern.name_stg        AS covname ,
                                                                                                                                                                          pc_etlcovtermpattern.covtermtype_stg AS covtermtype ,
                                                                                                                                                                          pc_etlclausepattern.name_stg         AS clausename ,
                                                                                                                                                                          pc_etlclausepattern.clausetype_stg   AS clausetype
                                                                                                                                                               FROM       db_t_prod_stag.pc_etlclausepattern
                                                                                                                                                               inner join db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                               ON         pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausepatternid_stg
                                                                                                                                                               UNION
                                                                                                                                                               SELECT    pc_etlclausepattern.patternid_stg                        AS clausepatternid ,
                                                                                                                                                                         pc_etlcovtermpattern.patternid_stg                       AS covtermpatternid ,
                                                                                                                                                                         coalesce(pc_etlcovtermpattern.columnname_stg, ''Clause'')  AS columnname ,
                                                                                                                                                                         pc_etlcovtermpattern.name_stg                            AS covname ,
                                                                                                                                                                         coalesce(pc_etlcovtermpattern.covtermtype_stg, ''Clause'') AS covtermtype ,
                                                                                                                                                                         pc_etlclausepattern.name_stg                             AS clausename ,
                                                                                                                                                                         pc_etlclausepattern.clausetype_stg                       AS clausetype
                                                                                                                                                               FROM      db_t_prod_stag.pc_etlclausepattern
                                                                                                                                                               left join
                                                                                                                                                                         (
                                                                                                                                                                                SELECT *
                                                                                                                                                                                FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                                                WHERE  name_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern
                                                                                                                                                               ON        pc_etlcovtermpattern.clausepatternid_stg = pc_etlclausepattern.id_stg
                                                                                                                                                               WHERE     pc_etlclausepattern.name_stg NOT LIKE ''ZZ%''
                                                                                                                                                               AND       pc_etlcovtermpattern.name_stg IS NULL
                                                                                                                                                               AND       pc_etlclausepattern.owningentitytype_stg=''BP7Location'' 
                                                                                                                                                    ) covterm
                                                                                                                                    ON              covterm.clausepatternid = polcov.patterncode
                                                                                                                                    AND             covterm.columnname = polcov.columnname
                                                                                                                                    left join
                                                                                                                                                    (
                                                                                                                                                           SELECT pc_etlcovtermpackage.patternid_stg   AS packagepatternid ,
                                                                                                                                                                  pc_etlcovtermpackage.packagecode_stg AS cov_id ,
                                                                                                                                                                  pc_etlcovtermpackage.packagecode_stg AS name
                                                                                                                                                           FROM   db_t_prod_stag.pc_etlcovtermpackage ) PACKAGE
                                                                                                                                    ON              PACKAGE.packagepatternid = polcov.val
                                                                                                                                    left join
                                                                                                                                                    (
                                                                                                                                                               SELECT     pc_etlcovtermoption.patternid_stg                   AS optionpatternid ,
                                                                                                                                                                          pc_etlcovtermoption.optioncode_stg                  AS name ,
                                                                                                                                                                          cast(pc_etlcovtermoption.value_stg AS VARCHAR(255)) AS value_stg ,
                                                                                                                                                                          pc_etlcovtermpattern.valuetype_stg                  AS valuetype
                                                                                                                                                               FROM       db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                               inner join db_t_prod_stag.pc_etlcovtermoption
                                                                                                                                                               ON         pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.coveragetermpatternid_stg ) optn
                                                                                                                                    ON              optn.optionpatternid = polcov.val
                                                                                                                                    inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                    ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status
                                                                                                                                    inner join      db_t_prod_stag.pc_job
                                                                                                                                    ON              pc_job.id_stg = pc_policyperiod.jobid
                                                                                                                                    inner join      db_t_prod_stag.pctl_job
                                                                                                                                    ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                                                                    inner join
                                                                                                                                                    (
                                                                                                                                                           SELECT cast(branchid_stg AS VARCHAR(100))AS branchid_stg,
                                                                                                                                                                  cast(fixedid_stg AS  VARCHAR(100))AS fixedid_stg,
                                                                                                                                                                  location_stg
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7location ) pcx_bp7location
                                                                                                                                    ON              pcx_bp7location.branchid_stg =pc_policyperiod.id
                                                                                                                                    AND             pcx_bp7location.fixedid_stg =polcov.assetkey
                                                                                                                                    inner join      db_t_prod_stag.pc_policylocation
                                                                                                                                    ON              pc_policylocation.id_stg = pcx_bp7location.location_stg
                                                                                                                                    inner join      db_t_prod_stag.pc_address
                                                                                                                                    ON              pc_policylocation.accountlocation_stg= pc_address.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_state
                                                                                                                                    ON              pc_address.state_stg = pctl_state.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_country
                                                                                                                                    ON              pctl_country.id_stg = pc_address.country_stg
                                                                                                                                    left join
                                                                                                                                                    (
                                                                                                                                                                    SELECT DISTINCT pc_contact.primaryaddressid_stg ,
                                                                                                                                                                                    pctl_contact.typecode_stg
                                                                                                                                                                    FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                    inner join      db_t_prod_stag.pctl_contact
                                                                                                                                                                    ON              pc_contact.subtype_stg = pctl_contact.id_stg
                                                                                                                                                                    AND             pctl_contact.typecode_stg = ''LegalVenue'' ) contact
                                                                                                                                    ON              contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                                    left join       db_t_prod_stag.pc_taxlocation
                                                                                                                                    ON              pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_jurisdiction
                                                                                                                                    ON              pctl_jurisdiction.id_stg = pc_taxlocation.state_stg
                                                                                                                                    WHERE           covterm.clausename NOT LIKE ''%ZZ%''
                                                                                                                                    AND             pctl_policyperiodstatus.typecode_stg = ''Bound''
                                                                                                                                    AND             pc_policyperiod.updatetime > (:start_dttm)
                                                                                                                                    AND             pc_policyperiod.updatetime <= (:end_dttm) 
                                                                                                                    ) pc_agmt_feat_loctr_x
                                                                                          inner join
                                                                                                                    (
                                                                                                                           SELECT cast(id_stg AS VARCHAR(100))AS id
                                                                                                                           FROM   db_t_prod_stag.pctl_state ) pctl_state
                                                                                                    ON              pc_agmt_feat_loctr_x.pl_state1=pctl_state.id
                                                                                                    inner join
                                                                                                                    (
                                                                                                                           SELECT cast(id_stg AS VARCHAR(100))AS id
                                                                                                                           FROM   db_t_prod_stag.pctl_country) pctl_country
                                                                                                    ON              pc_agmt_feat_loctr_x.pl_country1=pctl_country.id
                                                                                                    -- relocated here
                                                                                                    qualify row_number() over (PARTITION BY publicid,nk_publicid,feat_sbtype_cd,pl_addressline1,pl_addressline2,pl_addressline3,pl_county,pl_city,pl_state,pl_postalcode,pl_country ORDER BY updatetime DESC)=1
                                                                             union                       
                                                                            SELECT DISTINCT policynumber,
                                                                                                                    feat_sbtype_cd,
                                                                                                                    feat_amt,
                                                                                                                    addressbookuid,
                                                                                                                    addressline1,
                                                                                                                    addressline2,
                                                                                                                    addressline3,
                                                                                                                    county,
                                                                                                                    city,
                                                                                                                    state ,
                                                                                                                    country,
                                                                                                                    postalcode,
                                                                                                                    pl_addressline1,
                                                                                                                    pl_addressline2,
                                                                                                                    pl_addressline3,
                                                                                                                    pl_county,
                                                                                                                    pl_city,
                                                                                                                    pl_state,
                                                                                                                    pl_country,
                                                                                                                    pl_postalcode,
                                                                                                                    tax_city,
                                                                                                                    publicid,
                                                                                                                    loc_publicid,
                                                                                                                    nk_publicid,
                                                                                                                    agmt_feat_loctr_role_type_cd,
                                                                                                                    agmt_feat_loctr_strt_dt,
                                                                                                                    agmt_feat_loctr_end_dt,
                                                                                                                    cast(retired AS VARCHAR(60))retired,
                                                                                                                    createtime,
                                                                                                                    cast(ctl_id AS SMALLINT)ctl_id,
                                                                                                                    load_user,
                                                                                                                    cast(start_dttm AS timestamp(6)) start_dttm,
                                                                                                                    load_dttm,
                                                                                                                    updatetime,
                                                                                                                    cast(end_dttm AS timestamp(6))   end_dttm,
                                                                                                                    ''SRC_SYS4''                    AS src_cd,
                                                                                                                    NULL                          AS val_typ_cd 
                                                                                                                    --qualify row_number() over (PARTITION BY publicid,nk_publicid,feat_sbtype_cd,pl_addressline1,pl_addressline2,pl_addressline3,pl_county,pl_city,pl_state,pl_postalcode,pl_country ORDER BY updatetime DESC)=1
                                                                                                    FROM            (
                                                                                                                                    SELECT DISTINCT pc_policyperiod.policynumber,
                                                                                                                                                    ''FEAT_SBTYPE15''                            feat_sbtype_cd,
                                                                                                                                                    cast(NULL AS VARCHAR(100))                 feat_amt ,
                                                                                                                                                    (:start_dttm)                              AS start_dttm ,
                                                                                                                                                    (:end_dttm)                                AS end_dttm ,
                                                                                                                                                    assetkey                                   AS loc_publicid ,
                                                                                                                                                    pc_address.addressbookuid_stg              AS addressbookuid ,
                                                                                                                                                    pc_address.county_stg                      AS county ,
                                                                                                                                                    pc_address.postalcode_stg                  AS postalcode ,
                                                                                                                                                    pc_address.city_stg                        AS city ,
                                                                                                                                                    pc_address.addressline1_stg                AS addressline1 ,
                                                                                                                                                    pc_address.addressline2_stg                AS addressline2 ,
                                                                                                                                                    pc_address.addressline3_stg                AS addressline3 ,
                                                                                                                                                    pctl_country.typecode_stg                  AS country ,
                                                                                                                                                    pc_taxlocation.city_stg                    AS tax_city ,
                                                                                                                                                    cast(''UNK'' AS VARCHAR(50))                 AS agmt_feat_loctr_role_type_cd ,
                                                                                                                                                    pctl_jurisdiction.typecode_stg             AS state ,
                                                                                                                                                    pc_policylocation.countyinternal_stg       AS pl_county ,
                                                                                                                                                    pc_policylocation.postalcodeinternal_stg   AS pl_postalcode ,
                                                                                                                                                    pc_policylocation.cityinternal_stg         AS pl_city ,
                                                                                                                                                    pc_policylocation.addressline1internal_stg AS pl_addressline1 ,
                                                                                                                                                    pc_policylocation.addressline2internal_stg AS pl_addressline2 ,
                                                                                                                                                    pc_policylocation.addressline3internal_stg AS pl_addressline3 ,
                                                                                                                                                    pc_policylocation.stateinternal_stg        AS pl_state1 ,
                                                                                                                                                    pc_policylocation.countryinternal_stg      AS pl_country1 ,
                                                                                                                                                    pctl_state.typecode_stg                    AS pl_state ,
                                                                                                                                                    pctl_country.typecode_stg                  AS pl_country,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN polcov.effectivedate IS NULL THEN pc_policyperiod.periodstart
                                                                                                                                                                    ELSE polcov.effectivedate
                                                                                                                                                    END agmt_feat_loctr_strt_dt,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN polcov.expirationdate IS NULL THEN pc_policyperiod.periodend
                                                                                                                                                                    ELSE polcov.expirationdate
                                                                                                                                                    END         agmt_feat_loctr_end_dt,
                                                                                                                                                    patterncode nk_publicid ,
                                                                                                                                                    pc_policyperiod.publicid ,
                                                                                                                                                    pc_policyperiod.createtime ,
                                                                                                                                                    polcov.updatetime ,
                                                                                                                                                    pc_policyperiod.retired                AS retired ,
                                                                                                                                                    cast(''1'' AS VARCHAR(10))               AS ctl_id ,
                                                                                                                                                    (:p_load_user)                         AS load_user ,
                                                                                                                                                    cast(current_timestamp AS timestamp(6))AS load_dttm
                                                                                                                                    FROM            (
                                                                                                                                                               SELECT     cast(NULL AS VARCHAR(255))           AS columnname ,
                                                                                                                                                                          cast(NULL AS VARCHAR(255))           AS val,
                                                                                                                                                                          formpatterncode_stg                  AS patterncode ,
                                                                                                                                                                          cast(a.branchid_stg AS VARCHAR(255)) AS branchid ,
                                                                                                                                                                          cast(a.location_stg AS VARCHAR(255)) AS assetkey ,
                                                                                                                                                                          a.createtime_stg                     AS createtime ,
                                                                                                                                                                          a.effectivedate_stg                  AS effectivedate ,
                                                                                                                                                                          a.expirationdate_stg                 AS expirationdate ,
                                                                                                                                                                          a.updatetime_stg                     AS updatetime
                                                                                                                                                               FROM       db_t_prod_stag.pcx_bp7locationcov a
                                                                                                                                                               join       db_t_prod_stag.pc_policyperiod b
                                                                                                                                                               ON         b.id_stg = a.branchid_stg
                                                                                                                                                               join       db_t_prod_stag.pc_formpattern c
                                                                                                                                                               ON         c.clausepatterncode_stg = a.patterncode_stg
                                                                                                                                                               join       db_t_prod_stag.pc_form d
                                                                                                                                                               ON         d.formpatterncode_stg = c.code_stg
                                                                                                                                                               AND        d.branchid_stg = a.branchid_stg
                                                                                                                                                               join       db_t_prod_stag.pc_etlclausepattern e
                                                                                                                                                               ON         e.patternid_stg = a.patterncode_stg
                                                                                                                                                               inner join db_t_prod_stag.pctl_documenttype pd
                                                                                                                                                               ON         pd.id_stg = c.documenttype_stg
                                                                                                                                                               AND        pd.typecode_stg = ''endorsement_alfa''
                                                                                                                                                               WHERE      ( (
                                                                                                                                                                                                a.effectivedate_stg IS NULL)
                                                                                                                                                                          OR        (
                                                                                                                                                                                                a.effectivedate_stg > b.modeldate_stg
                                                                                                                                                                                     AND        coalesce( a.effectivedate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) <> coalesce(a.expirationdate_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))))
                                                                                                                                                               AND        b.status_stg = 9
                                                                                                                                                               AND        d.removedorsuperseded_stg IS NULL ) polcov
                                                                                                                                    inner join
                                                                                                                                                    (
                                                                                                                                                           SELECT cast(id_stg AS VARCHAR(255)) AS id ,
                                                                                                                                                                  policynumber_stg             AS policynumber ,
                                                                                                                                                                  periodstart_stg              AS periodstart ,
                                                                                                                                                                  periodend_stg                AS periodend ,
                                                                                                                                                                  status_stg                   AS status ,
                                                                                                                                                                  jobid_stg                    AS jobid ,
                                                                                                                                                                  publicid_stg                 AS publicid ,
                                                                                                                                                                  createtime_stg               AS createtime ,
                                                                                                                                                                  updatetime_stg               AS updatetime ,
                                                                                                                                                                  retired_stg                  AS retired ,
                                                                                                                                                                  policyid_stg                 AS policyid
                                                                                                                                                           FROM   db_t_prod_stag.pc_policyperiod ) pc_policyperiod
                                                                                                                                    ON              pc_policyperiod.id = polcov.branchid
                                                                                                                                    inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                    ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status
                                                                                                                                    inner join      db_t_prod_stag.pc_job
                                                                                                                                    ON              pc_job.id_stg = pc_policyperiod.jobid
                                                                                                                                    inner join      db_t_prod_stag.pctl_job
                                                                                                                                    ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                                                                    inner join
                                                                                                                                                    (
                                                                                                                                                           SELECT cast(branchid_stg AS VARCHAR(100))AS branchid_stg,
                                                                                                                                                                  cast(fixedid_stg AS  VARCHAR(100))AS fixedid_stg,
                                                                                                                                                                  location_stg
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7location ) pcx_bp7location
                                                                                                                                    ON              pcx_bp7location.branchid_stg =pc_policyperiod.id
                                                                                                                                    AND             pcx_bp7location.fixedid_stg =polcov.assetkey
                                                                                                                                    inner join      db_t_prod_stag.pc_policylocation
                                                                                                                                    ON              pc_policylocation.id_stg = pcx_bp7location.location_stg
                                                                                                                                    inner join      db_t_prod_stag.pc_address
                                                                                                                                    ON              pc_policylocation.accountlocation_stg= pc_address.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_state
                                                                                                                                    ON              pc_address.state_stg = pctl_state.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_country
                                                                                                                                    ON              pctl_country.id_stg = pc_address.country_stg
                                                                                                                                    left join
                                                                                                                                                    (
                                                                                                                                                                    SELECT DISTINCT pc_contact.primaryaddressid_stg ,
                                                                                                                                                                                    pctl_contact.typecode_stg
                                                                                                                                                                    FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                    inner join      db_t_prod_stag.pctl_contact
                                                                                                                                                                    ON              pc_contact.subtype_stg = pctl_contact.id_stg
                                                                                                                                                                    AND             pctl_contact.typecode_stg = ''LegalVenue'' ) contact
                                                                                                                                    ON              contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                                    left join       db_t_prod_stag.pc_taxlocation
                                                                                                                                    ON              pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg
                                                                                                                                    left join       db_t_prod_stag.pctl_jurisdiction
                                                                                                                                    ON              pctl_jurisdiction.id_stg = pc_taxlocation.state_stg
                                                                                                                                    WHERE           pctl_policyperiodstatus.typecode_stg = ''Bound''
                                                                                                                                    AND             pc_policyperiod.updatetime > (:start_dttm)
                                                                                                                                    AND             pc_policyperiod.updatetime <= (:end_dttm) 
                                                                                                                    ) pc_agmt_feat_loctr_x
                                                                                                    inner join
                                                                                                                    (
                                                                                                                           SELECT cast(id_stg AS VARCHAR(100))AS id
                                                                                                                           FROM   db_t_prod_stag.pctl_state ) pctl_state
                                                                                                    ON              pc_agmt_feat_loctr_x.pl_state1=pctl_state.id
                                                                                                    inner join
                                                                                                                    (
                                                                                                                           SELECT cast(id_stg AS VARCHAR(100))AS id
                                                                                                                           FROM   db_t_prod_stag.pctl_country) pctl_country
                                                                                                    ON              pc_agmt_feat_loctr_x.pl_country1=pctl_country.id
                                                                                                    -- relocated here
                                                                                                    qualify row_number() over (PARTITION BY publicid,nk_publicid,feat_sbtype_cd,pl_addressline1,pl_addressline2,pl_addressline3,pl_county,pl_city,pl_state,pl_postalcode,pl_country ORDER BY updatetime DESC)=1
) src_query
                                                                  --      )
                                                                      /*FEAT_SBTYPE_CD*/
                                                            left join
                                                                      (
                                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''FEAT_SBTYPE''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_feat_sbtype_cd
                                                            ON        lkp_feat_sbtype_cd.src_idntftn_val = v_feat_sbtype_cd
                                                                      /*lkp_teradata_etl_ref_xlat_src_cd*/
                                                            left join
                                                                      (
                                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_src_cd
                                                            ON        lkp_src_cd.src_idntftn_val = src_cd
                                                                      /*LKP_TERADATA_ETL_REF_XLAT_VAL_TYP_CD(in_VAL_TYP_CD)*/
                                                            left join
                                                                      (
                                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''VAL_TYPE''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''cctl_coveragebasis.name''
                                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_val_type_cd
                                                            ON        coalesce(lkp_val_type_cd.src_idntftn_val, ''~'')
                                                                      /*IS NULL*/
                                                                      = coalesce(val_typ_cd, ''~'')
                                                                      /* Having null */
                                                                      /*lkp_ctry*/
                                                            left join
                                                                      (
                                                                                      SELECT DISTINCT ctry_id,
                                                                                                      geogrcl_area_shrt_name
                                                                                      FROM            db_t_prod_core.ctry 
                                                                                      qualify row_number() over(PARTITION BY geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 
                                                                        ) ctry
                                                            ON        ctry.geogrcl_area_shrt_name = pl_country
                                                                      /*lkp_terr*/
                                                            left join
                                                                      (
                                                                                      SELECT DISTINCT terr_id,
                                                                                                      ctry_id,
                                                                                                      geogrcl_area_shrt_name
                                                                                      FROM            db_t_prod_core.terr
                                                                                      WHERE           terr.edw_end_dttm=''9999-12-31 23:59:59.999999'' 
                                                                                      qualify row_number() over(PARTITION BY ctry_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) terr
                                                            ON        o_country_id = terr.ctry_id
                                                            AND       pl_state = terr.geogrcl_area_shrt_name
                                                                      /*lkp_cnty*/
                                                            left join
                                                                      (
                                                                               SELECT   cnty_id,
                                                                                        terr_id,
                                                                                        geogrcl_area_shrt_name
                                                                               FROM     db_t_prod_core.cnty
                                                                               WHERE    edw_end_dttm=''9999-12-31 23:59:59.999999'' 
                                                                               qualify row_number() over(PARTITION BY terr_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) cnty
                                                            ON        o_terr_id = cnty.terr_id
                                                            AND       pl_county = cnty.geogrcl_area_shrt_name
                                                                      /*LKP_POSTL_CD*/
                                                            left join
                                                                      (
                                                                                      SELECT DISTINCT postl_cd_id,
                                                                                                      ctry_id,
                                                                                                      postl_cd_num
                                                                                      FROM            db_t_prod_core.postl_cd
                                                                                      WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' 
                                                                                      qualify row_number() over(PARTITION BY ctry_id,postl_cd_num ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) lkp_postl_cd
                                                            ON        o_country_id = lkp_postl_cd.ctry_id
                                                            AND       pl_postalcode = lkp_postl_cd.postl_cd_num
                                                                      /*LKP_CITY*/
                                                            left join
                                                                      (
                                                                                      SELECT DISTINCT city_id,
                                                                                                      terr_id,
                                                                                                      geogrcl_area_shrt_name
                                                                                      FROM            db_t_prod_core.city
                                                                                      WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' 
                                                                                      qualify row_number() over(PARTITION BY terr_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) lkp_city
                                                            ON        o_terr_id = lkp_city.terr_id
                                                            AND       upper(pl_city) = upper(lkp_city.geogrcl_area_shrt_name)
                                                                      /*lkp_street_addr*/
                                                            left join
                                                                      (
                                                                               SELECT   street_addr.street_addr_id      AS street_addr_id,
                                                                                        street_addr.dwlng_type_cd       AS dwlng_type_cd,
                                                                                        street_addr.carier_rte_txt      AS carier_rte_txt,
                                                                                        street_addr.sptl_pnt            AS sptl_pnt,
                                                                                        street_addr.loctr_sbtype_cd     AS loctr_sbtype_cd,
                                                                                        street_addr.addr_sbtype_cd      AS addr_sbtype_cd,
                                                                                        street_addr.geocode_sts_type_cd AS geocode_sts_type_cd,
                                                                                        street_addr.addr_stdzn_type_cd  AS addr_stdzn_type_cd,
                                                                                        street_addr.prcs_id             AS prcs_id,
                                                                                        street_addr.edw_strt_dttm       AS edw_strt_dttm,
                                                                                        street_addr.edw_end_dttm        AS edw_end_dttm,
                                                                                        street_addr.addr_ln_1_txt       AS addr_ln_1_txt,
                                                                                        street_addr.addr_ln_2_txt       AS addr_ln_2_txt,
                                                                                        street_addr.addr_ln_3_txt       AS addr_ln_3_txt,
                                                                                        street_addr.city_id             AS city_id,
                                                                                        street_addr.terr_id             AS terr_id,
                                                                                        street_addr.postl_cd_id         AS postl_cd_id,
                                                                                        street_addr.ctry_id             AS ctry_id,
                                                                                        street_addr.cnty_id             AS cnty_id
                                                                               FROM     db_t_prod_core.street_addr
                                                                               WHERE    street_addr.edw_end_dttm=''9999-12-31 23:59:59.999999''
                                                                                        /* AND STREET_ADDR.ADDR_LN_3_TXT IS NULL  */
                                                                                        qualify row_number() over(PARTITION BY addr_ln_1_txt,addr_ln_2_txt,addr_ln_3_txt,city_id,terr_id,postl_cd_id,ctry_id,cnty_id,edw_end_dttm ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 
                                                                        ) lkp_street_addr
                                                            ON        upper(lkp_street_addr.addr_ln_1_txt) = upper(pl_addressline1)
                                                            AND       coalesce(upper(lkp_street_addr.addr_ln_2_txt), ''~'') = coalesce(upper(pl_addressline2), ''~'')
                                                            AND       coalesce(upper(lkp_street_addr.addr_ln_3_txt), ''~'') = coalesce(upper(pl_addressline3), ''~'')
                                                            AND       lkp_street_addr.city_id = o_city_id
                                                            AND       lkp_street_addr.terr_id = o_terr_id
                                                            AND       lkp_street_addr.postl_cd_id = o_pstl_cd_id
                                                            AND       lkp_street_addr.ctry_id = o_country_id
                                                            AND       coalesce(lkp_street_addr.cnty_id, ''~'') = coalesce(o_cnty_id, ''~'') 
                                                    ) sq1 
                                                       left join
                                                                    (
                                                                            SELECT   agmt.agmt_id      AS agmt_id,
                                                                                    agmt.nk_src_key   AS nk_src_key,
                                                                                    agmt.agmt_type_cd AS agmt_type_cd
                                                                            FROM     db_t_prod_core.agmt 
                                                                            qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp_agmt
                                                        ON        lkp_agmt.nk_src_key = sq1.public_id
                                                        AND       lkp_agmt.agmt_type_cd = sq1.o_agmt_type_cd
                                                                    /*lkp_feat*/
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
                                                                            FROM     db_t_prod_core.feat 
                                                                            qualify row_number () over (PARTITION BY nk_src_key,feat_sbtype_cd ORDER BY edw_end_dttm DESC)=1 ) lkp_feat
                                                        ON        lkp_feat.feat_sbtype_cd = sq1.o_feat_sbtype_cd
                                                        AND       lkp_feat.nk_src_key = sq1.nk_public_id
                                                        WHERE     o_street_address_id IS NOT NULL
                                                                    /* loc_id is o_STREET_ADDRESS_ID */
                                    ) sq2 
                                /*where in_LOC_ID is not null*/
                                /*target lookup*/
                                left join
                                    (
                                                    SELECT DISTINCT agmt_feat_loctr.agmt_feat_loctr_role_cd   AS agmt_feat_loctr_role_cd,
                                                                    agmt_feat_loctr.agmt_feat_loctr_strt_dttm AS agmt_feat_loctr_strt_dttm,
                                                                    agmt_feat_loctr.agmt_feat_loctr_amt       AS agmt_feat_loctr_amt,
                                                                    agmt_feat_loctr.agmt_feat_loctr_end_dttm  AS agmt_feat_loctr_end_dttm,
                                                                    agmt_feat_loctr.edw_strt_dttm             AS edw_strt_dttm,
                                                                    agmt_feat_loctr.edw_end_dttm              AS edw_end_dttm,
                                                                    agmt_feat_loctr.agmt_id                   AS agmt_id,
                                                                    agmt_feat_loctr.feat_id                   AS feat_id,
                                                                    agmt_feat_loctr.loc_id                    AS loc_id
                                                    FROM            db_t_prod_core.agmt_feat_loctr 
                                                    qualify row_number() over(PARTITION BY agmt_id, feat_id, loc_id ORDER BY edw_end_dttm DESC) = 1 
                                        ) lkp_target 
                                        ON lkp_target.agmt_id = sq2.agmt_id
                                        AND lkp_target.feat_id = sq2.feat_id
                                        AND lkp_target.loc_id = sq2.o_street_address_id 
                        ) src                                     
            )
    );
 -- Component exp_md5_chk, Type EXPRESSION
CREATE
OR
replace TEMPORARY TABLE exp_md5_chk AS
(
       SELECT sq_pc_agmt_feat_loctr_x1.lkp_agmt_id                   AS lkp_agmt_id,
              sq_pc_agmt_feat_loctr_x1.lkp_feat_id                   AS lkp_feat_id,
              sq_pc_agmt_feat_loctr_x1.lkp_agmt_feat_loctr_role_cd   AS lkp_agmt_feat_loctr_role_cd,
              sq_pc_agmt_feat_loctr_x1.lkp_agmt_feat_loctr_strt_dttm AS lkp_agmt_feat_loctr_strt_dttm,
              sq_pc_agmt_feat_loctr_x1.lkp_loc_id                    AS lkp_loc_id,
              sq_pc_agmt_feat_loctr_x1.lkp_agmt_feat_loctr_amt       AS lkp_agmt_feat_loctr_amt,
              sq_pc_agmt_feat_loctr_x1.lkp_agmt_feat_loctr_end_dttm  AS lkp_agmt_feat_loctr_end_dttm,
              sq_pc_agmt_feat_loctr_x1.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
              sq_pc_agmt_feat_loctr_x1.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
              sq_pc_agmt_feat_loctr_x1.in_agmt_id                    AS in_agmt_id,
              sq_pc_agmt_feat_loctr_x1.in_feat_id                    AS in_feat_id,
              sq_pc_agmt_feat_loctr_x1.in_loc_id                     AS in_loc_id,
              sq_pc_agmt_feat_loctr_x1.in_agmt_feat_loctr_amt        AS in_agmt_feat_loctr_amt,
              sq_pc_agmt_feat_loctr_x1.in_agmt_feat_loctr_strt_dttm  AS in_agmt_feat_loctr_strt_dttm,
              sq_pc_agmt_feat_loctr_x1.in_agmt_feat_loctr_end_dttm   AS in_agmt_feat_loctr_end_dttm,
              sq_pc_agmt_feat_loctr_x1.o_prcs_id                     AS o_prcs_id,
              sq_pc_agmt_feat_loctr_x1.in_updatetime                 AS in_updatetime,
              sq_pc_agmt_feat_loctr_x1.agmt_feat_loctr_role_type_cd  AS agmt_feat_loctr_role_type_cd,
              sq_pc_agmt_feat_loctr_x1.o_flag                        AS o_flag,
              sq_pc_agmt_feat_loctr_x1.in_retired                    AS in_retired,
              sq_pc_agmt_feat_loctr_x1.source_record_id
       FROM   sq_pc_agmt_feat_loctr_x1 ); 
  -- Component rtr_agmt_feat_loctr_INSERT, Type ROUTER Output Group INSERT
CREATE
OR
replace TEMPORARY TABLE rtr_agmt_feat_loctr_insert AS
SELECT exp_md5_chk.lkp_agmt_id                   AS lkp_agmt_id,
       exp_md5_chk.lkp_feat_id                   AS lkp_feat_id,
       exp_md5_chk.lkp_agmt_feat_loctr_role_cd   AS lkp_agmt_feat_loctr_role_cd,
       exp_md5_chk.lkp_agmt_feat_loctr_strt_dttm AS lkp_agmt_feat_loctr_strt_dttm,
       exp_md5_chk.lkp_loc_id                    AS lkp_loc_id,
       exp_md5_chk.lkp_agmt_feat_loctr_amt       AS lkp_agmt_feat_loctr_amt,
       exp_md5_chk.lkp_agmt_feat_loctr_end_dttm  AS lkp_agmt_feat_loctr_end_dttm,
       exp_md5_chk.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
       exp_md5_chk.in_agmt_id                    AS in_agmt_id,
       exp_md5_chk.in_feat_id                    AS in_feat_id,
       exp_md5_chk.in_loc_id                     AS in_loc_id,
       exp_md5_chk.in_agmt_feat_loctr_amt        AS in_agmt_feat_loctr_amt,
       exp_md5_chk.in_agmt_feat_loctr_strt_dttm  AS in_agmt_feat_loctr_strt_dttm,
       exp_md5_chk.in_agmt_feat_loctr_end_dttm   AS in_agmt_feat_loctr_end_dttm,
       exp_md5_chk.o_prcs_id                     AS o_prcs_id,
       exp_md5_chk.o_flag                        AS o_flag,
       exp_md5_chk.in_updatetime                 AS in_updatetime,
       exp_md5_chk.in_retired                    AS in_retired,
       exp_md5_chk.lkp_edw_strt_dttm             AS lkp_edw_start_dttm,
       exp_md5_chk.agmt_feat_loctr_role_type_cd  AS in_agmt_feat_loctr_role_cd,
       exp_md5_chk.source_record_id
FROM   exp_md5_chk
WHERE  exp_md5_chk.o_flag = ''I''
AND    (
              exp_md5_chk.in_agmt_id IS NOT NULL
       AND    exp_md5_chk.in_feat_id IS NOT NULL
       AND    exp_md5_chk.in_loc_id IS NOT NULL )
OR     (
              exp_md5_chk.o_flag = ''U'' )
OR     (
              exp_md5_chk.in_retired = 0
       AND    exp_md5_chk.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) 
 /*- - > first INSERT INSERT incase OF CHANGE AND    now restored;*/
;

-- Component rtr_agmt_feat_loctr_RETIRED, Type ROUTER Output Group RETIRED
CREATE
OR
replace TEMPORARY TABLE rtr_agmt_feat_loctr_retired AS
SELECT exp_md5_chk.lkp_agmt_id                   AS lkp_agmt_id,
       exp_md5_chk.lkp_feat_id                   AS lkp_feat_id,
       exp_md5_chk.lkp_agmt_feat_loctr_role_cd   AS lkp_agmt_feat_loctr_role_cd,
       exp_md5_chk.lkp_agmt_feat_loctr_strt_dttm AS lkp_agmt_feat_loctr_strt_dttm,
       exp_md5_chk.lkp_loc_id                    AS lkp_loc_id,
       exp_md5_chk.lkp_agmt_feat_loctr_amt       AS lkp_agmt_feat_loctr_amt,
       exp_md5_chk.lkp_agmt_feat_loctr_end_dttm  AS lkp_agmt_feat_loctr_end_dttm,
       exp_md5_chk.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
       exp_md5_chk.in_agmt_id                    AS in_agmt_id,
       exp_md5_chk.in_feat_id                    AS in_feat_id,
       exp_md5_chk.in_loc_id                     AS in_loc_id,
       exp_md5_chk.in_agmt_feat_loctr_amt        AS in_agmt_feat_loctr_amt,
       exp_md5_chk.in_agmt_feat_loctr_strt_dttm  AS in_agmt_feat_loctr_strt_dttm,
       exp_md5_chk.in_agmt_feat_loctr_end_dttm   AS in_agmt_feat_loctr_end_dttm,
       exp_md5_chk.o_prcs_id                     AS o_prcs_id,
       exp_md5_chk.o_flag                        AS o_flag,
       exp_md5_chk.in_updatetime                 AS in_updatetime,
       exp_md5_chk.in_retired                    AS in_retired,
       exp_md5_chk.lkp_edw_strt_dttm             AS lkp_edw_start_dttm,
       exp_md5_chk.agmt_feat_loctr_role_type_cd  AS in_agmt_feat_loctr_role_cd,
       exp_md5_chk.source_record_id
FROM   exp_md5_chk
WHERE  exp_md5_chk.o_flag = ''R''
AND    exp_md5_chk.in_retired != 0
AND    exp_md5_chk.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) 
/*- - > NOT
INSERT
OR
UPDATE ,
       no CHANGE IN VALUES - - > but data IS retired - - >
UPDATE these records WITH current_timestamp
*/
;

-- Component upd_stg_upd_retire_rejected, Type UPDATE
CREATE
OR
replace TEMPORARY TABLE upd_stg_upd_retire_rejected AS
(
       /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
       SELECT rtr_agmt_feat_loctr_retired.lkp_agmt_id                  AS lkp_agmt_id3,
              rtr_agmt_feat_loctr_retired.lkp_feat_id                  AS lkp_feat_id3,
              rtr_agmt_feat_loctr_retired.lkp_loc_id                   AS lkp_loc_id3,
              rtr_agmt_feat_loctr_retired.lkp_agmt_feat_loctr_amt      AS lkp_agmt_feat_loctr_amt3,
              rtr_agmt_feat_loctr_retired.lkp_agmt_feat_loctr_role_cd  AS lkp_agmt_feat_loctr_role_cd3,
              rtr_agmt_feat_loctr_retired.lkp_edw_start_dttm           AS lkp_edw_start_dttm3,
              rtr_agmt_feat_loctr_retired.o_prcs_id                    AS o_prcs_id3,
              rtr_agmt_feat_loctr_retired.in_agmt_feat_loctr_strt_dttm AS in_agmt_feat_loctr_strt_dttm3,
              rtr_agmt_feat_loctr_retired.in_updatetime                AS in_updatetime3,
              1                                                        AS update_strategy_action,
              source_record_id
       FROM   rtr_agmt_feat_loctr_retired );
-- Component upd_stg_ins_upd, Type UPDATE
CREATE
OR
replace TEMPORARY TABLE upd_stg_ins_upd AS
(
       /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
       SELECT rtr_agmt_feat_loctr_insert.in_agmt_id                   AS in_agmt_id1,
              rtr_agmt_feat_loctr_insert.in_feat_id                   AS in_feat_id1,
              rtr_agmt_feat_loctr_insert.in_loc_id                    AS in_loc_id1,
              rtr_agmt_feat_loctr_insert.in_agmt_feat_loctr_amt       AS in_agmt_feat_loctr_amt1,
              rtr_agmt_feat_loctr_insert.in_agmt_feat_loctr_strt_dttm AS in_agmt_feat_loctr_strt_dttm1,
              rtr_agmt_feat_loctr_insert.in_agmt_feat_loctr_end_dttm  AS in_agmt_feat_loctr_end_dttm1,
              rtr_agmt_feat_loctr_insert.o_prcs_id                    AS o_prcs_id1,
              rtr_agmt_feat_loctr_insert.in_updatetime                AS in_updatetime1,
              NULL                                                    AS in_retired1,
              rtr_agmt_feat_loctr_insert.in_agmt_feat_loctr_role_cd   AS in_agmt_feat_loctr_role_cd1,
              0                                                       AS update_strategy_action,
              source_record_id
       FROM   rtr_agmt_feat_loctr_insert );
-- Component exp_pass_to_target_ins, Type EXPRESSION
CREATE
OR
replace TEMPORARY TABLE exp_pass_to_target_ins AS
(
       SELECT upd_stg_ins_upd.in_agmt_id1                                                    AS agmt_id,
              upd_stg_ins_upd.in_feat_id1                                                    AS feat_id,
              upd_stg_ins_upd.in_loc_id1                                                     AS loc_id,
              upd_stg_ins_upd.in_agmt_feat_loctr_amt1                                        AS agmt_feat_loctr_amt,
              upd_stg_ins_upd.in_agmt_feat_loctr_role_cd1                                    AS agmt_feat_loctr_role_cd,
              upd_stg_ins_upd.in_agmt_feat_loctr_strt_dttm1                                  AS agmt_feat_loctr_strt_dt,
              upd_stg_ins_upd.in_agmt_feat_loctr_end_dttm1                                   AS agmt_feat_loctr_end_dt,
              upd_stg_ins_upd.o_prcs_id1                                                     AS prcs_id,
              current_timestamp                                                              AS v_edw_strt_dttm,
              v_edw_strt_dttm                                                                AS o_edw_strt_dttm,
              to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS v_edw_end_dttm,
              v_edw_end_dttm                                                                 AS o_edw_end_dttm,
              upd_stg_ins_upd.in_updatetime1                                                 AS trans_strt_dttm,
              CASE
                     WHEN in_retired1 != 0 THEN upd_stg_ins_upd.in_updatetime1
                     ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
              END              AS v_trans_end_dttm,
              v_trans_end_dttm AS o_trans_end_dttm,
              upd_stg_ins_upd.source_record_id
       FROM   upd_stg_ins_upd );
       
-- Component tgt_AGMT_FEAT_LOCTR_ins_new, Type TARGET
INSERT INTO db_t_prod_core.agmt_feat_loctr
            (
                        agmt_id,
                        feat_id,
                        agmt_feat_loctr_role_cd,
                        agmt_feat_loctr_strt_dttm,
                        loc_id,
                        agmt_feat_loctr_amt,
                        agmt_feat_loctr_end_dttm,
                        prcs_id,
                        edw_strt_dttm,
                        edw_end_dttm,
                        trans_strt_dttm,
                        trans_end_dttm
            )
SELECT exp_pass_to_target_ins.agmt_id                 AS agmt_id,
       exp_pass_to_target_ins.feat_id                 AS feat_id,
       exp_pass_to_target_ins.agmt_feat_loctr_role_cd AS agmt_feat_loctr_role_cd,
       exp_pass_to_target_ins.agmt_feat_loctr_strt_dt AS agmt_feat_loctr_strt_dttm,
       exp_pass_to_target_ins.loc_id                  AS loc_id,
       exp_pass_to_target_ins.agmt_feat_loctr_amt     AS agmt_feat_loctr_amt,
       exp_pass_to_target_ins.agmt_feat_loctr_end_dt  AS agmt_feat_loctr_end_dttm,
       exp_pass_to_target_ins.prcs_id                 AS prcs_id,
       exp_pass_to_target_ins.o_edw_strt_dttm         AS edw_strt_dttm,
       exp_pass_to_target_ins.o_edw_end_dttm          AS edw_end_dttm,
       exp_pass_to_target_ins.trans_strt_dttm         AS trans_strt_dttm,
       exp_pass_to_target_ins.o_trans_end_dttm        AS trans_end_dttm
FROM   exp_pass_to_target_ins;

-- Component tgt_AGMT_FEAT_LOCTR_ins_new, Type Post SQL
UPDATE db_t_prod_core.agmt_feat_loctr
SET    trans_end_dttm= a.lead,
       edw_end_dttm=a.edw_lead
FROM   (
                       SELECT DISTINCT agmt_id,
                                       feat_id,
                                       loc_id,
                                       agmt_feat_loctr_role_cd ,
                                       edw_strt_dttm,
                                       max(trans_strt_dttm) over (PARTITION BY agmt_id,feat_id,loc_id,agmt_feat_loctr_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 SECOND'' AS lead,
                                       max(edw_strt_dttm) over (PARTITION BY agmt_id,feat_id,loc_id,agmt_feat_loctr_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 SECOND'' AS edw_lead
                       FROM            db_t_prod_core.agmt_feat_loctr ) a
WHERE  agmt_feat_loctr.edw_strt_dttm = a.edw_strt_dttm
AND    agmt_feat_loctr.agmt_id=a.agmt_id
AND    agmt_feat_loctr.feat_id=a.feat_id
AND    agmt_feat_loctr.loc_id=a.loc_id
AND    agmt_feat_loctr.agmt_feat_loctr_role_cd =a.agmt_feat_loctr_role_cd
AND    agmt_feat_loctr.trans_strt_dttm <>agmt_feat_loctr.trans_end_dttm
AND    lead IS NOT NULL;

-- Component exp_pass_to_target_upd__retire_rejected, Type EXPRESSION
CREATE
OR
replace TEMPORARY TABLE exp_pass_to_target_upd__retire_rejected AS
(
       SELECT upd_stg_upd_retire_rejected.lkp_agmt_id3                 AS agmt_id,
              upd_stg_upd_retire_rejected.lkp_feat_id3                 AS feat_id,
              upd_stg_upd_retire_rejected.lkp_loc_id3                  AS loc_id,
              upd_stg_upd_retire_rejected.lkp_agmt_feat_loctr_amt3     AS agmt_feat_loctr_amt,
              upd_stg_upd_retire_rejected.lkp_agmt_feat_loctr_role_cd3 AS agmt_feat_loctr_role_cd,
              current_timestamp                                        AS o_edw_end_dttm,
              upd_stg_upd_retire_rejected.lkp_edw_start_dttm3          AS edw_strt_dttm_upd3,
              --trans_strt_dttm4 
                in_updatetime3 AS o_trans_end_dttm,
              upd_stg_upd_retire_rejected.source_record_id
       FROM   upd_stg_upd_retire_rejected );

-- Component tgt_AGMT_FEAT_LOCTR_upd_retire_rejected, Type TARGET
merge INTO  db_t_prod_core.agmt_feat_loctr
USING        exp_pass_to_target_upd__retire_rejected
ON (
                          agmt_feat_loctr.agmt_id = exp_pass_to_target_upd__retire_rejected.agmt_id
             AND          agmt_feat_loctr.feat_id = exp_pass_to_target_upd__retire_rejected.feat_id
             AND          agmt_feat_loctr.loc_id = exp_pass_to_target_upd__retire_rejected.loc_id)
WHEN matched THEN
UPDATE
SET    agmt_id = exp_pass_to_target_upd__retire_rejected.agmt_id,
       feat_id = exp_pass_to_target_upd__retire_rejected.feat_id,
       agmt_feat_loctr_role_cd = exp_pass_to_target_upd__retire_rejected.agmt_feat_loctr_role_cd,
       loc_id = exp_pass_to_target_upd__retire_rejected.loc_id,
       agmt_feat_loctr_amt = exp_pass_to_target_upd__retire_rejected.agmt_feat_loctr_amt,
       edw_strt_dttm = exp_pass_to_target_upd__retire_rejected.edw_strt_dttm_upd3,
       edw_end_dttm = exp_pass_to_target_upd__retire_rejected.o_edw_end_dttm,
       trans_end_dttm = exp_pass_to_target_upd__retire_rejected.o_trans_end_dttm
       ;
END;
';