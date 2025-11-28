-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_DTL_CODE_XREF_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
  START_DTTM STRING;
  V_ASSET_DTL_CD STRING;
  V_IN_MD5 STRING;
  V_LKP_MD5 STRING;
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
  V_ASSET_DTL_CD := public.func_get_scoped_param(:run_id, ''v_asset_dtl_cd'', :workflow_name, :worklet_name, :session_name);
  V_IN_MD5 := public.func_get_scoped_param(:run_id, ''v_in_md5'', :workflow_name, :worklet_name, :session_name);
  V_LKP_MD5 := public.func_get_scoped_param(:run_id, ''v_lkp_md5'', :workflow_name, :worklet_name, :session_name);

  -- PIPELINE START FOR 1
  -- Component sq_pcx_dwelling_hoe, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_dwelling_hoe AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS prty_asset_id,
                $2  AS agmt_id,
                $3  AS out_asset_dtl_cd,
                $4  AS agmt_asset_dtl_xref_strt_dttm,
                $5  AS agmt_asset_dtl_xref_end_dttm,
                $6  AS ind,
                $7  AS trans_strt_dttm,
                $8  AS trans_end_dttm,
                $9  AS asset_dtl_txt,
                $10 AS ratingunitorigdate_alfa,
                $11 AS ins_upd_flag,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT sq_final.prty_asset_id,
                                                sq_final.agmt_id,
                                                sq_final.out_asset_dtl_cd,
                                                sq_final.agmt_asset_dtl_xref_strt_dttm,
                                                sq_final.agmt_asset_dtl_xref_end_dttm,
                                                sq_final.ind,
                                                sq_final.trans_strt_dttm,
                                                sq_final.trans_end_dttm,
                                                sq_final.asset_dtl_txt,
                                                sq_final.ratingunitorigdate_alfa,
                                                sq_final.ins_upd_flag
                                         FROM  (
                                                                SELECT DISTINCT sq1.fixedid,
                                                                                sq1.typecode,
                                                                                sq1.classification_code,
                                                                                sq1.asset_dtl_cd,
                                                                                sq1.agmt_asset_dtl_xref_strt_dttm,
                                                                                sq1.agmt_asset_dtl_xref_end_dttm,
                                                                                sq1.publicid,
                                                                                sq1.trans_strt_dttm,
                                                                                sq1.trans_end_dttm,
                                                                                sq1.asset_dtl_txt,
                                                                                sq1.ind,
                                                                                sq1.classcode_publicid,
                                                                                sq1.ratingunitorigdate_alfa ,
                                                                                sq1.out_prty_asset_sbtype_cd ,
                                                                                sq1.out_class_cd ,
                                                                                /* ,SQ1.in_ASSET_DTL_CD, */
                                                                                CASE
                                                                                                WHEN xlat_asset_dtl_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                ELSE xlat_asset_dtl_cd.tgt_idntftn_val
                                                                                END                                                      AS out_asset_dtl_cd ,
                                                                                lkp_dpa.prty_asset_id                                    AS prty_asset_id ,
                                                                                lkp_agmt.agmt_id                                         AS agmt_id ,
                                                                                tgt_agmt_asset_dtl_cd_xref.prty_asset_id                 AS tgt_prty_asset_id ,
                                                                                tgt_agmt_asset_dtl_cd_xref.agmt_id                       AS tgt_agmt_id ,
                                                                                tgt_agmt_asset_dtl_cd_xref.asset_dtl_cd                  AS tgt_asset_dtl_cd ,
                                                                                tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_strt_dttm AS tgt_agmt_asset_dtl_xref_strt_dttm ,
                                                                                tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_end_dttm  AS tgt_agmt_asset_dtl_xref_end_dttm ,
                                                                                tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_txt            AS tgt_agmt_asset_dtl_txt ,
                                                                                tgt_agmt_asset_dtl_cd_xref.asset_dtl_cd_ind              AS tgt_asset_dtl_cd_ind ,
                                                                                tgt_agmt_asset_dtl_cd_xref.ratingunitorigdate_alfa       AS tgt_ratingunitorigdate_alfa
                                                                                /* SOURCEMD5DATA */
                                                                                ,
                                                                                cast(concat(coalesce(trim(sq1.asset_dtl_txt),''''), coalesce(trim(out_asset_dtl_cd), ''''), coalesce(trim(cast(sq1.agmt_asset_dtl_xref_strt_dttm AS VARCHAR(100))), ''''), coalesce(trim(cast(sq1.agmt_asset_dtl_xref_end_dttm AS VARCHAR(100))), ''''), coalesce(trim(cast(sq1.ratingunitorigdate_alfa AS VARCHAR(100))), '''')) AS VARCHAR(1000)) AS sourcedata
                                                                                /* TARGETMD5DATA*/
                                                                                ,
                                                                                cast(concat(coalesce(trim(tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_txt), ''''), coalesce(trim(tgt_agmt_asset_dtl_cd_xref.asset_dtl_cd),''''), coalesce(trim(cast(tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_strt_dttm AS VARCHAR(100))), ''''), coalesce(trim(cast(tgt_agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_end_dttm AS VARCHAR(100))), ''''), coalesce(trim(cast(tgt_agmt_asset_dtl_cd_xref.ratingunitorigdate_alfa AS VARCHAR(100))), '''')) AS VARCHAR(1000)) AS targetdata
                                                                                /* FLAG*/
                                                                                ,
                                                                                CASE
                                                                                                WHEN trim(targetdata) IS NULL
                                                                                                OR              length(targetdata) = 0 THEN ''I''
                                                                                                WHEN trim(targetdata) <> trim(sourcedata) THEN ''U''
                                                                                                ELSE ''R''
                                                                                END AS ins_upd_flag
                                                                FROM            (
                                                                                                SELECT DISTINCT sq.fixedid                                                                                 AS fixedid,
                                                                                                                sq.typecode                                                                                AS typecode,
                                                                                                                sq.classification_code                                                                     AS classification_code,
                                                                                                                sq.asset_dtl_cd                                                                            AS asset_dtl_cd,
                                                                                                                coalesce(sq.agmt_asset_dtl_xref_strt_dttm,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                                /* SQ.AGMT_ASSET_DTL_XREF_STRT_DTTM AS AGMT_ASSET_DTL_XREF_STRT_DTTM,*/
                                                                                                                sq.agmt_asset_dtl_xref_end_dttm AS agmt_asset_dtl_xref_end_dttm,
                                                                                                                sq.publicid                     AS publicid,
                                                                                                                sq.trans_strt_dttm              AS trans_strt_dttm,
                                                                                                                sq.trans_end_dttm               AS trans_end_dttm,
                                                                                                                sq.asset_dtl_txt                AS asset_dtl_txt,
                                                                                                                sq.ind                          AS ind,
                                                                                                                sq.classcode_publicid           AS classcode_publicid,
                                                                                                                sq.ratingunitorigdate_alfa      AS ratingunitorigdate_alfa ,
                                                                                                                CASE
                                                                                                                                WHEN xlat_prty_asset_sbtype_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                                                ELSE xlat_prty_asset_sbtype_cd.tgt_idntftn_val
                                                                                                                END AS out_prty_asset_sbtype_cd ,
                                                                                                                CASE
                                                                                                                                WHEN xlat_class_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                                                ELSE xlat_class_cd.tgt_idntftn_val
                                                                                                                END AS out_class_cd
                                                                                                                /* ,Case When SQ.IND = ''CC'' THEN SQ.CLASSCODE_PUBLICID ELSE SQ.ASSET_DTL_CD END as in_ASSET_DTL_CD*/
                                                                                                                ,
                                                                                                                CASE
                                                                                                                                WHEN sq.ind IN(''PROP'',
                                                                                                                                               ''TERR'') THEN sq.asset_dtl_cd
                                                                                                                                WHEN sq.ind = ''CC'' THEN sq.classcode_publicid
                                                                                                                                ELSE sq.asset_dtl_cd
                                                                                                                END AS in_asset_dtl_cd
                                                                                                FROM            (
                                                                                                                         SELECT   fixedid,
                                                                                                                                  typecode,
                                                                                                                                  classification_code,
                                                                                                                                  asset_dtl_cd,
                                                                                                                                  agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                  agmt_asset_dtl_xref_end_dttm,
                                                                                                                                  publicid,
                                                                                                                                  trans_strt_dttm,
                                                                                                                                  trans_end_dttm,
                                                                                                                                  asset_dtl_txt,
                                                                                                                                  ind,
                                                                                                                                  classcode_publicid,
                                                                                                                                  ratingunitorigdate_alfa
                                                                                                                         FROM     (
                                                                                                                                                  /* Class Code & Property query*/
                                                                                                                                                  SELECT DISTINCT pcx_bp7agmt_asset_dtl_code.classificationfixedid                                  AS fixedid,
                                                                                                                                                                  ''PRTY_ASSET_SBTYPE13''                                                             AS typecode,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.classificationcode                                     AS classification_code,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.industryclasscode                                      AS asset_dtl_cd,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationcreatetime AS timestamp)            AS agmt_asset_dtl_xref_strt_dttm ,
                                                                                                                                                                  cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                                  AS agmt_asset_dtl_xref_end_dttm,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.ppv_publicid                                           AS publicid,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationupdatetime AS timestamp)            AS trans_strt_dttm,
                                                                                                                                                                  cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                                  AS trans_end_dttm,
                                                                                                                                                                  coalesce(cast(pcx_bp7agmt_asset_dtl_code.territorycode_alfa AS VARCHAR(48)), '' '') AS asset_dtl_txt,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.ind AS VARCHAR(50))                               AS ind,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.classcode_publicid                                     AS classcode_publicid,
                                                                                                                                                                  cast(NULL AS timestamp)                                                           AS ratingunitorigdate_alfa
                                                                                                                                                  FROM            (
                                                                                                                                                                                  /* CLASS CODE QUERY*/
                                                                                                                                                                                  SELECT DISTINCT c.fixedid_stg       AS classificationfixedid ,
                                                                                                                                                                                                  b.fixedid_stg       AS buildingfixedid ,
                                                                                                                                                                                                  pp.policynumber_stg AS policynumber ,
                                                                                                                                                                                                  pp.publicid_stg     AS ppv_publicid ,
                                                                                                                                                                                                  c.branchid_stg      AS classificationbranchid ,
                                                                                                                                                                                                  c.createtime_stg    AS classificationcreatetime ,
                                                                                                                                                                                                  c.effectivedate_stg AS classificationeffectivedate ,
                                                                                                                                                                                                  CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        cast(c.updatetime_stg AS timestamp) >= cast(cc.updatetime_stg AS timestamp)) THEN cast(c.updatetime_stg AS timestamp)
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        cast(cc.updatetime_stg AS timestamp)>= cast(c.updatetime_stg AS timestamp) ) THEN cast(cc.updatetime_stg AS timestamp)
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        coalesce(cast(cc.updatetime_stg AS DATE),cast(''1900-12-31'' AS DATE)) >=coalesce(cast(c.updatetime_stg AS DATE), cast(''1900-12-31'' AS DATE))) THEN cast(cc.updatetime_stg AS timestamp)
                                                                                                                                                                                                  END                                       AS classificationupdatetime ,
                                                                                                                                                                                                  c.expirationdate_stg                      AS classificationexpirationdate ,
                                                                                                                                                                                                  cc.effectivedate_stg                      AS classcodeeffectivedate ,
                                                                                                                                                                                                  cc.expirationdate_stg                     AS classcodeexpirationdate ,
                                                                                                                                                                                                  cc.code_stg                               AS industryclasscode ,
                                                                                                                                                                                                  cast(cc.propertytype_stg AS VARCHAR(255)) AS classcodepropertytype ,
                                                                                                                                                                                                  cc.description_stg                        AS description ,
                                                                                                                                                                                                  cp.typecode_stg                           AS classificationcode ,
                                                                                                                                                                                                  (:start_dttm)                             AS start_dttm ,
                                                                                                                                                                                                  (:end_dttm)                               AS end_dttm ,
                                                                                                                                                                                                  cast(''CC'' AS            VARCHAR(100))                AS ind ,
                                                                                                                                                                                                  cast(NULL AS            VARCHAR(255))                AS territorycode_alfa ,
                                                                                                                                                                                                  cast(cc.publicid_stg AS VARCHAR(255))                AS classcode_publicid
                                                                                                                                                                                  FROM            db_t_prod_stag.pcx_bp7classification c
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                  ON              pp.id_stg = c.branchid_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                                                                                                                                  ON              cp.id_stg = c.bp7classpropertytype_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_bp7classdescription cd_cls
                                                                                                                                                                                  ON              cd_cls.id_stg = c.bp7classdescription_stg
                                                                                                                                                                                  inner join
                                                                                                                                                                                                  (
                                                                                                                                                                                                        SELECT publicid_stg,
                                                                                                                                                                                                        code_stg,
                                                                                                                                                                                                        propertytype_stg,
                                                                                                                                                                                                        effectivedate_stg,
                                                                                                                                                                                                        updatetime_stg,
                                                                                                                                                                                                        description_stg,
                                                                                                                                                                                                        expirationdate_stg
                                                                                                                                                                                                        FROM   db_t_prod_stag.pcx_bp7classcode
                                                                                                                                                                                                        WHERE  publicid_stg IN
                                                                                                                                                                                                        (
                                                                                                                                                                                                        SELECT DISTINCT publicid
                                                                                                                                                                                                        FROM            (
                                                                                                                                                                                                        SELECT   max(publicid_stg) publicid,
                                                                                                                                                                                                        propertytype_stg,
                                                                                                                                                                                                        description_stg
                                                                                                                                                                                                        FROM     db_t_prod_stag.pcx_bp7classcode
                                                                                                                                                                                                        GROUP BY propertytype_stg,
                                                                                                                                                                                                        description_stg )a ) ) cc
                                                                                                                                                                                  ON              cc.description_stg COLLATE ''en-ci'' = cd_cls.description_stg COLLATE ''en-ci''
                                                                                                                                                                                  AND             cc.propertytype_stg = cp.name_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pcx_bp7building b
                                                                                                                                                                                  ON              b.fixedid_stg = c.building_stg
                                                                                                                                                                                  AND             b.branchid_stg = c.branchid_stg
                                                                                                                                                                                  WHERE           pp.status_stg = 9
                                                                                                                                                                                  AND             pp.mostrecentmodel_stg = 1
                                                                                                                                                                                  AND             cancellationdate_stg IS NULL
                                                                                                                                                                                  AND             c.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             (
                                                                                                                                                                                                        c.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                  AND             c.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                  /* PROPERTY QUERY*/
                                                                                                                                                                                  UNION ALL
                                                                                                                                                                                  SELECT DISTINCT c.fixedid_stg                         AS classificationfixedid ,
                                                                                                                                                                                                  b.fixedid_stg                         AS buildingfixedid ,
                                                                                                                                                                                                  pp.policynumber_stg                   AS policynumber ,
                                                                                                                                                                                                  pp.publicid_stg                       AS ppv_publicid ,
                                                                                                                                                                                                  c.branchid_stg                        AS classificationbranchid ,
                                                                                                                                                                                                  c.createtime_stg                      AS classificationcreatetime ,
                                                                                                                                                                                                  c.effectivedate_stg                   AS classificationeffectivedate ,
                                                                                                                                                                                                  c.updatetime_stg                      AS classificationupdatetime ,
                                                                                                                                                                                                  c.expirationdate_stg                  AS classificationexpirationdate ,
                                                                                                                                                                                                  cc.effectivedate_stg                  AS classcodeeffectivedate ,
                                                                                                                                                                                                  cc.expirationdate_stg                 AS classcodeexpirationdate ,
                                                                                                                                                                                                  pt.name_stg                           AS indusclass_propertycode ,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255))            AS classcodepropertytype ,
                                                                                                                                                                                                  pt.name_stg                           AS description ,
                                                                                                                                                                                                  cast(cp.typecode_stg AS VARCHAR(255)) AS classificationcode ,
                                                                                                                                                                                                  (:start_dttm)                         AS start_dttm ,
                                                                                                                                                                                                  (:end_dttm)                           AS end_dttm ,
                                                                                                                                                                                                  ''PROP''                                AS ind ,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255))            AS territorycode_alfa ,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255))            AS classcode_publicid
                                                                                                                                                                                  FROM            db_t_prod_stag.pcx_bp7classification c
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                  ON              pp.id_stg = c.branchid_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                                                                                                                                  ON              cp.id_stg = c.bp7classpropertytype_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_bp7classdescription cd_cls
                                                                                                                                                                                  ON              cd_cls.id_stg = c.bp7classdescription_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pcx_bp7classcode cc
                                                                                                                                                                                  ON              cc.description_stg COLLATE ''en-ci'' = cd_cls.description_stg  COLLATE ''en-ci''
                                                                                                                                                                                  AND             cc.propertytype_stg = cp.name_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pcx_bp7building b
                                                                                                                                                                                  ON              b.fixedid_stg = c.building_stg
                                                                                                                                                                                  AND             b.branchid_stg = c.branchid_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_bp7propertytype pt
                                                                                                                                                                                  ON              cp.typecode_stg=pt.typecode_stg
                                                                                                                                                                                  WHERE           pp.status_stg = 9
                                                                                                                                                                                  AND             pp.mostrecentmodel_stg = 1
                                                                                                                                                                                  AND             cancellationdate_stg IS NULL
                                                                                                                                                                                  AND             c.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             (
                                                                                                                                                                                                        c.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                  AND             c.updatetime_stg <= (:end_dttm)) ) pcx_bp7agmt_asset_dtl_code
                                                                                                                                                  WHERE           industryclasscode IS NOT NULL
                                                                                                                                                  AND             ind IN (''CC'',
                                                                                                                                                                          ''PROP'')
                                                                                                                                                  UNION ALL
                                                                                                                                                  /* eim-17404 AUTO query*/
                                                                                                                                                  SELECT DISTINCT pcx_bp7agmt_asset_dtl_code.classificationfixedid                                   AS fixedid,
                                                                                                                                                                  ''PRTY_ASSET_SBTYPE4''                                                               AS typecode ,
                                                                                                                                                                  ''PRTY_ASSET_CLASFCN3''                                                              AS classification_code,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.industryclasscode                                       AS asset_dtl_cd,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationeffectivedate AS timestamp)             agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationexpirationdate AS timestamp)            agmt_asset_dtl_xref_end_dttm,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.ppv_publicid                                            AS publicid,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationupdatetime AS timestamp)             AS trans_strt_dttm,
                                                                                                                                                                  cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                                   AS trans_end_dttm,
                                                                                                                                                                  coalesce(cast(pcx_bp7agmt_asset_dtl_code.territorycode_alfa AS VARCHAR(48)), '' '')  AS asset_dtl_txt ,
                                                                                                                                                                  ''TERR''                                                                             AS ind ,
                                                                                                                                                                  coalesce(cast(pcx_bp7agmt_asset_dtl_code.classcode_publicid AS VARCHAR(100)), '' '') AS classcode_publicid,
                                                                                                                                                                  cast(NULL AS timestamp)                                                            AS ratingunitorigdate_alfa
                                                                                                                                                  FROM            (
                                                                                                                                                                                  /*AUTO QUERY*/
                                                                                                                                                                                  SELECT DISTINCT c.fixedid_stg                                     AS classificationfixedid ,
                                                                                                                                                                                                  pl.fixedid_stg                                    AS buildingfixedid ,
                                                                                                                                                                                                  pp.policynumber_stg                               AS policynumber ,
                                                                                                                                                                                                  pp.publicid_stg                                   AS ppv_publicid ,
                                                                                                                                                                                                  c.branchid_stg                                    AS classificationbranchid ,
                                                                                                                                                                                                  c.createtime_stg                                  AS classificationcreatetime ,
                                                                                                                                                                                                  c.effectivedate_stg                               AS classificationeffectivedate ,
                                                                                                                                                                                                  c.updatetime_stg                                  AS classificationupdatetime ,
                                                                                                                                                                                                  c.expirationdate_stg                              AS classificationexpirationdate ,
                                                                                                                                                                                                  ptc.effectivedate_stg                             AS classcodeeffectivedate ,
                                                                                                                                                                                                  ptc.expirationdate_stg                            AS classcodeexpirationdate ,
                                                                                                                                                                                                  pctl.typecode_stg                                 AS industryclasscode ,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255))                        AS classcodepropertytype ,
                                                                                                                                                                                                  pctl.name_stg                                     AS description ,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255))                        AS classificationcode ,
                                                                                                                                                                                                  (:start_dttm)                                     AS start_dttm ,
                                                                                                                                                                                                  (:end_dttm)                                       AS end_dttm ,
                                                                                                                                                                                                  ''TERR_PPV''                                        AS ind ,
                                                                                                                                                                                                  cast(pl.alterritorycode_alfa_stg AS VARCHAR(255)) AS territorycode_alfa ,
                                                                                                                                                                                                  cast(NULL AS                        VARCHAR(255)) AS classcode_publicid
                                                                                                                                                                                  FROM            db_t_prod_stag.pc_personalvehicle c
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                  ON              pp.id_stg = c.branchid_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_policylocation pl
                                                                                                                                                                                  ON              c.garagelocation_stg =pl.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_territorycode ptc
                                                                                                                                                                                  ON              pl.id_stg=ptc.policylocation_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_territorycode pctl
                                                                                                                                                                                  ON              pctl.id_stg=ptc.subtype_stg
                                                                                                                                                                                  WHERE           pp.status_stg = 9
                                                                                                                                                                                  AND             c.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pl.alterritorycode_alfa_stg IS NOT NULL
                                                                                                                                                                                  AND             (
                                                                                                                                                                                                        c.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                  AND             c.updatetime_stg <= (:end_dttm)) )pcx_bp7agmt_asset_dtl_code
                                                                                                                                                  WHERE           pcx_bp7agmt_asset_dtl_code.industryclasscode IS NOT NULL
                                                                                                                                                  AND             ind=''TERR_PPV'' ) AS b qualify row_number () over ( PARTITION BY b.fixedid, b.publicid,b.classification_code,b.ind ORDER BY b.agmt_asset_dtl_xref_strt_dttm DESC)=1
                                                                                                                         UNION ALL
                                                                                                                         SELECT   fixedid,
                                                                                                                                  typecode,
                                                                                                                                  classification_code,
                                                                                                                                  asset_dtl_cd,
                                                                                                                                  agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                  agmt_asset_dtl_xref_end_dttm,
                                                                                                                                  publicid,
                                                                                                                                  trans_strt_dttm,
                                                                                                                                  trans_end_dttm,
                                                                                                                                  asset_dtl_txt,
                                                                                                                                  ind,
                                                                                                                                  classcode_publicid,
                                                                                                                                  ratingunitorigdate_alfa
                                                                                                                         FROM     (
                                                                                                                                                  /* EIM-35986 Incorporated Robin Comments as Per EIM-22873*/
                                                                                                                                                  SELECT DISTINCT pcx_bp7agmt_asset_dtl_code.classificationfixedid                                   AS fixedid,
                                                                                                                                                                  cast(''PRTY_ASSET_SBTYPE5'' AS  VARCHAR(100))                                        AS typecode ,
                                                                                                                                                                  cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(100))                                        AS classification_code,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.industryclasscode                                       AS asset_dtl_cd,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationeffectivedate AS timestamp)             agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationexpirationdate AS timestamp)            agmt_asset_dtl_xref_end_dttm,
                                                                                                                                                                  pcx_bp7agmt_asset_dtl_code.ppv_publicid                                            AS publicid,
                                                                                                                                                                  cast(pcx_bp7agmt_asset_dtl_code.classificationupdatetime AS timestamp)             AS trans_strt_dttm,
                                                                                                                                                                  cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                                   AS trans_end_dttm,
                                                                                                                                                                  coalesce(cast(pcx_bp7agmt_asset_dtl_code.territorycode_alfa AS VARCHAR(48)), '' '')  AS asset_dtl_txt,
                                                                                                                                                                  ''TERR''                                                                             AS ind ,
                                                                                                                                                                  coalesce(cast(pcx_bp7agmt_asset_dtl_code.classcode_publicid AS VARCHAR(100)), '' '') AS classcode_publicid,
                                                                                                                                                                  cast(NULL AS timestamp)                                                            AS ratingunitorigdate_alfa
                                                                                                                                                  FROM            (
                                                                                                                                                                                  SELECT DISTINCT pcx_dwelling_hoe.fixedid_stg                    AS classificationfixedid ,
                                                                                                                                                                                                  pc_policyperiod.policynumber_stg                AS policynumber ,
                                                                                                                                                                                                  pc_policyperiod.publicid_stg                    AS ppv_publicid ,
                                                                                                                                                                                                  pcx_dwelling_hoe.effectivedate_stg              AS classificationeffectivedate ,
                                                                                                                                                                                                  pcx_dwelling_hoe.updatetime_stg                 AS classificationupdatetime ,
                                                                                                                                                                                                  pcx_dwelling_hoe.expirationdate_stg             AS classificationexpirationdate ,
                                                                                                                                                                                                  pc_territorycode.effectivedate_stg              AS classcodeeffectivedate ,
                                                                                                                                                                                                  pc_territorycode.expirationdate_stg             AS classcodeexpirationdate ,
                                                                                                                                                                                                  pctl_territorycode.typecode_stg                 AS industryclasscode ,
                                                                                                                                                                                                  pctl_territorycode.name_stg                     AS description ,
                                                                                                                                                                                                  ''TERR_HO''                                       AS ind ,
                                                                                                                                                                                                  cast(pc_territorycode.code_stg AS VARCHAR(255)) AS territorycode_alfa ,
                                                                                                                                                                                                  cast(NULL AS                      VARCHAR(255)) AS classcode_publicid
                                                                                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                                                                                  join            db_t_prod_stag.pc_territorycode
                                                                                                                                                                                  ON              pc_territorycode.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                  join            db_t_prod_stag.pctl_territorycode pctl_territorycode
                                                                                                                                                                                  ON              pctl_territorycode.id_stg=pc_territorycode.subtype_stg
                                                                                                                                                                                  join            db_t_prod_stag.pc_policylocation pc_policylocation
                                                                                                                                                                                  ON              pc_policylocation.fixedid_stg = pc_territorycode.policylocation_stg
                                                                                                                                                                                  AND             pc_policylocation.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                                  join            db_t_prod_stag.pcx_holocation_hoe pcx_holocation_hoe
                                                                                                                                                                                  ON              pcx_holocation_hoe.policylocation_stg = pc_territorycode.policylocation_stg
                                                                                                                                                                                  AND             pcx_holocation_hoe.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                                  join            db_t_prod_stag.pcx_dwelling_hoe pcx_dwelling_hoe
                                                                                                                                                                                  ON              pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.fixedid_stg
                                                                                                                                                                                  AND             pcx_dwelling_hoe.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                                  WHERE           pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pc_policylocation.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pc_territorycode.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pcx_holocation_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg>(:start_dttm)
                                                                                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg <= (:end_dttm)
                                                                                                                                                                                  AND             pc_policyperiod.status_stg = 9 )pcx_bp7agmt_asset_dtl_code
                                                                                                                                                  WHERE           pcx_bp7agmt_asset_dtl_code.industryclasscode IS NOT NULL
                                                                                                                                                  AND             ind=''TERR_HO''
                                                                                                                                                  UNION ALL
                                                                                                                                                  /* EIM-35986 Adding New union to populate BP7TC records*/
                                                                                                                                                  SELECT DISTINCT b.fixedid                                                AS fixedid,
                                                                                                                                                                  cast(''PRTY_ASSET_SBTYPE32'' AS  VARCHAR(50))              AS typecode,
                                                                                                                                                                  cast(''PRTY_ASSET_CLASFCN10'' AS VARCHAR(50))              AS classification_code,
                                                                                                                                                                  b.typecode_territorycode                                 AS asset_dtl_cd,
                                                                                                                                                                  cast(b.effectivedate AS timestamp)                       AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                                                  cast(b.expirationdate AS timestamp)                      AS agmt_asset_dtl_xref_end_dttm,
                                                                                                                                                                  b.ppv_publicid                                           AS publicid,
                                                                                                                                                                  cast(b.updatetime AS timestamp)                          AS trans_strt_dttm,
                                                                                                                                                                  cast( ''9999-12-31 23:59:59.999999'' AS timestamp)         AS trans_end_dttm,
                                                                                                                                                                  coalesce(cast(b.code AS VARCHAR(48)),'' '')                AS asset_dtl_txt,
                                                                                                                                                                  ''TERR''                                                   AS ind,
                                                                                                                                                                  coalesce(cast(b.classcode_publicid AS VARCHAR(100)),'' '') AS classcode_publicid,
                                                                                                                                                                  cast(NULL AS timestamp)                                  AS ratingunitorigdate_alfa
                                                                                                                                                  FROM            (
                                                                                                                                                                                  SELECT DISTINCT a.fixedid_stg              AS fixedid,
                                                                                                                                                                                                  pp.policynumber_stg        AS policynumber,
                                                                                                                                                                                                  pp.publicid_stg            AS ppv_publicid ,
                                                                                                                                                                                                  a.effectivedate_stg        AS effectivedate ,
                                                                                                                                                                                                  a.updatetime_stg           AS updatetime ,
                                                                                                                                                                                                  a.expirationdate_stg       AS expirationdate ,
                                                                                                                                                                                                  tc.code_stg                AS code ,
                                                                                                                                                                                                  pctl.typecode_stg          AS typecode_territorycode,
                                                                                                                                                                                                  cast(NULL AS VARCHAR(255)) AS classcode_publicid,
                                                                                                                                                                                                  ''TERR_BO''                  AS ind
                                                                                                                                                                                  FROM            db_t_prod_stag.pcx_bp7building a
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_building b
                                                                                                                                                                                  ON              b.fixedid_stg = a.building_stg
                                                                                                                                                                                  AND             b.branchid_stg = a.branchid_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                  ON              pp.id_stg = a.branchid_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_territorycode tc
                                                                                                                                                                                  ON              tc.branchid_stg = pp.id_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_territorycode pctl
                                                                                                                                                                                  ON              pctl.id_stg=tc.subtype_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policylocation pl
                                                                                                                                                                                  ON              pl.fixedid_stg = tc.policylocation_stg
                                                                                                                                                                                  AND             pl.branchid_stg = pp.id_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policy p
                                                                                                                                                                                  ON              p.id_stg = pp.policyid_stg
                                                                                                                                                                                  inner join
                                                                                                                                                                                                  (
                                                                                                                                                                                                        SELECT   l.*,
                                                                                                                                                                                                        rank() over ( PARTITION BY l.fixedid_stg, l.branchid_stg ORDER BY l.updatetime_stg DESC) r
                                                                                                                                                                                                        FROM     db_t_prod_stag.pcx_bp7location l) l
                                                                                                                                                                                  ON              a.location_stg = l.fixedid_stg
                                                                                                                                                                                  AND             l.r = 1
                                                                                                                                                                                  AND             l.location_stg=tc.policylocation_stg
                                                                                                                                                                                  AND             l.branchid_stg=pp.id_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pc_policyline pol
                                                                                                                                                                                  ON              pol.branchid_stg = pp.id_stg
                                                                                                                                                                                  WHERE           a.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             b.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             l.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pl.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             ((
                                                                                                                                                                                                        a.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             a.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        b.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             b.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        l.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             l.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                  AND             pctl.name_stg=''BP7TerritoryCode_alfa''
                                                                                                                                                                                  AND             pp.status_stg = 9 )b
                                                                                                                                                  WHERE           b.typecode_territorycode IS NOT NULL
                                                                                                                                                  AND             ind=''TERR_BO'' ) AS b qualify row_number () over ( PARTITION BY b.fixedid, b.publicid,b.classification_code,b.ind ORDER BY b.trans_strt_dttm DESC)=1
                                                                                                                         UNION ALL
                                                                                                                         SELECT DISTINCT cast(ltrim(rtrim(tmp.id)) AS bigint) AS fixedid ,
                                                                                                                                         tmp.assettype                        AS typecode ,
                                                                                                                                         tmp.classification_code ,
                                                                                                                                         ''ASSET_DTL_TYPE13''                               AS asset_dtl_cd,
                                                                                                                                         cast( tmp.effectivedate AS timestamp)            AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         cast( tmp.expirationdate AS timestamp)           AS agmt_asset_dtl_xref_end_dttm,
                                                                                                                                         tmp.publicid                                     AS publicid,
                                                                                                                                         cast(tmp.updatetime AS timestamp)                AS trans_strt_dttm,
                                                                                                                                         cast( ''9999-12-31 23:59:59.999999'' AS timestamp) AS trans_end_dttm,
                                                                                                                                         cast(NULL AS VARCHAR(4))                            asset_dtl_txt,
                                                                                                                                         cast(NULL AS VARCHAR(4))                         AS ind,
                                                                                                                                         cast(NULL AS VARCHAR(4))                         AS classcode_publicid,
                                                                                                                                         tmp.ratingunitorigdate_alfa
                                                                                                                                         /* RUOD auto*/
                                                                                                                         FROM            (
                                                                                                                                                         /** DB_T_CORE_PROD.VEHICLE **/
                                                                                                                                                         /**watercraftmotor**/
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg                                    AS publicid ,
                                                                                                                                                                         cast(pamotor.fixedid_stg AS VARCHAR(100))          AS id ,
                                                                                                                                                                         pv.ratingunitorigdate_alfa_stg                     AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(pv.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(pv.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         cast(''PRTY_ASSET_SBTYPE4'' AS  VARCHAR(100))         AS assettype ,
                                                                                                                                                                         cast(''PRTY_ASSET_CLASFCN4'' AS VARCHAR(100))         AS classification_code,
                                                                                                                                                                         pv.updatetime_stg                                   AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_pawatercraftmotor_alfa pamotor
                                                                                                                                                         join            db_t_prod_stag.pc_personalvehicle pv
                                                                                                                                                         ON              pv.id_stg = pamotor.personalvehicle_stg
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = pamotor.branchid_stg
                                                                                                                                                         AND             pamotor.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             pv.expirationdate_stg IS NULL
                                                                                                                                                         WHERE           pamotor.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pamotor.updatetime_stg <= (:end_dttm)
                                                                                                                                                         AND             pv.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pv.updatetime_stg <= (:end_dttm)
                                                                                                                                                         /**WaterCrafttrailer**/
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(patrailer.fixedid_stg AS VARCHAR(100))AS id ,
                                                                                                                                                                         pv.ratingunitorigdate_alfa_stg,
                                                                                                                                                                         coalesce(pv.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(pv.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE4''                               AS assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN5''                              AS classification_code,
                                                                                                                                                                         pv.updatetime_stg
                                                                                                                                                         FROM            db_t_prod_stag.pcx_pawatercrafttrailer_alfa patrailer
                                                                                                                                                         join            db_t_prod_stag.pc_personalvehicle pv
                                                                                                                                                         ON              pv.id_stg = patrailer.personalvehicle_stg
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = patrailer.branchid_stg
                                                                                                                                                         AND             patrailer.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             pv.expirationdate_stg IS NULL
                                                                                                                                                         WHERE           patrailer.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             patrailer.updatetime_stg <= (:end_dttm)
                                                                                                                                                         AND             pv.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pv.updatetime_stg <= (:end_dttm)
                                                                                                                                                         /**Motor Vehicle**/
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(pv.fixedid_stg AS VARCHAR(100))AS id ,
                                                                                                                                                                         pv.ratingunitorigdate_alfa_stg,
                                                                                                                                                                         coalesce(pv.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(pv.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE4''                               AS assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN3''                              AS classification_code,
                                                                                                                                                                         pv.updatetime_stg
                                                                                                                                                         FROM            db_t_prod_stag.pc_personalvehicle pv
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = pv.branchid_stg
                                                                                                                                                         AND             pv.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             pv.expirationdate_stg IS NULL
                                                                                                                                                         WHERE           pv.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pv.updatetime_stg <= (:end_dttm)
                                                                                                                                                         AND             pp.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                                                                         /* RUOD Building*/
                                                                                                                                                         /** Real Estate **/
                                                                                                                                                         /** Main Dwelling **/
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(100))               AS id ,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE5''                              AS assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN1''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_dwelling_hoe b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             expirationdate IS NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         AND             pp.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             pp.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         /**Dwelling Personal Property and Other Structure**/
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(c.fixedid_stg AS VARCHAR(100))               AS id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         CASE
                                                                                                                                                                                         WHEN e.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                                                                                                        ''HOSI_SpecificOtherStructureExclItem_alfa'') THEN ''PRTY_ASSET_SBTYPE5''
                                                                                                                                                                                         WHEN e.patternid_stg=''HOSI_ScheduledPropertyItem_alfa'' THEN ''PRTY_ASSET_SBTYPE7''
                                                                                                                                                                                                        /*''REALSP-PP''*/
                                                                                                                                                                         END              AS assettype ,
                                                                                                                                                                         choiceterm1_stg  AS classification_code,
                                                                                                                                                                         b.updatetime_stg AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_holineschcovitemcov_alfa b
                                                                                                                                                         left outer join db_t_prod_stag.pc_etlclausepattern e
                                                                                                                                                         ON              e.patternid_stg=b.patterncode_stg
                                                                                                                                                         left outer join db_t_prod_stag.pcx_holineschedcovitem_alfa c
                                                                                                                                                         ON              c.id_stg=b.holineschcovitem_stg
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = cast(b.branchid_stg AS bigint)
                                                                                                                                                         WHERE           e.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                                                                                             ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                                                                                             ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                                                                                                         AND             c.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.expirationdate_stg IS NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         /**Building and property**/
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg,
                                                                                                                                                                         cast(c.fixedid_stg AS VARCHAR(100))               AS id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(c.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(c.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE13''                             AS assettype,
                                                                                                                                                                         cp.typecode_stg                                   AS classification_code,
                                                                                                                                                                         c.updatetime_stg                                  AS updatetime       
																																										 FROM
                                                                                                                                                                         /* DB_T_PROD_STAG.pcx_bp7building bp7*/
                                                                                                                                                                         db_t_prod_stag.pcx_bp7classification c 
                                                                                                                                                         inner join
                                                                                                                                                                         (
                                                                                                                                                                                  SELECT   b.*,
                                                                                                                                                                                           rank() over ( PARTITION BY b.fixedid_stg ORDER BY b.updatetime_stg DESC) r
                                                                                                                                                                                  FROM     db_t_prod_stag.pcx_bp7building b) bp7
                                                                                                                                                         ON              c.building_stg = bp7.fixedid_stg
                                                                                                                                                         AND             c.branchid_stg=bp7.branchid_stg
                                                                                                                                                         AND             bp7.r = 1
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = bp7.branchid_stg
                                                                                                                                                         inner join      db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                                                                                                         ON              cp.id_stg = c.bp7classpropertytype_stg
                                                                                                                                                         inner join
                                                                                                                                                                         (
                                                                                                                                                                                  SELECT   l.*,
                                                                                                                                                                                           rank() over ( PARTITION BY l.fixedid_stg ORDER BY l.updatetime_stg DESC) r
                                                                                                                                                                                  FROM     db_t_prod_stag.pcx_bp7location l) l
                                                                                                                                                         ON              bp7.location_stg = l.fixedid_stg
                                                                                                                                                         AND             l.r = 1
                                                                                                                                                         WHERE           bp7.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             bp7.expirationdate_stg IS NULL
                                                                                                                                                         AND             c.expirationdate_stg IS NULL
                                                                                                                                                         AND             l.expirationdate_stg IS NULL
                                                                                                                                                         AND             (
                                                                                                                                                                                         bp7.updatetime_stg> (:start_dttm)
                                                                                                                                                                         AND             bp7.updatetime_stg <= (:end_dttm)
                                                                                                                                                                         OR              c.updatetime_stg> (:start_dttm)
                                                                                                                                                                         AND             c.updatetime_stg <= (:end_dttm)
                                                                                                                                                                         OR              l.updatetime_stg> (:start_dttm)
                                                                                                                                                                         AND             l.updatetime_stg <= (:end_dttm))
                                                                                                                                                         UNION
                                                                                                                                                         SELECT pp.publicid_stg,
                                                                                                                                                                cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                ''PRTY_ASSET_SBTYPE23''                                assettype ,
                                                                                                                                                                ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM   db_t_prod_stag.pcx_bp7bldgschedcovitem b
                                                                                                                                                         join   db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON     pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE  b.expirationdate_stg IS NULL
                                                                                                                                                         AND    b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND    expirationdate_stg IS NULL
                                                                                                                                                         AND    b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND    b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE25''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7bldgschedexclitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE24''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7bldgschedconditem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE26''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7classschedcovitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE28''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7classschedexclitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg= b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE27''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7classschedconditem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE29''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7lineschedcovitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE31''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7lineschedexclitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE30''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7lineschedconditem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE20''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7locschedcovitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE22''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7locschedexclitem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm)
                                                                                                                                                         UNION
                                                                                                                                                         SELECT DISTINCT pp.publicid_stg ,
                                                                                                                                                                         cast(b.fixedid_stg AS VARCHAR(50))                id,
                                                                                                                                                                         cast(NULL AS timestamp)                           AS ratingunitorigdate_alfa,
                                                                                                                                                                         coalesce(b.effectivedate_stg, pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(b.expirationdate_stg, pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ''PRTY_ASSET_SBTYPE21''                                assettype ,
                                                                                                                                                                         ''PRTY_ASSET_CLASFCN8''                             AS classification_code,
                                                                                                                                                                         b.updatetime_stg                                  AS updatetime
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7locschedconditem b
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = b.branchid_stg
                                                                                                                                                         WHERE           expirationdate_stg IS NULL
                                                                                                                                                         AND             b.fixedid_stg IS NOT NULL
                                                                                                                                                         AND             b.updatetime_stg> (:start_dttm)
                                                                                                                                                         AND             b.updatetime_stg <= (:end_dttm) )tmp
                                                                                                                         /*FOUNDATION*/
                                                                                                                         /*EIM-14495*/
                                                                                                                         UNION
                                                                                                                         SELECT DISTINCT a.fixedid             AS fixedid,
                                                                                                                                         ''PRTY_ASSET_SBTYPE5''  AS typecode ,
                                                                                                                                         ''PRTY_ASSET_CLASFCN1'' AS classification_code,
                                                                                                                                         /* ''SRC_SYS4'' as src_cd,*/
                                                                                                                                         a.typecode_stg                                     asset_dtl_cd,
                                                                                                                                          cast(a.effectivedate AS timestamp)              agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp)    agmt_asset_dtl_xref_end_dttm ,
                                                                                                                                         publicid_stg                                       publicid ,
                                                                                                                                         cast(a.updatetime AS timestamp)                    trans_strt_dttm ,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp)    trans_end_dttm ,
                                                                                                                                         cast('' '' AS VARCHAR(255))                       AS asset_dtl_txt ,
                                                                                                                                         ''FOUND''                                         AS ind,
                                                                                                                                         ('' '')                                           AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                         AS ratingunitorigdate_alfa
                                                                                                                         FROM            (
                                                                                                                                                         SELECT DISTINCT a.updatetime_stg AS updatetime,
                                                                                                                                                                         a.fixedid_stg    AS fixedid,
                                                                                                                                                                         c.publicid_stg,
                                                                                                                                                                         coalesce(a.effectivedate_stg,c.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(a.expirationdate_stg,c.periodend_stg)  AS expirationdate,
                                                                                                                                                                         b.typecode_stg                                  AS typecode_stg
                                                                                                                                                         FROM            db_t_prod_stag.pcx_dwelling_hoe a
                                                                                                                                                         join            db_t_prod_stag.pc_policyperiod c
                                                                                                                                                         ON              c.id_stg = a.branchid_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_foundationtype_hoe b
                                                                                                                                                         ON              b.id_stg = a.foundation_stg
                                                                                                                                                         WHERE           a.expirationdate_stg IS NULL
                                                                                                                                                         AND             c.status_stg = 9
                                                                                                                                                         AND             ((
                                                                                                                                                                                                        c.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             c.updatetime_stg <= (:end_dttm))
                                                                                                                                                                         OR              (
                                                                                                                                                                                                        a.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             a.updatetime_stg <= (:end_dttm)) ) )a qualify row_number() over( PARTITION BY a.fixedid,publicid_stg ORDER BY effectivedate DESC,updatetime DESC)=1
                                                                                                                         UNION
                                                                                                                         SELECT DISTINCT b.fixedid                                       AS id,
                                                                                                                                         ''PRTY_ASSET_SBTYPE13''                           AS typecode,
                                                                                                                                         b.classificationcode                            AS classification_code,
                                                                                                                                         b.bp7foundationtype_alfa                        AS asset_dtl_cd,
                                                                                                                                         cast(b.effectivedate AS timestamp)              AS strt_dt ,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS end_dt,
                                                                                                                                         publicid_stg,
                                                                                                                                         cast(b.updatetime AS timestamp)                 AS trans_strt_dttm,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp)    trans_end_dttm,
                                                                                                                                         cast('' '' AS VARCHAR(255))                       AS asset_dtl_txt ,
                                                                                                                                         ''FOUND''                                         AS asset_dtl_schm_type_cd,
                                                                                                                                         ('' '')                                           AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                         AS ratingunitorigdate_alfa
                                                                                                                         FROM            (
                                                                                                                                                         SELECT DISTINCT c.fixedid_stg AS fixedid,
                                                                                                                                                                         pp.publicid_stg ,
                                                                                                                                                                         coalesce(c.effectivedate_stg , periodstart_stg) AS effectivedate ,
                                                                                                                                                                         c.updatetime_stg                                AS updatetime ,
                                                                                                                                                                         coalesce(c.expirationdate_stg,periodend_stg)    AS expirationdate ,
                                                                                                                                                                         cp.typecode_stg                                 AS classificationcode,
                                                                                                                                                                         ft.typecode_stg                                 AS bp7foundationtype_alfa
                                                                                                                                                         FROM            db_t_prod_stag.pcx_bp7classification c
                                                                                                                                                         inner join
                                                                                                                                                                         (
                                                                                                                                                                                  SELECT   b.*,
                                                                                                                                                                                           rank() over ( PARTITION BY b.fixedid_stg ORDER BY b.updatetime_stg DESC) r
                                                                                                                                                                                  FROM     db_t_prod_stag.pcx_bp7building b) pb
                                                                                                                                                         ON              c.building_stg = pb.fixedid_stg
                                                                                                                                                         AND             c.branchid_stg =pb.branchid_stg
                                                                                                                                                         AND             pb.r = 1
                                                                                                                                                                         /** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/
                                                                                                                                                         inner join      db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                                                                                                         ON              cp.id_stg = c.bp7classpropertytype_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_bp7classdescription cdes
                                                                                                                                                         ON              c.bp7classdescription_stg = cdes.id_stg
                                                                                                                                                         inner join      db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         ON              pp.id_stg = pb.branchid_stg
                                                                                                                                                         inner join      db_t_prod_stag.pctl_bp7foundationtype_alfa ft
                                                                                                                                                         ON              ft.id_stg=pb.bp7foundationtype_alfa_stg
                                                                                                                                                         WHERE           pb.expirationdate_stg IS NULL
                                                                                                                                                         AND             pp.status_stg = 9
                                                                                                                                                         AND             ((
                                                                                                                                                                                                        c.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             c.updatetime_stg <= (:end_dttm))
                                                                                                                                                                         OR              (
                                                                                                                                                                                                        pp.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             pp.updatetime_stg <= (:end_dttm))
                                                                                                                                                                         OR              (
                                                                                                                                                                                                        pb.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             pb.updatetime_stg <= (:end_dttm)) ) )b qualify row_number() over( PARTITION BY b.fixedid,publicid_stg ORDER BY effectivedate DESC,updatetime DESC)=1
                                                                                                                         /*PROTECTION CLASS CODE*/
                                                                                                                         /*EIM-40367*/
                                                                                                                         UNION
                                                                                                                         SELECT DISTINCT a.fixedid             AS fixedid,
                                                                                                                                         ''PRTY_ASSET_SBTYPE5''  AS typecode ,
                                                                                                                                         ''PRTY_ASSET_CLASFCN1'' AS classification_code,
                                                                                                                                         /* ''SRC_SYS4'' as src_cd,*/
                                                                                                                                         ''ASSET_DTL_TYPE19''                              AS asset_dtl_cd,
                                                                                                                                          cast(a.effectivedate AS timestamp)              agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         cast(a.expirationdate AS timestamp)                agmt_asset_dtl_xref_end_dttm ,
                                                                                                                                         publicid_stg                                       publicid ,
                                                                                                                                         cast(a.updatetime AS timestamp)                    trans_strt_dttm ,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp)    trans_end_dttm ,
                                                                                                                                         agmt_asset_dtl_txt                              AS asset_dtl_txt ,
                                                                                                                                         (''PROTCLS'')                                     AS ind ,
                                                                                                                                         ('' '')                                           AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                         AS ratingunitorigdate_alfa
                                                                                                                         FROM            (
                                                                                                                                                         SELECT DISTINCT loc.dwellingprotectionclasscode_stg AS agmt_asset_dtl_txt,
                                                                                                                                                                         dwell.id_stg,
                                                                                                                                                                         pp.termnumber_stg,
                                                                                                                                                                         pp.modelnumber_stg,
                                                                                                                                                                         pp.modeldate_stg ,
                                                                                                                                                                         tl.name_stg,
                                                                                                                                                                         sts.name_stg AS name_stg1,
                                                                                                                                                                         dwell.expirationdate_stg,
                                                                                                                                                                         dwell.updatetime_stg AS updatetime,
                                                                                                                                                                         dwell.fixedid_stg    AS fixedid,
                                                                                                                                                                         pp.publicid_stg,
                                                                                                                                                                         coalesce(dwell.effectivedate_stg,pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(dwell.expirationdate_stg,pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         /* b.TYPECODE_stg */
                                                                                                                                                                         ('' '') AS typecode_stg
                                                                                                                                                         FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         join            db_t_prod_stag.pcx_dwelling_hoe dwell
                                                                                                                                                         ON              dwell.branchid_stg = pp.id_stg
                                                                                                                                                         join            db_t_prod_stag.pcx_holocation_hoe loc
                                                                                                                                                         ON              loc.fixedid_stg = dwell.holocation_stg
                                                                                                                                                         AND             dwell.branchid_stg = loc.branchid_stg
                                                                                                                                                         join            db_t_prod_stag.pc_job job
                                                                                                                                                         ON              job.id_stg = pp.jobid_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_job tl
                                                                                                                                                         ON              tl.id_stg = job.subtype_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_policyperiodstatus sts
                                                                                                                                                         ON              sts.id_stg = pp.status_stg
                                                                                                                                                         WHERE           dwell.expirationdate_stg IS NULL
                                                                                                                                                         AND             pp.status_stg = 9
                                                                                                                                                         AND             ((
                                                                                                                                                                                                        pp.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             pp.updatetime_stg <= (:end_dttm))
                                                                                                                                                                         OR              (
                                                                                                                                                                                                        dwell.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             dwell.updatetime_stg <= (:end_dttm)) ) )a qualify row_number() over( PARTITION BY a.fixedid,publicid_stg ORDER BY effectivedate DESC,updatetime DESC)=1
                                                                                                                         /*PROTECTION CLASS CODE*/
                                                                                                                         /*EIM-41251*/
                                                                                                                         UNION
                                                                                                                         SELECT DISTINCT bp7query.fixedid                                AS fixedid,
                                                                                                                                         ''PRTY_ASSET_SBTYPE32''                           AS typecode ,
                                                                                                                                         ''PRTY_ASSET_CLASFCN10''                          AS classification_code,
                                                                                                                                         ''ASSET_DTL_TYPE18''                              AS asset_dtl_cd,
                                                                                                                                         cast(bp7query.effectivedate AS timestamp)          agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         cast(bp7query.expirationdate AS timestamp)         agmt_asset_dtl_xref_end_dttm ,
                                                                                                                                         publicid_stg                                       publicid ,
                                                                                                                                         cast(bp7query.updatetime AS timestamp)             trans_strt_dttm ,
                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp)    trans_end_dttm ,
                                                                                                                                         agmt_asset_dtl_txt                              AS asset_dtl_txt ,
                                                                                                                                         (''PROTCLS'')                                     AS ind ,
                                                                                                                                         ('' '')                                           AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                         AS ratingunitorigdate_alfa
                                                                                                                         FROM            (
                                                                                                                                                         SELECT DISTINCT
                                                                                                                                                                         /*  given*/
                                                                                                                                                                         /*pp.policynumber_stg, pp.termnumber_stg, pp.modelnumber_stg, loc.protectionclasscode_alfa_stg,*/
                                                                                                                                                                         loc.protectionclasscode_alfa_stg AS agmt_asset_dtl_txt,
                                                                                                                                                                                                           bp7bld.id_stg,
                                                                                                                                                                         pp.termnumber_stg,
                                                                                                                                                                         pp.modelnumber_stg,
                                                                                                                                                                         pp.modeldate_stg ,
                                                                                                                                                                         tl.name_stg,
                                                                                                                                                                         sts.name_stg AS name_stg1,
                                                                                                                                                                         bp7bld.expirationdate_stg,
                                                                                                                                                                         bp7bld.updatetime_stg AS updatetime,
                                                                                                                                                                         bp7bld.fixedid_stg    AS fixedid,
                                                                                                                                                                         pp.publicid_stg,
                                                                                                                                                                         coalesce(bp7bld.effectivedate_stg,pp.periodstart_stg) AS effectivedate,
                                                                                                                                                                         coalesce(bp7bld.expirationdate_stg,pp.periodend_stg)  AS expirationdate,
                                                                                                                                                                         ('' '')                                                 AS typecode_stg
                                                                                                                                                                         /*  tables*/
                                                                                                                                                                         
                                                                                                                                                         FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                         join            db_t_prod_stag.pcx_bp7building bp7bld
                                                                                                                                                         ON              bp7bld.branchid_stg = pp.id_stg
                                                                                                                                                         join            db_t_prod_stag.pcx_bp7location loc
                                                                                                                                                         ON              loc.fixedid_stg = bp7bld.location_stg
                                                                                                                                                         AND             bp7bld.branchid_stg = loc.branchid_stg
                                                                                                                                                         join            db_t_prod_stag.pc_job job
                                                                                                                                                         ON              job.id_stg = pp.jobid_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_job tl
                                                                                                                                                         ON              tl.id_stg = job.subtype_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_policyperiodstatus sts
                                                                                                                                                         ON              sts.id_stg = pp.status_stg
                                                                                                                                                         WHERE           bp7bld.expirationdate_stg IS NULL
                                                                                                                                                         AND             pp.status_stg = 9
                                                                                                                                                                         /* and pp.policynumber_stg = ''19000324753'' -- given*/
                                                                                                                                                                         
                                                                                                                                                         AND             ((
                                                                                                                                                                                                        pp.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             pp.updatetime_stg <= (:end_dttm))
                                                                                                                                                                         OR              (
                                                                                                                                                                                                        bp7bld.updatetime_stg > (:start_dttm)
                                                                                                                                                                                         AND             bp7bld.updatetime_stg <= (:end_dttm)) ) ) bp7query qualify row_number() over( PARTITION BY bp7query.fixedid, publicid_stg ORDER BY effectivedate DESC,updatetime DESC)=1
                                                                                                                         UNION ALL
                                                                                                                         SELECT DISTINCT val_type.classificationfixedid                                   AS fixedid,
                                                                                                                                         cast(''PRTY_ASSET_SBTYPE5'' AS  VARCHAR(100))                      AS typecode ,
                                                                                                                                         cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(100))                      AS classification_code,
                                                                                                                                         val_type.industryclasscode                                       AS asset_dtl_cd,
                                                                                                                                         cast(val_type.classificationeffectivedate AS timestamp)             agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         cast(val_type.classificationexpirationdate AS timestamp)            agmt_asset_dtl_xref_end_dttm,
                                                                                                                                         val_type.ppv_publicid                                            AS publicid,
                                                                                                                                         cast(val_type.classificationupdatetime AS timestamp)             AS trans_strt_dttm,
                                                                                                                                         cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                 AS trans_end_dttm,
                                                                                                                                         coalesce(cast(val_type.territorycode_alfa AS VARCHAR(48)),'' '')   AS asset_dtl_txt,
                                                                                                                                         ''VALUTYP''                                                        AS ind ,
                                                                                                                                         coalesce(cast(val_type.classcode_publicid AS VARCHAR(100)), '' '') AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                                          AS ratingunitorigdate_alfa
                                                                                                                         FROM            (
                                                                                                                                                         SELECT DISTINCT pcx_dwelling_hoe.fixedid_stg          AS classificationfixedid ,
                                                                                                                                                                         pc_policyperiod.policynumber_stg      AS policynumber ,
                                                                                                                                                                         pc_policyperiod.publicid_stg          AS ppv_publicid ,
                                                                                                                                                                         pcx_dwelling_hoe.effectivedate_stg    AS classificationeffectivedate ,
                                                                                                                                                                         pcx_dwelling_hoe.updatetime_stg       AS classificationupdatetime ,
                                                                                                                                                                         pcx_dwelling_hoe.expirationdate_stg   AS classificationexpirationdate ,
                                                                                                                                                                         pcx_dwelling_hoe.effectivedate_stg    AS classcodeeffectivedate ,
                                                                                                                                                                         pcx_dwelling_hoe.expirationdate_stg   AS classcodeexpirationdate,
                                                                                                                                                                         pctl_evaluationtype_alfa.typecode_stg AS industryclasscode ,
                                                                                                                                                                         pctl_evaluationtype_alfa.name_stg     AS description ,
                                                                                                                                                                         ''VALUTYP''                             AS ind ,
                                                                                                                                                                         cast(
                                                                                                                                                                         /*t1.code_stg*/
                                                                                                                                                                         NULL AS      VARCHAR(255)) AS territorycode_alfa ,
                                                                                                                                                                         cast(NULL AS VARCHAR(255)) AS classcode_publicid
                                                                                                                                                         FROM            db_t_prod_stag.pc_policyperiod pc_policyperiod
                                                                                                                                                         join            db_t_prod_stag.pcx_dwelling_hoe pcx_dwelling_hoe
                                                                                                                                                         ON              pcx_dwelling_hoe.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                         join            db_t_prod_stag.pctl_evaluationtype_alfa pctl_evaluationtype_alfa
                                                                                                                                                         ON              pctl_evaluationtype_alfa.id_stg = pcx_dwelling_hoe.evaluationtype_hoe_alfa_stg
                                                                                                                                                         WHERE           pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                                         AND             pcx_dwelling_hoe.updatetime_stg>(:start_dttm)
                                                                                                                                                         AND             pcx_dwelling_hoe.updatetime_stg <= (:end_dttm)
                                                                                                                                                         AND             pc_policyperiod.status_stg = 9 ) val_type qualify row_number() over( PARTITION BY val_type.classificationfixedid,val_type.ppv_publicid ORDER BY val_type.classcodeeffectivedate DESC,val_type.classificationupdatetime DESC)=1
                                                                                                                         UNION
                                                                                                                         /* -DB_T_PROD_STAG.pcx_fopdwelling */
                                                                                                                         SELECT DISTINCT pcx_fopdwelling.fixedid_stg                                                 AS fixedid ,
                                                                                                                                         ''PRTY_ASSET_SBTYPE37''                                                       AS typecode,
                                                                                                                                         ''PRTY_ASSET_CLASFCN15''                                                      AS classification_code,
                                                                                                                                         pctl_territorycode.typecode_stg                                             AS asset_dtl_cd ,
                                                                                                                                         coalesce(pcx_fopdwelling.effectivedate_stg,pc_policyperiod.periodstart_stg) AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         coalesce(pcx_fopdwelling.expirationdate_stg,pc_policyperiod.periodend_stg)  AS agmt_asset_dtl_xref_end_dttm ,
                                                                                                                                         pc_policyperiod.publicid_stg                                                AS ppv_publicid,
                                                                                                                                         pcx_fopdwelling.updatetime_stg                                              AS trans_strt_dttm ,
                                                                                                                                         cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                            AS trans_end_dttm ,
                                                                                                                                         pc_territorycode.code_stg                                                   AS asset_dtl_txt,
                                                                                                                                         ''TERR''                                                                      AS ind,
                                                                                                                                         cast(NULL AS VARCHAR(4))                                                    AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                                                     AS ratingunitorigdate_alfa
                                                                                                                         FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                         join            db_t_prod_stag.pcx_fopdwelling
                                                                                                                         ON              pcx_fopdwelling.branchid_stg = pc_policyperiod.id_stg
                                                                                                                         join            db_t_prod_stag.pcx_foplocation
                                                                                                                         ON              pcx_fopdwelling.location_stg = pcx_foplocation.fixedid_stg
                                                                                                                         AND             pcx_foplocation.branchid_stg=pcx_fopdwelling.branchid_stg
                                                                                                                         join            db_t_prod_stag.pc_policylocation
                                                                                                                         ON              pcx_foplocation.policylocationid_stg = pc_policylocation.fixedid_stg
                                                                                                                         AND             pc_policylocation.branchid_stg=pcx_foplocation.branchid_stg
                                                                                                                         join            db_t_prod_stag.pc_territorycode
                                                                                                                         ON              pc_territorycode.policylocation_stg = pc_policylocation.fixedid_stg
                                                                                                                         AND             pc_territorycode.branchid_stg=pcx_fopdwelling.branchid_stg
                                                                                                                         join            db_t_prod_stag.pctl_territorycode
                                                                                                                         ON              pctl_territorycode.id_stg = pc_territorycode.subtype_stg
                                                                                                                         WHERE           ((
                                                                                                                                                                         pcx_fopdwelling.expirationdate_stg IS NULL
                                                                                                                                                         OR              pcx_fopdwelling.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pcx_foplocation.expirationdate_stg IS NULL
                                                                                                                                                         OR              pcx_foplocation.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pc_policylocation.expirationdate_stg IS NULL
                                                                                                                                                         OR              pc_policylocation.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pc_territorycode.expirationdate_stg IS NULL
                                                                                                                                                         OR              pc_territorycode.expirationdate_stg>pc_policyperiod.modeldate_stg ))
                                                                                                                         AND             pcx_fopdwelling.updatetime_stg>(:start_dttm)
                                                                                                                         AND             pcx_fopdwelling.updatetime_stg <= (:end_dttm) qualify row_number() over(PARTITION BY fixedid,ppv_publicid ORDER BY coalesce(pcx_fopdwelling.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)))DESC,pcx_fopdwelling.updatetime_stg DESC,pcx_fopdwelling.createtime_stg DESC)=1
                                                                                                                         UNION
                                                                                                                         /* -DB_T_PROD_STAG.pcx_fopoutbuilding */
                                                                                                                         SELECT DISTINCT pcx_fopoutbuilding.fixedid_stg                                                 AS fixedid,
                                                                                                                                         ''PRTY_ASSET_SBTYPE36''                                                          AS typecode ,
                                                                                                                                         ''PRTY_ASSET_CLASFCN13''                                                         AS classification_code,
                                                                                                                                         pctl_territorycode.typecode_stg                                                AS asset_dtl_cd,
                                                                                                                                         coalesce(pcx_fopoutbuilding.effectivedate_stg,pc_policyperiod.periodstart_stg) AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                                                         coalesce(pcx_fopoutbuilding.expirationdate_stg,pc_policyperiod.periodend_stg)  AS agmt_asset_dtl_xref_end_dttm,
                                                                                                                                         pc_policyperiod.publicid_stg                                                   AS ppv_publicid,
                                                                                                                                         pcx_fopoutbuilding.updatetime_stg                                              AS trans_strt_dttm ,
                                                                                                                                         cast( ''9999-12-31 23:59:59.999999'' AS timestamp)                               AS trans_end_dttm ,
                                                                                                                                         pc_territorycode.code_stg                                                      AS asset_dtl_txt,
                                                                                                                                         ''TERR''                                                                         AS ind,
                                                                                                                                         cast(NULL AS VARCHAR(4))                                                       AS classcode_publicid,
                                                                                                                                         cast(NULL AS timestamp)                                                        AS ratingunitorigdate_alfa
                                                                                                                         FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                         join            db_t_prod_stag.pcx_fopoutbuilding
                                                                                                                         ON              pcx_fopoutbuilding.branchid_stg = pc_policyperiod.id_stg
                                                                                                                         join            db_t_prod_stag.pcx_foplocation
                                                                                                                         ON              pcx_fopoutbuilding.location_stg = pcx_foplocation.fixedid_stg
                                                                                                                         AND             pcx_fopoutbuilding.branchid_stg=pcx_foplocation.branchid_stg
                                                                                                                         join            db_t_prod_stag.pc_policylocation
                                                                                                                         ON              pcx_foplocation.policylocationid_stg = pc_policylocation.fixedid_stg
                                                                                                                         AND             pc_policylocation.branchid_stg=pcx_foplocation.branchid_stg
                                                                                                                         join            db_t_prod_stag.pc_territorycode
                                                                                                                         ON              pc_territorycode.policylocation_stg = pc_policylocation.fixedid_stg
                                                                                                                         AND             pc_territorycode.branchid_stg=pc_policylocation.branchid_stg
                                                                                                                         join            db_t_prod_stag.pctl_territorycode
                                                                                                                         ON              pctl_territorycode.id_stg = pc_territorycode.subtype_stg
                                                                                                                         WHERE           ((
                                                                                                                                                                         pcx_fopoutbuilding.expirationdate_stg IS NULL
                                                                                                                                                         OR              pcx_fopoutbuilding.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pcx_foplocation.expirationdate_stg IS NULL
                                                                                                                                                         OR              pcx_foplocation.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pc_policylocation.expirationdate_stg IS NULL
                                                                                                                                                         OR              pc_policylocation.expirationdate_stg>pc_policyperiod.modeldate_stg )
                                                                                                                                         AND             (
                                                                                                                                                                         pc_territorycode.expirationdate_stg IS NULL
                                                                                                                                                         OR              pc_territorycode.expirationdate_stg>pc_policyperiod.modeldate_stg ))
                                                                                                                         AND             pcx_fopoutbuilding.updatetime_stg>(:start_dttm)
                                                                                                                         AND             pcx_fopoutbuilding.updatetime_stg <= (:end_dttm) qualify row_number() over(PARTITION BY fixedid,ppv_publicid ORDER BY coalesce(pcx_fopoutbuilding.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))) DESC,pcx_fopoutbuilding.updatetime_stg DESC,pcx_fopoutbuilding.createtime_stg DESC)=1)sq
                                                                                                                /* out_prty_asset_sbtype_cd*/
                                                                                                                
                                                                                                left outer join
                                                                                                                (
                                                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
                                                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_prty_asset_sbtype_cd
                                                                                                ON              sq.typecode=xlat_prty_asset_sbtype_cd.src_idntftn_val
                                                                                                                /* out_Class_Cd*/
                                                                                                                
                                                                                                left outer join
                                                                                                                (
                                                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm IN ( ''derived'' ,
                                                                                                                                                                       ''pcx_holineschcovitemcov_alfa.ChoiceTerm1'',
                                                                                                                                                                       ''contentlineitemschedule.typecode'',
                                                                                                                                                                       ''pctl_bp7classificationproperty.typecode'')
                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                                                                                                                                        ''GW'')
                                                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_class_cd
                                                                                                ON              sq.classification_code=xlat_class_cd.src_idntftn_val ) sq1
                                                                                /* out_Asset_Dtl_cd ---Giving more records 12 need to fix qualify*/
                                                                                
                                                                left outer join
                                                                                (
                                                                                         SELECT   teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val,
                                                                                                  src_idntftn_nm
                                                                                                  /* , expn_dt, eff_dt*/
                                                                                                  
                                                                                         FROM     db_t_prod_core.teradata_etl_ref_xlat
                                                                                         WHERE    teradata_etl_ref_xlat.tgt_idntftn_nm= ''ASSET_DTL_TYPE''
                                                                                         AND      teradata_etl_ref_xlat.src_idntftn_nm IN(''derived'',
                                                                                                                                          ''pcx_bp7classcode.PublicId'',
                                                                                                                                          ''pctl_bp7propertytype.name'',
                                                                                                                                          ''pctl_territorycode.typecode'',
                                                                                                                                          ''pctl_bp7foundationtype_alfa.typecode'',
                                                                                                                                          ''pctl_foundationtype_hoe.typecode'',
                                                                                                                                          ''pctl_evaluationtype_alfa.typecode'',
                                                                                                                                          ''pctl_territorycode.typecode'' )
                                                                                         AND      teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                            ''DS'') qualify row_number () over( PARTITION BY teradata_etl_ref_xlat.src_idntftn_val,teradata_etl_ref_xlat.src_idntftn_nm ORDER BY teradata_etl_ref_xlat.expn_dt DESC, teradata_etl_ref_xlat.eff_dt DESC)=1 ) xlat_asset_dtl_cd
                                                                ON              sq1.in_asset_dtl_cd=xlat_asset_dtl_cd.src_idntftn_val
                                                                                /*qualify row_number () over(partition by XLAT_Asset_Dtl_cd.SRC_IDNTFTN_VAL,XLAT_Asset_Dtl_cd.SRC_IDNTFTN_NM order by expn_dt desc, eff_dt desc)=1*/
                                                                left outer join
                                                                                (
                                                                                       SELECT dpa.prty_asset_id,
                                                                                              dpa.prty_asset_sbtype_cd,
                                                                                              dpa.asset_host_id_val,
                                                                                              dpa.prty_asset_clasfcn_cd
                                                                                       FROM   db_t_prod_core.dir_prty_asset dpa) lkp_dpa
                                                                ON              sq1.out_prty_asset_sbtype_cd=lkp_dpa.prty_asset_sbtype_cd
                                                                AND             to_char(sq1.fixedid)=lkp_dpa.asset_host_id_val
                                                                AND             sq1.out_class_cd=lkp_dpa.prty_asset_clasfcn_cd
                                                                left outer join
                                                                                (
                                                                                         SELECT   agmt.agmt_id,
                                                                                                  agmt.nk_src_key,
                                                                                                  agmt.agmt_type_cd,
                                                                                                  agmt.host_agmt_num,
                                                                                                  agmt.edw_end_dttm
                                                                                         FROM     db_t_prod_core.agmt qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1) lkp_agmt
                                                                ON              lkp_agmt.nk_src_key=sq1.publicid
                                                                AND             lkp_agmt.agmt_type_cd=''PPV''
                                                                                /* :p_agmt_type_cd_policy_version*/
                                                                                /*QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_AGMT.NK_SRC_KEY,LKP_AGMT.HOST_AGMT_NUM ORDER BY LKP_AGMT.EDW_END_DTTM desc) = 1*/
                                                                                
                                                                left outer join
                                                                                (
                                                                                         SELECT   agmt_asset_dtl_cd_xref.prty_asset_id                 AS prty_asset_id,
                                                                                                  agmt_asset_dtl_cd_xref.agmt_id                       AS agmt_id,
                                                                                                  agmt_asset_dtl_cd_xref.asset_dtl_cd                  AS asset_dtl_cd,
                                                                                                  agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_strt_dttm AS agmt_asset_dtl_xref_strt_dttm,
                                                                                                  agmt_asset_dtl_cd_xref.agmt_asset_dtl_xref_end_dttm  AS agmt_asset_dtl_xref_end_dttm,
                                                                                                  agmt_asset_dtl_cd_xref.agmt_asset_dtl_txt            AS agmt_asset_dtl_txt ,
                                                                                                  agmt_asset_dtl_cd_xref.asset_dtl_cd_ind              AS asset_dtl_cd_ind ,
                                                                                                  agmt_asset_dtl_cd_xref.agmt_asset_dtl_dt             AS ratingunitorigdate_alfa ,
                                                                                                  agmt_asset_dtl_cd_xref.edw_end_dttm                  AS edw_end_dttm
                                                                                         FROM     db_t_prod_core.agmt_asset_dtl_cd_xref qualify row_number( ) over ( PARTITION BY agmt_asset_dtl_cd_xref.prty_asset_id,agmt_asset_dtl_cd_xref.agmt_id, agmt_asset_dtl_cd_xref.asset_dtl_cd_ind ORDER BY agmt_asset_dtl_cd_xref.edw_end_dttm DESC)=1 ) tgt_agmt_asset_dtl_cd_xref
                                                                ON              tgt_agmt_asset_dtl_cd_xref.prty_asset_id=lkp_dpa.prty_asset_id
                                                                AND             tgt_agmt_asset_dtl_cd_xref.agmt_id=lkp_agmt.agmt_id
                                                                                /* AND TGT_AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD_IND=SQ1.IND*/
                                                                                
                                                                AND             coalesce(trim(tgt_agmt_asset_dtl_cd_xref.asset_dtl_cd_ind), '''')=coalesce(trim(sq1.ind),'''')
                                                                                /* QUALIFY ROW_NUMBER( ) OVER (PARTITION BY TGT_AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID,TGT_AGMT_ASSET_DTL_CD_XREF.AGMT_ID,TGT_AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD_IND ORDER BY TGT_AGMT_ASSET_DTL_CD_XREF.EDW_END_DTTM DESC)=1*/
                                                )sq_final  ) src ) );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
         SELECT sq_pcx_dwelling_hoe.prty_asset_id                 AS prty_asset_id,
                sq_pcx_dwelling_hoe.agmt_id                       AS agmt_id,
                sq_pcx_dwelling_hoe.out_asset_dtl_cd              AS asset_dtl_cd,
                sq_pcx_dwelling_hoe.agmt_asset_dtl_xref_strt_dttm AS agmt_asset_dtl_xref_strt_dttm,
                sq_pcx_dwelling_hoe.agmt_asset_dtl_xref_end_dttm  AS agmt_asset_dtl_xref_end_dttm1,
                sq_pcx_dwelling_hoe.ind                           AS ind1,
                sq_pcx_dwelling_hoe.trans_strt_dttm               AS trans_strt_dttm1,
                sq_pcx_dwelling_hoe.trans_end_dttm                AS trans_end_dttm1,
                sq_pcx_dwelling_hoe.asset_dtl_txt                 AS asset_dtl_txt1,
                sq_pcx_dwelling_hoe.ratingunitorigdate_alfa       AS ratingunitorigdate_alfa,
                sq_pcx_dwelling_hoe.ins_upd_flag                  AS ins_upd_flag,
                :v_asset_dtl_cd                                   AS v_asset_dtl_cd,
                :prcs_id                                                                        AS out_prcs_id,
                current_timestamp                                                               AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                sq_pcx_dwelling_hoe.source_record_id
         FROM   sq_pcx_dwelling_hoe );
  -- Component exp_data_trans, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_trans AS
  (
         SELECT exp_data_transformation.prty_asset_id                 AS in_prty_asset_id,
                exp_data_transformation.agmt_id                       AS in_agmt_id,
                exp_data_transformation.asset_dtl_cd                  AS in_asset_dtl_cd,
                exp_data_transformation.agmt_asset_dtl_xref_strt_dttm AS in_asset_dtl_xref_strt_dttm,
                exp_data_transformation.agmt_asset_dtl_xref_end_dttm1 AS in_asset_dtl_xref_end_dttm,
                exp_data_transformation.ind1                          AS in_indicator,
                exp_data_transformation.edw_strt_dttm                 AS edw_strt_dttm,
                exp_data_transformation.edw_end_dttm                  AS edw_end_dttm,
                exp_data_transformation.trans_strt_dttm1              AS trans_strt_dttm,
                exp_data_transformation.trans_end_dttm1               AS trans_end_dttm,
                exp_data_transformation.asset_dtl_txt1                AS in_agmt_asset_dtl_txt,
                exp_data_transformation.out_prcs_id                   AS out_prcs_id,
                :v_lkp_md5                                            AS v_lkp_md5,
                :v_in_md5                                             AS v_in_md5,
                exp_data_transformation.ins_upd_flag            AS out_ins_upd_flag,
                exp_data_transformation.ratingunitorigdate_alfa AS in_ratingunitorigdate_alfa,
                exp_data_transformation.source_record_id
         FROM   exp_data_transformation );
  -- Component rtr_asset_dtl_cd_xref_Insert, Type ROUTER Output Group Insert
  CREATE
  OR
  replace TEMPORARY TABLE rtr_asset_dtl_cd_xref_insert AS
  SELECT exp_data_trans.in_prty_asset_id            AS in_prty_asset_id,
         exp_data_trans.in_agmt_id                  AS in_agmt_id,
         exp_data_trans.in_asset_dtl_cd             AS in_asset_dtl_cd,
         exp_data_trans.in_asset_dtl_xref_strt_dttm AS in_asset_dtl_xref_strt_dttm,
         exp_data_trans.in_asset_dtl_xref_end_dttm  AS in_asset_dtl_xref_end_dttm,
         exp_data_trans.in_agmt_asset_dtl_txt       AS in_agmt_asset_dtl_txt,
         exp_data_trans.in_indicator                AS in_indicator,
         exp_data_trans.edw_strt_dttm               AS edw_strt_dttm,
         exp_data_trans.edw_end_dttm                AS edw_end_dttm,
         exp_data_trans.trans_strt_dttm             AS trans_strt_dttm,
         exp_data_trans.trans_end_dttm              AS trans_end_dttm,
         exp_data_trans.out_prcs_id                 AS out_prcs_id,
         exp_data_trans.out_ins_upd_flag            AS out_ins_upd_flag,
         exp_data_trans.in_ratingunitorigdate_alfa  AS in_ratingunitorigdate_alfa,
         exp_data_trans.source_record_id
  FROM   exp_data_trans
  WHERE  exp_data_trans.out_ins_upd_flag = ''I''
  OR     exp_data_trans.out_ins_upd_flag = ''U'';
  
  -- Component updstg_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updstg_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_asset_dtl_cd_xref_insert.in_prty_asset_id            AS prty_asset_id,
                rtr_asset_dtl_cd_xref_insert.in_agmt_id                  AS agmt_id,
                rtr_asset_dtl_cd_xref_insert.in_asset_dtl_cd             AS asset_dtl_cd,
                rtr_asset_dtl_cd_xref_insert.in_asset_dtl_xref_strt_dttm AS asset_dtl_xref_strt_dttm,
                rtr_asset_dtl_cd_xref_insert.in_asset_dtl_xref_end_dttm  AS asset_dtl_xref_end_dttm,
                rtr_asset_dtl_cd_xref_insert.in_indicator                AS asset_dtl_cd_ind,
                rtr_asset_dtl_cd_xref_insert.edw_strt_dttm               AS edw_strt_dttm,
                rtr_asset_dtl_cd_xref_insert.edw_end_dttm                AS edw_end_dttm,
                rtr_asset_dtl_cd_xref_insert.trans_strt_dttm             AS trans_strt_dttm,
                rtr_asset_dtl_cd_xref_insert.trans_end_dttm              AS trans_end_dttm,
                rtr_asset_dtl_cd_xref_insert.out_prcs_id                 AS prcs_id,
                rtr_asset_dtl_cd_xref_insert.in_agmt_asset_dtl_txt       AS in_agmt_asset_dtl_txt1,
                rtr_asset_dtl_cd_xref_insert.in_ratingunitorigdate_alfa  AS ratingunitorigdate_alfa,
                rtr_asset_dtl_cd_xref_insert.source_record_id            AS source_record_id,
                0                                                        AS update_strategy_action
         FROM   rtr_asset_dtl_cd_xref_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT updstg_ins.prty_asset_id            AS prty_asset_id,
                updstg_ins.agmt_id                  AS agmt_id,
                updstg_ins.asset_dtl_cd             AS asset_dtl_cd,
                updstg_ins.asset_dtl_xref_strt_dttm AS asset_dtl_xref_strt_dttm,
                updstg_ins.asset_dtl_xref_end_dttm  AS asset_dtl_xref_end_dttm,
                updstg_ins.asset_dtl_cd_ind         AS asset_dtl_cd_ind,
                updstg_ins.edw_strt_dttm            AS edw_strt_dttm,
                updstg_ins.edw_end_dttm             AS edw_end_dttm,
                updstg_ins.trans_strt_dttm          AS trans_strt_dttm,
                updstg_ins.trans_end_dttm           AS trans_end_dttm,
                updstg_ins.prcs_id                  AS prcs_id,
                updstg_ins.in_agmt_asset_dtl_txt1   AS in_agmt_asset_dtl_txt1,
                updstg_ins.ratingunitorigdate_alfa  AS ratingunitorigdate_alfa,
                updstg_ins.source_record_id
         FROM   updstg_ins );
  -- Component Fil_agmt_asset_dtl_code_xref, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_agmt_asset_dtl_code_xref AS
  (
         SELECT exp_pass_to_tgt_ins.prty_asset_id            AS prty_asset_id,
                exp_pass_to_tgt_ins.agmt_id                  AS agmt_id,
                exp_pass_to_tgt_ins.asset_dtl_cd             AS asset_dtl_cd,
                exp_pass_to_tgt_ins.asset_dtl_xref_strt_dttm AS asset_dtl_xref_strt_dttm,
                exp_pass_to_tgt_ins.asset_dtl_xref_end_dttm  AS asset_dtl_xref_end_dttm,
                exp_pass_to_tgt_ins.asset_dtl_cd_ind         AS asset_dtl_cd_ind,
                exp_pass_to_tgt_ins.edw_strt_dttm            AS edw_strt_dttm,
                exp_pass_to_tgt_ins.edw_end_dttm             AS edw_end_dttm,
                exp_pass_to_tgt_ins.trans_strt_dttm          AS trans_strt_dttm,
                exp_pass_to_tgt_ins.trans_end_dttm           AS trans_end_dttm,
                exp_pass_to_tgt_ins.prcs_id                  AS prcs_id,
                exp_pass_to_tgt_ins.in_agmt_asset_dtl_txt1   AS in_agmt_asset_dtl_txt1,
                exp_pass_to_tgt_ins.ratingunitorigdate_alfa  AS ratingunitorigdate_alfa,
                exp_pass_to_tgt_ins.source_record_id
         FROM   exp_pass_to_tgt_ins
         WHERE  exp_pass_to_tgt_ins.prty_asset_id IS NOT NULL
         AND    exp_pass_to_tgt_ins.agmt_id IS NOT NULL );
  -- Component AGMT_ASSET_DTL_CD_XREF, Type TARGET
  INSERT INTO db_t_prod_core.agmt_asset_dtl_cd_xref
              (
                          prty_asset_id,
                          agmt_id,
                          asset_dtl_cd,
                          agmt_asset_dtl_xref_strt_dttm,
                          agmt_asset_dtl_xref_end_dttm,
                          agmt_asset_dtl_txt,
                          agmt_asset_dtl_dt,
                          asset_dtl_cd_ind,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT fil_agmt_asset_dtl_code_xref.prty_asset_id            AS prty_asset_id,
         fil_agmt_asset_dtl_code_xref.agmt_id                  AS agmt_id,
         fil_agmt_asset_dtl_code_xref.asset_dtl_cd             AS asset_dtl_cd,
         fil_agmt_asset_dtl_code_xref.asset_dtl_xref_strt_dttm AS agmt_asset_dtl_xref_strt_dttm,
         fil_agmt_asset_dtl_code_xref.asset_dtl_xref_end_dttm  AS agmt_asset_dtl_xref_end_dttm,
         fil_agmt_asset_dtl_code_xref.in_agmt_asset_dtl_txt1   AS agmt_asset_dtl_txt,
         fil_agmt_asset_dtl_code_xref.ratingunitorigdate_alfa  AS agmt_asset_dtl_dt,
         fil_agmt_asset_dtl_code_xref.asset_dtl_cd_ind         AS asset_dtl_cd_ind,
         fil_agmt_asset_dtl_code_xref.prcs_id                  AS prcs_id,
         fil_agmt_asset_dtl_code_xref.edw_strt_dttm            AS edw_strt_dttm,
         fil_agmt_asset_dtl_code_xref.edw_end_dttm             AS edw_end_dttm,
         fil_agmt_asset_dtl_code_xref.trans_strt_dttm          AS trans_strt_dttm,
         fil_agmt_asset_dtl_code_xref.trans_end_dttm           AS trans_end_dttm
  FROM   fil_agmt_asset_dtl_code_xref;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component sq_pcx_dwelling_hoe1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_dwelling_hoe1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS rooftypedescription,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cast(NULL AS VARCHAR(100)) AS addressline1
                                                  FROM            db_t_prod_stag.cc_vehicle ccv
                                                  WHERE           1=2 ) src ) );
  -- Component AGMT_ASSET_DTL_CD_XREF1, Type TARGET
  INSERT INTO db_t_prod_core.agmt_asset_dtl_cd_xref
              (
                          asset_dtl_cd_ind
              )
  SELECT sq_pcx_dwelling_hoe1.rooftypedescription AS asset_dtl_cd_ind
  FROM   sq_pcx_dwelling_hoe1;
  
  -- PIPELINE END FOR 2
  -- Component AGMT_ASSET_DTL_CD_XREF1, Type Post SQL
  UPDATE db_t_prod_core.agmt_asset_dtl_cd_xref
  SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         agmt_id,
                                         asset_dtl_cd,
                                         asset_dtl_cd_ind,
                                         agmt_asset_dtl_xref_strt_dttm,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over ( PARTITION BY prty_asset_id,agmt_id,asset_dtl_cd,asset_dtl_cd_ind ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 SECOND'' AS lead1,
                                         max(trans_strt_dttm) over ( PARTITION BY prty_asset_id,agmt_id,asset_dtl_cd,asset_dtl_cd_ind ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 SECOND'' AS lead2
                         FROM            db_t_prod_core.agmt_asset_dtl_cd_xref ) a
  WHERE  agmt_asset_dtl_cd_xref.prty_asset_id=a.prty_asset_id
  AND    agmt_asset_dtl_cd_xref.agmt_id=a.agmt_id
  AND    coalesce(agmt_asset_dtl_cd_xref.asset_dtl_cd_ind,''*'') = coalesce(a.asset_dtl_cd_ind,''*'')
  AND    agmt_asset_dtl_cd_xref.asset_dtl_cd =a.asset_dtl_cd
  AND    agmt_asset_dtl_cd_xref.edw_strt_dttm = a.edw_strt_dttm
  AND    cast(agmt_asset_dtl_cd_xref.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;

END;
';