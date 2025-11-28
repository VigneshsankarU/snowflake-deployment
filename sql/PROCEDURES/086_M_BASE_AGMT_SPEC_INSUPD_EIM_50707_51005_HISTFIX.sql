-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_SPEC_INSUPD_EIM_50707_51005_HISTFIX("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_spec_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_SPEC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN( ''pc_effectivedatedfields.othercarrier_alfa'',
                                                        ''derived'',
                                                        ''pc_effectivedatedfields.PriorCarrierExpDate_alfa'',
                                                        ''pcx_puprenreviewdetails_alfa.IsSubmitted'',
                                                        ''pc_effectivedatedfields.LatestDtOfLapses_alfa'',
                                                        ''pc_effectivedatedfields.LapsesInContService_alfa'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN ( ''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' -- ORDER BY SRC_IDNTFTN_VAL,TGT_IDNTFTN_VAL
  );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_spec_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SPEC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_number_alfa.TYPECODE'' ,
                                                         ''pctl_fopfarmingoperations.typecode'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' --
  );
  -- PIPELINE START FOR 1
  -- Component sq_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS policynumber,
                $2  AS publicid,
                $3  AS spec_type_cd,
                $4  AS effectivedate,
                $5  AS expirationdate,
                $6  AS trans_strt_dttm,
                $7  AS agmt_spec_strt_dttm,
                $8  AS agmt_spec_type_cd,
                $9  AS agmt_spec_ind,
                $10 AS agmt_spec_dt,
                $11 AS agmt_spec_txt,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   policynumber,
                                                    publicid,
                                                    spec_type_cd,
                                                    effectivedate,
                                                    expirationdate,
                                                    trans_strt_dttm,
                                                    agmt_spec_strt_dttm,
                                                    agmt_spec_type_cd,
                                                    agmt_spec_ind,
                                                    agmt_spec_dt ,
                                                    agmt_spec_txt
                                           FROM     (
                                                                    /* HO2 count for PARelationshipDiscount */
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrho2.typecode_stg        AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC25''              AS agmt_spec_type_cd,
                                                                                    cast(NULL AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_papolicytype_alfa pa
                                                                    ON              pa.id_stg = pl.papolicytype_alfa_stg
                                                                    join            db_t_prod_stag.pc_personalvehicle ppv
                                                                    ON              ppv.branchid_stg = pp.id_stg
                                                                    join            db_t_prod_stag.pc_pamodifier dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_relationshipdetails_alfa rldet
                                                                    ON              rldet.id_stg = pl.relationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho2
                                                                    ON              nbrho2.id_stg = rldet.numho2pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho2.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /* HO3 count    */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho3.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC3'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho3
                                                                    ON              nbrho3.id_stg = rldet.numho3pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho3.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  HO2 count    */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho2.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC25'' AS VARCHAR(100))       AS agmt_spec_type_cd,
                                                                                    cast(NULL AS          VARCHAR(100))       AS agmt_spec_ind,
                                                                                    cast(NULL AS          DATE)               AS agmt_spec_dt ,
                                                                                    cast(NULL AS          VARCHAR(255) )      AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho2
                                                                    ON              nbrho2.id_stg = rldet.numho2pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho2.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  HO4 count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho4.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC4'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho4
                                                                    ON              nbrho4.id_stg = rldet.numho4pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho4.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  HO5 count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho5.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC5'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho5
                                                                    ON              nbrho5.id_stg = rldet.numho5pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho5.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  HO6 count */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho6.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC6'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho6
                                                                    ON              nbrho6.id_stg = rldet.numho6pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho6.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  HO8 count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrho8.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC7'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho8
                                                                    ON              nbrho8.id_stg = rldet.numho8pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrho8.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  SF count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrsf.typecode_stg AS  VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC8'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrsf
                                                                    ON              nbrsf.id_stg = rldet.numsfpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrsf.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  MH count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(nbrmh.typecode_stg AS  VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC9'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS         VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS         DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS         VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrmh
                                                                    ON              nbrmh.id_stg = rldet.nummhpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrmh.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  DB_T_STAG_MEMBXREF_PROD.Life count  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS  VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS      VARCHAR(100)) AS publicid,
                                                                                    cast(nbrlife.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                          AS spec_updatetime,
                                                                                    pl.effectivedate_stg                       AS effectivedate,
                                                                                    pl.expirationdate_stg                      AS expirationdate,
                                                                                    pp.updatetime_stg                          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                   AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC10'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS          VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS          DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS          VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrlife
                                                                    ON              nbrlife.id_stg = rldet.numlifepol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrlife.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  Farm  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS  VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS      VARCHAR(100)) AS publicid,
                                                                                    cast(nbrfarm.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                          AS spec_updatetime,
                                                                                    pl.effectivedate_stg                       AS effectivedate,
                                                                                    pl.expirationdate_stg                      AS expirationdate,
                                                                                    pp.updatetime_stg                          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                   AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC11'' AS VARCHAR(100))        AS agmt_spec_type_cd,
                                                                                    cast(NULL AS          VARCHAR(100))        AS agmt_spec_ind,
                                                                                    cast(NULL AS          DATE)                AS agmt_spec_dt ,
                                                                                    cast(NULL AS          VARCHAR(255) )       AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrfarm
                                                                    ON              nbrfarm.id_stg = rldet.numfarmpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             nbrfarm.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  Watercraft  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(NULL AS                VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC13'' AS           VARCHAR(100))       AS agmt_spec_type_cd,
                                                                                    cast(rldet.watercraftind_stg AS VARCHAR(100))       AS agmt_spec_ind,
                                                                                    cast(NULL AS                    DATE)               AS agmt_spec_dt ,
                                                                                    cast(NULL AS                    VARCHAR(255) )      AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             rldet.watercraftind_stg <> 0
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    UNION
                                                                    /*  DB_T_STAG_MEMBXREF_PROD.Umbrella  */
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(NULL AS                VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC14'' AS         VARCHAR(100))       AS agmt_spec_type_cd,
                                                                                    cast(rldet.umbrellaind_stg AS VARCHAR(100))       AS agmt_spec_ind,
                                                                                    cast(NULL AS                  DATE)               AS agmt_spec_dt ,
                                                                                    cast(NULL AS                  VARCHAR(255) )      AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe pa
                                                                    ON              pa.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa rldet
                                                                    ON              rldet.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             rldet.umbrellaind_stg <> 0
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999''
                                                                    /* EIM-51005 */
                                                                    UNION
                                                                    SELECT DISTINCT cast(pp.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                                    cast(pp.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                                    cast(NULL AS                VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                         AS spec_updatetime,
                                                                                    pl.effectivedate_stg                      AS effectivedate,
                                                                                    pl.expirationdate_stg                     AS expirationdate,
                                                                                    pp.updatetime_stg                         AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC26'' AS              VARCHAR(100))       AS agmt_spec_type_cd,
                                                                                    cast(pl.ismhtierrating_alfa_stg AS VARCHAR(100))       AS agmt_spec_ind,
                                                                                    cast(NULL AS                       DATE)               AS agmt_spec_dt ,
                                                                                    cast(NULL AS                       VARCHAR(255) )      AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                                    /*  added as per logic in existing union. */
                                                                    WHERE           pl.patterncode_stg = ''HomeownersLine_HOE''
                                                                    AND             pl.ismhtierrating_alfa_stg = 1
                                                                    AND             pl.updatetime_stg >''1900-01-01 00:00:00.000000''
                                                                    AND             pl.updatetime_stg <=''9999-12-31 23:59:59.999999'' )agmt_spec_type_cd_z qualify row_number() over (PARTITION BY agmt_spec_type_cd_z.publicid, agmt_spec_type_cd_z.agmt_spec_type_cd, agmt_spec_type_cd_z.effectivedate ORDER BY agmt_spec_type_cd_z.spec_updatetime DESC) =1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    sq_pc_policyperiod.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
                      sq_pc_policyperiod.trans_strt_dttm     AS trans_strt_dttm,
                      sq_pc_policyperiod.publicid            AS publicid,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD */
                      AS out_spec_type_cd,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD */
                                                       AS out_agmt_spec_type_cd,
                      $p_agmt_type_cd_policy_version   AS out_agmt_type_cd,
                      sq_pc_policyperiod.agmt_spec_ind AS agmt_spec_ind,
                      sq_pc_policyperiod.agmt_spec_dt  AS agmt_spec_dt,
                      sq_pc_policyperiod.agmt_spec_txt AS agmt_spec_txt,
                      sq_pc_policyperiod.source_record_id,
                      row_number() over (PARTITION BY sq_pc_policyperiod.source_record_id ORDER BY sq_pc_policyperiod.source_record_id) AS rnk
            FROM      sq_pc_policyperiod
            left join lkp_teradata_etl_ref_xlat_spec_type_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_policyperiod.spec_type_cd
            left join lkp_teradata_etl_ref_xlat_agmt_spec_type_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_policyperiod.agmt_spec_type_cd qualify rnk = 1 );
  -- Component LKP_XREF_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_agmt AS
  (
            SELECT    lkp.agmt_id,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id DESC,lkp.nk_src_key DESC,lkp.term_num DESC,lkp.agmt_type_cd DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT dir_agmt.agmt_id                    AS agmt_id,
                                    ltrim(rtrim(dir_agmt.nk_src_key))   AS nk_src_key,
                                    dir_agmt.term_num                   AS term_num,
                                    ltrim(rtrim(dir_agmt.agmt_type_cd)) AS agmt_type_cd
                             FROM   db_t_prod_core.dir_agmt ) lkp
            ON        lkp.nk_src_key = exp_pass_from_source.publicid
            AND       lkp.term_num = $in_term_num
            AND       lkp.agmt_type_cd = exp_pass_from_source.out_agmt_type_cd 
            qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id DESC,lkp.nk_src_key DESC,lkp.term_num DESC,lkp.agmt_type_cd DESC) 
            = 1 );
  -- Component LKP_AGMT_SPEC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_spec AS
  (
             SELECT     lkp.agmt_id,
                        lkp.agmt_spec_type_cd,
                        lkp.agmt_spec_strt_dttm,
                        lkp.spec_type_cd,
                        lkp.agmt_spec_txt_lkp,
                        lkp_xref_agmt.agmt_id                      AS in_agmt_id,
                        exp_pass_from_source.out_agmt_spec_type_cd AS in_agmt_spec_type_cd,
                        exp_pass_from_source.agmt_spec_txt         AS agmt_spec_txt,
                        exp_pass_from_source.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_spec_type_cd ASC,lkp.agmt_spec_strt_dttm ASC,lkp.spec_type_cd ASC,lkp.agmt_spec_txt_lkp ASC) rnk
             FROM       exp_pass_from_source
             inner join lkp_xref_agmt
             ON         exp_pass_from_source.source_record_id = lkp_xref_agmt.source_record_id
             left join
                        (
                               SELECT agmt_spec.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
                                      agmt_spec.spec_type_cd        AS spec_type_cd,
                                      agmt_spec.agmt_spec_txt       AS agmt_spec_txt_lkp,
                                      agmt_spec.agmt_id             AS agmt_id,
                                      agmt_spec.agmt_spec_type_cd   AS agmt_spec_type_cd
                               FROM   db_t_prod_core.agmt_spec
                               WHERE  edw_end_dttm=cast(''9999-12-31'' AS DATE)
                               AND    agmt_spec_type_cd <>''AFFNTYGRP''
                                      /*  */
                        ) lkp
             ON         lkp.agmt_id = lkp_xref_agmt.agmt_id
             AND        lkp.agmt_spec_type_cd = exp_pass_from_source.out_agmt_spec_type_cd 
             qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_spec_type_cd ASC,lkp.agmt_spec_strt_dttm ASC,lkp.spec_type_cd ASC,lkp.agmt_spec_txt_lkp ASC) 
             = 1 );
  -- Component exp_pass_to_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_upd AS
  (
             SELECT     lkp_agmt_spec.agmt_id                                                 AS lkp_agmt_id,
                        lkp_agmt_spec.agmt_spec_type_cd                                       AS lkp_agmt_spec_type_cd,
                        lkp_agmt_spec.agmt_spec_strt_dttm                                     AS lkp_agmt_spec_strt_dttm,
                        lkp_agmt_spec.spec_type_cd                                            AS lkp_spec_type_cd,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_agmt_spec_end_dttm,
                        $prcs_id                                                              AS out_prcs_id,
                        current_timestamp                                                     AS out_edw_strt_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_trans_end_dttm,
                        md5 ( ltrim ( rtrim ( lkp_agmt_spec.spec_type_cd ) )
                                   || lkp_agmt_spec.agmt_id
                                   || lkp_agmt_spec.agmt_spec_txt_lkp ) AS checksum_lkp,
                        md5 ( ltrim ( rtrim ( exp_pass_from_source.out_spec_type_cd ) )
                                   || lkp_agmt_spec.in_agmt_id
                                   || lkp_agmt_spec.agmt_spec_txt ) AS checksum_in,
                        CASE
                                   WHEN checksum_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN checksum_lkp != checksum_in THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                    AS cdc_flag,
                        lkp_agmt_spec.in_agmt_id                               AS in_agmt_id,
                        lkp_agmt_spec.in_agmt_spec_type_cd                     AS in_agmt_spec_type_cd,
                        exp_pass_from_source.out_spec_type_cd                  AS in_spec_type_cd,
                        exp_pass_from_source.agmt_spec_strt_dttm               AS in_agmt_spec_strt_dttm,
                        exp_pass_from_source.trans_strt_dttm                   AS in_trans_strt_dttm,
                        ltrim ( rtrim ( exp_pass_from_source.agmt_spec_ind ) ) AS v_agmt_spec_ind,
                        CASE
                                   WHEN v_agmt_spec_ind = ''1'' THEN ''YES''
                                   ELSE exp_pass_from_source.agmt_spec_ind
                        END                               AS out_agmt_spec_ind,
                        exp_pass_from_source.agmt_spec_dt AS agmt_spec_dt,
                        lkp_agmt_spec.agmt_spec_txt       AS agmt_spec_txt,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_agmt_spec
             ON         exp_pass_from_source.source_record_id = lkp_agmt_spec.source_record_id );
  -- Component rtr_agmt_spec_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_agmt_spec_insert as
  SELECT exp_pass_to_upd.lkp_agmt_id             AS lkp_agmt_id,
         exp_pass_to_upd.lkp_agmt_spec_type_cd   AS lkp_agmt_spec_type_cd,
         exp_pass_to_upd.lkp_agmt_spec_strt_dttm AS lkp_agmt_spec_strt_dttm,
         NULL                                    AS lkp_agmt_spec_end_dttm,
         exp_pass_to_upd.lkp_spec_type_cd        AS lkp_spec_type_cd,
         NULL                                    AS lkp_agmt_spec_cnt,
         NULL                                    AS lkp_agmt_spec_txt,
         NULL                                    AS lkp_agmt_spec_qty,
         NULL                                    AS lkp_agmt_spec_rate,
         NULL                                    AS lkp_agmt_spec_amt,
         NULL                                    AS lkp_agmt_spec_dt,
         NULL                                    AS lkp_prcs_id,
         NULL                                    AS lkp_edw_strt_dttm,
         NULL                                    AS lkp_edw_end_dttm,
         NULL                                    AS lkp_trans_strt_dttm,
         exp_pass_to_upd.out_trans_end_dttm      AS trans_end_dttm,
         exp_pass_to_upd.cdc_flag                AS cdc_flag,
         exp_pass_to_upd.in_agmt_id              AS in_agmt_id,
         exp_pass_to_upd.in_agmt_spec_type_cd    AS in_agmt_spec_type_cd,
         exp_pass_to_upd.in_spec_type_cd         AS in_spec_type_cd,
         exp_pass_to_upd.out_agmt_spec_ind       AS out_agmt_spec_ind,
         exp_pass_to_upd.in_agmt_spec_strt_dttm  AS in_agmt_spec_strt_dttm,
         exp_pass_to_upd.in_trans_strt_dttm      AS in_trans_strt_dttm,
         exp_pass_to_upd.out_prcs_id             AS in_prcs_id,
         exp_pass_to_upd.out_edw_strt_dttm       AS in_edw_strt_dttm,
         exp_pass_to_upd.out_edw_end_dttm        AS in_edw_end_dttm,
         exp_pass_to_upd.out_agmt_spec_end_dttm  AS in_agmt_spec_end_dttm,
         exp_pass_to_upd.agmt_spec_dt            AS in_agmt_spec_dt,
         exp_pass_to_upd.agmt_spec_txt           AS agmt_spec_txt,
         exp_pass_to_upd.source_record_id
  FROM   exp_pass_to_upd
  WHERE  (
                exp_pass_to_upd.cdc_flag = ''I''
         OR     exp_pass_to_upd.cdc_flag = ''U'' )
  AND    exp_pass_to_upd.in_agmt_id IS NOT NULL;
  
  -- Component upd_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_spec_insert.in_agmt_id             AS agmt_id,
                rtr_agmt_spec_insert.in_agmt_spec_type_cd   AS agmt_spec_type_cd,
                rtr_agmt_spec_insert.in_agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
                rtr_agmt_spec_insert.in_agmt_spec_end_dttm  AS agmt_spec_end_dttm,
                rtr_agmt_spec_insert.in_spec_type_cd        AS spec_type_cd,
                rtr_agmt_spec_insert.out_agmt_spec_ind      AS agmt_spec_ind,
                rtr_agmt_spec_insert.in_prcs_id             AS prcs_id,
                rtr_agmt_spec_insert.in_edw_strt_dttm       AS edw_strt_dttm,
                rtr_agmt_spec_insert.in_edw_end_dttm        AS edw_end_dttm,
                rtr_agmt_spec_insert.in_trans_strt_dttm     AS trans_strt_dttm,
                rtr_agmt_spec_insert.trans_end_dttm         AS trans_end_dttm,
                rtr_agmt_spec_insert.in_agmt_spec_dt        AS agmt_spec_dt,
                rtr_agmt_spec_insert.agmt_spec_txt          AS agmt_spec_txt1,
                0                                           AS update_strategy_action,
                rtr_agmt_spec_insert.source_record_id
         FROM   rtr_agmt_spec_insert );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_ins_upd.agmt_id             AS agmt_id,
                upd_ins_upd.agmt_spec_type_cd   AS agmt_spec_type_cd,
                upd_ins_upd.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
                upd_ins_upd.agmt_spec_end_dttm  AS agmt_spec_end_dttm,
                upd_ins_upd.spec_type_cd        AS spec_type_cd,
                NULL                            AS agmt_spec_cnt,
                upd_ins_upd.agmt_spec_txt1      AS agmt_spec_txt,
                NULL                            AS agmt_spec_qty,
                NULL                            AS agmt_spec_rate,
                NULL                            AS agmt_spec_amt,
                upd_ins_upd.agmt_spec_dt        AS agmt_spec_dt,
                upd_ins_upd.agmt_spec_ind       AS agmt_spec_ind,
                upd_ins_upd.prcs_id             AS prcs_id,
                upd_ins_upd.edw_strt_dttm       AS edw_strt_dttm,
                upd_ins_upd.edw_end_dttm        AS edw_end_dttm,
                upd_ins_upd.trans_strt_dttm     AS trans_strt_dttm,
                upd_ins_upd.trans_end_dttm      AS trans_end_dttm,
                upd_ins_upd.source_record_id
         FROM   upd_ins_upd );
  -- Component AGMT_SPEC, Type TARGET
  INSERT INTO db_t_prod_core.agmt_spec
              (
                          agmt_id,
                          agmt_spec_type_cd,
                          agmt_spec_strt_dttm,
                          agmt_spec_end_dttm,
                          spec_type_cd,
                          agmt_spec_cnt,
                          agmt_spec_txt,
                          agmt_spec_qty,
                          agmt_spec_rate,
                          agmt_spec_amt,
                          agmt_spec_dt,
                          agmt_spec_ind,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id             AS agmt_id,
         exp_pass_to_target_ins.agmt_spec_type_cd   AS agmt_spec_type_cd,
         exp_pass_to_target_ins.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
         exp_pass_to_target_ins.agmt_spec_end_dttm  AS agmt_spec_end_dttm,
         exp_pass_to_target_ins.spec_type_cd        AS spec_type_cd,
         exp_pass_to_target_ins.agmt_spec_cnt       AS agmt_spec_cnt,
         exp_pass_to_target_ins.agmt_spec_txt       AS agmt_spec_txt,
         exp_pass_to_target_ins.agmt_spec_qty       AS agmt_spec_qty,
         exp_pass_to_target_ins.agmt_spec_rate      AS agmt_spec_rate,
         exp_pass_to_target_ins.agmt_spec_amt       AS agmt_spec_amt,
         exp_pass_to_target_ins.agmt_spec_dt        AS agmt_spec_dt,
         exp_pass_to_target_ins.agmt_spec_ind       AS agmt_spec_ind,
         exp_pass_to_target_ins.prcs_id             AS prcs_id,
         exp_pass_to_target_ins.edw_strt_dttm       AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm        AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm     AS trans_strt_dttm,
         exp_pass_to_target_ins.trans_end_dttm      AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- PIPELINE END FOR 1
  -- Component AGMT_SPEC, Type Post SQL
  UPDATE db_t_prod_core.agmt_spec
 SET    agmt_spec_end_dttm=a.lead1,
         edw_end_dttm=a.lead2,
         trans_end_dttm=a.lead3
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         agmt_spec_type_cd,
                                         edw_strt_dttm,
                                         max(agmt_spec_strt_dttm) over(PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)      - interval ''1 second'' AS lead2,
                                         max(trans_strt_dttm) over (PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)    - interval ''1 second'' AS lead3
                         FROM            db_t_prod_core.agmt_spec
                         WHERE           edw_end_dttm=cast(''9999-12-31'' AS DATE)
                         AND             agmt_spec_type_cd<>''AFFNTYGRP'' ) a
 
  WHERE  agmt_spec.edw_strt_dttm = a.edw_strt_dttm
  AND    agmt_spec.agmt_id=a.agmt_id
  AND    agmt_spec.agmt_spec_type_cd = a.agmt_spec_type_cd
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL
  AND    lead3 IS NOT NULL;
  
  -- PIPELINE START FOR 2
  -- Component sq_pc_policyperiod1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS publicid,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   policynumber,
                                                    publicid,
                                                    agmt_spec_type_cd,
                                                    effectivedate,
                                                    expirationdate,
                                                    policyperiod_updatetime,
                                                    editeffectivedate,
                                                    agmt_spec_type
                                           FROM     (
                                                           SELECT pp1.policynumber_stg AS policynumber,
                                                                  /* 01  */
                                                                  pp1.publicid_stg AS publicid,
                                                                  /* 02 */
                                                                  agtype.typecode_stg AS agmt_spec_type_cd,
                                                                  /* 03  */
                                                                  ppl1.effectivedate_stg AS effectivedate,
                                                                  /* 04 */
                                                                  ppl1.expirationdate_stg AS expirationdate,
                                                                  /* 05 */
                                                                  pp1.updatetime_stg AS policyperiod_updatetime,
                                                                  /* 06 */
                                                                  pp1.editeffectivedate_stg AS editeffectivedate,
                                                                  /* 07 */
                                                                  pag.typecode_stg AS agmt_spec_type,
                                                                  /* 08 */
                                                                  ag.updatetime_stg AS spec_updatetime
                                                           FROM   db_t_prod_stag.pc_policyperiod pp1
                                                           join   db_t_prod_stag.pc_policyline ppl1
                                                           ON     ppl1.branchid_stg = pp1.id_stg
                                                           AND    ppl1.expirationdate_stg IS NULL
                                                           join   db_t_prod_stag.pctl_policyperiodstatus ps
                                                           ON     ps.id_stg = pp1.status_stg
                                                           join   db_t_prod_stag.pcx_affinitygroup_alfa ag
                                                           ON     ag.personalautoline_stg = ppl1.id_stg
                                                           join   db_t_prod_stag.pctl_affinitygrouptype_alfa agtype
                                                           ON     agtype.id_stg = ag.affinitytype_stg
                                                           join   db_t_prod_stag.pctl_affinitygroup_alfa pag
                                                           ON     pag.id_stg = ag.subtype_stg
                                                           WHERE  ps.typecode_stg = ''Bound'' ) agmt_spec_x
                                           join     db_t_prod_stag.pctl_affinitygroup_alfa
                                           ON       pctl_affinitygroup_alfa.typecode_stg = agmt_spec_type
                                           AND      1 = 2 qualify row_number() over (PARTITION BY agmt_spec_x.publicid , agmt_spec_x.agmt_spec_type_cd ORDER BY agmt_spec_x.spec_updatetime DESC) =1 ) src ) );
  -- Component AGMT_SPEC_dummy, Type TARGET
  INSERT INTO db_t_prod_core.agmt_spec
              (
                          agmt_spec_type_cd
              )
  SELECT sq_pc_policyperiod1.publicid AS agmt_spec_type_cd
  FROM   sq_pc_policyperiod1;
  
  -- PIPELINE END FOR 2
  -- Component AGMT_SPEC_dummy, Type Post SQL
  UPDATE db_t_prod_core.agmt_spec
    SET    agmt_spec_end_dttm=a.lead1,
         edw_end_dttm=a.lead2,
         trans_end_dttm=a.lead3
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         agmt_spec_type_cd,
                                         edw_strt_dttm,
                                         max(agmt_spec_strt_dttm) over(PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)      - interval ''1 second'' AS lead2,
                                         max(trans_strt_dttm) over (PARTITION BY agmt_id, agmt_spec_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)    - interval ''1 second'' AS lead3
                         FROM            db_t_prod_core.agmt_spec
                         WHERE           edw_end_dttm=cast(''9999-12-31'' AS DATE)
                         AND             agmt_spec_type_cd<>''AFFNTYGRP'' ) a

  WHERE  agmt_spec.edw_strt_dttm = a.edw_strt_dttm
  AND    agmt_spec.agmt_id=a.agmt_id
  AND    agmt_spec.agmt_spec_type_cd = a.agmt_spec_type_cd
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL
  AND    lead3 IS NOT NULL;

END;
';