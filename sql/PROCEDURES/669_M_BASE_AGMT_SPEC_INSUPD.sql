-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_SPEC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  prcs_id int;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
  IN_TERM_NUM INT;
BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
P_AGMT_TYPE_CD_POLICY_VERSION :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_AGMT_TYPE_CD_POLICY_VERSION'' order by insert_ts desc limit 1);
IN_TERM_NUM :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''IN_TERM_NUM'' order by insert_ts desc limit 1);

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
                                                    /* EIM_18145_Start                                    */
                                                    agmt_spec_ind ,
                                                    agmt_spec_dt ,
                                                    agmt_spec_txt
                                                    /* EIM_18145_End                                    */
                                           FROM     (
                                                              SELECT    cast(a.policynumber_stg AS VARCHAR(100)) AS policynumber,
                                                                        cast(a.publicid_stg AS     VARCHAR(100)) AS publicid,
                                                                        cast(c.typecode_stg AS     VARCHAR(100)) AS spec_type_cd,
                                                                        b.updatetime_stg                         AS spec_updatetime,
                                                                        b.effectivedate_stg                      AS effectivedate,
                                                                        b.expirationdate_stg                     AS expirationdate,
                                                                        a.updatetime_stg                         AS trans_strt_dttm,
                                                                        a.editeffectivedate_stg                  AS agmt_spec_strt_dttm,
                                                                        cast(''AGMT_SPEC1'' AS VARCHAR(100))       AS agmt_spec_type_cd,
                                                                        cast(NULL AS         VARCHAR(100))       AS agmt_spec_ind,
                                                                        cast(NULL AS         DATE )              AS agmt_spec_dt,
                                                                        cast(NULL AS         VARCHAR(255) )      AS agmt_spec_txt
                                                              FROM      db_t_prod_stag.pc_policyperiod a
                                                              join      db_t_prod_stag.pc_policyline b
                                                              ON        b.branchid_stg = a.id_stg
                                                              AND       b.expirationdate_stg IS NULL
                                                              join      db_t_prod_stag.pctl_policyperiodstatus ps
                                                              ON        ps.id_stg = a.status_stg
                                                              left join db_t_prod_stag.pctl_number_alfa c
                                                              ON        c.id_stg = b.latepaycount_alfa_stg
                                                              WHERE     ps.typecode_stg = ''Bound''
                                                              AND       b.updatetime_stg >:START_DTTM
                                                              AND       b.updatetime_stg <=:END_DTTM )agmt_spec_x
                                           WHERE    agmt_spec_type_cd =''AGMT_SPEC1'' 
										   qualify row_number() over ( PARTITION BY agmt_spec_x.publicid ORDER BY agmt_spec_x.spec_updatetime DESC) =1
                                           /* EIM_18145_Start                                    */
                                           UNION
                                           SELECT   policynumber,
                                                    publicid,
                                                    spec_type_cd,
                                                    effectivedate,
                                                    expirationdate,
                                                    trans_strt_dttm,
                                                    agmt_spec_strt_dttm,
                                                    agmt_spec_type_cd,
                                                    agmt_spec_ind,
                                                    agmt_spec_dt,
                                                    agmt_spec_txt
                                           FROM     (
                                                              SELECT    cast(a.policynumber_stg AS VARCHAR(100))                   AS policynumber,
                                                                        cast(a.publicid_stg AS     VARCHAR(100))                   AS publicid,
                                                                        cast(c.typecode_stg AS     VARCHAR(100))                   AS spec_type_cd,
                                                                        b.updatetime_stg                                           AS spec_updatetime,
                                                                        b.effectivedate_stg                                        AS effectivedate,
                                                                        b.expirationdate_stg                                       AS expirationdate,
                                                                        a.updatetime_stg                                           AS trans_strt_dttm,
                                                                        a.editeffectivedate_stg                                    AS agmt_spec_strt_dttm,
                                                                        ''AGMT_SPEC2''                                               AS agmt_spec_type_cd,
                                                                        cast(b.isnamedperilexistonpolicy_alfa_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                        cast(NULL AS                                 DATE )        AS agmt_spec_dt ,
                                                                        cast(NULL AS                                 VARCHAR(255) )AS agmt_spec_txt
                                                              FROM      db_t_prod_stag.pc_policyperiod a
                                                              join      db_t_prod_stag.pc_policyline b
                                                              ON        b.branchid_stg = a.id_stg
                                                              AND       b.expirationdate_stg IS NULL
                                                              join      db_t_prod_stag.pctl_policyperiodstatus ps
                                                              ON        ps.id_stg = a.status_stg
                                                              left join db_t_prod_stag.pctl_number_alfa c
                                                              ON        c.id_stg = b.latepaycount_alfa_stg
                                                              WHERE     ps.typecode_stg = ''Bound''
                                                              AND       cast(b.isnamedperilexistonpolicy_alfa_stg AS VARCHAR(100)) = ''1''
                                                              AND       b.updatetime_stg >:START_DTTM
                                                              AND       b.updatetime_stg <=:END_DTTM)agmt_spec_x
                                           WHERE    agmt_spec_type_cd =''AGMT_SPEC2'' 
										   qualify row_number() over ( PARTITION BY agmt_spec_x.publicid, agmt_spec_x.agmt_spec_type_cd, agmt_spec_x.effectivedate ORDER BY agmt_spec_x.spec_updatetime DESC) =1
                                           /* EIM_18145_End]                                    */
                                           UNION
                                           SELECT   policynumber,
                                                    publicid,
                                                    spec_type_cd,
                                                    effectivedate,
                                                    expirationdate,
                                                    trans_strt_dttm,
                                                    agmt_spec_strt_dttm,
                                                    agmt_spec_type_cd,
                                                    agmt_spec_ind ,
                                                    agmt_spec_dt ,
                                                    agmt_spec_txt
                                           FROM     (
                                                              SELECT    cast(a.policynumber_stg AS VARCHAR(100))        AS policynumber,
                                                                        cast(a.publicid_stg AS     VARCHAR(100))        AS publicid,
                                                                        cast(c.typecode_stg AS     VARCHAR(100))        AS spec_type_cd,
                                                                        b.updatetime_stg                                AS spec_updatetime,
                                                                        b.effectivedate_stg                             AS effectivedate,
                                                                        b.expirationdate_stg                            AS expirationdate,
                                                                        a.updatetime_stg                                AS trans_strt_dttm,
                                                                        a.editeffectivedate_stg                         AS agmt_spec_strt_dttm,
                                                                        cast(''PriorCarrierExpDate_alfa''AS VARCHAR(100)) AS agmt_spec_type_cd,
                                                                        cast(NULL AS                      VARCHAR(100)) AS agmt_spec_ind,
                                                                        pce.priorcarrierexpdate_alfa_stg                AS agmt_spec_dt,
                                                                        cast(NULL AS VARCHAR(255) )                     AS agmt_spec_txt
                                                              FROM      db_t_prod_stag.pc_policyperiod a
                                                              join      db_t_prod_stag.pc_effectivedatedfields pce
                                                              ON        pce.branchid_stg = a.id_stg
                                                              AND       pce.expirationdate_stg IS NULL
                                                              join      db_t_prod_stag.pc_policyline b
                                                              ON        b.branchid_stg = a.id_stg
                                                              AND       b.expirationdate_stg IS NULL
                                                              join      db_t_prod_stag.pctl_policyperiodstatus ps
                                                              ON        ps.id_stg = a.status_stg
                                                              left join db_t_prod_stag.pctl_number_alfa c
                                                              ON        c.id_stg = b.latepaycount_alfa_stg
                                                              WHERE     ps.typecode_stg = ''Bound''
                                                              AND       b.updatetime_stg >:START_DTTM
                                                              AND       b.updatetime_stg <=:END_DTTM
                                                              AND       pce.priorcarrierexpdate_alfa_stg IS NOT NULL )agmt_spec_x
                                           WHERE    agmt_spec_type_cd =''PriorCarrierExpDate_alfa'' 
										   qualify row_number() over (PARTITION BY agmt_spec_x.publicid, agmt_spec_x.agmt_spec_type_cd, agmt_spec_x.effectivedate ORDER BY agmt_spec_x.spec_updatetime DESC) =1
                                           /*Relationship DB_T_CORE_DM_PROD.discount counts of other DB_T_CORE_DM_PROD.policy types  */
                                           UNION
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
                                                                    join            db_t_prod_stag.pctl_papolicytype_alfa pa
                                                                    ON              pa.id_stg = pl.papolicytype_alfa_stg
                                                                    join            db_t_prod_stag.pc_personalvehicle ppv
                                                                    ON              ppv.branchid_stg = pp.id_stg
                                                                    join            db_t_prod_stag.pc_pamodifier dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             dis.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_relationshipdetails_alfa rldet
                                                                    ON              rldet.id_stg = pl.relationshipdetails_alfa_stg
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho3
                                                                    ON              nbrho3.id_stg = rldet.numho3pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho3.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrho4.typecode_stg        AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC4''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho4
                                                                    ON              nbrho4.id_stg = rldet.numho4pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho4.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrho5.typecode_stg        AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC5''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho5
                                                                    ON              nbrho5.id_stg = rldet.numho5pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho5.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrho6.typecode_stg        AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC6''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho6
                                                                    ON              nbrho6.id_stg = rldet.numho6pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho6.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrho8.typecode_stg        AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC7''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrho8
                                                                    ON              nbrho8.id_stg = rldet.numho8pol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrho8.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrsf.typecode_stg         AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC8''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrsf
                                                                    ON              nbrsf.id_stg = rldet.numsfpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrsf.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrmh.typecode_stg         AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC9''               AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrmh
                                                                    ON              nbrmh.id_stg = rldet.nummhpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrmh.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrlife.typecode_stg       AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC10''              AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrlife
                                                                    ON              nbrlife.id_stg = rldet.numlifepol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrlife.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg        AS policynumber,
                                                                                    pp.publicid_stg            AS publicid,
                                                                                    nbrfarm.typecode_stg       AS spec_type_cd,
                                                                                    pl.updatetime_stg          AS spec_updatetime,
                                                                                    pl.effectivedate_stg       AS effectivedate,
                                                                                    pl.expirationdate_stg      AS expirationdate,
                                                                                    pp.updatetime_stg          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC11''              AS agmt_spec_type_cd,
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
                                                                    left join       db_t_prod_stag.pctl_number_alfa nbrfarm
                                                                    ON              nbrfarm.id_stg = rldet.numfarmpol_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             nbrfarm.typecode_stg IS NOT NULL
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    /*PMOP -3049*/
                                                                    UNION
                                                                    SELECT a.policynumber_stg                             AS policynumber,
                                                                           a.publicid_stg                                 AS publicid,
                                                                           cast( NULL AS VARCHAR(100))                    AS spec_type_cd,
                                                                           a.updatetime_stg                               AS spec_updatetime,
                                                                           pce.effectivedate_stg                          AS effectivedate,
                                                                           pce.expirationdate_stg                         AS expirationdate,
                                                                           a.updatetime_stg                               AS trans_strt_dttm,
                                                                           a.editeffectivedate_stg                        AS agmt_spec_strt_dttm,
                                                                           cast( ''LatestDtOfLapses_alfa'' AS VARCHAR(100)) AS agmt_spec_type_cd,
                                                                           cast(NULL AS                     VARCHAR(100)) AS agmt_spec_ind,
                                                                           pce.latestdtoflapses_alfa_stg                  AS agmt_spec_dt ,
                                                                           cast(NULL AS VARCHAR(255) )                    AS agmt_spec_txt
                                                                    FROM   db_t_prod_stag.pc_policyperiod a
                                                                    join   db_t_prod_stag.pc_effectivedatedfields pce
                                                                    ON     pce.branchid_stg = a.id_stg
                                                                    AND    pce.expirationdate_stg IS NULL
                                                                    join   db_t_prod_stag.pctl_policyperiodstatus ps
                                                                    ON     ps.id_stg = a.status_stg
                                                                    WHERE  ps.typecode_stg = ''Bound''
                                                                    AND    a.updatetime_stg >:START_DTTM
                                                                    AND    a.updatetime_stg <=:END_DTTM
                                                                    AND    pce.latestdtoflapses_alfa_stg IS NOT NULL
                                                                    UNION
                                                                    SELECT a.policynumber_stg                                 AS policynumber,
                                                                           a.publicid_stg                                     AS publicid,
                                                                           cast(lapsesincontservice_alfa_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                           a.updatetime_stg                                   AS spec_updatetime,
                                                                           pce.effectivedate_stg                              AS effectivedate,
                                                                           pce.expirationdate_stg                             AS expirationdate,
                                                                           a.updatetime_stg                                   AS trans_strt_dttm,
                                                                           a.editeffectivedate_stg                            AS agmt_spec_strt_dttm,
                                                                           ''LapsesInContService_alfa''                         AS agmt_spec_type_cd,
                                                                           cast(NULL AS VARCHAR(100))                         AS agmt_spec_ind,
                                                                           cast(NULL AS DATE )                                AS agmt_spec_dt ,
                                                                           cast(NULL AS VARCHAR(255) )                        AS agmt_spec_txt
                                                                    FROM   db_t_prod_stag.pc_policyperiod a
                                                                    join   db_t_prod_stag.pc_effectivedatedfields pce
                                                                    ON     pce.branchid_stg = a.id_stg
                                                                    AND    pce.expirationdate_stg IS NULL
                                                                    join   db_t_prod_stag.pctl_policyperiodstatus ps
                                                                    ON     ps.id_stg = a.status_stg
                                                                    WHERE  ps.typecode_stg = ''Bound''
                                                                    AND    a.updatetime_stg >:START_DTTM
                                                                    AND    a.updatetime_stg <=:END_DTTM
                                                                    AND    pce.lapsesincontservice_alfa_stg IS NOT NULL
                                                                    /*PMOP -3049*/
                                                                    UNION
                                                                    /* EIM-35945 Added Watercraft and DB_T_STAG_MEMBXREF_PROD.Umbrella relationship DB_T_CORE_DM_PROD.discount                                     */
                                                                    SELECT DISTINCT pp.policynumber_stg                                     AS policynumber,
                                                                                    pp.publicid_stg                                         AS publicid,
                                                                                    cast(pl.isrelationdiseligible_alfa_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                                       AS spec_updatetime,
                                                                                    pl.effectivedate_stg                                    AS effectivedate,
                                                                                    pl.expirationdate_stg                                   AS expirationdate,
                                                                                    pp.updatetime_stg                                       AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                                AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC13''                                           AS agmt_spec_type_cd,
                                                                                    cast(rldet.watercraftind_stg AS VARCHAR(100))           AS agmt_spec_ind,
                                                                                    cast(NULL AS                    DATE)                   AS agmt_spec_dt ,
                                                                                    cast(NULL AS                    VARCHAR(255) )          AS agmt_spec_txt
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
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             rldet.watercraftind_stg <> 0
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                                    /*PMOP-54878 Personal DB_T_STAG_MEMBXREF_PROD.Umbrella*/ /*  AGMT_SPEC*/
                                                                                    
                                                                    UNION
                                                                    SELECT pp.policynumber_stg        AS policynumber,
                                                                           pp.publicid_stg            AS publicid,
                                                                           cast(NULL AS VARCHAR(100)) AS spec_type_cd,
                                                                           pl.updatetime_stg          AS spec_updatetime,
                                                                           pl.effectivedate_stg       AS effectivedate,
                                                                           pl.expirationdate_stg      AS expirationdate,
                                                                           pp.updatetime_stg          AS trans_strt_dttm,
                                                                           pp.editeffectivedate_stg   AS agmt_spec_strt_dttm,
                                                                           ''IsSubmitted''              AS agmt_spec_type_cd,
                                                                           cast(
                                                                           CASE
                                                                                  WHEN issubmitted_stg=1 THEN NULL
                                                                                  ELSE ''Yes''
                                                                           END AS       VARCHAR(100))    agmt_spec_ind,
                                                                           cast(NULL AS DATE)         AS agmt_spec_dt ,
                                                                           cast(NULL AS VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM   db_t_prod_stag.pc_policyperiod pp
                                                                    join   db_t_prod_stag.pc_policyline pl
                                                                    ON     pl.branchid_stg = pp.id_stg
                                                                    AND    pl.expirationdate_stg IS NULL
                                                                    AND    pp.status_stg = 9
                                                                    join   db_t_prod_stag.pc_policy pc
                                                                    ON     pp.policyid_stg = pc.id_stg
                                                                    join   db_t_prod_stag.pcx_puprenreviewdetails_alfa pup
                                                                    ON     pup.associatedpuppolicy_stg =pc.id_stg
                                                                    WHERE  pp.updatetime_stg >:START_DTTM
                                                                    AND    pp.updatetime_stg <=:END_DTTM
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg                                     AS policynumber,
                                                                                    pp.publicid_stg                                         AS publicid,
                                                                                    cast(pl.isrelationdiseligible_alfa_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                                       AS spec_updatetime,
                                                                                    pl.effectivedate_stg                                    AS effectivedate,
                                                                                    pl.expirationdate_stg                                   AS expirationdate,
                                                                                    pp.updatetime_stg                                       AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                                AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC14''                                           AS agmt_spec_type_cd,
                                                                                    cast(rldet.umbrellaind_stg AS VARCHAR(100))             AS agmt_spec_ind,
                                                                                    cast(NULL AS                  DATE)                     AS agmt_spec_dt ,
                                                                                    cast(NULL AS                  VARCHAR(255) )            AS agmt_spec_txt
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
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PARelationshipDiscount_alfa''
                                                                    AND             rldet.umbrellaind_stg <> 0
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    /* -EIM-50809 AutoInd union */
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg                                             AS policynumber,
                                                                                    pp.publicid_stg                                                 AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))                                     AS spec_type_cd,
                                                                                    pl.updatetime_stg                                               AS spec_updatetime,
                                                                                    pl.effectivedate_stg                                            AS effectivedate,
                                                                                    pl.expirationdate_stg                                           AS expirationdate,
                                                                                    pp.updatetime_stg                                               AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                                        AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC24''                                                   AS agmt_spec_type_cd,
                                                                                    cast(pcx_horelationshipdetail_alfa.autoind_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                                      DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                                      VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe
                                                                    ON              pctl_hopolicytype_hoe.id_stg = pl.hopolicytype_stg
                                                                    join            db_t_prod_stag.pcx_homodifier_hoe
                                                                    ON              pcx_homodifier_hoe.branchid_stg = pp.id_stg
                                                                    AND             pcx_homodifier_hoe.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pcx_horelationshipdetail_alfa
                                                                    ON              pcx_horelationshipdetail_alfa.id_stg = pl.horelationshipdetails_alfa_stg
                                                                    WHERE           pcx_homodifier_hoe.eligible_stg = 1
                                                                    AND             pcx_homodifier_hoe.patterncode_stg = ''HORelationshipDisc_alfa''
                                                                    AND             pcx_horelationshipdetail_alfa.autoind_stg <> 0
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    /* EIM-50810 */
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg                      AS policynumber,
                                                                                    pp.publicid_stg                          AS publicid,
                                                                                    cast( pctl.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                        AS spec_updatetime,
                                                                                    pl.effectivedate_stg                     AS effectivedate,
                                                                                    pl.expirationdate_stg                    AS expirationdate,
                                                                                    pp.updatetime_stg                        AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                 AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC23'' AS VARCHAR(100))      AS agmt_spec_type_cd,
                                                                                    cast(NULL AS          VARCHAR(100))      AS agmt_spec_ind,
                                                                                    cast(NULL AS          DATE)              AS agmt_spec_dt ,
                                                                                    cast(NULL AS          VARCHAR(255) )     AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                    join            db_t_prod_stag.pcx_fopfarmingoperations pcx
                                                                    ON              pcx.branchid_stg = pp.id_stg
                                                                    left join       db_t_prod_stag.pctl_fopfarmingoperations pctl
                                                                    ON              pctl.id_stg = pcx.farmingoperationtype_stg
                                                                    WHERE           pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    /* EIM-35945 ends here                                    */
                                                                    /*EIM-36361 STARTS HERE*/
																	/* -Add new INFORMATION_SCHEMA.fields to DB_T_STAG_MEMBXREF_PROD.Life DB_T_CORE_DM_PROD.discount                                    */
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg                    AS policynumber,
                                                                                    pp.publicid_stg                        AS publicid,
                                                                                    cast(pct.typecode_stg AS VARCHAR(100)) AS spec_type_cd,
                                                                                    pl.updatetime_stg                      AS spec_updatetime,
                                                                                    pl.effectivedate_stg                   AS effectivedate,
                                                                                    pl.expirationdate_stg                  AS expirationdate,
                                                                                    pp.updatetime_stg                      AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg               AS agmt_spec_strt_dttm,
                                                                                    ''AGMT_SPEC15''                          AS agmt_spec_type_cd,
                                                                                    cast(NULL AS VARCHAR(100))             AS agmt_spec_ind,
                                                                                    cast(NULL AS DATE )                    AS agmt_spec_dt ,
                                                                                    cast(NULL AS VARCHAR(255) )            AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             (
                                                                                                    pl.expirationdate_stg IS NULL
                                                                                    OR              pl.expirationdate_stg >
                                                                                                    CASE
                                                                                                                    WHEN pp.editeffectivedate_stg >= pp.modeldate_stg THEN pp.editeffectivedate_stg
                                                                                                                    ELSE pp.modeldate_stg
                                                                                                    END)
                                                                    join            db_t_prod_stag.pc_pamodifier dis
                                                                    ON              dis.branchid_stg = pp.id_stg
                                                                    AND             (
                                                                                                    dis.expirationdate_stg IS NULL
                                                                                    OR              dis.expirationdate_stg >
                                                                                                    CASE
                                                                                                                    WHEN pp.editeffectivedate_stg >= pp.modeldate_stg THEN pp.editeffectivedate_stg
                                                                                                                    ELSE pp.modeldate_stg
                                                                                                    END)
                                                                    join            db_t_prod_stag.pctl_number_alfa pct
                                                                    ON              pct.id_stg = pl.numoflifepolicies_alfa_stg
                                                                    WHERE           dis.eligible_stg = 1
                                                                    AND             dis.patterncode_stg = ''PALifePolicyDiscount_alfa''
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over (PARTITION BY policynumber, publicid ORDER BY pl.expirationdate_stg ASC, dis.expirationdate_stg ASC, pp.editeffectivedate_stg DESC, pp.modeldate_stg DESC) =1
                                                                    /*EIM-48134*/
                                                                    UNION
                                                                    SELECT DISTINCT pp.policynumber_stg                        AS policynumber,
                                                                                    pp.publicid_stg                            AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))                AS spec_type_cd,
                                                                                    pce.updatetime_stg                         AS spec_updatetime,
                                                                                    pce.effectivedate_stg                      AS effectivedate,
                                                                                    cast(''9999-12-31'' AS DATE)                 AS expirationdate,
                                                                                    pp.updatetime_stg                          AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg                   AS agmt_spec_strt_dttm,
                                                                                    cast( ''PriorCarrierOther'' AS  VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(NULL AS                  VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                  DATE )        AS agmt_spec_dt ,
                                                                                    cast(othercarrier_alfa_stg AS VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_job pj
                                                                    ON              pj.id_stg=pp.jobid_stg
                                                                    join            db_t_prod_stag.pc_effectivedatedfields pce
                                                                    ON              pce.branchid_stg = pp.id_stg
                                                                    AND             pce.expirationdate_stg IS NULL
                                                                    join            db_t_prod_stag.pctl_policyperiodstatus ps
                                                                    ON              ps.id_stg = pp.status_stg
                                                                    WHERE           ps.typecode_stg = ''Bound''
                                                                    AND             pce.othercarrier_alfa_stg IS NOT NULL
                                                                    AND             pce.updatetime_stg >:START_DTTM
                                                                    AND             pce.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber,publicid ORDER BY pce.updatetime_stg DESC)=1
                                                                                    /*EIM-48134 ENDS*/
                                                    ) agmt_spec_type_cd_x 
													qualify row_number() over (PARTITION BY agmt_spec_type_cd_x.publicid, agmt_spec_type_cd_x.agmt_spec_type_cd, agmt_spec_type_cd_x.effectivedate ORDER BY agmt_spec_type_cd_x.spec_updatetime DESC) =1
                                           UNION
                                           /** Farm policies EIM-48821**/
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
                                                                    /* BlanketCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC16'' AS                 VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.blanketcoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                          DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                          VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.blanketcoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* DwellingCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC17'' AS                  VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.dwellingcoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                           DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                           VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.dwellingcoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* FeedAndSeedCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC18'' AS                     VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.feedandseedcoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                              DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                              VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.feedandseedcoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* LiabilityCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC19'' AS                   VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.liabilitycoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                            DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                            VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.liabilitycoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* LivestockCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC20'' AS                   VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.livestockcoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                            DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                            VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.livestockcoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* MachineryCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC21'' AS                   VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.machinerycoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                            DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                            VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.machinerycoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1
                                                                    UNION
                                                                    /* OutbuildingCoverableExists */
                                                                    SELECT DISTINCT pp.policynumber_stg                 AS policynumber,
                                                                                    pp.publicid_stg                     AS publicid,
                                                                                    cast( NULL AS VARCHAR(100))         AS spec_type_cd,
                                                                                    pl.updatetime_stg                   AS spec_updatetime,
                                                                                    pl.effectivedate_stg                AS effectivedate,
                                                                                    pl.expirationdate_stg               AS expirationdate,
                                                                                    pp.updatetime_stg                   AS trans_strt_dttm,
                                                                                    pp.editeffectivedate_stg            AS agmt_spec_strt_dttm,
                                                                                    cast(''AGMT_SPEC22'' AS                     VARCHAR(100)) AS agmt_spec_type_cd,
                                                                                    cast(pl.outbuildingcoverableexists_stg AS VARCHAR(100)) AS agmt_spec_ind,
                                                                                    cast(NULL AS                              DATE)         AS agmt_spec_dt ,
                                                                                    cast(NULL AS                              VARCHAR(255) )AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    WHERE           pl.outbuildingcoverableexists_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM 
																	qualify row_number() over(PARTITION BY policynumber, publicid ORDER BY spec_updatetime DESC)=1 )agmt_spec_y 
																	qualify row_number() over (PARTITION BY agmt_spec_y.publicid, agmt_spec_y.agmt_spec_type_cd, agmt_spec_y.effectivedate ORDER BY agmt_spec_y.spec_updatetime DESC) =1
                                           UNION
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
                                                                    /* EIM-49230 */
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
                                                                                    cast(pl.issftierrating_alfa_stg AS VARCHAR(100))       AS agmt_spec_ind,
                                                                                    cast(NULL AS                       DATE)               AS agmt_spec_dt ,
                                                                                    cast(NULL AS                       VARCHAR(255) )      AS agmt_spec_txt
                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                    join            db_t_prod_stag.pc_policyline pl
                                                                    ON              pl.branchid_stg = pp.id_stg
                                                                    AND             pl.expirationdate_stg IS NULL
                                                                    AND             pp.status_stg = 9
                                                                                    /*  added as per logic in existing union. */
                                                                    WHERE           pl.patterncode_stg = ''HomeownersLine_HOE''
                                                                    AND             pl.issftierrating_alfa_stg = 1
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM
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
                                                                    AND             pl.updatetime_stg >:START_DTTM
                                                                    AND             pl.updatetime_stg <=:END_DTTM )agmt_spec_type_cd_z 
																	qualify row_number() over (PARTITION BY agmt_spec_type_cd_z.publicid, agmt_spec_type_cd_z.agmt_spec_type_cd, agmt_spec_type_cd_z.effectivedate ORDER BY agmt_spec_type_cd_z.spec_updatetime DESC) =1 ) src ) );
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
                      :P_AGMT_TYPE_CD_POLICY_VERSION   AS out_agmt_type_cd,
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
            AND       lkp.term_num = :in_term_num
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
                        :prcs_id                                                              AS out_prcs_id,
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

END;
';