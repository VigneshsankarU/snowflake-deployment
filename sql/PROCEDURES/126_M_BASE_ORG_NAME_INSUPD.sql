-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_ORG_NAME_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       prcs_id STRING;
	v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       prcs_id := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''\\$\\$WKLT_PRCS_ID'' LIMIT 1);
	v_start_time := CURRENT_TIMESTAMP();

  -- Component LKP_BUSN, Type Prerequisite Lookup Object
  CREATE OR replace TEMPORARY TABLE lkp_busn AS
  (
           SELECT   busn.busn_prty_id     AS busn_prty_id,
                    busn.src_sys_cd       AS src_sys_cd,
                    busn.tax_brakt_cd     AS tax_brakt_cd,
                    busn.org_type_cd      AS org_type_cd,
                    busn.gics_sbidstry_cd AS gics_sbidstry_cd,
                    busn.lifcycl_cd       AS lifcycl_cd,
                    busn.prty_type_cd     AS prty_type_cd,
                    busn.busn_end_dttm    AS busn_end_dttm,
                    busn.busn_strt_dttm   AS busn_strt_dttm,
                    busn.inc_ind          AS inc_ind,
                    busn.edw_strt_dttm    AS edw_strt_dttm,
                    busn.edw_end_dttm     AS edw_end_dttm,
                    busn.busn_ctgy_cd     AS busn_ctgy_cd,
                    busn.nk_busn_cd       AS nk_busn_cd
           FROM     db_t_prod_core.busn qualify row_number () over (PARTITION BY nk_busn_cd,busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1 );
  -- Component LKP_INTRNL_ORG, Type Prerequisite Lookup Object
  create or replace TEMPORARY TABLE lkp_intrnl_org AS
  (
           SELECT   intrnl_org.intrnl_org_prty_id   AS intrnl_org_prty_id,
                    intrnl_org.intrnl_org_type_cd   AS intrnl_org_type_cd,
                    intrnl_org.intrnl_org_sbtype_cd AS intrnl_org_sbtype_cd,
                    intrnl_org.intrnl_org_num       AS intrnl_org_num,
                    intrnl_org.src_sys_cd           AS src_sys_cd
           FROM     db_t_prod_core.intrnl_org qualify row_number () over (PARTITION BY intrnl_org_num,intrnl_org_type_cd,intrnl_org_sbtype_cd,src_sys_cd ORDER BY edw_end_dttm DESC)=1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY, Type Prerequisite Lookup Object
  create or replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_busn_ctgy AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm IN (''BUSN_CTGY'',
                                                         ''ORG_TYPE'',
                                                         ''PRTY_TYPE'')
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''derived'',
                                                         ''cctl_contact.typecode'',
                                                         ''cctl_contact.name'',
                                                         ''abtl_abcontact.name'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE, Type Prerequisite Lookup Object
  create or replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_intrnl_org_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INTRNL_ORG_SBTYPE'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE, Type Prerequisite Lookup Object
  create or replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_intrnl_org_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INTRNL_ORG_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object
  create or replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_cctl_salvageyard_alfa, Type SOURCE
  create or replace TEMPORARY TABLE sq_cctl_salvageyard_alfa AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS typecode,
                $2  AS name_type_cd,
                $3  AS name,
                $4  AS description,
                $5  AS req,
                $6  AS src_strt_dt,
                $7  AS src_end_dt,
                $8  AS trans_strt_dt,
                $9  AS sys_src_cd,
                $10 AS retired,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cast(
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  mortgageelienholdernumber_alfa_stg IS NOT NULL
                                                                                                  AND             ab_abcontact.linkid_stg NOT LIKE ''%MORT%''
                                                                                                  AND             ab_abcontact.linkid_stg NOT LIKE ''%IRS%''
                                                                                                  AND             ab_abcontact.source_stg = ''ContactManager'' )THEN mortgageelienholdernumber_alfa_stg
                                                                                  WHEN (
                                                                                                                  ab_abcontact.source_stg = ''ContactManager'' )THEN ab_abcontact.linkid_stg
                                                                                  WHEN ab_abcontact.source_stg = ''ClaimCenter'' THEN ab_abcontact.publicid_stg
                                                                                  ELSE ''UNK''
                                                                  END AS VARCHAR(100))  AS typecode_stg,
                                                                  ''DBA''                 AS name_type_cd_stg,
                                                                  ab_abcontact.name_stg AS org_name_stg,
                                                                  ab_abcontact.name_stg,
                                                                  cast(ab_abcontact.tl_cnt_name_stg AS VARCHAR(50))  AS req_stg,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                  CASE
                                                                                  WHEN ab_abcontact.updatetime_stg IS NULL THEN to_date(''19000101'', ''YYYYMMDD'')
                                                                                  ELSE ab_abcontact.updatetime_stg
                                                                  END         AS trans_strt_dt_stg,
                                                                  ''SRC_SYS7''  AS src_sys_cd_stg,
                                                                  retired_stg AS retired_stg
                                                  FROM            (
                                                                                  SELECT          cast( ''ClaimCenter'' AS          VARCHAR(20))  AS source_stg,
                                                                                                  cast(NULL AS                    VARCHAR(100)) AS mortgageelienholdernumber_alfa_stg,
                                                                                                  cast(bc_contact.publicid_stg AS VARCHAR(100)) AS publicid_stg,
                                                                                                  bc_contact.updatetime_stg,
                                                                                                  cast(bc_contact.addressbookuid_stg AS VARCHAR(100))AS linkid_stg,
                                                                                                  bctl_contact.name_stg                              AS tl_cnt_name_stg,
                                                                                                  bc_contact.name_stg,
                                                                                                  ''SRC_SYS5'' AS sys_src_cd_stg,
                                                                                                  bc_contact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_contact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime_stg
                                                                                  FROM            db_t_prod_stag.bc_contact
                                                                                  left outer join db_t_prod_stag.bctl_contact
                                                                                  ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                  left outer join db_t_prod_stag.bctl_gendertype
                                                                                  ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                  ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxstatus
                                                                                  ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                  ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_nameprefix
                                                                                  ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_namesuffix
                                                                                  ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.bc_user
                                                                                  ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.bc_credential
                                                                                  ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                  WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                  AND
                                                                                                  /*below condition added to avoid duplicates*/
                                                                                                  bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                  ''systemTables:1'',
                                                                                                                                  ''systemTables:2'')
                                                                                  AND             ((
                                                                                                                                  bc_contact.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_contact.updatetime_stg <=(cast(:end_dttm AS timestamp)))
                                                                                                  OR              (
                                                                                                                                  bc_user.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_user.updatetime_stg <= (cast(:end_dttm AS timestamp))))
                                                                                  UNION
                                                                                  SELECT          cast( ''ClaimCenter'' AS VARCHAR(20))  AS source_stg,
                                                                                                  cast(NULL AS           VARCHAR(100)) AS mortgageelienholdernumber_alfa_stg,
                                                                                                  cast(
                                                                                                  CASE
                                                                                                                  WHEN(
                                                                                                                                                  bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                  ELSE bc_contact.publicid_stg
                                                                                                  END AS VARCHAR(100))AS publicid_stg,
                                                                                                  bc_contact.updatetime_stg,
                                                                                                  cast(bc_contact.addressbookuid_stg AS VARCHAR(100))AS linkid_stg,
                                                                                                  bctl_contact.name_stg                              AS tl_cnt_name_stg,
                                                                                                  bc_contact.name_stg,
                                                                                                  ''SRC_SYS5'' AS sys_src_cd_stg,
                                                                                                  bc_contact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_contact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime_stg
                                                                                  FROM            db_t_prod_stag.bc_account a
                                                                                  inner join      db_t_prod_stag.bc_accountcontact h
                                                                                  ON              h.accountid_stg = a.id_stg
                                                                                  inner join      db_t_prod_stag.bc_contact
                                                                                  ON              bc_contact.id_stg = h.contactid_stg
                                                                                  join            db_t_prod_stag.bctl_contact
                                                                                  ON              bctl_contact.id_stg =bc_contact.subtype_stg
                                                                                  left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                  ON              i.accountcontactid_stg = h.id_stg
                                                                                  left join       db_t_prod_stag.bctl_accountrole j
                                                                                  ON              j.id_stg = i.role_stg
                                                                                  left outer join db_t_prod_stag.bctl_gendertype
                                                                                  ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                  ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxstatus
                                                                                  ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                  ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_nameprefix
                                                                                  ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_namesuffix
                                                                                  ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.bc_user
                                                                                  ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.bc_credential
                                                                                  ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                  WHERE           ((
                                                                                                                                  h.primarypayer_stg = 1)
                                                                                                  OR              (
                                                                                                                                  j.name_stg = ''Payer''))
                                                                                  AND             ((
                                                                                                                                  bc_contact.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_contact.updatetime_stg <=(cast(:end_dttm AS timestamp)))
                                                                                                  OR              (
                                                                                                                                  bc_user.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_user.updatetime_stg <= (cast(:end_dttm AS timestamp))))
                                                                                  UNION
                                                                                  SELECT          cast( ''ClaimCenter'' AS VARCHAR(20))  AS source_stg,
                                                                                                  cast(NULL AS           VARCHAR(100)) AS mortgageelienholdernumber_alfa_stg,
                                                                                                  cast(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                  ELSE bc_contact.externalid_stg
                                                                                                  END AS VARCHAR(100))AS publicid_stg,
                                                                                                  bc_contact.updatetime_stg,
                                                                                                  cast(bc_contact.addressbookuid_stg AS VARCHAR(100))AS linkid_stg,
                                                                                                  bctl_contact.name_stg                              AS tl_cnt_name_stg,
                                                                                                  bc_contact.name_stg,
                                                                                                  ''SRC_SYS5'' AS sys_src_cd_stg,
                                                                                                  bc_contact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_contact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(bc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)) >= coalesce(bc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN bc_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime
                                                                                  FROM            db_t_prod_stag.bc_account a
                                                                                  inner join      db_t_prod_stag.bc_invoicestream b
                                                                                  ON              a.id_stg = b.accountid_stg
                                                                                  inner join      db_t_prod_stag.bc_accountcontact c
                                                                                  ON              c.accountid_stg=a.id_stg
                                                                                  inner join      db_t_prod_stag.bc_contact
                                                                                  ON              bc_contact.id_stg = c.contactid_stg
                                                                                  join            db_t_prod_stag.bctl_contact
                                                                                  ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                  left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                  ON              f.accountcontactid_stg = c.id_stg
                                                                                  left join       db_t_prod_stag.bctl_accountrole g
                                                                                  ON              g.id_stg = f.role_stg
                                                                                  left outer join db_t_prod_stag.bctl_gendertype
                                                                                  ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                  ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_taxstatus
                                                                                  ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                  ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_nameprefix
                                                                                  ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_namesuffix
                                                                                  ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.bc_user
                                                                                  ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.bc_credential
                                                                                  ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                  WHERE           ((
                                                                                                                                  b.overridingpayer_alfa_stg IS NULL
                                                                                                                  AND             c.primarypayer_stg = 1)
                                                                                                  OR              (
                                                                                                                                  b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                  AND             ((
                                                                                                                                  bc_contact.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_contact.updatetime_stg <=(cast(:end_dttm AS timestamp)))
                                                                                                  OR              (
                                                                                                                                  bc_user.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             bc_user.updatetime_stg <= (cast(:end_dttm AS timestamp))))
                                                                                  UNION
                                                                                  SELECT          cast( ''ClaimCenter'' AS VARCHAR(20))  AS source_stg,
                                                                                                  cast(NULL AS           VARCHAR(100)) AS mortgageelienholdernumber_alfa_stg,
                                                                                                  pc_contact.publicid_stg,
                                                                                                  pc_contact.updatetime_stg,
                                                                                                  pc_contact.addressbookuid_stg AS linkid_stg,
                                                                                                  pctl_contact.name_stg         AS tl_cnt_name_stg,
                                                                                                  pc_contact.name_stg,
                                                                                                  ''SRC_SYS4'' AS sys_src_cd_stg,
                                                                                                  pc_contact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(pc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(pc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN pc_contact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(pc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(pc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN pc_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(pc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(pc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(pc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN pc_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime_stg
                                                                                  FROM            db_t_prod_stag.pc_contact
                                                                                  left outer join db_t_prod_stag.pctl_contact
                                                                                  ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                  left outer join db_t_prod_stag.pctl_gendertype
                                                                                  ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.pctl_taxfilingstatustype
                                                                                  ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.pctl_taxstatus
                                                                                  ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.pctl_maritalstatus
                                                                                  ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.pctl_nameprefix
                                                                                  ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.pctl_namesuffix
                                                                                  ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.pc_user
                                                                                  ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.pc_credential
                                                                                  ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  left outer join db_t_prod_stag.pc_producercode
                                                                                  ON              pc_producercode.id_stg = pc_effectivedatedfields.producercodeid_stg
                                                                                  WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                  AND
                                                                                                  /*  below condition added to avoid duplicates */
                                                                                                  pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                  ''systemTables:1'',
                                                                                                                                  ''systemTables:2'')
                                                                                  AND             ((
                                                                                                                                  pc_contact.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             pc_contact.updatetime_stg <= (cast(:end_dttm AS timestamp)))
                                                                                                  OR              (
                                                                                                                                  pc_user.updatetime_stg>(cast(:start_dttm AS timestamp))
                                                                                                                  AND             pc_user.updatetime_stg <= (cast(:end_dttm AS timestamp))))
                                                                                  UNION
                                                                                  SELECT          cast( ''ClaimCenter'' AS VARCHAR(20)) AS source_stg,
                                                                                                  cast(NULL AS           VARCHAR(50)) AS mortgageelienholdernumber_alfa_stg,
                                                                                                  cc_contact.publicid_stg,
                                                                                                  cc_contact.updatetime_stg,
                                                                                                  cc_contact.addressbookuid_stg AS linkid_stg,
                                                                                                  cctl_contact.name_stg         AS tl_cnt_name_stg,
                                                                                                  cc_contact.name_stg,
                                                                                                  ''SRC_SYS6'' AS sys_src_cd_stg,
                                                                                                  cc_contact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(cc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(cc_contact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN cc_contact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(cc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(cc_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN cc_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(cc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_contact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(cc_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(cc_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN cc_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime_stg
                                                                                  FROM            db_t_prod_stag.cc_contact
                                                                                  left outer join db_t_prod_stag.cctl_contact
                                                                                  ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                  left outer join db_t_prod_stag.cctl_gendertype
                                                                                  ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.cctl_taxfilingstatustype
                                                                                  ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.cctl_taxstatus
                                                                                  ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.cctl_maritalstatus
                                                                                  ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.cctl_nameprefix
                                                                                  ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.cctl_namesuffix
                                                                                  ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.cc_user
                                                                                  ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                  left outer join db_t_prod_stag.cc_credential
                                                                                  ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                  left outer join db_t_prod_stag.cc_claimcontact
                                                                                  ON              cc_contact.id_stg = cc_claimcontact.contactid_stg
                                                                                  left outer join db_t_prod_stag.cc_claimcontactrole
                                                                                  ON              cc_claimcontact.id_stg = cc_claimcontactrole.claimcontactid_stg
                                                                                  left outer join db_t_prod_stag.cc_incident
                                                                                  ON              cc_claimcontactrole.claimcontactid_stg = cc_incident.id_stg
                                                                                  WHERE           (
                                                                                                                  cc_contact.updatetime_stg>(:start_dttm)
                                                                                                  AND             cc_contact.updatetime_stg <= (:end_dttm) )
                                                                                  OR              (
                                                                                                                  cc_user.updatetime_stg>(:start_dttm)
                                                                                                  AND             cc_user.updatetime_stg <=(:end_dttm) )
                                                                                  UNION
                                                                                  SELECT          cast(''ContactManager'' AS VARCHAR(20)) AS source_stg,
                                                                                                  ab_abcontact.mortgageelienholdernumber_alfa_stg ,
                                                                                                  cast(NULL AS VARCHAR(20)) publicid_stg,
                                                                                                  ab_abcontact.updatetime_stg,
                                                                                                  ab_abcontact.linkid_stg,
                                                                                                  abtl_abcontact.name_stg AS tl_cnt_name_stg,
                                                                                                  ab_abcontact.name_stg,
                                                                                                  ''SRC_SYS7'' AS sys_src_cd_stg,
                                                                                                  ab_abcontact.retired_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(ab_abcontact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_credential.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(ab_abcontact.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_user.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN ab_abcontact.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(ab_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_user.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(ab_credential.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_abcontact.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN ab_credential.updatetime_stg
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(ab_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_abcontact.updatetime_stg, cast(''1900-12-31'' AS DATE)))
                                                                                                                  AND             (
                                                                                                                                                  coalesce(ab_user.updatetime_stg,cast(''1900-12-31'' AS DATE)) >= coalesce(ab_credential.updatetime_stg, cast(''1900-12-31'' AS DATE))) THEN ab_user.updatetime_stg
                                                                                                  END AS prty_idntftn_updatetime_stg
                                                                                  FROM            db_t_prod_stag.ab_abcontact
                                                                                  left outer join db_t_prod_stag.abtl_abcontact
                                                                                  ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                  left outer join db_t_prod_stag.abtl_gendertype
                                                                                  ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_taxfilingstatustype
                                                                                  ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_taxstatus
                                                                                  ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_maritalstatus
                                                                                  ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_nameprefix
                                                                                  ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_namesuffix
                                                                                  ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                  left outer join db_t_prod_stag.ab_user
                                                                                  ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                  left outer join db_t_prod_stag.ab_credential
                                                                                  ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                  left outer join db_t_prod_stag.abtl_occupation
                                                                                  ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                  WHERE           ab_abcontact.updatetime_stg>cast(:start_dttm AS timestamp)
                                                                                  AND             ab_abcontact.updatetime_stg <= cast(:end_dttm AS timestamp) ) ab_abcontact
                                                  WHERE           tl_cnt_name_stg IN (''Company'',
                                                                                      ''Vendor (Company)'',
                                                                                      ''AUTO Repair Shop'',
                                                                                      ''AUTO Towing Agcy'',
                                                                                      ''Law Firm'',
                                                                                      ''Medical Care Organization'',
                                                                                      ''Lodging (Company)'',
                                                                                      ''Lodging Provider (Org)'')
                                                  AND             ab_abcontact.name_stg IS NOT NULL qualify row_number() over( PARTITION BY typecode_stg, req_stg ORDER BY trans_strt_dt_stg DESC) =1
                                                  UNION
                                                  SELECT DISTINCT upper(typecode_stg)                                AS typecode_stg,
                                                                  ''DBA''                                              AS name_type_cd_stg,
                                                                  name_stg                                           AS org_name_stg,
                                                                  description_stg                                    AS org_name_desc_stg,
                                                                  ''INSURANCE CARRIER''                                AS req_stg,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS trans_strt_dt_stg,
                                                                  ''SRC_SYS4''                                         AS src_sys_cd_stg,
                                                                  retired_stg                                        AS retired_stg
                                                  FROM            db_t_prod_stag.pctl_priorcarrier_alfa
                                                  UNION
                                                  SELECT DISTINCT upper(typecode_stg)                                AS typecode_stg,
                                                                  ''DBA''                                              AS name_type_cd_stg,
                                                                  name_stg                                           AS org_name_stg,
                                                                  description_stg                                    AS org_name_desc_stg,
                                                                  ''SALVG''                                            AS req_stg,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS trans_strt_dt_stg,
                                                                  ''SRC_SYS6''                                         AS src_sys_cd_stg,
                                                                  retired_stg                                        AS retired_stg
                                                  FROM            db_t_prod_stag.cctl_salvageyard_alfa
                                                  UNION
                                                  SELECT   a.*
                                                  FROM     (
                                                                  SELECT
                                                                         /* Use Internal Organization as lookup to get party id */
                                                                         DISTINCT cast(cctl_grouptype.typecode_stg AS VARCHAR(50)) AS typecode_stg ,
                                                                         ''INT''                                                     AS name_type_cd_stg,
                                                                         cc_group.name_stg                                         AS id_stg,
                                                                         cc_group.name_stg,
                                                                         ''CLH''                                              AS req_stg,
                                                                         to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                         CASE
                                                                                WHEN cc_group.updatetime_stg IS NULL THEN to_date(''19000101'',''YYYYMMDD'')
                                                                                ELSE cc_group.updatetime_stg
                                                                         END                  AS trans_strt_dt_stg,
                                                                         ''SRC_SYS6''           AS sys_src_cd_stg,
                                                                         cc_group.retired_stg AS retired_stg
                                                                  FROM   db_t_prod_stag.cc_group,
                                                                         db_t_prod_stag.cctl_grouptype
                                                                  WHERE  cc_group.grouptype_stg = cctl_grouptype.id_stg
                                                                  AND    cc_group.updatetime_stg > (cast(:start_dttm AS timestamp))
                                                                  AND    cc_group.updatetime_stg <= (cast(:end_dttm AS timestamp)) ) a qualify row_number() over(PARTITION BY typecode_stg,id_stg ORDER BY trans_strt_dt_stg DESC) =1
                                                  UNION
                                                  /*******************Sales Hierarchy**************/
                                                  /***********Sales State****************/
                                                  SELECT DISTINCT ''INTRNL_ORG_SBTYPE4''                               AS typecode_stg ,
                                                                  ''INT''                                              AS type_stg,
                                                                  pc_region_zone.code_stg                            AS key_stg,
                                                                  pc_region_zone.code_stg                            AS name_stg,
                                                                  ''SLH''                                              AS req_stg,
                                                                  to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                  CASE
                                                                                  WHEN current_date IS NULL THEN to_date(''19000101'', ''YYYYMMDD'')
                                                                                  ELSE current_date
                                                                  END        AS trans_strt_dt_stg,
                                                                  ''SRC_SYS4'' AS sys_src_cd_stg,
                                                                  0          AS retired_stg
                                                  FROM            db_t_prod_stag.pc_region_zone
                                                  inner join      db_t_prod_stag.pctl_zonetype
                                                  ON              pc_region_zone.zonetype_stg =pctl_zonetype.id_stg
                                                  WHERE           pctl_zonetype.typecode_stg =''state'' qualify row_number() over( PARTITION BY pc_region_zone.code_stg ORDER BY trans_strt_dt_stg DESC) =1
                                                  /*************************************/
                                                  UNION
                                                  SELECT DISTINCT pctl_grouptype.typecode_stg AS typecode_stg,
                                                                  ''INT''                       AS type_stg,
                                                                  pc_group.name_stg           AS key_stg,
                                                                  pc_group.name_stg,
                                                                  ''SCH'' AS req_stg,
                                                                  CASE
                                                                                  WHEN pc_group.createtime_stg IS NULL THEN to_date(''19000101'',''YYYYMMDD'')
                                                                                  ELSE pc_group.createtime_stg
                                                                  END                                                AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                  CASE
                                                                                  WHEN pc_group.updatetime_stg IS NULL THEN to_date(''19000101'',''YYYYMMDD'')
                                                                                  ELSE pc_group.updatetime_stg
                                                                  END                  AS trans_strt_dt_stg,
                                                                  ''SRC_SYS4''           AS sys_src_cd_stg,
                                                                  pc_group.retired_stg AS retired_stg
                                                  FROM            db_t_prod_stag.pc_group
                                                  inner join      db_t_prod_stag.pctl_grouptype
                                                  ON              pctl_grouptype.id_stg = pc_group.grouptype_stg
                                                  WHERE           pctl_grouptype.typecode_stg IN (''region'',
                                                                                                  ''salesdistrict_alfa'')
                                                  AND             pc_group.updatetime_stg > (cast(:start_dttm AS timestamp))
                                                  AND             pc_group.updatetime_stg <= (cast(:end_dttm AS timestamp)) qualify row_number() over( PARTITION BY typecode_stg, pc_group.name_stg ORDER BY trans_strt_dt_stg DESC) =1
                                                  /* (''root'',''region'',''salesdistrict_alfa'',''servicecenter_alfa'') root not present in Internagal Org query */
                                                  /***********************************/
                                                  UNION
                                                        /**********Agent Name**********/
                                                  SELECT   ''INTRNL_ORG_SBTYPE2'' AS typecode_stg,
                                                           ''INT''                AS type_stg,
                                                           code_stg             AS key_stg,
                                                           description_stg,
                                                           ''PRD''                                              AS req_stg,
                                                           to_date(''19000101'',''YYYYMMDD'')                     AS src_strt_dt_stg,
                                                           cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                           CASE
                                                                    WHEN pc_producercode.updatetime_stg IS NULL THEN to_date(''19000101'', ''YYYYMMDD'')
                                                                    ELSE pc_producercode.updatetime_stg
                                                           END                         AS trans_strt_dt_stg,
                                                           ''SRC_SYS4''                  AS sys_src_cd_stg,
                                                           pc_producercode.retired_stg AS retired_stg
                                                  FROM     db_t_prod_stag.pc_producercode
                                                  join     db_t_prod_stag.pc_userproducercode upc
                                                  ON       upc.producercodeid_stg = pc_producercode.id_stg
                                                  join     db_t_prod_stag.pc_user usr
                                                  ON       usr.id_stg = upc.userid_stg
                                                  join     db_t_prod_stag.pc_contact cnt
                                                  ON       cnt.id_stg = usr.contactid_stg
                                                  WHERE    pc_producercode.updatetime_stg > (cast(:start_dttm AS timestamp))
                                                  AND      pc_producercode.updatetime_stg <= (cast(:end_dttm AS timestamp)) qualify row_number() over( PARTITION BY code_stg ORDER BY trans_strt_dt_stg DESC) =1
                                                  UNION
                                                  /* UNION */
                                                  /*********** Service Center*************/
                                                  SELECT DISTINCT pctl_grouptype.typecode_stg AS typecode_stg,
                                                                  ''INT''                       AS type_stg,
                                                                  pc_group.name_stg           AS key_stg,
                                                                  pc_group.org_name_stg,
                                                                  ''SCH'' AS req_stg,
                                                                  CASE
                                                                                  WHEN pc_group.createtime_stg IS NULL THEN to_date(''19000101'',''YYYYMMDD'')
                                                                                  ELSE pc_group.createtime_stg
                                                                  END                                                AS src_strt_dt_stg,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt_stg,
                                                                  CASE
                                                                                  WHEN pc_group.updatetime_stg IS NULL THEN to_date(''19000101'',''YYYYMMDD'')
                                                                                  ELSE pc_group.updatetime_stg
                                                                  END                  AS trans_strt_dt_stg,
                                                                  ''SRC_SYS4''           AS sys_src_cd_stg,
                                                                  pc_group.retired_stg AS retired_stg
                                                  FROM            (
                                                                            SELECT    pc_group.name_stg,
                                                                                      pc_group.createtime_stg,
                                                                                      CASE
                                                                                                WHEN pc_group.updatetime_stg>pc_contact.updatetime_stg THEN pc_group.updatetime_stg
                                                                                                ELSE pc_contact.updatetime_stg
                                                                                      END AS updatetime_stg,
                                                                                      pc_group.retired_stg,
                                                                                      pc_group.grouptype_stg,
                                                                                      pc_contact.name_stg AS org_name_stg
                                                                            FROM      db_t_prod_stag.pc_group
                                                                            left join db_t_prod_stag.pc_contact
                                                                            ON        pc_contact.id_stg = pc_group.contact_alfa_stg ) pc_group
                                                  inner join      db_t_prod_stag.pctl_grouptype
                                                  ON              pctl_grouptype.id_stg = pc_group.grouptype_stg
                                                  WHERE           pctl_grouptype.typecode_stg IN (''servicecenter_alfa'')
                                                  AND             ((
                                                                                                  pc_group.updatetime_stg > (cast(:start_dttm AS timestamp))
                                                                                  AND             pc_group.updatetime_stg <= (cast(:end_dttm AS timestamp)))
                                                                  OR              (
                                                                                                  pc_contact.updatetime_stg > (cast(:start_dttm AS timestamp))
                                                                                  AND             pc_contact.updatetime_stg <= (cast(:end_dttm AS timestamp)))) qualify row_number() over( PARTITION BY typecode_stg , pc_group.name_stg ORDER BY trans_strt_dt_stg DESC) =1 ) src ) );
  -- Component exp_all_source, Type EXPRESSION
  create or replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT    lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */
                      AS v_typecode,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */
                                                                     AS intrnl_org_type_cd,
                      sq_cctl_salvageyard_alfa.name_type_cd          AS in_name_type_cd,
                      upper ( sq_cctl_salvageyard_alfa.name )        AS o_name,
                      upper ( sq_cctl_salvageyard_alfa.description ) AS o_description,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      AS v_sys_src_cd,
                      decode ( TRUE ,
                              sq_cctl_salvageyard_alfa.req = ''INSURANCE CARRIER''
                    OR        sq_cctl_salvageyard_alfa.req = ''INSU'' , lkp_4.busn_prty_id
                              /* replaced lookup LKP_BUSN */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''SALVG''
                    OR        sq_cctl_salvageyard_alfa.req = ''SALV'' , lkp_5.busn_prty_id
                              /* replaced lookup LKP_BUSN */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''UWC'' , lkp_6.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''UWH'' , lkp_7.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''CLH'' , lkp_8.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''SLH'' , lkp_9.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''SCH'' , lkp_10.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              sq_cctl_salvageyard_alfa.req = ''PRD'' , lkp_11.intrnl_org_prty_id
                              /* replaced lookup LKP_INTRNL_ORG */
                              ,
                              lkp_12.busn_prty_id
                              /* replaced lookup LKP_BUSN */
                              )                                                              AS var_prty_id,
                      var_prty_id                                                            AS in_prty_id,
                      :prcs_id                                                               AS prcs_id,
                      sq_cctl_salvageyard_alfa.src_strt_dt                                   AS src_strt_dt,
                      sq_cctl_salvageyard_alfa.src_end_dt                                    AS src_end_dt,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      sq_cctl_salvageyard_alfa.retired                                       AS retired,
                      sq_cctl_salvageyard_alfa.trans_strt_dt                                 AS trans_strt_dt,
                      sq_cctl_salvageyard_alfa.source_record_id,
                      row_number() over (PARTITION BY sq_cctl_salvageyard_alfa.source_record_id ORDER BY sq_cctl_salvageyard_alfa.source_record_id) AS rnk
            FROM      sq_cctl_salvageyard_alfa
            left join lkp_teradata_etl_ref_xlat_intrnl_org_sbtype lkp_1
            ON        lkp_1.src_idntftn_val = sq_cctl_salvageyard_alfa.typecode
            left join lkp_teradata_etl_ref_xlat_intrnl_org_type lkp_2
            ON        lkp_2.src_idntftn_val = ''INTRNL_ORG_TYPE15''
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_3
            ON        lkp_3.src_idntftn_val = sq_cctl_salvageyard_alfa.sys_src_cd
            left join lkp_busn lkp_4
            ON        lkp_4.busn_ctgy_cd = (SELECT tgt_idntftn_val FROM lkp_teradata_etl_ref_xlat_busn_ctgy WHERE src_idntftn_val = ''BUSN_CTGY6'' LIMIT 1)
            AND       lkp_4.nk_busn_cd = sq_cctl_salvageyard_alfa.typecode
            left join lkp_busn lkp_5
            ON        lkp_5.busn_ctgy_cd = (SELECT tgt_idntftn_val FROM lkp_teradata_etl_ref_xlat_busn_ctgy WHERE src_idntftn_val = ''BUSN_CTGY5'' LIMIT 1)
            AND       lkp_5.nk_busn_cd = sq_cctl_salvageyard_alfa.typecode
            left join lkp_intrnl_org lkp_6
            ON        lkp_6.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_6.intrnl_org_sbtype_cd = ''CO''
            AND       lkp_6.intrnl_org_num = v_typecode
            left join lkp_intrnl_org lkp_7
            ON        lkp_7.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_7.intrnl_org_sbtype_cd = v_typecode
            AND       lkp_7.intrnl_org_num = sq_cctl_salvageyard_alfa.name
            left join lkp_intrnl_org lkp_8
            ON        lkp_8.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_8.intrnl_org_sbtype_cd = v_typecode
            AND       lkp_8.intrnl_org_num = sq_cctl_salvageyard_alfa.name
            left join lkp_intrnl_org lkp_9
            ON        lkp_9.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_9.intrnl_org_sbtype_cd = v_typecode
            AND       lkp_9.intrnl_org_num = sq_cctl_salvageyard_alfa.name
            left join lkp_intrnl_org lkp_10
            ON        lkp_10.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_10.intrnl_org_sbtype_cd = v_typecode
            AND       lkp_10.intrnl_org_num = sq_cctl_salvageyard_alfa.name
            left join lkp_intrnl_org lkp_11
            ON        lkp_11.intrnl_org_type_cd = intrnl_org_type_cd
            AND       lkp_11.intrnl_org_sbtype_cd = v_typecode
            AND       lkp_11.intrnl_org_num = sq_cctl_salvageyard_alfa.name
            left join lkp_busn lkp_12
            ON        lkp_12.busn_ctgy_cd = (SELECT tgt_idntftn_val FROM lkp_teradata_etl_ref_xlat_busn_ctgy WHERE src_idntftn_val = sq_cctl_salvageyard_alfa.req LIMIT 1)
            AND       lkp_12.nk_busn_cd = sq_cctl_salvageyard_alfa.typecode qualify rnk = 1 );
  -- Component LKP_ORG_NAME, Type LOOKUP
  create or replace TEMPORARY TABLE lkp_org_name AS
  (
            SELECT    lkp.name_type_cd,
                      lkp.org_name_strt_dttm,
                      lkp.prty_id,
                      lkp.org_name,
                      lkp.org_name_desc,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_all_source.in_name_type_cd AS in_name_type_cd,
                      exp_all_source.in_prty_id      AS in_prty_id,
                      exp_all_source.source_record_id,
                      row_number() over(PARTITION BY exp_all_source.source_record_id ORDER BY lkp.name_type_cd ASC,lkp.org_name_strt_dttm ASC,lkp.prty_id ASC,lkp.org_name ASC,lkp.org_name_desc ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_all_source
            left join
                      (
                               SELECT   org_name.org_name_strt_dttm AS org_name_strt_dttm,
                                        org_name.org_name           AS org_name,
                                        org_name.org_name_desc      AS org_name_desc,
                                        org_name.edw_strt_dttm      AS edw_strt_dttm,
                                        org_name.edw_end_dttm       AS edw_end_dttm,
                                        org_name.name_type_cd       AS name_type_cd,
                                        org_name.prty_id            AS prty_id
                               FROM     db_t_prod_core.org_name qualify row_number() over(PARTITION BY org_name.name_type_cd,org_name.prty_id ORDER BY org_name.edw_strt_dttm DESC) = 1 ) lkp
            ON        lkp.name_type_cd = exp_all_source.in_name_type_cd
            AND       lkp.prty_id = exp_all_source.in_prty_id qualify rnk = 1 );
  -- Component exp_compare_data, Type EXPRESSION
  create or replace TEMPORARY TABLE exp_compare_data AS
  (
             SELECT     lkp_org_name.name_type_cd       AS lkp_name_type_cd,
                        lkp_org_name.org_name_strt_dttm AS lkp_org_name_strt_dt,
                        lkp_org_name.prty_id            AS lkp_prty_id,
                        lkp_org_name.org_name           AS lkp_org_name,
                        NULL                            AS lkp_org_name_end_dt,
                        lkp_org_name.edw_strt_dttm      AS lkp_edw_strt_dttm,
                        lkp_org_name.edw_end_dttm       AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( lkp_org_name.org_name_strt_dttm , ''yyyy-mm-dd'' ) ) )
                                   || ltrim ( rtrim ( lkp_org_name.org_name ) )
                                   || ltrim ( rtrim ( lkp_org_name.org_name_desc ) ) ) AS v_lkp_checksum,
                        lkp_org_name.in_name_type_cd                                   AS in_name_type_cd,
                        exp_all_source.o_name                                          AS in_name,
                        exp_all_source.o_description                                   AS in_description,
                        lkp_org_name.in_prty_id                                        AS in_prty_id,
                        exp_all_source.src_strt_dt                                     AS in_src_strt_dt,
                        exp_all_source.src_end_dt                                      AS in_src_end_dt,
                        exp_all_source.edw_strt_dttm                                   AS in_edw_strt_dttm,
                        exp_all_source.edw_end_dttm                                    AS in_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( exp_all_source.src_strt_dt , ''yyyy-mm-dd'' ) ) )
                                   || ltrim ( rtrim ( exp_all_source.o_name ) )
                                   || ltrim ( rtrim ( exp_all_source.o_description ) ) ) AS v_in_checksum,
                        CASE
                                   WHEN v_lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_lkp_checksum != v_in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                          AS calc_ins_upd,
                        exp_all_source.prcs_id       AS prcs_id,
                        exp_all_source.retired       AS retired,
                        exp_all_source.trans_strt_dt AS trans_strt_dt,
                        exp_all_source.source_record_id
             FROM       exp_all_source
             inner join lkp_org_name
             ON         exp_all_source.source_record_id = lkp_org_name.source_record_id );
  -- Component rtr_insert_update_flag_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_insert_update_flag_INSERT AS
  SELECT exp_compare_data.lkp_name_type_cd     AS lkp_name_type_cd,
         exp_compare_data.lkp_prty_id          AS lkp_prty_id,
         exp_compare_data.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_compare_data.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_compare_data.in_name_type_cd      AS in_name_type_cd,
         exp_compare_data.in_name              AS in_name,
         exp_compare_data.in_description       AS in_description,
         exp_compare_data.in_prty_id           AS in_prty_id,
         exp_compare_data.in_src_strt_dt       AS in_src_strt_dt,
         exp_compare_data.in_src_end_dt        AS in_src_end_dt,
         exp_compare_data.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_compare_data.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_compare_data.prcs_id              AS prcs_id,
         exp_compare_data.calc_ins_upd         AS calc_ins_upd,
         exp_compare_data.lkp_org_name_strt_dt AS lkp_org_name_strt_dt,
         exp_compare_data.lkp_org_name         AS lkp_org_name,
         exp_compare_data.lkp_org_name_end_dt  AS lkp_org_name_end_dt,
         exp_compare_data.retired              AS retired,
         exp_compare_data.trans_strt_dt        AS trans_strt_dt,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  exp_compare_data.in_prty_id IS NOT NULL
  AND    (
                exp_compare_data.calc_ins_upd = ''I''
         OR     (
                       exp_compare_data.calc_ins_upd = ''U''
                AND    exp_compare_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_compare_data.retired = 0
                AND    exp_compare_data.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_insert_update_flag_Retired, Type ROUTER Output Group Retired
  create or replace TEMPORARY TABLE rtr_insert_update_flag_Retired AS
  SELECT exp_compare_data.lkp_name_type_cd     AS lkp_name_type_cd,
         exp_compare_data.lkp_prty_id          AS lkp_prty_id,
         exp_compare_data.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_compare_data.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_compare_data.in_name_type_cd      AS in_name_type_cd,
         exp_compare_data.in_name              AS in_name,
         exp_compare_data.in_description       AS in_description,
         exp_compare_data.in_prty_id           AS in_prty_id,
         exp_compare_data.in_src_strt_dt       AS in_src_strt_dt,
         exp_compare_data.in_src_end_dt        AS in_src_end_dt,
         exp_compare_data.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_compare_data.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_compare_data.prcs_id              AS prcs_id,
         exp_compare_data.calc_ins_upd         AS calc_ins_upd,
         exp_compare_data.lkp_org_name_strt_dt AS lkp_org_name_strt_dt,
         exp_compare_data.lkp_org_name         AS lkp_org_name,
         exp_compare_data.lkp_org_name_end_dt  AS lkp_org_name_end_dt,
         exp_compare_data.retired              AS retired,
         exp_compare_data.trans_strt_dt        AS trans_strt_dt,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  exp_compare_data.calc_ins_upd = ''R''
  AND    exp_compare_data.retired != 0
  AND    exp_compare_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    exp_compare_data.in_prty_id IS NOT NULL;
  
  -- Component upd_org_name_retired, Type UPDATE
  create or replace TEMPORARY TABLE upd_org_name_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_retired.lkp_name_type_cd     AS lkp_name_type_cd4,
                rtr_insert_update_flag_retired.lkp_prty_id          AS lkp_prty_id4,
                rtr_insert_update_flag_retired.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm4,
                rtr_insert_update_flag_retired.in_edw_strt_dttm     AS in_edw_strt_dttm4,
                rtr_insert_update_flag_retired.in_edw_end_dttm      AS in_edw_end_dttm4,
                rtr_insert_update_flag_retired.lkp_org_name_strt_dt AS lkp_org_name_strt_dt4,
                rtr_insert_update_flag_retired.lkp_org_name         AS lkp_org_name4,
                rtr_insert_update_flag_retired.lkp_org_name_end_dt  AS lkp_org_name_end_dt4,
                rtr_insert_update_flag_retired.trans_strt_dt        AS trans_strt_dt4,
                1                                                   AS update_strategy_action,
                rtr_insert_update_flag_retired.source_record_id
         FROM   rtr_insert_update_flag_retired );
  -- Component exp_RETIRED, Type EXPRESSION
  create or replace TEMPORARY TABLE exp_retired AS
  (
         SELECT upd_org_name_retired.lkp_name_type_cd4         AS lkp_name_type_cd4,
                upd_org_name_retired.lkp_prty_id4              AS lkp_prty_id4,
                upd_org_name_retired.lkp_edw_strt_dttm4        AS lkp_edw_strt_dttm4,
                DATEADD(SECOND, -1, CURRENT_TIMESTAMP) AS edw_end_dttm,
                dateadd (second, -1, upd_org_name_retired.trans_strt_dt4 ) AS trans_end_dttm,
                upd_org_name_retired.source_record_id
         FROM   upd_org_name_retired );
  -- Component tgt_org_name_retired, Type TARGET
  merge INTO db_t_prod_core.org_name USING exp_retired
  ON (
                            org_name.name_type_cd = exp_retired.lkp_name_type_cd4
               AND          org_name.prty_id = exp_retired.lkp_prty_id4
               AND          org_name.edw_strt_dttm = exp_retired.lkp_edw_strt_dttm4)
  WHEN matched THEN
  UPDATE
  SET    name_type_cd = exp_retired.lkp_name_type_cd4,
         prty_id = exp_retired.lkp_prty_id4,
         edw_strt_dttm = exp_retired.lkp_edw_strt_dttm4,
         edw_end_dttm = exp_retired.edw_end_dttm,
         trans_end_dttm = exp_retired.trans_end_dttm;
  
  -- Component tgt_org_name_retired, Type Post SQL
  UPDATE db_t_prod_core.org_name
  SET edw_end_dttm=a.lead1, trans_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT prty_id,
                                         name_type_cd,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_id,name_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)     - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY prty_id,name_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.org_name ) a

  WHERE  org_name.edw_strt_dttm = a.edw_strt_dttm
  AND    org_name.trans_strt_dttm = a.trans_strt_dttm
  AND    org_name.prty_id=a.prty_id
  AND    org_name.name_type_cd=a.name_type_cd
  AND    cast(org_name.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(org_name.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead IS NOT NULL;
  
  -- Component upd_org_name_insert, Type UPDATE
  create or replace TEMPORARY TABLE upd_org_name_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_insert.in_name_type_cd  AS in_name_type_cd1,
                rtr_insert_update_flag_insert.in_name          AS in_name1,
                rtr_insert_update_flag_insert.in_description   AS in_description1,
                rtr_insert_update_flag_insert.in_prty_id       AS in_prty_id1,
                rtr_insert_update_flag_insert.in_src_strt_dt   AS in_src_strt_dt1,
                rtr_insert_update_flag_insert.in_src_end_dt    AS in_src_end_dt1,
                rtr_insert_update_flag_insert.in_edw_strt_dttm AS in_edw_strt_dttm1,
                rtr_insert_update_flag_insert.in_edw_end_dttm  AS in_edw_end_dttm1,
                rtr_insert_update_flag_insert.prcs_id          AS prcs_id1,
                rtr_insert_update_flag_insert.retired          AS retired1,
                rtr_insert_update_flag_insert.trans_strt_dt    AS trans_strt_dt1,
                0                                              AS update_strategy_action,
                rtr_insert_update_flag_insert.source_record_id
         FROM   rtr_insert_update_flag_insert );
  -- Component exp_org_name_insert, Type EXPRESSION
  create or replace TEMPORARY TABLE exp_org_name_insert AS
  (
         SELECT upd_org_name_insert.in_name_type_cd1  AS in_name_type_cd1,
                upd_org_name_insert.in_name1          AS in_name1,
                upd_org_name_insert.in_description1   AS in_description1,
                upd_org_name_insert.in_prty_id1       AS in_prty_id1,
                upd_org_name_insert.in_src_strt_dt1   AS in_src_strt_dt1,
                upd_org_name_insert.in_src_end_dt1    AS in_src_end_dt1,
                upd_org_name_insert.in_edw_strt_dttm1 AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_org_name_insert.retired1 != 0 THEN upd_org_name_insert.in_edw_strt_dttm1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                AS o_edw_end_dttm,
                upd_org_name_insert.prcs_id1       AS prcs_id1,
                upd_org_name_insert.trans_strt_dt1 AS trans_strt_dt1,
                CASE
                       WHEN upd_org_name_insert.retired1 != 0 THEN upd_org_name_insert.trans_strt_dt1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trns_end_dttm,
                upd_org_name_insert.source_record_id
         FROM   upd_org_name_insert );
  -- Component tgt_org_name_insert, Type TARGET
  INSERT INTO db_t_prod_core.org_name
              (
                          name_type_cd,
                          org_name_strt_dttm,
                          prty_id,
                          org_name,
                          org_name_desc,
                          org_name_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_org_name_insert.in_name_type_cd1  AS name_type_cd,
         exp_org_name_insert.in_src_strt_dt1   AS org_name_strt_dttm,
         exp_org_name_insert.in_prty_id1       AS prty_id,
         exp_org_name_insert.in_name1          AS org_name,
         exp_org_name_insert.in_description1   AS org_name_desc,
         exp_org_name_insert.in_src_end_dt1    AS org_name_end_dttm,
         exp_org_name_insert.prcs_id1          AS prcs_id,
         exp_org_name_insert.in_edw_strt_dttm1 AS edw_strt_dttm,
         exp_org_name_insert.o_edw_end_dttm    AS edw_end_dttm,
         exp_org_name_insert.trans_strt_dt1    AS trans_strt_dttm,
         exp_org_name_insert.out_trns_end_dttm AS trans_end_dttm
  FROM   exp_org_name_insert;

END;
';