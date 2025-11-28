-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INDIV_MRTL_STS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


  -- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_clm_ctr AS
  (
         SELECT indiv.indiv_prty_id AS indiv_prty_id,
                indiv.nk_publc_id   AS nk_publc_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NOT NULL );
  -- Component LKP_INDIV_CNT_MGR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_cnt_mgr AS
  (
         SELECT indiv.indiv_prty_id AS indiv_prty_id,
                indiv.nk_link_id    AS nk_link_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NULL );
  -- Component LKP_INDIV_MRTL_STS, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_mrtl_sts AS
  (
         SELECT indiv_prty_id
         FROM   db_t_prod_core.indiv_mrtl_sts );
  -- Component LKP_INDIV_MRTL_STS_CHANGED_RECORDS, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_mrtl_sts_changed_records AS
  (
         SELECT indiv_mrtl_sts.indiv_prty_id AS indiv_prty_id,
                indiv_mrtl_sts.mrtl_sts_cd   AS mrtl_sts_cd
         FROM   db_t_prod_core.indiv_mrtl_sts
         WHERE  indiv_mrtl_sts_end_dttm IS NULL );
  -- Component LKP_TERADATA_ETL_REF_XLAT_MRTL_STS_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_mrtl_sts_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''MRTL_STS_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN ( ''cctl_maritalstatus.typecode'' ,
                                                         ''pctl_maritalstatus.typecode'',
                                                         ''abtl_maritalstatus.typecode'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_ab_abcontact, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_ab_abcontact AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS linkid,
                $2 AS typecode,
                $3 AS publicid,
                $4 AS source,
                $5 AS updatetime,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  /* -----DB_T_PROD_STAG.bc_contact start----- */
                                                  SELECT DISTINCT upper(cast(bc_contact.addressbookuid_stg AS VARCHAR(64))) AS linkid ,
                                                                  cast(NULL AS                    VARCHAR(30))                                 AS mrtl_sts_cd ,
                                                                  cast(bc_contact.publicid_stg AS VARCHAR(64))                                    publicid_stg ,
                                                                  cast(''ClaimCenter'' AS           VARCHAR (20))                                AS source,
                                                                  max(bc_contact.updatetime_stg)                                               AS maxdt
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
                                                                  /*  below condition added to avoid duplicates */
                                                                  bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                  ''systemTables:1'',
                                                                                                  ''systemTables:2'')
                                                  AND             ((
                                                                                                  bc_contact.updatetime_stg>(:START_DTTM)
                                                                                  AND             bc_contact.updatetime_stg <=(:END_DTTM))
                                                                  OR              (
                                                                                                  bc_user.updatetime_stg>(:START_DTTM)
                                                                                  AND             bc_user.updatetime_stg <= (:END_DTTM)))
                                                  AND             bctl_contact.name_stg IN (''Person'',
                                                                                            ''Adjudicator'',
                                                                                            ''User Contact'',
                                                                                            ''Vendor (Person)'',
                                                                                            ''Attorney'',
                                                                                            ''Doctor'',
                                                                                            ''Policy Person'',
                                                                                            ''Lodging (Person)'')
                                                                  /* AND bc_contact.PublicID_stg = ''prodcc:2388751'' */
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             bc_contact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  bc_contact.publicid_stg ,
                                                                  source
                                                  UNION
                                                  SELECT DISTINCT upper(cast(bc_contact.addressbookuid_stg AS VARCHAR(64)) ) AS linkid ,
                                                                  cast(NULL AS                    VARCHAR(30))                                  AS mrtl_sts_cd ,
                                                                  cast(bc_contact.publicid_stg AS VARCHAR(64))                                     publicid_stg ,
                                                                  cast(''ClaimCenter''AS            VARCHAR (20))                                 AS source,
                                                                  max(bc_contact.updatetime_stg)                                                AS maxdt
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
                                                                                                  bc_contact.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             bc_contact.updatetime_stg <=(cast(:END_DTTM AS timestamp)))
                                                                  OR              (
                                                                                                  bc_user.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             bc_user.updatetime_stg <= (cast(:END_DTTM AS timestamp))))
                                                                  /*  AND bc_contact.PublicID_stg = ''prodcc:2388751'' */
                                                  AND             bctl_contact.name_stg IN (''Person'',
                                                                                            ''Adjudicator'',
                                                                                            ''User Contact'',
                                                                                            ''Vendor (Person)'',
                                                                                            ''Attorney'',
                                                                                            ''Doctor'',
                                                                                            ''Policy Person'',
                                                                                            ''Lodging (Person)'')
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             bc_contact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  bc_contact.publicid_stg ,
                                                                  source
                                                  UNION
                                                  SELECT DISTINCT upper(bc_contact.addressbookuid_stg) AS linkid ,
                                                                  cast(NULL AS VARCHAR(30))            AS mrtl_sts_cd ,
                                                                  bc_contact.publicid_stg ,
                                                                  cast(''ClaimCenter''AS VARCHAR (20)) AS source,
                                                                  max(bc_contact.updatetime_stg)     AS maxdt
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
                                                                                                  bc_contact.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             bc_contact.updatetime_stg <=(cast(:END_DTTM AS timestamp)))
                                                                  OR              (
                                                                                                  bc_user.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             bc_user.updatetime_stg <= (cast(:END_DTTM AS timestamp))))
                                                  AND             bc_contact.publicid_stg = ''prodcc:2388751''
                                                  AND             bctl_contact.name_stg IN (''Person'',
                                                                                            ''Adjudicator'',
                                                                                            ''User Contact'',
                                                                                            ''Vendor (Person)'',
                                                                                            ''Attorney'',
                                                                                            ''Doctor'',
                                                                                            ''Policy Person'',
                                                                                            ''Lodging (Person)'')
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             bc_contact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  bc_contact.publicid_stg ,
                                                                  source
                                                  /* -----DB_T_PROD_STAG.bc_contact end----- */
                                                  UNION
                                                  /* --DB_T_PROD_STAG.pc_contact start----- */
                                                  SELECT DISTINCT upper(pc_contact.addressbookuid_stg) AS linkid ,
                                                                  pctl_maritalstatus.typecode_stg      AS mrtl_sts_cd ,
                                                                  pc_contact.publicid_stg ,
                                                                  cast(''ClaimCenter''AS VARCHAR (20)) AS source,
                                                                  max(pc_contact.updatetime_stg)     AS maxdt
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
                                                                                                  pc_contact.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             pc_contact.updatetime_stg <= (cast(:END_DTTM AS timestamp)))
                                                                  OR              (
                                                                                                  pc_user.updatetime_stg>(cast(:START_DTTM AS timestamp))
                                                                                  AND             pc_user.updatetime_stg <= (cast(:END_DTTM AS timestamp))))
                                                  AND             pctl_contact.name_stg IN (''Person'',
                                                                                            ''Adjudicator'',
                                                                                            ''User Contact'',
                                                                                            ''Vendor (Person)'',
                                                                                            ''Attorney'',
                                                                                            ''Doctor'',
                                                                                            ''Policy Person'',
                                                                                            ''Lodging (Person)'')
                                                                  /* AND pc_contact.PublicID_stg = ''prodcc:2388751'' */
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             pc_contact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  pc_contact.publicid_stg ,
                                                                  source
                                                  /* --DB_T_PROD_STAG.pc_contact end----- */
                                                  UNION
                                                  /* --DB_T_PROD_STAG.cc_contact start--- */
                                                  SELECT DISTINCT upper(cc_contact.addressbookuid_stg) AS linkid ,
                                                                  cctl_maritalstatus.typecode_stg      AS mrtl_sts_cd ,
                                                                  cc_contact.publicid_stg ,
                                                                  cast(''ClaimCenter''AS VARCHAR (20)) AS source,
                                                                  max(cc_contact.updatetime_stg)     AS maxdt
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
                                                  WHERE           ((
                                                                                                  cc_contact.updatetime_stg>(:START_DTTM)
                                                                                  AND             cc_contact.updatetime_stg <= (:END_DTTM) )
                                                                  OR              (
                                                                                                  cc_user.updatetime_stg>(:START_DTTM)
                                                                                  AND             cc_user.updatetime_stg <=(:END_DTTM)))
                                                  AND             cctl_contact.name_stg IN (''Person'',
                                                                                            ''Adjudicator'',
                                                                                            ''User Contact'',
                                                                                            ''Vendor (Person)'',
                                                                                            ''Attorney'',
                                                                                            ''Doctor'',
                                                                                            ''Policy Person'',
                                                                                            ''Lodging (Person)'')
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             cc_contact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  cc_contact.publicid_stg ,
                                                                  source
                                                  /* --DB_T_PROD_STAG.cc_contact end--- */
                                                  UNION
                                                  /* --DB_T_PROD_STAG.ab_abcontact start--- */
                                                  SELECT DISTINCT upper(ab_abcontact.linkid_stg)  AS linkid ,
                                                                  abtl_maritalstatus.typecode_stg AS mrtl_sts_cd ,
                                                                  cast(NULL AS            VARCHAR(30))       AS publicid_stg ,
                                                                  cast(''ContactManager''AS VARCHAR (20))      AS source,
                                                                  max(ab_abcontact.updatetime_stg)           AS maxdt
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
                                                  WHERE           ab_abcontact.updatetime_stg>cast(:START_DTTM AS timestamp)
                                                  AND             ab_abcontact.updatetime_stg <= cast(:END_DTTM AS timestamp)
                                                  AND             abtl_abcontact.name_stg IN (''Person'',
                                                                                              ''Adjudicator'',
                                                                                              ''User Contact'',
                                                                                              ''Vendor (Person)'',
                                                                                              ''Attorney'',
                                                                                              ''Doctor'',
                                                                                              ''Policy Person'',
                                                                                              ''Lodging (Person)'')
                                                  AND             ((
                                                                                                  source = ''ClaimCenter''
                                                                                  AND             ab_abcontact.publicid_stg IS NOT NULL)
                                                                  OR              (
                                                                                                  source = ''ContactManager''
                                                                                  AND             linkid IS NOT NULL))
                                                  AND             mrtl_sts_cd IS NOT NULL
                                                  GROUP BY        linkid ,
                                                                  mrtl_sts_cd ,
                                                                  ab_abcontact.publicid_stg ,
                                                                  source
                                                                  /* --DB_T_PROD_STAG.ab_abcontact end--- */
                                  ) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_MRTL_STS_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_MRTL_STS_CD */
                      END AS var_typecode,
                      decode ( TRUE ,
                              sq_ab_abcontact.source = ''ContactManager'' ,
                              CASE
                                        WHEN lkp_3.indiv_prty_id
                                                  /* replaced lookup LKP_INDIV_CNT_MGR */
                                                  IS NULL THEN 9999
                                        ELSE lkp_4.indiv_prty_id
                                                  /* replaced lookup LKP_INDIV_CNT_MGR */
                              END ,
                              sq_ab_abcontact.source = ''ClaimCenter'' ,
                              CASE
                                        WHEN lkp_5.indiv_prty_id
                                                  /* replaced lookup LKP_INDIV_CLM_CTR */
                                                  IS NULL THEN 9999
                                        ELSE lkp_6.indiv_prty_id
                                                  /* replaced lookup LKP_INDIV_CLM_CTR */
                              END ,
                              9999 ) AS var_indiv_prty_id,
                      lkp_7.indiv_prty_id
                      /* replaced lookup LKP_INDIV_MRTL_STS */
                      AS var_new_records,
                      lkp_8.indiv_prty_id
                      /* replaced lookup LKP_INDIV_MRTL_STS_CHANGED_RECORDS */
                                                 AS var_changed_records,
                      var_indiv_prty_id          AS out_indiv_prty_id,
                      var_typecode               AS out_typecode,
                      sq_ab_abcontact.updatetime AS indiv_mrtl_sts_strt_dt,
                      sq_ab_abcontact.source_record_id,
                      row_number() over (PARTITION BY sq_ab_abcontact.source_record_id ORDER BY sq_ab_abcontact.source_record_id) AS rnk
            FROM      sq_ab_abcontact
            left join lkp_teradata_etl_ref_xlat_mrtl_sts_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_ab_abcontact.typecode
            left join lkp_teradata_etl_ref_xlat_mrtl_sts_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_ab_abcontact.typecode
            left join lkp_indiv_cnt_mgr lkp_3
            ON        lkp_3.nk_link_id = sq_ab_abcontact.linkid
            left join lkp_indiv_cnt_mgr lkp_4
            ON        lkp_4.nk_link_id = sq_ab_abcontact.linkid
            left join lkp_indiv_clm_ctr lkp_5
            ON        lkp_5.nk_publc_id = sq_ab_abcontact.publicid
            left join lkp_indiv_clm_ctr lkp_6
            ON        lkp_6.nk_publc_id = sq_ab_abcontact.publicid
            left join lkp_indiv_mrtl_sts lkp_7
            ON        lkp_7.indiv_prty_id = var_indiv_prty_id
            left join lkp_indiv_mrtl_sts_changed_records lkp_8
            ON        lkp_8.indiv_prty_id = var_indiv_prty_id
            AND       lkp_8.mrtl_sts_cd = var_typecode qualify rnk = 1 );
  -- Component LKP_INDIV_MRTL_STS_NEW, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_mrtl_sts_new AS
  (
            SELECT    lkp.indiv_prty_id,
                      lkp.mrtl_sts_cd,
                      lkp.indiv_mrtl_sts_strt_dttm,
                      exp_all_source.source_record_id,
                      row_number() over(PARTITION BY exp_all_source.source_record_id ORDER BY lkp.indiv_prty_id DESC,lkp.mrtl_sts_cd DESC,lkp.indiv_mrtl_sts_strt_dttm DESC) rnk
            FROM      exp_all_source
            left join
                      (
                               SELECT   indiv_mrtl_sts.mrtl_sts_cd              AS mrtl_sts_cd,
                                        indiv_mrtl_sts.indiv_mrtl_sts_strt_dttm AS indiv_mrtl_sts_strt_dttm,
                                        indiv_mrtl_sts.indiv_prty_id            AS indiv_prty_id
                               FROM     db_t_prod_core.indiv_mrtl_sts qualify row_number() over( PARTITION BY indiv_prty_id ORDER BY indiv_mrtl_sts_strt_dttm DESC, edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.indiv_prty_id = exp_all_source.out_indiv_prty_id qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_all_source.out_indiv_prty_id                                       AS in_indiv_prty_id,
                        exp_all_source.out_typecode                                            AS in_mrtl_sts_cd,
                        exp_all_source.indiv_mrtl_sts_strt_dt                                  AS in_indiv_mrtl_sts_strt_dt,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_indiv_mrtl_sts_end_dt,
                        :prcs_id                                                               AS in_prcs_id,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_all_source.indiv_mrtl_sts_strt_dt                                  AS in_trans_strt_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS in_trans_end_dttm,
                        lkp_indiv_mrtl_sts_new.indiv_prty_id                                   AS lkp_indiv_prty_id,
                        lkp_indiv_mrtl_sts_new.mrtl_sts_cd                                     AS lkp_mrtl_sts_cd,
                        lkp_indiv_mrtl_sts_new.indiv_mrtl_sts_strt_dttm                        AS lkp_indiv_mrtl_sts_strt_dt,
                        md5 ( exp_all_source.out_typecode
                                   || ltrim ( rtrim ( to_char ( exp_all_source.indiv_mrtl_sts_strt_dt , ''MM/DD/YYYY'' ) ) ) ) AS o_md5_src,
                        md5 ( lkp_indiv_mrtl_sts_new.mrtl_sts_cd
                                   || ltrim ( rtrim ( to_char ( lkp_indiv_mrtl_sts_new.indiv_mrtl_sts_strt_dttm , ''MM/DD/YYYY'' ) ) ) ) AS o_md5_tgt,
                        CASE
                                   WHEN o_md5_tgt IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN o_md5_src = o_md5_tgt THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS o_src_tgt,
                        exp_all_source.source_record_id
             FROM       exp_all_source
             inner join lkp_indiv_mrtl_sts_new
             ON         exp_all_source.source_record_id = lkp_indiv_mrtl_sts_new.source_record_id );
  -- Component rtr_insert_update_flag_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_insert_update_flag_insert as
  SELECT exp_cdc_check.in_indiv_prty_id           AS in_indiv_prty_id,
         exp_cdc_check.in_mrtl_sts_cd             AS in_mrtl_sts_cd,
         exp_cdc_check.in_indiv_mrtl_sts_strt_dt  AS in_indiv_mrtl_sts_strt_dt,
         exp_cdc_check.in_indiv_mrtl_sts_end_dt   AS in_indiv_mrtl_sts_end_dt,
         exp_cdc_check.in_prcs_id                 AS in_prcs_id,
         exp_cdc_check.in_edw_strt_dttm           AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm            AS in_edw_end_dttm,
         exp_cdc_check.in_trans_strt_dttm         AS in_trans_strt_dttm,
         exp_cdc_check.in_trans_end_dttm          AS in_trans_end_dttm,
         exp_cdc_check.lkp_indiv_prty_id          AS lkp_indiv_prty_id,
         exp_cdc_check.lkp_mrtl_sts_cd            AS lkp_mrtl_sts_cd,
         exp_cdc_check.lkp_indiv_mrtl_sts_strt_dt AS lkp_indiv_mrtl_sts_strt_dt,
         exp_cdc_check.o_src_tgt                  AS o_src_tgt,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''I''
  OR     exp_cdc_check.o_src_tgt = ''U'';
  
  -- Component tgt_indiv_mrtl_sts_insert, Type TARGET
  INSERT INTO db_t_prod_core.indiv_mrtl_sts
              (
                          indiv_prty_id,
                          mrtl_sts_cd,
                          indiv_mrtl_sts_strt_dttm,
                          indiv_mrtl_sts_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT rtr_insert_update_flag_insert.in_indiv_prty_id          AS indiv_prty_id,
         rtr_insert_update_flag_insert.in_mrtl_sts_cd            AS mrtl_sts_cd,
         rtr_insert_update_flag_insert.in_indiv_mrtl_sts_strt_dt AS indiv_mrtl_sts_strt_dttm,
         rtr_insert_update_flag_insert.in_indiv_mrtl_sts_end_dt  AS indiv_mrtl_sts_end_dttm,
         rtr_insert_update_flag_insert.in_prcs_id                AS prcs_id,
         rtr_insert_update_flag_insert.in_edw_strt_dttm          AS edw_strt_dttm,
         rtr_insert_update_flag_insert.in_edw_end_dttm           AS edw_end_dttm,
         rtr_insert_update_flag_insert.in_trans_strt_dttm        AS trans_strt_dttm,
         rtr_insert_update_flag_insert.in_trans_end_dttm         AS trans_end_dttm
  FROM   rtr_insert_update_flag_insert;
  
  -- Component tgt_indiv_mrtl_sts_insert, Type Post SQL
  UPDATE db_t_prod_core.indiv_mrtl_sts
    SET    edw_end_dttm=a.lead1 ,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT indiv_prty_id,
                                         edw_strt_dttm,
                                         trans_strt_dttm ,
                                         max(edw_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)     - interval ''1 second'' AS lead1 ,
                                         max(trans_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.indiv_mrtl_sts ) a

  WHERE  indiv_mrtl_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    indiv_mrtl_sts.trans_strt_dttm = a.trans_strt_dttm
  AND    indiv_mrtl_sts.indiv_prty_id=a.indiv_prty_id
  AND    cast(indiv_mrtl_sts.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(indiv_mrtl_sts.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;

END;
';