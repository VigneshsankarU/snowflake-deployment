-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INSRBL_INT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
DWELLFIXEDFILTER STRING;
BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);
P_AGMT_TYPE_CD_POLICY_VERSION :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_AGMT_TYPE_CD_POLICY_VERSION'' order by insert_ts desc limit 1);

  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                -- AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_sys_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_INSURABLE_INTEREST, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_insurable_interest AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS prty_asset_sb_type,
                $2 AS insrbl_int_type_cd,
                $3 AS insrbl_int_key,
                $4 AS classification_cd,
                $5 AS src_sys,
                $6 AS retired,
                $7 AS ctstrph_expsr_ind,
                $8 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT type_stg                  AS prty_asset_sb_type,
                                                                  insurableinterestcategory AS insrbl_int_type_cd,
                                                                  nk_key                    AS insrbl_int_key ,
                                                                  classification_code       AS classification_cd,
                                                                  src_cd                    AS src_sys,
                                                                  retired,
                                                                  NULL AS ctstrph_expsr_ind
                                                  FROM            (
                                                                         SELECT insurableinterestcategory,
                                                                                nk_key,
                                                                                type_stg,
                                                                                classification_code,
                                                                                src_cd,
                                                                                updatetime_stg,
                                                                                retired ,
                                                                                (:START_DTTM) AS start_dttm,
                                                                                (:END_DTTM)   AS end_dttm
                                                                         FROM   (
                                                                                         SELECT   insurableinterestcategory,
                                                                                                  nk_key,
                                                                                                  type_stg,
                                                                                                  classification_code,
                                                                                                  src_cd,
                                                                                                  updatetime_stg,
                                                                                                  retired,
                                                                                                  (:START_DTTM)                                                                                                                AS start_dttm,
                                                                                                  (:END_DTTM)                                                                                                                  AS end_dttm,
                                                                                                  rank() over (PARTITION BY insurableinterestcategory,nk_key,type_stg,classification_code,src_cd ORDER BY updatetime_stg DESC)    rk
                                                                                         FROM     (
                                                                                                                  /************************ Injured Party*********************/
                                                                                                                  SELECT DISTINCT cast(''PERSON'' AS                VARCHAR(60)) AS insurableinterestcategory ,
                                                                                                                                  cast(cc_contact.publicid_stg AS VARCHAR(64)) AS nk_key,
                                                                                                                                  cast('''' AS                      VARCHAR(60)) AS type_stg ,
                                                                                                                                  cast('''' AS                      VARCHAR(60)) AS classification_code,
                                                                                                                                  ''SRC_SYS6''                                   AS src_cd,
                                                                                                                                  cc_contact.updatetime_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_incident.retired_stg=0
                                                                                                                                                  AND             cc_claim.retired_stg=0
                                                                                                                                                  AND             cc_claimcontact.retired_stg=0 THEN 0
                                                                                                                                                  ELSE 1
                                                                                                                                  END           AS retired,
                                                                                                                                  (:START_DTTM) AS start_dttm,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.cc_incident
                                                                                                                  inner join
                                                                                                                                  (
                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                  ON              cc_claim.id_stg=cc_incident.claimid_stg
                                                                                                                  inner join      db_t_prod_stag.cc_claimcontactrole
                                                                                                                  ON              cc_incident.id_stg=cc_claimcontactrole.incidentid_stg
                                                                                                                  inner join      db_t_prod_stag.cctl_incident
                                                                                                                  ON              cctl_incident.id_stg=cc_incident.subtype_stg
                                                                                                                  left outer join db_t_prod_stag.cc_claimcontact
                                                                                                                  ON              cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg
                                                                                                                  left outer join db_t_prod_stag.cc_contact
                                                                                                                  ON              cc_claimcontact.contactid_stg=cc_contact.id_stg
                                                                                                                  inner join      db_t_prod_stag.cctl_contact
                                                                                                                  ON              cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_contactrole
                                                                                                                  ON              (
                                                                                                                                                  cc_claimcontactrole.role_stg = cctl_contactrole.id_stg
                                                                                                                                  AND             cctl_contactrole.name_stg = ''Injured Party'')
                                                                                                                  left outer join db_t_prod_stag.cctl_severitytype
                                                                                                                  ON              cc_incident.severity_stg = cctl_severitytype.id_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_yesno
                                                                                                                  ON              cc_incident.attorneyrepresented_alfa_stg=cctl_yesno.id_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_occupancytype
                                                                                                                  ON              cc_incident.occupancytype_stg=cctl_occupancytype.id_stg
                                                                                                                  WHERE           cctl_incident.name_stg = ''InjuryIncident''
                                                                                                                  AND             cctl_contact.name_stg IN (''Person'',
                                                                                                                                                            ''Adjudicator'',
                                                                                                                                                            ''User Contact'',
                                                                                                                                                            ''Vendor (Person)'',
                                                                                                                                                            ''Attorney'',
                                                                                                                                                            ''Doctor'',
                                                                                                                                                            ''Policy Person'')
                                                                                                                  UNION
                                                                                                                  /**********************Damaged Vechicle***************************/
                                                                                                                  SELECT DISTINCT cast(''ASSET'' AS VARCHAR(60))AS insurableinterestcategory,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_vehicle.policysystemid_stg IS NOT NULL THEN substr(cc_vehicle.policysystemid_stg,position('':'' IN cc_vehicle.policysystemid_stg)+1,length(cc_vehicle.policysystemid_stg))
                                                                                                                                                  WHEN (
                                                                                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                                                                                  AND             cc_vehicle.vin_stg IS NOT NULL) THEN ''VIN:''
                                                                                                                                                                                  ||cc_vehicle.vin_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                                                                                  AND             cc_vehicle.licenseplate_stg IS NOT NULL) THEN ''LP:''
                                                                                                                                                                                  ||cc_vehicle.licenseplate_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                                                                                  AND             cc_vehicle.licenseplate_stg IS NULL) THEN cast(cc_vehicle.publicid_stg AS VARCHAR(64))
                                                                                                                                  END                                       AS nk_key,
                                                                                                                                  cast(''PRTY_ASSET_SBTYPE4'' AS  VARCHAR(60)) AS type_stg ,
                                                                                                                                  cast(''PRTY_ASSET_CLASFCN3'' AS VARCHAR(60)) AS classification_code,
                                                                                                                                  ''SRC_SYS6''                                 AS src_cd,
                                                                                                                                  cc_vehicle.updatetime_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_incident.retired_stg=0
                                                                                                                                                  AND             cc_claim.retired_stg=0
                                                                                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                                                                                  ELSE 1
                                                                                                                                  END           AS retired ,
                                                                                                                                  (:START_DTTM) AS start_dttm,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.cc_incident
                                                                                                                  inner join
                                                                                                                                  (
                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                  ON              cc_claim.id_stg=cc_incident.claimid_stg
                                                                                                                  inner join      db_t_prod_stag.cc_vehicle
                                                                                                                  ON              cc_incident.vehicleid_stg=cc_vehicle.id_stg
                                                                                                                  inner join      db_t_prod_stag.cctl_incident
                                                                                                                  ON              cctl_incident.id_stg=cc_incident.subtype_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_severitytype
                                                                                                                  ON              cc_incident.severity_stg = cctl_severitytype.id_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_yesno
                                                                                                                  ON              cc_incident.attorneyrepresented_alfa_stg=cctl_yesno.id_stg
                                                                                                                  left outer join db_t_prod_stag.cctl_occupancytype
                                                                                                                  ON              cc_incident.occupancytype_stg=cctl_occupancytype.id_stg
                                                                                                                  WHERE           cctl_incident.typecode_stg = ''VehicleIncident'' qualify rank() over (PARTITION BY insurableinterestcategory,upper(nk_key),type_stg,classification_code,src_cd ORDER BY retired ASC, cc_vehicle.updatetime_stg DESC) =1
                                                                                                                  /**********************Property***************************/
                                                                                                                  UNION
                                                                                                                  SELECT DISTINCT cast(''ASSET''AS                   VARCHAR(60)) AS insurableinterestcategory,
                                                                                                                                  cast(cc_incident.publicid_stg AS VARCHAR(64)) AS nk_key,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cctl_incident.name_stg = ''FixedPropertyIncident'' THEN ''PRTY_ASSET_SBTYPE5''
                                                                                                                                                  WHEN cctl_incident.name_stg =''OtherStructureIncident'' THEN ''PRTY_ASSET_SBTYPE11''
                                                                                                                                  END AS type_stg ,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cctl_incident.name_stg =''FixedPropertyIncident'' THEN ''PRTY_ASSET_CLASFCN1''
                                                                                                                                                  WHEN cctl_incident.name_stg = ''OtherStructureIncident'' THEN ''PRTY_ASSET_CLASFCN7''
                                                                                                                                  END        AS classification_code ,
                                                                                                                                  ''SRC_SYS6'' AS src_cd,
                                                                                                                                  cc_incident.updatetime_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_incident.retired_stg=0 THEN 0
                                                                                                                                                  ELSE 1
                                                                                                                                  END           AS retired ,
                                                                                                                                  (:START_DTTM) AS start_dttm,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.cc_incident
                                                                                                                  inner join
                                                                                                                                  (
                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                  ON              cc_incident.claimid_stg= cc_claim.id_stg
                                                                                                                  inner join      db_t_prod_stag.cctl_incident
                                                                                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                                                                  WHERE           cctl_incident.name_stg IN (''FixedPropertyIncident'',
                                                                                                                                                             ''OtherStructureIncident'')
                                                                                                                  UNION
                                                                                                                  SELECT DISTINCT cast(''ASSET''AS VARCHAR(60)) AS insurableinterestcategory,
                                                                                                                                  CASE
                                                                                                                                                  WHEN policysystemid_stg IS NULL THEN cast(cc_incident.publicid_stg AS VARCHAR(64))
                                                                                                                                                  ELSE substr(cc_policylocation.policysystemid_stg,position('':'' IN cc_policylocation.policysystemid_stg)+1,length(cc_policylocation.policysystemid_stg))
                                                                                                                                  END                                       AS nk_key,
                                                                                                                                  cast(''PRTY_ASSET_SBTYPE5'' AS  VARCHAR(60)) AS type_stg ,
                                                                                                                                  cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(60)) AS classification_code ,
                                                                                                                                  ''SRC_SYS6''                                 AS src_cd,
                                                                                                                                  cc_incident.updatetime_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_incident.retired_stg=0
                                                                                                                                                  AND             cc_claim.retired_stg=0
                                                                                                                                                  AND             cc_address.retired_stg=0 THEN 0
                                                                                                                                                  ELSE 1
                                                                                                                                  END           AS retired_stg,
                                                                                                                                  (:START_DTTM) AS start_dttm ,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            (
                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                  left join       db_t_prod_stag.cc_incident
                                                                                                                  ON              cc_claim.id_stg = cc_incident.claimid_stg
                                                                                                                  left join       db_t_prod_stag.cctl_incident
                                                                                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                                                                  left join       db_t_prod_stag.cc_address
                                                                                                                  ON              cc_claim.losslocationid_stg = cc_address.id_stg
                                                                                                                  left join       db_t_prod_stag.cc_policylocation
                                                                                                                  ON              cc_policylocation.addressid_stg= cc_address.id_stg
                                                                                                                  WHERE           cctl_incident.name_stg =''DwellingIncident''
                                                                                                                  UNION
                                                                                                                  SELECT DISTINCT cast(''ASSET''AS                                    VARCHAR(60)) AS insurableinterestcategory,
                                                                                                                                  cast(cc_assessmentcontentitem.publicid_stg AS     VARCHAR(64)) AS nk_key,
                                                                                                                                  cast(''PRTY_ASSET_SBTYPE11'' AS                     VARCHAR(60)) AS type_stg ,
                                                                                                                                  cast(cctl_contentlineitemschedule.typecode_stg AS VARCHAR(60)) AS classification_code,
                                                                                                                                  ''SRC_SYS6''                                                     AS src_cd,
                                                                                                                                  cc_assessmentcontentitem.updatetime_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN cc_incident.retired_stg=0
                                                                                                                                                  AND             cc_claim.retired_stg=0
                                                                                                                                                  AND             cc_assessmentcontentitem.retired_stg=0 THEN 0
                                                                                                                                                  ELSE 1
                                                                                                                                  END           AS retired_stg ,
                                                                                                                                  (:START_DTTM) AS start_dttm,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.cc_incident
                                                                                                                  inner join      db_t_prod_stag.cctl_incident
                                                                                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                                                                  inner join
                                                                                                                                  (
                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                  ON              cc_claim.id_stg=cc_incident.claimid_stg
                                                                                                                  inner join      db_t_prod_stag.cc_assessmentcontentitem
                                                                                                                  ON              cc_incident.id_stg = cc_assessmentcontentitem.incidentid_stg
                                                                                                                  left join       db_t_prod_stag.cc_policylocation
                                                                                                                  ON              cc_policylocation.id_stg=cc_incident.propertyid_stg
                                                                                                                  left join       db_t_prod_stag.cctl_contentlineitemcategory
                                                                                                                  ON              cc_assessmentcontentitem.contentcategory_stg = cctl_contentlineitemcategory.id_stg
                                                                                                                  left join       db_t_prod_stag.cctl_contentlineitemschedule
                                                                                                                  ON              cc_assessmentcontentitem.contentschedule_stg = cctl_contentlineitemschedule.id_stg
                                                                                                                  WHERE           cctl_incident.name_stg=''PropertyContentsIncident'' ) x ) y
                                                                         WHERE  rk=1
                                                                         UNION ALL
                                                                         SELECT insurableinterestcategory,
                                                                                nk_key,
                                                                                type_stg,
                                                                                classification_code,
                                                                                src_cd,
                                                                                updatetime_stg,
                                                                                retired,
                                                                                (:START_DTTM) AS start_dttm,
                                                                                (:END_DTTM)   AS end_dttm
                                                                         FROM   (
                                                                                         SELECT   insurableinterestcategory,
                                                                                                  nk_key,
                                                                                                  type_stg,
                                                                                                  classification_code,
                                                                                                  src_cd,
                                                                                                  updatetime_stg,
                                                                                                  retired,
                                                                                                  (:START_DTTM)                                                                                                                AS start_dttm,
                                                                                                  (:END_DTTM)                                                                                                                  AS end_dttm,
                                                                                                  rank() over (PARTITION BY insurableinterestcategory,nk_key,type_stg,classification_code,src_cd ORDER BY updatetime_stg DESC)    rk
                                                                                         FROM     (
                                                                                                                  SELECT DISTINCT cast(''ASSET'' AS                                 VARCHAR(60)) AS insurableinterestcategory,
                                                                                                                                  cast(pcx_holineschedcovitem_alfa.fixedid_stg AS VARCHAR(60)) AS nk_key,
                                                                                                                                  CASE
                                                                                                                                                  WHEN pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                                                                                             ''HOSI_SpecificOtherStructureExclItem_alfa'')THEN ''PRTY_ASSET_SBTYPE5''
                                                                                                                                                  WHEN pc_etlclausepattern.patternid_stg=''HOSI_ScheduledPropertyItem_alfa'' THEN ''PRTY_ASSET_SBTYPE7''
                                                                                                                                  END                                        AS type_stg,
                                                                                                                                  cast(choiceterm1_stg AS VARCHAR(60))       AS classification_code,
                                                                                                                                  ''SRC_SYS4''                                 AS src_cd,
                                                                                                                                  pcx_holineschedcovitem_alfa.updatetime_stg AS updatetime_stg,
                                                                                                                                  0                                          AS retired,
                                                                                                                                  (:START_DTTM)                              AS start_dttm,
                                                                                                                                  (:END_DTTM)                                AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.pcx_holineschcovitemcov_alfa
                                                                                                                  left join       db_t_prod_stag.pc_etlclausepattern
                                                                                                                  ON              pc_etlclausepattern.patternid_stg=pcx_holineschcovitemcov_alfa.patterncode_stg
                                                                                                                  left join       db_t_prod_stag.pcx_holineschedcovitem_alfa
                                                                                                                  ON              pcx_holineschedcovitem_alfa.id_stg=pcx_holineschcovitemcov_alfa.holineschcovitem_stg
                                                                                                                  WHERE           pc_etlclausepattern.patternid_stg IN (''HOSI_SpecificOtherStructureItem_alfa'',
                                                                                                                                                                        ''HOSI_ScheduledPropertyItem_alfa'',
                                                                                                                                                                        ''HOSI_SpecificOtherStructureExclItem_alfa'')
                                                                                                                  UNION
                                                                                                                  SELECT DISTINCT cast(''ASSET''AS                VARCHAR(60)) AS insurableinterestcategory,
                                                                                                                                  cast(fixedid_stg AS           VARCHAR(60)) AS nk_key,
                                                                                                                                  cast(''PRTY_ASSET_SBTYPE5'' AS  VARCHAR(60)) AS type_stg ,
                                                                                                                                  cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(60)) AS classification_code,
                                                                                                                                  ''SRC_SYS4''                                 AS src_cd,
                                                                                                                                  updatetime_stg,
                                                                                                                                  0             AS retired,
                                                                                                                                  (:START_DTTM) AS start_dttm,
                                                                                                                                  (:END_DTTM)   AS end_dttm
                                                                                                                  FROM            db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                  WHERE           fixedid_stg IS NOT NULL ) x ) y
                                                                         WHERE  rk=1 ) insurable_interest qualify row_number() over (PARTITION BY insrbl_int_type_cd,insrbl_int_key , prty_asset_sb_type, classification_cd,src_sys ORDER BY retired ASC) = 1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    sq_insurable_interest.insrbl_int_key     AS insrbl_int_key,
                      sq_insurable_interest.insrbl_int_type_cd AS insrbl_int_type_cd,
                      sq_insurable_interest.ctstrph_expsr_ind  AS ctstrph_expsr_ind,
                      sq_insurable_interest.retired            AS retired,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                      END AS out_classification_cd,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */
                                          IS NULL THEN sq_insurable_interest.src_sys
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */
                      END AS out_src_sys,
                      CASE
                                WHEN lkp_5.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_6.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      END AS out_prty_asset_sb_type,
                      sq_insurable_interest.source_record_id,
                      row_number() over (PARTITION BY sq_insurable_interest.source_record_id ORDER BY sq_insurable_interest.source_record_id) AS rnk
            FROM      sq_insurable_interest
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_1
            ON        lkp_1.src_idntftn_val = sq_insurable_interest.classification_cd
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_2
            ON        lkp_2.src_idntftn_val = sq_insurable_interest.classification_cd
            left join lkp_teradata_etl_ref_xlat_sys_src_cd lkp_3
            ON        lkp_3.src_idntftn_val = sq_insurable_interest.src_sys
            left join lkp_teradata_etl_ref_xlat_sys_src_cd lkp_4
            ON        lkp_4.src_idntftn_val = sq_insurable_interest.src_sys
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_5
            ON        lkp_5.src_idntftn_val = sq_insurable_interest.prty_asset_sb_type
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_6
            ON        lkp_6.src_idntftn_val = sq_insurable_interest.prty_asset_sb_type qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_id AS
  (
            SELECT    lkp.prty_asset_id,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                               SELECT   prty_asset.prty_asset_id             AS prty_asset_id,
                                        prty_asset.asset_insrnc_hist_type_cd AS asset_insrnc_hist_type_cd,
                                        prty_asset.asset_desc                AS asset_desc,
                                        prty_asset.prty_asset_name           AS prty_asset_name,
                                        prty_asset.prty_asset_strt_dttm      AS prty_asset_strt_dttm,
                                        prty_asset.prty_asset_end_dttm       AS prty_asset_end_dttm,
                                        prty_asset.edw_strt_dttm             AS edw_strt_dttm,
                                        prty_asset.edw_end_dttm              AS edw_end_dttm,
                                        prty_asset.src_sys_cd                AS src_sys_cd,
                                        prty_asset.asset_host_id_val         AS asset_host_id_val,
                                        prty_asset.prty_asset_sbtype_cd      AS prty_asset_sbtype_cd,
                                        prty_asset.prty_asset_clasfcn_cd     AS prty_asset_clasfcn_cd
                               FROM     db_t_prod_core.prty_asset qualify row_number() over(PARTITION BY asset_host_id_val,prty_asset_sbtype_cd,prty_asset_clasfcn_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.asset_host_id_val = exp_pass_from_source.insrbl_int_key
            AND       lkp.prty_asset_sbtype_cd = exp_pass_from_source.out_prty_asset_sb_type
            AND       lkp.prty_asset_clasfcn_cd = exp_pass_from_source.out_classification_cd qualify rnk = 1 );
  -- Component LKP_INDIV_CLM_CTR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_clm_ctr AS
  (
            SELECT    lkp.indiv_prty_id,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.indiv_prty_id DESC,lkp.nk_publc_id DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                    indiv.nk_publc_id   AS nk_publc_id
                             FROM   db_t_prod_core.indiv
                             WHERE  indiv.nk_publc_id IS NOT NULL ) lkp
            ON        lkp.nk_publc_id = exp_pass_from_source.insrbl_int_key qualify rnk = 1 );
  -- Component exp_data_transformation_input, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation_input AS
  (
             SELECT     lkp_prty_asset_id.prty_asset_id         AS prty_asset_id,
                        lkp_indiv_clm_ctr.indiv_prty_id         AS indiv_prty_id,
                        exp_pass_from_source.insrbl_int_type_cd AS insrbl_int_ctgy_cd,
                        exp_pass_from_source.ctstrph_expsr_ind  AS ctstrph_expsr_ind,
                        exp_pass_from_source.retired            AS retired,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_prty_asset_id
             ON         exp_pass_from_source.source_record_id = lkp_prty_asset_id.source_record_id
             inner join lkp_indiv_clm_ctr
             ON         lkp_prty_asset_id.source_record_id = lkp_indiv_clm_ctr.source_record_id );
  -- Component LKP_INSRBL_INT_PRTYID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_insrbl_int_prtyid AS
  (
             SELECT     lkp.insrbl_int_id,
                        lkp.insrbl_int_ctgy_cd,
                        lkp.ctstrph_expsr_ind,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_pass_from_source.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.insrbl_int_id DESC,lkp.insrbl_int_ctgy_cd DESC,lkp.ctstrph_expsr_ind DESC,lkp.src_sys_cd DESC,lkp.injured_prty_id DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
             FROM       exp_pass_from_source
             inner join exp_data_transformation_input
             ON         exp_pass_from_source.source_record_id = exp_data_transformation_input.source_record_id
             left join
                        (
                                 SELECT   insrbl_int.insrbl_int_id      AS insrbl_int_id,
                                          insrbl_int.ctstrph_expsr_ind  AS ctstrph_expsr_ind,
                                          insrbl_int.edw_strt_dttm      AS edw_strt_dttm,
                                          insrbl_int.edw_end_dttm       AS edw_end_dttm,
                                          insrbl_int.insrbl_int_ctgy_cd AS insrbl_int_ctgy_cd,
                                          insrbl_int.src_sys_cd         AS src_sys_cd,
                                          insrbl_int.injured_prty_id    AS injured_prty_id
                                 FROM     db_t_prod_core.insrbl_int
                                 WHERE    insrbl_int.insrbl_int_ctgy_cd = ''PERSON''
                                 AND      insrbl_int.injured_prty_id IS NOT NULL qualify row_number () over (PARTITION BY insrbl_int_ctgy_cd,insrbl_int_id ORDER BY edw_end_dttm DESC)=1 ) lkp
             ON         lkp.insrbl_int_ctgy_cd = exp_data_transformation_input.insrbl_int_ctgy_cd
             AND        lkp.src_sys_cd = exp_pass_from_source.out_src_sys
             AND        lkp.injured_prty_id = exp_data_transformation_input.indiv_prty_id qualify rnk = 1 );
  -- Component LKP_INSRBL_INT_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_insrbl_int_prty_asset_id AS
  (
             SELECT     lkp.insrbl_int_id,
                        lkp.insrbl_int_ctgy_cd,
                        lkp.ctstrph_expsr_ind,
                        lkp.prty_asset_id,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_pass_from_source.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.insrbl_int_id DESC,lkp.insrbl_int_ctgy_cd DESC,lkp.ctstrph_expsr_ind DESC,lkp.src_sys_cd DESC,lkp.prty_asset_id DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
             FROM       exp_pass_from_source
             inner join exp_data_transformation_input
             ON         exp_pass_from_source.source_record_id = exp_data_transformation_input.source_record_id
             left join
                        (
                                 SELECT   insrbl_int.insrbl_int_id      AS insrbl_int_id,
                                          insrbl_int.ctstrph_expsr_ind  AS ctstrph_expsr_ind,
                                          insrbl_int.edw_strt_dttm      AS edw_strt_dttm,
                                          insrbl_int.edw_end_dttm       AS edw_end_dttm,
                                          insrbl_int.insrbl_int_ctgy_cd AS insrbl_int_ctgy_cd,
                                          insrbl_int.src_sys_cd         AS src_sys_cd,
                                          insrbl_int.prty_asset_id      AS prty_asset_id
                                 FROM     db_t_prod_core.insrbl_int
                                 WHERE    insrbl_int.insrbl_int_ctgy_cd = ''ASSET''
                                 AND      insrbl_int.prty_asset_id IS NOT NULL qualify row_number () over (PARTITION BY insrbl_int_ctgy_cd,insrbl_int_id ORDER BY edw_end_dttm DESC)=1 ) lkp
             ON         lkp.insrbl_int_ctgy_cd = exp_data_transformation_input.insrbl_int_ctgy_cd
             AND        lkp.src_sys_cd = exp_pass_from_source.out_src_sys
             AND        lkp.prty_asset_id = exp_data_transformation_input.prty_asset_id qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     exp_data_transformation_input.prty_asset_id      AS prty_asset_id,
                        exp_data_transformation_input.insrbl_int_ctgy_cd AS insrbl_int_ctgy_cd,
                        exp_data_transformation_input.indiv_prty_id      AS indiv_prty_id,
                        exp_data_transformation_input.ctstrph_expsr_ind  AS in_ctstrph_expsr_ind,
                        exp_pass_from_source.out_src_sys                 AS sys_src_cd,
                        exp_data_transformation_input.retired            AS retired,
                        lkp_insrbl_int_prtyid.edw_end_dttm               AS lkp_edw_end_dttm_prty,
                        lkp_insrbl_int_prty_asset_id.edw_end_dttm        AS lkp_edw_end_dttm_prty_asset,
                        CASE
                                   WHEN (
                                                         exp_data_transformation_input.insrbl_int_ctgy_cd = ''PERSON''
                                              AND        lkp_insrbl_int_prtyid.insrbl_int_id IS NOT NULL ) THEN lkp_insrbl_int_prtyid.insrbl_int_id
                                   ELSE
                                              CASE
                                                         WHEN exp_data_transformation_input.insrbl_int_ctgy_cd = ''ASSET''
                                                         AND        lkp_insrbl_int_prty_asset_id.insrbl_int_id IS NOT NULL THEN lkp_insrbl_int_prty_asset_id.insrbl_int_id
                                                         ELSE NULL
                                              END
                        END                                                                    AS v_insrbl_int_id,
                        v_insrbl_int_id                                                        AS lkp_insrbl_int_id,
                        :prcs_id                                                               AS out_prcs_id,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        CASE
                                   WHEN v_insrbl_int_id IS NULL THEN ''I''
                                   ELSE ''R''
                        END AS insupd_flag,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join exp_data_transformation_input
             ON         exp_pass_from_source.source_record_id = exp_data_transformation_input.source_record_id
             inner join lkp_insrbl_int_prtyid
             ON         exp_data_transformation_input.source_record_id = lkp_insrbl_int_prtyid.source_record_id
             inner join lkp_insrbl_int_prty_asset_id
             ON         lkp_insrbl_int_prtyid.source_record_id = lkp_insrbl_int_prty_asset_id.source_record_id );
  -- Component rtr_insrbl_int_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_insrbl_int_insert as
  SELECT exp_data_transformation.lkp_insrbl_int_id           AS insrbl_int_id,
         exp_data_transformation.prty_asset_id               AS prty_asset_id,
         exp_data_transformation.indiv_prty_id               AS indiv_prty_id,
         exp_data_transformation.insrbl_int_ctgy_cd          AS insrbl_int_ctgy_cd,
         exp_data_transformation.in_ctstrph_expsr_ind        AS out_ctstrph_expsr_ind,
         exp_data_transformation.out_prcs_id                 AS out_prcs_id,
         exp_data_transformation.edw_strt_dttm               AS edw_strt_dttm,
         exp_data_transformation.edw_end_dttm                AS edw_end_dttm,
         exp_data_transformation.sys_src_cd                  AS sys_src_cd,
         exp_data_transformation.retired                     AS retired,
         exp_data_transformation.insupd_flag                 AS insupd_flag,
         exp_data_transformation.lkp_edw_end_dttm_prty       AS lkp_edw_end_dttm_prty,
         exp_data_transformation.lkp_edw_end_dttm_prty_asset AS lkp_edw_end_dttm_prty_asset,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.insupd_flag = ''I''
  AND    ( (
                       exp_data_transformation.insrbl_int_ctgy_cd = ''PERSON''
                AND    exp_data_transformation.indiv_prty_id IS NOT NULL )
         OR     (
                       exp_data_transformation.insrbl_int_ctgy_cd = ''ASSET''
                AND    exp_data_transformation.prty_asset_id IS NOT NULL ) ) /*- -
  OR     updateflag = 1*/
  ;
  
  -- Component rtr_insrbl_int_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_insrbl_int_retired as
  SELECT exp_data_transformation.lkp_insrbl_int_id           AS insrbl_int_id,
         exp_data_transformation.prty_asset_id               AS prty_asset_id,
         exp_data_transformation.indiv_prty_id               AS indiv_prty_id,
         exp_data_transformation.insrbl_int_ctgy_cd          AS insrbl_int_ctgy_cd,
         exp_data_transformation.in_ctstrph_expsr_ind        AS out_ctstrph_expsr_ind,
         exp_data_transformation.out_prcs_id                 AS out_prcs_id,
         exp_data_transformation.edw_strt_dttm               AS edw_strt_dttm,
         exp_data_transformation.edw_end_dttm                AS edw_end_dttm,
         exp_data_transformation.sys_src_cd                  AS sys_src_cd,
         exp_data_transformation.retired                     AS retired,
         exp_data_transformation.insupd_flag                 AS insupd_flag,
         exp_data_transformation.lkp_edw_end_dttm_prty       AS lkp_edw_end_dttm_prty,
         exp_data_transformation.lkp_edw_end_dttm_prty_asset AS lkp_edw_end_dttm_prty_asset,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.insupd_flag = ''R''
  AND    exp_data_transformation.retired != 0
  AND    (
                exp_data_transformation.lkp_edw_end_dttm_prty = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         OR     exp_data_transformation.lkp_edw_end_dttm_prty_asset = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component updstr_insrbl_int_upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updstr_insrbl_int_upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insrbl_int_retired.insrbl_int_id AS insrbl_int_id3,
                rtr_insrbl_int_retired.edw_strt_dttm AS edw_end_dttm1,
                1                                    AS update_strategy_action,
				rtr_insrbl_int_retired.source_record_id
         FROM   rtr_insrbl_int_retired );
  -- Component exp_pass_to_target_upd_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd_retired AS
  (
         SELECT updstr_insrbl_int_upd_retired.insrbl_int_id3 AS insrbl_int_id,
                updstr_insrbl_int_upd_retired.edw_end_dttm1  AS edw_end_dttm1,
                updstr_insrbl_int_upd_retired.source_record_id
         FROM   updstr_insrbl_int_upd_retired );
  -- Component updstr_insrbl_int_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updstr_insrbl_int_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insrbl_int_insert.prty_asset_id         AS prty_asset_id,
                rtr_insrbl_int_insert.indiv_prty_id         AS indiv_prty_id,
                rtr_insrbl_int_insert.insrbl_int_ctgy_cd    AS insrbl_int_ctgy_cd,
                rtr_insrbl_int_insert.out_ctstrph_expsr_ind AS out_ctstrph_expsr_ind,
                rtr_insrbl_int_insert.out_prcs_id           AS out_prcs_id,
                rtr_insrbl_int_insert.edw_strt_dttm         AS edw_strt_dttm1,
                rtr_insrbl_int_insert.edw_end_dttm          AS edw_end_dttm1,
                rtr_insrbl_int_insert.sys_src_cd            AS sys_src_cd1,
                rtr_insrbl_int_insert.retired               AS retired1,
                0                                           AS update_strategy_action,
				rtr_insrbl_int_insert.source_record_id
         FROM   rtr_insrbl_int_insert );
  -- Component tgt_insrbl_int_upd_retired, Type TARGET
  merge
  INTO         db_t_prod_core.insrbl_int
  USING        exp_pass_to_target_upd_retired
  ON (
                            insrbl_int.insrbl_int_id = exp_pass_to_target_upd_retired.insrbl_int_id)
  WHEN matched THEN
  UPDATE
  SET    insrbl_int_id = exp_pass_to_target_upd_retired.insrbl_int_id,
         edw_end_dttm = exp_pass_to_target_upd_retired.edw_end_dttm1;
  
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT seq_insrbl_int.NEXTVAL                      AS insrbl_int_id,
                updstr_insrbl_int_ins.prty_asset_id         AS prty_asset_id,
                updstr_insrbl_int_ins.indiv_prty_id         AS indiv_prty_id,
                updstr_insrbl_int_ins.insrbl_int_ctgy_cd    AS insrbl_int_ctgy_cd,
                updstr_insrbl_int_ins.out_ctstrph_expsr_ind AS ctstrph_expsr_ind,
                updstr_insrbl_int_ins.out_prcs_id           AS prcs_id,
                updstr_insrbl_int_ins.edw_strt_dttm1        AS edw_strt_dttm1,
                CASE
                       WHEN updstr_insrbl_int_ins.retired1 = 0 THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE current_timestamp
                END                               AS out_edw_end_dttm1,
                updstr_insrbl_int_ins.sys_src_cd1 AS sys_src_cd1,
                updstr_insrbl_int_ins.source_record_id
         FROM   updstr_insrbl_int_ins );
  -- Component tgt_insrbl_int_ins, Type TARGET
  INSERT INTO db_t_prod_core.insrbl_int
              (
                          insrbl_int_id,
                          insrbl_int_ctgy_cd,
                          ctstrph_expsr_ind,
                          injured_prty_id,
                          prty_asset_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          src_sys_cd
              )
  SELECT exp_pass_to_target_ins.insrbl_int_id      AS insrbl_int_id,
         exp_pass_to_target_ins.insrbl_int_ctgy_cd AS insrbl_int_ctgy_cd,
         exp_pass_to_target_ins.ctstrph_expsr_ind  AS ctstrph_expsr_ind,
         exp_pass_to_target_ins.indiv_prty_id      AS injured_prty_id,
         exp_pass_to_target_ins.prty_asset_id      AS prty_asset_id,
         exp_pass_to_target_ins.prcs_id            AS prcs_id,
         exp_pass_to_target_ins.edw_strt_dttm1     AS edw_strt_dttm,
         exp_pass_to_target_ins.out_edw_end_dttm1  AS edw_end_dttm,
         exp_pass_to_target_ins.sys_src_cd1        AS src_sys_cd
  FROM   exp_pass_to_target_ins;

END;
';