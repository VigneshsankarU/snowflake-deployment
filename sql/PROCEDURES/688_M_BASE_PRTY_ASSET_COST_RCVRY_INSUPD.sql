-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_COST_RCVRY_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component LKP_CLM, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm AS
  (
           SELECT   clm.clm_id                    AS clm_id,
                    clm.clm_type_cd               AS clm_type_cd,
                    clm.clm_mdia_type_cd          AS clm_mdia_type_cd,
                    clm.clm_submtl_type_cd        AS clm_submtl_type_cd,
                    clm.acdnt_type_cd             AS acdnt_type_cd,
                    clm.clm_ctgy_type_cd          AS clm_ctgy_type_cd,
                    clm.addl_insrnc_pln_ind       AS addl_insrnc_pln_ind,
                    clm.emplmt_rltd_ind           AS emplmt_rltd_ind,
                    clm.attny_invlvmt_ind         AS attny_invlvmt_ind,
                    clm.clm_prir_ind              AS clm_prir_ind,
                    clm.pmt_mode_cd               AS pmt_mode_cd,
                    clm.clm_oblgtn_type_cd        AS clm_oblgtn_type_cd,
                    clm.subrgtn_elgbl_cd          AS subrgtn_elgbl_cd,
                    clm.subrgtn_elgbly_rsn_cd     AS subrgtn_elgbly_rsn_cd,
                    clm.cury_cd                   AS cury_cd,
                    clm.incdt_ev_id               AS incdt_ev_id,
                    clm.insrd_at_fault_ind        AS insrd_at_fault_ind,
                    clm.cvge_in_ques_ind          AS cvge_in_ques_ind,
                    clm.extnt_of_fire_dmg_type_cd AS extnt_of_fire_dmg_type_cd,
                    clm.vfyd_clm_ind              AS vfyd_clm_ind,
                    clm.prcs_id                   AS prcs_id,
                    clm.clm_strt_dttm             AS clm_strt_dttm,
                    clm.clm_end_dttm              AS clm_end_dttm,
                    clm.edw_strt_dttm             AS edw_strt_dttm,
                    clm.edw_end_dttm              AS edw_end_dttm,
                    clm.trans_strt_dttm           AS trans_strt_dttm,
                    ltrim(rtrim(clm.clm_num))     AS clm_num,
                    clm.src_sys_cd                AS src_sys_cd
           FROM     db_t_prod_core.clm qualify row_number() over(PARTITION BY clm.clm_num,clm.src_sys_cd ORDER BY clm.edw_end_dttm DESC) = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''COST_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
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
  -- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_cc_prty_asset_cost_rcvry_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_prty_asset_cost_rcvry_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS policysystemid,
                $2  AS type_asset,
                $3  AS classification_code,
                $4  AS src_cd,
                $5  AS amt_cd,
                $6  AS recov_amt,
                $7  AS recov_dt,
                $8  AS updatetime,
                $9  AS retired,
                $10 AS rnk,
                $11 AS claimnumber,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH cc_prty_asset_cost_rcvry_x AS
                                  (
                                                  /* part1 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             vin_stg IS NOT NULL) THEN ''VIN:''
                                                                                                                  || vin_stg
                                                                                                  /* when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  concat(''VIN:'',cc_vehicle.Vin_stg ) */
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NOT NULL) THEN ''LP:''
                                                                                                                  || cc_vehicle.licenseplate_stg
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NULL) THEN cast(cc_vehicle.publicid_stg AS VARCHAR(64))
                                                                                                  /* else substring(PolicySystemId,CHARINDEX('':'',PolicySystemId)+1,len(PolicySystemId)-CHARINDEX('':'',PolicySystemId))  */
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                             AS src_cd,
                                                                  ''COST_TYPE7''                       amt_cd,
                                                                  cc_incident.salvageproceeds_stg AS recov_amt,
                                                                  cc_incident.datevehiclesold_stg AS recov_dt,
                                                                  cc_incident.updatetime_stg,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond ,
                                                                  ''PRTY_ASSET_COST_RCVRY''                                       AS idntn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                  /* left join pceim.DB_T_PROD_STAG.pc_personalvehicle on pc_personalvehicle.id=substring(PolicySystemId,CHARINDEX('':'',PolicySystemId)+1,len(PolicySystemId)-CHARINDEX('':'',PolicySystemId)) */
                                                  WHERE
                                                                  /* SalvageProceeds  is not null  */
                                                                  cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part2 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce(cc_vehicle.vin_stg,cc_vehicle.licenseplate_stg),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code ,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                        AS src_cd ,
                                                                  ''COST_TYPE1''                  cost_type ,
                                                                  salvagetow_stg             AS cost_amt ,
                                                                  cc_incident.updatetime_stg AS strt_dt ,
                                                                  cc_incident.updatetime_stg ,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond ,
                                                                  ''PRTY_ASSET_COST''                                             AS idntn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                  WHERE           salvagetow_stg IS NOT NULL
                                                  AND             cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part3 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce(cc_vehicle.vin_stg,cc_vehicle.licenseplate_stg),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                        AS src_cd,
                                                                  ''COST_TYPE2''                  cost_type,
                                                                  salvagestorage_stg            cost_amt,
                                                                  cc_incident.updatetime_stg    strt_dt ,
                                                                  cc_incident.updatetime_stg ,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond ,
                                                                  ''PRTY_ASSET_COST''                                             AS idntn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg,
                                                                  (:START_DTTM) AS start_dttm,
                                                                  (:END_DTTM)   AS end_dttm
                                                  FROM            db_t_prod_stag.cc_incident
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  inner join
                                                                  (
                                                                             SELECT     cc_claim.*
                                                                             FROM       db_t_prod_stag.cc_claim
                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                  /* left join pceim.DB_T_PROD_STAG.pc_personalvehicle on pc_personalvehicle.id=substring(PolicySystemId,CHARINDEX('':'',PolicySystemId)+1,len(PolicySystemId)-CHARINDEX('':'',PolicySystemId)) */
                                                  WHERE           salvagestorage_stg IS NOT NULL
                                                  AND             cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part4 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce(cc_vehicle.vin_stg,cc_vehicle.licenseplate_stg),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                        AS src_cd,
                                                                  ''COST_TYPE3''                  cost_type,
                                                                  salvagetitle_stg           AS cost_amt,
                                                                  cc_incident.updatetime_stg    strt_dt,
                                                                  cc_incident.updatetime_stg ,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond ,
                                                                  ''PRTY_ASSET_COST''                                             AS idntn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                  /* left join pceim.DB_T_PROD_STAG.pc_personalvehicle on pc_personalvehicle.id=substring(PolicySystemId,CHARINDEX('':'',PolicySystemId)+1,len(PolicySystemId)-CHARINDEX('':'',PolicySystemId)) */
                                                  WHERE           salvagetitle_stg IS NOT NULL
                                                  AND             cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part5 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce(cc_vehicle.vin_stg,cc_vehicle.licenseplate_stg),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                        AS src_cd,
                                                                  ''COST_TYPE4''                  cost_type,
                                                                  salvageprep_stg               cost_amt,
                                                                  cc_incident.updatetime_stg    strt_dt,
                                                                  cc_incident.updatetime_stg ,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond ,
                                                                  ''PRTY_ASSET_COST''                                             AS idntn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg ,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                                  /* left join pceim.DB_T_PROD_STAG.pc_personalvehicle on pc_personalvehicle.id=substring(PolicySystemId,CHARINDEX('':'',PolicySystemId)+1,len(PolicySystemId)-CHARINDEX('':'',PolicySystemId)) */
                                                  WHERE           salvageprep_stg IS NOT NULL
                                                  AND             cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part6 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NOT NULL) THEN ''VIN:''
                                                                                                                  || cc_vehicle.vin_stg
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NOT NULL) THEN ''LP:''
                                                                                                                  || cc_vehicle.licenseplate_stg
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NULL) THEN cast(cc_vehicle.publicid_stg AS VARCHAR(64))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END                             AS src_cd,
                                                                  cctl_salvagestatus.typecode_stg    salvage_sts,
                                                                  NULL,
                                                                  cc_incident.updatetime_stg strt_dt,
                                                                  cc_incident.updatetime_stg,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond,
                                                                  ''PRTY_ASSET_SALV_STS''                                            idtfctn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg ,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  left outer join db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  inner join      db_t_prod_stag.cc_transaction
                                                  ON              cc_transaction.claimid_stg=cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cctl_transaction
                                                  ON              cc_transaction.subtype_stg=cctl_transaction.id_stg
                                                  inner join      db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                  left outer join db_t_prod_stag.cctl_recoverycategory
                                                  ON              cc_transaction.recoverycategory_stg = cctl_recoverycategory.id_stg
                                                  left outer join db_t_prod_stag.cctl_salvagestatus
                                                  ON              cctl_salvagestatus.id_stg=cc_incident.salvagestatus_alfa_stg
                                                                  /* where cctl_recoverycategory.TYPECODE=''salvage'' and cctl_transaction.TYPECODE=''Recovery''  */
                                                  WHERE           cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM)
                                                  UNION
                                                  /* part7 */
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NOT NULL) THEN ''VIN:''
                                                                                                                  || cc_vehicle.vin_stg
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NOT NULL) THEN ''LP:''
                                                                                                                  || cc_vehicle.licenseplate_stg
                                                                                  WHEN (
                                                                                                                  cc_vehicle.policysystemid_stg IS NULL
                                                                                                  AND             cc_vehicle.vin_stg IS NULL
                                                                                                  AND             cc_vehicle.licenseplate_stg IS NULL) THEN cast(cc_vehicle.publicid_stg AS VARCHAR(64))
                                                                                  ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1)
                                                                  END                   AS id ,
                                                                  ''PRTY_ASSET_SBTYPE4''  AS type_asset ,
                                                                  ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                  CASE
                                                                                  WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                  ELSE ''SRC_SYS4''
                                                                  END          AS src_cd,
                                                                  ''ASSETRECOV''    salvage_sts,
                                                                  NULL,
                                                                  datevehiclerecovered_stg strt_dt,
                                                                  cc_incident.updatetime_stg,
                                                                  substr(policysystemid_stg,position('':'',policysystemid_stg)+1) AS join_cond,
                                                                  ''PRTY_ASSET_SALV_STS''                                            idtfctn_code,
                                                                  salvagestatus_alfa_stg,
                                                                  CASE
                                                                                  WHEN cc_incident.retired_stg=0
                                                                                  AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                  ELSE 1
                                                                  END AS retired,
                                                                  cc_claim.claimnumber_stg,
                                                                  cc_incident.createtime_stg ,
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
                                                  ON              cc_incident.claimid_stg = cc_claim.id_stg
                                                  inner join      db_t_prod_stag.cc_vehicle
                                                  ON              cc_incident.vehicleid_stg = cc_vehicle.id_stg
                                                  left outer join db_t_prod_stag.cctl_incident
                                                  ON              cc_incident.subtype_stg = cctl_incident.id_stg
                                                  WHERE           cc_incident.datevehiclerecovered_stg IS NOT NULL
                                                  AND             cc_incident.updatetime_stg > (:START_DTTM)
                                                  AND             cc_incident.updatetime_stg <= (:END_DTTM) )
                  SELECT   cc_prty_asset_cost_rcvry_x.id                                                                                AS policysystemid,
                           cc_prty_asset_cost_rcvry_x.type_asset                                                                        AS type_asset,
                           cc_prty_asset_cost_rcvry_x.classification_code                                                               AS classification_code,
                           cc_prty_asset_cost_rcvry_x.src_cd                                                                            AS src_cd,
                           cc_prty_asset_cost_rcvry_x.amt_cd                                                                            AS amt_cd,
                           cc_prty_asset_cost_rcvry_x.recov_amt                                                                         AS recov_amt,
                           cc_prty_asset_cost_rcvry_x.recov_dt                                                                          AS recov_dt,
                           cc_prty_asset_cost_rcvry_x.updatetime_stg                                                                    AS updatetime,
                           cc_prty_asset_cost_rcvry_x.retired                                                                           AS retired ,
                           rank() over(PARTITION BY policysystemid,type_asset,classification_code,claimnumber_stg ORDER BY updatetime ) AS rnk,
                           ltrim(rtrim(cc_prty_asset_cost_rcvry_x.claimnumber_stg))                                                     AS claimnumber
                  FROM     cc_prty_asset_cost_rcvry_x
                  WHERE    idntn_code=''PRTY_ASSET_COST_RCVRY'' qualify row_number() over(PARTITION BY policysystemid, cc_prty_asset_cost_rcvry_x.type_asset, cc_prty_asset_cost_rcvry_x.classification_code, claimnumber_stg ORDER BY updatetime DESC) = 1 ) src ) );
  -- Component exp_all_sources, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_sources AS
  (
            SELECT    lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                                                   AS out_class_cd,
                      sq_cc_prty_asset_cost_rcvry_x.policysystemid AS vin,
                      sq_cc_prty_asset_cost_rcvry_x.recov_amt      AS salvageproceeds,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      AS out_sbtype_cd,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                                                             AS out_prty_asset_rcvry_amt_cd,
                      sq_cc_prty_asset_cost_rcvry_x.recov_dt AS rcvry_dt,
                      lkp_4.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                               AS var_sys_src_cd,
                      var_sys_src_cd                           AS o_sys_src_cd,
                      sq_cc_prty_asset_cost_rcvry_x.retired    AS retired,
                      --sq_cc_prty_asset_cost_rcvry_x.rnk        AS rnk,
                      sq_cc_prty_asset_cost_rcvry_x.updatetime AS updatetime,
                      lkp_5.clm_id
                      /* replaced lookup LKP_CLM */
                      AS out_claimid,
                      lkp_6.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      AS var_sys_src_cd1,
                      sq_cc_prty_asset_cost_rcvry_x.source_record_id,
                      row_number() over (PARTITION BY sq_cc_prty_asset_cost_rcvry_x.source_record_id ORDER BY sq_cc_prty_asset_cost_rcvry_x.source_record_id) AS rnk
            FROM      sq_cc_prty_asset_cost_rcvry_x
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_1
            ON        lkp_1.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.classification_code
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_2
            ON        lkp_2.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.type_asset
            left join lkp_teradata_etl_ref_xlat lkp_3
            ON        lkp_3.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.amt_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_4
            ON        lkp_4.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.src_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_6
            ON        lkp_6.src_idntftn_val = ''SRC_SYS6''
			left join lkp_clm lkp_5
            ON        lkp_5.clm_num = sq_cc_prty_asset_cost_rcvry_x.claimnumber
            AND       lkp_5.src_sys_cd = lkp_6.tgt_idntftn_val --var_sys_src_cd1
			 qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_id AS
  (
            SELECT    lkp.prty_asset_id,
                      exp_all_sources.source_record_id,
                      row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_all_sources
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
            ON        lkp.asset_host_id_val = exp_all_sources.vin
            AND       lkp.prty_asset_sbtype_cd = exp_all_sources.out_sbtype_cd
            AND       lkp.prty_asset_clasfcn_cd = exp_all_sources.out_class_cd 
			qualify row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) 
			= 1 );
  -- Component LKP_PRTY_ASSET_COST_RCVRY, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_cost_rcvry AS
  (
             SELECT     lkp.prty_asset_cost_rcvry_dttm,
                        lkp.prty_asset_rcvry_amt,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        lkp.prty_asset_id,
                        exp_all_sources.source_record_id,
                        row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.prty_asset_cost_rcvry_dttm ASC,lkp.prty_asset_rcvry_amt ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.prty_asset_id ASC) rnk
             FROM       exp_all_sources
             inner join lkp_prty_asset_id
             ON         exp_all_sources.source_record_id = lkp_prty_asset_id.source_record_id
             left join
                        (
                                 SELECT   prty_asset_cost_rcvry.prty_asset_cost_rcvry_dttm AS prty_asset_cost_rcvry_dttm,
                                          prty_asset_cost_rcvry.prty_asset_rcvry_amt       AS prty_asset_rcvry_amt,
                                          prty_asset_cost_rcvry.edw_strt_dttm              AS edw_strt_dttm,
                                          prty_asset_cost_rcvry.edw_end_dttm               AS edw_end_dttm,
                                          prty_asset_cost_rcvry.prty_asset_id              AS prty_asset_id,
                                          prty_asset_cost_rcvry.rcvry_amt_type_cd          AS rcvry_amt_type_cd,
                                          prty_asset_cost_rcvry.clm_id                     AS clm_id
                                 FROM     db_t_prod_core.prty_asset_cost_rcvry qualify row_number() over(PARTITION BY prty_asset_cost_rcvry.prty_asset_id,prty_asset_cost_rcvry.rcvry_amt_type_cd, prty_asset_cost_rcvry.clm_id ORDER BY prty_asset_cost_rcvry.edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.prty_asset_id = lkp_prty_asset_id.prty_asset_id
             AND        lkp.rcvry_amt_type_cd = exp_all_sources.out_prty_asset_rcvry_amt_cd
             AND        lkp.clm_id = exp_all_sources.out_claimid 
			 qualify row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.prty_asset_cost_rcvry_dttm ASC,lkp.prty_asset_rcvry_amt ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.prty_asset_id ASC) 
			 = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     exp_all_sources.out_prty_asset_rcvry_amt_cd AS prty_asset_rcvry_amt_cd,
                        exp_all_sources.rcvry_dt                    AS rcvry_dt,
                        CASE
                                   WHEN exp_all_sources.rcvry_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                   ELSE exp_all_sources.rcvry_dt
                        END                             AS var_prty_asset_cost_rcvry_dt,
                        var_prty_asset_cost_rcvry_dt    AS o_prty_asset_cost_rcvry_dt,
                        exp_all_sources.salvageproceeds AS prty_asset_rcvry_amt,
                        CASE
                                   WHEN exp_all_sources.salvageproceeds IS NULL THEN 0
                                   ELSE exp_all_sources.salvageproceeds
                        END                                                  AS var_prty_asset_rcvry_amt,
                        :prcs_id                                             AS prcs_id,
                        lkp_prty_asset_cost_rcvry.prty_asset_id              AS lkp_prty_asset_id,
                        lkp_prty_asset_cost_rcvry.prty_asset_cost_rcvry_dttm AS lkp_prty_asset_cost_rcvry_dt1,
                        CASE
                                   WHEN lkp_prty_asset_cost_rcvry.prty_asset_rcvry_amt IS NULL THEN 0
                                   ELSE lkp_prty_asset_cost_rcvry.prty_asset_rcvry_amt
                        END AS lkp_prty_asset_rcvry_amt_var,
                        CASE
                                   WHEN lkp_prty_asset_id.prty_asset_id IS NULL THEN 9999
                                   ELSE lkp_prty_asset_id.prty_asset_id
                        END AS out_default_id,
                        md5 ( ltrim ( rtrim ( var_prty_asset_rcvry_amt ) )
                                   || ltrim ( rtrim ( var_prty_asset_cost_rcvry_dt ) ) ) AS chksum_inp,
                        md5 ( ltrim ( rtrim ( lkp_prty_asset_rcvry_amt_var ) )
                                   || ltrim ( rtrim ( lkp_prty_asset_cost_rcvry.prty_asset_cost_rcvry_dttm ) ) ) AS chksum_lkp,
                        CASE
                                   WHEN chksum_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN chksum_inp != chksum_lkp THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                                    AS out_ins_upd_flag,
                        lkp_prty_asset_cost_rcvry.edw_strt_dttm                                AS lkp_edw_strt_dttm,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        dateadd (second, -1, current_timestamp )                         AS edw_end_dttm_exp,
                        exp_all_sources.retired                                                AS retired,
                        lkp_prty_asset_cost_rcvry.edw_end_dttm                                 AS lkp_edw_end_dttm,
                        exp_all_sources.rnk                                                    AS rnk,
                        exp_all_sources.updatetime                                             AS updatetime,
                        exp_all_sources.out_claimid                                            AS claim_id,
                        exp_all_sources.source_record_id
             FROM       exp_all_sources
             inner join lkp_prty_asset_id
             ON         exp_all_sources.source_record_id = lkp_prty_asset_id.source_record_id
             inner join lkp_prty_asset_cost_rcvry
             ON         lkp_prty_asset_id.source_record_id = lkp_prty_asset_cost_rcvry.source_record_id );
  -- Component rtr_prty_asset_cost_recovery_INSERT, Type ROUTER Output Group INSERT
  create or replace table rtr_prty_asset_cost_recovery_insert as
  SELECT exp_data_transformation.out_default_id                AS pa_prty_asset_id,
         exp_data_transformation.out_ins_upd_flag              AS in_ins_upd_flag,
         exp_data_transformation.prty_asset_rcvry_amt_cd       AS prty_asset_rcvry_amt_cd,
         exp_data_transformation.o_prty_asset_cost_rcvry_dt    AS prty_asset_cost_rcvry_dt,
         exp_data_transformation.prty_asset_rcvry_amt          AS prty_asset_rcvry_amt,
         exp_data_transformation.prcs_id                       AS prcs_id,
         exp_data_transformation.lkp_prty_asset_id             AS lkp_prty_asset_id,
         exp_data_transformation.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
         exp_data_transformation.edw_strt_dttm                 AS edw_strt_dttm,
         exp_data_transformation.edw_end_dttm                  AS edw_end_dttm,
         exp_data_transformation.edw_end_dttm_exp              AS edw_end_dttm_exp,
         exp_data_transformation.lkp_prty_asset_cost_rcvry_dt1 AS lkp_prty_asset_cost_rcvry_dt1,
         exp_data_transformation.retired                       AS retired,
         exp_data_transformation.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
         exp_data_transformation.rnk                           AS rnk,
         exp_data_transformation.updatetime                    AS updatetime,
         exp_data_transformation.claim_id                      AS claim_id,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  (
                exp_data_transformation.out_ins_upd_flag = ''I''
         OR     exp_data_transformation.out_ins_upd_flag = ''U''
         OR     (
                       exp_data_transformation.retired = 0
                AND    exp_data_transformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) )
  AND    exp_data_transformation.out_default_id IS NOT NULL
  AND    exp_data_transformation.out_default_id <> 9999
  AND    exp_data_transformation.claim_id IS NOT NULL
  AND    exp_data_transformation.claim_id <> 9999 /*- - >
  INSERT                                          - - >
  INSERT incase OF CHANGE                         - - > retired earlier
  AND    now restored*/
  ;
  
  -- Component rtr_prty_asset_cost_recovery_RETIRE, Type ROUTER Output Group RETIRE
  create or replace table rtr_prty_asset_cost_recovery_retire as
  SELECT exp_data_transformation.out_default_id                AS pa_prty_asset_id,
         exp_data_transformation.out_ins_upd_flag              AS in_ins_upd_flag,
         exp_data_transformation.prty_asset_rcvry_amt_cd       AS prty_asset_rcvry_amt_cd,
         exp_data_transformation.o_prty_asset_cost_rcvry_dt    AS prty_asset_cost_rcvry_dt,
         exp_data_transformation.prty_asset_rcvry_amt          AS prty_asset_rcvry_amt,
         exp_data_transformation.prcs_id                       AS prcs_id,
         exp_data_transformation.lkp_prty_asset_id             AS lkp_prty_asset_id,
         exp_data_transformation.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
         exp_data_transformation.edw_strt_dttm                 AS edw_strt_dttm,
         exp_data_transformation.edw_end_dttm                  AS edw_end_dttm,
         exp_data_transformation.edw_end_dttm_exp              AS edw_end_dttm_exp,
         exp_data_transformation.lkp_prty_asset_cost_rcvry_dt1 AS lkp_prty_asset_cost_rcvry_dt1,
         exp_data_transformation.retired                       AS retired,
         exp_data_transformation.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
         exp_data_transformation.rnk                           AS rnk,
         exp_data_transformation.updatetime                    AS updatetime,
         exp_data_transformation.claim_id                      AS claim_id,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.out_ins_upd_flag = ''R''
  AND    exp_data_transformation.retired != 0
  AND    exp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_pa_reocvery_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_pa_reocvery_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_prty_asset_cost_recovery_insert.prty_asset_rcvry_amt_cd  AS prty_asset_rcvry_amt_cd1,
                rtr_prty_asset_cost_recovery_insert.prty_asset_cost_rcvry_dt AS prty_asset_cost_rcvry_dt1,
                rtr_prty_asset_cost_recovery_insert.prty_asset_rcvry_amt     AS prty_asset_rcvry_amt1,
                rtr_prty_asset_cost_recovery_insert.prcs_id                  AS prcs_id1,
                rtr_prty_asset_cost_recovery_insert.pa_prty_asset_id         AS prty_asset_id1,
                rtr_prty_asset_cost_recovery_insert.edw_strt_dttm            AS edw_strt_dttm1,
                rtr_prty_asset_cost_recovery_insert.edw_end_dttm             AS edw_end_dttm1,
                rtr_prty_asset_cost_recovery_insert.retired                  AS retired1,
                rtr_prty_asset_cost_recovery_insert.rnk                      AS rnk1,
                rtr_prty_asset_cost_recovery_insert.updatetime               AS updatetime1,
                rtr_prty_asset_cost_recovery_insert.claim_id                 AS claim_id1,
                0                                                            AS update_strategy_action,
				rtr_prty_asset_cost_recovery_insert.source_record_id
         FROM   rtr_prty_asset_cost_recovery_insert );
  -- Component upd_pa_recovery_upd1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_pa_recovery_upd1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_prty_asset_cost_recovery_retire.prty_asset_rcvry_amt_cd       AS prty_asset_rcvry_amt_cd3,
                rtr_prty_asset_cost_recovery_retire.prty_asset_cost_rcvry_dt      AS prty_asset_cost_rcvry_dt3,
                rtr_prty_asset_cost_recovery_retire.prty_asset_rcvry_amt          AS prty_asset_rcvry_amt3,
                rtr_prty_asset_cost_recovery_retire.prcs_id                       AS prcs_id3,
                rtr_prty_asset_cost_recovery_retire.lkp_prty_asset_id             AS prty_asset_id3,
                rtr_prty_asset_cost_recovery_retire.pa_prty_asset_id              AS pa_prty_asset_id3,
                rtr_prty_asset_cost_recovery_retire.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm3,
                rtr_prty_asset_cost_recovery_retire.edw_end_dttm_exp              AS edw_end_dttm_exp3,
                rtr_prty_asset_cost_recovery_retire.lkp_prty_asset_cost_rcvry_dt1 AS lkp_prty_asset_cost_rcvry_dt13,
                rtr_prty_asset_cost_recovery_retire.updatetime                    AS updatetime3,
                rtr_prty_asset_cost_recovery_retire.claim_id                      AS claim_id,
                1                                                                 AS update_strategy_action,
				rtr_prty_asset_cost_recovery_retire.source_record_id
         FROM   rtr_prty_asset_cost_recovery_retire );
  -- Component exp_pass_to_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt AS
  (
         SELECT upd_pa_reocvery_ins.prty_asset_rcvry_amt_cd1                                        AS prty_asset_rcvry_amt_cd1,
                upd_pa_reocvery_ins.prty_asset_cost_rcvry_dt1                                       AS prty_asset_cost_rcvry_dt1,
                upd_pa_reocvery_ins.prty_asset_rcvry_amt1                                           AS prty_asset_rcvry_amt1,
                upd_pa_reocvery_ins.prcs_id1                                                        AS prcs_id1,
                upd_pa_reocvery_ins.prty_asset_id1                                                  AS prty_asset_id1,
                dateadd ( second, ( 2 * ( upd_pa_reocvery_ins.rnk1 - 1 ) ), current_timestamp  ) AS var_edw_strt_dttm1,
                var_edw_strt_dttm1                                                                  AS edw_strt_dttm11,
                CASE
                       WHEN upd_pa_reocvery_ins.retired1 != 0 THEN var_edw_strt_dttm1
                       ELSE upd_pa_reocvery_ins.edw_end_dttm1
                END                             AS o_edw_end_dttm,
                upd_pa_reocvery_ins.updatetime1 AS updatetime1,
                upd_pa_reocvery_ins.claim_id1   AS claim_id1,
                CASE
                       WHEN upd_pa_reocvery_ins.retired1 != 0 THEN upd_pa_reocvery_ins.updatetime1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trans_end_dttm,
                upd_pa_reocvery_ins.source_record_id
         FROM   upd_pa_reocvery_ins );
  -- Component exp_pass_to_tgt11, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt11 AS
  (
         SELECT upd_pa_recovery_upd1.pa_prty_asset_id3              AS pa_prty_asset_id3,
                upd_pa_recovery_upd1.lkp_edw_strt_dttm3             AS lkp_edw_strt_dttm3,
                current_timestamp                                   AS edw_end_dttm_exp3,
                upd_pa_recovery_upd1.lkp_prty_asset_cost_rcvry_dt13 AS lkp_prty_asset_cost_rcvry_dt13,
                current_timestamp                                   AS in_trans_end_dttm_ret,
                upd_pa_recovery_upd1.claim_id                       AS claim_id,
                upd_pa_recovery_upd1.source_record_id
         FROM   upd_pa_recovery_upd1 );
  -- Component PRTY_ASSET_COST_RCVRY_upd1, Type TARGET
  merge
  INTO         db_t_prod_core.prty_asset_cost_rcvry
  USING        exp_pass_to_tgt11
  ON (
                            prty_asset_cost_rcvry.prty_asset_id = exp_pass_to_tgt11.pa_prty_asset_id3
               AND          prty_asset_cost_rcvry.prty_asset_cost_rcvry_dttm = exp_pass_to_tgt11.lkp_prty_asset_cost_rcvry_dt13
               AND          prty_asset_cost_rcvry.clm_id = exp_pass_to_tgt11.claim_id
               AND          prty_asset_cost_rcvry.edw_strt_dttm = exp_pass_to_tgt11.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    prty_asset_id = exp_pass_to_tgt11.pa_prty_asset_id3,
         prty_asset_cost_rcvry_dttm = exp_pass_to_tgt11.lkp_prty_asset_cost_rcvry_dt13,
         clm_id = exp_pass_to_tgt11.claim_id,
         edw_strt_dttm = exp_pass_to_tgt11.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt11.edw_end_dttm_exp3,
         trans_end_dttm = exp_pass_to_tgt11.in_trans_end_dttm_ret;
  
  -- Component PRTY_ASSET_COST_RCVRY, Type TARGET
  INSERT INTO db_t_prod_core.prty_asset_cost_rcvry
              (
                          prty_asset_id,
                          prty_asset_cost_rcvry_dttm,
                          rcvry_amt_type_cd,
                          prty_asset_rcvry_amt,
                          clm_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt.prty_asset_id1            AS prty_asset_id,
         exp_pass_to_tgt.prty_asset_cost_rcvry_dt1 AS prty_asset_cost_rcvry_dttm,
         exp_pass_to_tgt.prty_asset_rcvry_amt_cd1  AS rcvry_amt_type_cd,
         exp_pass_to_tgt.prty_asset_rcvry_amt1     AS prty_asset_rcvry_amt,
         exp_pass_to_tgt.claim_id1                 AS clm_id,
         exp_pass_to_tgt.prcs_id1                  AS prcs_id,
         exp_pass_to_tgt.edw_strt_dttm11           AS edw_strt_dttm,
         exp_pass_to_tgt.o_edw_end_dttm            AS edw_end_dttm,
         exp_pass_to_tgt.updatetime1               AS trans_strt_dttm,
         exp_pass_to_tgt.out_trans_end_dttm        AS trans_end_dttm
  FROM   exp_pass_to_tgt;
  
  -- Component PRTY_ASSET_COST_RCVRY, Type Post SQL
  UPDATE db_t_prod_core.prty_asset_cost_rcvry
    SET    trans_end_dttm = a.lead,
         edw_end_dttm = a.lead1
  FROM   (
                         SELECT DISTINCT prty_asset_id ,
                                         rcvry_amt_type_cd,
                                         clm_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id ,rcvry_amt_type_cd, clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY prty_asset_id ,rcvry_amt_type_cd, clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.prty_asset_cost_rcvry ) a

  WHERE  prty_asset_cost_rcvry.prty_asset_id = a.prty_asset_id
  AND    prty_asset_cost_rcvry.rcvry_amt_type_cd = a.rcvry_amt_type_cd
  AND    prty_asset_cost_rcvry.clm_id = a.clm_id
  AND    prty_asset_cost_rcvry.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_asset_cost_rcvry.trans_strt_dttm <> prty_asset_cost_rcvry.trans_end_dttm
  AND    a.lead IS NOT NULL ;

END;
';