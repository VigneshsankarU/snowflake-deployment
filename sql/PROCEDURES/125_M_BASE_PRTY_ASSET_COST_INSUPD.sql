-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_COST_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
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
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   cc_prty_asset_cost_rcvry_x.id     AS policysystemid_stg,
                                                    cc_prty_asset_cost_rcvry_x.type_a AS type_asset,
                                                    cc_prty_asset_cost_rcvry_x.classification_code,
                                                    cc_prty_asset_cost_rcvry_x.src_cd,
                                                    cost_type                                                                              AS amt_cd,
                                                    cc_prty_asset_cost_rcvry_x.cost_amt                                                                   AS recov_amt,
                                                    cc_prty_asset_cost_rcvry_x.strt_dt                                                                    AS recov_dt,
                                                    cc_prty_asset_cost_rcvry_x.updatetime_stg                                                             AS updatetime,
                                                    cc_prty_asset_cost_rcvry_x.retired                                                                    AS retired,
                                                    rank() over(PARTITION BY policysystemid_stg,type_asset,classification_code,amt_cd ORDER BY recov_dt ) AS rnk
                                           FROM     (
                                                                    SELECT DISTINCT
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce((''VIN:''
                                                                                                                                    ||cc_vehicle.vin_stg),(''LP:''
                                                                                                                                    ||cc_vehicle.licenseplate_stg)),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                                    ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)- position('':'',policysystemid_stg))
                                                                                    END                   AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''  AS type_a ,
                                                                                    ''PRTY_ASSET_CLASFCN3'' AS classification_code ,
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                                    ELSE ''SRC_SYS4''
                                                                                    END                        AS src_cd ,
                                                                                    ''COST_TYPE1''                  cost_type ,
                                                                                    salvagetow_stg                cost_amt ,
                                                                                    cc_incident.updatetime_stg    strt_dt ,
                                                                                    cc_incident.updatetime_stg ,
                                                                                    substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)-position('':'',policysystemid_stg)) AS join_cond ,
                                                                                    ''PRTY_ASSET_COST''                                                                                                         AS idntn_code,
                                                                                    salvagestatus_alfa_stg,
                                                                                    CASE
                                                                                                    WHEN cc_incident.retired_stg=0
                                                                                                    AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                                    ELSE 1
                                                                                    END AS retired,
                                                                                    cc_claim.claimnumber_stg,
                                                                                    cc_incident.createtime_stg
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
                                                                    WHERE
                                                                                    /* salvagetow_STG IS NOT NULL --EIM-45819 */
                                                                                    cc_incident.updatetime_stg > ($start_dttm)
                                                                    AND             cc_incident.updatetime_stg <= ($end_dttm)
                                                                    UNION
                                                                    SELECT DISTINCT
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce((''VIN:''
                                                                                                                                    ||cc_vehicle.vin_stg),(''LP:''
                                                                                                                                    ||cc_vehicle.licenseplate_stg)),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                                    ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)- position('':'',policysystemid_stg))
                                                                                    END                   AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''  AS type_a ,
                                                                                    ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                                    ELSE ''SRC_SYS4''
                                                                                    END                        AS src_cd,
                                                                                    ''COST_TYPE2''                  cost_type ,
                                                                                    salvagestorage_stg            cost_amt ,
                                                                                    cc_incident.updatetime_stg    strt_dt ,
                                                                                    cc_incident.updatetime_stg ,
                                                                                    substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)-position('':'',policysystemid_stg)) AS join_cond ,
                                                                                    ''PRTY_ASSET_COST''                                                                                                         AS idntn_code,
                                                                                    salvagestatus_alfa_stg,
                                                                                    CASE
                                                                                                    WHEN cc_incident.retired_stg=0
                                                                                                    AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                                    ELSE 1
                                                                                    END AS retired,
                                                                                    cc_claim.claimnumber_stg,
                                                                                    cc_incident.createtime_stg
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
                                                                    WHERE
                                                                                    /* SalvageStorage_STG IS NOT NULL --EIM-45819 */
                                                                                    cc_incident.updatetime_stg > ($start_dttm)
                                                                    AND             cc_incident.updatetime_stg <= ($end_dttm)
                                                                    UNION
                                                                    SELECT DISTINCT
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce((''VIN:''
                                                                                                                                    ||cc_vehicle.vin_stg),(''LP:''
                                                                                                                                    ||cc_vehicle.licenseplate_stg)),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                                    ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)- position('':'',policysystemid_stg))
                                                                                    END                   AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''  AS type_a ,
                                                                                    ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                                    ELSE ''SRC_SYS4''
                                                                                    END                        AS src_cd,
                                                                                    ''COST_TYPE3''                  cost_type,
                                                                                    salvagetitle_stg           AS cost_amt,
                                                                                    cc_incident.updatetime_stg    strt_dt,
                                                                                    cc_incident.updatetime_stg ,
                                                                                    substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)-position('':'',policysystemid_stg)) AS join_cond ,
                                                                                    ''PRTY_ASSET_COST''                                                                                                         AS idntn_code,
                                                                                    salvagestatus_alfa_stg,
                                                                                    CASE
                                                                                                    WHEN cc_incident.retired_stg=0
                                                                                                    AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                                    ELSE 1
                                                                                    END AS retired,
                                                                                    cc_claim.claimnumber_stg,
                                                                                    cc_incident.createtime_stg
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
                                                                    WHERE
                                                                                    /* salvagetitle_stg IS NOT NULL --EIM-45819 */
                                                                                    cc_incident.updatetime_stg > ($start_dttm)
                                                                    AND             cc_incident.updatetime_stg <= ($end_dttm)
                                                                    UNION
                                                                    SELECT DISTINCT
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN cast(coalesce(coalesce((''VIN:''
                                                                                                                                    ||cc_vehicle.vin_stg),(''LP:''
                                                                                                                                    ||cc_vehicle.licenseplate_stg)),cc_vehicle.publicid_stg) AS VARCHAR(100))
                                                                                                    ELSE substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)- position('':'',policysystemid_stg))
                                                                                    END                   AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''  AS type_a ,
                                                                                    ''PRTY_ASSET_CLASFCN3'' AS classification_code,
                                                                                    CASE
                                                                                                    WHEN policysystemid_stg IS NULL THEN ''SRC_SYS6''
                                                                                                    ELSE ''SRC_SYS4''
                                                                                    END                        AS src_cd,
                                                                                    ''COST_TYPE4''               AS cost_type,
                                                                                    salvageprep_stg               cost_amt,
                                                                                    cc_incident.updatetime_stg AS strt_dt,
                                                                                    cc_incident.updatetime_stg ,
                                                                                    substr(policysystemid_stg,position('':'',policysystemid_stg)+1,length(policysystemid_stg)-position('':'',policysystemid_stg)) AS join_cond ,
                                                                                    ''PRTY_ASSET_COST''                                                                                                         AS idntn_code,
                                                                                    salvagestatus_alfa_stg,
                                                                                    CASE
                                                                                                    WHEN cc_incident.retired_stg=0
                                                                                                    AND             cc_vehicle.retired_stg=0 THEN 0
                                                                                                    ELSE 1
                                                                                    END AS retired,
                                                                                    cc_claim.claimnumber_stg,
                                                                                    cc_incident.createtime_stg
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
                                                                                    /* salvageprep_stg IS NOT NULL   --EIM-45819 */
                                                                                    cc_incident.updatetime_stg > ($start_dttm)
                                                                    AND             cc_incident.updatetime_stg <= ($end_dttm)
                                                                                    /* EIM-40879 */
                                                    ) cc_prty_asset_cost_rcvry_x ) src ) );
  -- Component exp_pass_to_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt AS
  (
            SELECT    lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                                                               AS out_class_cd,
                      sq_cc_prty_asset_cost_rcvry_x.recov_amt                  AS cost_amount,
                      to_char ( sq_cc_prty_asset_cost_rcvry_x.policysystemid ) AS var_fixedid,
                      CASE
                                WHEN lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                      END         AS out_cost_type,
                      var_fixedid AS out_asset_host_id_val,
                      lkp_4.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      AS out_prty_asset_sb_type_cd,
                      lkp_5.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      AS o_src_sys_cd1,
                      CASE
                                WHEN sq_cc_prty_asset_cost_rcvry_x.updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                ELSE sq_cc_prty_asset_cost_rcvry_x.updatetime
                      END                                                                    AS o_updatetime,
                      to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd HH24:MI:SS.FF6'' ) AS end_dt,
                      sq_cc_prty_asset_cost_rcvry_x.retired                                  AS retired,
                      --sq_cc_prty_asset_cost_rcvry_x.rnk                                      AS rnk,
                      sq_cc_prty_asset_cost_rcvry_x.source_record_id,
                      row_number() over (PARTITION BY sq_cc_prty_asset_cost_rcvry_x.source_record_id ORDER BY sq_cc_prty_asset_cost_rcvry_x.source_record_id) AS rnk
            FROM      sq_cc_prty_asset_cost_rcvry_x
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_1
            ON        lkp_1.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.classification_code
            left join lkp_teradata_etl_ref_xlat lkp_2
            ON        lkp_2.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.amt_cd
            left join lkp_teradata_etl_ref_xlat lkp_3
            ON        lkp_3.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.amt_cd
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_4
            ON        lkp_4.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.type_asset
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_5
            ON        lkp_5.src_idntftn_val = sq_cc_prty_asset_cost_rcvry_x.src_cd qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_id AS
  (
            SELECT    lkp.prty_asset_id,
                      exp_pass_to_tgt.source_record_id,
                      row_number() over(PARTITION BY exp_pass_to_tgt.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_pass_to_tgt
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
            ON        lkp.asset_host_id_val = exp_pass_to_tgt.out_asset_host_id_val
            AND       lkp.prty_asset_sbtype_cd = exp_pass_to_tgt.out_prty_asset_sb_type_cd
            AND       lkp.prty_asset_clasfcn_cd = exp_pass_to_tgt.out_class_cd qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET_COST, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_cost AS
  (
             SELECT     lkp.prty_asset_id,
                        lkp.cost_type_cd,
                        lkp.prty_asset_cost_strt_dttm,
                        lkp.prty_asset_cost_end_dttm,
                        lkp.prty_asset_cost_amt,
                        lkp.trans_strt_dttm,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_pass_to_tgt.source_record_id,
                        row_number() over(PARTITION BY exp_pass_to_tgt.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.cost_type_cd ASC,lkp.prty_asset_cost_strt_dttm ASC,lkp.prty_asset_cost_end_dttm ASC,lkp.prty_asset_cost_amt ASC,lkp.trans_strt_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
             FROM       exp_pass_to_tgt
             inner join lkp_prty_asset_id
             ON         exp_pass_to_tgt.source_record_id = lkp_prty_asset_id.source_record_id
             left join
                        (
                                 SELECT   prty_asset_cost.prty_asset_cost_strt_dttm AS prty_asset_cost_strt_dttm,
                                          prty_asset_cost.prty_asset_cost_end_dttm  AS prty_asset_cost_end_dttm,
                                          prty_asset_cost.prty_asset_cost_amt       AS prty_asset_cost_amt,
                                          prty_asset_cost.trans_strt_dttm           AS trans_strt_dttm,
                                          prty_asset_cost.edw_strt_dttm             AS edw_strt_dttm,
                                          prty_asset_cost.edw_end_dttm              AS edw_end_dttm,
                                          prty_asset_cost.prty_asset_id             AS prty_asset_id,
                                          prty_asset_cost.cost_type_cd              AS cost_type_cd
                                 FROM     db_t_prod_core.prty_asset_cost qualify row_number() over(PARTITION BY prty_asset_cost.prty_asset_id,prty_asset_cost.cost_type_cd ORDER BY prty_asset_cost.edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.prty_asset_id = lkp_prty_asset_id.prty_asset_id
             AND        lkp.cost_type_cd = exp_pass_to_tgt.out_cost_type 
			 qualify row_number() over(PARTITION BY exp_pass_to_tgt.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.cost_type_cd ASC,lkp.prty_asset_cost_strt_dttm ASC,lkp.prty_asset_cost_end_dttm ASC,lkp.prty_asset_cost_amt ASC,lkp.trans_strt_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
			 = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_prty_asset_cost.prty_asset_id       AS cst_prty_asset_id,
                        lkp_prty_asset_cost.cost_type_cd        AS cst_cost_type,
                        lkp_prty_asset_cost.prty_asset_cost_amt AS cst_cost_amt,
                        lkp_prty_asset_id.prty_asset_id         AS pa_prty_asset_id,
                        exp_pass_to_tgt.cost_amount             AS pa_cost_amt,
                        CASE
                                   WHEN exp_pass_to_tgt.o_updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                   ELSE exp_pass_to_tgt.o_updatetime
                        END AS out_strt_dttm,
                        CASE
                                   WHEN exp_pass_to_tgt.o_updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                   ELSE exp_pass_to_tgt.o_updatetime
                        END                                                                    AS var_strt_dttm1,
                        exp_pass_to_tgt.out_cost_type                                          AS out_cost_type,
                        lkp_prty_asset_cost.edw_strt_dttm                                      AS lkp_edw_strt_dttm,
                        lkp_prty_asset_cost.prty_asset_cost_strt_dttm                          AS lkp_prty_asset_cost_strt_dttm,
                        exp_pass_to_tgt.end_dt                                                 AS prty_asset_end_strt_dttm1,
                        current_timestamp                                                      AS edw_strt_dttm1,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        dateadd (second, -1,  current_timestamp  )                         AS edw_end_dttm_exp,
                        md5 ( ltrim ( rtrim ( lkp_prty_asset_cost.prty_asset_cost_amt ) )
                                   || ltrim ( rtrim ( lkp_prty_asset_cost.prty_asset_cost_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_prty_asset_cost.prty_asset_cost_end_dttm ) ) ) AS chksum_lkp,
                        md5 ( ltrim ( rtrim ( exp_pass_to_tgt.cost_amount ) )
                                   || ltrim ( rtrim ( var_strt_dttm1 ) )
                                   || ltrim ( rtrim ( exp_pass_to_tgt.end_dt ) ) ) AS chksum_inp,
                        CASE
                                   WHEN chksum_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN chksum_inp != chksum_lkp THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                              AS o_flag,
                        exp_pass_to_tgt.retired          AS retired,
                        lkp_prty_asset_cost.edw_end_dttm AS lkp_edw_end_dttm,
                        exp_pass_to_tgt.rnk              AS rnk,
                        CASE
                                   WHEN exp_pass_to_tgt.o_updatetime > lkp_prty_asset_cost.trans_strt_dttm THEN ''Y''
                                   ELSE ''N''
                        END AS o_lkp_flag,
                        exp_pass_to_tgt.source_record_id
             FROM       exp_pass_to_tgt
             inner join lkp_prty_asset_id
             ON         exp_pass_to_tgt.source_record_id = lkp_prty_asset_id.source_record_id
             inner join lkp_prty_asset_cost
             ON         lkp_prty_asset_id.source_record_id = lkp_prty_asset_cost.source_record_id );
  -- Component rtr_prty_asset_cost_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_prty_asset_cost_insert as
  SELECT exp_data_transformation.cst_prty_asset_id             AS cst_prty_asset_id,
         exp_data_transformation.cst_cost_type                 AS cst_cost_type,
         exp_data_transformation.cst_cost_amt                  AS cst_cost_amt,
         exp_data_transformation.pa_prty_asset_id              AS pa_prty_asset_id,
         exp_data_transformation.out_cost_type                 AS pa_cost_type,
         exp_data_transformation.pa_cost_amt                   AS pa_cost_amt,
         exp_data_transformation.out_strt_dttm                 AS in_strt_dttm,
         exp_data_transformation.o_flag                        AS o_flag,
         exp_data_transformation.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
         exp_data_transformation.edw_strt_dttm1                AS edw_strt_dttm1,
         exp_data_transformation.edw_end_dttm                  AS edw_end_dttm,
         exp_data_transformation.edw_end_dttm_exp              AS edw_end_dttm_exp,
         exp_data_transformation.lkp_prty_asset_cost_strt_dttm AS lkp_prty_asset_cost_strt_dttm,
         exp_data_transformation.prty_asset_end_strt_dttm1     AS prty_asset_end_strt_dttm1,
         exp_data_transformation.retired                       AS retired,
         exp_data_transformation.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
         exp_data_transformation.o_lkp_flag                    AS o_lkp_flag,
         exp_data_transformation.rnk                           AS rnk,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  (
                exp_data_transformation.o_flag = ''I''
         AND    exp_data_transformation.pa_prty_asset_id IS NOT NULL
         AND    exp_data_transformation.pa_cost_amt IS NOT NULL
         AND    exp_data_transformation.pa_cost_amt <> 0 ) /*-- filtering records
  HAVING NULL
  OR     0 salvage_amt
  FROM   being inserted*/
  OR     (
                exp_data_transformation.o_flag = ''U''
         AND    exp_data_transformation.cst_prty_asset_id IS NOT NULL
         AND    exp_data_transformation.o_lkp_flag = ''Y'' ) -- ONLY updating WITH NEW source records that have updatetime > trans_strt_dttm IN target
  OR     (
                exp_data_transformation.retired = 0
         AND    exp_data_transformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- ( exp_data_transformation.o_flag = ''I'' AND    exp_data_transformation.pa_prty_asset_id IS NOT NULL )
  OR     (
                exp_data_transformation.o_flag = ''U''
         AND    exp_data_transformation.pa_prty_asset_id IS NOT NULL )
  OR     (
                exp_data_transformation.retired = 0
         AND    exp_data_transformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- exp_data_transformation.o_flag = ''I'' AND    exp_data_transformation.pa_prty_asset_id IS NOT NULL                                                                        --
/*          CASE
                WHEN exp_data_transformation.cst_prty_asset_id IS NULL THEN TRUE
                ELSE FALSE
         END 
  AND        
         CASE
                WHEN exp_data_transformation.pa_prty_asset_id IS NOT NULL THEN TRUE
                ELSE FALSE
         END*/
;
  
  -- Component rtr_prty_asset_cost_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_prty_asset_cost_retire as
  SELECT exp_data_transformation.cst_prty_asset_id             AS cst_prty_asset_id,
         exp_data_transformation.cst_cost_type                 AS cst_cost_type,
         exp_data_transformation.cst_cost_amt                  AS cst_cost_amt,
         exp_data_transformation.pa_prty_asset_id              AS pa_prty_asset_id,
         exp_data_transformation.out_cost_type                 AS pa_cost_type,
         exp_data_transformation.pa_cost_amt                   AS pa_cost_amt,
         exp_data_transformation.out_strt_dttm                 AS in_strt_dttm,
         exp_data_transformation.o_flag                        AS o_flag,
         exp_data_transformation.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
         exp_data_transformation.edw_strt_dttm1                AS edw_strt_dttm1,
         exp_data_transformation.edw_end_dttm                  AS edw_end_dttm,
         exp_data_transformation.edw_end_dttm_exp              AS edw_end_dttm_exp,
         exp_data_transformation.lkp_prty_asset_cost_strt_dttm AS lkp_prty_asset_cost_strt_dttm,
         exp_data_transformation.prty_asset_end_strt_dttm1     AS prty_asset_end_strt_dttm1,
         exp_data_transformation.retired                       AS retired,
         exp_data_transformation.lkp_edw_end_dttm              AS lkp_edw_end_dttm,
         exp_data_transformation.o_lkp_flag                    AS o_lkp_flag,
         exp_data_transformation.rnk                           AS rnk,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.o_flag = ''R''
  AND    exp_data_transformation.cst_prty_asset_id IS NOT NULL
  AND    exp_data_transformation.retired != 0
  AND    exp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_prty_asset_cost_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_asset_cost_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_prty_asset_cost_insert.cst_prty_asset_id         AS cst_prty_asset_id1,
                rtr_prty_asset_cost_insert.cst_cost_type             AS cst_cost_type1,
                rtr_prty_asset_cost_insert.cst_cost_amt              AS cst_cost_amt1,
                rtr_prty_asset_cost_insert.pa_prty_asset_id          AS pa_prty_asset_id1,
                rtr_prty_asset_cost_insert.pa_cost_type              AS pa_cost_type1,
                rtr_prty_asset_cost_insert.pa_cost_amt               AS pa_cost_amt1,
                rtr_prty_asset_cost_insert.in_strt_dttm              AS in_strt_dttm1,
                rtr_prty_asset_cost_insert.edw_strt_dttm1            AS edw_strt_dttm11,
                rtr_prty_asset_cost_insert.edw_end_dttm              AS edw_end_dttm1,
                rtr_prty_asset_cost_insert.prty_asset_end_strt_dttm1 AS prty_asset_end_strt_dttm11,
                rtr_prty_asset_cost_insert.retired                   AS retired1,
                rtr_prty_asset_cost_insert.rnk                       AS rnk1,
                0                                                    AS update_strategy_action,
                rtr_prty_asset_cost_insert.source_record_id
         FROM   rtr_prty_asset_cost_insert );
  -- Component upd_prty_asset_cost_Retire_Reject, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_asset_cost_retire_reject AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_prty_asset_cost_retire.cst_prty_asset_id             AS cst_prty_asset_id3,
                rtr_prty_asset_cost_retire.cst_cost_type                 AS cst_cost_type3,
                rtr_prty_asset_cost_retire.cst_cost_amt                  AS cst_cost_amt3,
                rtr_prty_asset_cost_retire.pa_prty_asset_id              AS pa_prty_asset_id3,
                rtr_prty_asset_cost_retire.pa_cost_type                  AS pa_cost_type3,
                rtr_prty_asset_cost_retire.pa_cost_amt                   AS pa_cost_amt3,
                rtr_prty_asset_cost_retire.in_strt_dttm                  AS in_strt_dttm3,
                rtr_prty_asset_cost_retire.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm3,
                rtr_prty_asset_cost_retire.edw_end_dttm_exp              AS edw_end_dttm_exp3,
                rtr_prty_asset_cost_retire.lkp_prty_asset_cost_strt_dttm AS lkp_prty_asset_cost_strt_dttm3,
                1                                                        AS update_strategy_action,
				rtr_prty_asset_cost_retire.source_record_id
         FROM   rtr_prty_asset_cost_retire );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT $prcs_id                                  AS prcs_id,
                ''UNK''                                     AS tm_prod_cd,
                upd_prty_asset_cost_ins.pa_prty_asset_id1 AS pa_prty_asset_id1,
                upd_prty_asset_cost_ins.pa_cost_type1     AS pa_cost_type1,
                CASE
                       WHEN upd_prty_asset_cost_ins.pa_cost_amt1 IS NULL THEN 0
                       ELSE upd_prty_asset_cost_ins.pa_cost_amt1
                END                                                                                     AS out_pa_cost_amt,
                upd_prty_asset_cost_ins.in_strt_dttm1                                                   AS in_strt_dttm1,
                upd_prty_asset_cost_ins.prty_asset_end_strt_dttm11                                      AS prty_asset_cost_end_dttm1,
                dateadd (second, ( 2 * ( upd_prty_asset_cost_ins.rnk1 - 1 ) ), current_timestamp   ) AS edw_strt_dttm11,
                CASE
                       WHEN upd_prty_asset_cost_ins.retired1 != 0 THEN current_timestamp
                       ELSE upd_prty_asset_cost_ins.edw_end_dttm1
                END AS o_edw_end_dttm,
                CASE
                       WHEN upd_prty_asset_cost_ins.retired1 != 0 THEN current_timestamp
                       ELSE to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                END AS o_trans_end_dttm,
                upd_prty_asset_cost_ins.source_record_id
         FROM   upd_prty_asset_cost_ins );
  -- Component exp_pass_to_tgt_Retire_Reject, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_retire_reject AS
  (
         SELECT upd_prty_asset_cost_retire_reject.cst_prty_asset_id3             AS cst_prty_asset_id3,
                upd_prty_asset_cost_retire_reject.pa_cost_type3                  AS pa_cost_type3,
                upd_prty_asset_cost_retire_reject.in_strt_dttm3                  AS in_strt_dttm3,
                upd_prty_asset_cost_retire_reject.lkp_edw_strt_dttm3             AS lkp_edw_strt_dttm3,
                current_timestamp                                                AS edw_end_dttm_exp3,
                upd_prty_asset_cost_retire_reject.lkp_prty_asset_cost_strt_dttm3 AS lkp_prty_asset_cost_strt_dttm3,
                upd_prty_asset_cost_retire_reject.source_record_id
         FROM   upd_prty_asset_cost_retire_reject );
  -- Component tgt_PRTY_ASSET_COST_insert, Type TARGET
  INSERT INTO db_t_prod_core.prty_asset_cost
              (
                          prty_asset_id,
                          cost_type_cd,
                          prty_asset_cost_strt_dttm,
                          prty_asset_cost_end_dttm,
                          tm_prd_cd,
                          prty_asset_cost_amt,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.pa_prty_asset_id1         AS prty_asset_id,
         exp_pass_to_tgt_ins.pa_cost_type1             AS cost_type_cd,
         exp_pass_to_tgt_ins.in_strt_dttm1             AS prty_asset_cost_strt_dttm,
         exp_pass_to_tgt_ins.prty_asset_cost_end_dttm1 AS prty_asset_cost_end_dttm,
         exp_pass_to_tgt_ins.tm_prod_cd                AS tm_prd_cd,
         exp_pass_to_tgt_ins.out_pa_cost_amt           AS prty_asset_cost_amt,
         exp_pass_to_tgt_ins.prcs_id                   AS prcs_id,
         exp_pass_to_tgt_ins.edw_strt_dttm11           AS edw_strt_dttm,
         exp_pass_to_tgt_ins.o_edw_end_dttm            AS edw_end_dttm,
         exp_pass_to_tgt_ins.in_strt_dttm1             AS trans_strt_dttm,
         exp_pass_to_tgt_ins.o_trans_end_dttm          AS trans_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component tgt_PRTY_ASSET_COST_insert, Type Post SQL
  UPDATE db_t_prod_core.prty_asset_cost
    SET    trans_end_dttm= a.lead1,
         edw_end_dttm = a.lead
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         cost_type_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id,cost_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead,
                                         max(trans_strt_dttm) over (PARTITION BY prty_asset_id,cost_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.prty_asset_cost ) a

  WHERE  prty_asset_cost.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_asset_cost.prty_asset_id=a.prty_asset_id
  AND    prty_asset_cost.cost_type_cd=a.cost_type_cd
  AND    prty_asset_cost.trans_strt_dttm <>prty_asset_cost.trans_end_dttm
  AND    lead IS NOT NULL;
  
  -- Component PRTY_ASSET_COST_Retire_Reject, Type TARGET
  merge
  INTO         db_t_prod_core.prty_asset_cost
  USING        exp_pass_to_tgt_retire_reject
  ON (
                            prty_asset_cost.prty_asset_id = exp_pass_to_tgt_retire_reject.cst_prty_asset_id3
               AND          prty_asset_cost.cost_type_cd = exp_pass_to_tgt_retire_reject.pa_cost_type3
               AND          prty_asset_cost.prty_asset_cost_strt_dttm = exp_pass_to_tgt_retire_reject.lkp_prty_asset_cost_strt_dttm3
               AND          prty_asset_cost.edw_strt_dttm = exp_pass_to_tgt_retire_reject.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    prty_asset_id = exp_pass_to_tgt_retire_reject.cst_prty_asset_id3,
         cost_type_cd = exp_pass_to_tgt_retire_reject.pa_cost_type3,
         prty_asset_cost_strt_dttm = exp_pass_to_tgt_retire_reject.lkp_prty_asset_cost_strt_dttm3,
         edw_strt_dttm = exp_pass_to_tgt_retire_reject.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_retire_reject.edw_end_dttm_exp3,
         trans_end_dttm = exp_pass_to_tgt_retire_reject.in_strt_dttm3;

END;
';