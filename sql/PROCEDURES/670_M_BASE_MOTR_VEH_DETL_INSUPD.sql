-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MOTR_VEH_DETL_INSUPD("WORKLET_NAME" VARCHAR)
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
                                                         ''contentlineitemschedule.typecode'')
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
  -- Component sq_cc_vehicle, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_vehicle AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS fixedid,
                $2  AS sbtype_cd,
                $3  AS class_cd,
                $4  AS licenseplate,
                $5  AS annualmileage,
                $6  AS commutingmiles,
                $7  AS vehiclecol,
                $8  AS src_strt_dt,
                $9  AS src_end_dt,
                $10 AS sys_src_cd,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   id,
                                                    type_code,
                                                    classification_code,
                                                    licenseplate,
                                                    annualmileage,
                                                    commutingmiles,
                                                    vehiclecolor,
                                                    src_strt_dt,
                                                    src_end_dt,
                                                    src_cd 
                                           FROM     (
                                                                    SELECT DISTINCT cast (fixedid_stg AS VARCHAR (64))                                                     AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''                                                                   AS type_code ,
                                                                                    ''PRTY_ASSET_CLASFCN3''                                                                  AS classification_code,
                                                                                    pc_personalvehicle.licenseplate_stg                                                    AS licenseplate,
                                                                                    pc_personalvehicle.annualmileage_stg                                                   AS annualmileage,
                                                                                    pc_personalvehicle.commutingmiles_stg                                                  AS commutingmiles,
                                                                                    pc_personalvehicle.color_stg                                                           AS vehiclecolor,
                                                                                    coalesce (pc_personalvehicle.effectivedate_stg, pc_policyperiod.editeffectivedate_stg) AS src_strt_dt,
                                                                                    /* pc_personalvehicle.createtime AS SRC_STRT_DT, */
                                                                                    /* TO_DATE(''19000101'',''YYYYMMDD'') AS SRC_END_DT, */
                                                                                    pc_policyperiod.periodend_stg AS src_end_dt,
                                                                                    ''SRC_SYS4''                    AS src_cd
                                                                    FROM            db_t_prod_stag.pc_personalvehicle
                                                                    left outer join db_t_prod_stag.pc_policyperiod
                                                                    ON              pc_personalvehicle.branchid_stg=pc_policyperiod.id_stg
                                                                    WHERE           fixedid_stg IS NOT NULL
                                                                    AND             pc_personalvehicle.updatetime_stg> (:START_DTTM)
                                                                    AND             pc_personalvehicle.updatetime_stg <= (:END_DTTM)
                                                                    AND             (
                                                                                                    pc_personalvehicle.expirationdate_stg IS NULL
                                                                                    OR              pc_personalvehicle.expirationdate_stg >:START_DTTM)
                                                                    /* EIM-15097 Added Expiration_date filter */
                                                                    UNION
                                                                    SELECT DISTINCT
                                                                                    /* cast(coalesce(cc_vehicle.VIN,cc_vehicle.LicensePlate,cc_vehicle.PublicID) as varchar(100)) as id  */
                                                                                    CASE
                                                                                                                    /* when cc_vehicle.PolicySystemId is not null then SUBSTRING(cc_vehicle.policysystemid,charindex('':'',cc_vehicle.policysystemid)+1,LEN(cc_vehicle.policysystemid)) */
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
                                                                                                                    AND             cc_vehicle.licenseplate_stg IS NULL) THEN cc_vehicle.publicid_stg
                                                                                    END                                                AS id ,
                                                                                    ''PRTY_ASSET_SBTYPE4''                               AS type_code ,
                                                                                    ''PRTY_ASSET_CLASFCN3''                              AS classification_code,
                                                                                    cc_vehicle.licenseplate_stg                        AS licenseplate,
                                                                                    NULL                                               AS annualmileage,
                                                                                    NULL                                               AS commutingmiles,
                                                                                    cc_vehicle.color_stg                               AS vehiclecolor,
                                                                                    cc_vehicle.createtime_stg                          AS src_strt_dt,
                                                                                    cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt,
                                                                                    ''SRC_SYS6''                                         AS src_cd
                                                                    FROM            db_t_prod_stag.cc_vehicle
                                                                    left outer join db_t_prod_stag.cc_incident
                                                                    ON              cc_vehicle.id_stg =cc_incident.vehicleid_stg
                                                                    WHERE           policysystemid_stg IS NULL
                                                                    AND             cc_incident.subtype_stg IS NOT NULL
                                                                    AND             cc_vehicle.updatetime_stg > (:START_DTTM)
                                                                    AND             cc_vehicle.updatetime_stg <= (:END_DTTM) 
													qualify row_number() over(PARTITION BY id,type_code,classification_code ORDER BY licenseplate DESC,src_strt_dt DESC,src_end_dt DESC,annualmileage DESC,commutingmiles DESC,vehiclecolor DESC,src_cd DESC ) =1) x 
												) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT
                      CASE
                                WHEN sq_cc_vehicle.licenseplate IS NULL THEN ''UNK''
                                ELSE upper ( sq_cc_vehicle.licenseplate )
                      END AS o_licenseplate,
                      CASE
                                WHEN sq_cc_vehicle.annualmileage IS NULL THEN 0
                                ELSE sq_cc_vehicle.annualmileage
                      END AS o_annualmileage,
                      CASE
                                WHEN sq_cc_vehicle.commutingmiles IS NULL THEN 0
                                ELSE sq_cc_vehicle.commutingmiles
                      END AS o_commutingmiles,
                      CASE
                                WHEN sq_cc_vehicle.vehiclecol IS NULL THEN ''UNK''
                                ELSE upper ( sq_cc_vehicle.vehiclecol )
                      END                               AS o_vehiclecolor,
                      to_char ( sq_cc_vehicle.fixedid ) AS var_fixedid,
                      var_fixedid                       AS out_fixedid,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      END AS out_prty_asset_sbtype_cd,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                      END      AS out_class_cd,
                      :prcs_id AS out_process_id,
                      lkp_5.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                                             AS out_src_cd,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      sq_cc_vehicle.src_strt_dt                                              AS in_src_strt_dt,
                      sq_cc_vehicle.src_end_dt                                               AS in_src_end_dt,
                      sq_cc_vehicle.source_record_id,
                      row_number() over (PARTITION BY sq_cc_vehicle.source_record_id ORDER BY sq_cc_vehicle.source_record_id) AS rnk
            FROM      sq_cc_vehicle
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_1
            ON        lkp_1.src_idntftn_val = sq_cc_vehicle.sbtype_cd
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_2
            ON        lkp_2.src_idntftn_val = sq_cc_vehicle.sbtype_cd
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_3
            ON        lkp_3.src_idntftn_val = sq_cc_vehicle.class_cd
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_4
            ON        lkp_4.src_idntftn_val = sq_cc_vehicle.class_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_5
            ON        lkp_5.src_idntftn_val = sq_cc_vehicle.sys_src_cd qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_id AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_all_source.source_record_id,
                      row_number() over(PARTITION BY exp_all_source.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_all_source
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
            ON        lkp.asset_host_id_val = exp_all_source.out_fixedid
            AND       lkp.prty_asset_sbtype_cd = exp_all_source.out_prty_asset_sbtype_cd
            AND       lkp.prty_asset_clasfcn_cd = exp_all_source.out_class_cd qualify rnk = 1 );
  -- Component LKP_MOTR_VEH, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_motr_veh AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.motr_veh_dtl_strt_dttm,
                      lkp.motr_veh_dtl_end_dttm,
                      lkp.color_desc,
                      lkp.lic_plate_num,
                      lkp.annual_dstc_meas,
                      lkp.daily_comut_dstc_meas,
                      lkp_prty_asset_id.source_record_id,
                      row_number() over(PARTITION BY lkp_prty_asset_id.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.motr_veh_dtl_strt_dttm ASC,lkp.motr_veh_dtl_end_dttm ASC,lkp.color_desc ASC,lkp.lic_plate_num ASC,lkp.annual_dstc_meas ASC,lkp.daily_comut_dstc_meas ASC) rnk
            FROM      lkp_prty_asset_id
            left join
                      (
                               SELECT   motr_veh_dtl.motr_veh_dtl_strt_dttm AS motr_veh_dtl_strt_dttm,
                                        motr_veh_dtl.motr_veh_dtl_end_dttm  AS motr_veh_dtl_end_dttm,
                                        motr_veh_dtl.color_desc             AS color_desc,
                                        motr_veh_dtl.lic_plate_num          AS lic_plate_num,
                                        motr_veh_dtl.annual_dstc_meas       AS annual_dstc_meas,
                                        motr_veh_dtl.daily_comut_dstc_meas  AS daily_comut_dstc_meas,
                                        motr_veh_dtl.prty_asset_id          AS prty_asset_id
                               FROM     db_t_prod_core.motr_veh_dtl qualify row_number() over(PARTITION BY prty_asset_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.prty_asset_id = lkp_prty_asset_id.prty_asset_id qualify rnk = 1 );
  -- Component exp_compare_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_compare_data AS
  (
             SELECT     md5 ( ltrim ( rtrim ( to_char ( lkp_motr_veh.motr_veh_dtl_strt_dttm ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_motr_veh.motr_veh_dtl_end_dttm ) ) )
                                   || ltrim ( rtrim ( lkp_motr_veh.color_desc ) )
                                   || ltrim ( rtrim ( lkp_motr_veh.lic_plate_num ) )
                                   || ltrim ( rtrim ( to_char ( lkp_motr_veh.annual_dstc_meas ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_motr_veh.daily_comut_dstc_meas ) ) ) ) AS v_lkp_checksum,
                        lkp_prty_asset_id.prty_asset_id                                                    AS in_prty_asset_id,
                        exp_all_source.in_src_strt_dt                                                      AS in_src_strt_dttm,
                        exp_all_source.in_src_end_dt                                                       AS in_src_end_dttm,
                        exp_all_source.o_vehiclecolor                                                      AS in_vehiclecolor,
                        exp_all_source.o_licenseplate                                                      AS in_licenseplate,
                        exp_all_source.o_annualmileage                                                     AS in_annualmileage,
                        exp_all_source.o_commutingmiles                                                    AS in_commutingmiles,
                        exp_all_source.out_process_id                                                      AS in_process_id,
                        exp_all_source.edw_strt_dttm                                                       AS in_edw_strt_dttm,
                        exp_all_source.edw_end_dttm                                                        AS in_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( exp_all_source.in_src_strt_dt ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_all_source.in_src_end_dt ) ) )
                                   || ltrim ( rtrim ( exp_all_source.o_vehiclecolor ) )
                                   || ltrim ( rtrim ( exp_all_source.o_licenseplate ) )
                                   || ltrim ( rtrim ( to_char ( exp_all_source.o_annualmileage ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_all_source.o_commutingmiles ) ) ) ) AS v_in_checksum,
                        CASE
                                   WHEN v_lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_lkp_checksum != v_in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END AS calc_ins_upd,
                        exp_all_source.source_record_id
             FROM       exp_all_source
             inner join lkp_prty_asset_id
             ON         exp_all_source.source_record_id = lkp_prty_asset_id.source_record_id
             inner join lkp_motr_veh
             ON         lkp_prty_asset_id.source_record_id = lkp_motr_veh.source_record_id );
  -- Component rtr_mtr_veh_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_mtr_veh_insert as
  SELECT exp_compare_data.in_prty_asset_id  AS in_prty_asset_id,
         exp_compare_data.in_src_strt_dttm  AS in_src_strt_dttm,
         exp_compare_data.in_src_end_dttm   AS in_src_end_dttm,
         exp_compare_data.in_vehiclecolor   AS in_vehiclecolor,
         exp_compare_data.in_licenseplate   AS in_licenseplate,
         exp_compare_data.in_annualmileage  AS in_annualmileage,
         exp_compare_data.in_commutingmiles AS in_commutingmiles,
         exp_compare_data.in_process_id     AS in_process_id,
         exp_compare_data.in_edw_strt_dttm  AS in_edw_strt_dttm,
         exp_compare_data.in_edw_end_dttm   AS in_edw_end_dttm,
         exp_compare_data.calc_ins_upd      AS calc_ins_upd,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  (
                exp_compare_data.calc_ins_upd = ''I''
         AND    exp_compare_data.in_prty_asset_id IS NOT NULL
         AND    exp_compare_data.in_prty_asset_id <> 9999 )
  OR     (
                exp_compare_data.calc_ins_upd = ''U'' );
  
  -- Component upd_motr_veh_dtl_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_motr_veh_dtl_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_mtr_veh_insert.in_prty_asset_id  AS in_prty_asset_id1,
                rtr_mtr_veh_insert.in_src_strt_dttm  AS in_src_strt_dttm1,
                rtr_mtr_veh_insert.in_src_end_dttm   AS in_src_end_dttm1,
                rtr_mtr_veh_insert.in_vehiclecolor   AS in_vehiclecolor1,
                rtr_mtr_veh_insert.in_licenseplate   AS in_licenseplate1,
                rtr_mtr_veh_insert.in_annualmileage  AS in_annualmileage1,
                rtr_mtr_veh_insert.in_commutingmiles AS in_commutingmiles1,
                rtr_mtr_veh_insert.in_process_id     AS in_process_id1,
                rtr_mtr_veh_insert.in_edw_strt_dttm  AS in_edw_strt_dttm1,
                rtr_mtr_veh_insert.in_edw_end_dttm   AS in_edw_end_dttm1,
                0                                    AS update_strategy_action,
				rtr_mtr_veh_insert.source_record_id
         FROM   rtr_mtr_veh_insert );
  -- Component exp_pass_to_target_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert AS
  (
         SELECT upd_motr_veh_dtl_insert.in_prty_asset_id1  AS in_prty_asset_id1,
                upd_motr_veh_dtl_insert.in_src_strt_dttm1  AS in_src_strt_dttm1,
                upd_motr_veh_dtl_insert.in_src_end_dttm1   AS in_src_end_dttm1,
                upd_motr_veh_dtl_insert.in_vehiclecolor1   AS in_vehiclecolor1,
                upd_motr_veh_dtl_insert.in_licenseplate1   AS in_licenseplate1,
                upd_motr_veh_dtl_insert.in_annualmileage1  AS in_annualmileage1,
                upd_motr_veh_dtl_insert.in_commutingmiles1 AS in_commutingmiles1,
                upd_motr_veh_dtl_insert.in_process_id1     AS in_process_id1,
                upd_motr_veh_dtl_insert.in_edw_strt_dttm1  AS in_edw_strt_dttm1,
                upd_motr_veh_dtl_insert.in_edw_end_dttm1   AS in_edw_end_dttm1,
                upd_motr_veh_dtl_insert.source_record_id
         FROM   upd_motr_veh_dtl_insert );
  -- Component tgt_motr_veh_dtl_insert, Type TARGET
  INSERT INTO db_t_prod_core.motr_veh_dtl
              (
                          prty_asset_id,
                          motr_veh_dtl_strt_dttm,
                          motr_veh_dtl_end_dttm,
                          color_desc,
                          lic_plate_num,
                          annual_dstc_meas,
                          daily_comut_dstc_meas,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_insert.in_prty_asset_id1  AS prty_asset_id,
         exp_pass_to_target_insert.in_src_strt_dttm1  AS motr_veh_dtl_strt_dttm,
         exp_pass_to_target_insert.in_src_end_dttm1   AS motr_veh_dtl_end_dttm,
         exp_pass_to_target_insert.in_vehiclecolor1   AS color_desc,
         exp_pass_to_target_insert.in_licenseplate1   AS lic_plate_num,
         exp_pass_to_target_insert.in_annualmileage1  AS annual_dstc_meas,
         exp_pass_to_target_insert.in_commutingmiles1 AS daily_comut_dstc_meas,
         exp_pass_to_target_insert.in_process_id1     AS prcs_id,
         exp_pass_to_target_insert.in_edw_strt_dttm1  AS edw_strt_dttm,
         exp_pass_to_target_insert.in_edw_end_dttm1   AS edw_end_dttm
  FROM   exp_pass_to_target_insert;
  
  -- Component tgt_motr_veh_dtl_insert, Type Post SQL
  UPDATE db_t_prod_core.motr_veh_dtl
    SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.motr_veh_dtl ) a

  WHERE  motr_veh_dtl.edw_strt_dttm = a.edw_strt_dttm
  AND    motr_veh_dtl.prty_asset_id=a.prty_asset_id
  AND    cast(motr_veh_dtl.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';