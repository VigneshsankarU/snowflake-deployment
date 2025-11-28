-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CITY_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       prcs_id STRING;

BEGIN
   run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
   start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
   end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
 --  prcs_id := (SELECT ifnull(param_value,''test'') FROM control_params WHERE run_id = :run_id AND param_name = ''prcs_id'' LIMIT 1);
 prcs_id :=1;

   --SS := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''SS'' LIMIT 1);
   --59 := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''59'' LIMIT 1);
   --MI := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''MI'' LIMIT 1);


-- Component LKP_CITY, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_city AS
  (
         SELECT city.city_id                AS city_id,
                city.edw_strt_dttm          AS edw_strt_dttm,
                city.edw_end_dttm           AS edw_end_dttm,
                city.terr_id                AS terr_id,
                city.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
         FROM   db_t_prod_core.city
         WHERE  cast(city.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) );
  -- Component LKP_CTRY, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_ctry AS
  (
         SELECT ctry.ctry_id                AS ctry_id,
                ctry.geogrcl_area_name      AS geogrcl_area_name,
                ctry.geogrcl_area_desc      AS geogrcl_area_desc,
                ctry.edw_strt_dttm          AS edw_strt_dttm,
                ctry.edw_end_dttm           AS edw_end_dttm,
                ctry.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
         FROM   db_t_prod_core.ctry
         WHERE  cast(ctry.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) );
  -- Component LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_geogrcl_area_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''GEOGRCL_AREA_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_loctr_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_terr AS
  (
         SELECT terr.terr_id                AS terr_id,
                terr.geogrcl_area_name      AS geogrcl_area_name,
                terr.geogrcl_area_desc      AS geogrcl_area_desc,
                terr.geogrcl_area_strt_dttm AS geogrcl_area_strt_dttm,
                terr.geogrcl_area_end_dttm  AS geogrcl_area_end_dttm,
                terr.loctr_sbtype_cd        AS loctr_sbtype_cd,
                terr.geogrcl_area_sbtype_cd AS geogrcl_area_sbtype_cd,
                terr.edw_strt_dttm          AS edw_strt_dttm,
                terr.edw_end_dttm           AS edw_end_dttm,
                terr.ctry_id                AS ctry_id,
                terr.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
         FROM   db_t_prod_core.terr
         WHERE  cast(terr.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) );
  -- Component sq_bc_address, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_address AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS state_typecode,
                $2 AS ctry_typecode,
                $3 AS city,
                $4 AS strt_dt,
                $5 AS end_dt,
                $6 AS retired,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   state_typecode,
                                                    CASE
                                                             WHEN (
                                                                               ctry_typecode IS NULL) THEN ''US''
                                                             ELSE ctry_typecode
                                                    END ctry_typecd ,
                                                    city,
                                                    strt_dt,
                                                    end_dt,
                                                    retired 
													--qualify row_number() over(PARTITION BY state_typecode, ctry_typecd, city ORDER BY strt_dt DESC ,end_dt DESC)=1
                                           FROM     (
                                                              /**************bc_address**********************/
                                                              SELECT    cast(bctl_state.typecode_stg AS   VARCHAR(255))AS state_typecode,
                                                                        cast(bctl_country.typecode_stg AS VARCHAR(255))AS ctry_typecode,
                                                                        cast(bc_address.city_stg AS       VARCHAR(255))AS city,
                                                                        bc_address.createtime_stg                      AS strt_dt,
                                                                        to_date(''12/31/9999'' ,''MM/DD/YYYY'') AS end_dt,
                                                                        bc_address.retired_stg                         AS retired
                                                              FROM      (
                                                                               SELECT bc_address.city_stg,
                                                                                      bc_address.createtime_stg,
                                                                                      bc_address.country_stg,
                                                                                      bc_address.state_stg,
                                                                                      bc_address.retired_stg,
                                                                                      bc_address.geocodestatus_stg,
                                                                                      bc_address.standardizationtype_alfa_stg
                                                                               FROM   db_t_prod_stag.bc_address
                                                                               WHERE  bc_address.updatetime_stg>(:start_dttm)
                                                                               AND    bc_address.updatetime_stg <= (:end_dttm) )bc_address
                                                              left join db_t_prod_stag.bctl_country
                                                              ON        bctl_country.id_stg=bc_address.country_stg
                                                              join      db_t_prod_stag.bctl_state
                                                              ON        bc_address.state_stg=bctl_state.id_stg
                                                              WHERE     bc_address.city_stg IS NOT NULL
                                                              AND       bc_address.retired_stg=0
                                                              UNION
                                                              /***************cc_address****************/
                                                              SELECT    cctl_state.typecode_stg                        AS state_typecode,
                                                                        cctl_country.typecode_stg                      AS ctry_typecode,
                                                                        cc_address.city_stg                            AS city,
                                                                        cc_address.createtime_stg                      AS strt_dt,
                                                                        to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt,
                                                                        cc_address.retired_stg                         AS retired
                                                              FROM      (
                                                                               SELECT cc_address.city_stg,
                                                                                      cc_address.createtime_stg,
                                                                                      cc_address.country_stg,
                                                                                      cc_address.state_stg ,
                                                                                      cc_address.geocodestatus_stg,
                                                                                      cc_address.standardizationtype_alfa_stg,
                                                                                      cc_address.retired_stg
                                                                               FROM   db_t_prod_stag.cc_address
                                                                               WHERE  cc_address.updatetime_stg>(:start_dttm)
                                                                               AND    cc_address.updatetime_stg <= (:end_dttm) ) cc_address
                                                              left join db_t_prod_stag.cctl_country
                                                              ON        cctl_country.id_stg=cc_address.country_stg
                                                              join      db_t_prod_stag.cctl_state
                                                              ON        cc_address.state_stg=cctl_state.id_stg
                                                              WHERE     cc_address.city_stg IS NOT NULL
                                                              AND       cc_address.retired_stg=0
                                                              UNION
                                                              /*******************pc_address****************/
                                                              SELECT    pctl_state.typecode_stg                        AS state_typecode,
                                                                        pctl_country.typecode_stg                      AS ctry_typecode,
                                                                        pc_address.city_stg                            AS city,
                                                                        pc_address.createtime_stg                      AS strt_dt,
                                                                        to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt,
                                                                        pc_address.retired_stg                         AS retired
                                                              FROM      (
                                                                               SELECT pc_address.city_stg,
                                                                                      pc_address.geocodestatus_stg,
                                                                                      pc_address.standardizedtype_alfa_stg,
                                                                                      pc_address.country_stg,
                                                                                      pc_address.state_stg,
                                                                                      pc_address.retired_stg,
                                                                                      pc_address.createtime_stg
                                                                               FROM   db_t_prod_stag.pc_address
                                                                               WHERE  pc_address.updatetime_stg> (:start_dttm)
                                                                               AND    pc_address.updatetime_stg<= (:end_dttm) ) pc_address
                                                              left join db_t_prod_stag.pctl_country
                                                              ON        pctl_country.id_stg=pc_address.country_stg
                                                              join      db_t_prod_stag.pctl_state
                                                              ON        pc_address.state_stg=pctl_state.id_stg
                                                              WHERE     pc_address.city_stg IS NOT NULL
                                                              AND       pc_address.retired_stg=0
                                                              UNION
                                                              /* BOP AND CHURCH -POLICY LOCATION*/
                                                              SELECT    pctl_state.typecode_stg                        AS state_typecode,
                                                                        pctl_country.typecode_stg                      AS ctry_typecode,
                                                                        pc_policylocation.cityinternal_stg             AS city,
                                                                        pc_policylocation.createtime_stg               AS strt_dt,
                                                                        to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt ,
                                                                        0                                              AS retired
                                                              FROM      db_t_prod_stag.pc_policylocation
                                                              left join db_t_prod_stag.pctl_country
                                                              ON        pctl_country.id_stg=pc_policylocation.countryinternal_stg
                                                              join      db_t_prod_stag.pctl_state
                                                              ON        pc_policylocation.stateinternal_stg=pctl_state.id_stg
                                                              WHERE     pc_policylocation.cityinternal_stg IS NOT NULL
                                                              AND       pc_policylocation.updatetime_stg > (:start_dttm)
                                                              AND       pc_policylocation.updatetime_stg <= (:end_dttm)
                                                              UNION
                                                              /*************pc_loc_master_x*************************/
                                                              SELECT pc_loc_master_x.state_code,
                                                                     pc_loc_master_x.country_code                   AS ctry_typecode,
                                                                     city                            AS city,
                                                                     to_date(''01/01/1900'' , ''MM/DD/YYYY'') AS strt_dt,
                                                                     to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt,
                                                                     0                                              AS retired
                                                              FROM   (
                                                                            SELECT state1.country_typecode AS country_code,
                                                                                   state1.state_typecode   AS state_code,
                                                                                   city.name_stg           AS city
                                                                            FROM   (
                                                                                              SELECT     pc_zone.id_stg,
                                                                                                         pc_zone.name_stg    AS state_typecode,
                                                                                                         pctl_state.name_stg AS state_name,
                                                                                                         pctl_state.description_stg,
                                                                                                         pctl_country.typecode_stg    AS country_typecode,
                                                                                                         pctl_country.name_stg        AS cntry_name,
                                                                                                         pctl_country.description_stg AS cntry_desc
                                                                                              FROM       db_t_prod_stag.pc_zone
                                                                                              inner join db_t_prod_stag.pctl_zonetype
                                                                                              ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                              inner join db_t_prod_stag.pctl_country
                                                                                              ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                              inner join db_t_prod_stag.pctl_state
                                                                                              ON         pctl_state.typecode_stg=pc_zone.name_stg
                                                                                              WHERE      pctl_zonetype.typecode_stg=''state'') state1,
                                                                                   (
                                                                                              SELECT     pc_zone.id_stg,
                                                                                                         pc_zone.name_stg
                                                                                              FROM       db_t_prod_stag.pc_zone
                                                                                              inner join db_t_prod_stag.pctl_zonetype
                                                                                              ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                              inner join db_t_prod_stag.pctl_country
                                                                                              ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                              WHERE      pctl_zonetype.typecode_stg=''county'') county,
                                                                                   (
                                                                                              SELECT     pc_zone.id_stg,
                                                                                                         pc_zone.name_stg
                                                                                              FROM       db_t_prod_stag.pc_zone
                                                                                              inner join db_t_prod_stag.pctl_zonetype
                                                                                              ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                              inner join db_t_prod_stag.pctl_country
                                                                                              ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                              WHERE      pctl_zonetype.typecode_stg=''city'') city,
                                                                                   (
                                                                                              SELECT     pc_zone.id_stg,
                                                                                                         pc_zone.name_stg
                                                                                              FROM       db_t_prod_stag.pc_zone
                                                                                              inner join db_t_prod_stag.pctl_zonetype
                                                                                              ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                              inner join db_t_prod_stag.pctl_country
                                                                                              ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                              WHERE      pctl_zonetype.typecode_stg=''zip'') zip,
                                                                                   db_t_prod_stag.pc_zone_link zl_state_county,
                                                                                   db_t_prod_stag.pc_zone_link zl_county_city,
                                                                                   db_t_prod_stag.pc_zone_link zl_city_zip
                                                                            WHERE  state1.id_stg=zl_state_county.zone1id_stg
                                                                            AND    zl_state_county.zone2id_stg=county.id_stg
                                                                            AND    county.id_stg=zl_county_city.zone1id_stg
                                                                            AND    zl_county_city.zone2id_stg=city.id_stg
                                                                            AND    city.id_stg=zl_city_zip.zone1id_stg
                                                                            AND    zl_city_zip.zone2id_stg=zip.id_stg ) pc_loc_master_x
                                                              WHERE  city IS NOT NULL
                                                              UNION
                                                              /************PC_TAXLOCATION*********************/
                                                              SELECT pctl_jurisdiction.typecode_stg,
                                                                     ''US''                                           AS country_code,
                                                                     city_stg                                       AS city,
                                                                     to_date(''01/01/1900'' , ''MM/DD/YYYY'') AS strt_dt,
                                                                     to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt,
                                                                     pc_taxlocation.retired_stg                     AS retired
                                                              FROM   db_t_prod_stag.pc_taxlocation
                                                              join   db_t_prod_stag.pctl_jurisdiction
                                                              ON     pctl_jurisdiction.id_stg=pc_taxlocation.state_stg
                                                              WHERE  city_stg IS NOT NULL
                                                              AND    pc_taxlocation.retired_stg=0
                                                              AND    db_t_prod_stag.pc_taxlocation .updatetime_stg > (:start_dttm)
                                                              AND    pc_taxlocation.updatetime_stg <= (:end_dttm)
                                                              UNION
                                                              /****************pc_policylocation*******************/
                                                              SELECT    pctl_state.typecode_stg                        AS state_typecode,
                                                                        pctl_country.typecode_stg                      AS ctry_typecode,
                                                                        pc_policylocation.cityinternal_stg             AS city,
                                                                        pc_policylocation.createtime_stg               AS strt_dt,
                                                                        to_date(''12/31/9999'' , ''MM/DD/YYYY'') AS end_dt ,
                                                                        0                                              AS retired
                                                              FROM      db_t_prod_stag.pc_policylocation
                                                              left join db_t_prod_stag.pctl_country
                                                              ON        pctl_country.id_stg=pc_policylocation.countryinternal_stg
                                                              join      db_t_prod_stag.pctl_state
                                                              ON        pc_policylocation.stateinternal_stg=pctl_state.id_stg
                                                              WHERE     pc_policylocation.cityinternal_stg IS NOT NULL
                                                              AND       pc_policylocation.updatetime_stg > (:start_dttm)
                                                              AND       pc_policylocation.updatetime_stg <= (:end_dttm) ) x 
											qualify row_number() over(PARTITION BY state_typecode, ctry_typecd, city ORDER BY strt_dt DESC ,end_dt DESC)=1  
												) src ) );
  -- Component exp_pass_from_source_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source_bc AS
  (
         SELECT sq_bc_address.state_typecode AS state_typecode,
                sq_bc_address.ctry_typecode  AS ctry_typecode,
                sq_bc_address.city           AS city,
                sq_bc_address.strt_dt        AS strt_dt,
                sq_bc_address.end_dt         AS end_dt,
                sq_bc_address.retired        AS retired,
                sq_bc_address.source_record_id
         FROM   sq_bc_address );
  -- Component exp_data_transform_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transform_bc AS
  (
            SELECT    exp_pass_from_source_bc.city AS city,
                      lkp_1.ctry_id
                      /* replaced lookup LKP_CTRY */
                      AS v_ctry_id,
                      lkp_2.terr_id
                      /* replaced lookup LKP_TERR */
                      AS v_terr_id,
                      lkp_3.city_id
                      /* replaced lookup LKP_CITY */
                                             AS v_city_id,
                      ''LOCTR_SBTYPE3''        AS v_loctr_sbtype_val,
                      ''GEOGRCL_AREA_SBTYPE1'' AS v_geogrcl_area_sbtype_val,
                      lkp_4.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */
                      AS v_loctr_sbtype,
                      lkp_5.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */
                      AS v_geogrcl_area_sbtype,
                      CASE
                                WHEN v_city_id IS NULL THEN 1
                                ELSE 0
                      END                             AS v_upd_or_ins,
                      exp_pass_from_source_bc.strt_dt AS strt_dt,
                      exp_pass_from_source_bc.end_dt  AS end_dt,
                      v_terr_id                       AS out_terr_id,
                      v_city_id                       AS out_city_id,
                      v_loctr_sbtype                  AS out_loctr_sbtype,
                      v_geogrcl_area_sbtype           AS out_geogrcl_area_sbtype,
                      :prcs_id                        AS out_process_id,
                      exp_pass_from_source_bc.retired AS retired,
                      exp_pass_from_source_bc.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source_bc.source_record_id ORDER BY exp_pass_from_source_bc.source_record_id) AS rnk
            FROM      exp_pass_from_source_bc
            left join lkp_ctry lkp_1
            ON        lkp_1.geogrcl_area_shrt_name = exp_pass_from_source_bc.ctry_typecode
            left join lkp_terr lkp_2
            ON        lkp_2.ctry_id = v_ctry_id
            AND       lkp_2.geogrcl_area_shrt_name = exp_pass_from_source_bc.state_typecode
            left join lkp_city lkp_3
            ON        lkp_3.terr_id = v_terr_id
            AND       lkp_3.geogrcl_area_shrt_name = exp_pass_from_source_bc.city
            left join lkp_teradata_etl_ref_xlat_loctr_sbtype lkp_4
            ON        lkp_4.src_idntftn_val = v_loctr_sbtype_val
            left join lkp_teradata_etl_ref_xlat_geogrcl_area_sbtype lkp_5
            ON        lkp_5.src_idntftn_val = v_geogrcl_area_sbtype_val qualify rnk = 1 );
  -- Component LKP_CITY_CDC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_city_cdc AS
  (
            SELECT    lkp.city_id,
                      lkp.geogrcl_area_name,
                      lkp.loctr_sbtype_cd,
                      lkp.geogrcl_area_sbtype_cd,
                      lkp.edw_end_dttm,
                      exp_data_transform_bc.source_record_id,
                      row_number() over(PARTITION BY exp_data_transform_bc.source_record_id ORDER BY lkp.city_id DESC,lkp.terr_id DESC,lkp.geogrcl_area_shrt_name DESC,lkp.geogrcl_area_name DESC,lkp.loctr_sbtype_cd DESC,lkp.geogrcl_area_sbtype_cd DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_data_transform_bc
            left join
                      (
                             SELECT city.city_id                AS city_id,
                                    city.geogrcl_area_name      AS geogrcl_area_name,
                                    city.loctr_sbtype_cd        AS loctr_sbtype_cd,
                                    city.geogrcl_area_sbtype_cd AS geogrcl_area_sbtype_cd,
                                    city.edw_end_dttm           AS edw_end_dttm,
                                    city.geogrcl_area_shrt_name AS geogrcl_area_shrt_name,
                                    city.terr_id                AS terr_id
                             FROM   db_t_prod_core.city
                             WHERE  cast(city.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) ) lkp
            ON        lkp.geogrcl_area_shrt_name = exp_data_transform_bc.city
            AND       lkp.terr_id = exp_data_transform_bc.out_terr_id qualify rnk = 1 );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
             SELECT     lkp_city_cdc.city_id                          AS lkp_city_id,
                        lkp_city_cdc.geogrcl_area_name                AS lkp_geogrcl_area_name,
                        lkp_city_cdc.loctr_sbtype_cd                  AS lkp_loctr_sbtype_cd,
                        lkp_city_cdc.geogrcl_area_sbtype_cd           AS lkp_geogrcl_area_sbtype_cd,
                        lkp_city_cdc.edw_end_dttm                     AS lkp_edw_end_dttm,
                        exp_data_transform_bc.city                    AS city,
                        exp_data_transform_bc.out_terr_id             AS out_terr_id,
                        exp_data_transform_bc.out_city_id             AS out_city_id,
                        exp_data_transform_bc.out_loctr_sbtype        AS out_loctr_sbtype,
                        exp_data_transform_bc.out_geogrcl_area_sbtype AS out_geogrcl_area_sbtype,
                        exp_data_transform_bc.out_process_id          AS out_process_id,
                        md5 ( ltrim ( rtrim ( lkp_city_cdc.city_id ) )
                                   || ltrim ( rtrim ( upper ( lkp_city_cdc.geogrcl_area_name ) ) )
                                   || ltrim ( rtrim ( lkp_city_cdc.loctr_sbtype_cd ) )
                                   || ltrim ( rtrim ( lkp_city_cdc.geogrcl_area_sbtype_cd ) ) ) AS var_orig_chksm,
                        md5 ( ltrim ( rtrim ( exp_data_transform_bc.out_city_id ) )
                                   || ltrim ( rtrim ( upper ( exp_data_transform_bc.city ) ) )
                                   || ltrim ( rtrim ( exp_data_transform_bc.out_loctr_sbtype ) )
                                   || ltrim ( rtrim ( exp_data_transform_bc.out_geogrcl_area_sbtype ) ) ) AS var_calc_chksm,
                        CASE
                                   WHEN var_orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                           AS out_ins_upd,
                        exp_data_transform_bc.strt_dt AS geogrcl_area_strt_dt,
                        exp_data_transform_bc.end_dt  AS geogrcl_area_end_dt,
                        exp_data_transform_bc.retired AS retired,
                        exp_data_transform_bc.source_record_id
             FROM       exp_data_transform_bc
             inner join lkp_city_cdc
             ON         exp_data_transform_bc.source_record_id = lkp_city_cdc.source_record_id );
  -- Component rtr_update_insert_bc_INS, Type ROUTER Output Group INS
  create or replace temporary table rtr_update_insert_bc_ins as
  SELECT exp_ins_upd.city                       AS short_name,
         exp_ins_upd.city                       AS name,
         exp_ins_upd.out_terr_id                AS terr_id,
         exp_ins_upd.out_ins_upd                AS upd_or_ins,
         exp_ins_upd.out_city_id                AS city_id,
         exp_ins_upd.out_process_id             AS out_process_id,
         exp_ins_upd.out_loctr_sbtype           AS out_loctr_sbtype,
         exp_ins_upd.out_geogrcl_area_sbtype    AS out_geogrcl_area_sbtype,
         exp_ins_upd.lkp_city_id                AS lkp_city_id,
         exp_ins_upd.lkp_geogrcl_area_name      AS lkp_geogrcl_area_name,
         exp_ins_upd.lkp_loctr_sbtype_cd        AS lkp_loctr_sbtype_cd,
         exp_ins_upd.lkp_geogrcl_area_sbtype_cd AS lkp_geogrcl_area_sbtype_cd,
         exp_ins_upd.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_ins_upd.geogrcl_area_strt_dt       AS geogrcl_area_strt_dt,
         exp_ins_upd.geogrcl_area_end_dt        AS geogrcl_area_end_dt,
         exp_ins_upd.retired                    AS retired,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  (
                exp_ins_upd.out_ins_upd = ''I'' ) --
  OR     exp_ins_upd.out_ins_upd = ''U''
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_insert_bc, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert_bc AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_update_insert_bc_ins.short_name              AS short_name1,
                rtr_update_insert_bc_ins.name                    AS name1,
                rtr_update_insert_bc_ins.terr_id                 AS terr_id1,
                rtr_update_insert_bc_ins.out_process_id          AS o_process_id1,
                rtr_update_insert_bc_ins.out_loctr_sbtype        AS out_loctr_sbtype1,
                rtr_update_insert_bc_ins.out_geogrcl_area_sbtype AS out_geogrcl_area_sbtype1,
                rtr_update_insert_bc_ins.geogrcl_area_strt_dt    AS geogrcl_area_strt_dt1,
                rtr_update_insert_bc_ins.geogrcl_area_end_dt     AS geogrcl_area_end_dt1,
                rtr_update_insert_bc_ins.lkp_city_id             AS lkp_city_id1,
                rtr_update_insert_bc_ins.lkp_edw_end_dttm        AS lkp_edw_end_dttm1,
                0                                                AS update_strategy_action,
				rtr_update_insert_bc_ins.source_record_id
         FROM   rtr_update_insert_bc_ins );
  -- Component exp_pass_to_target_insert_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert_bc AS
  (
           SELECT   upd_insert_bc.short_name1   AS short_name1,
                    upd_insert_bc.name1         AS name1,
                    upd_insert_bc.terr_id1      AS terr_id1,
                    upd_insert_bc.o_process_id1 AS out_process_id1,
                    CASE
                             WHEN upd_insert_bc.lkp_city_id1 IS NULL THEN row_number() over (ORDER BY 1)
                             ELSE upd_insert_bc.lkp_city_id1
                    END                                                                    AS var_city_id,
                    var_city_id                                                            AS out_city_id,
                    upd_insert_bc.out_loctr_sbtype1                                        AS out_loctr_sbtype1,
                    upd_insert_bc.out_geogrcl_area_sbtype1                                 AS out_geogrcl_area_sbtype1,
                    current_timestamp                                                      AS out_edw_strt_dttm,
                    to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                    upd_insert_bc.geogrcl_area_strt_dt1                                    AS geogrcl_area_strt_dt1,
                    upd_insert_bc.geogrcl_area_end_dt1                                     AS geogrcl_area_end_dt1,
                    upd_insert_bc.source_record_id
           FROM     upd_insert_bc );
  -- Component tgt_city_insert_bc, Type TARGET
  INSERT INTO db_t_prod_core.city
              (
                          city_id,
                          terr_id,
                          geogrcl_area_shrt_name,
                          geogrcl_area_name,
                          geogrcl_area_strt_dttm,
                          geogrcl_area_end_dttm,
                          loctr_sbtype_cd,
                          geogrcl_area_sbtype_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_insert_bc.out_city_id              AS city_id,
         exp_pass_to_target_insert_bc.terr_id1                 AS terr_id,
         exp_pass_to_target_insert_bc.short_name1              AS geogrcl_area_shrt_name,
         exp_pass_to_target_insert_bc.name1                    AS geogrcl_area_name,
         exp_pass_to_target_insert_bc.geogrcl_area_strt_dt1    AS geogrcl_area_strt_dttm,
         exp_pass_to_target_insert_bc.geogrcl_area_end_dt1     AS geogrcl_area_end_dttm,
         exp_pass_to_target_insert_bc.out_loctr_sbtype1        AS loctr_sbtype_cd,
         exp_pass_to_target_insert_bc.out_geogrcl_area_sbtype1 AS geogrcl_area_sbtype_cd,
         exp_pass_to_target_insert_bc.out_process_id1          AS prcs_id,
         exp_pass_to_target_insert_bc.out_edw_strt_dttm        AS edw_strt_dttm,
         exp_pass_to_target_insert_bc.out_edw_end_dttm         AS edw_end_dttm
  FROM   exp_pass_to_target_insert_bc;
  
  -- Component tgt_city_insert_bc, Type Post SQL
  /*UPDATE CITY FROM
(SELECT distinct GEOGRCL_AREA_SHRT_NAME,TERR_ID,EDW_STRT_DTTM,
max(EDW_STRT_DTTM) over (partition by GEOGRCL_AREA_SHRT_NAME,TERR_ID
ORDER BY EDW_STRT_DTTM ASC rows between 1 following
and 1 following) - INTERVAL ''1'' SECOND
as lead1
FROM CITY) A
SET EDW_END_DTTM = A.lead1
WHERE
CITY.EDW_STRT_DTTM = A.EDW_STRT_DTTM
AND CITY.GEOGRCL_AREA_SHRT_NAME = A.GEOGRCL_AREA_SHRT_NAME
AND CITY.TERR_ID = A.TERR_ID
AND A.lead1 is not null;*/

END;
';