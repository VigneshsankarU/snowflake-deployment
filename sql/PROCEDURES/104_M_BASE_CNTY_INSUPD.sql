-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CNTY_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
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
                $3 AS county,
                $4 AS createtime,
                $5 AS retired,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   state_typecode,
                                                    CASE
                                                             WHEN (
                                                                               ctry_typecode IS NULL) THEN ''US''
                                                             ELSE ctry_typecode
                                                    END                 ctry_typecd,
                                                    county_stg          county,
                                                    max(createtime_stg) createtime,
                                                    cast (0 AS bigint)  retired
                                           FROM     (
                                                                    SELECT DISTINCT bctl_state.typecode_stg   AS state_typecode,
                                                                                    bctl_country.typecode_stg AS ctry_typecode,
                                                                                    upper(ba.county_stg)      AS county_stg ,
                                                                                    ba.createtime_stg
                                                                    FROM            db_t_prod_stag.bc_address ba
                                                                    join            db_t_prod_stag.bctl_state
                                                                    ON              ba.state_stg=bctl_state.id_stg
                                                                    left join       db_t_prod_stag.bctl_country
                                                                    ON              bctl_country.id_stg=ba.country_stg
                                                                    WHERE           ba.updatetime_stg>($start_dttm)
                                                                    AND             ba.updatetime_stg <= ($end_dttm)
                                                                    AND             ba.county_stg IS NOT NULL
                                                                    UNION
                                                                    SELECT DISTINCT cctl_state.typecode_stg   AS state_typecode,
                                                                                    cctl_country.typecode_stg AS ctry_typecode,
                                                                                    upper(ca.county_stg)      AS county_stg,
                                                                                    ca.createtime_stg
                                                                    FROM            db_t_prod_stag.cc_address ca
                                                                    join            db_t_prod_stag.cctl_state
                                                                    ON              ca.state_stg=cctl_state.id_stg
                                                                    left join       db_t_prod_stag.cctl_country
                                                                    ON              cctl_country.id_stg=ca.country_stg
                                                                    WHERE           ca.updatetime_stg>($start_dttm)
                                                                    AND             ca.updatetime_stg <= ($end_dttm)
                                                                    AND             ca.county_stg IS NOT NULL
                                                                    UNION
                                                                    SELECT DISTINCT pctl_state.typecode_stg   AS state_typecode,
                                                                                    pctl_country.typecode_stg AS ctry_typecode,
                                                                                    upper(pa.county_stg)      AS county_stg,
                                                                                    pa.createtime_stg
                                                                    FROM            db_t_prod_stag.pc_address pa
                                                                    left join       db_t_prod_stag.pctl_country
                                                                    ON              pctl_country.id_stg=pa.country_stg
                                                                    join            db_t_prod_stag.pctl_state
                                                                    ON              pa.state_stg=pctl_state.id_stg
                                                                    WHERE           pa.updatetime_stg> ($start_dttm)
                                                                    AND             pa.updatetime_stg <= ($end_dttm)
                                                                    AND             pa.county_stg IS NOT NULL
                                                                    UNION
                                                                          /*  BOP AND CHURCH -POLICY LOCATION*/
                                                                          
                                                                    SELECT DISTINCT pctl_state.typecode_stg                     AS state_typecode,
                                                                                    pctl_country.typecode_stg                   AS ctry_typecode,
                                                                                    upper(pc_policylocation.countyinternal_stg) AS county_stg,
                                                                                    pc_policylocation.createtime_stg
                                                                    FROM            db_t_prod_stag.pc_policylocation
                                                                    join            db_t_prod_stag.pctl_state
                                                                    ON              pc_policylocation.stateinternal_stg=pctl_state.id_stg
                                                                    left join       db_t_prod_stag.pctl_country
                                                                    ON              pctl_country.id_stg=pc_policylocation.countryinternal_stg
                                                                    WHERE           pc_policylocation.countyinternal_stg IS NOT NULL
                                                                    AND             pc_policylocation.updatetime_stg > ($start_dttm)
                                                                    AND             pc_policylocation.updatetime_stg <= ($end_dttm)
                                                                    UNION
                                                                    SELECT DISTINCT state1.state_typecode,
                                                                                    state1.country_typecode AS ctry_typecode,
                                                                                    county.name_stg,
                                                                                    to_date(''1900-01-01'' ,''yyyy-mm-dd'') AS createtime_stg
                                                                    FROM            (
                                                                                               SELECT     pc_zone.id_stg,
                                                                                                          zl_state_county.zone2id_stg,
                                                                                                          pc_zone.name_stg AS state_typecode,
                                                                                                          /* 01 */
                                                                                                          pctl_country.typecode_stg AS country_typecode
                                                                                                          /* 02 */
                                                                                               FROM       db_t_prod_stag.pc_zone
                                                                                               inner join db_t_prod_stag.pctl_zonetype
                                                                                               ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                               inner join db_t_prod_stag.pctl_country
                                                                                               ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                               inner join db_t_prod_stag.pctl_state
                                                                                               ON         pctl_state.typecode_stg=pc_zone.name_stg
                                                                                               inner join db_t_prod_stag.pc_zone_link zl_state_county
                                                                                               ON         pc_zone.id_stg=zl_state_county.zone1id_stg
                                                                                               WHERE      pctl_zonetype.typecode_stg=''state'') state1
                                                                    inner join
                                                                                    (
                                                                                               SELECT     pc_zone.id_stg,
                                                                                                          pc_zone.name_stg,
                                                                                                          zl_county_city.zone2id_stg
                                                                                               FROM       db_t_prod_stag.pc_zone
                                                                                               inner join db_t_prod_stag.pctl_zonetype
                                                                                               ON         pctl_zonetype.id_stg=pc_zone.zonetype_stg
                                                                                               inner join db_t_prod_stag.pctl_country
                                                                                               ON         pctl_country.id_stg=pc_zone.country_stg
                                                                                               inner join db_t_prod_stag.pc_zone_link zl_county_city
                                                                                               ON         pc_zone.id_stg=zl_county_city.zone1id_stg
                                                                                               WHERE      pctl_zonetype.typecode_stg=''county'') county
                                                                    ON              state1.zone2id_stg=county.id_stg ) tmp
                                           GROUP BY state_typecode,
                                                    ctry_typecd,
                                                    county_stg ) src ) );
  -- Component exp_pass_from_source_pc111, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source_pc111 AS
  (
         SELECT sq_bc_address.state_typecode AS state_typecode,
                sq_bc_address.ctry_typecode  AS ctry_typecode,
                sq_bc_address.county         AS county,
                sq_bc_address.createtime     AS createtime,
                sq_bc_address.retired        AS retired,
                sq_bc_address.source_record_id
         FROM   sq_bc_address );
  -- Component exp_id_lookup_pc1111, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_id_lookup_pc1111 AS
  (
            SELECT    exp_pass_from_source_pc111.county AS county,
                      lkp_1.ctry_id
                      /* replaced lookup LKP_CTRY */
                      AS v_ctry_id,
                      lkp_2.terr_id
                      /* replaced lookup LKP_TERR */
                      AS v_terr_id,
                      CASE
                                WHEN v_terr_id IS NULL THEN 9999
                                ELSE v_terr_id
                      END                                   AS o_terr_id,
                      exp_pass_from_source_pc111.createtime AS createtime,
                      exp_pass_from_source_pc111.retired    AS retired,
                      exp_pass_from_source_pc111.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source_pc111.source_record_id ORDER BY exp_pass_from_source_pc111.source_record_id) AS rnk
            FROM      exp_pass_from_source_pc111
            left join lkp_ctry lkp_1
            ON        lkp_1.geogrcl_area_shrt_name = exp_pass_from_source_pc111.ctry_typecode
            left join lkp_terr lkp_2
            ON        lkp_2.ctry_id = v_ctry_id
            AND       lkp_2.geogrcl_area_shrt_name = exp_pass_from_source_pc111.state_typecode qualify rnk = 1 );
  -- Component LKP_COUNTY, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_county AS
  (
            SELECT    lkp.cnty_id,
                      lkp.terr_id,
                      lkp.geogrcl_area_sbtype_cd,
                      lkp.geogrcl_area_shrt_name,
                      lkp.loctr_sbtype_cd,
                      lkp.geogrcl_area_strt_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_id_lookup_pc1111.source_record_id,
                      row_number() over(PARTITION BY exp_id_lookup_pc1111.source_record_id ORDER BY lkp.cnty_id DESC,lkp.terr_id DESC,lkp.geogrcl_area_sbtype_cd DESC,lkp.geogrcl_area_shrt_name DESC,lkp.loctr_sbtype_cd DESC,lkp.geogrcl_area_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_id_lookup_pc1111
            left join
                      (
                             SELECT cnty.cnty_id                AS cnty_id,
                                    cnty.geogrcl_area_sbtype_cd AS geogrcl_area_sbtype_cd,
                                    cnty.loctr_sbtype_cd        AS loctr_sbtype_cd,
                                    cnty.geogrcl_area_strt_dttm AS geogrcl_area_strt_dttm,
                                    cnty.edw_strt_dttm          AS edw_strt_dttm,
                                    cnty.edw_end_dttm           AS edw_end_dttm,
                                    cnty.terr_id                AS terr_id,
                                    cnty.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                             FROM   db_t_prod_core.cnty
                             WHERE  cast(cnty.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) ) lkp
            ON        lkp.terr_id = exp_id_lookup_pc1111.o_terr_id
            AND       lkp.geogrcl_area_shrt_name = exp_id_lookup_pc1111.county 
			qualify row_number() over(PARTITION BY exp_id_lookup_pc1111.source_record_id ORDER BY lkp.cnty_id DESC,lkp.terr_id DESC,lkp.geogrcl_area_sbtype_cd DESC,lkp.geogrcl_area_shrt_name DESC,lkp.loctr_sbtype_cd DESC,lkp.geogrcl_area_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) 
            			= 1 );
  -- Component exp_id_lookup_pc111, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_id_lookup_pc111 AS
  (
             SELECT     exp_pass_from_source_pc111.county AS county,
                        lkp_1.ctry_id
                        /* replaced lookup LKP_CTRY */
                        AS v_ctry_id,
                        lkp_2.terr_id
                        /* replaced lookup LKP_TERR */
                                               AS v_terr_id,
                        ''LOCTR_SBTYPE3''        AS v_loctr_sbtype_val,
                        ''GEOGRCL_AREA_SBTYPE3'' AS v_geogrcl_sbtype_val,
                        lkp_3.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */
                        AS v_loctr_sbtype,
                        lkp_4.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */
                        AS v_geogrcl_sbtype,
                        CASE
                                   WHEN v_terr_id IS NULL THEN 9999
                                   ELSE v_terr_id
                        END                                   AS o_terr_id,
                        lkp_county.cnty_id                    AS lkp_cnty_id,
                        v_loctr_sbtype                        AS o_loctr_sbtype,
                        v_geogrcl_sbtype                      AS o_geogrcl_sbtype,
                        $prcs_id                              AS o_process_id,
                        exp_pass_from_source_pc111.createtime AS createtime,
                        lkp_county.geogrcl_area_strt_dttm     AS geogrcl_area_strt_dt,
                        md5 ( lkp_county.geogrcl_area_sbtype_cd
                                   || lkp_county.loctr_sbtype_cd ) AS lkp_checksum,
                        md5 ( v_geogrcl_sbtype
                                   || v_loctr_sbtype ) AS in_checksum,
                        CASE
                                   WHEN lkp_county.cnty_id IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN lkp_checksum <> in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END )
                        END                                                                    AS o_upd_or_ins,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        lkp_county.terr_id                                                     AS lkp_terr_id,
                        lkp_county.geogrcl_area_shrt_name                                      AS lkp_geogrcl_area_shrt_name,
                        lkp_county.edw_strt_dttm                                               AS lkp_edw_strt_dttm,
                        lkp_county.cnty_id                                                     AS cnty_id,
                        exp_id_lookup_pc1111.retired                                           AS retired,
                        lkp_county.edw_end_dttm                                                AS lkp_edw_end_dttm,
                        exp_pass_from_source_pc111.source_record_id,
                        row_number() over (PARTITION BY exp_pass_from_source_pc111.source_record_id ORDER BY exp_pass_from_source_pc111.source_record_id) AS rnk
             FROM       exp_pass_from_source_pc111
             inner join exp_id_lookup_pc1111
             ON         exp_pass_from_source_pc111.source_record_id = exp_id_lookup_pc1111.source_record_id
             inner join lkp_county
             ON         exp_id_lookup_pc1111.source_record_id = lkp_county.source_record_id
             left join  lkp_ctry lkp_1
             ON         lkp_1.geogrcl_area_shrt_name = exp_pass_from_source_pc111.ctry_typecode
             left join  lkp_terr lkp_2
             ON         lkp_2.ctry_id = v_ctry_id
             AND        lkp_2.geogrcl_area_shrt_name = exp_pass_from_source_pc111.state_typecode
             left join  lkp_teradata_etl_ref_xlat_loctr_sbtype lkp_3
             ON         lkp_3.src_idntftn_val = v_loctr_sbtype_val
             left join  lkp_teradata_etl_ref_xlat_geogrcl_area_sbtype lkp_4
             ON         lkp_4.src_idntftn_val = v_geogrcl_sbtype_val 
			 qualify row_number() over (PARTITION BY exp_pass_from_source_pc111.source_record_id ORDER BY exp_pass_from_source_pc111.source_record_id) 
			 = 1 );
  -- Component rtr_update_insert_pc111_INS, Type ROUTER Output Group INS
  create or replace table rtr_update_insert_pc111_ins as
  SELECT exp_id_lookup_pc111.county                     AS short_name,
         exp_id_lookup_pc111.county                     AS name,
         exp_id_lookup_pc111.o_terr_id                  AS terr_id,
         exp_id_lookup_pc111.o_upd_or_ins               AS upd_or_ins,
         exp_id_lookup_pc111.lkp_cnty_id                AS out_cnty_id,
         exp_id_lookup_pc111.o_process_id               AS o_process_id,
         exp_id_lookup_pc111.o_loctr_sbtype             AS o_loctr_sbtype,
         exp_id_lookup_pc111.o_geogrcl_sbtype           AS o_geogrcl_sbtype,
         exp_id_lookup_pc111.edw_strt_dttm              AS edw_strt_dttm,
         exp_id_lookup_pc111.edw_end_dttm               AS edw_end_dttm,
         exp_id_lookup_pc111.createtime                 AS createtime,
         exp_id_lookup_pc111.lkp_terr_id                AS lkp_terr_id,
         exp_id_lookup_pc111.lkp_geogrcl_area_shrt_name AS lkp_geogrcl_area_shrt_name,
         exp_id_lookup_pc111.geogrcl_area_strt_dt       AS geogrcl_area_strt_dt4,
         exp_id_lookup_pc111.cnty_id                    AS cnty_id,
         exp_id_lookup_pc111.retired                    AS retired,
         exp_id_lookup_pc111.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_id_lookup_pc111.lkp_edw_strt_dttm          AS lkp_edw_strt_dttm,
         exp_id_lookup_pc111.source_record_id
  FROM   exp_id_lookup_pc111
  WHERE  (
                exp_id_lookup_pc111.o_upd_or_ins = ''I'' ) --- -
  OR     (
                exp_id_lookup_pc111.o_upd_or_ins = ''U'' ) --- - exp_id_lookup_pc111.o_upd_or_ins = ''U''
  AND    exp_id_lookup_pc111.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_insert_pc111, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert_pc111 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_update_insert_pc111_ins.short_name       AS short_name1,
                rtr_update_insert_pc111_ins.name             AS name1,
                rtr_update_insert_pc111_ins.terr_id          AS terr_id1,
                rtr_update_insert_pc111_ins.o_process_id     AS o_process_id1,
                rtr_update_insert_pc111_ins.o_loctr_sbtype   AS o_loctr_sbtype1,
                rtr_update_insert_pc111_ins.o_geogrcl_sbtype AS o_geogrcl_sbtype1,
                rtr_update_insert_pc111_ins.edw_strt_dttm    AS edw_strt_dttm1,
                rtr_update_insert_pc111_ins.edw_end_dttm     AS edw_end_dttm1,
                rtr_update_insert_pc111_ins.createtime       AS createtime,
                0                                            AS update_strategy_action,
				source_record_id
         FROM   rtr_update_insert_pc111_ins );
  -- Component exp_pass_to_target_insert_pc111, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert_pc111 AS
  (
         SELECT upd_insert_pc111.short_name1       AS short_name1,
                upd_insert_pc111.name1             AS name1,
                upd_insert_pc111.terr_id1          AS terr_id1,
                upd_insert_pc111.o_process_id1     AS o_process_id1,
				seqcnty.NEXTVAL                    AS var_cnty_id,  -- created this sequence during testing
                var_cnty_id                        AS out_cnty_id,
                upd_insert_pc111.o_loctr_sbtype1   AS o_loctr_sbtype1,
                upd_insert_pc111.o_geogrcl_sbtype1 AS o_geogrcl_sbtype1,
                upd_insert_pc111.edw_strt_dttm1    AS edw_strt_dttm1,
                upd_insert_pc111.edw_end_dttm1     AS edw_end_dttm1,
                upd_insert_pc111.createtime        AS createtime,
                upd_insert_pc111.source_record_id
         FROM   upd_insert_pc111 );
  -- Component tgt_cnty_insert_bc, Type TARGET
  INSERT INTO db_t_prod_core.cnty
              (
                          cnty_id,
                          terr_id,
                          geogrcl_area_shrt_name,
                          geogrcl_area_name,
                          geogrcl_area_strt_dttm,
                          loctr_sbtype_cd,
                          geogrcl_area_sbtype_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_insert_pc111.out_cnty_id       AS cnty_id,
         exp_pass_to_target_insert_pc111.terr_id1          AS terr_id,
         exp_pass_to_target_insert_pc111.short_name1       AS geogrcl_area_shrt_name,
         exp_pass_to_target_insert_pc111.name1             AS geogrcl_area_name,
         exp_pass_to_target_insert_pc111.createtime        AS geogrcl_area_strt_dttm,
         exp_pass_to_target_insert_pc111.o_loctr_sbtype1   AS loctr_sbtype_cd,
         exp_pass_to_target_insert_pc111.o_geogrcl_sbtype1 AS geogrcl_area_sbtype_cd,
         exp_pass_to_target_insert_pc111.o_process_id1     AS prcs_id,
         exp_pass_to_target_insert_pc111.edw_strt_dttm1    AS edw_strt_dttm,
         exp_pass_to_target_insert_pc111.edw_end_dttm1     AS edw_end_dttm
  FROM   exp_pass_to_target_insert_pc111;

END;
';