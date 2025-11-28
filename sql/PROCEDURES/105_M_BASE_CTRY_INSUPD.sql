-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CTRY_INSUPD("PARAM_JSON" VARCHAR)
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
  -- Component sq_bctl_country, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bctl_country AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS description,
                $2 AS name,
                $3 AS typecode,
                $4 AS strt_dt,
                $5 AS end_dt,
                $6 AS retired,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT description,
                                                                  max(name),
                                                                  typecode,
                                                                  to_date(''01/01/1900'' ,''MM/DD/YYYY'') AS strt_dt,
                                                                  to_date(''12/31/9999'' ,''MM/DD/YYYY'') AS end_dt,
                                                                  retired
                                                  FROM            (
                                                                         SELECT description_stg     description ,
                                                                                upper(name_stg)     name ,
                                                                                upper(typecode_stg) typecode ,
                                                                                retired_stg         retired
                                                                         FROM   db_t_prod_stag.bctl_country
                                                                         UNION
                                                                         SELECT description_stg     description ,
                                                                                upper(name_stg)     name ,
                                                                                upper(typecode_stg) typecode ,
                                                                                retired_stg         retired
                                                                         FROM   db_t_prod_stag.cctl_country
                                                                         UNION
                                                                         SELECT description_stg     description ,
                                                                                upper(name_stg)     name ,
                                                                                upper(typecode_stg) typecode ,
                                                                                retired_stg         retired
                                                                         FROM   db_t_prod_stag.pctl_country ) ctry
                                                  WHERE           retired=0
                                                  GROUP BY        description,
                                                                  typecode,
                                                                  retired ) src ) );
  -- Component exp_pass_from_source_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source_bc AS
  (
         SELECT sq_bctl_country.typecode    AS typecode,
                sq_bctl_country.name        AS name,
                sq_bctl_country.description AS description,
                sq_bctl_country.strt_dt     AS strt_dt,
                sq_bctl_country.end_dt      AS end_dt,
                sq_bctl_country.retired     AS retired,
                sq_bctl_country.source_record_id
         FROM   sq_bctl_country );
  -- Component exp_data_transformation_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation_bc AS
  (
            SELECT    exp_pass_from_source_bc.typecode    AS typecode,
                      exp_pass_from_source_bc.name        AS name,
                      exp_pass_from_source_bc.description AS description,
                      ''LOCTR_SBTYPE3''                     AS v_loctr_sbtype_val,
                      ''GEOGRCL_AREA_SBTYPE5''              AS v_geogrcl_area_sbtype_val,
                      lkp_1.ctry_id
                      /* replaced lookup LKP_CTRY */
                      AS v_ctry_id,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */
                      AS v_loctr_sbtype,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */
                                                      AS v_geogrcl_area_sbtype,
                      exp_pass_from_source_bc.strt_dt AS strt_dt,
                      exp_pass_from_source_bc.end_dt  AS end_dt,
                      exp_pass_from_source_bc.retired AS retired,
                      v_ctry_id                       AS out_ctry_id,
                      v_loctr_sbtype                  AS out_loctr_sbtype,
                      v_geogrcl_area_sbtype           AS out_geogrcl_area_sbtype,
                      $prcs_id                        AS out_process_id,
                      exp_pass_from_source_bc.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source_bc.source_record_id ORDER BY exp_pass_from_source_bc.source_record_id) AS rnk
            FROM      exp_pass_from_source_bc
            left join lkp_ctry lkp_1
            ON        lkp_1.geogrcl_area_shrt_name = exp_pass_from_source_bc.typecode
            left join lkp_teradata_etl_ref_xlat_loctr_sbtype lkp_2
            ON        lkp_2.src_idntftn_val = v_loctr_sbtype_val
            left join lkp_teradata_etl_ref_xlat_geogrcl_area_sbtype lkp_3
            ON        lkp_3.src_idntftn_val = v_geogrcl_area_sbtype_val qualify rnk = 1 );
  -- Component LKP_CTRY_CDC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_ctry_cdc AS
  (
            SELECT    lkp.ctry_id,
                      lkp.cal_type_cd,
                      lkp.iso_3166_ctry_num,
                      lkp.geogrcl_area_shrt_name,
                      lkp.geogrcl_area_name,
                      lkp.geogrcl_area_desc,
                      lkp.cury_cd,
                      lkp.geogrcl_area_strt_dttm,
                      lkp.geogrcl_area_end_dttm,
                      lkp.loctr_sbtype_cd,
                      lkp.geogrcl_area_sbtype_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_data_transformation_bc.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation_bc.source_record_id ORDER BY lkp.ctry_id ASC,lkp.cal_type_cd ASC,lkp.iso_3166_ctry_num ASC,lkp.geogrcl_area_shrt_name ASC,lkp.geogrcl_area_name ASC,lkp.geogrcl_area_desc ASC,lkp.cury_cd ASC,lkp.geogrcl_area_strt_dttm ASC,lkp.geogrcl_area_end_dttm ASC,lkp.loctr_sbtype_cd ASC,lkp.geogrcl_area_sbtype_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_data_transformation_bc
            left join
                      (
                             SELECT ctry.ctry_id                AS ctry_id,
                                    ctry.cal_type_cd            AS cal_type_cd,
                                    ctry.iso_3166_ctry_num      AS iso_3166_ctry_num,
                                    ctry.geogrcl_area_name      AS geogrcl_area_name,
                                    ctry.geogrcl_area_desc      AS geogrcl_area_desc,
                                    ctry.cury_cd                AS cury_cd,
                                    ctry.geogrcl_area_strt_dttm AS geogrcl_area_strt_dttm,
                                    ctry.geogrcl_area_end_dttm  AS geogrcl_area_end_dttm,
                                    ctry.loctr_sbtype_cd        AS loctr_sbtype_cd,
                                    ctry.geogrcl_area_sbtype_cd AS geogrcl_area_sbtype_cd,
                                    ctry.prcs_id                AS prcs_id,
                                    ctry.edw_strt_dttm          AS edw_strt_dttm,
                                    ctry.edw_end_dttm           AS edw_end_dttm,
                                    ctry.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                             FROM   db_t_prod_core.ctry
                             WHERE  cast(ctry.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) ) lkp
            ON        lkp.geogrcl_area_shrt_name = exp_data_transformation_bc.typecode 
			qualify row_number() over(PARTITION BY exp_data_transformation_bc.source_record_id ORDER BY lkp.ctry_id ASC,lkp.cal_type_cd ASC,lkp.iso_3166_ctry_num ASC,lkp.geogrcl_area_shrt_name ASC,lkp.geogrcl_area_name ASC,lkp.geogrcl_area_desc ASC,lkp.cury_cd ASC,lkp.geogrcl_area_strt_dttm ASC,lkp.geogrcl_area_end_dttm ASC,lkp.loctr_sbtype_cd ASC,lkp.geogrcl_area_sbtype_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
            			= 1 );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
             SELECT     lkp_ctry_cdc.ctry_id                               AS lkp_ctry_id,
                        lkp_ctry_cdc.geogrcl_area_shrt_name                AS lkp_geogrcl_area_shrt_name,
                        lkp_ctry_cdc.edw_strt_dttm                         AS lkp_edw_strt_dttm,
                        lkp_ctry_cdc.edw_end_dttm                          AS lkp_edw_end_dttm,
                        exp_data_transformation_bc.typecode                AS typecode,
                        exp_data_transformation_bc.name                    AS name,
                        exp_data_transformation_bc.description             AS description,
                        exp_data_transformation_bc.out_ctry_id             AS out_ctry_id,
                        exp_data_transformation_bc.out_loctr_sbtype        AS out_loctr_sbtype,
                        exp_data_transformation_bc.out_geogrcl_area_sbtype AS out_geogrcl_area_sbtype,
                        exp_data_transformation_bc.strt_dt                 AS geogrcl_area_strt_dt,
                        exp_data_transformation_bc.end_dt                  AS geogrcl_area_end_dt,
                        exp_data_transformation_bc.retired                 AS retired,
                        exp_data_transformation_bc.out_process_id          AS out_process_id,
                        md5 ( ltrim ( rtrim ( lkp_ctry_cdc.ctry_id ) )
                                   || ltrim ( rtrim ( lkp_ctry_cdc.geogrcl_area_name ) )
                                   || ltrim ( rtrim ( lkp_ctry_cdc.geogrcl_area_desc ) )
                                   || ltrim ( rtrim ( lkp_ctry_cdc.loctr_sbtype_cd ) )
                                   || ltrim ( rtrim ( lkp_ctry_cdc.geogrcl_area_sbtype_cd ) ) ) AS var_orig_chksm,
                        md5 ( ltrim ( rtrim ( exp_data_transformation_bc.out_ctry_id ) )
                                   || ltrim ( rtrim ( exp_data_transformation_bc.name ) )
                                   || ltrim ( rtrim ( exp_data_transformation_bc.description ) )
                                   || ltrim ( rtrim ( exp_data_transformation_bc.out_loctr_sbtype ) )
                                   || ltrim ( rtrim ( exp_data_transformation_bc.out_geogrcl_area_sbtype ) ) ) AS var_calc_chksm,
                        CASE
                                   WHEN var_orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE var_orig_chksm --$3
                                              END
                        END AS out_upd_or_ins,
                        exp_data_transformation_bc.source_record_id
             FROM       exp_data_transformation_bc
             inner join lkp_ctry_cdc
             ON         exp_data_transformation_bc.source_record_id = lkp_ctry_cdc.source_record_id );
  -- Component rtr_update_insert_bc_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_update_insert_bc_insert as
  SELECT exp_ins_upd.typecode                   AS typecode,
         exp_ins_upd.name                       AS name,
         exp_ins_upd.description                AS description,
         exp_ins_upd.out_ctry_id                AS ctry_id,
         exp_ins_upd.out_upd_or_ins             AS upd_or_ins,
         exp_ins_upd.out_loctr_sbtype           AS out_loctr_sbtype,
         exp_ins_upd.out_geogrcl_area_sbtype    AS out_geogrcl_area_sbtype,
         exp_ins_upd.geogrcl_area_strt_dt       AS geogrcl_area_strt_dt,
         exp_ins_upd.geogrcl_area_end_dt        AS geogrcl_area_end_dt,
         exp_ins_upd.retired                    AS retired,
         exp_ins_upd.out_process_id             AS out_process_id,
         exp_ins_upd.lkp_ctry_id                AS lkp_ctry_id,
         exp_ins_upd.lkp_geogrcl_area_shrt_name AS lkp_geogrcl_area_shrt_name,
         exp_ins_upd.lkp_edw_strt_dttm          AS lkp_edw_strt_dttm,
         exp_ins_upd.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.out_upd_or_ins = ''I''  --- -
  OR     exp_ins_upd.out_upd_or_ins = ''U''
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_insert_bc, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert_bc AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_update_insert_bc_insert.typecode                AS typecode,
                rtr_update_insert_bc_insert.name                    AS name,
                rtr_update_insert_bc_insert.description             AS description,
                rtr_update_insert_bc_insert.out_process_id          AS out_process_id1,
                rtr_update_insert_bc_insert.out_loctr_sbtype        AS out_loctr_sbtype1,
                rtr_update_insert_bc_insert.out_geogrcl_area_sbtype AS out_geogrcl_area_sbtype1,
                rtr_update_insert_bc_insert.geogrcl_area_strt_dt    AS geogrcl_area_strt_dt,
                rtr_update_insert_bc_insert.geogrcl_area_end_dt     AS geogrcl_area_end_dt,
                rtr_update_insert_bc_insert.retired                 AS retired1,
                0                                                   AS update_strategy_action,
				rtr_update_insert_bc_insert.source_record_id
         FROM   rtr_update_insert_bc_insert );
  -- Component exp_pass_to_target_insert_bc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert_bc AS
  (
         SELECT upd_insert_bc.typecode                                                 AS typecode,
                upd_insert_bc.name                                                     AS name,
                upd_insert_bc.description                                              AS description,
                ''UNK''                                                                  AS out_cal_type_cd,
                ''UNK''                                                                  AS out_iso_3166_ctry_num,
                upd_insert_bc.out_process_id1                                          AS out_process_id1,
                upd_insert_bc.out_loctr_sbtype1                                        AS out_loctr_sbtype1,
                upd_insert_bc.out_geogrcl_area_sbtype1                                 AS out_geogrcl_area_sbtype1,
                upd_insert_bc.geogrcl_area_strt_dt                                     AS geogrcl_area_strt_dt,
                upd_insert_bc.geogrcl_area_end_dt                                      AS geogrcl_area_end_dt,
                seq_loc.NEXTVAL                                                        AS ctry_id,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                upd_insert_bc.source_record_id
         FROM   upd_insert_bc );
  -- Component tgt_ctry_insert_bc, Type TARGET
  INSERT INTO db_t_prod_core.ctry
              (
                          ctry_id,
                          cal_type_cd,
                          iso_3166_ctry_num,
                          geogrcl_area_shrt_name,
                          geogrcl_area_name,
                          geogrcl_area_desc,
                          geogrcl_area_strt_dttm,
                          geogrcl_area_end_dttm,
                          loctr_sbtype_cd,
                          geogrcl_area_sbtype_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_insert_bc.ctry_id                  AS ctry_id,
         exp_pass_to_target_insert_bc.out_cal_type_cd          AS cal_type_cd,
         exp_pass_to_target_insert_bc.out_iso_3166_ctry_num    AS iso_3166_ctry_num,
         exp_pass_to_target_insert_bc.typecode                 AS geogrcl_area_shrt_name,
         exp_pass_to_target_insert_bc.name                     AS geogrcl_area_name,
         exp_pass_to_target_insert_bc.description              AS geogrcl_area_desc,
         exp_pass_to_target_insert_bc.geogrcl_area_strt_dt     AS geogrcl_area_strt_dttm,
         exp_pass_to_target_insert_bc.geogrcl_area_end_dt      AS geogrcl_area_end_dttm,
         exp_pass_to_target_insert_bc.out_loctr_sbtype1        AS loctr_sbtype_cd,
         exp_pass_to_target_insert_bc.out_geogrcl_area_sbtype1 AS geogrcl_area_sbtype_cd,
         exp_pass_to_target_insert_bc.out_process_id1          AS prcs_id,
         exp_pass_to_target_insert_bc.out_edw_strt_dttm        AS edw_strt_dttm,
         exp_pass_to_target_insert_bc.out_edw_end_dttm         AS edw_end_dttm
  FROM   exp_pass_to_target_insert_bc;

END;
';