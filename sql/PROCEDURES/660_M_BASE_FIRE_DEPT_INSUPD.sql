-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FIRE_DEPT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;

BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'');
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'');


  -- Component LKP_COUNTY, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_county AS
  (
           SELECT   cnty.cnty_id                AS cnty_id,
                    cnty.geogrcl_area_sbtype_cd AS geogrcl_area_sbtype_cd,
                    cnty.loctr_sbtype_cd        AS loctr_sbtype_cd,
                    cnty.geogrcl_area_strt_dttm AS geogrcl_area_strt_dttm,
                    cnty.edw_strt_dttm          AS edw_strt_dttm,
                    cnty.edw_end_dttm           AS edw_end_dttm,
                    cnty.terr_id                AS terr_id,
                    cnty.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
           FROM     db_t_prod_core.cnty qualify row_number() over(PARTITION BY terr_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC) = 1 );
  -- Component LKP_CTRY, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_ctry AS
  (
           SELECT   ctry.ctry_id                AS ctry_id,
                    ctry.geogrcl_area_name      AS geogrcl_area_name,
                    ctry.geogrcl_area_desc      AS geogrcl_area_desc,
                    ctry.edw_strt_dttm          AS edw_strt_dttm,
                    ctry.edw_end_dttm           AS edw_end_dttm,
                    ctry.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
           FROM     db_t_prod_core.ctry qualify row_number() over(PARTITION BY geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC) = 1
                    /* WHERE CTRY.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
  );
  -- Component LKP_TERR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_terr AS
  (
           SELECT   terr.terr_id                AS terr_id,
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
           FROM     db_t_prod_core.terr qualify row_number () over (PARTITION BY ctry_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC)=1 );
  -- Component SQ_pcx_firedepartment_alfa, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_firedepartment_alfa AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS publicid,
                $2  AS code,
                $3  AS name,
                $4  AS retired,
                $5  AS county_name,
                $6  AS state,
                $7  AS country,
                $8  AS effectivedate,
                $9  AS expirationdate,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pcx_firedepartment_alfa.publicid_stg       AS publicid,
                                                                  pcx_firedepartment_alfa.code_stg           AS code,
                                                                  pcx_firedepartment_alfa.name_stg           AS name,
                                                                  pcx_firedepartment_alfa.retired_stg        AS retired,
                                                                  county_tl.name_stg                         AS county_name,
                                                                  statecounty.typecode_stg                   AS statetypecode,
                                                                  ''US''                                       AS country,
                                                                  pcx_firedepartment_alfa.effectivedate_stg  AS effectivedate,
                                                                  pcx_firedepartment_alfa.expirationdate_stg AS expirationdate
                                                  FROM            db_t_prod_stag.pcx_firedepartment_alfa
                                                  left outer join db_t_prod_stag.pctl_jurisdiction
                                                  ON              pcx_firedepartment_alfa.state_stg=pctl_jurisdiction.id_stg
                                                  left outer join db_t_prod_stag.pcx_countycode_alfa county_tl
                                                  ON              pcx_firedepartment_alfa.countycode_alfa_stg = county_tl.id_stg
                                                  left outer join db_t_prod_stag.pctl_jurisdiction statecounty
                                                  ON              statecounty.id_stg =county_tl.state_stg
                                                  WHERE           pcx_firedepartment_alfa.updatetime_stg > (:START_DTTM)
                                                  AND             pcx_firedepartment_alfa.updatetime_stg <= (:END_DTTM) ) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT    sq_pcx_firedepartment_alfa.publicid AS publicid,
                      sq_pcx_firedepartment_alfa.code     AS code,
                      sq_pcx_firedepartment_alfa.name     AS name,
                      lkp_1.ctry_id
                      /* replaced lookup LKP_CTRY */
                      AS var_ctry_id,
                      lkp_2.terr_id
                      /* replaced lookup LKP_TERR */
                      AS var_terr_id,
                      CASE
                                WHEN lkp_3.cnty_id
                                          /* replaced lookup LKP_COUNTY */
                                          IS NULL THEN 9999
                                ELSE lkp_4.cnty_id
                                          /* replaced lookup LKP_COUNTY */
                      END                                       AS out_cnty_id,
                      :prcs_id                                  AS prcs_id,
                      sq_pcx_firedepartment_alfa.effectivedate  AS effectivedate,
                      sq_pcx_firedepartment_alfa.expirationdate AS expirationdate,
                      sq_pcx_firedepartment_alfa.retired        AS retired,
                      sq_pcx_firedepartment_alfa.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_firedepartment_alfa.source_record_id ORDER BY sq_pcx_firedepartment_alfa.source_record_id) AS rnk
            FROM      sq_pcx_firedepartment_alfa
            left join lkp_ctry lkp_1
            ON        lkp_1.geogrcl_area_shrt_name = sq_pcx_firedepartment_alfa.country
            left join lkp_terr lkp_2
            ON        lkp_2.ctry_id = var_ctry_id
            AND       lkp_2.geogrcl_area_shrt_name = sq_pcx_firedepartment_alfa.state
            left join lkp_county lkp_3
            ON        lkp_3.terr_id = var_terr_id
            AND       lkp_3.geogrcl_area_shrt_name = sq_pcx_firedepartment_alfa.county_name
            left join lkp_county lkp_4
            ON        lkp_4.terr_id = var_terr_id
            AND       lkp_4.geogrcl_area_shrt_name = sq_pcx_firedepartment_alfa.county_name qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
         SELECT seq_fire_dept_id.NEXTVAL      AS fire_dept_id,
                exp_all_source.code           AS fire_dept_cd,
                exp_all_source.name           AS fire_dept_name,
                exp_all_source.out_cnty_id    AS cnty_id,
                exp_all_source.publicid       AS host_fire_dept_num,
                exp_all_source.prcs_id        AS prcs_id,
                exp_all_source.effectivedate  AS effectivedate,
                exp_all_source.expirationdate AS expirationdate,
                exp_all_source.retired        AS retired,
                exp_all_source.source_record_id
         FROM   exp_all_source );
  -- Component exp_SRC_Fields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_fields AS
  (
         SELECT exp_data_transformation.fire_dept_id       AS in_fire_dept_id,
                exp_data_transformation.fire_dept_cd       AS in_fire_dept_cd,
                exp_data_transformation.fire_dept_name     AS in_fire_dept_name,
                exp_data_transformation.cnty_id            AS in_cnty_id,
                exp_data_transformation.host_fire_dept_num AS in_host_fire_dept_num,
                exp_data_transformation.prcs_id            AS in_prcs_id,
                exp_data_transformation.effectivedate      AS in_fire_dept_strt_dt,
                CASE
                       WHEN exp_data_transformation.expirationdate IS NULL THEN to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE exp_data_transformation.expirationdate
                END                                                                    AS o_in_fire_dept_end_dt,
                current_timestamp                                                      AS in_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                exp_data_transformation.retired                                        AS retired,
                exp_data_transformation.source_record_id
         FROM   exp_data_transformation );
  -- Component LKP_FIREDEPT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_firedept AS
  (
            SELECT    lkp.fire_dept_id,
                      lkp.fire_dept_cd,
                      lkp.fire_dept_name,
                      lkp.cnty_id,
                      lkp.host_fire_dept_num,
                      lkp.fire_dept_strt_dttm,
                      lkp.fire_dept_end_dttm,
                      lkp.edw_end_dttm,
                      exp_src_fields.source_record_id,
                      row_number() over(PARTITION BY exp_src_fields.source_record_id ORDER BY lkp.fire_dept_id DESC,lkp.fire_dept_cd DESC,lkp.fire_dept_name DESC,lkp.cnty_id DESC,lkp.host_fire_dept_num DESC,lkp.fire_dept_strt_dttm DESC,lkp.fire_dept_end_dttm DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_src_fields
            left join
                      (
                               SELECT   fire_dept.fire_dept_id        AS fire_dept_id,
                                        fire_dept.fire_dept_cd        AS fire_dept_cd,
                                        fire_dept.fire_dept_name      AS fire_dept_name,
                                        fire_dept.cnty_id             AS cnty_id,
                                        fire_dept.fire_dept_strt_dttm AS fire_dept_strt_dttm,
                                        fire_dept.fire_dept_end_dttm  AS fire_dept_end_dttm,
                                        fire_dept.edw_end_dttm        AS edw_end_dttm,
                                        fire_dept.host_fire_dept_num  AS host_fire_dept_num
                               FROM     db_t_prod_core.fire_dept qualify row_number() over(PARTITION BY fire_dept.host_fire_dept_num ORDER BY fire_dept.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.host_fire_dept_num = exp_src_fields.in_host_fire_dept_num qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_src_fields.in_fire_dept_id       AS in_fire_dept_id,
                        exp_src_fields.in_fire_dept_cd       AS in_fire_dept_cd,
                        exp_src_fields.in_fire_dept_name     AS in_fire_dept_name,
                        exp_src_fields.in_cnty_id            AS in_cnty_id,
                        exp_src_fields.in_host_fire_dept_num AS in_host_fire_dept_num,
                        exp_src_fields.in_prcs_id            AS in_prcs_id,
                        exp_src_fields.in_fire_dept_strt_dt  AS in_fire_dept_strt_dt,
                        exp_src_fields.o_in_fire_dept_end_dt AS in_fire_dept_end_dt,
                        exp_src_fields.in_edw_strt_dttm      AS in_edw_strt_dttm,
                        exp_src_fields.in_edw_end_dttm       AS in_edw_end_dttm,
                        lkp_firedept.host_fire_dept_num      AS lkp_host_fire_dept_num,
                        lkp_firedept.fire_dept_id            AS lkp_fire_dept_id,
                        lkp_firedept.edw_end_dttm            AS lkp_edw_end_dttm,
                        md5 ( exp_src_fields.in_fire_dept_cd
                                   || exp_src_fields.in_fire_dept_name
                                   || exp_src_fields.in_cnty_id
                                   || to_char ( exp_src_fields.in_fire_dept_strt_dt , ''dd/mm/yyyy'' )
                                   || to_char ( exp_src_fields.o_in_fire_dept_end_dt , ''dd/mm/yyyy'' ) ) AS v_src_md5,
                        md5 ( lkp_firedept.fire_dept_cd
                                   || lkp_firedept.fire_dept_name
                                   || lkp_firedept.cnty_id
                                   || to_char ( lkp_firedept.fire_dept_strt_dttm , ''dd/mm/yyyy'' )
                                   || to_char ( lkp_firedept.fire_dept_end_dttm , ''dd/mm/yyyy'' ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END                    AS o_cdc_check,
                        exp_src_fields.retired AS retired,
                        exp_src_fields.source_record_id
             FROM       exp_src_fields
             inner join lkp_firedept
             ON         exp_src_fields.source_record_id = lkp_firedept.source_record_id );
  -- Component rtr_fire_dept_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_fire_dept_insert as
  SELECT exp_cdc_check.in_fire_dept_id        AS in_fire_dept_id,
         exp_cdc_check.in_fire_dept_cd        AS in_fire_dept_cd,
         exp_cdc_check.in_fire_dept_name      AS in_fire_dept_name,
         exp_cdc_check.in_cnty_id             AS in_cnty_id,
         exp_cdc_check.in_host_fire_dept_num  AS in_host_fire_dept_num,
         exp_cdc_check.in_prcs_id             AS in_prcs_id,
         exp_cdc_check.in_fire_dept_strt_dt   AS in_fire_dept_strt_dt,
         exp_cdc_check.in_fire_dept_end_dt    AS in_fire_dept_end_dt,
         exp_cdc_check.in_edw_strt_dttm       AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm        AS in_edw_end_dttm,
         exp_cdc_check.lkp_host_fire_dept_num AS lkp_host_fire_dept_num,
         exp_cdc_check.o_cdc_check            AS o_cdc_check,
         exp_cdc_check.retired                AS retired,
         exp_cdc_check.lkp_fire_dept_id       AS lkp_fire_dept_id,
         exp_cdc_check.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.in_fire_dept_id IS NOT NULL
  AND    (
                exp_cdc_check.o_cdc_check = ''I''
         OR     (
                       exp_cdc_check.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_cdc_check.retired = 0 ) );
  
  -- Component rtr_fire_dept_retired, Type ROUTER Output Group retired
  create or replace temporary table rtr_fire_dept_retired as
  SELECT exp_cdc_check.in_fire_dept_id        AS in_fire_dept_id,
         exp_cdc_check.in_fire_dept_cd        AS in_fire_dept_cd,
         exp_cdc_check.in_fire_dept_name      AS in_fire_dept_name,
         exp_cdc_check.in_cnty_id             AS in_cnty_id,
         exp_cdc_check.in_host_fire_dept_num  AS in_host_fire_dept_num,
         exp_cdc_check.in_prcs_id             AS in_prcs_id,
         exp_cdc_check.in_fire_dept_strt_dt   AS in_fire_dept_strt_dt,
         exp_cdc_check.in_fire_dept_end_dt    AS in_fire_dept_end_dt,
         exp_cdc_check.in_edw_strt_dttm       AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm        AS in_edw_end_dttm,
         exp_cdc_check.lkp_host_fire_dept_num AS lkp_host_fire_dept_num,
         exp_cdc_check.o_cdc_check            AS o_cdc_check,
         exp_cdc_check.retired                AS retired,
         exp_cdc_check.lkp_fire_dept_id       AS lkp_fire_dept_id,
         exp_cdc_check.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_cdc_check = ''R''
  AND    exp_cdc_check.retired != 0
  AND    exp_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_fire_dept_update, Type ROUTER Output Group update
  create or replace temporary table rtr_fire_dept_update as
  SELECT exp_cdc_check.in_fire_dept_id        AS in_fire_dept_id,
         exp_cdc_check.in_fire_dept_cd        AS in_fire_dept_cd,
         exp_cdc_check.in_fire_dept_name      AS in_fire_dept_name,
         exp_cdc_check.in_cnty_id             AS in_cnty_id,
         exp_cdc_check.in_host_fire_dept_num  AS in_host_fire_dept_num,
         exp_cdc_check.in_prcs_id             AS in_prcs_id,
         exp_cdc_check.in_fire_dept_strt_dt   AS in_fire_dept_strt_dt,
         exp_cdc_check.in_fire_dept_end_dt    AS in_fire_dept_end_dt,
         exp_cdc_check.in_edw_strt_dttm       AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm        AS in_edw_end_dttm,
         exp_cdc_check.lkp_host_fire_dept_num AS lkp_host_fire_dept_num,
         exp_cdc_check.o_cdc_check            AS o_cdc_check,
         exp_cdc_check.retired                AS retired,
         exp_cdc_check.lkp_fire_dept_id       AS lkp_fire_dept_id,
         exp_cdc_check.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.in_fire_dept_id IS NOT NULL
  AND    (
                exp_cdc_check.o_cdc_check = ''U''
         AND    exp_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component exp_firedept_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_firedept_update AS
  (
         SELECT rtr_fire_dept_update.lkp_fire_dept_id       AS in_fire_dept_id1,
                rtr_fire_dept_update.in_fire_dept_cd        AS in_fire_dept_cd1,
                rtr_fire_dept_update.in_fire_dept_name      AS in_fire_dept_name1,
                rtr_fire_dept_update.in_cnty_id             AS in_cnty_id1,
                rtr_fire_dept_update.lkp_host_fire_dept_num AS in_host_fire_dept_num1,
                rtr_fire_dept_update.in_prcs_id             AS in_prcs_id1,
                rtr_fire_dept_update.in_fire_dept_strt_dt   AS in_fire_dept_strt_dt1,
                rtr_fire_dept_update.in_fire_dept_end_dt    AS in_fire_dept_end_dt1,
                rtr_fire_dept_update.in_edw_strt_dttm       AS in_edw_strt_dttm1,
                CASE
                       WHEN rtr_fire_dept_update.retired <> 0 THEN rtr_fire_dept_update.in_edw_strt_dttm
                       ELSE rtr_fire_dept_update.in_edw_end_dttm
                END AS o_edw_end_dttm,
                rtr_fire_dept_update.source_record_id
         FROM   rtr_fire_dept_update );
  -- Component upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_fire_dept_retired.lkp_fire_dept_id       AS lkp_fire_dept_id3,
                rtr_fire_dept_retired.lkp_host_fire_dept_num AS lkp_fire_dept_num3,
                rtr_fire_dept_retired.in_edw_strt_dttm       AS edw_strt_dttm1,
                1                                            AS update_strategy_action,
				source_record_id
         FROM   rtr_fire_dept_retired );
  -- Component exp_firedept_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_firedept_insert AS
  (
         SELECT rtr_fire_dept_insert.in_fire_dept_id       AS in_fire_dept_id1,
                rtr_fire_dept_insert.in_fire_dept_cd       AS in_fire_dept_cd1,
                rtr_fire_dept_insert.in_fire_dept_name     AS in_fire_dept_name1,
                rtr_fire_dept_insert.in_cnty_id            AS in_cnty_id1,
                rtr_fire_dept_insert.in_host_fire_dept_num AS in_host_fire_dept_num1,
                rtr_fire_dept_insert.in_prcs_id            AS in_prcs_id1,
                rtr_fire_dept_insert.in_fire_dept_strt_dt  AS in_fire_dept_strt_dt1,
                rtr_fire_dept_insert.in_fire_dept_end_dt   AS in_fire_dept_end_dt1,
                rtr_fire_dept_insert.in_edw_strt_dttm      AS in_edw_strt_dttm1,
                CASE
                       WHEN rtr_fire_dept_insert.retired <> 0 THEN current_timestamp
                       ELSE rtr_fire_dept_insert.in_edw_end_dttm
                END AS o_edw_end_dttm,
                rtr_fire_dept_insert.source_record_id
         FROM   rtr_fire_dept_insert );
  -- Component tgt_FIRE_DEPT_retired, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.fire_dept
  USING        upd_retired
  ON (
                            update_strategy_action = 1
               AND          fire_dept.fire_dept_id = upd_retired.lkp_fire_dept_id3
               AND          fire_dept.host_fire_dept_num = upd_retired.lkp_fire_dept_num3
               AND          fire_dept.edw_strt_dttm = upd_retired.edw_strt_dttm1)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = upd_retired.edw_strt_dttm1 ;
  
  -- Component tgt_FIRE_DEPT_retired, Type Post SQL
  UPDATE db_t_prod_core.fire_dept
   SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT fire_dept_id,
                                         host_fire_dept_num,
                                         edw_strt_dttm ,
                                         max(edw_strt_dttm) over (PARTITION BY host_fire_dept_num ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.fire_dept ) a
 
  WHERE  fire_dept.edw_strt_dttm=a.edw_strt_dttm
  AND    fire_dept.fire_dept_id=a.fire_dept_id
  AND    fire_dept.host_fire_dept_num=a.host_fire_dept_num
  AND    cast(fire_dept.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;
  
  -- Component Union, Type UNION_TRANSFORMATION
  CREATE
  OR
  replace TEMPORARY TABLE
  tbl_UNION
        AS
        (
               /* Union Group INS */
               SELECT exp_firedept_insert.in_fire_dept_id1,
                      exp_firedept_insert.in_fire_dept_cd1,
                      exp_firedept_insert.in_fire_dept_name1,
                      exp_firedept_insert.in_cnty_id1,
                      exp_firedept_insert.in_host_fire_dept_num1,
                      exp_firedept_insert.in_prcs_id1,
                      exp_firedept_insert.in_fire_dept_end_dt1,
                      exp_firedept_insert.in_edw_strt_dttm1,
                      exp_firedept_insert.o_edw_end_dttm,
                      exp_firedept_insert.in_fire_dept_strt_dt1,
                      exp_firedept_insert.source_record_id
               FROM   exp_firedept_insert
               UNION ALL
               /* Union Group UPD */
               SELECT exp_firedept_update.in_fire_dept_id1,
                      exp_firedept_update.in_fire_dept_cd1,
                      exp_firedept_update.in_fire_dept_name1,
                      exp_firedept_update.in_cnty_id1,
                      exp_firedept_update.in_host_fire_dept_num1,
                      exp_firedept_update.in_prcs_id1,
                      exp_firedept_update.in_fire_dept_end_dt1,
                      exp_firedept_update.in_edw_strt_dttm1,
                      exp_firedept_update.o_edw_end_dttm,
                      exp_firedept_update.in_fire_dept_strt_dt1,
                      exp_firedept_update.source_record_id
               FROM   exp_firedept_update );
  
  -- Component tgt_FIRE_DEPT_insert, Type TARGET
  INSERT INTO db_t_prod_core.fire_dept
              (
                          fire_dept_id,
                          fire_dept_cd,
                          fire_dept_name,
                          cnty_id,
                          host_fire_dept_num,
                          prcs_id,
                          fire_dept_strt_dttm,
                          fire_dept_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT tbl_union.in_fire_dept_id1       AS fire_dept_id,
         tbl_union.in_fire_dept_cd1       AS fire_dept_cd,
         tbl_union.in_fire_dept_name1     AS fire_dept_name,
         tbl_union.in_cnty_id1            AS cnty_id,
         tbl_union.in_host_fire_dept_num1 AS host_fire_dept_num,
         tbl_union.in_prcs_id1            AS prcs_id,
         tbl_union.in_fire_dept_strt_dt1  AS fire_dept_strt_dttm,
         tbl_union.in_fire_dept_end_dt1   AS fire_dept_end_dttm,
         tbl_union.in_edw_strt_dttm1      AS edw_strt_dttm,
         tbl_union.o_edw_end_dttm         AS edw_end_dttm
  FROM
  tbl_UNION;

END;
';