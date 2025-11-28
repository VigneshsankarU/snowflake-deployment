-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MFG_HOME_PRK_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);



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
  -- Component SQ_pcx_manhomeparkcode_alfa, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_manhomeparkcode_alfa AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS publicid,
                $2 AS code,
                $3 AS name,
                $4 AS county,
                $5 AS state,
                $6 AS country,
                $7 AS retired,
                $8 AS source_record_id
         FROM   (
                                SELECT          src.*,
                                                row_number() over (ORDER BY 1) AS source_record_id
                                FROM            ( select pcx_manhomeparkcode_alfa.publicid_stg, pcx_manhomeparkcode_alfa.code_stg, pcx_manhomeparkcode_alfa.name_stg, pcx_countycode_alfa.name_stg AS county_name_stg, pctl_jurisdiction.typecode_stg AS statetypecode_stg, ''US'' AS country_stg, pcx_manhomeparkcode_alfa.retired_stg 
												FROM 	db_t_prod_stag.pcx_manhomeparkcode_alfa
												left outer join db_t_prod_stag.pcx_countycode_alfa
												ON              pcx_manhomeparkcode_alfa.countycode_alfa_stg = pcx_countycode_alfa.id_stg
												left outer join db_t_prod_stag.pctl_jurisdiction
												ON              pctl_jurisdiction.id_stg = pcx_countycode_alfa.state_stg WHERE pcx_manhomeparkcode_alfa.updatetime_stg > (:start_dttm)
												AND             pcx_manhomeparkcode_alfa.updatetime_stg <= (:end_dttm) 
												) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT    sq_pcx_manhomeparkcode_alfa.code AS in_mfg_home_prk_cd,
                      sq_pcx_manhomeparkcode_alfa.name AS in_mfg_home_prk_name,
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
                      END                                                                    AS in_cnty_id,
                      :prcs_id                                                               AS in_prcs_id,
                      current_timestamp                                                      AS in_edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                      sq_pcx_manhomeparkcode_alfa.retired                                    AS retired,
                      sq_pcx_manhomeparkcode_alfa.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_manhomeparkcode_alfa.source_record_id ORDER BY sq_pcx_manhomeparkcode_alfa.source_record_id) AS rnk
            FROM      sq_pcx_manhomeparkcode_alfa
            left join lkp_ctry lkp_1
            ON        lkp_1.geogrcl_area_shrt_name = sq_pcx_manhomeparkcode_alfa.country
            left join lkp_terr lkp_2
            ON        lkp_2.ctry_id = var_ctry_id
            AND       lkp_2.geogrcl_area_shrt_name = sq_pcx_manhomeparkcode_alfa.state
            left join lkp_county lkp_3
            ON        lkp_3.terr_id = var_terr_id
            AND       lkp_3.geogrcl_area_shrt_name = sq_pcx_manhomeparkcode_alfa.county
            left join lkp_county lkp_4
            ON        lkp_4.terr_id = var_terr_id
            AND       lkp_4.geogrcl_area_shrt_name = sq_pcx_manhomeparkcode_alfa.county qualify rnk = 1 );
  -- Component LKP_MFG_HOME_PRK_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_mfg_home_prk_id AS
  (
            SELECT    lkp.mfg_home_prk_name,
                      exp_all_source.source_record_id,
                      row_number() over(PARTITION BY exp_all_source.source_record_id ORDER BY lkp.mfg_home_prk_name ASC) rnk
            FROM      exp_all_source
            left join
                      (
                               SELECT   mfg_home_prk.mfg_home_prk_name AS mfg_home_prk_name,
                                        mfg_home_prk.mfg_home_prk_cd   AS mfg_home_prk_cd,
                                        mfg_home_prk.cnty_id           AS cnty_id
                               FROM     db_t_prod_core.mfg_home_prk qualify row_number() over( PARTITION BY mfg_home_prk.mfg_home_prk_cd, mfg_home_prk.cnty_id ORDER BY mfg_home_prk.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.mfg_home_prk_cd = exp_all_source.in_mfg_home_prk_cd
            AND       lkp.cnty_id = exp_all_source.in_cnty_id qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     seq_mfg_home_prk_id.NEXTVAL                                       AS in_mfg_home_prk_id,
                        exp_all_source.in_mfg_home_prk_cd                                 AS in_mfg_home_prk_cd,
                        exp_all_source.in_mfg_home_prk_name                               AS in_mfg_home_prk_name,
                        exp_all_source.in_cnty_id                                         AS in_cnty_id,
                        exp_all_source.in_prcs_id                                         AS in_prcs_id,
                        exp_all_source.in_edw_strt_dttm                                   AS in_edw_strt_dttm,
                        exp_all_source.in_edw_end_dttm                                    AS in_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( exp_all_source.in_mfg_home_prk_name ) ) )   AS v_src_md5,
                        md5 ( ltrim ( rtrim ( lkp_mfg_home_prk_id.mfg_home_prk_name ) ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 != v_tgt_md5 THEN ''U''
                                                         ELSE v_tgt_md5 --$3
                                              END
                        END                    AS o_cdc_check,
                        exp_all_source.retired AS retired,
                        exp_all_source.source_record_id
             FROM       exp_all_source
             inner join lkp_mfg_home_prk_id
             ON         exp_all_source.source_record_id = lkp_mfg_home_prk_id.source_record_id );
  -- Component rtr_mfg_home_prk_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_mfg_home_prk_insert as
  SELECT exp_cdc_check.in_mfg_home_prk_id   AS in_mfg_home_prk_id,
         exp_cdc_check.in_mfg_home_prk_cd   AS in_mfg_home_prk_cd,
         exp_cdc_check.in_mfg_home_prk_name AS in_mfg_home_prk_name,
         exp_cdc_check.in_cnty_id           AS in_cnty_id,
         exp_cdc_check.in_prcs_id           AS in_prcs_id,
         exp_cdc_check.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_cdc_check.o_cdc_check          AS o_cdc_check,
         exp_cdc_check.retired              AS retired,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_cdc_check = ''I''  --   - - OR     exp_cdc_check.o_cdc_check = ''U''  
--  AND    lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) 
 ;
  
  -- Component upd_INSERT, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_mfg_home_prk_insert.in_mfg_home_prk_id   AS in_mfg_home_prk_id1,
                rtr_mfg_home_prk_insert.in_mfg_home_prk_cd   AS in_mfg_home_prk_cd1,
                rtr_mfg_home_prk_insert.in_mfg_home_prk_name AS in_mfg_home_prk_name1,
                rtr_mfg_home_prk_insert.in_cnty_id           AS in_cnty_id1,
                rtr_mfg_home_prk_insert.in_prcs_id           AS in_prcs_id1,
                rtr_mfg_home_prk_insert.in_edw_strt_dttm     AS in_edw_strt_dttm1,
                rtr_mfg_home_prk_insert.in_edw_end_dttm      AS in_edw_end_dttm1,
                rtr_mfg_home_prk_insert.retired              AS retired1,
                0                                            AS update_strategy_action,
				rtr_mfg_home_prk_insert.source_record_id
         FROM   rtr_mfg_home_prk_insert );
  -- Component exp_INSERT, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert AS
  (
         SELECT upd_insert.in_mfg_home_prk_id1   AS in_mfg_home_prk_id1,
                upd_insert.in_mfg_home_prk_cd1   AS in_mfg_home_prk_cd1,
                upd_insert.in_mfg_home_prk_name1 AS in_mfg_home_prk_name1,
                upd_insert.in_cnty_id1           AS in_cnty_id1,
                upd_insert.in_prcs_id1           AS in_prcs_id1,
                upd_insert.in_edw_strt_dttm1     AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_insert.retired1 != 0 THEN current_timestamp
                       ELSE upd_insert.in_edw_end_dttm1
                END AS o_edw_end_dttm,
                upd_insert.source_record_id
         FROM   upd_insert );
  -- Component TGT_MFG_HOME_PRK_INS, Type TARGET
  INSERT INTO db_t_prod_core.mfg_home_prk
              (
                          mfg_home_prk_id,
                          mfg_home_prk_cd,
                          mfg_home_prk_name,
                          cnty_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_insert.in_mfg_home_prk_id1   AS mfg_home_prk_id,
         exp_insert.in_mfg_home_prk_cd1   AS mfg_home_prk_cd,
         exp_insert.in_mfg_home_prk_name1 AS mfg_home_prk_name,
         exp_insert.in_cnty_id1           AS cnty_id,
         exp_insert.in_prcs_id1           AS prcs_id,
         exp_insert.in_edw_strt_dttm1     AS edw_strt_dttm,
         exp_insert.o_edw_end_dttm        AS edw_end_dttm
  FROM   exp_insert;

END;
';