-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PROD_RLTD_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id STRING;
  prcs_id int;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component LKP_BASE_PROD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_base_prod AS
  (
         SELECT prod_id,
                prod_scrp_id,
                prod_sbtype_cd,
                prod_desc,
                prod_name,
                host_prod_id,
                prod_strt_dttm,
                prod_end_dttm,
                prod_pkg_type_cd,
                fincl_prod_ind,
                prod_txt,
                prod_crtn_dt,
                insrnc_type_cd,
                dy_cnt_bss_cd,
                sry_lvl_cd,
                cury_cd,
                insrnc_lob_type_cd,
                prcs_id
         FROM   db_t_prod_core.prod
         WHERE  prod_sbtype_cd=''PRDDOM'' );
  -- Component LKP_PROD1, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prod1 AS
  (
         SELECT prod_id,
                prod_scrp_id,
                prod_sbtype_cd,
                prod_desc,
                prod_name,
                host_prod_id,
                prod_strt_dttm,
                prod_end_dttm,
                prod_pkg_type_cd,
                fincl_prod_ind,
                prod_txt,
                prod_crtn_dt,
                insrnc_type_cd,
                dy_cnt_bss_cd,
                sry_lvl_cd,
                cury_cd,
                prcs_id,
                insrnc_lob_type_cd
         FROM   db_t_prod_core.prod
         WHERE  prod_sbtype_cd=''PLCYTYPE'' );
  -- Component LKP_PROD_RLTD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prod_rltd AS
  (
         SELECT prod_id,
                rltd_prod_id,
                prod_rltnshp_type_cd,
                prod_rltd_strt_dttm,
                prod_rltd_end_dttm,
                prod_rltd_corrl_pct,
                prcs_id
         FROM   db_t_prod_core.prod_rltd );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm =''PROD_RLTNSHP_TYPE''
                --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'',''pctl_relationship.typecode'')
                --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_GW_POLTYPE_CONV, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_poltype_conv AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS parent_prod,
                $2 AS child_prod,
                $3 AS rel_typ,
                $4 AS updatetime,
                $5 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT ltrim(rtrim(gw_product_cd_stg))          AS parent_prod,
                                                ltrim(rtrim(gw_pol_typ_cd_stg))          AS child_prod,
                                                cast(''BI Product Domain'' AS VARCHAR(50))    rel_typ,
                                                CASE
                                                       WHEN gw_poltype_conv.updatetime_stg IS NULL THEN to_date (''1900/01/01'' , ''yyyy/mm/dd'')
                                                       ELSE gw_poltype_conv.updatetime_stg
                                                END AS updatetime
                                         FROM   db_t_prod_stag.gw_poltype_conv
                                         UNION
                                         SELECT ltrim(rtrim(edm_inf_cnt_grp_stg)),
                                                ltrim(rtrim(gw_pol_typ_cd_stg)),
                                                cast(''Enterprise Product Domain'' AS VARCHAR(50)),
                                                CASE
                                                       WHEN gw_poltype_conv.updatetime_stg IS NULL THEN to_date (''1900/01/01'' , ''yyyy/mm/dd'')
                                                       ELSE gw_poltype_conv.updatetime_stg
                                                END AS updatetime
                                         FROM   db_t_prod_stag.gw_poltype_conv ) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
            SELECT    lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                      AS var_typecode,
                      CASE
                                WHEN var_typecode IS NULL THEN sq_gw_poltype_conv.rel_typ
                                ELSE var_typecode
                      END            AS var_typecode_1,
                      var_typecode_1 AS out_typecode,
                      lkp_2.prod_id
                      /* replaced lookup LKP_BASE_PROD */
                      AS var_prod_id,
                      decode ( TRUE ,
                              var_prod_id IS NOT NULL , var_prod_id ,
                              9999 ) AS out_prod_id,
                      lkp_3.prod_id
                      /* replaced lookup LKP_PROD1 */
                      AS var_rltd_prod_id,
                      decode ( TRUE ,
                              var_rltd_prod_id IS NOT NULL , var_rltd_prod_id ,
                              9999 )                                                         AS out_rltd_prod_id,
                      current_timestamp                                                      AS out_prod_rltd_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_prord_rltd_end_dttm,
                      :PRCS_ID                                                               AS prcs_id,
                      to_date ( ''1900/01/01'' , ''yyyy/mm/dd'' )                                AS default_date,
                      sq_gw_poltype_conv.source_record_id,
                      row_number() over (PARTITION BY sq_gw_poltype_conv.source_record_id ORDER BY sq_gw_poltype_conv.source_record_id) AS rnk
            FROM      sq_gw_poltype_conv
            left join lkp_teradata_etl_ref_xlat lkp_1
            ON        lkp_1.src_idntftn_val = sq_gw_poltype_conv.rel_typ
            left join lkp_base_prod lkp_2
            ON        lkp_2.prod_name = ltrim ( rtrim ( sq_gw_poltype_conv.parent_prod ) )
            left join lkp_prod1 lkp_3
            ON        lkp_3.prod_name = ltrim ( rtrim ( decode ( sq_gw_poltype_conv.child_prod ,
                                                                ''BOP'' , ''BUSINESSOWNERS'' ,
                                                                ''PERSUMBRELLA'' , ''PERSONALUMBRELLA'' ,
                                                                ''FARMUMBRELLA'' , ''FarmUmbrella'' ,
                                                                ''FARMOWNERS'' , ''Farmowners'' ,
                                                                sq_gw_poltype_conv.child_prod ) ) ) qualify rnk = 1 );
  -- Component exp_SrcFields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_srcfields AS
  (
         SELECT exp_all_source.out_prod_id             AS in_prod_id,
                exp_all_source.out_rltd_prod_id        AS in_rltd_prod_id,
                exp_all_source.out_typecode            AS in_prod_rltnshp_type_cd,
                exp_all_source.out_prod_rltd_strt_dttm AS in_prod_rltd_strt_dt,
                exp_all_source.out_prord_rltd_end_dttm AS in_prod_rltd_end_dt,
                exp_all_source.prcs_id                 AS in_prcs_id,
                exp_all_source.default_date            AS default_date,
                exp_all_source.source_record_id
         FROM   exp_all_source );
  -- Component LKP_PROD_RLTD_CDC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prod_rltd_cdc AS
  (
            SELECT    lkp.prod_id,
                      lkp.rltd_prod_id,
                      lkp.prod_rltnshp_type_cd,
                      lkp.prod_rltd_end_dttm,
                      lkp.edw_strt_dttm,
                      exp_srcfields.source_record_id,
                      row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.prod_id ASC,lkp.rltd_prod_id ASC,lkp.prod_rltnshp_type_cd ASC,lkp.prod_rltd_end_dttm ASC,lkp.edw_strt_dttm ASC) rnk
            FROM      exp_srcfields
            left join
                      (
                               SELECT   prod_rltd.prod_rltd_end_dttm   AS prod_rltd_end_dttm,
                                        prod_rltd.edw_strt_dttm        AS edw_strt_dttm,
                                        prod_rltd.prod_id              AS prod_id,
                                        prod_rltd.rltd_prod_id         AS rltd_prod_id,
                                        prod_rltd.prod_rltnshp_type_cd AS prod_rltnshp_type_cd
                               FROM     db_t_prod_core.prod_rltd qualify row_number() over(PARTITION BY prod_id,rltd_prod_id,prod_rltnshp_type_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.prod_id = exp_srcfields.in_prod_id
            AND       lkp.rltd_prod_id = exp_srcfields.in_rltd_prod_id
            AND       lkp.prod_rltnshp_type_cd = exp_srcfields.in_prod_rltnshp_type_cd qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     lkp_prod_rltd_cdc.prod_id                                        AS lkp_prod_id,
                        lkp_prod_rltd_cdc.rltd_prod_id                                   AS lkp_rltd_prod_id,
                        lkp_prod_rltd_cdc.prod_rltnshp_type_cd                           AS lkp_prod_rltnshp_type_cd,
                        lkp_prod_rltd_cdc.edw_strt_dttm                                  AS lkp_edw_strt_dttm,
                        exp_srcfields.in_prod_id                                         AS in_prod_id,
                        exp_srcfields.in_rltd_prod_id                                    AS in_rltd_prod_id,
                        exp_srcfields.in_prod_rltnshp_type_cd                            AS in_prod_rltnshp_type_cd,
                        exp_srcfields.in_prod_rltd_strt_dt                               AS in_prod_rltd_strt_dt,
                        exp_srcfields.in_prod_rltd_end_dt                                AS in_prod_rltd_end_dt,
                        exp_srcfields.in_prcs_id                                         AS in_prcs_id,
                        md5 ( ltrim ( rtrim ( exp_srcfields.in_prod_rltd_end_dt ) ) )    AS v_src_md5,
                        md5 ( ltrim ( rtrim ( lkp_prod_rltd_cdc.prod_rltd_end_dttm ) ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''X''
                                                         ELSE ''U''
                                              END
                        END                                                                    AS o_src_tgt,
                        current_timestamp                                                      AS startdate,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS enddate,
                        exp_srcfields.default_date                                             AS default_date,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS busn_end_dt,
                        exp_srcfields.source_record_id
             FROM       exp_srcfields
             inner join lkp_prod_rltd_cdc
             ON         exp_srcfields.source_record_id = lkp_prod_rltd_cdc.source_record_id );
  -- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_cdc_insert as
  SELECT exp_cdc_check.lkp_prod_id              AS lkp_prod_id,
         exp_cdc_check.lkp_rltd_prod_id         AS lkp_rltd_prod_id,
         exp_cdc_check.lkp_prod_rltnshp_type_cd AS lkp_prod_rltnshp_type_cd,
         exp_cdc_check.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_cdc_check.in_prod_id               AS in_prod_id,
         exp_cdc_check.in_rltd_prod_id          AS in_rltd_prod_id,
         exp_cdc_check.in_prod_rltnshp_type_cd  AS in_prod_rltnshp_type_cd,
         exp_cdc_check.in_prod_rltd_strt_dt     AS in_prod_rltd_strt_dt,
         exp_cdc_check.in_prod_rltd_end_dt      AS in_prod_rltd_end_dt,
         exp_cdc_check.in_prcs_id               AS in_prcs_id,
         exp_cdc_check.o_src_tgt                AS o_src_tgt,
         exp_cdc_check.startdate                AS startdate,
         exp_cdc_check.enddate                  AS enddate,
         exp_cdc_check.default_date             AS default_date,
         exp_cdc_check.busn_end_dt              AS busn_end_dt,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''I''
  AND    exp_cdc_check.in_rltd_prod_id != 9999;
  
  -- Component rtr_CDC_Update, Type ROUTER Output Group Update
  create or replace temporary table rtr_cdc_update as
  SELECT exp_cdc_check.lkp_prod_id              AS lkp_prod_id,
         exp_cdc_check.lkp_rltd_prod_id         AS lkp_rltd_prod_id,
         exp_cdc_check.lkp_prod_rltnshp_type_cd AS lkp_prod_rltnshp_type_cd,
         exp_cdc_check.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_cdc_check.in_prod_id               AS in_prod_id,
         exp_cdc_check.in_rltd_prod_id          AS in_rltd_prod_id,
         exp_cdc_check.in_prod_rltnshp_type_cd  AS in_prod_rltnshp_type_cd,
         exp_cdc_check.in_prod_rltd_strt_dt     AS in_prod_rltd_strt_dt,
         exp_cdc_check.in_prod_rltd_end_dt      AS in_prod_rltd_end_dt,
         exp_cdc_check.in_prcs_id               AS in_prcs_id,
         exp_cdc_check.o_src_tgt                AS o_src_tgt,
         exp_cdc_check.startdate                AS startdate,
         exp_cdc_check.enddate                  AS enddate,
         exp_cdc_check.default_date             AS default_date,
         exp_cdc_check.busn_end_dt              AS busn_end_dt,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''U'';
  
  -- Component tgt_PROD_RLTD_Upd_Insert, Type TARGET
  INSERT INTO db_t_prod_core.prod_rltd
              (
                          prod_id,
                          rltd_prod_id,
                          prod_rltnshp_type_cd,
                          prod_rltd_strt_dttm,
                          prod_rltd_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT rtr_cdc_update.lkp_prod_id              AS prod_id,
         rtr_cdc_update.lkp_rltd_prod_id         AS rltd_prod_id,
         rtr_cdc_update.lkp_prod_rltnshp_type_cd AS prod_rltnshp_type_cd,
         rtr_cdc_update.default_date             AS prod_rltd_strt_dttm,
         rtr_cdc_update.enddate                  AS prod_rltd_end_dttm,
         rtr_cdc_update.in_prcs_id               AS prcs_id,
         rtr_cdc_update.startdate                AS edw_strt_dttm,
         rtr_cdc_update.enddate                  AS edw_end_dttm,
         rtr_cdc_update.startdate                AS trans_strt_dttm
  FROM   rtr_cdc_update;
  
  -- Component tgt_PROD_RLTD_NewInsert, Type TARGET
  INSERT INTO db_t_prod_core.prod_rltd
              (
                          prod_id,
                          rltd_prod_id,
                          prod_rltnshp_type_cd,
                          prod_rltd_strt_dttm,
                          prod_rltd_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT rtr_cdc_insert.in_prod_id              AS prod_id,
         rtr_cdc_insert.in_rltd_prod_id         AS rltd_prod_id,
         rtr_cdc_insert.in_prod_rltnshp_type_cd AS prod_rltnshp_type_cd,
         rtr_cdc_insert.default_date            AS prod_rltd_strt_dttm,
         rtr_cdc_insert.busn_end_dt             AS prod_rltd_end_dttm,
         rtr_cdc_insert.in_prcs_id              AS prcs_id,
         rtr_cdc_insert.startdate               AS edw_strt_dttm,
         rtr_cdc_insert.enddate                 AS edw_end_dttm,
         rtr_cdc_insert.default_date            AS trans_strt_dttm
  FROM   rtr_cdc_insert;
  
  -- Component upd_Update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_cdc_update.lkp_prod_id              AS lkp_prod_id3,
                rtr_cdc_update.lkp_rltd_prod_id         AS lkp_rltd_prod_id3,
                rtr_cdc_update.lkp_prod_rltnshp_type_cd AS lkp_prod_rltnshp_type_cd3,
                rtr_cdc_update.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_cdc_update.default_date             AS default_date3,
                1                                       AS update_strategy_action,
				rtr_cdc_update.source_record_id
         FROM   rtr_cdc_update );
  -- Component exp_DateExpiry, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_dateexpiry AS
  (
         SELECT upd_update.lkp_prod_id3                        AS lkp_prod_id3,
                upd_update.lkp_rltd_prod_id3                   AS lkp_rltd_prod_id3,
                upd_update.lkp_prod_rltnshp_type_cd3           AS lkp_prod_rltnshp_type_cd3,
                upd_update.lkp_edw_strt_dttm3                  AS lkp_edw_strt_dttm3,
                dateadd (second,-1, current_timestamp  ) AS enddate,
                upd_update.source_record_id
         FROM   upd_update );
  -- Component tgt_PROD_RLTD_Update, Type TARGET
  merge
  INTO         db_t_prod_core.prod_rltd
  USING        exp_dateexpiry
  ON (
                            prod_rltd.prod_id = exp_dateexpiry.lkp_prod_id3
               AND          prod_rltd.rltd_prod_id = exp_dateexpiry.lkp_rltd_prod_id3
               AND          prod_rltd.prod_rltnshp_type_cd = exp_dateexpiry.lkp_prod_rltnshp_type_cd3
               AND          prod_rltd.edw_strt_dttm = exp_dateexpiry.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    prod_id = exp_dateexpiry.lkp_prod_id3,
         rltd_prod_id = exp_dateexpiry.lkp_rltd_prod_id3,
         prod_rltnshp_type_cd = exp_dateexpiry.lkp_prod_rltnshp_type_cd3,
         edw_strt_dttm = exp_dateexpiry.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_dateexpiry.enddate,
         trans_end_dttm = exp_dateexpiry.enddate;

END;
';