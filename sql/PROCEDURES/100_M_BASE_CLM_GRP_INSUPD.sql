-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_GRP_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_CLM_GRP_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_clm_grp_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''CLM_GRP_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_agmt_clm, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_agmt_clm AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS clm_grp_type_cd,
                $2 AS clm_grp_ctlg_cd,
                $3 AS clm_grp_strt_dt,
                $4 AS updatetime,
                $5 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT ''CLM_GRP_TYPE1''                    AS clm_grp_tye ,
                                                                  cat_pool_ind                       AS clm_grp_catlog ,
                                                                  to_date(''1900/01/01'',''YYYY/MM/DD'') AS clm_grp_st_dt,
                                                                  updatetime
                                                                  /* CAT_POOL_ACT_DT as clm_grp_st_dt */
                                                  FROM            db_t_prod_stag.gw_claims_status_v
                                                  WHERE           cat_pool_ind <> '' '' qualify row_number() over(PARTITION BY cat_pool_ind ORDER BY updatetime DESC) = 1
                                                                  /*SELECT
distinct ''POOL'' as clm_grp_tye ,CAT_POOL_IND as clm_grp_catlog ,CAT_POOL_ACT_DT as clm_grp_st_dt
FROM DB_T_PROD_STAG.GW_CLAIMS_STATUS_V
where CAT_POOL_IND <> ''''*/
                                  ) src ) );
  -- Component exp_pass_frm_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_source AS
  (
            SELECT    ltrim ( rtrim ( sq_agmt_clm.clm_grp_type_cd ) ) AS var_clm_grp_type_cd,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_GRP_TYPE_CD */
                                                  AS out_clm_grp_type_cd,
                      sq_agmt_clm.clm_grp_ctlg_cd AS clm_grp_ctlg_cd,
                      CASE
                                WHEN sq_agmt_clm.clm_grp_strt_dt IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' )
                                ELSE sq_agmt_clm.clm_grp_strt_dt
                      END                                                                    AS o_clm_grp_strt_dt,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_clm_grp_end_dt,
                      NULL                                                                   AS out_parnt_clm_grp_id,
                      $prcs_id                                                               AS prcs_id,
                      sq_agmt_clm.updatetime                                                 AS updatetime,
                      sq_agmt_clm.source_record_id,
                      row_number() over (PARTITION BY sq_agmt_clm.source_record_id ORDER BY sq_agmt_clm.source_record_id) AS rnk
            FROM      sq_agmt_clm
            left join lkp_teradata_etl_ref_xlat_clm_grp_type_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_agmt_clm.clm_grp_type_cd qualify rnk = 1 );
  -- Component LKP_CLM_GRP_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_grp_id AS
  (
            SELECT    lkp.clm_grp_id,
                      lkp.clm_grp_strt_dt,
                      lkp.clm_grp_end_dt,
                      lkp.clm_grp_ctlg_cd,
                      lkp.edw_strt_dttm,
                      exp_pass_frm_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY lkp.clm_grp_id ASC,lkp.clm_grp_type_cd ASC,lkp.clm_grp_strt_dt ASC,lkp.clm_grp_end_dt ASC,lkp.clm_grp_ctlg_cd ASC,lkp.edw_strt_dttm ASC) rnk
            FROM      exp_pass_frm_source
            left join
                      (
                             SELECT clm_grp.clm_grp_id      AS clm_grp_id,
                                    clm_grp.clm_grp_strt_dt AS clm_grp_strt_dt,
                                    clm_grp.clm_grp_end_dt  AS clm_grp_end_dt,
                                    clm_grp.edw_strt_dttm   AS edw_strt_dttm,
                                    clm_grp.edw_end_dttm    AS edw_end_dttm,
                                    clm_grp.clm_grp_type_cd AS clm_grp_type_cd,
                                    clm_grp.clm_grp_ctlg_cd AS clm_grp_ctlg_cd
                             FROM   db_t_prod_core.clm_grp
                             WHERE  clm_grp.edw_end_dttm=cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) ) lkp
            ON        lkp.clm_grp_type_cd = exp_pass_frm_source.out_clm_grp_type_cd
            AND       lkp.clm_grp_ctlg_cd = exp_pass_frm_source.clm_grp_ctlg_cd 
            qualify row_number() over(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY lkp.clm_grp_id ASC,lkp.clm_grp_type_cd ASC,lkp.clm_grp_strt_dt ASC,lkp.clm_grp_end_dt ASC,lkp.clm_grp_ctlg_cd ASC,lkp.edw_strt_dttm ASC) 
                        = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_clm_grp_id.clm_grp_id                AS clm_grp_id,
                        exp_pass_frm_source.out_clm_grp_type_cd  AS clm_grp_type_cd,
                        exp_pass_frm_source.clm_grp_ctlg_cd      AS clm_grp_ctlg_cd,
                        exp_pass_frm_source.o_clm_grp_strt_dt    AS clm_grp_strt_dt,
                        exp_pass_frm_source.out_clm_grp_end_dt   AS clm_grp_end_dt,
                        exp_pass_frm_source.out_parnt_clm_grp_id AS parnt_clm_grp_id,
                        md5 ( to_char ( date_trunc(day, exp_pass_frm_source.o_clm_grp_strt_dt) )
                                   || to_char ( date_trunc(day, exp_pass_frm_source.out_clm_grp_end_dt) ) ) AS v_md5_src,
                        md5 ( to_char ( date_trunc(day, lkp_clm_grp_id.clm_grp_strt_dt) )
                                   || to_char ( date_trunc(day, lkp_clm_grp_id.clm_grp_end_dt) ) ) AS v_md5_lkp,
                        CASE
                                   WHEN v_md5_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_md5_src != v_md5_lkp THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                                    AS o_ins_upd,
                        current_timestamp                                                      AS edw_start_dt,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dt,
                        dateadd(''second'', - 1, current_timestamp)                                  AS edw_end_dt_exp,
                        exp_pass_frm_source.prcs_id                                            AS prcs_id,
                        lkp_clm_grp_id.edw_strt_dttm                                           AS lkp_edw_strt_dt,
                        CASE
                                   WHEN exp_pass_frm_source.updatetime IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )
                                   ELSE exp_pass_frm_source.updatetime
                        END AS out_trans_strt_dttm,
                        exp_pass_frm_source.source_record_id
             FROM       exp_pass_frm_source
             inner join lkp_clm_grp_id
             ON         exp_pass_frm_source.source_record_id = lkp_clm_grp_id.source_record_id );
  -- Component rtr_clm_grp_Insert, Type ROUTER Output Group Insert
  create or replace TEMPORARY TABLE rtr_clm_grp_insert AS
  SELECT exp_data_transformation.clm_grp_id          AS clm_grp_id,
         exp_data_transformation.clm_grp_type_cd     AS clm_grp_type_cd,
         exp_data_transformation.clm_grp_ctlg_cd     AS clm_grp_ctlg_cd,
         exp_data_transformation.clm_grp_strt_dt     AS clm_grp_strt_dt,
         exp_data_transformation.clm_grp_end_dt      AS clm_grp_end_dt,
         exp_data_transformation.parnt_clm_grp_id    AS parnt_clm_grp_id,
         exp_data_transformation.prcs_id             AS prcs_id,
         exp_data_transformation.edw_start_dt        AS edw_strt_dttm,
         exp_data_transformation.edw_end_dt          AS edw_end_dttm,
         exp_data_transformation.edw_end_dt_exp      AS edw_end_dt_exp,
         exp_data_transformation.lkp_edw_strt_dt     AS lkp_edw_strt_dt,
         exp_data_transformation.o_ins_upd           AS o_ins_upd,
         exp_data_transformation.out_trans_strt_dttm AS updatetime,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.o_ins_upd = ''I'';
  
  -- Component rtr_clm_grp_Update, Type ROUTER Output Group Update
  create or replace TEMPORARY TABLE rtr_clm_grp_update AS
  SELECT exp_data_transformation.clm_grp_id          AS clm_grp_id,
         exp_data_transformation.clm_grp_type_cd     AS clm_grp_type_cd,
         exp_data_transformation.clm_grp_ctlg_cd     AS clm_grp_ctlg_cd,
         exp_data_transformation.clm_grp_strt_dt     AS clm_grp_strt_dt,
         exp_data_transformation.clm_grp_end_dt      AS clm_grp_end_dt,
         exp_data_transformation.parnt_clm_grp_id    AS parnt_clm_grp_id,
         exp_data_transformation.prcs_id             AS prcs_id,
         exp_data_transformation.edw_start_dt        AS edw_strt_dttm,
         exp_data_transformation.edw_end_dt          AS edw_end_dttm,
         exp_data_transformation.edw_end_dt_exp      AS edw_end_dt_exp,
         exp_data_transformation.lkp_edw_strt_dt     AS lkp_edw_strt_dt,
         exp_data_transformation.o_ins_upd           AS o_ins_upd,
         exp_data_transformation.out_trans_strt_dttm AS updatetime,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.o_ins_upd = ''U'';
  
  -- Component upd_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_grp_insert.clm_grp_id       AS clm_grp_id1,
                rtr_clm_grp_insert.clm_grp_type_cd  AS clm_grp_type_cd1,
                rtr_clm_grp_insert.clm_grp_strt_dt  AS clm_grp_strt_dt1,
                rtr_clm_grp_insert.clm_grp_end_dt   AS clm_grp_end_dt1,
                rtr_clm_grp_insert.parnt_clm_grp_id AS parnt_clm_grp_id1,
                rtr_clm_grp_insert.clm_grp_ctlg_cd  AS clm_grp_ctlg_cd1,
                rtr_clm_grp_insert.prcs_id          AS prcs_id,
                rtr_clm_grp_insert.edw_strt_dttm    AS edw_strt_dttm,
                rtr_clm_grp_insert.edw_end_dttm     AS edw_end_dttm,
                rtr_clm_grp_insert.edw_end_dt_exp   AS edw_end_dt_exp1,
                rtr_clm_grp_insert.lkp_edw_strt_dt  AS lkp_edw_strt_dt1,
                rtr_clm_grp_insert.updatetime       AS updatetime1,
                0                           AS update_strategy_action,
               rtr_clm_grp_insert.source_record_id
         FROM   rtr_clm_grp_insert );
  -- Component upd_ins_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_grp_update.clm_grp_id       AS clm_grp_id3,
                rtr_clm_grp_update.clm_grp_type_cd  AS clm_grp_type_cd3,
                rtr_clm_grp_update.clm_grp_ctlg_cd  AS clm_grp_ctlg_cd3,
                rtr_clm_grp_update.clm_grp_strt_dt  AS clm_grp_strt_dt3,
                rtr_clm_grp_update.clm_grp_end_dt   AS clm_grp_end_dt3,
                rtr_clm_grp_update.parnt_clm_grp_id AS parnt_clm_grp_id3,
                rtr_clm_grp_update.prcs_id          AS prcs_id3,
                rtr_clm_grp_update.edw_strt_dttm    AS edw_strt_dttm3,
                rtr_clm_grp_update.edw_end_dttm     AS edw_end_dttm3,
                rtr_clm_grp_update.edw_end_dt_exp   AS edw_end_dt_exp3,
                rtr_clm_grp_update.lkp_edw_strt_dt  AS lkp_edw_strt_dt3,
                rtr_clm_grp_update.updatetime       AS updatetime3,
                0                           AS update_strategy_action,
               rtr_clm_grp_update.source_record_id
         FROM   rtr_clm_grp_update );
  -- Component exp_pass_to_tgt_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins_upd AS
  (
         SELECT upd_ins_update.clm_grp_id3       AS clm_grp_id3,
                upd_ins_update.clm_grp_strt_dt3  AS clm_grp_strt_dt3,
                upd_ins_update.clm_grp_end_dt3   AS clm_grp_end_dt3,
                upd_ins_update.parnt_clm_grp_id3 AS parnt_clm_grp_id3,
                upd_ins_update.prcs_id3          AS prcs_id3,
                upd_ins_update.edw_strt_dttm3    AS edw_strt_dttm3,
                upd_ins_update.edw_end_dttm3     AS edw_end_dttm3,
                upd_ins_update.updatetime3       AS updatetime3,
                upd_ins_update.source_record_id
         FROM   upd_ins_update );
  -- Component CLM_GRP_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.clm_grp
              (
                          clm_grp_id,
                          clm_grp_strt_dt,
                          clm_grp_end_dt,
                          parnt_clm_grp_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt_ins_upd.clm_grp_id3       AS clm_grp_id,
         exp_pass_to_tgt_ins_upd.clm_grp_strt_dt3  AS clm_grp_strt_dt,
         exp_pass_to_tgt_ins_upd.clm_grp_end_dt3   AS clm_grp_end_dt,
         exp_pass_to_tgt_ins_upd.parnt_clm_grp_id3 AS parnt_clm_grp_id,
         exp_pass_to_tgt_ins_upd.prcs_id3          AS prcs_id,
         exp_pass_to_tgt_ins_upd.edw_strt_dttm3    AS edw_strt_dttm,
         exp_pass_to_tgt_ins_upd.edw_end_dttm3     AS edw_end_dttm,
         exp_pass_to_tgt_ins_upd.updatetime3       AS trans_strt_dttm
  FROM   exp_pass_to_tgt_ins_upd;
  
  -- Component upd_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_grp_update.clm_grp_id       AS clm_grp_id1,
                rtr_clm_grp_update.clm_grp_type_cd  AS clm_grp_type_cd1,
                rtr_clm_grp_update.clm_grp_strt_dt  AS clm_grp_strt_dt1,
                rtr_clm_grp_update.clm_grp_end_dt   AS clm_grp_end_dt1,
                rtr_clm_grp_update.parnt_clm_grp_id AS parnt_clm_grp_id1,
                rtr_clm_grp_update.clm_grp_ctlg_cd  AS clm_grp_ctlg_cd1,
                rtr_clm_grp_update.prcs_id          AS prcs_id3,
                rtr_clm_grp_update.edw_strt_dttm    AS edw_strt_dttm3,
                rtr_clm_grp_update.edw_end_dttm     AS edw_end_dttm3,
                rtr_clm_grp_update.edw_end_dt_exp   AS edw_end_dt_exp3,
                rtr_clm_grp_update.lkp_edw_strt_dt  AS lkp_edw_strt_dt3,
                rtr_clm_grp_update.updatetime       AS updatetime3,
                1                           AS update_strategy_action,
               rtr_clm_grp_update.source_record_id
         FROM   rtr_clm_grp_update );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_insert.clm_grp_type_cd1  AS clm_grp_type_cd1,
                upd_insert.clm_grp_strt_dt1  AS clm_grp_strt_dt1,
                upd_insert.clm_grp_end_dt1   AS clm_grp_end_dt1,
                upd_insert.parnt_clm_grp_id1 AS parnt_clm_grp_id1,
                upd_insert.clm_grp_ctlg_cd1  AS clm_grp_ctlg_cd1,
                upd_insert.prcs_id           AS prcs_id,
                upd_insert.edw_strt_dttm     AS edw_strt_dttm,
                upd_insert.edw_end_dttm      AS edw_end_dttm,
                seqtrans.NEXTVAL             AS NEXTVAL,
                upd_insert.updatetime1       AS updatetime1,
                upd_insert.source_record_id
         FROM   upd_insert );
  -- Component CLM_GRP, Type TARGET
  INSERT INTO db_t_prod_core.clm_grp
              (
                          clm_grp_id,
                          clm_grp_type_cd,
                          clm_grp_strt_dt,
                          clm_grp_end_dt,
                          parnt_clm_grp_id,
                          clm_grp_ctlg_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT   row_number() over (ORDER BY 1)        AS clm_grp_id,
           exp_pass_to_tgt_ins.clm_grp_type_cd1  AS clm_grp_type_cd,
           exp_pass_to_tgt_ins.clm_grp_strt_dt1  AS clm_grp_strt_dt,
           exp_pass_to_tgt_ins.clm_grp_end_dt1   AS clm_grp_end_dt,
           exp_pass_to_tgt_ins.parnt_clm_grp_id1 AS parnt_clm_grp_id,
           exp_pass_to_tgt_ins.clm_grp_ctlg_cd1  AS clm_grp_ctlg_cd,
           exp_pass_to_tgt_ins.prcs_id           AS prcs_id,
           exp_pass_to_tgt_ins.edw_strt_dttm     AS edw_strt_dttm,
           exp_pass_to_tgt_ins.edw_end_dttm      AS edw_end_dttm,
           exp_pass_to_tgt_ins.updatetime1       AS trans_strt_dttm
  FROM     exp_pass_to_tgt_ins;
  
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT upd_update.clm_grp_id1                     AS clm_grp_id1,
                upd_update.edw_end_dt_exp3                 AS edw_end_dt_exp3,
                upd_update.lkp_edw_strt_dt3                AS lkp_edw_strt_dt3,
                dateadd(''second'', - 1, upd_update.updatetime3) AS trans_end_dttm,
                upd_update.source_record_id
         FROM   upd_update );
  -- Component CLM_GRP_upd, Type TARGET
  merge
  INTO         db_t_prod_core.clm_grp
  USING        exp_pass_to_tgt_upd
  ON (
                            clm_grp.clm_grp_id = exp_pass_to_tgt_upd.clm_grp_id1
               AND          clm_grp.edw_strt_dttm = exp_pass_to_tgt_upd.lkp_edw_strt_dt3)
  WHEN matched THEN
  UPDATE
  SET    clm_grp_id = exp_pass_to_tgt_upd.clm_grp_id1,
         edw_strt_dttm = exp_pass_to_tgt_upd.lkp_edw_strt_dt3,
         edw_end_dttm = exp_pass_to_tgt_upd.edw_end_dt_exp3,
         trans_end_dttm = exp_pass_to_tgt_upd.trans_end_dttm;

END;
';