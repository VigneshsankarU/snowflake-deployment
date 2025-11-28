-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_GW_PREM_TRANS_PLCY_INFO("WORKLET_NAME" VARCHAR)
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

  -- Component SQ_GW_PREMIUM_TRANS, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_premium_trans AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS policy_nbr,
                $2 AS service_center,
                $3 AS policy_eff_dt,
                $4 AS policy_exp_dt,
                $5 AS policy_term_nbr,
                $6 AS policy_model_nbr,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT gw_premium_trans.policy_nbr,
                                                gw_premium_trans.service_center,
                                                gw_premium_trans.policy_eff_dt,
                                                gw_premium_trans.policy_exp_dt,
                                                gw_premium_trans.policy_term_nbr,
                                                gw_premium_trans.policy_model_nbr
                                         FROM   db_t_prod_stag.gw_premium_trans
                                         WHERE  ltrim(rtrim(batch_type))=''GL ROW'' ) src ) );
  -- Component agg, Type AGGREGATOR
  CREATE
  OR
  replace TEMPORARY TABLE agg AS
  (
           SELECT   sq_gw_premium_trans.policy_nbr            AS policy_nbr,
                    min(sq_gw_premium_trans.service_center)   AS service_center,
                    min(sq_gw_premium_trans.policy_eff_dt)    AS policy_eff_dt,
                    min(sq_gw_premium_trans.policy_exp_dt)    AS policy_exp_dt,
                    sq_gw_premium_trans.policy_term_nbr       AS policy_term_nbr,
                    sq_gw_premium_trans.policy_model_nbr      AS policy_model_nbr,
                    min(sq_gw_premium_trans.source_record_id) AS source_record_id
           FROM     sq_gw_premium_trans
           GROUP BY sq_gw_premium_trans.policy_nbr,
                    sq_gw_premium_trans.policy_term_nbr,
                    sq_gw_premium_trans.policy_model_nbr );
  -- Component exp_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through AS
  (
         SELECT agg.policy_nbr       AS policy_nbr,
                agg.service_center   AS service_center,
                agg.policy_eff_dt    AS policy_eff_dt,
                agg.policy_exp_dt    AS policy_exp_dt,
                agg.policy_term_nbr  AS policy_term_nbr,
                agg.policy_model_nbr AS policy_model_nbr,
                ''DB2''                AS src_sys_cd,
                agg.source_record_id
         FROM   agg );
  -- Component LKP_TGT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_tgt AS
  (
            SELECT    lkp.plcy_info_id,
                      lkp.plcy_num,
                      lkp.plcy_term_num,
                      lkp.plcy_modl_num,
                      lkp.plcy_eff_dt,
                      lkp.plcy_expn_dt,
                      lkp.srvc_ctr,
                      lkp.prcs_id,
                      lkp.src_sys_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_pass_through.source_record_id,
                      row_number() over(PARTITION BY exp_pass_through.source_record_id ORDER BY lkp.plcy_info_id ASC,lkp.plcy_num ASC,lkp.plcy_term_num ASC,lkp.plcy_modl_num ASC,lkp.plcy_eff_dt ASC,lkp.plcy_expn_dt ASC,lkp.srvc_ctr ASC,lkp.prcs_id ASC,lkp.src_sys_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_pass_through
            left join
                      (
                             SELECT gw_prem_trans_plcy_info.plcy_info_id  AS plcy_info_id,
                                    gw_prem_trans_plcy_info.plcy_eff_dt   AS plcy_eff_dt,
                                    gw_prem_trans_plcy_info.plcy_expn_dt  AS plcy_expn_dt,
                                    gw_prem_trans_plcy_info.srvc_ctr      AS srvc_ctr,
                                    gw_prem_trans_plcy_info.prcs_id       AS prcs_id,
                                    gw_prem_trans_plcy_info.src_sys_cd    AS src_sys_cd,
                                    gw_prem_trans_plcy_info.edw_strt_dttm AS edw_strt_dttm,
                                    gw_prem_trans_plcy_info.edw_end_dttm  AS edw_end_dttm,
                                    gw_prem_trans_plcy_info.plcy_num      AS plcy_num,
                                    gw_prem_trans_plcy_info.plcy_term_num AS plcy_term_num,
                                    gw_prem_trans_plcy_info.plcy_modl_num AS plcy_modl_num
                             FROM   db_t_prod_comn.gw_prem_trans_plcy_info
                             WHERE  gw_prem_trans_plcy_info.edw_end_dttm =to_date(''12/31/9999'',''MM/DD/YYYY'') ) lkp
            ON        lkp.plcy_num = exp_pass_through.policy_nbr
            AND       lkp.plcy_term_num = exp_pass_through.policy_term_nbr
            AND       lkp.plcy_modl_num = exp_pass_through.policy_model_nbr qualify rnk = 1 );
  -- Component exp_flag_check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_flag_check AS
  (
             SELECT     lkp_tgt.plcy_info_id                    AS lkp_plcy_info_id,
                        lkp_tgt.plcy_num                        AS lkp_plcy_num,
                        lkp_tgt.plcy_term_num                   AS lkp_plcy_term_num,
                        lkp_tgt.plcy_modl_num                   AS lkp_plcy_modl_num,
                        lkp_tgt.plcy_eff_dt                     AS lkp_plcy_eff_dt,
                        lkp_tgt.plcy_expn_dt                    AS lkp_plcy_expn_dt,
                        lkp_tgt.srvc_ctr                        AS lkp_srvc_ctr,
                        lkp_tgt.prcs_id                         AS lkp_prcs_id,
                        lkp_tgt.src_sys_cd                      AS lkp_src_sys_cd,
                        lkp_tgt.edw_strt_dttm                   AS lkp_edw_strt_dttm,
                        lkp_tgt.edw_end_dttm                    AS lkp_edw_end_dttm,
                        exp_pass_through.policy_nbr             AS policy_nbr,
                        exp_pass_through.service_center         AS service_center,
                        exp_pass_through.policy_eff_dt          AS policy_eff_dt,
                        exp_pass_through.policy_exp_dt          AS policy_exp_dt,
                        exp_pass_through.policy_term_nbr        AS policy_term_nbr,
                        exp_pass_through.policy_model_nbr       AS policy_model_nbr,
                        exp_pass_through.src_sys_cd             AS src_sys_cd,
                        current_timestamp                       AS start_ts,
                        to_date ( ''12/31/9999'' , ''MM/DD/YYYY'' ) AS end_ts,
                        :PRCS_ID                                AS prcs_id,
                        CASE
                                   WHEN lkp_tgt.plcy_info_id IS NULL
                                   OR         to_char ( lkp_tgt.plcy_info_id ) = '''' THEN 1
                                   ELSE
                                              CASE
                                                         WHEN
                                                                    CASE
                                                                               WHEN lkp_tgt.plcy_eff_dt IS NULL THEN to_date ( ''9999-12-31'' , ''YYYY-MM-DD'' )
                                                                               ELSE lkp_tgt.plcy_eff_dt
                                                                    END <>
                                                                    CASE
                                                                               WHEN exp_pass_through.policy_eff_dt IS NULL THEN to_date ( ''9999-12-31'' , ''YYYY-MM-DD'' )
                                                                               ELSE exp_pass_through.policy_eff_dt
                                                                    END
                                                         OR
                                                                    CASE
                                                                               WHEN lkp_tgt.plcy_expn_dt IS NULL THEN to_date ( ''9999-12-31'' , ''YYYY-MM-DD'' )
                                                                               ELSE lkp_tgt.plcy_expn_dt
                                                                    END <>
                                                                    CASE
                                                                               WHEN exp_pass_through.policy_exp_dt IS NULL THEN to_date ( ''9999-12-31'' , ''YYYY-MM-DD'' )
                                                                               ELSE exp_pass_through.policy_exp_dt
                                                                    END
                                                         OR
                                                                    CASE
                                                                               WHEN lkp_tgt.srvc_ctr IS NULL THEN 0
                                                                               ELSE lkp_tgt.srvc_ctr
                                                                    END <>
                                                                    CASE
                                                                               WHEN exp_pass_through.service_center IS NULL THEN 0
                                                                               ELSE exp_pass_through.service_center
                                                                    END THEN 0
                                                         ELSE 2
                                              END
                        END    AS upd_sw,
                        upd_sw AS out_upd_sw,
                        exp_pass_through.source_record_id
             FROM       exp_pass_through
             inner join lkp_tgt
             ON         exp_pass_through.source_record_id = lkp_tgt.source_record_id );
  -- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_ins_upd_insert AS
  SELECT exp_flag_check.policy_nbr        AS policy_nbr,
         exp_flag_check.service_center    AS service_center,
         exp_flag_check.policy_eff_dt     AS policy_eff_dt,
         exp_flag_check.policy_exp_dt     AS policy_exp_dt,
         exp_flag_check.policy_term_nbr   AS policy_term_nbr,
         exp_flag_check.policy_model_nbr  AS policy_model_nbr,
         exp_flag_check.src_sys_cd        AS src_sys_cd,
         exp_flag_check.start_ts          AS start_ts,
         exp_flag_check.end_ts            AS end_ts,
         exp_flag_check.out_upd_sw        AS out_upd_sw,
         exp_flag_check.prcs_id           AS prcs_id,
         exp_flag_check.lkp_plcy_info_id  AS lkp_plcy_info_id,
         exp_flag_check.lkp_plcy_num      AS lkp_plcy_num,
         exp_flag_check.lkp_plcy_term_num AS lkp_plcy_term_num,
         exp_flag_check.lkp_plcy_modl_num AS lkp_plcy_modl_num,
         exp_flag_check.lkp_plcy_eff_dt   AS lkp_plcy_eff_dt,
         exp_flag_check.lkp_plcy_expn_dt  AS lkp_plcy_expn_dt,
         exp_flag_check.lkp_srvc_ctr      AS lkp_srvc_ctr,
         exp_flag_check.lkp_prcs_id       AS lkp_prcs_id,
         exp_flag_check.lkp_src_sys_cd    AS lkp_src_sys_cd,
         exp_flag_check.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_flag_check.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_flag_check.source_record_id
  FROM   exp_flag_check
  WHERE  exp_flag_check.out_upd_sw = 1;
  
  -- Component rtr_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
  create or replace TEMPORARY TABLE rtr_ins_upd_update AS
  SELECT exp_flag_check.policy_nbr        AS policy_nbr,
         exp_flag_check.service_center    AS service_center,
         exp_flag_check.policy_eff_dt     AS policy_eff_dt,
         exp_flag_check.policy_exp_dt     AS policy_exp_dt,
         exp_flag_check.policy_term_nbr   AS policy_term_nbr,
         exp_flag_check.policy_model_nbr  AS policy_model_nbr,
         exp_flag_check.src_sys_cd        AS src_sys_cd,
         exp_flag_check.start_ts          AS start_ts,
         exp_flag_check.end_ts            AS end_ts,
         exp_flag_check.out_upd_sw        AS out_upd_sw,
         exp_flag_check.prcs_id           AS prcs_id,
         exp_flag_check.lkp_plcy_info_id  AS lkp_plcy_info_id,
         exp_flag_check.lkp_plcy_num      AS lkp_plcy_num,
         exp_flag_check.lkp_plcy_term_num AS lkp_plcy_term_num,
         exp_flag_check.lkp_plcy_modl_num AS lkp_plcy_modl_num,
         exp_flag_check.lkp_plcy_eff_dt   AS lkp_plcy_eff_dt,
         exp_flag_check.lkp_plcy_expn_dt  AS lkp_plcy_expn_dt,
         exp_flag_check.lkp_srvc_ctr      AS lkp_srvc_ctr,
         exp_flag_check.lkp_prcs_id       AS lkp_prcs_id,
         exp_flag_check.lkp_src_sys_cd    AS lkp_src_sys_cd,
         exp_flag_check.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_flag_check.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_flag_check.source_record_id
  FROM   exp_flag_check
  WHERE  exp_flag_check.out_upd_sw = 0;
  
  -- Component exp_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_upd AS
  (
         SELECT rtr_ins_upd_update.lkp_plcy_info_id AS lkp_plcy_info_id3,
                rtr_ins_upd_update.lkp_srvc_ctr     AS service_center3,
                rtr_ins_upd_update.lkp_plcy_eff_dt  AS policy_eff_dt3,
                rtr_ins_upd_update.lkp_plcy_expn_dt AS policy_exp_dt3,
                current_timestamp                   AS end_ts,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT rtr_ins_upd_update.lkp_plcy_info_id AS lkp_plcy_info_id3,
                rtr_ins_upd_update.policy_nbr       AS policy_nbr3,
                rtr_ins_upd_update.service_center   AS service_center3,
                rtr_ins_upd_update.policy_eff_dt    AS policy_eff_dt3,
                rtr_ins_upd_update.policy_exp_dt    AS policy_exp_dt3,
                rtr_ins_upd_update.policy_term_nbr  AS policy_term_nbr3,
                rtr_ins_upd_update.policy_model_nbr AS policy_model_nbr3,
                rtr_ins_upd_update.src_sys_cd       AS src_sys_cd3,
                rtr_ins_upd_update.start_ts         AS start_ts3,
                rtr_ins_upd_update.end_ts           AS end_ts3,
                rtr_ins_upd_update.prcs_id          AS prcs_id3,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component GW_PREM_TRANS_PLCY_INFO_ins, Type TARGET
  INSERT INTO db_t_prod_comn.gw_prem_trans_plcy_info
              (
                          plcy_info_id,
                          plcy_num,
                          plcy_term_num,
                          plcy_modl_num,
                          plcy_eff_dt,
                          plcy_expn_dt,
                          srvc_ctr,
                          prcs_id,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT   row_number() over (ORDER BY 1)      AS plcy_info_id,
           rtr_ins_upd_insert.policy_nbr       AS plcy_num,
           rtr_ins_upd_insert.policy_term_nbr  AS plcy_term_num,
           rtr_ins_upd_insert.policy_model_nbr AS plcy_modl_num,
           rtr_ins_upd_insert.policy_eff_dt    AS plcy_eff_dt,
           rtr_ins_upd_insert.policy_exp_dt    AS plcy_expn_dt,
           rtr_ins_upd_insert.service_center   AS srvc_ctr,
           rtr_ins_upd_insert.prcs_id          AS prcs_id,
           rtr_ins_upd_insert.src_sys_cd       AS src_sys_cd,
           rtr_ins_upd_insert.start_ts         AS edw_strt_dttm,
           rtr_ins_upd_insert.end_ts           AS edw_end_dttm
  FROM     rtr_ins_upd_insert;
  
  -- Component upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_upd.lkp_plcy_info_id3 AS lkp_plcy_info_id3,
                exp_upd.service_center3   AS service_center3,
                exp_upd.policy_eff_dt3    AS policy_eff_dt3,
                exp_upd.policy_exp_dt3    AS policy_exp_dt3,
                exp_upd.end_ts            AS end_ts,
                1                         AS update_strategy_action
         FROM   exp_upd );
  -- Component upd_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_ins_upd.lkp_plcy_info_id3 AS lkp_plcy_info_id3,
                exp_ins_upd.policy_nbr3       AS policy_nbr3,
                exp_ins_upd.service_center3   AS service_center3,
                exp_ins_upd.policy_eff_dt3    AS policy_eff_dt3,
                exp_ins_upd.policy_exp_dt3    AS policy_exp_dt3,
                exp_ins_upd.policy_term_nbr3  AS policy_term_nbr3,
                exp_ins_upd.policy_model_nbr3 AS policy_model_nbr3,
                exp_ins_upd.src_sys_cd3       AS src_sys_cd3,
                exp_ins_upd.start_ts3         AS start_ts3,
                exp_ins_upd.end_ts3           AS end_ts3,
                exp_ins_upd.prcs_id3          AS prcs_id3,
                0                             AS update_strategy_action
         FROM   exp_ins_upd );
  -- Component GW_PREM_TRANS_PLCY_INFO_upd, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_comn.gw_prem_trans_plcy_info
  USING        upd
  ON (
                            update_strategy_action = 1
               AND          gw_prem_trans_plcy_info.plcy_info_id = upd.lkp_plcy_info_id3)
  WHEN matched THEN
  UPDATE
  SET    plcy_eff_dt = upd.policy_eff_dt3,
         plcy_expn_dt = upd.policy_exp_dt3,
         srvc_ctr = upd.service_center3,
         edw_end_dttm = upd.end_ts ;
  
  -- Component GW_PREM_TRANS_PLCY_INFO_ins_upd, Type TARGET
  INSERT INTO db_t_prod_comn.gw_prem_trans_plcy_info
              (
                          plcy_info_id,
                          plcy_num,
                          plcy_term_num,
                          plcy_modl_num,
                          plcy_eff_dt,
                          plcy_expn_dt,
                          srvc_ctr,
                          prcs_id,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT upd_ins_upd.lkp_plcy_info_id3 AS plcy_info_id,
         upd_ins_upd.policy_nbr3       AS plcy_num,
         upd_ins_upd.policy_term_nbr3  AS plcy_term_num,
         upd_ins_upd.policy_model_nbr3 AS plcy_modl_num,
         upd_ins_upd.policy_eff_dt3    AS plcy_eff_dt,
         upd_ins_upd.policy_exp_dt3    AS plcy_expn_dt,
         upd_ins_upd.service_center3   AS srvc_ctr,
         upd_ins_upd.prcs_id3          AS prcs_id,
         upd_ins_upd.src_sys_cd3       AS src_sys_cd,
         upd_ins_upd.start_ts3         AS edw_strt_dttm,
         upd_ins_upd.end_ts3           AS edw_end_dttm
  FROM   upd_ins_upd;

END;
';