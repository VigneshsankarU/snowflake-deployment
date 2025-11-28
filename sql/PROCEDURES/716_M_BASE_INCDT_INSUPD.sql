-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INCDT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);



  -- Component sq_cc_catastrophe, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_catastrophe AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS src_catastrophenumber,
                $2  AS src_name,
                $3  AS src_ev_act_type_cd,
                $4  AS src_catastrophevalid_from,
                $5  AS src_catastrophevalid_to,
                $6  AS src_retired,
                $7  AS src_incdt_ev_id,
                $8  AS tgt_incdt_ev_id,
                $9  AS tgt_name,
                $10 AS tgt_catastrophenumber,
                $11 AS tgt_edw_strt_dttm,
                $12 AS tgt_edw_end_dttm,
                $13 AS src_md5,
                $14 AS tgt_md5,
                $15 AS ins_upd_flg,
                $16 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH intrm_src AS
                                  (
                                                  SELECT DISTINCT catastrophenumber_stg          AS catastrophenumber,
                                                                  cccat.name_stg                 AS nm,
                                                                  typeid.typecode_stg            AS ev_act_type_code,
                                                                  cccat.catastrophevalidfrom_stg AS catastrophevalidfrom,
                                                                  cccat.catastrophevalidto_stg   AS catastrophevalidto,
                                                                  cccat.retired_stg              AS retired
                                                  FROM            db_t_prod_stag.cc_catastrophe cccat
                                                  inner join      db_t_prod_stag.cctl_catastrophetype typeid
                                                  ON              typeid.id_stg = cccat.type_stg
                                                  AND             cccat.updatetime_stg>(:START_DTTM)
                                                  AND             cccat.updatetime_stg <=(:END_DTTM) )
                  SELECT DISTINCT catastrophenumber,
                                  nm, (
                                  CASE
                                                  WHEN (
                                                                                  ev_act_type_cd.tgt_idntftn_val IS NULL) THEN ''UNK''
                                                  ELSE ev_act_type_cd.tgt_idntftn_val
                                  END) AS acty_subtype_cd,
                                  catastrophevalidfrom,
                                  coalesce(catastrophevalidto,''9999-12-31 23:59:59.999999'') AS catastrophevalidto,
                                  retired,
                                  ev_lkp.ev_id AS src_ev_id,
                                  tgt_lkp_incdt.incdt_ev_id,
                                  tgt_lkp_incdt.ctstrph_name,
                                  tgt_lkp_incdt.host_ctstrph_ref_num,
                                  tgt_lkp_incdt.edw_strt_dttm,
                                  tgt_lkp_incdt.edw_end_dttm,
                                  /* Source data */
                                  cast(trim(nm)
                                                  ||trim(catastrophenumber)
                                                  ||cast(catastrophevalidfrom AS DATE)
                                                  ||cast (catastrophevalidto AS  CHAR(10) ) AS VARCHAR(1100)) AS src_md5,
                                  /*EIM-47587*/
                                  /* Target data */
                                  cast(trim(ctstrph_name)
                                                  ||trim(host_ctstrph_ref_num)
                                                  ||cast(tgt_lkp_incdt.incdt_strt_dttm AS DATE)
                                                  || cast(tgt_lkp_incdt.incdt_end_dttm AS CHAR(10)) AS VARCHAR(1100)) AS tgt_md5,
                                  /*EIM-47587*/
                                  /* Flag */
                                  CASE
                                                  WHEN tgt_md5 IS NULL THEN ''I''
                                                  WHEN src_md5<>tgt_md5 THEN ''U''
                                                  ELSE ''R''
                                  END AS ins_upd_flag
                  FROM            (
                                         SELECT catastrophenumber,
                                                nm,
                                                ev_act_type_code, 
                                                CASE
                                                       WHEN 
                                                                     catastrophevalidfrom IS NULL THEN to_date(''01/01/1900'',''MM/DD/YYYY'')
                                                       ELSE catastrophevalidfrom
                                                END                                             AS catastrophevalidfrom,
                                                TO_CHAR(
													CAST(catastrophevalidto AS TIMESTAMP),
													''YYYY-MM-DD HH24:MI:SS.FF3''
												) AS catastrophevalidto,
                                                retired
                                         FROM   intrm_src )src
                                  /*********************************** EV_ACT_TYPE_CD *******************************************************************/
                  left outer join
                                  (
                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                         FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) ev_act_type_cd
                  ON              ev_act_type_cd.src_idntftn_val=src.ev_act_type_code
                                  /*********************************** DB_T_PROD_CORE.EV ****************************************************************************/
                  left outer join
                                  (
                                           SELECT   ev.ev_id            AS ev_id,
                                                    ev.ev_desc          AS ev_desc,
                                                    ev.ev_strt_dttm     AS ev_strt_dttm,
                                                    ev.ev_end_dttm      AS ev_end_dttm,
                                                    ev.ev_rsn_cd        AS ev_rsn_cd,
                                                    ev.agmt_id          AS agmt_id,
                                                    ev.prcsd_src_sys_cd AS prcsd_src_sys_cd,
                                                    ev.func_cd          AS func_cd,
                                                    ev.ev_dttm          AS ev_dttm,
                                                    ev.edw_strt_dttm    AS edw_strt_dttm,
                                                    ev.src_trans_id     AS src_trans_id,
                                                    ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                    ev.ev_actvy_type_cd AS ev_actvy_type_cd
                                           FROM     db_t_prod_core.ev 
                                           WHERE    ev.ev_sbtype_cd=''CATSTRPH''
                                           AND      src_trans_id IN
                                                                     (
                                                                     SELECT DISTINCT catastrophenumber
                                                                     FROM            intrm_src) qualify row_number() over( PARTITION BY ev.ev_sbtype_cd,ev.ev_actvy_type_cd,ev.src_trans_id ORDER BY ev.edw_end_dttm DESC) = 1 )ev_lkp
                  ON              ev_lkp.src_trans_id=src.catastrophenumber
                  AND             ev_lkp.ev_actvy_type_cd=acty_subtype_cd
                                  /*********************************** TARGET_LOOKUP *******************************************************************/
                  left outer join
                                  (
                                           SELECT   incdt.ctstrph_name                                                                    AS ctstrph_name,
                                                    incdt.host_ctstrph_ref_num                                                            AS host_ctstrph_ref_num,
                                                    incdt.edw_strt_dttm                                                                   AS edw_strt_dttm,
                                                    incdt.edw_end_dttm                                                                    AS edw_end_dttm,
                                                    incdt.incdt_strt_dttm                                                                 AS incdt_strt_dttm ,
                                                    to_char(incdt.incdt_end_dttm ,''YYYY-MM-DD HH24:MI:SS.FF6'') 				  AS incdt_end_dttm,
                                                    /*EIM-47587*/
                                                    incdt.incdt_ev_id AS incdt_ev_id
                                           FROM     db_t_prod_core.incdt  qualify row_number () over ( PARTITION BY incdt_ev_id ORDER BY edw_end_dttm DESC)=1 )tgt_lkp_incdt
                  ON              tgt_lkp_incdt.incdt_ev_id=ev_lkp.ev_id ) src ) );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT sq_cc_catastrophe.tgt_incdt_ev_id       AS lkp_incdt_ev_id,
                sq_cc_catastrophe.tgt_name              AS lkp_ctstrph_name,
                sq_cc_catastrophe.tgt_catastrophenumber AS lkp_host_ctstrph_ref_num,
                sq_cc_catastrophe.tgt_edw_strt_dttm     AS lkp_edw_strt_dttm,
                sq_cc_catastrophe.tgt_edw_end_dttm      AS lkp_edw_end_dttm,
                md5 ( sq_cc_catastrophe.tgt_name
                       || sq_cc_catastrophe.tgt_catastrophenumber ) AS orig_chksm,
                sq_cc_catastrophe.src_incdt_ev_id                   AS incdt_ev_id,
                sq_cc_catastrophe.src_name                          AS name,
                sq_cc_catastrophe.src_catastrophenumber             AS catastrophenumber,
                md5 ( sq_cc_catastrophe.src_name
                       || sq_cc_catastrophe.src_catastrophenumber )                    AS calc_chksm,
                :prcs_id                                                               AS process_id,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                sq_cc_catastrophe.src_catastrophevalid_from                            AS incdt_strt_dt,
                sq_cc_catastrophe.src_catastrophevalid_to                              AS incdt_end_dt,
                sq_cc_catastrophe.src_retired                                          AS retired,
                sq_cc_catastrophe.ins_upd_flg                                          AS ins_upd_flg,
                sq_cc_catastrophe.source_record_id
         FROM   sq_cc_catastrophe );
  -- Component rtr_incdt_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_incdt_insert as
  SELECT exp_ins_upd.lkp_incdt_ev_id          AS lkp_incdt_ev_id,
         exp_ins_upd.ins_upd_flg              AS out_flag,
         exp_ins_upd.name                     AS name,
         exp_ins_upd.catastrophenumber        AS catastrophenumber,
         exp_ins_upd.incdt_ev_id              AS incdt_ev_id4,
         exp_ins_upd.process_id               AS process_id,
         exp_ins_upd.out_edw_strt_dttm        AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm         AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_ins_upd.incdt_strt_dt            AS incdt_strt_dt,
         exp_ins_upd.incdt_end_dt             AS incdt_end_dt,
         exp_ins_upd.lkp_ctstrph_name         AS lkp_ctstrph_name,
         exp_ins_upd.lkp_host_ctstrph_ref_num AS lkp_host_ctstrph_ref_num,
         exp_ins_upd.lkp_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_ins_upd.retired                  AS retired,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flg = ''I''
  OR     (
                exp_ins_upd.retired = 0
         AND    exp_ins_upd.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_incdt_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_incdt_retire as
  SELECT exp_ins_upd.lkp_incdt_ev_id          AS lkp_incdt_ev_id,
         exp_ins_upd.ins_upd_flg              AS out_flag,
         exp_ins_upd.name                     AS name,
         exp_ins_upd.catastrophenumber        AS catastrophenumber,
         exp_ins_upd.incdt_ev_id              AS incdt_ev_id4,
         exp_ins_upd.process_id               AS process_id,
         exp_ins_upd.out_edw_strt_dttm        AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm         AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_ins_upd.incdt_strt_dt            AS incdt_strt_dt,
         exp_ins_upd.incdt_end_dt             AS incdt_end_dt,
         exp_ins_upd.lkp_ctstrph_name         AS lkp_ctstrph_name,
         exp_ins_upd.lkp_host_ctstrph_ref_num AS lkp_host_ctstrph_ref_num,
         exp_ins_upd.lkp_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_ins_upd.retired                  AS retired,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flg = ''R''
  AND    exp_ins_upd.retired != 0
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_incdt_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_incdt_update as
  SELECT exp_ins_upd.lkp_incdt_ev_id          AS lkp_incdt_ev_id,
         exp_ins_upd.ins_upd_flg              AS out_flag,
         exp_ins_upd.name                     AS name,
         exp_ins_upd.catastrophenumber        AS catastrophenumber,
         exp_ins_upd.incdt_ev_id              AS incdt_ev_id4,
         exp_ins_upd.process_id               AS process_id,
         exp_ins_upd.out_edw_strt_dttm        AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm         AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_ins_upd.incdt_strt_dt            AS incdt_strt_dt,
         exp_ins_upd.incdt_end_dt             AS incdt_end_dt,
         exp_ins_upd.lkp_ctstrph_name         AS lkp_ctstrph_name,
         exp_ins_upd.lkp_host_ctstrph_ref_num AS lkp_host_ctstrph_ref_num,
         exp_ins_upd.lkp_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_ins_upd.retired                  AS retired,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flg = ''U''
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_incdt_upd1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_incdt_upd1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_incdt_retire.lkp_incdt_ev_id          AS lkp_incdt_ev_id3,
                rtr_incdt_retire.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_incdt_retire.process_id               AS process_id3,
                rtr_incdt_retire.lkp_ctstrph_name         AS lkp_ctstrph_name,
                rtr_incdt_retire.lkp_host_ctstrph_ref_num AS lkp_host_ctstrph_ref_num,
                rtr_incdt_retire.out_edw_strt_dttm        AS out_edw_strt_dttm4,
                1                                         AS update_strategy_action,
				rtr_incdt_retire.source_record_id
         FROM   rtr_incdt_retire );
  -- Component upd_incdt_ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_incdt_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_incdt_insert.incdt_ev_id4      AS incdt_ev_id,
                rtr_incdt_insert.out_flag          AS out_flag1,
                rtr_incdt_insert.name              AS name1,
                rtr_incdt_insert.catastrophenumber AS catastrophenumber1,
                rtr_incdt_insert.out_edw_strt_dttm AS out_edw_strt_dttm1,
                rtr_incdt_insert.out_edw_end_dttm  AS out_edw_end_dttm1,
                rtr_incdt_insert.process_id        AS process_id1,
                rtr_incdt_insert.incdt_strt_dt     AS incdt_strt_dt1,
                rtr_incdt_insert.incdt_end_dt      AS incdt_end_dt1,
                rtr_incdt_insert.retired           AS retired1,
                0                                  AS update_strategy_action,
				rtr_incdt_insert.source_record_id
         FROM   rtr_incdt_insert );
  -- Component upd_incdt_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_incdt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_incdt_update.name              AS name1,
                rtr_incdt_update.catastrophenumber AS catastrophenumber1,
                rtr_incdt_update.incdt_ev_id4      AS incdt_ev_id43,
                rtr_incdt_update.process_id        AS process_id3,
                rtr_incdt_update.out_edw_strt_dttm AS out_edw_strt_dttm3,
                rtr_incdt_update.out_edw_end_dttm  AS out_edw_end_dttm3,
                rtr_incdt_update.incdt_strt_dt     AS incdt_strt_dt3,
                rtr_incdt_update.incdt_end_dt      AS incdt_end_dt3,
                rtr_incdt_update.retired           AS retired3,
                0                                  AS update_strategy_action,
				rtr_incdt_update.source_record_id
         FROM   rtr_incdt_update );
  -- Component upd_incdt_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_incdt_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_incdt_update.lkp_incdt_ev_id          AS lkp_incdt_ev_id3,
                rtr_incdt_update.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_incdt_update.process_id               AS process_id3,
                rtr_incdt_update.lkp_ctstrph_name         AS lkp_ctstrph_name,
                rtr_incdt_update.lkp_host_ctstrph_ref_num AS lkp_host_ctstrph_ref_num,
                rtr_incdt_update.out_edw_strt_dttm        AS in_edw_strt_dttm3,
                1                                         AS update_strategy_action,
				rtr_incdt_update.source_record_id
         FROM   rtr_incdt_update );
  -- Component FILTRANS, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans AS
  (
         SELECT upd_incdt_ins.name1              AS name1,
                upd_incdt_ins.catastrophenumber1 AS catastrophenumber1,
                upd_incdt_ins.incdt_ev_id43      AS incdt_ev_id43,
                upd_incdt_ins.process_id3        AS process_id3,
                upd_incdt_ins.out_edw_strt_dttm3 AS out_edw_strt_dttm3,
                upd_incdt_ins.out_edw_end_dttm3  AS out_edw_end_dttm3,
                upd_incdt_ins.incdt_strt_dt3     AS incdt_strt_dt3,
                upd_incdt_ins.incdt_end_dt3      AS incdt_end_dt3,
                upd_incdt_ins.retired3           AS retired3,
                upd_incdt_ins.source_record_id
         FROM   upd_incdt_ins
         WHERE  upd_incdt_ins.retired3 = 0 );
  -- Component exp_pass_to_target1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target1 AS
  (
         SELECT upd_incdt_ins_new.incdt_ev_id        AS incdt_ev_id,
                upd_incdt_ins_new.name1              AS name1,
                upd_incdt_ins_new.catastrophenumber1 AS catastrophenumber1,
                upd_incdt_ins_new.out_edw_strt_dttm1 AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_incdt_ins_new.retired1 = 0 THEN upd_incdt_ins_new.out_edw_end_dttm1
                       ELSE upd_incdt_ins_new.out_edw_strt_dttm1
                END                              AS out_edw_end_dttm11,
                upd_incdt_ins_new.process_id1    AS process_id1,
                upd_incdt_ins_new.incdt_strt_dt1 AS incdt_strt_dt1,
                upd_incdt_ins_new.incdt_end_dt1  AS incdt_end_dt1,
                upd_incdt_ins_new.source_record_id
         FROM   upd_incdt_ins_new );
  -- Component exp_pass_to_tgt_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
  (
         SELECT upd_incdt_upd1.lkp_incdt_ev_id3   AS lkp_incdt_ev_id3,
                upd_incdt_upd1.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                upd_incdt_upd1.out_edw_strt_dttm4 AS edw_end_dttm,
                upd_incdt_upd1.source_record_id
         FROM   upd_incdt_upd1 );
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT upd_incdt_upd.lkp_incdt_ev_id3                      AS lkp_incdt_ev_id3,
                upd_incdt_upd.lkp_edw_strt_dttm3                    AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, upd_incdt_upd.in_edw_strt_dttm3) AS edw_end_dttm,
                upd_incdt_upd.source_record_id
         FROM   upd_incdt_upd );
  -- Component tgt_incdt_retire, Type TARGET
  merge
  INTO         db_t_prod_core.incdt
  USING        exp_pass_to_tgt_upd1
  ON (
                            incdt.incdt_ev_id = exp_pass_to_tgt_upd1.lkp_incdt_ev_id3
               AND          incdt.edw_strt_dttm = exp_pass_to_tgt_upd1.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    incdt_ev_id = exp_pass_to_tgt_upd1.lkp_incdt_ev_id3,
         edw_strt_dttm = exp_pass_to_tgt_upd1.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_upd1.edw_end_dttm;
  
  -- Component tgt_incdt_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.incdt
              (
                          incdt_ev_id,
                          ctstrph_name,
                          host_ctstrph_ref_num,
                          prcs_id,
                          incdt_strt_dttm,
                          incdt_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target1.incdt_ev_id        AS incdt_ev_id,
         exp_pass_to_target1.name1              AS ctstrph_name,
         exp_pass_to_target1.catastrophenumber1 AS host_ctstrph_ref_num,
         exp_pass_to_target1.process_id1        AS prcs_id,
         exp_pass_to_target1.incdt_strt_dt1     AS incdt_strt_dttm,
         exp_pass_to_target1.incdt_end_dt1      AS incdt_end_dttm,
         exp_pass_to_target1.in_edw_strt_dttm1  AS edw_strt_dttm,
         exp_pass_to_target1.out_edw_end_dttm11 AS edw_end_dttm
  FROM   exp_pass_to_target1;
  
  -- Component exp_pass_to_target, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target AS
  (
         SELECT filtrans.name1              AS name1,
                filtrans.catastrophenumber1 AS catastrophenumber1,
                filtrans.incdt_ev_id43      AS incdt_ev_id43,
                filtrans.process_id3        AS process_id3,
                filtrans.out_edw_strt_dttm3 AS out_edw_strt_dttm3,
                filtrans.out_edw_end_dttm3  AS out_edw_end_dttm3,
                filtrans.incdt_strt_dt3     AS incdt_strt_dt3,
                filtrans.incdt_end_dt3      AS incdt_end_dt3,
                filtrans.source_record_id
         FROM   filtrans );
  -- Component tgt_incdt_upd, Type TARGET
  merge
  INTO         db_t_prod_core.incdt
  USING        exp_pass_to_tgt_upd
  ON (
                            incdt.incdt_ev_id = exp_pass_to_tgt_upd.lkp_incdt_ev_id3
               AND          incdt.edw_strt_dttm = exp_pass_to_tgt_upd.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    incdt_ev_id = exp_pass_to_tgt_upd.lkp_incdt_ev_id3,
         edw_strt_dttm = exp_pass_to_tgt_upd.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_tgt_upd.edw_end_dttm;
  
  -- Component tgt_incdt_ins, Type TARGET
  INSERT INTO db_t_prod_core.incdt
              (
                          incdt_ev_id,
                          ctstrph_name,
                          host_ctstrph_ref_num,
                          prcs_id,
                          incdt_strt_dttm,
                          incdt_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target.incdt_ev_id43      AS incdt_ev_id,
         exp_pass_to_target.name1              AS ctstrph_name,
         exp_pass_to_target.catastrophenumber1 AS host_ctstrph_ref_num,
         exp_pass_to_target.process_id3        AS prcs_id,
         exp_pass_to_target.incdt_strt_dt3     AS incdt_strt_dttm,
         exp_pass_to_target.incdt_end_dt3      AS incdt_end_dttm,
         exp_pass_to_target.out_edw_strt_dttm3 AS edw_strt_dttm,
         exp_pass_to_target.out_edw_end_dttm3  AS edw_end_dttm
  FROM   exp_pass_to_target;

END;
';