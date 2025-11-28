-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_TXT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
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
  -- Component SQ_cc_claim, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_claim AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS claimnumber,
                $2 AS lossdate,
                $3 AS description,
                $4 AS src_cd,
                $5 AS createtime,
                $6 AS closedate,
                $7 AS updatetime,
                $8 AS retired,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cc_claim.claimnumber_stg,
                                                                  cc_claim.lossdate_stg,
                                                                  cc_claim.description_stg,
                                                                  ''SRC_SYS6'' AS src_cd,
                                                                  cc_claim.createtime_stg,
                                                                  cc_claim.closedate_stg,
                                                                  cc_claim.updatetime_stg,
                                                                  cc_claim.retired_stg
                                                  FROM            db_t_prod_stag.cc_claim
                                                  inner join      db_t_prod_stag.cctl_claimstate
                                                  ON              cc_claim.state_stg= cctl_claimstate.id_stg
                                                  AND             cctl_claimstate.name_stg <> ''Draft''
                                                  AND             (
                                                                                  cc_claim.updatetime_stg>($start_dttm)
                                                                  AND             cc_claim.updatetime_stg <= ($end_dttm)) ) src ) );
  -- Component exp_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through AS
  (
            SELECT    sq_cc_claim.claimnumber AS claimnumber,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                        AS o_src_cd,
                      sq_cc_claim.lossdate                              AS lossdate,
                      sq_cc_claim.description                           AS description,
                      ''LOSSDESC''                                        AS clm_txt_type_cd,
                      to_char ( sq_cc_claim.createtime , ''YYYY/MM/DD'' ) AS v_createtime,
                      to_date ( v_createtime , ''YYYY/MM/DD'' )           AS v_createtime1,
                      CASE
                                WHEN v_createtime1 IS NULL THEN to_date ( ''1900/01/01'' , ''YYYY/MM/DD'' )
                                ELSE v_createtime1
                      END                                              AS o_createtime,
                      to_char ( sq_cc_claim.closedate , ''YYYY/MM/DD'' ) AS v_closedate,
                      to_date ( v_closedate , ''YYYY/MM/DD'' )           AS v_closedate1,
                      CASE
                                WHEN v_closedate1 IS NULL THEN to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                ELSE v_closedate1
                      END                    AS o_closedate,
                      sq_cc_claim.updatetime AS updatetime,
                      sq_cc_claim.retired    AS retired,
                      sq_cc_claim.source_record_id,
                      row_number() over (PARTITION BY sq_cc_claim.source_record_id ORDER BY sq_cc_claim.source_record_id) AS rnk
            FROM      sq_cc_claim
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_cc_claim.src_cd qualify rnk = 1 );
  -- Component LKP_CLM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm AS
  (
            SELECT    lkp.clm_id,
                      exp_pass_through.source_record_id,
                      row_number() over(PARTITION BY exp_pass_through.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) rnk
            FROM      exp_pass_through
            left join
                      (
                               SELECT   clm.clm_id                    AS clm_id,
                                        clm.clm_type_cd               AS clm_type_cd,
                                        clm.clm_mdia_type_cd          AS clm_mdia_type_cd,
                                        clm.clm_submtl_type_cd        AS clm_submtl_type_cd,
                                        clm.acdnt_type_cd             AS acdnt_type_cd,
                                        clm.clm_ctgy_type_cd          AS clm_ctgy_type_cd,
                                        clm.addl_insrnc_pln_ind       AS addl_insrnc_pln_ind,
                                        clm.emplmt_rltd_ind           AS emplmt_rltd_ind,
                                        clm.attny_invlvmt_ind         AS attny_invlvmt_ind,
                                        clm.clm_prir_ind              AS clm_prir_ind,
                                        clm.pmt_mode_cd               AS pmt_mode_cd,
                                        clm.clm_oblgtn_type_cd        AS clm_oblgtn_type_cd,
                                        clm.subrgtn_elgbl_cd          AS subrgtn_elgbl_cd,
                                        clm.subrgtn_elgbly_rsn_cd     AS subrgtn_elgbly_rsn_cd,
                                        clm.cury_cd                   AS cury_cd,
                                        clm.incdt_ev_id               AS incdt_ev_id,
                                        clm.insrd_at_fault_ind        AS insrd_at_fault_ind,
                                        clm.cvge_in_ques_ind          AS cvge_in_ques_ind,
                                        clm.extnt_of_fire_dmg_type_cd AS extnt_of_fire_dmg_type_cd,
                                        clm.vfyd_clm_ind              AS vfyd_clm_ind,
                                        clm.prcs_id                   AS prcs_id,
                                        clm.clm_strt_dttm             AS clm_strt_dttm,
                                        clm.clm_end_dttm              AS clm_end_dttm,
                                        clm.edw_strt_dttm             AS edw_strt_dttm,
                                        clm.edw_end_dttm              AS edw_end_dttm,
                                        clm.trans_strt_dttm           AS trans_strt_dttm,
                                        clm.lgcy_clm_num              AS lgcy_clm_num,
                                        clm.clm_num                   AS clm_num,
                                        clm.src_sys_cd                AS src_sys_cd
                               FROM     db_t_prod_core.clm qualify row_number() over(PARTITION BY clm.clm_num,clm.src_sys_cd ORDER BY clm.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.clm_num = exp_pass_through.claimnumber
            AND       lkp.src_sys_cd = exp_pass_through.o_src_cd 
			qualify row_number() over(PARTITION BY exp_pass_through.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) 
            		= 1 );
  -- Component exp_SrcFields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_srcfields AS
  (
             SELECT     lkp_clm.clm_id                                                         AS in_clm_id,
                        exp_pass_through.clm_txt_type_cd                                       AS in_clm_txt_type_cd,
                        exp_pass_through.lossdate                                              AS in_clm_txt_dttm,
                        exp_pass_through.description                                           AS in_clm_txt,
                        exp_pass_through.o_createtime                                          AS in_clm_txt_strt_dt,
                        exp_pass_through.o_closedate                                           AS in_clm_txt_end_dt,
                        exp_pass_through.updatetime                                            AS in_trans_strt_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS in_trans_end_dttm,
                        exp_pass_through.retired                                               AS retired,
                        exp_pass_through.source_record_id
             FROM       exp_pass_through
             inner join lkp_clm
             ON         exp_pass_through.source_record_id = lkp_clm.source_record_id );
  -- Component LKP_CLM_TXT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_txt AS
  (
            SELECT    lkp.clm_id,
                      lkp.clm_txt_type_cd,
                      lkp.clm_txt_dttm,
                      lkp.clm_txt,
                      lkp.clm_txt_strt_dttm,
                      lkp.clm_txt_end_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_srcfields.in_clm_id          AS in_clm_id,
                      exp_srcfields.in_clm_txt_type_cd AS in_clm_txt_type_cd,
                      exp_srcfields.source_record_id,
                      row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_txt_type_cd DESC,lkp.clm_txt_dttm DESC,lkp.clm_txt DESC,lkp.clm_txt_strt_dttm DESC,lkp.clm_txt_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_srcfields
            left join
                      (
                               SELECT   clm_txt.clm_txt_dttm      AS clm_txt_dttm,
                                        clm_txt.clm_txt           AS clm_txt,
                                        clm_txt.clm_txt_strt_dttm AS clm_txt_strt_dttm,
                                        clm_txt.clm_txt_end_dttm  AS clm_txt_end_dttm,
                                        clm_txt.edw_strt_dttm     AS edw_strt_dttm,
                                        clm_txt.edw_end_dttm      AS edw_end_dttm,
                                        clm_txt.clm_id            AS clm_id,
                                        clm_txt.clm_txt_type_cd   AS clm_txt_type_cd
                               FROM     db_t_prod_core.clm_txt qualify row_number() over(PARTITION BY clm_txt.clm_id,clm_txt.clm_txt_type_cd ORDER BY clm_txt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.clm_id = exp_srcfields.in_clm_id
            AND       lkp.clm_txt_type_cd = exp_srcfields.in_clm_txt_type_cd 
			qualify row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_txt_type_cd DESC,lkp.clm_txt_dttm DESC,lkp.clm_txt DESC,lkp.clm_txt_strt_dttm DESC,lkp.clm_txt_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) 
            			= 1 );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
             SELECT     exp_srcfields.in_clm_id                                                AS in_clm_id,
                        exp_srcfields.in_clm_txt_type_cd                                       AS in_clm_txt_type_cd,
                        exp_srcfields.in_clm_txt_dttm                                          AS in_clm_txt_dttm,
                        exp_srcfields.in_clm_txt                                               AS in_clm_txt,
                        exp_srcfields.in_clm_txt_strt_dt                                       AS in_clm_txt_strt_dt,
                        exp_srcfields.in_clm_txt_end_dt                                        AS in_clm_txt_end_dt,
                        $prcs_id                                                               AS in_prcs_id,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_srcfields.in_trans_strt_dttm                                       AS in_trans_strt_dttm,
                        exp_srcfields.in_trans_end_dttm                                        AS in_trans_end_dttm,
                        lkp_clm_txt.clm_id                                                     AS lkp_clm_id,
                        lkp_clm_txt.clm_txt_type_cd                                            AS lkp_clm_txt_type_cd,
                        lkp_clm_txt.edw_strt_dttm                                              AS lkp_edw_strt_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( exp_srcfields.in_clm_txt_dttm , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( exp_srcfields.in_clm_txt ) )
                                   || ltrim ( rtrim ( to_char ( exp_srcfields.in_clm_txt_strt_dt , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_srcfields.in_clm_txt_end_dt , ''MM/DD/YYYY'' ) ) ) ) AS v_srcmd5,
                        md5 ( ltrim ( rtrim ( to_char ( lkp_clm_txt.clm_txt_dttm , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( lkp_clm_txt.clm_txt ) )
                                   || ltrim ( rtrim ( to_char ( lkp_clm_txt.clm_txt_strt_dttm , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_clm_txt.clm_txt_end_dttm , ''MM/DD/YYYY'' ) ) ) ) AS v_tgtmd5,
                        CASE
                                   WHEN v_tgtmd5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_srcmd5 != v_tgtmd5 THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                      AS o_cdc_chk,
                        exp_srcfields.retired    AS retired,
                        lkp_clm_txt.edw_end_dttm AS lkp_edw_end_dttm,
                        exp_srcfields.source_record_id
             FROM       exp_srcfields
             inner join lkp_clm_txt
             ON         exp_srcfields.source_record_id = lkp_clm_txt.source_record_id );
  -- Component rtr_ins_upd_INS_UPD, Type ROUTER Output Group INS_UPD
  create or replace temporary table rtr_ins_upd_INS_UPD as
  SELECT exp_check_flag.in_clm_id           AS in_clm_id,
         exp_check_flag.in_clm_txt_type_cd  AS in_clm_txt_type_cd,
         exp_check_flag.in_clm_txt_dttm     AS in_clm_txt_dttm,
         exp_check_flag.in_clm_txt          AS in_clm_txt,
         exp_check_flag.in_clm_txt_strt_dt  AS in_clm_txt_strt_dt,
         exp_check_flag.in_clm_txt_end_dt   AS in_clm_txt_end_dt,
         exp_check_flag.in_prcs_id          AS in_prcs_id,
         exp_check_flag.in_edw_strt_dttm    AS in_edw_strt_dttm,
         exp_check_flag.in_edw_end_dttm     AS in_edw_end_dttm,
         exp_check_flag.in_trans_strt_dttm  AS in_trans_strt_dttm,
         exp_check_flag.in_trans_end_dttm   AS in_trans_end_dttm,
         exp_check_flag.lkp_clm_id          AS lkp_clm_id,
         exp_check_flag.lkp_clm_txt_type_cd AS lkp_clm_txt_type_cd,
         exp_check_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_check_flag.o_cdc_chk           AS o_cdc_chk,
         exp_check_flag.retired             AS retired,
         exp_check_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.in_clm_id IS NOT NULL
  AND    ( (
                       exp_check_flag.o_cdc_chk = ''I'' )
         OR     (
                       exp_check_flag.retired = 0
                AND    exp_check_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_check_flag.o_cdc_chk = ''U''
                AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_ins_upd_RETIRE as
  SELECT exp_check_flag.in_clm_id           AS in_clm_id,
         exp_check_flag.in_clm_txt_type_cd  AS in_clm_txt_type_cd,
         exp_check_flag.in_clm_txt_dttm     AS in_clm_txt_dttm,
         exp_check_flag.in_clm_txt          AS in_clm_txt,
         exp_check_flag.in_clm_txt_strt_dt  AS in_clm_txt_strt_dt,
         exp_check_flag.in_clm_txt_end_dt   AS in_clm_txt_end_dt,
         exp_check_flag.in_prcs_id          AS in_prcs_id,
         exp_check_flag.in_edw_strt_dttm    AS in_edw_strt_dttm,
         exp_check_flag.in_edw_end_dttm     AS in_edw_end_dttm,
         exp_check_flag.in_trans_strt_dttm  AS in_trans_strt_dttm,
         exp_check_flag.in_trans_end_dttm   AS in_trans_end_dttm,
         exp_check_flag.lkp_clm_id          AS lkp_clm_id,
         exp_check_flag.lkp_clm_txt_type_cd AS lkp_clm_txt_type_cd,
         exp_check_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_check_flag.o_cdc_chk           AS o_cdc_chk,
         exp_check_flag.retired             AS retired,
         exp_check_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.o_cdc_chk = ''R''
  AND    exp_check_flag.retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_CLM_TXT_Insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_txt_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_ins_upd.in_clm_id          AS in_clm_id1,
                rtr_ins_upd_ins_upd.in_clm_txt_type_cd AS in_clm_txt_type_cd1,
                rtr_ins_upd_ins_upd.in_clm_txt_dttm    AS in_clm_txt_dttm1,
                rtr_ins_upd_ins_upd.in_clm_txt         AS in_clm_txt1,
                rtr_ins_upd_ins_upd.in_prcs_id         AS in_prcs_id1,
                rtr_ins_upd_ins_upd.in_clm_txt_strt_dt AS in_clm_txt_strt_dt1,
                rtr_ins_upd_ins_upd.in_clm_txt_end_dt  AS in_clm_txt_end_dt1,
                rtr_ins_upd_ins_upd.in_edw_strt_dttm   AS in_edw_strt_dttm1,
                rtr_ins_upd_ins_upd.in_edw_end_dttm    AS in_edw_end_dttm1,
                rtr_ins_upd_ins_upd.in_trans_strt_dttm AS in_trans_strt_dttm1,
                rtr_ins_upd_ins_upd.in_trans_end_dttm  AS in_trans_end_dttm1,
                rtr_ins_upd_ins_upd.retired            AS retired1,
                0                                      AS update_strategy_action,
				rtr_ins_upd_ins_upd.source_record_id
         FROM   rtr_ins_upd_ins_upd );
  -- Component upd_CLM_TXT_Retire_Rejected, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_txt_retire_rejected AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_retire.lkp_clm_id          AS lkp_clm_id3,
                rtr_ins_upd_retire.lkp_clm_txt_type_cd AS lkp_clm_txt_type_cd3,
                rtr_ins_upd_retire.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm3,
                rtr_ins_upd_retire.in_trans_strt_dttm  AS in_trans_strt_dttm4,
                1                                      AS update_strategy_action,
				source_record_id
         FROM   rtr_ins_upd_retire );
  -- Component exp_CLM_TXT_Insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_txt_insert AS
  (
         SELECT upd_clm_txt_insert.in_clm_id1          AS in_clm_id1,
                upd_clm_txt_insert.in_clm_txt_type_cd1 AS in_clm_txt_type_cd1,
                upd_clm_txt_insert.in_clm_txt_dttm1    AS in_clm_txt_dttm1,
                upd_clm_txt_insert.in_clm_txt1         AS in_clm_txt1,
                upd_clm_txt_insert.in_prcs_id1         AS in_prcs_id1,
                upd_clm_txt_insert.in_clm_txt_strt_dt1 AS in_clm_txt_strt_dt1,
                upd_clm_txt_insert.in_clm_txt_end_dt1  AS in_clm_txt_end_dt1,
                upd_clm_txt_insert.in_edw_strt_dttm1   AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_clm_txt_insert.retired1 != 0 THEN upd_clm_txt_insert.in_edw_strt_dttm1
                       ELSE upd_clm_txt_insert.in_edw_end_dttm1
                END                                    AS o_edw_end_dttm,
                upd_clm_txt_insert.in_trans_strt_dttm1 AS in_trans_strt_dttm1,
                CASE
                       WHEN upd_clm_txt_insert.retired1 <> 0 THEN upd_clm_txt_insert.in_trans_strt_dttm1
                       ELSE upd_clm_txt_insert.in_trans_end_dttm1
                END AS o_trans_end_dttm,
                upd_clm_txt_insert.source_record_id
         FROM   upd_clm_txt_insert );
  -- Component exp_CLM_TXT_Retire_Rejected, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_txt_retire_rejected AS
  (
         SELECT upd_clm_txt_retire_rejected.lkp_clm_id3          AS lkp_clm_id3,
                upd_clm_txt_retire_rejected.lkp_clm_txt_type_cd3 AS lkp_clm_txt_type_cd3,
                upd_clm_txt_retire_rejected.lkp_edw_strt_dttm3   AS lkp_edw_strt_dttm3,
                upd_clm_txt_retire_rejected.in_trans_strt_dttm4  AS in_trans_strt_dttm4,
                current_timestamp                                AS o_dateexpiry,
                upd_clm_txt_retire_rejected.source_record_id
         FROM   upd_clm_txt_retire_rejected );
  -- Component CLM_TXT_Retire_Rejected, Type TARGET
  merge
  INTO         db_t_prod_core.clm_txt
  USING        exp_clm_txt_retire_rejected
  ON (
                            clm_txt.clm_id = exp_clm_txt_retire_rejected.lkp_clm_id3
               AND          clm_txt.clm_txt_type_cd = exp_clm_txt_retire_rejected.lkp_clm_txt_type_cd3
               AND          clm_txt.edw_strt_dttm = exp_clm_txt_retire_rejected.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_clm_txt_retire_rejected.lkp_clm_id3,
         clm_txt_type_cd = exp_clm_txt_retire_rejected.lkp_clm_txt_type_cd3,
         edw_strt_dttm = exp_clm_txt_retire_rejected.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_clm_txt_retire_rejected.o_dateexpiry,
         trans_end_dttm = exp_clm_txt_retire_rejected.in_trans_strt_dttm4;
  
  -- Component CLM_TXT_Retire_Rejected, Type Post SQL
  UPDATE db_t_prod_core.clm_txt
    SET    edw_end_dttm=updt.lead1,
         trans_end_dttm=updt.lead2
  FROM   (
                         SELECT DISTINCT clm_id,
                                         clm_txt_type_cd,
                                         clm_txt_end_dttm,
                                         edw_strt_dttm ,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY clm_id,clm_txt_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)     - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY clm_id,clm_txt_type_cd ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.clm_txt
                         GROUP BY        clm_id,
                                         clm_txt_type_cd,
                                         clm_txt_end_dttm,
                                         edw_strt_dttm ,
                                         trans_strt_dttm ) updt

  WHERE  clm_txt.clm_id=updt.clm_id
  AND    clm_txt.clm_txt_type_cd=updt.clm_txt_type_cd
  AND    clm_txt.edw_strt_dttm=updt.edw_strt_dttm
  AND    clm_txt.trans_strt_dttm = updt.trans_strt_dttm
  AND    cast(clm_txt.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(clm_txt.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;
  
  -- Component CLM_TXT_insert, Type TARGET
  INSERT INTO db_t_prod_core.clm_txt
              (
                          clm_id,
                          clm_txt_type_cd,
                          clm_txt_dttm,
                          clm_txt,
                          prcs_id,
                          clm_txt_strt_dttm,
                          clm_txt_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_clm_txt_insert.in_clm_id1          AS clm_id,
         exp_clm_txt_insert.in_clm_txt_type_cd1 AS clm_txt_type_cd,
         exp_clm_txt_insert.in_clm_txt_dttm1    AS clm_txt_dttm,
         exp_clm_txt_insert.in_clm_txt1         AS clm_txt,
         exp_clm_txt_insert.in_prcs_id1         AS prcs_id,
         exp_clm_txt_insert.in_clm_txt_strt_dt1 AS clm_txt_strt_dttm,
         exp_clm_txt_insert.in_clm_txt_end_dt1  AS clm_txt_end_dttm,
         exp_clm_txt_insert.in_edw_strt_dttm1   AS edw_strt_dttm,
         exp_clm_txt_insert.o_edw_end_dttm      AS edw_end_dttm,
         exp_clm_txt_insert.in_trans_strt_dttm1 AS trans_strt_dttm,
         exp_clm_txt_insert.o_trans_end_dttm    AS trans_end_dttm
  FROM   exp_clm_txt_insert;

END;
';