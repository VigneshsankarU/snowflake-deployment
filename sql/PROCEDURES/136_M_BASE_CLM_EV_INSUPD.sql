-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EV_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       PRCS_ID STRING;
BEGIN
   run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
   start_dttm := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''start_dttm'' LIMIT 1);
   end_dttm := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''end_dttm'' LIMIT 1);
 --  SS := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''SS'' LIMIT 1);
   prcs_id := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''prcs_id'' LIMIT 1);
 --  59 = (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''59'' LIMIT 1)::STRING;
 --  MI = (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''MI'' LIMIT 1)::STRING;


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_clm_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_cc_catastrophe, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_catastrophe AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS catastrophenumber,
                $2 AS claimnumber,
                $3 AS ev_act_type_code,
                $4 AS clm_src_cd,
                $5 AS retired,
                $6 AS trns_strt_date,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT catastrophenumber,
                                                                  claimnumber ,
                                                                  cctl_catastrophetype.typecode_stg AS "ev_act_type_code",
                                                                  ''SRC_SYS6''                        AS clm_src_cd,
                                                                  CASE
                                                                                  WHEN retired_cat=0
                                                                                  AND             ccclm.retired=0 THEN 0
                                                                                  ELSE 1
                                                                  END              AS retired,
                                                                  ccclm.updatetime AS trns_strt_dt
                                                  FROM            (
                                                                            SELECT    cc_claim.claimnumber_stg             AS claimnumber,
                                                                                      cc_claim.updatetime_stg              AS updatetime,
                                                                                      cc_claim.retired_stg                 AS retired,
                                                                                      cc_catastrophe.retired_stg           AS retired_cat,
                                                                                      cc_catastrophe.type_stg              AS type_cat,
                                                                                      cc_catastrophe.catastrophenumber_stg AS catastrophenumber
                                                                            FROM      (
                                                                                                 SELECT     cc_claim.claimnumber_stg,
                                                                                                            cc_claim.updatetime_stg,
                                                                                                            cc_claim.retired_stg,
                                                                                                            cc_claim.catastropheid_stg
                                                                                                 FROM       db_t_prod_stag.cc_claim
                                                                                                 inner join db_t_prod_stag.cctl_claimstate
                                                                                                 ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                 WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                            left join db_t_prod_stag.cc_catastrophe
                                                                            ON        cc_claim.catastropheid_stg=cc_catastrophe.id_stg
                                                                            WHERE     cc_claim.updatetime_stg > (:start_dttm)
                                                                            AND       cc_claim.updatetime_stg <= (:end_dttm) ) ccclm
                                                  left join       db_t_prod_stag.cctl_catastrophetype
                                                  ON              cctl_catastrophetype.id_stg = ccclm.type_cat
                                                  WHERE           claimnumber IS NOT NULL
                                                  ORDER BY        ccclm.claimnumber,
                                                                  ccclm.updatetime ) src ) );
  -- Component exp_pass_frpm_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frpm_source AS
  (
            SELECT    sq_cc_catastrophe.catastrophenumber AS catastrophenumber,
                      sq_cc_catastrophe.claimnumber       AS claimnumber,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                      END        AS o_ev_act_type_code1,
                      ''CATSTRPH'' AS SUBTYPE,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD */
                                                       AS out_clm_src_cd,
                      sq_cc_catastrophe.retired        AS retired,
                      sq_cc_catastrophe.trns_strt_date AS trns_strt_date,
                      sq_cc_catastrophe.source_record_id,
                      row_number() over (PARTITION BY sq_cc_catastrophe.source_record_id ORDER BY sq_cc_catastrophe.source_record_id) AS rnk
            FROM      sq_cc_catastrophe
            left join lkp_teradata_etl_ref_xlat lkp_1
            ON        lkp_1.src_idntftn_val = sq_cc_catastrophe.ev_act_type_code
            left join lkp_teradata_etl_ref_xlat lkp_2
            ON        lkp_2.src_idntftn_val = sq_cc_catastrophe.ev_act_type_code
            left join lkp_teradata_etl_ref_xlat_clm_src_cd lkp_3
            ON        lkp_3.src_idntftn_val = sq_cc_catastrophe.clm_src_cd qualify rnk = 1 );
  -- Component LKP_EV, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_ev AS
  (
            SELECT    lkp.ev_id,
                      exp_pass_frpm_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frpm_source.source_record_id ORDER BY lkp.ev_id DESC,lkp.src_trans_id DESC,lkp.ev_sbtype_cd DESC,lkp.ev_actvy_type_cd DESC,lkp.ev_desc DESC,lkp.ev_strt_dttm DESC,lkp.ev_end_dttm DESC,lkp.ev_rsn_cd DESC,lkp.agmt_id DESC,lkp.prcsd_src_sys_cd DESC,lkp.func_cd DESC,lkp.ev_dttm DESC,lkp.edw_strt_dttm DESC) rnk
            FROM      exp_pass_frpm_source
            left join
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
                               WHERE    ev_sbtype_cd=''CATSTRPH'' qualify row_number() over(PARTITION BY ev.ev_sbtype_cd,ev.ev_actvy_type_cd,ev.src_trans_id ORDER BY ev.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.src_trans_id = exp_pass_frpm_source.catastrophenumber
            AND       lkp.ev_sbtype_cd = exp_pass_frpm_source.SUBTYPE
            AND       lkp.ev_actvy_type_cd = exp_pass_frpm_source.o_ev_act_type_code1 
			qualify row_number() over(PARTITION BY exp_pass_frpm_source.source_record_id ORDER BY lkp.ev_id DESC,lkp.src_trans_id DESC,lkp.ev_sbtype_cd DESC,lkp.ev_actvy_type_cd DESC,lkp.ev_desc DESC,lkp.ev_strt_dttm DESC,lkp.ev_end_dttm DESC,lkp.ev_rsn_cd DESC,lkp.agmt_id DESC,lkp.prcsd_src_sys_cd DESC,lkp.func_cd DESC,lkp.ev_dttm DESC,lkp.edw_strt_dttm DESC) 
            			= 1 );
  -- Component LKP_CLM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm AS
  (
            SELECT    lkp.clm_id,
                      exp_pass_frpm_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frpm_source.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) rnk
            FROM      exp_pass_frpm_source
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
            ON        lkp.clm_num = exp_pass_frpm_source.claimnumber
            AND       lkp.src_sys_cd = exp_pass_frpm_source.out_clm_src_cd 
			qualify row_number() over(PARTITION BY exp_pass_frpm_source.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) 
            			= 1 );
  -- Component exp_clm_ev_id, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_ev_id AS
  (
             SELECT     lkp_ev.ev_id                        AS in_ev_id,
                        lkp_clm.clm_id                      AS in_clm_id,
                        exp_pass_frpm_source.retired        AS retired,
                        ''ASSOC''                             AS out_clm_ev_role_cd,
                        exp_pass_frpm_source.trns_strt_date AS trns_strt_date,
                        exp_pass_frpm_source.source_record_id
             FROM       exp_pass_frpm_source
             inner join lkp_ev
             ON         exp_pass_frpm_source.source_record_id = lkp_ev.source_record_id
             inner join lkp_clm
             ON         lkp_ev.source_record_id = lkp_clm.source_record_id );
  -- Component LKP_CLM_EV, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_ev AS
  (
            SELECT    lkp.ev_id,
                      lkp.clm_id,
                      lkp.clm_ev_role_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      lkp.trans_strt_dttm,
                      exp_clm_ev_id.in_clm_id AS in_clm_id,
                      exp_clm_ev_id.source_record_id,
                      row_number() over(PARTITION BY exp_clm_ev_id.source_record_id ORDER BY lkp.ev_id DESC,lkp.clm_id DESC,lkp.clm_ev_role_cd DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.trans_strt_dttm DESC) rnk
            FROM      exp_clm_ev_id
            left join
                      (
                               SELECT   clm_ev.ev_id           AS ev_id,
                                        clm_ev.clm_id          AS clm_id ,
                                        clm_ev.clm_ev_role_cd  AS clm_ev_role_cd,
                                        clm_ev.edw_strt_dttm   AS edw_strt_dttm,
                                        clm_ev.edw_end_dttm    AS edw_end_dttm,
                                        clm_ev.trans_strt_dttm AS trans_strt_dttm
                               FROM     db_t_prod_core.clm_ev qualify row_number() over(PARTITION BY clm_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.clm_id = exp_clm_ev_id.in_clm_id 
			qualify row_number() over(PARTITION BY exp_clm_ev_id.source_record_id ORDER BY lkp.ev_id DESC,lkp.clm_id DESC,lkp.clm_ev_role_cd DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.trans_strt_dttm DESC) 
			= 1 );
  -- Component exp_clm_ev, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_ev AS
  (
             SELECT     lkp_clm_ev.ev_id                                                       AS lkp_ev_id,
                        lkp_clm_ev.clm_id                                                      AS lkp_clm_id,
                        lkp_clm_ev.edw_strt_dttm                                               AS lkp_edw_start_dttm,
                        lkp_clm_ev.trans_strt_dttm                                             AS lkp_trans_strt_dttm,
                        exp_clm_ev_id.in_ev_id                                                 AS in_ev_id,
                        lkp_clm_ev.in_clm_id                                                   AS in_clm_id,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        decode ( TRUE ,
                                lkp_clm_ev.clm_id IS NULL , ''I'' ,
                                lkp_clm_ev.clm_id IS NOT NULL
                     AND        ( (
                                                      lkp_clm_ev.ev_id <> exp_clm_ev_id.in_ev_id )
                                OR         (
                                                      lkp_clm_ev.clm_ev_role_cd <> exp_clm_ev_id.out_clm_ev_role_cd ) ) , ''U'' ,
                                ''R'' )                                                          AS out_id,
                        exp_clm_ev_id.retired                                                  AS retired,
                        lkp_clm_ev.edw_end_dttm                                                AS lkp_edw_end_dttm,
                        exp_clm_ev_id.out_clm_ev_role_cd                                       AS clm_ev_role_cd,
                        exp_clm_ev_id.trns_strt_date                                           AS in_trans_strt_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS in_trans_end_dttm,
                        exp_clm_ev_id.source_record_id
             FROM       exp_clm_ev_id
             inner join lkp_clm_ev
             ON         exp_clm_ev_id.source_record_id = lkp_clm_ev.source_record_id );
  -- Component rtr_clm_ev_INSERT, Type ROUTER Output Group INSERT
create or replace TEMPORARY TABLE rtr_clm_ev_insert AS
  SELECT exp_clm_ev.lkp_ev_id           AS lkp_ev_id,
         exp_clm_ev.lkp_clm_id          AS lkp_clm_id,
         exp_clm_ev.lkp_edw_start_dttm  AS lkp_edw_strt_dttm,
         exp_clm_ev.lkp_trans_strt_dttm AS lkp_trans_strt_dttm,
         exp_clm_ev.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_clm_ev.in_ev_id            AS in_ev_id,
         exp_clm_ev.in_clm_id           AS in_clm_id,
         exp_clm_ev.in_edw_strt_dttm    AS in_edw_strt_dttm,
         exp_clm_ev.in_trans_strt_dttm  AS in_trans_strt_dttm,
         exp_clm_ev.in_edw_end_dttm     AS in_edw_end_dttm,
         exp_clm_ev.in_trans_end_dttm   AS in_trans_end_dttm,
         exp_clm_ev.out_id              AS out_id,
         exp_clm_ev.retired             AS retired,
         exp_clm_ev.clm_ev_role_cd      AS clm_ev_role_cd,
         exp_clm_ev.source_record_id
  FROM   exp_clm_ev
  WHERE  exp_clm_ev.in_ev_id IS NOT NULL
  AND    exp_clm_ev.in_clm_id IS NOT NULL
  AND    (
                exp_clm_ev.out_id = ''I''
         OR     (
                       exp_clm_ev.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_clm_ev.retired = 0 )
         OR     exp_clm_ev.out_id = ''U'' )  /*- - exp_clm_ev.out_id = ''I'' - -
         CASE
                WHEN exp_clm_ev.out_id = ''Y'' THEN TRUE
                ELSE FALSE
         END  - -
         CASE
                WHEN exp_clm_ev.in_clm_id IS NULL THEN FALSE
                ELSE
                       CASE
                              WHEN exp_clm_ev.out_id = ''Y'' THEN TRUE
                              ELSE FALSE
                       END
         END
         */
         ;
  
  -- Component rtr_clm_ev_RETIRED, Type ROUTER Output Group RETIRED
create or replace TEMPORARY TABLE rtr_clm_ev_retired AS
  SELECT exp_clm_ev.lkp_ev_id           AS lkp_ev_id,
         exp_clm_ev.lkp_clm_id          AS lkp_clm_id,
         exp_clm_ev.lkp_edw_start_dttm  AS lkp_edw_strt_dttm,
         exp_clm_ev.lkp_trans_strt_dttm AS lkp_trans_strt_dttm,
         exp_clm_ev.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_clm_ev.in_ev_id            AS in_ev_id,
         exp_clm_ev.in_clm_id           AS in_clm_id,
         exp_clm_ev.in_edw_strt_dttm    AS in_edw_strt_dttm,
         exp_clm_ev.in_trans_strt_dttm  AS in_trans_strt_dttm,
         exp_clm_ev.in_edw_end_dttm     AS in_edw_end_dttm,
         exp_clm_ev.in_trans_end_dttm   AS in_trans_end_dttm,
         exp_clm_ev.out_id              AS out_id,
         exp_clm_ev.retired             AS retired,
         exp_clm_ev.clm_ev_role_cd      AS clm_ev_role_cd,
         exp_clm_ev.source_record_id
  FROM   exp_clm_ev
  WHERE  exp_clm_ev.out_id = ''R''
  AND    exp_clm_ev.retired != 0
  AND    exp_clm_ev.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) /*- - exp_clm_ev.out_id = ''R'' - -
         CASE
                WHEN exp_clm_ev.out_id = ''N'' THEN TRUE
                ELSE FALSE
         END*/
		 ;
  
  -- Component upd_clm_ev_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_ev_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_ev_insert.in_ev_id           AS ev_id1,
                rtr_clm_ev_insert.in_clm_id          AS clm_id1,
                rtr_clm_ev_insert.in_edw_strt_dttm   AS in_edw_strt_dttm1,
                rtr_clm_ev_insert.in_edw_end_dttm    AS in_edw_end_dttm1,
                rtr_clm_ev_insert.retired            AS retired1,
                rtr_clm_ev_insert.clm_ev_role_cd     AS clm_ev_role_cd,
                rtr_clm_ev_insert.in_trans_strt_dttm AS in_trans_strt_dttm41,
                rtr_clm_ev_insert.in_trans_end_dttm  AS in_trans_end_dttm1,
                0                                    AS update_strategy_action,
				rtr_clm_ev_insert.source_record_id
         FROM   rtr_clm_ev_insert );
  -- Component exp_pass_to_target1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target1 AS
  (
         SELECT upd_clm_ev_ins.ev_id1            AS ev_id3,
                upd_clm_ev_ins.clm_id1           AS clm_id3,
                :prcs_id                         AS out_prcs_id,
                upd_clm_ev_ins.clm_ev_role_cd    AS clm_ev_role_cd,
                upd_clm_ev_ins.in_edw_strt_dttm1 AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_clm_ev_ins.retired1 <> 0 THEN upd_clm_ev_ins.in_edw_strt_dttm1
                       ELSE upd_clm_ev_ins.in_edw_end_dttm1
                END                                 AS out_edw_end_dttm11,
                upd_clm_ev_ins.in_trans_strt_dttm41 AS in_trans_strt_dttm41,
                CASE
                       WHEN upd_clm_ev_ins.retired1 <> 0 THEN upd_clm_ev_ins.in_trans_strt_dttm41
                       ELSE upd_clm_ev_ins.in_trans_end_dttm1
                END AS out_trans_end_dttm11,
                upd_clm_ev_ins.source_record_id
         FROM   upd_clm_ev_ins );
  -- Component upd_clm_ev_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_ev_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_ev_retired.lkp_ev_id           AS ev_id1,
                rtr_clm_ev_retired.lkp_clm_id          AS clm_id1,
                rtr_clm_ev_retired.in_edw_strt_dttm    AS in_edw_strt_dttm1,
                rtr_clm_ev_retired.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm3,
                rtr_clm_ev_retired.lkp_trans_strt_dttm AS lkp_trans_strt_dttm3,
                rtr_clm_ev_retired.in_trans_strt_dttm  AS in_trans_strt_dttm3,
                1                                      AS update_strategy_action,
				rtr_clm_ev_retired.source_record_id
         FROM   rtr_clm_ev_retired );
  -- Component tgt_clm_ev_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.clm_ev
              (
                          ev_id,
                          clm_id,
                          clm_ev_role_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target1.ev_id3               AS ev_id,
         exp_pass_to_target1.clm_id3              AS clm_id,
         exp_pass_to_target1.clm_ev_role_cd       AS clm_ev_role_cd,
         exp_pass_to_target1.out_prcs_id          AS prcs_id,
         exp_pass_to_target1.in_edw_strt_dttm1    AS edw_strt_dttm,
         exp_pass_to_target1.out_edw_end_dttm11   AS edw_end_dttm,
         exp_pass_to_target1.in_trans_strt_dttm41 AS trans_strt_dttm,
         exp_pass_to_target1.out_trans_end_dttm11 AS trans_end_dttm
  FROM   exp_pass_to_target1;
  
  -- Component tgt_clm_ev_ins_new, Type Post SQL
  UPDATE db_t_prod_core.clm_ev
    SET    trans_end_dttm = a.lead,
         edw_end_dttm = a.lead1
  FROM   (
                         SELECT DISTINCT clm_id ,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.clm_ev ) a

  WHERE  clm_ev.clm_id = a.clm_id
  AND    clm_ev.edw_strt_dttm = a.edw_strt_dttm
  AND    clm_ev.trans_strt_dttm <> clm_ev.trans_end_dttm
  AND    cast(clm_ev.edw_end_dttm AS DATE)=''9999-12-31''
  AND    a.lead IS NOT NULL ;
  
  -- Component exp_pass_to_target11, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target11 AS
  (
         SELECT upd_clm_ev_update.clm_id1             AS clm_id3,
                upd_clm_ev_update.in_edw_strt_dttm1   AS out_edw_end_dttm11,
                upd_clm_ev_update.lkp_edw_strt_dttm3  AS lkp_edw_strt_dttm3,
                upd_clm_ev_update.in_trans_strt_dttm3 AS out_trans_end_dttm11,
                upd_clm_ev_update.source_record_id
         FROM   upd_clm_ev_update );
  -- Component tgt_clm_ev_ins_UPDATE, Type TARGET
  merge
  INTO         db_t_prod_core.clm_ev
  USING        exp_pass_to_target11
  ON (
                            clm_ev.clm_id = exp_pass_to_target11.clm_id3
               AND          clm_ev.edw_strt_dttm = exp_pass_to_target11.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_pass_to_target11.clm_id3,
         edw_strt_dttm = exp_pass_to_target11.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target11.out_edw_end_dttm11,
         trans_end_dttm = exp_pass_to_target11.out_trans_end_dttm11;

END;
';