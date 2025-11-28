-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_CLM_GRP_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_sys_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_GW_CLAIMS_STATUS_V, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_claims_status_v AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS clm_grp_type_cd,
                $2 AS clm_grp_ctlg_cd,
                $3 AS clm_clm_grp_strt_dt,
                $4 AS claim_nbr,
                $5 AS sys_src,
                $6 AS updatetime,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT ''POOL''       AS clm_grp_tye ,
                                                                  cat_pool_ind AS clm_grp_catlog ,
                                                                  decode(trim(cat_pool_act_dt),
                                                                         '''',NULL,
                                                                         trim(cat_pool_act_dt)) AS clm_grp_st_dt,
                                                                  claim_nbr,
                                                                  ''SRC_SYS6'' AS sys_src_cd,
                                                                  updatetime
                                                  FROM            db_t_prod_stag.gw_claims_status_v
                                                  WHERE           cat_pool_ind <> '''' ) src ) );
  -- Component exp_pass_frm_source1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_source1 AS
  (
            SELECT    sq_gw_claims_status_v.clm_grp_type_cd AS clm_grp_type_cd,
                      sq_gw_claims_status_v.clm_grp_ctlg_cd AS clm_grp_ctlg_cd,
                      sq_gw_claims_status_v.claim_nbr       AS cc_claim_clmnumber,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */
                      AS out_sys_src,
                      CASE
                                WHEN sq_gw_claims_status_v.clm_clm_grp_strt_dt IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' )
                                ELSE sq_gw_claims_status_v.clm_clm_grp_strt_dt
                      END                                                                    AS out_clm_grp_strt_dt,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_clm_grp_end_dt,
                      $prcs_id                                                               AS out_prcs_id,
                      sq_gw_claims_status_v.updatetime                                       AS updatetime,
                      sq_gw_claims_status_v.source_record_id,
                      row_number() over (PARTITION BY sq_gw_claims_status_v.source_record_id ORDER BY sq_gw_claims_status_v.source_record_id) AS rnk
            FROM      sq_gw_claims_status_v
            left join lkp_teradata_etl_ref_xlat_sys_src_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_gw_claims_status_v.sys_src qualify rnk = 1 );
  -- Component LKP_CLM_GRP_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_grp_id AS
  (
            SELECT    lkp.clm_grp_id,
                      exp_pass_frm_source1.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_source1.source_record_id ORDER BY lkp.clm_grp_id ASC,lkp.clm_grp_type_cd ASC,lkp.clm_grp_strt_dt ASC,lkp.clm_grp_end_dt ASC,lkp.parnt_clm_grp_id ASC,lkp.clm_grp_ctlg_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_pass_frm_source1
            left join
                      (
                               SELECT   clm_grp.clm_grp_id       AS clm_grp_id,
                                        clm_grp.clm_grp_strt_dt  AS clm_grp_strt_dt,
                                        clm_grp.clm_grp_end_dt   AS clm_grp_end_dt,
                                        clm_grp.parnt_clm_grp_id AS parnt_clm_grp_id,
                                        clm_grp.prcs_id          AS prcs_id,
                                        clm_grp.edw_strt_dttm    AS edw_strt_dttm,
                                        clm_grp.edw_end_dttm     AS edw_end_dttm,
                                        clm_grp.clm_grp_type_cd  AS clm_grp_type_cd,
                                        clm_grp.clm_grp_ctlg_cd  AS clm_grp_ctlg_cd
                               FROM     db_t_prod_core.clm_grp qualify row_number() over(PARTITION BY clm_grp_type_cd,clm_grp_ctlg_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.clm_grp_type_cd = exp_pass_frm_source1.clm_grp_type_cd
            AND       lkp.clm_grp_ctlg_cd = exp_pass_frm_source1.clm_grp_ctlg_cd 
            qualify row_number() over(PARTITION BY exp_pass_frm_source1.source_record_id ORDER BY lkp.clm_grp_id ASC,lkp.clm_grp_type_cd ASC,lkp.clm_grp_strt_dt ASC,lkp.clm_grp_end_dt ASC,lkp.parnt_clm_grp_id ASC,lkp.clm_grp_ctlg_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
                       = 1 );
  -- Component LKP_CLM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm AS
  (
            SELECT    lkp.clm_id,
                      exp_pass_frm_source1.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_source1.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) rnk
            FROM      exp_pass_frm_source1
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
            ON        lkp.clm_num = exp_pass_frm_source1.cc_claim_clmnumber
            AND       lkp.src_sys_cd = exp_pass_frm_source1.out_sys_src 
            qualify row_number() over(PARTITION BY exp_pass_frm_source1.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) 
                        = 1 );
  -- Component exp_pass_frm_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_source AS
  (
             SELECT     lkp_clm_grp_id.clm_grp_id                AS clm_grp_id,
                        lkp_clm.clm_id                           AS clm_id,
                        exp_pass_frm_source1.out_clm_grp_strt_dt AS clm_clm_grp_strt_dt,
                        exp_pass_frm_source1.out_clm_grp_end_dt  AS clm_clm_grp_end_dt,
                        exp_pass_frm_source1.updatetime          AS updatetime,
                        exp_pass_frm_source1.source_record_id
             FROM       exp_pass_frm_source1
             inner join lkp_clm_grp_id
             ON         exp_pass_frm_source1.source_record_id = lkp_clm_grp_id.source_record_id
             inner join lkp_clm
             ON         lkp_clm_grp_id.source_record_id = lkp_clm.source_record_id );
  -- Component LKP_CLM_CLM_GRP, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_clm_grp AS
  (
            SELECT    lkp.clm_grp_id,
                      lkp.clm_id,
                      exp_pass_frm_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY lkp.clm_grp_id DESC,lkp.clm_id DESC) rnk
            FROM      exp_pass_frm_source
            left join
                      (
                             SELECT clm_clm_grp.clm_grp_id AS clm_grp_id,
                                    clm_clm_grp.clm_id     AS clm_id
                             FROM   db_t_prod_core.clm_clm_grp
                             WHERE  cast(trans_end_dttm AS DATE)=''9999-12-31'' ) lkp
            ON        lkp.clm_id = exp_pass_frm_source.clm_id 
            qualify row_number() over(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY lkp.clm_grp_id DESC,lkp.clm_id DESC) 
            = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     exp_pass_frm_source.clm_grp_id                     AS clm_grp_id,
                        exp_pass_frm_source.clm_id                         AS clm_id,
                        exp_pass_frm_source.clm_clm_grp_strt_dt            AS clm_clm_grp_strt_dt,
                        exp_pass_frm_source.clm_clm_grp_end_dt             AS clm_clm_grp_end_dt,
                        $prcs_id                                           AS prcs_id,
                        md5 ( to_char ( exp_pass_frm_source.clm_grp_id ) ) AS in_chksum,
                        md5 ( to_char ( lkp_clm_clm_grp.clm_grp_id ) )     AS lkp_chksum,
                        CASE
                                   WHEN lkp_clm_clm_grp.clm_id IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN in_chksum != lkp_chksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END               AS o_ins_upd,
                        current_timestamp AS out_trans_strt_dttm,
                        CASE
                                   WHEN exp_pass_frm_source.updatetime IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )
                                   ELSE exp_pass_frm_source.updatetime
                        END AS out_trans_strt_dttm1,
                        exp_pass_frm_source.source_record_id
             FROM       exp_pass_frm_source
             inner join lkp_clm_clm_grp
             ON         exp_pass_frm_source.source_record_id = lkp_clm_clm_grp.source_record_id );
  -- Component rtr_clm_clm_grp_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_clm_clm_grp_insert as
  SELECT exp_data_transformation.clm_grp_id           AS clm_grp_id,
         exp_data_transformation.clm_id               AS clm_id,
         exp_data_transformation.clm_clm_grp_strt_dt  AS clm_clm_grp_strt_dt,
         exp_data_transformation.clm_clm_grp_end_dt   AS clm_clm_grp_end_dt,
         exp_data_transformation.prcs_id              AS prcs_id,
         exp_data_transformation.o_ins_upd            AS ins_upd_flag,
         exp_data_transformation.out_trans_strt_dttm  AS out_trans_strt_dttm,
         exp_data_transformation.out_trans_strt_dttm1 AS trans_strt_dttm,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.o_ins_upd = ''I''
  OR     exp_data_transformation.o_ins_upd = ''U'';
  
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_clm_grp_insert.clm_grp_id          AS clm_grp_id1,
                rtr_clm_clm_grp_insert.clm_id              AS clm_id1,
                rtr_clm_clm_grp_insert.clm_clm_grp_strt_dt AS clm_clm_grp_strt_dt1,
                rtr_clm_clm_grp_insert.clm_clm_grp_end_dt  AS clm_clm_grp_end_dt1,
                rtr_clm_clm_grp_insert.prcs_id             AS prcs_id1,
                rtr_clm_clm_grp_insert.out_trans_strt_dttm AS out_trans_strt_dttm1,
                rtr_clm_clm_grp_insert.trans_strt_dttm     AS trans_strt_dttm,
                0                                  AS update_strategy_action,
                rtr_clm_clm_grp_insert.source_record_id
         FROM   rtr_clm_clm_grp_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_ins.clm_grp_id1                                                    AS clm_grp_id1,
                upd_ins.clm_id1                                                        AS clm_id1,
                upd_ins.clm_clm_grp_strt_dt1                                           AS clm_clm_grp_strt_dt1,
                upd_ins.clm_clm_grp_end_dt1                                            AS clm_clm_grp_end_dt1,
                upd_ins.prcs_id1                                                       AS prcs_id1,
                upd_ins.trans_strt_dttm                                                AS trans_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_trans_end_dttm,
                upd_ins.source_record_id
         FROM   upd_ins );
  -- Component CLM_CLM_GRP_ins, Type TARGET
  INSERT INTO db_t_prod_core.clm_clm_grp
              (
                          clm_grp_id,
                          clm_id,
                          clm_clm_grp_strt_dt,
                          clm_clm_grp_end_dt,
                          prcs_id,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.clm_grp_id1          AS clm_grp_id,
         exp_pass_to_tgt_ins.clm_id1              AS clm_id,
         exp_pass_to_tgt_ins.clm_clm_grp_strt_dt1 AS clm_clm_grp_strt_dt,
         exp_pass_to_tgt_ins.clm_clm_grp_end_dt1  AS clm_clm_grp_end_dt,
         exp_pass_to_tgt_ins.prcs_id1             AS prcs_id,
         exp_pass_to_tgt_ins.trans_strt_dttm      AS trans_strt_dttm,
         exp_pass_to_tgt_ins.out_trans_end_dttm   AS trans_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component CLM_CLM_GRP_ins, Type Post SQL
  UPDATE db_t_prod_core.clm_clm_grp
 SET    trans_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT clm_id,
                                         clm_grp_id,
                                         clm_clm_grp_strt_dt,
                                         clm_clm_grp_end_dt,
                                         trans_strt_dttm,
                                         prcs_id,
                                         max(trans_strt_dttm) over (PARTITION BY clm_id ORDER BY prcs_id ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.clm_clm_grp ) a
 
  WHERE  clm_clm_grp.clm_id=a.clm_id
  AND    clm_clm_grp.clm_grp_id=a.clm_grp_id
  AND    clm_clm_grp.clm_clm_grp_strt_dt=a.clm_clm_grp_strt_dt
  AND    clm_clm_grp.clm_clm_grp_end_dt=a.clm_clm_grp_end_dt
  AND    clm_clm_grp.trans_strt_dttm=a.trans_strt_dttm
  AND    lead IS NOT NULL;

END;
';