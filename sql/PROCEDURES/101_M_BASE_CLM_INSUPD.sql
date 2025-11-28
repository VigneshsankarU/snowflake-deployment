-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_INSUPD("PARAM_JSON" VARCHAR)
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
                $1  AS claimnumber,
                $2  AS faultrating,
                $3  AS hspindicator,
                $4  AS coverageinquestion,
                $5  AS verified,
                $6  AS retired,
                $7  AS src_cd,
                $8  AS createtime,
                $9  AS closedate,
                $10 AS updatetime,
                $11 AS legacyclaimnumber_alfa,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cc_claim.claimnumber_stg AS claimnumber,
                                                                  cc_claim.faultrating_stg AS faultrating,
                                                                  hspindicator,
                                                                  /*cc_claim.CoverageInQuestion_stg */
                                                                  CASE
                                                                                  WHEN cc_claim.coverageinquestion_stg = 0 THEN ''F''
                                                                                  WHEN cc_claim.coverageinquestion_stg = 1 THEN ''T''
                                                                                  ELSE NULL
                                                                  END AS coverageinquestion,
                                                                  /*cc_policy.Verified_stg*/
                                                                  0                                   AS verified,
                                                                  cc_claim.retired_stg                AS retired,
                                                                  ''SRC_SYS6''                          AS src_cd,
                                                                  cc_claim.createtime_stg             AS createtime,
                                                                  cc_claim.closedate_stg              AS closedate,
                                                                  cc_claim.updatetime_stg             AS updatetime,
                                                                  cc_claim.legacyclaimnumber_alfa_stg AS legacyclaimnumber_alfa
                                                  FROM            (
                                                                                  SELECT DISTINCT cc_claim.claimnumber_stg,
                                                                                                  cc_claim.faultrating_stg,
                                                                                                  cc_claim.coverageinquestion_stg,
                                                                                                  cc_claim.retired_stg,
                                                                                                  cc_claim.createtime_stg,
                                                                                                  cc_claim.closedate_stg,
                                                                                                  cc_claim.updatetime_stg,
                                                                                                  cc_claim.legacyclaimnumber_alfa_stg,
                                                                                                  cc_claim.policyid_stg,
                                                                                                  hsp.hspindicator_stg AS hspindicator
                                                                                  FROM            db_t_prod_stag.cc_claim
                                                                                  inner join      db_t_prod_stag.cctl_claimstate
                                                                                  ON              cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                  left join       db_t_prod_stag.ccx_homesystemprotect_alfa hsp
                                                                                  ON              cc_claim.id_stg = hsp.claim_stg
                                                                                  WHERE           cctl_claimstate.name_stg <> ''Draft''
                                                                                  AND             cc_claim.updatetime_stg>($start_dttm)
                                                                                  AND             cc_claim.updatetime_stg <= ($end_dttm) ) cc_claim
                                                  left join       db_t_prod_stag.cc_policy
                                                  ON              cc_policy.id_stg=cc_claim.policyid_stg ) src ) );
  -- Component exp_pass_verified_value, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_verified_value AS
  (
         SELECT sq_cc_claim.claimnumber AS claimnumber,
                ''UNK''                   AS clm__mdia_type,
                CASE
                       WHEN sq_cc_claim.verified = 1 THEN ''Y''
                       ELSE ''N''
                END                     AS verified_out,
                $prcs_id                AS prcs_id,
                NULL                    AS typecode,
                sq_cc_claim.faultrating AS faultrating,
                decode ( sq_cc_claim.hspindicator ,
                        1 , ''Y'' ,
                        0 , ''N'' )                                 AS out_hspindicator,
                sq_cc_claim.coverageinquestion                    AS coverageinquestion,
                sq_cc_claim.retired                               AS retired,
                sq_cc_claim.src_cd                                AS src_cd,
                sq_cc_claim.createtime                            AS createtime,
                to_char ( sq_cc_claim.createtime , ''YYYY-MM-DD'' ) AS v_createtime,
                to_char ( sq_cc_claim.closedate , ''YYYY-MM-DD'' )  AS v_closedate,
                to_date ( v_closedate , ''YYYY-MM-DD'' )            AS o_closedate,
                sq_cc_claim.legacyclaimnumber_alfa                AS legacyclaimnumber_alfa,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_verified_value.source_record_id,
                      row_number() over(PARTITION BY exp_pass_verified_value.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_verified_value
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EXTNT_OF_FIRE_DMG_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''cctl_damageextent_alfa.typecode''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_verified_value.typecode qualify rnk = 1 );
  -- Component exp_convert_typecode, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_convert_typecode AS
  (
         SELECT
                CASE
                       WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN ''UNK''
                       ELSE lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                END AS out_tgt_ref_type_cd,
                lkp_teradata_etl_ref_xlat.source_record_id
         FROM   lkp_teradata_etl_ref_xlat );
  -- Component exp_collect_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_collect_data AS
  (
             SELECT     exp_pass_verified_value.claimnumber    AS claimnumber,
                        exp_pass_verified_value.clm__mdia_type AS clm__mdia_type,
                        exp_pass_verified_value.verified_out   AS verified_out,
                        exp_pass_verified_value.prcs_id        AS prcs_id,
                        CASE
                                   WHEN exp_pass_verified_value.faultrating IS NULL THEN NULL
                                   ELSE decode ( exp_pass_verified_value.faultrating ,
                                                10001 , ''NO'' ,
                                                10002 , ''YES'' )
                        END                                                                    AS v_faultrating,
                        exp_pass_verified_value.out_hspindicator                               AS hspindicator,
                        exp_pass_verified_value.coverageinquestion                             AS coverageinquestion,
                        exp_convert_typecode.out_tgt_ref_type_cd                               AS out_tgt_ref_type_cd,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        exp_pass_verified_value.retired                                        AS retired,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                       AS out_src_cd,
                        ''CLM''                                          AS out_type_cd,
                        exp_pass_verified_value.legacyclaimnumber_alfa AS legacyclaimnumber_alfa,
                        exp_pass_verified_value.source_record_id,
                        row_number() over (PARTITION BY exp_pass_verified_value.source_record_id ORDER BY exp_pass_verified_value.source_record_id) AS rnk
             FROM       exp_pass_verified_value
             inner join exp_convert_typecode
             ON         exp_pass_verified_value.source_record_id = exp_convert_typecode.source_record_id
             left join  lkp_teradata_etl_ref_xlat_src_cd lkp_1
             ON         lkp_1.src_idntftn_val = exp_pass_verified_value.src_cd 
			 qualify row_number() over (PARTITION BY exp_pass_verified_value.source_record_id ORDER BY exp_pass_verified_value.source_record_id) 
			 = 1 );
  -- Component LKP_CLM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm AS
  (
            SELECT    lkp.clm_id,
                      lkp.clm_type_cd,
                      lkp.clm_mdia_type_cd,
                      lkp.clm_submtl_type_cd,
                      lkp.acdnt_type_cd,
                      lkp.clm_ctgy_type_cd,
                      lkp.addl_insrnc_pln_ind,
                      lkp.emplmt_rltd_ind,
                      lkp.attny_invlvmt_ind,
                      lkp.clm_num,
                      lkp.clm_prir_ind,
                      lkp.pmt_mode_cd,
                      lkp.clm_oblgtn_type_cd,
                      lkp.subrgtn_elgbl_cd,
                      lkp.subrgtn_elgbly_rsn_cd,
                      lkp.cury_cd,
                      lkp.incdt_ev_id,
                      lkp.insrd_at_fault_ind,
                      lkp.cvge_in_ques_ind,
                      lkp.extnt_of_fire_dmg_type_cd,
                      lkp.vfyd_clm_ind,
                      lkp.prcs_id,
                      lkp.clm_strt_dttm,
                      lkp.clm_end_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      lkp.src_sys_cd,
                      lkp.trans_strt_dttm,
                      lkp.lgcy_clm_num,
                      exp_collect_data.out_src_cd AS in_src_sys_cd,
                      exp_collect_data.source_record_id,
                      row_number() over(PARTITION BY exp_collect_data.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) rnk
            FROM      exp_collect_data
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
            ON        lkp.clm_num = exp_collect_data.claimnumber
            AND       lkp.src_sys_cd = exp_collect_data.out_src_cd 
			qualify row_number() over(PARTITION BY exp_collect_data.source_record_id ORDER BY lkp.clm_id DESC,lkp.clm_type_cd DESC,lkp.clm_mdia_type_cd DESC,lkp.clm_submtl_type_cd DESC,lkp.acdnt_type_cd DESC,lkp.clm_ctgy_type_cd DESC,lkp.addl_insrnc_pln_ind DESC,lkp.emplmt_rltd_ind DESC,lkp.attny_invlvmt_ind DESC,lkp.clm_num DESC,lkp.clm_prir_ind DESC,lkp.pmt_mode_cd DESC,lkp.clm_oblgtn_type_cd DESC,lkp.subrgtn_elgbl_cd DESC,lkp.subrgtn_elgbly_rsn_cd DESC,lkp.cury_cd DESC,lkp.incdt_ev_id DESC,lkp.insrd_at_fault_ind DESC,lkp.cvge_in_ques_ind DESC,lkp.extnt_of_fire_dmg_type_cd DESC,lkp.vfyd_clm_ind DESC,lkp.prcs_id DESC,lkp.clm_strt_dttm DESC,lkp.clm_end_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.src_sys_cd DESC,lkp.trans_strt_dttm DESC,lkp.lgcy_clm_num DESC) 
            			= 1 );
  -- Component LKP_XREF_CLM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_clm AS
  (
            SELECT    lkp.clm_id,
                      exp_collect_data.source_record_id,
                      row_number() over(PARTITION BY exp_collect_data.source_record_id ORDER BY lkp.clm_id DESC,lkp.nk_src_key DESC,lkp.dir_clm_val DESC) rnk
            FROM      exp_collect_data
            left join
                      (
                             SELECT clm_id,
                                    nk_src_key,
                                    dir_clm_val
                             FROM   db_t_prod_core.dir_clm ) lkp
            ON        lkp.nk_src_key = exp_collect_data.claimnumber
            AND       lkp.dir_clm_val = exp_collect_data.out_type_cd 
			qualify row_number() over(PARTITION BY exp_collect_data.source_record_id ORDER BY lkp.clm_id DESC,lkp.nk_src_key DESC,lkp.dir_clm_val DESC) 
			= 1 );
  -- Component exp_comp_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_comp_data AS
  (
             SELECT     lkp_clm.src_sys_cd    AS lkp_clm_src_cd,
                        lkp_clm.edw_strt_dttm AS lkp_edw_strt_dttm_upd,
                        lkp_clm.edw_end_dttm  AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( ( ltrim ( rtrim ( lkp_clm.clm_mdia_type_cd ) )
                                   || ltrim ( rtrim ( lkp_clm.clm_mdia_type_cd ) )
                                   || ltrim ( rtrim ( lkp_clm.insrd_at_fault_ind ) )
                                   || ltrim ( rtrim ( lkp_clm.cvge_in_ques_ind )
                                   || ltrim ( rtrim ( lkp_clm.vfyd_clm_ind ) ) ) )
                                   || ltrim ( rtrim ( lkp_clm.clm_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_clm.clm_end_dttm ) )
                                   || ltrim ( rtrim ( lkp_clm.lgcy_clm_num ) ) ) ) ) AS v_lkp_checksum,
                        exp_collect_data.prcs_id                                     AS in_prcs_id,
                        exp_collect_data.edw_strt_dttm                               AS in_edw_strt_dttm,
                        exp_collect_data.edw_end_dttm                                AS in_edw_end_dttm,
                        exp_collect_data.clm__mdia_type                              AS in_clm__mdia_type,
                        exp_collect_data.verified_out                                AS in_verified_out,
                        exp_collect_data.v_faultrating                               AS in_faultrating,
                        exp_collect_data.hspindicator                                AS hspindicator,
                        exp_collect_data.coverageinquestion                          AS in_coverageinquestion,
                        exp_collect_data.out_tgt_ref_type_cd                         AS in_tgt_ref_type_cd,
                        exp_collect_data.claimnumber                                 AS in_claimnumber,
                        exp_pass_verified_value.createtime                           AS createtime,
                        exp_pass_verified_value.o_closedate                          AS closedate,
                        CASE
                                   WHEN exp_pass_verified_value.createtime IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' )
                                   ELSE exp_pass_verified_value.createtime
                        END AS v_clm_strt_dt,
                        CASE
                                   WHEN exp_pass_verified_value.o_closedate IS NULL THEN to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                   ELSE exp_pass_verified_value.o_closedate
                        END           AS v_clm_end_dt,
                        v_clm_strt_dt AS in_clm_strt_dt,
                        v_clm_end_dt  AS in_clm_end_dt,
                        md5 ( ltrim ( rtrim ( ( ltrim ( rtrim ( exp_collect_data.clm__mdia_type ) )
                                   || ltrim ( rtrim ( exp_collect_data.clm__mdia_type ) )
                                   || ltrim ( rtrim ( exp_collect_data.v_faultrating ) )
                                   || ltrim ( rtrim ( exp_collect_data.coverageinquestion ) )
                                   || ltrim ( rtrim ( exp_collect_data.verified_out ) ) )
                                   || ltrim ( rtrim ( v_clm_strt_dt ) )
                                   || ltrim ( rtrim ( v_clm_end_dt ) )
                                   || ltrim ( rtrim ( exp_collect_data.legacyclaimnumber_alfa ) ) ) ) ) AS v_in_checksum,
                        CASE
                                   WHEN v_lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_lkp_checksum != v_in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                     AS calc_ins_upd,
                        lkp_clm.in_src_sys_cd                   AS in_clm_src_cd,
                        exp_collect_data.retired                AS retired,
                        sq_cc_claim.updatetime                  AS updatetime,
                        exp_collect_data.legacyclaimnumber_alfa AS legacyclaimnumber_alfa,
                        sq_cc_claim.source_record_id
             FROM       sq_cc_claim
             inner join exp_pass_verified_value
             ON         sq_cc_claim.source_record_id = exp_pass_verified_value.source_record_id
             inner join exp_collect_data
             ON         exp_pass_verified_value.source_record_id = exp_collect_data.source_record_id
             inner join lkp_clm
             ON         exp_collect_data.source_record_id = lkp_clm.source_record_id );
  -- Component xref_exp, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE xref_exp AS
  (
         SELECT lkp_xref_clm.clm_id AS id,
                lkp_xref_clm.source_record_id
         FROM   lkp_xref_clm );
  -- Component rtr_data_ins_upd_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_data_ins_upd_Insert as
  SELECT    lkp_clm.clm_id                       AS clm_id,
            xref_exp.id                          AS xref_clm_id,
            exp_comp_data.in_clm__mdia_type      AS in_clm__mdia_type,
            exp_comp_data.in_verified_out        AS in_verified_out,
            exp_comp_data.in_faultrating         AS in_faultrating,
            exp_comp_data.hspindicator           AS in_hspindicator,
            exp_comp_data.in_coverageinquestion  AS in_coverageinquestion,
            exp_comp_data.in_tgt_ref_type_cd     AS in_tgt_ref_type_cd,
            exp_comp_data.in_clm_src_cd          AS in_clm_src_cd,
            exp_comp_data.calc_ins_upd           AS calc_ins_upd,
            exp_comp_data.in_prcs_id             AS in_prcs_id,
            exp_comp_data.in_edw_strt_dttm       AS in_edw_strt_dttm,
            exp_comp_data.in_edw_end_dttm        AS in_edw_end_dttm,
            exp_comp_data.in_claimnumber         AS in_claimnumber,
            exp_comp_data.lkp_edw_strt_dttm_upd  AS lkp_edw_strt_dttm_upd,
            exp_comp_data.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
            exp_comp_data.lkp_clm_src_cd         AS lkp_clm_src_cd,
            exp_comp_data.retired                AS retired,
            exp_comp_data.in_clm_strt_dt         AS in_clm_strt_dt,
            exp_comp_data.in_clm_end_dt          AS in_clm_end_dt,
            exp_comp_data.updatetime             AS updatetime,
            lkp_clm.trans_strt_dttm              AS trans_strt_dttm,
            exp_comp_data.legacyclaimnumber_alfa AS legacyclaimnumber_alfa,
            lkp_clm.source_record_id
  FROM      lkp_clm
  left join exp_comp_data
  ON        lkp_clm.source_record_id = exp_comp_data.source_record_id
  left join xref_exp
  ON        exp_comp_data.source_record_id = xref_exp.source_record_id
  WHERE     (
                      exp_comp_data.calc_ins_upd = ''I'' )
  OR        (
                      exp_comp_data.retired = 0
            AND       exp_comp_data.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
  OR        (
                      exp_comp_data.calc_ins_upd = ''U''
            AND       exp_comp_data.retired = 0
            AND       exp_comp_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_data_ins_upd_Retired, Type ROUTER Output Group Retired
  create or replace temporary table rtr_data_ins_upd_retired as
  SELECT    lkp_clm.clm_id                       AS clm_id,
            xref_exp.id                          AS xref_clm_id,
            exp_comp_data.in_clm__mdia_type      AS in_clm__mdia_type,
            exp_comp_data.in_verified_out        AS in_verified_out,
            exp_comp_data.in_faultrating         AS in_faultrating,
            exp_comp_data.hspindicator           AS in_hspindicator,
            exp_comp_data.in_coverageinquestion  AS in_coverageinquestion,
            exp_comp_data.in_tgt_ref_type_cd     AS in_tgt_ref_type_cd,
            exp_comp_data.in_clm_src_cd          AS in_clm_src_cd,
            exp_comp_data.calc_ins_upd           AS calc_ins_upd,
            exp_comp_data.in_prcs_id             AS in_prcs_id,
            exp_comp_data.in_edw_strt_dttm       AS in_edw_strt_dttm,
            exp_comp_data.in_edw_end_dttm        AS in_edw_end_dttm,
            exp_comp_data.in_claimnumber         AS in_claimnumber,
            exp_comp_data.lkp_edw_strt_dttm_upd  AS lkp_edw_strt_dttm_upd,
            exp_comp_data.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
            exp_comp_data.lkp_clm_src_cd         AS lkp_clm_src_cd,
            exp_comp_data.retired                AS retired,
            exp_comp_data.in_clm_strt_dt         AS in_clm_strt_dt,
            exp_comp_data.in_clm_end_dt          AS in_clm_end_dt,
            exp_comp_data.updatetime             AS updatetime,
            lkp_clm.trans_strt_dttm              AS trans_strt_dttm,
            exp_comp_data.legacyclaimnumber_alfa AS legacyclaimnumber_alfa,
            lkp_clm.source_record_id
  FROM      lkp_clm
  left join exp_comp_data
  ON        lkp_clm.source_record_id = exp_comp_data.source_record_id
  left join xref_exp
  ON        exp_comp_data.source_record_id = xref_exp.source_record_id
  WHERE     exp_comp_data.calc_ins_upd = ''R''
  AND       exp_comp_data.retired != 0
  AND       exp_comp_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_CLM_update_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_update_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_data_ins_upd_retired.clm_id                AS clm_id4,
                rtr_data_ins_upd_retired.in_edw_strt_dttm      AS in_edw_strt_dttm4,
                rtr_data_ins_upd_retired.lkp_edw_strt_dttm_upd AS lkp_edw_strt_dttm_upd4,
                rtr_data_ins_upd_retired.lkp_edw_end_dttm      AS lkp_edw_end_dttm4,
                rtr_data_ins_upd_retired.in_prcs_id            AS in_prcs_id4,
                rtr_data_ins_upd_retired.lkp_clm_src_cd        AS lkp_clm_src_cd4,
                rtr_data_ins_upd_retired.updatetime            AS updatetime4,
                NULL                                           AS legacyclaimnumber_alfa2,
                rtr_data_ins_upd_retired.in_hspindicator       AS in_hspindicator3,
                1                                              AS update_strategy_action,
				rtr_data_ins_upd_retired.source_record_id
         FROM   rtr_data_ins_upd_retired );
  -- Component upd_CLM_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_clm_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_data_ins_upd_insert.xref_clm_id            AS clm_id1,
                rtr_data_ins_upd_insert.in_clm__mdia_type      AS in_clm__mdia_type1,
                rtr_data_ins_upd_insert.in_verified_out        AS in_verified_out1,
                rtr_data_ins_upd_insert.in_faultrating         AS in_faultrating1,
                rtr_data_ins_upd_insert.in_hspindicator        AS in_hspindicator1,
                rtr_data_ins_upd_insert.in_coverageinquestion  AS in_coverageinquestion1,
                rtr_data_ins_upd_insert.in_tgt_ref_type_cd     AS in_tgt_ref_type_cd1,
                rtr_data_ins_upd_insert.in_prcs_id             AS in_prcs_id1,
                rtr_data_ins_upd_insert.in_edw_strt_dttm       AS in_edw_strt_dttm1,
                rtr_data_ins_upd_insert.in_edw_end_dttm        AS in_edw_end_dttm1,
                rtr_data_ins_upd_insert.in_claimnumber         AS in_claimnumber,
                rtr_data_ins_upd_insert.in_clm_src_cd          AS in_clm_src_cd1,
                rtr_data_ins_upd_insert.retired                AS retired1,
                rtr_data_ins_upd_insert.in_clm_strt_dt         AS in_clm_strt_dt1,
                rtr_data_ins_upd_insert.in_clm_end_dt          AS in_clm_end_dt1,
                rtr_data_ins_upd_insert.updatetime             AS updatetime1,
                rtr_data_ins_upd_insert.legacyclaimnumber_alfa AS legacyclaimnumber_alfa2,
                0                                              AS update_strategy_action,
				rtr_data_ins_upd_insert.source_record_id
         FROM   rtr_data_ins_upd_insert );
  -- Component exp_clm_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_insert AS
  (
         SELECT upd_clm_insert.clm_id1                AS clm_id1,
                upd_clm_insert.in_clm__mdia_type1     AS in_clm__mdia_type1,
                upd_clm_insert.in_verified_out1       AS in_verified_out1,
                upd_clm_insert.in_faultrating1        AS in_faultrating1,
                upd_clm_insert.in_hspindicator1       AS in_hspindicator1,
                upd_clm_insert.in_coverageinquestion1 AS in_coverageinquestion1,
                upd_clm_insert.in_prcs_id1            AS in_prcs_id1,
                upd_clm_insert.in_edw_strt_dttm1      AS in_edw_strt_dttm1,
                upd_clm_insert.in_claimnumber         AS in_claimnumber,
                upd_clm_insert.in_clm_src_cd1         AS in_clm_src_cd1,
                CASE
                       WHEN upd_clm_insert.retired1 != 0 THEN current_timestamp
                       ELSE upd_clm_insert.in_edw_end_dttm1
                END                            AS o_edw_end_dttm1,
                upd_clm_insert.in_clm_strt_dt1 AS in_clm_strt_dt1,
                upd_clm_insert.in_clm_end_dt1  AS in_clm_end_dt1,
                upd_clm_insert.updatetime1     AS trans_strt_dttm,
                CASE
                       WHEN upd_clm_insert.retired1 != 0 THEN upd_clm_insert.updatetime1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                    AS trans_end_dttm,
                upd_clm_insert.legacyclaimnumber_alfa2 AS legacyclaimnumber_alfa2,
                upd_clm_insert.source_record_id
         FROM   upd_clm_insert );
  -- Component exp_update_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_update_retired AS
  (
         SELECT upd_clm_update_retired.clm_id4                AS clm_id4,
                upd_clm_update_retired.lkp_edw_strt_dttm_upd4 AS in_edw_strt_dttm4,
                current_timestamp                             AS edw_end_dttm,
                upd_clm_update_retired.lkp_clm_src_cd4        AS lkp_clm_src_cd4,
                upd_clm_update_retired.updatetime4            AS updatetime4,
                upd_clm_update_retired.source_record_id
         FROM   upd_clm_update_retired );
  -- Component tgt_clm_insert, Type TARGET
  INSERT INTO db_t_prod_core.clm
              (
                          clm_id,
                          clm_mdia_type_cd,
                          addl_insrnc_pln_ind,
                          clm_num,
                          cury_cd,
                          insrd_at_fault_ind,
                          cvge_in_ques_ind,
                          vfyd_clm_ind,
                          lgcy_clm_num,
                          prcs_id,
                          clm_strt_dttm,
                          clm_end_dttm,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_clm_insert.clm_id1                 AS clm_id,
         exp_clm_insert.in_clm__mdia_type1      AS clm_mdia_type_cd,
         exp_clm_insert.in_hspindicator1        AS addl_insrnc_pln_ind,
         exp_clm_insert.in_claimnumber          AS clm_num,
         exp_clm_insert.in_clm__mdia_type1      AS cury_cd,
         exp_clm_insert.in_faultrating1         AS insrd_at_fault_ind,
         exp_clm_insert.in_coverageinquestion1  AS cvge_in_ques_ind,
         exp_clm_insert.in_verified_out1        AS vfyd_clm_ind,
         exp_clm_insert.legacyclaimnumber_alfa2 AS lgcy_clm_num,
         exp_clm_insert.in_prcs_id1             AS prcs_id,
         exp_clm_insert.in_clm_strt_dt1         AS clm_strt_dttm,
         exp_clm_insert.in_clm_end_dt1          AS clm_end_dttm,
         exp_clm_insert.in_clm_src_cd1          AS src_sys_cd,
         exp_clm_insert.in_edw_strt_dttm1       AS edw_strt_dttm,
         exp_clm_insert.o_edw_end_dttm1         AS edw_end_dttm,
         exp_clm_insert.trans_strt_dttm         AS trans_strt_dttm,
         exp_clm_insert.trans_end_dttm          AS trans_end_dttm
  FROM   exp_clm_insert;
  
  -- Component tgt_clm_insert, Type Post SQL
  UPDATE db_t_prod_core.clm
    SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT clm_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY clm_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.clm ) a

  WHERE  clm.edw_strt_dttm = a.edw_strt_dttm
  AND    clm.clm_id=a.clm_id
  AND    lead1 IS NOT NULL;
  
  -- Component tgt_clm_update_rejected_retired, Type TARGET
  merge
  INTO         db_t_prod_core.clm
  USING        exp_update_retired
  ON (
                            clm.clm_id = exp_update_retired.clm_id4)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_update_retired.clm_id4,
         src_sys_cd = exp_update_retired.lkp_clm_src_cd4,
         edw_strt_dttm = exp_update_retired.in_edw_strt_dttm4,
         edw_end_dttm = exp_update_retired.edw_end_dttm,
         trans_end_dttm = exp_update_retired.updatetime4;

END;
';