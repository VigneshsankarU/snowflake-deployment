-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_POLICYNUMBER_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component sq_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_agmt_id,
                $2  AS lkp_agmt_eff_dttm,
                $3  AS lkp_agmt_opn_dttm,
                $4  AS lkp_agmt_plnd_expn_dt,
                $5  AS lkp_agmt_signd_dt,
                $6  AS lkp_agmt_type_cd,
                $7  AS lkp_agmt_cur_sts_cd,
                $8  AS lkp_agmt_cur_sts_rsn_cd,
                $9  AS lkp_agmt_obtnd_cd,
                $10 AS lkp_agmt_sbtype_cd,
                $11 AS lkp_agmt_prcsg_dt,
                $12 AS lkp_stmt_cycl_cd,
                $13 AS lkp_agmt_objtv_type_cd,
                $14 AS lkp_mkt_risk_type_cd,
                $15 AS lkp_ntwk_srvc_agmt_type_cd,
                $16 AS lkp_frmlty_type_cd,
                $17 AS lkp_agmt_idntftn_cd,
                $18 AS lkp_trmtn_type_cd,
                $19 AS lkp_int_pmt_meth_cd,
                $20 AS lkp_pmt_pln_type_cd,
                $21 AS lkp_modl_crtn_dttm,
                $22 AS lkp_cntnus_srvc_dt,
                $23 AS lkp_agmt_legly_bindg_ind,
                $24 AS lkp_lgcy_dscnt_ind,
                $25 AS lkp_bilg_meth_type_cd,
                $26 AS lkp_modl_eff_dttm,
                $27 AS lkp_modl_actl_end_dttm,
                $28 AS lkp_tier_type_cd,
                $29 AS lkp_edw_end_dttm,
                $30 AS lkp_vfyd_plcy_ind,
                $31 AS lkp_nk_src_key,
                $32 AS policynumber,
                $33 AS modelnumber,
                $34 AS termnumber,
                $35 AS periodstart,
                $36 AS originaleffectivedate,
                $37 AS periodend,
                $38 AS issuedate,
                $39 AS processingdate,
                $40 AS out_agmt_type_cd,
                $41 AS out_agmt_cur_sts_cd,
                $42 AS out_agmt_cur_sts_rsn_cd,
                $43 AS out_agmt_obtnd_cd,
                $44 AS out_agmt_sbtype_cd,
                $45 AS out_agmt_objtv_type_cd,
                $46 AS out_mkt_risk_type_cd,
                $47 AS out_ntwk_srvc_agmt_type_cd,
                $48 AS out_frmlty_type_cd,
                $49 AS out_agmt_idntftn_cd,
                $50 AS out_trmtn_type_cd,
                $51 AS out_int_pmt_meth_cd,
                $52 AS out_prcs_id,
                $53 AS out_pmt_pln_type_cd,
                $54 AS out_bilg_meth_type_cd,
                $55 AS out_stmt_cycle_cd,
                $56 AS editeffectivedate,
                $57 AS model_actl_end_ddtm,
                $58 AS modeldate,
                $59 AS publicid,
                $60 AS previnsurance_alfa,
                $61 AS continuousservicedate_alfa,
                $62 AS edw_strt_dttm1,
                $63 AS legacydiscount_alfa,
                $64 AS edw_end_dttm,
                $65 AS in_tier_type,
                $66 AS vfyd_plcy_ind,
                $67 AS updatetime,
                $68 AS out_src_cd,
                $69 AS lkp_src_of_busn_cd,
                $70 AS lkp_edw_strt_dttm,
                $71 AS retired,
                $72 AS out_override_commission_value,
                $73 AS in_legacypolind_alfa,
                $74 AS out_legacypolind_alfa,
                $75 AS out_sourceofbusiness_alfa_typecode,
                $76 AS stmt_cycle_cd,
                $77 AS v_agmt_src_cd,
                $78 AS v_lkp_agmt_src_cd,
                $79 AS agmt_src_cd,
                $80 AS lkp_ovrd_coms_type_cd,
                $81 AS lkp_trans_strt_dttm,
                $82 AS lkp_in_lgcy_plcy_ind,
                $83 AS sub_lkp_checksum,
                $84 AS lkp_main_checksum,
                $85 AS v_orig_chcksm,
                $86 AS sub_v_checksum,
                $87 AS main_checksum,
                $88 AS calc_chksm,
                $89 AS out_ins_upd,
                $90 AS agmt_id,
                $91 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    lkp_agmt.agmt_id                AS lkp_agmt_id,
                                                      lkp_agmt.agmt_eff_dttm          AS lkp_agmt_eff_dttm,
                                                      lkp_agmt.agmt_opn_dttm          AS lkp_agmt_opn_dttm,
                                                      lkp_agmt.agmt_plnd_expn_dttm    AS lkp_agmt_plnd_expn_dt,
                                                      lkp_agmt.agmt_signd_dttm        AS lkp_agmt_signd_dt,
                                                      lkp_agmt.agmt_type_cd           AS lkp_agmt_type_cd,
                                                      lkp_agmt.agmt_cur_sts_cd        AS lkp_agmt_cur_sts_cd,
                                                      lkp_agmt.agmt_cur_sts_rsn_cd    AS lkp_agmt_cur_sts_rsn_cd,
                                                      lkp_agmt.agmt_obtnd_cd          AS lkp_agmt_obtnd_cd,
                                                      lkp_agmt.agmt_sbtype_cd         AS lkp_agmt_sbtype_cd,
                                                      lkp_agmt.agmt_prcsg_dttm        AS lkp_agmt_prcsg_dt,
                                                      lkp_agmt.stmt_cycl_cd           AS lkp_stmt_cycl_cd,
                                                      lkp_agmt.agmt_objtv_type_cd     AS lkp_agmt_objtv_type_cd,
                                                      lkp_agmt.mkt_risk_type_cd       AS lkp_mkt_risk_type_cd,
                                                      lkp_agmt.ntwk_srvc_agmt_type_cd AS lkp_ntwk_srvc_agmt_type_cd,
                                                      lkp_agmt.frmlty_type_cd         AS lkp_frmlty_type_cd,
                                                      lkp_agmt.agmt_idntftn_cd        AS lkp_agmt_idntftn_cd,
                                                      lkp_agmt.trmtn_type_cd          AS lkp_trmtn_type_cd,
                                                      lkp_agmt.int_pmt_meth_cd        AS lkp_int_pmt_meth_cd,
                                                      lkp_agmt.pmt_pln_type_cd        AS lkp_pmt_pln_type_cd,
                                                      lkp_agmt.modl_crtn_dttm         AS lkp_modl_crtn_dttm,
                                                      lkp_agmt.cntnus_srvc_dttm       AS lkp_cntnus_srvc_dt,
                                                      lkp_agmt.agmt_legly_bindg_ind   AS lkp_agmt_legly_bindg_ind,
                                                      lkp_agmt.lgcy_dscnt_ind         AS lkp_lgcy_dscnt_ind,
                                                      lkp_agmt.bilg_meth_type_cd      AS lkp_bilg_meth_type_cd,
                                                      lkp_agmt.modl_eff_dttm          AS lkp_modl_eff_dttm,
                                                      lkp_agmt.modl_actl_end_dttm     AS lkp_modl_actl_end_dttm,
                                                      lkp_agmt.tier_type_cd           AS lkp_tier_type_cd,
                                                      lkp_agmt.edw_end_dttm           AS lkp_edw_end_dttm,
                                                      lkp_agmt.vfyd_plcy_ind          AS lkp_vfyd_plcy_ind,
                                                      lkp_agmt.nk_src_key             AS lkp_nk_src_key,
                                                      /*  Other INFORMATION_SCHEMA.columns */
                                                      policynumber,
                                                      modelnumber,
                                                      termnumber,
                                                      periodstart,
                                                      originaleffectivedate,
                                                      periodend,
                                                      issuedate,
                                                      processingdate,
                                                      out_agmt_type_cd,
                                                      out_agmt_cur_sts_cd,
                                                      out_agmt_cur_sts_rsn_cd,
                                                      out_agmt_obtnd_cd,
                                                      out_agmt_sbtype_cd,
                                                      out_agmt_objtv_type_cd,
                                                      out_mkt_risk_type_cd,
                                                      out_ntwk_srvc_agmt_type_cd,
                                                      out_frmlty_type_cd,
                                                      out_agmt_idntftn_cd,
                                                      out_trmtn_type_cd,
                                                      out_int_pmt_meth_cd,
                                                      out_prcs_id,
                                                      out_pmt_pln_type_cd,
                                                      out_bilg_meth_type_cd,
                                                      out_stmt_cycle_cd,
                                                      editeffectivedate,
                                                      model_actl_end_ddtm,
                                                      modeldate,
                                                      publicid,
                                                      previnsurance_alfa,
                                                      continuousservicedate_alfa,
                                                      edw_strt_dttm1,
                                                      legacydiscount_alfa,
                                                      sq3.edw_end_dttm AS edw_end_dttm,
                                                      in_tier_type,
                                                      sq3.vfyd_plcy_ind AS vfyd_plcy_ind,
                                                      updatetime,
                                                      out_src_cd,
                                                      lkp_agmt.src_of_busn_cd AS lkp_src_of_busn_cd,
                                                      lkp_agmt.edw_strt_dttm  AS lkp_edw_strt_dttm,
                                                      retired,
                                                      out_override_commission_value,
                                                      legacypolind_alfa AS in_legacypolind_alfa,
                                                      /*  CASE WHEN in_LegacyPolInd_alfa IS NULL THEN ''N'' ELSE in_LegacyPolInd_alfa END */
                                                      CASE
                                                                WHEN in_legacypolind_alfa IS NULL THEN ''N''
                                                                ELSE in_legacypolind_alfa
                                                      END AS out_legacypolind_alfa,
                                                      out_sourceofbusiness_alfa_typecode,
                                                      stmt_cycle_cd,
                                                      v_agmt_src_cd,
                                                      lkp_data_src.tgt_idntftn_val AS v_lkp_agmt_src_cd,
                                                      /*  CASE WHEN TRIM(v_AGMT_SRC_CD) = '''' OR v_AGMT_SRC_CD IS NULL OR LENGTH(v_AGMT_SRC_CD) = 0  */
                                                      /*  OR v_lkp_AGMT_SRC_CD IS NULL THEN ''UNK'' ELSE v_lkp_AGMT_SRC_CD END */
                                                      CASE
                                                                WHEN length(trim(v_agmt_src_cd)) = 0
                                                                OR        v_agmt_src_cd IS NULL
                                                                OR        length(v_agmt_src_cd) = 0
                                                                OR        v_lkp_agmt_src_cd IS NULL THEN ''UNK''
                                                                ELSE v_lkp_agmt_src_cd
                                                      END                        AS agmt_src_cd,
                                                      lkp_agmt.ovrd_coms_type_cd AS lkp_ovrd_coms_type_cd,
                                                      lkp_agmt.trans_strt_dttm   AS lkp_trans_strt_dttm,
                                                      lkp_agmt.lgcy_plcy_ind     AS lkp_in_lgcy_plcy_ind,
                                                      /*  flag */
                                                      /*MD5(TO_CHAR(LKP_AGMT_EFF_DTTM)||TO_CHAR(LKP_AGMT_OPN_DTTM)||TO_CHAR(LKP_AGMT_PLND_EXPN_DT)
||TO_CHAR(LKP_AGMT_SIGND_DT)||TO_CHAR(LKP_AGMT_PRCSG_DT)||LKP_AGMT_TYPE_CD
||LKP_AGMT_CUR_STS_CD||LKP_AGMT_CUR_STS_RSN_CD||LKP_AGMT_OBTND_CD||LKP_AGMT_SBTYPE_CD
||LKP_AGMT_OBJTV_TYPE_CD||LKP_MKT_RISK_TYPE_CD||
LKP_NTWK_SRVC_AGMT_TYPE_CD||LKP_FRMLTY_TYPE_CD
||LKP_AGMT_IDNTFTN_CD||LKP_TRMTN_TYPE_CD||LKP_INT_PMT_METH_CD||LKP_PMT_PLN_TYPE_CD||LKP_BILG_METH_TYPE_CD
||LKP_STMT_CYCL_CD||TO_CHAR(LKP_MODL_EFF_DTTM)
||TO_CHAR(LKP_MODL_CRTN_DTTM)||rtrim(ltrim(TO_CHAR(LKP_CNTNUS_SRVC_DT)))||
ltrim(rtrim(lkp_VFYD_PLCY_IND))||RTRIM(LTRIM(LKP_SRC_OF_BUSN_CD))
||CASE WHEN Updatetime>02/28/2019 16:09:51.257000 THEN RTRIM(LTRIM(lkp_OVRD_COMS_TYPE_CD)) ELSE null END)||rtrim(ltrim(LKp_IN_LGCY_PLCY_IND))*/
                                                      CASE
                                                                WHEN updatetime > ''$Override_Commission_start_dttm'' THEN lkp_ovrd_coms_type_cd
                                                                ELSE ''''
                                                      END AS sub_lkp_checksum,
                                                       CASE
                                                                WHEN updatetime > ''$Override_Commission_start_dttm'' THEN out_override_commission_value
                                                                ELSE ''''
                                                      END AS sub_v_checksum,
                                                      CASE
                                                                WHEN sub_v_checksum IS NOT NULL THEN trim(sub_v_checksum)
                                                                ELSE trim('''')
                                                      END AS lkp_main_checksum,
                                                      cast((coalesce(to_char(lkp_agmt_eff_dttm), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(lkp_agmt_opn_dttm), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(lkp_agmt_plnd_expn_dt), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(lkp_agmt_signd_dt), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(lkp_agmt_prcsg_dt), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(lkp_agmt_type_cd, ''~'')
                                                                ||coalesce(lkp_agmt_cur_sts_cd, ''~'')
                                                                ||coalesce(lkp_agmt_cur_sts_rsn_cd, ''~'')
                                                                ||coalesce(lkp_agmt_obtnd_cd, ''~'')
                                                                ||coalesce(lkp_agmt_sbtype_cd, ''~'')
                                                                ||coalesce(lkp_agmt_objtv_type_cd, ''~'')
                                                                ||coalesce(lkp_mkt_risk_type_cd, ''~'')
                                                                ||coalesce(lkp_ntwk_srvc_agmt_type_cd, ''~'')
                                                                ||coalesce(lkp_frmlty_type_cd, ''~'')
                                                                ||coalesce(lkp_agmt_idntftn_cd, ''~'')
                                                                ||coalesce(lkp_trmtn_type_cd, ''~'')
                                                                ||coalesce(lkp_int_pmt_meth_cd, ''~'')
                                                                ||coalesce(lkp_pmt_pln_type_cd, ''~'')
                                                                ||coalesce(lkp_bilg_meth_type_cd, ''~'')
                                                                ||coalesce(lkp_stmt_cycl_cd, ''~'')
                                                                ||coalesce(to_char(lkp_modl_eff_dttm), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(lkp_modl_crtn_dttm), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(trim(to_char(lkp_cntnus_srvc_dt)), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(trim(lkp_vfyd_plcy_ind), ''~'')
                                                                ||coalesce(trim(lkp_src_of_busn_cd), ''~'')
                                                                ||coalesce(lkp_main_checksum, ''~'') )AS VARCHAR(255))
                                                                || coalesce(trim(lkp_in_lgcy_plcy_ind), ''~'') AS v_orig_chcksm,
                                                      /*MD5(TO_CHAR(PeriodStart)||TO_CHAR(OriginalEffectiveDate)||TO_CHAR(PeriodEnd)||TO_CHAR(IssueDate)
||TO_CHAR(ProcessingDate)||out_AGMT_TYPE_CD||
out_AGMT_CUR_STS_CD||out_AGMT_CUR_STS_RSN_CD||out_AGMT_OBTND_CD||out_AGMT_SBTYPE_CD||out_AGMT_OBJTV_TYPE_CD
||out_MKT_RISK_TYPE_CD||out_NTWK_SRVC_AGMT_TYPE_CD||out_FRMLTY_TYPE_CD||
out_AGMT_IDNTFTN_CD||out_TRMTN_TYPE_CD||out_INT_PMT_METH_CD||out_PMT_PLN_TYPE_CD||out_BILG_METH_TYPE_CD
||out_STMT_CYCLE_CD||TO_CHAR(EditEffectiveDate)||
rtrim(ltrim(TO_CHAR(modeldate)||to_char(ContinuousServiceDate_alfa)))||rtrim(ltrim(VFYD_PLCY_IND))
||LTRIM(RTRIM(out_SourceOfBusiness_alfa_typecode))
||CASE WHEN Updatetime>02/28/2019 16:09:51.257000 THEN RTRIM(LTRIM(out_Override_Commission_Value)) ELSE null END)
||RTRIM(LTRIM(LegacyPolInd_alfa))*/
                                                      /*  cast( as varchar(100)) */

                                                      CASE
                                                                WHEN sub_v_checksum IS NOT NULL THEN trim(sub_v_checksum)
                                                                ELSE trim('''')
                                                      END AS main_checksum,
                                                      cast((coalesce(to_char(periodstart), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(originaleffectivedate), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(periodend), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(issuedate), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(processingdate), ''1900-00-00 00:00:00.000000'')
                                                                || coalesce(out_agmt_type_cd, ''~'')
                                                                || coalesce(out_agmt_cur_sts_cd, ''~'')
                                                                ||coalesce(out_agmt_cur_sts_rsn_cd, ''~'')
                                                                ||coalesce(out_agmt_obtnd_cd, ''~'')
                                                                ||coalesce(out_agmt_sbtype_cd, ''~'')
                                                                ||coalesce(out_agmt_objtv_type_cd, ''~'')
                                                                ||coalesce(out_mkt_risk_type_cd, ''~'')
                                                                ||coalesce(out_ntwk_srvc_agmt_type_cd, ''~'')
                                                                ||coalesce(out_frmlty_type_cd, ''~'')
                                                                || coalesce(out_agmt_idntftn_cd, ''~'')
                                                                ||coalesce(out_trmtn_type_cd, ''~'')
                                                                ||coalesce(out_int_pmt_meth_cd, ''~'')
                                                                ||coalesce(out_pmt_pln_type_cd, ''~'')
                                                                ||coalesce(out_bilg_meth_type_cd, ''~'')
                                                                || coalesce(out_stmt_cycle_cd, ''~'')
                                                                ||coalesce(to_char(editeffectivedate), ''1900-00-00 00:00:00.000000'')
                                                                ||trim(trim(coalesce(to_char(modeldate), ''1900-00-00 00:00:00.000000'')
                                                                ||coalesce(to_char(continuousservicedate_alfa), ''1900-00-00 00:00:00.000000'')))
                                                                ||coalesce(trim(sq3.vfyd_plcy_ind), ''~'')
                                                                ||coalesce(trim(out_sourceofbusiness_alfa_typecode), ''~'')
                                                                || coalesce(main_checksum, ''~'') ) AS VARCHAR(255))
                                                                || coalesce(trim(legacypolind_alfa), ''~'') AS calc_chksm,
                                                      /*CASE WHEN v_ORIG_CHCKSM IS NULL THEN ''I'' ELSE CASE WHEN v_ORIG_CHCKSM!=CALC_CHKSM THEN ''U'' ELSE ''R'' END END*/
                                                      CASE
                                                                WHEN lkp_agmt.agmt_id IS NULL
                                                                AND       trim(lkp_xref_agmt.agmt_id) IS NOT NULL THEN ''I''
                                                                WHEN v_orig_chcksm <> calc_chksm
                                                                AND       trim(lkp_xref_agmt.agmt_id) IS NOT NULL THEN ''U''
                                                                ELSE ''R''
                                                      END AS out_ins_upd,
                                                      /* CASE
WHEN (OUT_INS_UPD = ''I'' or
(OUT_INS_UPD = ''U'' and lkp_EDW_END_DTTM = cast(''9999-12-31 23:59:59.999999'' as timestamp))
or
(retired=0 AND lkp_EDW_END_DTTM <> cast(''9999-12-31 23:59:59.999999'' as timestamp))
)
THEN trim(lkp_xref_agmt.AGMT_ID)
ELSE LKP_AGMT_ID
END as agmt_id*/
                                                      trim(lkp_xref_agmt.agmt_id) AS agmt_id
                                            FROM      (
                                                      (
                                                                SELECT    policynumber,
                                                                          modelnumber,
                                                                          termnumber,
                                                                          o_periodstart AS periodstart,
                                                                          originaleffectivedate,
                                                                          o_periodend AS periodend,
                                                                          issuedate1  AS issuedate,
                                                                          processingdate,
                                                                          editeffectivedate,
                                                                          model_actl_end_ddtm,
                                                                          modeldate,
                                                                          ''$p_agmt_type_cd_policy_version'' AS out_agmt_type_cd,
                                                                          /*  CASE WHEN in_AGMT_CUR_STS_CD IS NULL THEN ''UNK'' ELSE in_AGMT_CUR_STS_CD END */
                                                                          CASE
                                                                                    WHEN lkp_status1 IS NULL THEN ''UNK''
                                                                                    ELSE lkp_status1
                                                                          END AS out_agmt_cur_sts_cd,
                                                                          /*  CASE WHEN in_AGMT_CUR_STS_RSN_CD IS NULL THEN ''UNK'' ELSE in_AGMT_CUR_STS_RSN_CD END */
                                                                          CASE
                                                                                    WHEN lkp_agmt_sts_rsn IS NULL THEN ''UNK''
                                                                                    ELSE lkp_agmt_sts_rsn
                                                                          END   AS out_agmt_cur_sts_rsn_cd,
                                                                          ''UNK'' AS out_agmt_obtnd_cd,
                                                                          CASE
                                                                                    WHEN lkp_agmt_sb_type.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                    ELSE lkp_agmt_sb_type.tgt_idntftn_val
                                                                          END   AS out_agmt_sbtype_cd,
                                                                          ''UNK'' AS out_agmt_objtv_type_cd,
                                                                          ''UNK'' AS out_mkt_risk_type_cd,
                                                                          ''UNK'' AS out_ntwk_srvc_agmt_type_cd,
                                                                          ''UNK'' AS out_frmlty_type_cd,
                                                                          ''UNK'' AS out_agmt_idntftn_cd,
                                                                          ''UNK'' AS out_trmtn_type_cd,
                                                                          ''UNK'' AS out_int_pmt_meth_cd,
                                                                          CASE
                                                                                    WHEN lkp_ref_billing_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                    ELSE lkp_ref_billing_cd.tgt_idntftn_val
                                                                          END      AS out_bilg_meth_type_cd,
                                                                          $prcs_id AS out_prcs_id,
                                                                          CASE
                                                                                    WHEN etl_ref_stmt_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                    ELSE etl_ref_stmt_cd.tgt_idntftn_val
                                                                          END      AS out_stmt_cycle_cd,
                                                                          pmt_lkp1 AS out_pmt_pln_type_cd,
                                                                          publicid,
                                                                          updatetime,
                                                                          previnsurance_alfa,
                                                                          lkp_src_sys.tgt_idntftn_val                     AS out_src_cd,
                                                                          cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS edw_end_dttm,
                                                                          cast(current_timestamp(0) AS timestamp)         AS edw_strt_dttm1,
                                                                          continuousservicedate_alfa1                     AS continuousservicedate_alfa,
                                                                          retired,
                                                                          in_tier_type,
                                                                          legacydiscount_alfa,
                                                                          vfyd_plcy_ind,
                                                                          out_sourceofbusiness_alfa_typecode,
                                                                          stmt_cycle_cd,
                                                                          out_override_commission_value,
                                                                          legacypolind_alfa,
                                                                          /*  LTRIM(RTRIM(CASE WHEN ISBINDONLINE_ALFA=''T'' THEN ''DATA_SRC_TYPE8'' ELSE ''DATA_SRC_TYPE2'' END)) */
                                                                          CASE
                                                                                    WHEN trim(isbindonline_alfa) = ''T'' THEN ''DATA_SRC_TYPE8''
                                                                                    ELSE ''DATA_SRC_TYPE2''
                                                                          END AS v_agmt_src_cd
                                                                FROM      (
                                                                          (
                                                                                    SELECT    policynumber,
                                                                                              modelnumber,
                                                                                              termnumber,
                                                                                              /* CASE WHEN PeriodStart IS NULL THEN to_date(''1900-01-01'',''YYYY-MM-DD'') ELSE PeriodStart END */
                                                                                              CASE
                                                                                                        WHEN periodstart IS NULL THEN cast(cast(''01-01-1900'' AS DATE ) AS timestamp)
                                                                                                        ELSE periodstart
                                                                                              END AS o_periodstart,
                                                                                              originaleffectivedate,
                                                                                              /*  CASE WHEN PeriodEnd IS NULL THEN TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.NS'') ELSE PeriodEnd END */
                                                                                              CASE
                                                                                                        WHEN periodend IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp)
                                                                                                        ELSE periodend
                                                                                              END AS o_periodend,
                                                                                              /*  CASE WHEN IssueDate IS NULL THEN TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.NS'') ELSE IssueDate END */
                                                                                              CASE
                                                                                                        WHEN issuedate IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp)
                                                                                                        ELSE issuedate
                                                                                              END AS issuedate1,
                                                                                              rejectreason,
                                                                                              processingdate,
                                                                                              agmt_current_status,
                                                                                              pmt_pln_type_cd,
                                                                                              stmt_cycle_cd,
                                                                                              bilg_meth_type_cd,
                                                                                              editeffectivedate,
                                                                                              modeldate,
                                                                                              publicid,
                                                                                              updatetime,
                                                                                              previnsurance_alfa,
                                                                                              src_cd,
                                                                                              model_actl_end_ddtm,
                                                                                              /*  CASE WHEN ContinuousServiceDate_alfa IS NULL THEN TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.NS'') ELSE ContinuousServiceDate_alfa END */
                                                                                              CASE
                                                                                                        WHEN continuousservicedate_alfa IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp)
                                                                                                        ELSE continuousservicedate_alfa
                                                                                              END AS continuousservicedate_alfa1,
                                                                                              retired,
                                                                                              lkp_tier_type_cd.tgt_idntftn_val AS in_tier_type,
                                                                                              legacydiscount_alfa,
                                                                                              vfyd_plcy_ind,
                                                                                              CASE
                                                                                                        WHEN lkp_busn_cd.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                        ELSE lkp_busn_cd.tgt_idntftn_val
                                                                                              END AS out_sourceofbusiness_alfa_typecode,
                                                                                              invoicefrequency,
                                                                                              CASE
                                                                                                        WHEN ovrd_coms_type_cd IS NULL THEN NULL
                                                                                                        WHEN lkp_override.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                        ELSE lkp_override.tgt_idntftn_val
                                                                                              END AS out_override_commission_value,
                                                                                              legacypolind_alfa,
                                                                                              isbindonline_alfa,
                                                                                              lkp_ref_pmt.tgt_idntftn_val           AS pmt_lkp1,
                                                                                              lkp_status.tgt_idntftn_val            AS lkp_status1,
                                                                                              lkp_agmt_sts_rsn_type.tgt_idntftn_val AS lkp_agmt_sts_rsn
                                                                                    FROM      (
                                                                                              (
                                                                                                     /*SQ query starts here*/
                                                                                                     SELECT policynumber ,
                                                                                                            modelnumber,
                                                                                                            termnumber,
                                                                                                            editeffectivedate,
                                                                                                            periodstart,
                                                                                                            originaleffectivedate,
                                                                                                            periodend,
                                                                                                            issuedate,
                                                                                                            rejectreason,
                                                                                                            processingdate,
                                                                                                            agmt_current_status,
                                                                                                            pmt_pln_type_cd,
                                                                                                            invoicefrequency,
                                                                                                            stmt_cycle_cd,
                                                                                                            bilg_meth_type_cd,
                                                                                                            modeldate,
                                                                                                            publicid,
                                                                                                            updatetime,
                                                                                                            CASE
                                                                                                                   WHEN previnsurance_alfa=1 THEN ''Y''
                                                                                                                   WHEN previnsurance_alfa=0 THEN ''N''
                                                                                                            END previnsurance_alfa,
                                                                                                            /* EIM-24545 change */
                                                                                                            model_actl_end_ddtm,
                                                                                                            src_cd,
                                                                                                            continuousservicedate_alfa ,
                                                                                                            retired,
                                                                                                            generalplustier_alfa,
                                                                                                            /* EIM-24545 Change--case when GeneralPlusTier_alfa =0 then ''F''when  GeneralPlusTier_alfa =1 then ''T'' end  GeneralPlusTier_alfa,-- implemented the stag exisitng mapping logic in new sq mapping.need to be corrected when revisted */
                                                                                                            CASE
                                                                                                                   WHEN legacydiscount_alfa=1 THEN ''Y''
                                                                                                                   WHEN legacydiscount_alfa=0 THEN ''N''
                                                                                                            END legacydiscount_alfa,
                                                                                                            /* EIM-24545 change */
                                                                                                            /* case when (cast(LegacyDiscount_alfa as varchar(10))=''TRUE'') then ''1'' when cast(LegacyDiscount_alfa as varchar(10))= ''FALSE'' then ''0'' end  LegacyDiscount_alfa, -- implemented the stag exisitng mapping logic in new sq mapping.need to be corrected when revisted */
                                                                                                            vfyd_plcy_ind,
                                                                                                            sourceofbusiness_alfa_typecode,
                                                                                                            ovrd_coms_type_cd,
                                                                                                            legacypolind_alfa,
                                                                                                            CASE
                                                                                                                   WHEN isbindonline_alfa =0 THEN ''F''
                                                                                                                   WHEN isbindonline_alfa =1 THEN ''T''
                                                                                                            END isbindonline_alfa
                                                                                                            /*  implemented the stag exisitng mapping logic in new sq mapping.need to be corrected when revisted */
                                                                                                     FROM  (
                                                                                                                   SELECT cast( z.policynumber AS VARCHAR (60))        AS policynumber ,
                                                                                                                          cast(z.modelnumber AS   INTEGER)             AS modelnumber,
                                                                                                                          cast (z.termnumber AS   INTEGER)             AS termnumber,
                                                                                                                          cast (z.editeffectivedate AS timestamp)      AS editeffectivedate,
                                                                                                                          cast (z.periodstart AS timestamp)            AS periodstart,
                                                                                                                          cast (z.originaleffectivedate AS timestamp)  AS originaleffectivedate,
                                                                                                                          cast (z.periodend AS timestamp)              AS periodend,
                                                                                                                          cast (z.issuedate AS timestamp)              AS issuedate,
                                                                                                                          cast (z.rejectreason AS VARCHAR (60))        AS rejectreason,
                                                                                                                          cast (z.processingdate AS timestamp)         AS processingdate,
                                                                                                                          cast (z.agmt_current_status AS VARCHAR (60)) AS agmt_current_status,
                                                                                                                          cast (z.pmt_pln_type_cd AS     VARCHAR (60)) AS pmt_pln_type_cd,
                                                                                                                          cast (z.invoicefrequency AS    INTEGER)      AS invoicefrequency,
                                                                                                                          cast (z.stmt_cycle_cd AS       VARCHAR (60)) AS stmt_cycle_cd,
                                                                                                                          cast (z.bilg_meth_type_cd AS   VARCHAR (60)) AS bilg_meth_type_cd,
                                                                                                                          cast (z.modeldate AS timestamp)              AS modeldate,
                                                                                                                          cast (z.publicid AS VARCHAR (60))            AS publicid,
                                                                                                                          cast (z.updatetime AS timestamp)             AS updatetime,
                                                                                                                          z.previnsurance_alfa                         AS previnsurance_alfa,
                                                                                                                          CASE
                                                                                                                                 WHEN z.cancellationdate IS NOT NULL THEN z.cancellationdate
                                                                                                                                 WHEN z.cancellationdate IS NULL
                                                                                                                                 AND    z.modl_actl_end_dttm IS NOT NULL THEN z.modl_actl_end_dttm
                                                                                                                                 WHEN z.cancellationdate IS NULL
                                                                                                                                 AND    z.modl_actl_end_dttm IS NULL THEN z.periodend
                                                                                                                          END model_actl_end_ddtm,
                                                                                                                          z.src_cd,
                                                                                                                          cast(z.continuousservicedate_alfa AS timestamp)continuousservicedate_alfa,
                                                                                                                          z.retired,
                                                                                                                          z.generalplustier_alfa,
                                                                                                                          z.legacydiscount_alfa,
                                                                                                                          z.vfyd_plcy_ind,
                                                                                                                          z.sourceofbusiness_alfa_typecode,
                                                                                                                          cast(ovrd_coms_type_cd AS VARCHAR(100)) ovrd_coms_type_cd,
                                                                                                                          z.legacypolind_alfa,
                                                                                                                          z.isbindonline_alfa
                                                                                                                   FROM   (
                                                                                                                                   SELECT   x.*,
                                                                                                                                            row_number() over (PARTITION BY x.policynumber,x.modelnumber,x.termnumber ORDER BY x. modl_actl_end_dttm) AS r
                                                                                                                                   FROM     (
                                                                                                                                                            SELECT DISTINCT outer_pc.policynumber,
                                                                                                                                                                            outer_pc.modelnumber,
                                                                                                                                                                            outer_pc.termnumber,
                                                                                                                                                                            outer_pc.editeffectivedate,
                                                                                                                                                                            outer_pc.periodstart,
                                                                                                                                                                            outer_pc.originaleffectivedate,
                                                                                                                                                                            outer_pc.periodend,
                                                                                                                                                                            outer_pc.issuedate,
                                                                                                                                                                            outer_pc.rejectreason,
                                                                                                                                                                            outer_pc.createtime AS processingdate,
                                                                                                                                                                            outer_pc.agmt_current_status,
                                                                                                                                                                            outer_pc.pmt_pln_type_cd,
                                                                                                                                                                            outer_pc.invoicefrequency,
                                                                                                                                                                            outer_pc.stmt_cycle_cd AS stmt_cycle_cd,
                                                                                                                                                                            outer_pc.bilgmethtype  AS bilg_meth_type_cd,
                                                                                                                                                                            outer_pc.modeldate,
                                                                                                                                                                            outer_pc.cancellationdate,
                                                                                                                                                                            outer_pc.publicid,
                                                                                                                                                                            outer_pc.updatetime,
                                                                                                                                                                            outer_pc.previnsurance_alfa,
                                                                                                                                                                            outer_pc.continuousservicedate_alfa,
                                                                                                                                                                            outer_pc.src_cd,
                                                                                                                                                                            outer_pc.retired,
                                                                                                                                                                            outer_pc.generalplustier_alfa,
                                                                                                                                                                            outer_pc.sourceofbusiness_alfa_typecode,
                                                                                                                                                                            min (outer_pc.editeffectivedate) over (PARTITION BY outer_pc.policynumber,outer_pc.termnumber ORDER BY outer_pc.modelnumber ROWS BETWEEN 1 following AND             1 following) AS modl_actl_end_dttm,
                                                                                                                                                                            outer_pc.legacydiscount_alfa,
                                                                                                                                                                            coalesce(outer_pc.vfyd_plcy_ind,''N'') AS vfyd_plcy_ind,
                                                                                                                                                                            ovrd_coms_type_cd,
                                                                                                                                                                            outer_pc.legacypolind_alfa,
                                                                                                                                                                            outer_pc.isbindonline_alfa
                                                                                                                                                            FROM            (
                                                                                                                                                                                            SELECT DISTINCT pc_policyperiod.policynumber_stg           policynumber,
                                                                                                                                                                                                        pc_policyperiod.modelnumber_stg            modelnumber,
                                                                                                                                                                                                        pc_policyperiod.termnumber_stg             termnumber,
                                                                                                                                                                                                        pc_policyperiod.editeffectivedate_stg      editeffectivedate,
                                                                                                                                                                                                        pc_policyperiod.periodstart_stg            periodstart,
                                                                                                                                                                                                        pc_policy.originaleffectivedate_stg        originaleffectivedate,
                                                                                                                                                                                                        pc_policyperiod.periodend_stg              periodend,
                                                                                                                                                                                                        pc_policy.issuedate_stg                    issuedate,
                                                                                                                                                                                                        pctl_reasoncode.typecode_stg               AS rejectreason,
                                                                                                                                                                                                        pc_policyperiod.createtime_stg                createtime,
                                                                                                                                                                                                        pctl_policyperiodstatus.typecode_stg       AS agmt_current_status,
                                                                                                                                                                                                        pc_paymentplansummary.name_stg             AS pmt_pln_type_cd,
                                                                                                                                                                                                        pc_paymentplansummary.invoicefrequency_stg    invoicefrequency,
                                                                                                                                                                                                        pctl_billingperiodicity.typecode_stg       AS stmt_cycle_cd,
                                                                                                                                                                                                        pctl_billingmethod.typecode_stg            AS bilgmethtype,
                                                                                                                                                                                                        pc_policyperiod.modeldate_stg                 modeldate,
                                                                                                                                                                                                        pc_policyperiod.cancellationdate_stg          cancellationdate,
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg                updatetime,
                                                                                                                                                                                                        /* pc_effectivedatedfields.UpdateTime_stg, */
                                                                                                                                                                                                        pc_policyperiod.publicid_stg                              publicid,
                                                                                                                                                                                                        pc_effectivedatedfields.previnsurance_alfa_stg            previnsurance_alfa,
                                                                                                                                                                                                        pc_effectivedatedfields.continuousservicedate_alfa_stg    continuousservicedate_alfa ,
                                                                                                                                                                                                        pc_policyperiod.retired_stg                               retired,
                                                                                                                                                                                                        ''SRC_SYS4''                                             AS src_cd,
                                                                                                                                                                                                        pctl_sourceofbusiness_alfa.typecode_stg                   sourceofbusiness_alfa_typecode,
                                                                                                                                                                                                        pctl_job.name_stg                                         name,
                                                                                                                                                                                                        generalplustier_alfa_stg                                  generalplustier_alfa,
                                                                                                                                                                                                        pc_effectivedatedfields.legacydiscount_alfa_stg           legacydiscount_alfa,
                                                                                                                                                                                                        /* case when pc_effectivedatedfields.CreateTime_stg IS NULL then cast(''1900-01-01 00:00:00.000000'' as timestamp)  else pc_effectivedatedfields.CreateTime_stg end CreateTime2, */
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN pc_policyperiod.editeffectivedate_stg IS NULL THEN cast(''1900-01-01 00:00:00.000000'' AS timestamp)
                                                                                                                                                                                                        ELSE pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                                        END createtime2,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN pc_effectivedatedfields.expirationdate_stg IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp)
                                                                                                                                                                                                        ELSE pc_effectivedatedfields.expirationdate_stg
                                                                                                                                                                                                        END           expirationdate2,
                                                                                                                                                                                                        ''Y''           AS vfyd_plcy_ind ,
                                                                                                                                                                                                        ($start_dttm) AS start_dttm,
                                                                                                                                                                                                        ($end_dttm)   AS end_dttm,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        status_stg = 9
                                                                                                                                                                                                        AND             tl3.name_stg IS NOT NULL )THEN tl3.typecode_stg
                                                                                                                                                                                                        ELSE NULL
                                                                                                                                                                                                        END                   ovrd_coms_type_cd ,
                                                                                                                                                                                                        ''N''                   AS legacypolind_alfa ,
                                                                                                                                                                                                        isbindonline_alfa_stg    isbindonline_alfa
                                                                                                                                                                                            FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                                                                                            inner join      db_t_prod_stag.pc_policy
                                                                                                                                                                                            ON              pc_policy.id_stg=pc_policyperiod.policyid_stg
                                                                                                                                                                                            left outer join db_t_prod_stag.pc_paymentplansummary
                                                                                                                                                                                            ON              (
                                                                                                                                                                                                        pc_paymentplansummary.policyperiod_stg = pc_policyperiod.id_stg
                                                                                                                                                                                                        AND             pc_paymentplansummary.retired_stg=0)
                                                                                                                                                                                            inner join      db_t_prod_stag.pc_job
                                                                                                                                                                                            ON              pc_policyperiod.jobid_stg=pc_job.id_stg
                                                                                                                                                                                            inner join      db_t_prod_stag.pctl_job
                                                                                                                                                                                            ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                                            left outer join db_t_prod_stag.pctl_billingperiodicity
                                                                                                                                                                                            ON              pctl_billingperiodicity.id_stg=pc_paymentplansummary.invoicefrequency_stg
                                                                                                                                                                                            left outer join db_t_prod_stag.pctl_billingmethod
                                                                                                                                                                                            ON              pc_policyperiod.billingmethod_stg=pctl_billingmethod.id_stg
                                                                                                                                                                                            left outer join db_t_prod_stag.pctl_reasoncode
                                                                                                                                                                                            ON              pctl_reasoncode.id_stg=pc_job.rejectreason_stg
                                                                                                                                                                                            inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                                            ON              pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                                                                                                                                                                            left outer join
                                                                                                                                                                                                        (
                                                                                                                                                                                                        SELECT *
                                                                                                                                                                                                        FROM   db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                                                        WHERE  id_stg IN
                                                                                                                                                                                                        (
                                                                                                                                                                                                        SELECT (id_stg)
                                                                                                                                                                                                        FROM   db_t_prod_stag.pc_effectivedatedfields)) pc_effectivedatedfields
                                                                                                                                                                                            ON              pc_effectivedatedfields.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                                        /*EIM-14816 - Add Override_Commission_Value*/
                                                                                                                                                                                            left join       db_t_prod_stag.pctl_commissionovrdtype_alfa tl3
                                                                                                                                                                                            ON              tl3.id_stg = pc_effectivedatedfields.overridecommissionrate_alfa_stg
                                                                                                                                                                                            left outer join db_t_prod_stag.pctl_sourceofbusiness_alfa
                                                                                                                                                                                            ON              pc_effectivedatedfields.sourceofbusiness_alfa_stg = pctl_sourceofbusiness_alfa.id_stg
                                                                                                                                                                                                        /* and pctl_sourceofbusiness_alfa.typecode=''companytocompanytransfer''  */
                                                                                                                                                                                                         /**** This filter should be in reporting layer for POL_DA_521****/
                                                                                                                                                                                                        
                                                                                                                                                                                            WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                                            AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                                            AND             pc_policyperiod.updatetime_stg > ($start_dttm)
                                                                                                                                                                                            AND             pc_policyperiod.updatetime_stg <= ($end_dttm) ) outer_pc
                                                                                                                                                            WHERE           src_cd=''SRC_SYS4'' )x )z
                                                                                                                   WHERE  r=1
                                                                                                                   /* and policynumber=''16000000119'' */
                                                                                                                   UNION ALL
                                                                                                                   SELECT cast( z.policynumber AS VARCHAR (60))        AS policynumber ,
                                                                                                                          cast(z.modelnumber AS   INTEGER)             AS modelnumber,
                                                                                                                          cast (z.termnumber AS   INTEGER)             AS termnumber,
                                                                                                                          cast (z.editeffectivedate AS timestamp )     AS editeffectivedate,
                                                                                                                          cast (z.periodstart AS timestamp)            AS periodstart,
                                                                                                                          cast (z.originaleffectivedate AS timestamp)  AS originaleffectivedate,
                                                                                                                          cast (z.periodend AS timestamp)              AS periodend,
                                                                                                                          cast (z.issuedate AS timestamp)              AS issuedate,
                                                                                                                          cast (z.rejectreason AS VARCHAR (60))        AS rejectreason,
                                                                                                                          cast (z.processingdate AS timestamp)         AS processingdate,
                                                                                                                          cast (z.agmt_current_status AS VARCHAR (60)) AS agmt_current_status,
                                                                                                                          cast (z.pmt_pln_type_cd AS     VARCHAR (60)) AS pmt_pln_type_cd,
                                                                                                                          cast (z.invoicefrequency AS    INTEGER)      AS invoicefrequency,
                                                                                                                          cast (z.stmt_cycle_cd AS       VARCHAR (60)) AS stmt_cycle_cd,
                                                                                                                          cast (z.bilg_meth_type_cd AS   VARCHAR (60)) AS bilg_meth_type_cd,
                                                                                                                          cast (z.modeldate AS timestamp)              AS modeldate,
                                                                                                                          cast (z.publicid AS VARCHAR (60))            AS publicid,
                                                                                                                          cast (z.updatetime AS timestamp)             AS updatetime,
                                                                                                                          z.previnsurance_alfa                         AS previnsurance_alfa ,
                                                                                                                          CASE
                                                                                                                                 WHEN z.cancellationdate IS NOT NULL THEN z.cancellationdate
                                                                                                                                 WHEN z.cancellationdate IS NULL
                                                                                                                                 AND    z.modl_actl_end_dttm IS NOT NULL THEN z.modl_actl_end_dttm
                                                                                                                                 WHEN z.cancellationdate IS NULL
                                                                                                                                 AND    z.modl_actl_end_dttm IS NULL THEN z.periodend
                                                                                                                          END model_actl_end_ddtm,
                                                                                                                          z.src_cd,
                                                                                                                          cast(z.continuousservicedate_alfa AS timestamp)continuousservicedate_alfa,
                                                                                                                          z.retired,
                                                                                                                          z.generalplustier_alfa,
                                                                                                                          z.legacydiscount_alfa,
                                                                                                                          z.vfyd_plcy_ind,
                                                                                                                          z.sourceofbusiness_alfa_typecode,
                                                                                                                          z.ovrd_coms_type_cd,
                                                                                                                          z.legacypolind_alfa,
                                                                                                                          z.isbindonline_alfa
                                                                                                                   FROM   (
                                                                                                                                   SELECT   x.*,
                                                                                                                                            row_number() over (PARTITION BY x.policynumber ORDER BY x. modl_actl_end_dttm) AS r
                                                                                                                                   FROM     (
                                                                                                                                                            SELECT DISTINCT outer_pc.policynumber,
                                                                                                                                                                            outer_pc.modelnumber,
                                                                                                                                                                            outer_pc.termnumber,
                                                                                                                                                                            outer_pc.editeffectivedate,
                                                                                                                                                                            outer_pc.periodstart,
                                                                                                                                                                            outer_pc.originaleffectivedate,
                                                                                                                                                                            outer_pc.periodend,
                                                                                                                                                                            outer_pc.issuedate,
                                                                                                                                                                            outer_pc.rejectreason,
                                                                                                                                                                            outer_pc.createtime AS processingdate,
                                                                                                                                                                            outer_pc.agmt_current_status,
                                                                                                                                                                            outer_pc.pmt_pln_type_cd,
                                                                                                                                                                            outer_pc.invoicefrequency,
                                                                                                                                                                            outer_pc.stmt_cycle_cd AS stmt_cycle_cd,
                                                                                                                                                                            outer_pc.bilgmethtype  AS bilg_meth_type_cd,
                                                                                                                                                                            outer_pc.modeldate,
                                                                                                                                                                            outer_pc.cancellationdate,
                                                                                                                                                                            outer_pc.publicid,
                                                                                                                                                                            outer_pc.updatetime,
                                                                                                                                                                            outer_pc.previnsurance_alfa,
                                                                                                                                                                            outer_pc.continuousservicedate_alfa,
                                                                                                                                                                            outer_pc.src_cd,
                                                                                                                                                                            outer_pc.retired,
                                                                                                                                                                            outer_pc.generalplustier_alfa,
                                                                                                                                                                            outer_pc.sourceofbusiness_alfa_typecode,
                                                                                                                                                                            min (outer_pc.editeffectivedate) over (PARTITION BY outer_pc.policynumber,outer_pc.termnumber ORDER BY outer_pc.modelnumber ROWS BETWEEN 1 following AND             1 following) AS modl_actl_end_dttm,
                                                                                                                                                                            outer_pc.legacydiscount_alfa,
                                                                                                                                                                            coalesce(outer_pc.vfyd_plcy_ind,''N'') AS vfyd_plcy_ind,
                                                                                                                                                                            ovrd_coms_type_cd,
                                                                                                                                                                            outer_pc.legacypolind_alfa,
                                                                                                                                                                            outer_pc.isbindonline_alfa
                                                                                                                                                            FROM            (
                                                                                                                                                                                            SELECT          cast (cc_policy.policynumber_stg AS VARCHAR (60))     AS policynumber ,
                                                                                                                                                                                                        (NULL )                                               AS modelnumber,
                                                                                                                                                                                                        (NULL )                                               AS termnumber,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS editeffectivedate,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS periodstart,
                                                                                                                                                                                                        (cc_policy.origeffectivedate_stg)                     AS originaleffectivedate,
                                                                                                                                                                                                        (cc_policy.expirationdate_stg )                       AS periodend,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS issuedate,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS rejectreason,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS createtime,
                                                                                                                                                                                                        cast (cctl_policystatus.typecode_stg AS VARCHAR (60)) AS agmt_current_status,
                                                                                                                                                                                                        cast (NULL AS                           VARCHAR (60)) AS pmt_pln_type_cd,
                                                                                                                                                                                                        (NULL )                                               AS invoicefrequency,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS stmt_cycle_cd,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS bilgmethtype,
                                                                                                                                                                                                        cast(NULL AS  VARCHAR (60))                           AS modeldate,
                                                                                                                                                                                                        cast(''9999-12-31 23:59:59.999999'' AS timestamp)       AS cancellationdate,
                                                                                                                                                                                                        updatetime_stg                                           updatetime,
                                                                                                                                                                                                        cast (cc_policy.id_stg AS VARCHAR (60))               AS publicid,
                                                                                                                                                                                                        (NULL )                                               AS previnsurance_alfa,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS continuousservicedate_alfa ,
                                                                                                                                                                                                        cc_policy.retired_stg                                    retired,
                                                                                                                                                                                                        ''SRC_SYS6''                                            AS src_cd,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS sourceofbusiness_alfa_typecode,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS name,
                                                                                                                                                                                                        (NULL )                                               AS generalplustier_alfa,
                                                                                                                                                                                                        cast (NULL AS INTEGER)                                AS legacydiscount_alfa,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS createtime2,
                                                                                                                                                                                                        cast(''9999-12-31 23:59:59.999999'' AS timestamp)       AS expirationdate2,
                                                                                                                                                                                                        ''N''                                                   AS vfyd_plcy_ind ,
                                                                                                                                                                                                        ($start_dttm)                                         AS start_dttm,
                                                                                                                                                                                                        ($end_dttm)                                           AS end_dttm,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS ovrd_coms_type_cd,
                                                                                                                                                                                                        ''N''                                                   AS legacypolind_alfa,
                                                                                                                                                                                                        NULL                                                  AS isbindonline_alfa
                                                                                                                                                                                            FROM            db_t_prod_stag.cc_policy
                                                                                                                                                                                            left outer join db_t_prod_stag.cctl_policystatus
                                                                                                                                                                                            ON              cctl_policystatus.id_stg=cc_policy.status_stg
                                                                                                                                                                                                        /*  normal unverified DB_T_STAG_DM_PROD.claims */
                                                                                                                                                                                            WHERE           cc_policy.updatetime_stg > ($start_dttm)
                                                                                                                                                                                            AND             cc_policy.updatetime_stg <= ($end_dttm)
                                                                                                                                                                                            AND             cc_policy.verified_stg = 0
                                                                                                                                                                                            AND             coalesce(cc_policy.legacypolind_alfa_stg,0) <> 1
                                                                                                                                                                                            UNION
                                                                                                                                                                                            SELECT          cast (cc_policy.policynumber_stg AS VARCHAR (60))     AS policynumber ,
                                                                                                                                                                                                        (NULL )                                               AS modelnumber,
                                                                                                                                                                                                        (NULL )                                               AS termnumber,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS editeffectivedate,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS periodstart,
                                                                                                                                                                                                        (cc_policy.origeffectivedate_stg)                     AS origeffectivedate,
                                                                                                                                                                                                        (cc_policy.expirationdate_stg )                       AS expirationdate,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS issuedate,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS rejectreason,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp)       AS createtime,
                                                                                                                                                                                                        cast (cctl_policystatus.typecode_stg AS VARCHAR (60)) AS agmt_current_status,
                                                                                                                                                                                                        cast (NULL AS                           VARCHAR (60)) AS pmt_pln_type_cd,
                                                                                                                                                                                                        (NULL )                                               AS invoicefrequency,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS stmt_cycle_cd,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                           AS bilg_meth_type_cd,
                                                                                                                                                                                                        cast(NULL AS  VARCHAR (60))                           AS modeldate,
                                                                                                                                                                                                        cast(''9999-12-31 23:59:59.999999'' AS timestamp)       AS cancellationdate,
                                                                                                                                                                                                        updatetime_stg,
                                                                                                                                                                                                        cast (cc_policy.id_stg AS VARCHAR (60))         AS id,
                                                                                                                                                                                                        (NULL )                                         AS previnsurance_alfa,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS continuousservicedate_alfa ,
                                                                                                                                                                                                        cc_policy.retired_stg,
                                                                                                                                                                                                        ''SRC_SYS6''                                      AS src_cd,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                     AS typecode,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60))                     AS name,
                                                                                                                                                                                                        (NULL )                                         AS generalplustier_alfa,
                                                                                                                                                                                                        cast (NULL AS INTEGER)                          AS legacydiscount_alfa,
                                                                                                                                                                                                        cast(''1900-01-01 00:00:00.000000'' AS timestamp) AS createtime2,
                                                                                                                                                                                                        cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS expirationdate2,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN cc_policy.verified_stg=0 THEN ''N''
                                                                                                                                                                                                        ELSE ''Y''
                                                                                                                                                                                                        END                         AS vfyd_plcy_ind ,
                                                                                                                                                                                                        ($start_dttm)               AS start_dttm,
                                                                                                                                                                                                        ($end_dttm)                 AS end_dttm,
                                                                                                                                                                                                        cast (NULL AS VARCHAR (60)) AS override_commission_value,
                                                                                                                                                                                                        ''Y''                         AS legacypolind_alfa,
                                                                                                                                                                                                        NULL                        AS isbindonline_alfa
                                                                                                                                                                                            FROM            db_t_prod_stag.cc_policy
                                                                                                                                                                                            left outer join db_t_prod_stag.cctl_policystatus
                                                                                                                                                                                            ON              cctl_policystatus.id_stg=cc_policy.status_stg
                                                                                                                                                                                                        /*  normal unverified DB_T_STAG_DM_PROD.claims */
                                                                                                                                                                                            WHERE           cc_policy.updatetime_stg > ($start_dttm)
                                                                                                                                                                                            AND             cc_policy.updatetime_stg <= ($end_dttm)
                                                                                                                                                                                                        /* and cc_policy.verified_stg = 0 */
                                                                                                                                                                                            AND             coalesce(cc_policy.legacypolind_alfa_stg,0) = 1) outer_pc
                                                                                                                                                            WHERE           src_cd=''SRC_SYS6'' )x )z
                                                                                                                   WHERE  z.policynumber IS NOT NULL
                                                                                                                          /* and z.policynumber=''16000000119'' --added DB_T_CORE_DM_PROD.policy */
                                                                                                            )a
                                                                                                            /* where policynumber = ''16000000119'' */
                                                                                                            /*SQ query ends here*/
                                                                                              ) sq1)
                                                                                    left join
                                                                                              (
                                                                                                              /*tier_type_cd*/
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''TIER_TYPE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pc_policyperiod.GeneralPlusTier_alfa''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_tier_type_cd
                                                                                    ON        lkp_tier_type_cd.src_idntftn_val = sq1.generalplustier_alfa
                                                                                              /*busn_cd*/
                                                                                    left join
                                                                                              (
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_OF_BUSN''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_sourceofbusiness_alfa.typecode''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_busn_cd
                                                                                    ON        lkp_busn_cd.src_idntftn_val = sq1.sourceofbusiness_alfa_typecode
                                                                                              /*Override*/
                                                                                    left join
                                                                                              (
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''OVRD_COMS_TYPE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_commissionovrdtype_alfa.typecode'')
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_override
                                                                                    ON        lkp_override.src_idntftn_val = sq1.ovrd_coms_type_cd
                                                                                              /*ref_pmt*/
                                                                                    left join
                                                                                              (
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''PMT_PLN_TYPE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pc_paymentplansummary.name''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_ref_pmt
                                                                                    ON        lkp_ref_pmt.src_idntftn_val = sq1.pmt_pln_type_cd
                                                                                              /*status*/
                                                                                    left join
                                                                                              (
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_TYPE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm IN ( ''pctl_policyperiodstatus.TYPECODE'' ,
                                                                                                                                                                       ''cctl_policystatus.TYPECODE'')
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_status
                                                                                    ON        lkp_status.src_idntftn_val = sq1.agmt_current_status
                                                                                              /*agmt_sts_rsn_type*/
                                                                                    left join
                                                                                              (
                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_RSN_TYPE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_reasoncode.TYPECODE''
                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_agmt_sts_rsn_type
                                                                                    ON        lkp_agmt_sts_rsn_type.src_idntftn_val = sq1.rejectreason ) sq2)
                                                                          /*AGMT_SBTYPE*/
                                                                left join
                                                                          (
                                                                                          SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                          teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                          FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                          WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_SBTYPE''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                          AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_agmt_sb_type
                                                                ON        lkp_agmt_sb_type.src_idntftn_val = ''AGMT_SBTYPE2''
                                                                          /*ref_billing_cd*/
                                                                left join
                                                                          (
                                                                                          SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                          teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                          FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                          WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''BILG_METH_TYPE''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_billingmethod.typecode''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                          AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_ref_billing_cd
                                                                ON        lkp_ref_billing_cd.src_idntftn_val = sq2.bilg_meth_type_cd
                                                                          /*etl_ref_stmt_cd*/
                                                                left join
                                                                          (
                                                                                          SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                          teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                          FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                          WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CYCL_TYPE''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_billingperiodicity.typecode''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                          AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_stmt_cd
                                                                ON        etl_ref_stmt_cd.src_idntftn_val = sq2.stmt_cycle_cd
                                                                          /*SRC_SYS*/
                                                                left join
                                                                          (
                                                                                          SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                          teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                          FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                          WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                          AND             teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                          AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_src_sys
                                                                ON        lkp_src_sys.src_idntftn_val = sq2.src_cd ) sq3 )
                                                      /*data_src*/
                                            left join
                                                      (
                                                                      SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                      FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                      WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''DATA_SRC_TYPE''
                                                                      AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_data_src
                                            ON        lkp_data_src.src_idntftn_val = sq3.v_agmt_src_cd
                                                      /*AGMT*/
                                            left join
                                                      (
                                                                      SELECT DISTINCT agmt.agmt_id                     AS agmt_id,
                                                                                      agmt.host_agmt_num               AS host_agmt_num,
                                                                                      agmt.agmt_name                   AS agmt_name,
                                                                                      agmt.agmt_opn_dttm               AS agmt_opn_dttm,
                                                                                      agmt.agmt_cls_dttm               AS agmt_cls_dttm,
                                                                                      agmt.agmt_plnd_expn_dttm         AS agmt_plnd_expn_dttm,
                                                                                      agmt.agmt_signd_dttm             AS agmt_signd_dttm,
                                                                                      agmt.agmt_legly_bindg_ind        AS agmt_legly_bindg_ind,
                                                                                      agmt.agmt_src_cd                 AS agmt_src_cd,
                                                                                      agmt.agmt_cur_sts_cd             AS agmt_cur_sts_cd,
                                                                                      agmt.agmt_cur_sts_rsn_cd         AS agmt_cur_sts_rsn_cd,
                                                                                      agmt.agmt_obtnd_cd               AS agmt_obtnd_cd,
                                                                                      agmt.agmt_sbtype_cd              AS agmt_sbtype_cd,
                                                                                      agmt.agmt_prcsg_dttm             AS agmt_prcsg_dttm,
                                                                                      agmt.alt_agmt_name               AS alt_agmt_name,
                                                                                      agmt.asset_liabty_cd             AS asset_liabty_cd,
                                                                                      agmt.bal_shet_cd                 AS bal_shet_cd,
                                                                                      agmt.stmt_cycl_cd                AS stmt_cycl_cd,
                                                                                      agmt.stmt_ml_type_cd             AS stmt_ml_type_cd,
                                                                                      agmt.prposl_id                   AS prposl_id,
                                                                                      agmt.agmt_objtv_type_cd          AS agmt_objtv_type_cd,
                                                                                      agmt.fincl_agmt_sbtype_cd        AS fincl_agmt_sbtype_cd,
                                                                                      agmt.mkt_risk_type_cd            AS mkt_risk_type_cd,
                                                                                      agmt.orignl_maturty_dt           AS orignl_maturty_dt,
                                                                                      agmt.risk_expsr_mtgnt_sbtype_cd  AS risk_expsr_mtgnt_sbtype_cd,
                                                                                      agmt.bnk_trd_bk_cd               AS bnk_trd_bk_cd,
                                                                                      agmt.prcg_meth_sbtype_cd         AS prcg_meth_sbtype_cd,
                                                                                      agmt.fincl_agmt_type_cd          AS fincl_agmt_type_cd,
                                                                                      agmt.dy_cnt_bss_cd               AS dy_cnt_bss_cd,
                                                                                      agmt.frst_prem_due_dt            AS frst_prem_due_dt,
                                                                                      agmt.insrnc_agmt_sbtype_cd       AS insrnc_agmt_sbtype_cd,
                                                                                      agmt.insrnc_agmt_type_cd         AS insrnc_agmt_type_cd,
                                                                                      agmt.ntwk_srvc_agmt_type_cd      AS ntwk_srvc_agmt_type_cd,
                                                                                      agmt.frmlty_type_cd              AS frmlty_type_cd,
                                                                                      agmt.cntrct_term_num             AS cntrct_term_num,
                                                                                      agmt.rate_rprcg_cycl_mth_num     AS rate_rprcg_cycl_mth_num,
                                                                                      agmt.cmpnd_int_cycl_mth_num      AS cmpnd_int_cycl_mth_num,
                                                                                      agmt.mdterm_int_pmt_cycl_mth_num AS mdterm_int_pmt_cycl_mth_num,
                                                                                      agmt.prev_mdterm_int_pmt_dt      AS prev_mdterm_int_pmt_dt,
                                                                                      agmt.nxt_mdterm_int_pmt_dt       AS nxt_mdterm_int_pmt_dt,
                                                                                      agmt.prev_int_rate_rvsd_dt       AS prev_int_rate_rvsd_dt,
                                                                                      agmt.nxt_int_rate_rvsd_dt        AS nxt_int_rate_rvsd_dt,
                                                                                      agmt.prev_ref_dt_int_rate        AS prev_ref_dt_int_rate,
                                                                                      agmt.nxt_ref_dt_for_int_rate     AS nxt_ref_dt_for_int_rate,
                                                                                      agmt.mdterm_cncltn_dt            AS mdterm_cncltn_dt,
                                                                                      agmt.stk_flow_clas_in_mth_ind    AS stk_flow_clas_in_mth_ind,
                                                                                      agmt.stk_flow_clas_in_term_ind   AS stk_flow_clas_in_term_ind,
                                                                                      agmt.lgcy_dscnt_ind              AS lgcy_dscnt_ind,
                                                                                      agmt.agmt_idntftn_cd             AS agmt_idntftn_cd,
                                                                                      agmt.trmtn_type_cd               AS trmtn_type_cd,
                                                                                      agmt.int_pmt_meth_cd             AS int_pmt_meth_cd,
                                                                                      agmt.lbr_agmt_desc               AS lbr_agmt_desc,
                                                                                      agmt.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
                                                                                      agmt.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
                                                                                      agmt.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
                                                                                      agmt.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
                                                                                      agmt.busn_prty_id                AS busn_prty_id,
                                                                                      agmt.pmt_pln_type_cd             AS pmt_pln_type_cd,
                                                                                      agmt.invc_strem_type_cd          AS invc_strem_type_cd,
                                                                                      agmt.modl_crtn_dttm              AS modl_crtn_dttm,
                                                                                      agmt.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
                                                                                      agmt.bilg_meth_type_cd           AS bilg_meth_type_cd,
                                                                                      agmt.src_sys_cd                  AS src_sys_cd,
                                                                                      agmt.agmt_eff_dttm               AS agmt_eff_dttm,
                                                                                      agmt.modl_eff_dttm               AS modl_eff_dttm,
                                                                                      agmt.prcs_id                     AS prcs_id,
                                                                                      agmt.modl_actl_end_dttm          AS modl_actl_end_dttm,
                                                                                      agmt.tier_type_cd                AS tier_type_cd,
                                                                                      agmt.edw_strt_dttm               AS edw_strt_dttm,
                                                                                      agmt.edw_end_dttm                AS edw_end_dttm,
                                                                                      agmt.vfyd_plcy_ind               AS vfyd_plcy_ind,
                                                                                      agmt.src_of_busn_cd              AS src_of_busn_cd,
                                                                                      agmt.ovrd_coms_type_cd           AS ovrd_coms_type_cd,
                                                                                      agmt.lgcy_plcy_ind               AS lgcy_plcy_ind,
                                                                                      agmt.trans_strt_dttm             AS trans_strt_dttm,
                                                                                      agmt.nk_src_key                  AS nk_src_key,
                                                                                      agmt.agmt_type_cd                AS agmt_type_cd
                                                                      FROM            db_t_prod_core.agmt
                                                                      WHERE           agmt_type_cd=''PPV'' qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp_agmt
                                            ON        lkp_agmt.nk_src_key = sq3.publicid
                                            AND       lkp_agmt.agmt_type_cd = sq3.out_agmt_type_cd
                                                      /*LKP_XREF_AGMT*/
                                            left join
                                                      (
                                                                      SELECT DISTINCT dir_agmt.agmt_id            AS agmt_id,
                                                                                      trim(dir_agmt.nk_src_key)   AS nk_src_key,
                                                                                      dir_agmt.term_num           AS term_num,
                                                                                      trim(dir_agmt.agmt_type_cd) AS agmt_type_cd
                                                                      FROM            db_t_prod_core.dir_agmt 
                                                                      WHERE           agmt_type_cd=''PPV'' ) lkp_xref_agmt
                                            ON        cast(lkp_xref_agmt.nk_src_key AS VARCHAR(100)) = cast(publicid AS VARCHAR(100))
                                            AND       lkp_xref_agmt.agmt_type_cd = ''PPV'' ) src ) );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT sq_pc_policyperiod.lkp_agmt_id                        AS lkp_agmt_id,
                sq_pc_policyperiod.lkp_edw_end_dttm                   AS lkp_edw_end_dttm,
                sq_pc_policyperiod.lkp_nk_src_key                     AS lkp_nk_src_key,
                sq_pc_policyperiod.policynumber                       AS policynumber,
                sq_pc_policyperiod.modelnumber                        AS modelnumber,
                sq_pc_policyperiod.termnumber                         AS termnumber,
                sq_pc_policyperiod.periodstart                        AS periodstart,
                sq_pc_policyperiod.originaleffectivedate              AS originaleffectivedate,
                sq_pc_policyperiod.periodend                          AS periodend,
                sq_pc_policyperiod.issuedate                          AS issuedate,
                sq_pc_policyperiod.processingdate                     AS processingdate,
                sq_pc_policyperiod.out_agmt_type_cd                   AS out_agmt_type_cd,
                sq_pc_policyperiod.out_agmt_cur_sts_cd                AS out_agmt_cur_sts_cd,
                sq_pc_policyperiod.out_agmt_cur_sts_rsn_cd            AS out_agmt_cur_sts_rsn_cd,
                sq_pc_policyperiod.out_agmt_obtnd_cd                  AS out_agmt_obtnd_cd,
                sq_pc_policyperiod.out_agmt_sbtype_cd                 AS out_agmt_sbtype_cd,
                sq_pc_policyperiod.out_agmt_objtv_type_cd             AS out_agmt_objtv_type_cd,
                sq_pc_policyperiod.out_mkt_risk_type_cd               AS out_mkt_risk_type_cd,
                sq_pc_policyperiod.out_ntwk_srvc_agmt_type_cd         AS out_ntwk_srvc_agmt_type_cd,
                sq_pc_policyperiod.out_frmlty_type_cd                 AS out_frmlty_type_cd,
                sq_pc_policyperiod.out_agmt_idntftn_cd                AS out_agmt_idntftn_cd,
                sq_pc_policyperiod.out_trmtn_type_cd                  AS out_trmtn_type_cd,
                sq_pc_policyperiod.out_int_pmt_meth_cd                AS out_int_pmt_meth_cd,
                sq_pc_policyperiod.out_prcs_id                        AS out_prcs_id,
                sq_pc_policyperiod.out_pmt_pln_type_cd                AS out_pmt_pln_type_cd,
                sq_pc_policyperiod.out_bilg_meth_type_cd              AS out_bilg_meth_type_cd,
                sq_pc_policyperiod.out_stmt_cycle_cd                  AS out_stmt_cycle_cd,
                sq_pc_policyperiod.editeffectivedate                  AS editeffectivedate,
                sq_pc_policyperiod.model_actl_end_ddtm                AS modl_actl_end_dttm,
                sq_pc_policyperiod.modeldate                          AS modeldate,
                sq_pc_policyperiod.publicid                           AS publicid,
                sq_pc_policyperiod.previnsurance_alfa                 AS previnsurance_alfa,
                sq_pc_policyperiod.continuousservicedate_alfa         AS continuousservicedate_alfa,
                sq_pc_policyperiod.edw_strt_dttm1                     AS edw_strt_dttm1,
                sq_pc_policyperiod.legacydiscount_alfa                AS legacydiscount_alfa,
                sq_pc_policyperiod.edw_end_dttm                       AS edw_end_dttm,
                sq_pc_policyperiod.in_tier_type                       AS in_tier_type,
                sq_pc_policyperiod.vfyd_plcy_ind                      AS vfyd_plcy_ind,
                sq_pc_policyperiod.out_src_cd                         AS out_src_cd,
                sq_pc_policyperiod.out_sourceofbusiness_alfa_typecode AS out_sourceofbusiness_alfa_typecode,
                sq_pc_policyperiod.v_lkp_agmt_src_cd                  AS agmt_src_cd,
                sq_pc_policyperiod.updatetime                         AS updatetime,
                sq_pc_policyperiod.lkp_edw_strt_dttm                  AS lkp_edw_strt_dttm,
                sq_pc_policyperiod.retired                            AS retired,
                sq_pc_policyperiod.out_override_commission_value      AS out_override_commission_value,
                sq_pc_policyperiod.in_legacypolind_alfa               AS in_legacypolind_alfa,
                sq_pc_policyperiod.out_legacypolind_alfa              AS out_legacypolind_alfa,
                sq_pc_policyperiod.out_ins_upd                        AS out_ins_upd,
                sq_pc_policyperiod.agmt_id                            AS agmt_id,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component rtr_AGMT_INSERT, Type ROUTER Output Group INSERT
  create
  OR
  replace TEMPORARY TABLE rtr_agmt_insert AS
  SELECT exp_ins_upd.lkp_agmt_id                        AS agmt_id,
         exp_ins_upd.policynumber                       AS policynumber,
         exp_ins_upd.modelnumber                        AS modelnumber,
         exp_ins_upd.termnumber                         AS termnumber,
         exp_ins_upd.periodstart                        AS periodstart,
         exp_ins_upd.originaleffectivedate              AS originaleffectivedate,
         exp_ins_upd.periodend                          AS periodend,
         exp_ins_upd.issuedate                          AS issuedate,
         exp_ins_upd.processingdate                     AS processingdate,
         exp_ins_upd.out_agmt_type_cd                   AS agmt_type_cd,
         exp_ins_upd.out_agmt_cur_sts_cd                AS agmt_cur_sts_cd,
         exp_ins_upd.out_agmt_cur_sts_rsn_cd            AS agmt_cur_sts_rsn_cd,
         exp_ins_upd.out_agmt_obtnd_cd                  AS agmt_obtnd_cd,
         exp_ins_upd.out_agmt_sbtype_cd                 AS agmt_sbtype_cd,
         exp_ins_upd.out_agmt_objtv_type_cd             AS agmt_objtv_type_cd,
         exp_ins_upd.out_mkt_risk_type_cd               AS mkt_risk_type_cd,
         exp_ins_upd.out_ntwk_srvc_agmt_type_cd         AS ntwk_srvc_agmt_type_cd,
         exp_ins_upd.out_frmlty_type_cd                 AS frmlty_type_cd,
         exp_ins_upd.out_agmt_idntftn_cd                AS agmt_idntftn_cd,
         exp_ins_upd.out_trmtn_type_cd                  AS trmtn_type_cd,
         exp_ins_upd.out_int_pmt_meth_cd                AS int_pmt_meth_cd,
         exp_ins_upd.out_prcs_id                        AS prcs_id,
         exp_ins_upd.out_pmt_pln_type_cd                AS pmt_pln_type_cd,
         exp_ins_upd.out_bilg_meth_type_cd              AS out_bilg_meth_type_cd,
         exp_ins_upd.out_stmt_cycle_cd                  AS out_stmt_cycle_cd,
         exp_ins_upd.editeffectivedate                  AS editeffectivedate,
         exp_ins_upd.modl_actl_end_dttm                 AS modl_actl_end_dttm,
         exp_ins_upd.modeldate                          AS modeldate,
         exp_ins_upd.publicid                           AS publicid,
         exp_ins_upd.previnsurance_alfa                 AS previnsurance_alfa,
         exp_ins_upd.edw_strt_dttm1                     AS edw_strt_dttm1,
         exp_ins_upd.edw_end_dttm                       AS edw_end_dttm,
         exp_ins_upd.out_ins_upd                        AS out_ins_upd,
         exp_ins_upd.lkp_edw_strt_dttm                  AS edw_strt_dttm_upd,
         exp_ins_upd.out_src_cd                         AS out_src_cd,
         exp_ins_upd.continuousservicedate_alfa         AS cntnus_srvc_dt,
         exp_ins_upd.retired                            AS retired,
         exp_ins_upd.lkp_edw_end_dttm                   AS lkp_edw_end_dttm,
         exp_ins_upd.lkp_nk_src_key                     AS lkp_nk_src_key,
         exp_ins_upd.in_tier_type                       AS in_tier_type,
         exp_ins_upd.legacydiscount_alfa                AS legacydiscount_alfa,
         exp_ins_upd.vfyd_plcy_ind                      AS vfyd_plcy_ind,
         exp_ins_upd.agmt_src_cd                        AS agmt_src_cd,
         exp_ins_upd.updatetime                         AS updatetime,
         exp_ins_upd.out_sourceofbusiness_alfa_typecode AS out_sourceofbusiness_alfa_typecode,
         exp_ins_upd.out_override_commission_value      AS out_override_commission_value,
         exp_ins_upd.out_legacypolind_alfa              AS legacypolind_alfa,
         exp_ins_upd.agmt_id                            AS logic_agmt_id,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  (
                exp_ins_upd.out_ins_upd = ''I''
         OR     (
                       exp_ins_upd.out_ins_upd = ''U''
                AND    exp_ins_upd.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_ins_upd.retired = 0
                AND    exp_ins_upd.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_AGMT_Retired, Type ROUTER Output Group Retired
  CREATE
  OR
  replace TEMPORARY TABLE rtr_agmt_retired AS
  SELECT exp_ins_upd.lkp_agmt_id                        AS agmt_id,
         exp_ins_upd.policynumber                       AS policynumber,
         exp_ins_upd.modelnumber                        AS modelnumber,
         exp_ins_upd.termnumber                         AS termnumber,
         exp_ins_upd.periodstart                        AS periodstart,
         exp_ins_upd.originaleffectivedate              AS originaleffectivedate,
         exp_ins_upd.periodend                          AS periodend,
         exp_ins_upd.issuedate                          AS issuedate,
         exp_ins_upd.processingdate                     AS processingdate,
         exp_ins_upd.out_agmt_type_cd                   AS agmt_type_cd,
         exp_ins_upd.out_agmt_cur_sts_cd                AS agmt_cur_sts_cd,
         exp_ins_upd.out_agmt_cur_sts_rsn_cd            AS agmt_cur_sts_rsn_cd,
         exp_ins_upd.out_agmt_obtnd_cd                  AS agmt_obtnd_cd,
         exp_ins_upd.out_agmt_sbtype_cd                 AS agmt_sbtype_cd,
         exp_ins_upd.out_agmt_objtv_type_cd             AS agmt_objtv_type_cd,
         exp_ins_upd.out_mkt_risk_type_cd               AS mkt_risk_type_cd,
         exp_ins_upd.out_ntwk_srvc_agmt_type_cd         AS ntwk_srvc_agmt_type_cd,
         exp_ins_upd.out_frmlty_type_cd                 AS frmlty_type_cd,
         exp_ins_upd.out_agmt_idntftn_cd                AS agmt_idntftn_cd,
         exp_ins_upd.out_trmtn_type_cd                  AS trmtn_type_cd,
         exp_ins_upd.out_int_pmt_meth_cd                AS int_pmt_meth_cd,
         exp_ins_upd.out_prcs_id                        AS prcs_id,
         exp_ins_upd.out_pmt_pln_type_cd                AS pmt_pln_type_cd,
         exp_ins_upd.out_bilg_meth_type_cd              AS out_bilg_meth_type_cd,
         exp_ins_upd.out_stmt_cycle_cd                  AS out_stmt_cycle_cd,
         exp_ins_upd.editeffectivedate                  AS editeffectivedate,
         exp_ins_upd.modl_actl_end_dttm                 AS modl_actl_end_dttm,
         exp_ins_upd.modeldate                          AS modeldate,
         exp_ins_upd.publicid                           AS publicid,
         exp_ins_upd.previnsurance_alfa                 AS previnsurance_alfa,
         exp_ins_upd.edw_strt_dttm1                     AS edw_strt_dttm1,
         exp_ins_upd.edw_end_dttm                       AS edw_end_dttm,
         exp_ins_upd.out_ins_upd                        AS out_ins_upd,
         exp_ins_upd.lkp_edw_strt_dttm                  AS edw_strt_dttm_upd,
         exp_ins_upd.out_src_cd                         AS out_src_cd,
         exp_ins_upd.continuousservicedate_alfa         AS cntnus_srvc_dt,
         exp_ins_upd.retired                            AS retired,
         exp_ins_upd.lkp_edw_end_dttm                   AS lkp_edw_end_dttm,
         exp_ins_upd.lkp_nk_src_key                     AS lkp_nk_src_key,
         exp_ins_upd.in_tier_type                       AS in_tier_type,
         exp_ins_upd.legacydiscount_alfa                AS legacydiscount_alfa,
         exp_ins_upd.vfyd_plcy_ind                      AS vfyd_plcy_ind,
         exp_ins_upd.agmt_src_cd                        AS agmt_src_cd,
         exp_ins_upd.updatetime                         AS updatetime,
         exp_ins_upd.out_sourceofbusiness_alfa_typecode AS out_sourceofbusiness_alfa_typecode,
         exp_ins_upd.out_override_commission_value      AS out_override_commission_value,
         exp_ins_upd.out_legacypolind_alfa              AS legacypolind_alfa,
         exp_ins_upd.agmt_id                            AS logic_agmt_id,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.out_ins_upd = ''R''
  AND    exp_ins_upd.retired != 0
  AND    exp_ins_upd.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_retired.agmt_id           AS agmt_id5,
                rtr_agmt_retired.lkp_nk_src_key    AS lkp_nk_src_key5,
                rtr_agmt_retired.edw_strt_dttm_upd AS edw_strt_dttm_upd5,
                rtr_agmt_retired.updatetime        AS updatetime3,
                1                                  AS update_strategy_action,
                rtr_agmt_retired.source_record_id
         FROM   rtr_agmt_retired );
  -- Component upd_AGMT_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_insert.policynumber                       AS policynumber,
                rtr_agmt_insert.logic_agmt_id                      AS agmt_logic,
                rtr_agmt_insert.modelnumber                        AS modelnumber,
                rtr_agmt_insert.termnumber                         AS termnumber,
                rtr_agmt_insert.periodstart                        AS periodstart,
                rtr_agmt_insert.originaleffectivedate              AS originaleffectivedate,
                rtr_agmt_insert.periodend                          AS periodend,
                rtr_agmt_insert.issuedate                          AS issuedate,
                rtr_agmt_insert.processingdate                     AS createtime,
                rtr_agmt_insert.agmt_type_cd                       AS agmt_type_cd,
                rtr_agmt_insert.agmt_cur_sts_cd                    AS agmt_cur_sts_cd,
                rtr_agmt_insert.agmt_cur_sts_rsn_cd                AS agmt_cur_sts_rsn_cd,
                rtr_agmt_insert.agmt_obtnd_cd                      AS agmt_obtnd_cd,
                rtr_agmt_insert.agmt_sbtype_cd                     AS agmt_sbtype_cd,
                rtr_agmt_insert.agmt_objtv_type_cd                 AS agmt_objtv_type_cd,
                rtr_agmt_insert.mkt_risk_type_cd                   AS mkt_risk_type_cd,
                rtr_agmt_insert.ntwk_srvc_agmt_type_cd             AS ntwk_srvc_agmt_type_cd,
                rtr_agmt_insert.frmlty_type_cd                     AS frmlty_type_cd,
                rtr_agmt_insert.agmt_idntftn_cd                    AS agmt_idntftn_cd,
                rtr_agmt_insert.trmtn_type_cd                      AS trmtn_type_cd,
                rtr_agmt_insert.int_pmt_meth_cd                    AS int_pmt_meth_cd,
                rtr_agmt_insert.prcs_id                            AS prcs_id,
                rtr_agmt_insert.pmt_pln_type_cd                    AS pmt_pln_type_cd2,
                rtr_agmt_insert.out_bilg_meth_type_cd              AS out_bilg_meth_type_cd1,
                rtr_agmt_insert.out_stmt_cycle_cd                  AS out_stmt_cycle_cd1,
                rtr_agmt_insert.editeffectivedate                  AS editeffectivedate,
                rtr_agmt_insert.modl_actl_end_dttm                 AS modl_actl_end_dttm,
                rtr_agmt_insert.modeldate                          AS modeldate,
                rtr_agmt_insert.publicid                           AS publicid1,
                rtr_agmt_insert.edw_strt_dttm1                     AS edw_strt_dttm1,
                rtr_agmt_insert.edw_end_dttm                       AS edw_end_dttm1,
                rtr_agmt_insert.previnsurance_alfa                 AS previnsurance_alfa1,
                rtr_agmt_insert.out_src_cd                         AS out_src_cd1,
                rtr_agmt_insert.cntnus_srvc_dt                     AS cntnus_srvc_dt4,
                rtr_agmt_insert.retired                            AS retired1,
                rtr_agmt_insert.in_tier_type                       AS in_tier_type1,
                rtr_agmt_insert.legacydiscount_alfa                AS legacydiscount_alfa3,
                rtr_agmt_insert.vfyd_plcy_ind                      AS vfyd_plcy_ind1,
                rtr_agmt_insert.agmt_src_cd                        AS agmt_src_cd1,
                rtr_agmt_insert.updatetime                         AS updatetime1,
                rtr_agmt_insert.out_sourceofbusiness_alfa_typecode AS out_sourceofbusiness_alfa_typecode1,
                rtr_agmt_insert.out_override_commission_value      AS out_override_commission_value1,
                rtr_agmt_insert.legacypolind_alfa                  AS legacypolind_alfa1,
                0                                                  AS update_strategy_action,
               rtr_agmt_insert.source_record_id
                FROM   rtr_agmt_insert );
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT upd_retired.agmt_id5                  AS agmt_id5,
                upd_retired.lkp_nk_src_key5           AS lkp_nk_src_key5,
                dateadd(''second'', - 1, current_timestamp) AS edw_end_dttm,
                upd_retired.edw_strt_dttm_upd5        AS lkp_edw_strt_dttm,
                upd_retired.updatetime3               AS updatetime3,
                upd_retired.source_record_id
         FROM   upd_retired );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_agmt_ins.agmt_logic             AS agmt_id,
                upd_agmt_ins.policynumber           AS host_agmt_num,
                upd_agmt_ins.originaleffectivedate  AS agmt_opn_dttm,
                upd_agmt_ins.periodend              AS agmt_plnd_expn_dt,
                upd_agmt_ins.issuedate              AS agmt_signd_dt,
                upd_agmt_ins.agmt_type_cd           AS agmt_type_cd,
                upd_agmt_ins.out_src_cd1            AS agmt_src_cd,
                upd_agmt_ins.agmt_cur_sts_cd        AS agmt_cur_sts_cd,
                upd_agmt_ins.agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
                upd_agmt_ins.agmt_obtnd_cd          AS agmt_obtnd_cd,
                upd_agmt_ins.agmt_sbtype_cd         AS agmt_sbtype_cd,
                upd_agmt_ins.createtime             AS agmt_prcsg_dt,
                upd_agmt_ins.out_stmt_cycle_cd1     AS stmt_cycl_cd,
                upd_agmt_ins.agmt_objtv_type_cd     AS agmt_objtv_type_cd,
                upd_agmt_ins.mkt_risk_type_cd       AS mkt_risk_type_cd,
                upd_agmt_ins.ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
                upd_agmt_ins.frmlty_type_cd         AS frmlty_type_cd,
                upd_agmt_ins.agmt_idntftn_cd        AS agmt_idntftn_cd,
                upd_agmt_ins.trmtn_type_cd          AS trmtn_type_cd,
                upd_agmt_ins.int_pmt_meth_cd        AS int_pmt_meth_cd,
                upd_agmt_ins.pmt_pln_type_cd2       AS pmt_pln_type_cd,
                upd_agmt_ins.modelnumber            AS modl_num,
                upd_agmt_ins.modeldate              AS modeldate,
                upd_agmt_ins.out_bilg_meth_type_cd1 AS bilg_meth_type_cd,
                upd_agmt_ins.periodstart            AS eff_dt,
                upd_agmt_ins.termnumber             AS term_num,
                upd_agmt_ins.editeffectivedate      AS editeffectivedate,
                upd_agmt_ins.prcs_id                AS prcs_id,
                upd_agmt_ins.modl_actl_end_dttm     AS modl_actl_end_dttm,
                upd_agmt_ins.publicid1              AS publicid1,
                upd_agmt_ins.edw_strt_dttm1         AS edw_strt_dttm1,
                CASE
                       WHEN upd_agmt_ins.retired1 <> 0 THEN current_timestamp
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                               AS edw_end_dttm11,
                upd_agmt_ins.previnsurance_alfa1  AS previnsurance_alfa1,
                upd_agmt_ins.out_src_cd1          AS out_src_cd1,
                upd_agmt_ins.cntnus_srvc_dt4      AS cntnus_srvc_dt4,
                upd_agmt_ins.in_tier_type1        AS in_tier_type1,
                upd_agmt_ins.legacydiscount_alfa3 AS legacydiscount_alfa3,
                upd_agmt_ins.vfyd_plcy_ind1       AS vfyd_plcy_ind1,
                upd_agmt_ins.agmt_src_cd1         AS agmt_src_cd1,
                upd_agmt_ins.updatetime1          AS updatetime1,
                CASE
                       WHEN upd_agmt_ins.retired1 <> 0 THEN upd_agmt_ins.updatetime1
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                              AS trans_end_dttm,
                upd_agmt_ins.out_sourceofbusiness_alfa_typecode1 AS out_sourceofbusiness_alfa_typecode1,
                upd_agmt_ins.out_override_commission_value1      AS out_override_commission_value1,
                upd_agmt_ins.legacypolind_alfa1                  AS legacypolind_alfa1,
                upd_agmt_ins.source_record_id
         FROM   upd_agmt_ins );
  -- Component AGMT_Ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_opn_dttm,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_cur_sts_rsn_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          stmt_cycl_cd,
                          agmt_objtv_type_cd,
                          mkt_risk_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          pmt_pln_type_cd,
                          modl_num,
                          modl_crtn_dttm,
                          bilg_meth_type_cd,
                          agmt_eff_dttm,
                          term_num,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          prior_insrnc_ind,
                          nk_src_key,
                          tier_type_cd,
                          lgcy_dscnt_ind,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          vfyd_plcy_ind,
                          src_of_busn_cd,
                          ovrd_coms_type_cd,
                          lgcy_plcy_ind,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id                             AS agmt_id,
         exp_pass_to_target_ins.host_agmt_num                       AS host_agmt_num,
         exp_pass_to_target_ins.agmt_opn_dttm                       AS agmt_opn_dttm,
         exp_pass_to_target_ins.agmt_plnd_expn_dt                   AS agmt_plnd_expn_dttm,
         exp_pass_to_target_ins.agmt_signd_dt                       AS agmt_signd_dttm,
         exp_pass_to_target_ins.agmt_type_cd                        AS agmt_type_cd,
         exp_pass_to_target_ins.agmt_src_cd1                        AS agmt_src_cd,
         exp_pass_to_target_ins.agmt_cur_sts_cd                     AS agmt_cur_sts_cd,
         exp_pass_to_target_ins.agmt_cur_sts_rsn_cd                 AS agmt_cur_sts_rsn_cd,
         exp_pass_to_target_ins.agmt_obtnd_cd                       AS agmt_obtnd_cd,
         exp_pass_to_target_ins.agmt_sbtype_cd                      AS agmt_sbtype_cd,
         exp_pass_to_target_ins.agmt_prcsg_dt                       AS agmt_prcsg_dttm,
         exp_pass_to_target_ins.stmt_cycl_cd                        AS stmt_cycl_cd,
         exp_pass_to_target_ins.agmt_objtv_type_cd                  AS agmt_objtv_type_cd,
         exp_pass_to_target_ins.mkt_risk_type_cd                    AS mkt_risk_type_cd,
         exp_pass_to_target_ins.ntwk_srvc_agmt_type_cd              AS ntwk_srvc_agmt_type_cd,
         exp_pass_to_target_ins.frmlty_type_cd                      AS frmlty_type_cd,
         exp_pass_to_target_ins.agmt_idntftn_cd                     AS agmt_idntftn_cd,
         exp_pass_to_target_ins.trmtn_type_cd                       AS trmtn_type_cd,
         exp_pass_to_target_ins.int_pmt_meth_cd                     AS int_pmt_meth_cd,
         exp_pass_to_target_ins.pmt_pln_type_cd                     AS pmt_pln_type_cd,
         exp_pass_to_target_ins.modl_num                            AS modl_num,
         exp_pass_to_target_ins.modeldate                           AS modl_crtn_dttm,
         exp_pass_to_target_ins.bilg_meth_type_cd                   AS bilg_meth_type_cd,
         exp_pass_to_target_ins.eff_dt                              AS agmt_eff_dttm,
         exp_pass_to_target_ins.term_num                            AS term_num,
         exp_pass_to_target_ins.editeffectivedate                   AS modl_eff_dttm,
         exp_pass_to_target_ins.prcs_id                             AS prcs_id,
         exp_pass_to_target_ins.modl_actl_end_dttm                  AS modl_actl_end_dttm,
         exp_pass_to_target_ins.cntnus_srvc_dt4                     AS cntnus_srvc_dttm,
         exp_pass_to_target_ins.previnsurance_alfa1                 AS prior_insrnc_ind,
         exp_pass_to_target_ins.publicid1                           AS nk_src_key,
         exp_pass_to_target_ins.in_tier_type1                       AS tier_type_cd,
         exp_pass_to_target_ins.legacydiscount_alfa3                AS lgcy_dscnt_ind,
         exp_pass_to_target_ins.agmt_src_cd                         AS src_sys_cd,
         exp_pass_to_target_ins.edw_strt_dttm1                      AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm11                      AS edw_end_dttm,
         exp_pass_to_target_ins.vfyd_plcy_ind1                      AS vfyd_plcy_ind,
         exp_pass_to_target_ins.out_sourceofbusiness_alfa_typecode1 AS src_of_busn_cd,
         exp_pass_to_target_ins.out_override_commission_value1      AS ovrd_coms_type_cd,
         exp_pass_to_target_ins.legacypolind_alfa1                  AS lgcy_plcy_ind,
         exp_pass_to_target_ins.updatetime1                         AS trans_strt_dttm,
         exp_pass_to_target_ins.trans_end_dttm                      AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component AGMT_Upd_retired, Type TARGET
  merge
  INTO         db_t_prod_core.agmt
  USING        exp_retired
  ON (
                            agmt.agmt_id = exp_retired.agmt_id5
               AND          agmt.nk_src_key = exp_retired.lkp_nk_src_key5
               AND          agmt.edw_strt_dttm = exp_retired.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_retired.agmt_id5,
         nk_src_key = exp_retired.lkp_nk_src_key5,
         edw_strt_dttm = exp_retired.lkp_edw_strt_dttm,
         edw_end_dttm = exp_retired.edw_end_dttm,
         trans_end_dttm = exp_retired.updatetime3;
  
  -- Component AGMT_Upd_retired, Type Post SQL
  UPDATE db_t_prod_core.agmt AS a
  SET    edw_end_dttm=b.new_edw_end_dttm,
         trans_end_dttm=b.new_trans_end_dttm
  FROM   (
                  SELECT   agmt_id,
                           agmt_type_cd,
                           nk_src_key,
                           edw_strt_dttm,
                           trans_strt_dttm,
                           coalesce(max(edw_strt_dttm) over (PARTITION BY agmt_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following)                   - interval ''1 second'', edw_end_dttm)  AS new_edw_end_dttm,
                           coalesce(max(trans_strt_dttm) over (PARTITION BY agmt_id ORDER BY trans_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following) - interval ''1 second'',trans_end_dttm) AS new_trans_end_dttm
                  FROM     db_t_prod_core.agmt
                  WHERE    agmt_type_cd=''PPV'' qualify edw_end_dttm <> new_edw_end_dttm
                  OR       trans_end_dttm <> new_trans_end_dttm ) b
  WHERE  a.agmt_id=b.agmt_id
  AND    a.agmt_type_cd=b.agmt_type_cd
  AND    a.nk_src_key=b.nk_src_key
  AND    a.edw_strt_dttm=b.edw_strt_dttm
  AND    a.trans_strt_dttm=b.trans_strt_dttm;
  
  UPDATE db_t_prod_core.agmt AS a
  SET    modl_actl_end_dttm=b.new_modl_actl_end_dttm
  FROM   (
                  SELECT   e1.host_agmt_num,
                           e1.term_num,
                           e1.modl_num,
                           e1.agmt_id,
                           e1.agmt_type_cd,
                           e1.nk_src_key,
                           e1.modl_eff_dttm,
                           coalesce(max(e1.modl_eff_dttm) over(PARTITION BY e1.host_agmt_num,e1.term_num ORDER BY e1.modl_num ROWS BETWEEN 1 following AND      1 following),modl_actl_end_dttm) AS new_modl_actl_end_dttm
                  FROM     db_t_prod_core.agmt e1
                  join
                           (
                                    SELECT   agmt_id,
                                             max(edw_strt_dttm) AS max_edw_strt_dttm
                                    FROM     db_t_prod_core.agmt
                                    WHERE    agmt_type_cd = ''PPV''
                                    GROUP BY 1) e2
                  ON       e1.agmt_id = e2.agmt_id
                  AND      e1.edw_strt_dttm=e2.max_edw_strt_dttm ) b
  WHERE  a.host_agmt_num=b.host_agmt_num
  AND    a.term_num=b.term_num
  AND    a.modl_num=b.modl_num
  AND    a.agmt_id=b.agmt_id
  AND    a.agmt_type_cd=b.agmt_type_cd
  AND    a.nk_src_key=b.nk_src_key
  AND    a.modl_eff_dttm=b.modl_eff_dttm
  AND    a.modl_actl_end_dttm <> b.new_modl_actl_end_dttm ;

END;
';