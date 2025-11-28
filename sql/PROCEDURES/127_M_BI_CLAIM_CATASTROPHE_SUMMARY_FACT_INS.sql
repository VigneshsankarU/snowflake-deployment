-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_CLAIM_CATASTROPHE_SUMMARY_FACT_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       v_start_time TIMESTAMP;
BEGIN
       v_start_time := CURRENT_TIMESTAMP();

-- Component src_sq_cat_clm_ctstrph_sumry_f, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_clm_ctstrph_sumry_f AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ctstrph_id,
$2 as plcy_type_cd,
$3 as risk_loss_loc_id,
$4 as srvc_ctr_id,
$5 as snpshot_intrval_dttm,
$6 as clm_sts_type_cd,
$7 as undrwrtr_id,
$8 as clm_cnt,
$9 as clm_pmt_cnt,
$10 as clm_tot_pd_loss_amt,
$11 as clm_tot_rserv_amt,
$12 as clm_salv_rcvry_net_amt,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_clm_ctstrph_sumry_f.ctstrph_id,
cat_clm_ctstrph_sumry_f.plcy_type_cd,
cat_clm_ctstrph_sumry_f.risk_loss_loc_id,
cat_clm_ctstrph_sumry_f.srvc_ctr_id,
cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm,
cat_clm_ctstrph_sumry_f.clm_sts_type_cd,
cat_clm_ctstrph_sumry_f.undrwrtr_id,
cat_clm_ctstrph_sumry_f.clm_cnt,
cat_clm_ctstrph_sumry_f.clm_pmt_cnt,
cat_clm_ctstrph_sumry_f.clm_tot_pd_loss_amt,
cat_clm_ctstrph_sumry_f.clm_tot_rserv_amt,
cat_clm_ctstrph_sumry_f.clm_salv_rcvry_net_amt
FROM DB_T_PROD_STAG.cat_clm_ctstrph_sumry_f
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
src_sq_cat_clm_ctstrph_sumry_f.ctstrph_id as ctstrph_id,
src_sq_cat_clm_ctstrph_sumry_f.plcy_type_cd as plcy_type_cd,
src_sq_cat_clm_ctstrph_sumry_f.risk_loss_loc_id as risk_loss_loc_id,
src_sq_cat_clm_ctstrph_sumry_f.srvc_ctr_id as srvc_ctr_id,
src_sq_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm as snpshot_intrval_dttm,
src_sq_cat_clm_ctstrph_sumry_f.clm_sts_type_cd as clm_sts_type_cd,
src_sq_cat_clm_ctstrph_sumry_f.undrwrtr_id as undrwrtr_id,
src_sq_cat_clm_ctstrph_sumry_f.clm_cnt as clm_cnt,
src_sq_cat_clm_ctstrph_sumry_f.clm_pmt_cnt as clm_pmt_cnt,
src_sq_cat_clm_ctstrph_sumry_f.clm_tot_pd_loss_amt as clm_tot_pd_loss_amt,
src_sq_cat_clm_ctstrph_sumry_f.clm_tot_rserv_amt as clm_tot_rserv_amt,
src_sq_cat_clm_ctstrph_sumry_f.clm_salv_rcvry_net_amt as clm_salv_rcvry_net_amt,
src_sq_cat_clm_ctstrph_sumry_f.source_record_id
FROM
src_sq_cat_clm_ctstrph_sumry_f
);


-- Component tgt_CLM_CTSTRPH_SUMRY_F, Type TARGET 
INSERT INTO DB_V_PROD_BASE.CLM_CTSTRPH_SUMRY_F
(
CTSTRPH_ID,
PLCY_TYPE_CD,
RISK_LOSS_LOC_ID,
SRVC_CTR_ID,
SNPSHOT_INTRVAL_DTTM,
CLM_STS_TYPE_CD,
UNDRWRTR_ID,
CLM_CNT,
CLM_PMT_CNT,
CLM_TOT_PD_LOSS_AMT,
CLM_TOT_RSERV_AMT,
CLM_SALV_RCVRY_NET_AMT
)
SELECT
exp_pass_to_tgt.ctstrph_id as CTSTRPH_ID,
exp_pass_to_tgt.plcy_type_cd as PLCY_TYPE_CD,
exp_pass_to_tgt.risk_loss_loc_id as RISK_LOSS_LOC_ID,
exp_pass_to_tgt.srvc_ctr_id as SRVC_CTR_ID,
exp_pass_to_tgt.snpshot_intrval_dttm as SNPSHOT_INTRVAL_DTTM,
exp_pass_to_tgt.clm_sts_type_cd as CLM_STS_TYPE_CD,
exp_pass_to_tgt.undrwrtr_id as UNDRWRTR_ID,
exp_pass_to_tgt.clm_cnt as CLM_CNT,
exp_pass_to_tgt.clm_pmt_cnt as CLM_PMT_CNT,
exp_pass_to_tgt.clm_tot_pd_loss_amt as CLM_TOT_PD_LOSS_AMT,
exp_pass_to_tgt.clm_tot_rserv_amt as CLM_TOT_RSERV_AMT,
exp_pass_to_tgt.clm_salv_rcvry_net_amt as CLM_SALV_RCVRY_NET_AMT
FROM
exp_pass_to_tgt;


END; ';