-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_CLAIM_STATUS_DIM_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
 

-- Component src_sq_cat_clm_sts_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_clm_sts_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as clm_sts_type_cd,
$2 as clm_sts_type_desc,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_clm_sts_d.clm_sts_type_cd,
cat_clm_sts_d.clm_sts_type_desc
FROM db_t_prod_stag.cat_clm_sts_d
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
src_sq_cat_clm_sts_d.clm_sts_type_cd as clm_sts_type_cd,
src_sq_cat_clm_sts_d.clm_sts_type_desc as clm_sts_type_desc,
src_sq_cat_clm_sts_d.source_record_id
FROM
src_sq_cat_clm_sts_d
);


-- Component tgt_clm_sts_cd, Type TARGET 
INSERT INTO db_v_prod_base.CLM_STS_D
(
CLM_STS_TYPE_CD,
CLM_STS_TYPE_DESC
)
SELECT
exp_pass_to_tgt.clm_sts_type_cd as CLM_STS_TYPE_CD,
exp_pass_to_tgt.clm_sts_type_desc as CLM_STS_TYPE_DESC
FROM
exp_pass_to_tgt;


END; ';