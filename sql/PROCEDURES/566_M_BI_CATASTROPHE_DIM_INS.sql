-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_CATASTROPHE_DIM_INS("WORKLET_NAME" VARCHAR)
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
 

-- Component src_sq_cat_ctstrph_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_ctstrph_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ctstrph_id,
$2 as ctstrph_strt_dt,
$3 as ctstrph_end_dt,
$4 as ctstrph_name,
$5 as ctstrph_num,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_ctstrph_d.ctstrph_id,
cat_ctstrph_d.ctstrph_strt_dt,
cat_ctstrph_d.ctstrph_end_dt,
cat_ctstrph_d.ctstrph_name,
cat_ctstrph_d.ctstrph_num
FROM db_t_prod_stag.cat_ctstrph_d
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
src_sq_cat_ctstrph_d.ctstrph_id as ctstrph_id,
src_sq_cat_ctstrph_d.ctstrph_strt_dt as ctstrph_strt_dt,
src_sq_cat_ctstrph_d.ctstrph_end_dt as ctstrph_end_dt,
src_sq_cat_ctstrph_d.ctstrph_name as ctstrph_name,
src_sq_cat_ctstrph_d.ctstrph_num as ctstrph_num,
src_sq_cat_ctstrph_d.source_record_id
FROM
src_sq_cat_ctstrph_d
);


-- Component tgt_ctstrph_d, Type TARGET 
INSERT INTO db_v_prod_base.CTSTRPH_D
(
CTSTRPH_ID,
CTSTRPH_STRT_DT,
CTSTRPH_END_DT,
CTSTRPH_NAME,
CTSTRPH_NUM
)
SELECT
exp_pass_to_tgt.ctstrph_id as CTSTRPH_ID,
exp_pass_to_tgt.ctstrph_strt_dt as CTSTRPH_STRT_DT,
exp_pass_to_tgt.ctstrph_end_dt as CTSTRPH_END_DT,
exp_pass_to_tgt.ctstrph_name as CTSTRPH_NAME,
exp_pass_to_tgt.ctstrph_num as CTSTRPH_NUM
FROM
exp_pass_to_tgt;


END; ';