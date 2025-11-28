-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_SERVICE_CENTER_DIM_INS("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_cat_srvc_ctr_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cat_srvc_ctr_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as srvc_ctr_id,
$2 as srvc_ctr_num,
$3 as srvc_ctr_name,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_srvc_ctr_d.srvc_ctr_id,
cat_srvc_ctr_d.srvc_ctr_num,
cat_srvc_ctr_d.srvc_ctr_name
FROM db_t_prod_stag.cat_srvc_ctr_d
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
SQ_cat_srvc_ctr_d.srvc_ctr_id as srvc_ctr_id,
SQ_cat_srvc_ctr_d.srvc_ctr_num as srvc_ctr_num,
SQ_cat_srvc_ctr_d.srvc_ctr_name as srvc_ctr_name,
SQ_cat_srvc_ctr_d.source_record_id
FROM
SQ_cat_srvc_ctr_d
);


-- Component SRVC_CTR_D, Type TARGET 
INSERT INTO db_v_prod_base.SRVC_CTR_D
(
SRVC_CTR_ID,
SRVC_CTR_NUM,
SRVC_CTR_NAME
)
SELECT
exp_pass_to_tgt.srvc_ctr_id as SRVC_CTR_ID,
exp_pass_to_tgt.srvc_ctr_num as SRVC_CTR_NUM,
exp_pass_to_tgt.srvc_ctr_name as SRVC_CTR_NAME
FROM
exp_pass_to_tgt;


END; ';