-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_UNDERWRITER_DIM_INS("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_cat_undrwrtr_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cat_undrwrtr_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as undrwrtr_id,
$2 as undrwrtr_cd,
$3 as undrwrtr_co_name,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_undrwrtr_d.undrwrtr_id,
cat_undrwrtr_d.undrwrtr_cd,
cat_undrwrtr_d.undrwrtr_co_name
FROM db_t_prod_stag.cat_undrwrtr_d
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
SQ_cat_undrwrtr_d.undrwrtr_id as undrwrtr_id,
SQ_cat_undrwrtr_d.undrwrtr_cd as undrwrtr_cd,
SQ_cat_undrwrtr_d.undrwrtr_co_name as undrwrtr_co_name,
SQ_cat_undrwrtr_d.source_record_id
FROM
SQ_cat_undrwrtr_d
);


-- Component UNDRWRTR_D, Type TARGET 
INSERT INTO db_v_prod_base.UNDRWRTR_D
(
UNDRWRTR_ID,
UNDRWRTR_CD,
UNDRWRTR_CO_NAME
)
SELECT
exp_pass_to_tgt.undrwrtr_id as UNDRWRTR_ID,
exp_pass_to_tgt.undrwrtr_cd as UNDRWRTR_CD,
exp_pass_to_tgt.undrwrtr_co_name as UNDRWRTR_CO_NAME
FROM
exp_pass_to_tgt;


END; ';