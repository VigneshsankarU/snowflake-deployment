-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGT_ASIC("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  PRCS_ID STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_AGT_ASIC, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AGT_ASIC AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as DISTRICT,
$3 as REGION,
$4 as APP_YEAR,
$5 as APP_MONTH,
$6 as APPS_N,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
AGT_ASIC.AGENT_NBR,
AGT_ASIC.DISTRICT,
AGT_ASIC.REGION,
AGT_ASIC.APP_YEAR,
AGT_ASIC.APP_MONTH,
AGT_ASIC.APPS_N
FROM db_t_prod_stag.agt_asic
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_AGT_ASIC.AGENT_NBR as AGENT_NBR,
SQ_AGT_ASIC.DISTRICT as DISTRICT,
SQ_AGT_ASIC.REGION as REGION,
SQ_AGT_ASIC.APP_YEAR as APP_YEAR,
SQ_AGT_ASIC.APP_MONTH as APP_MONTH,
SQ_AGT_ASIC.APPS_N as APPS_N,
:PRCS_ID as PRCS_ID,
SQ_AGT_ASIC.source_record_id
FROM
SQ_AGT_ASIC
);


-- Component AGT_ASIC1, Type TARGET 
INSERT INTO db_t_prod_comn.AGT_ASIC
(
AGENT_NBR,
DISTRICT,
REGION,
APP_YEAR,
APP_MONTH,
APPS_N,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.REGION as REGION,
exp_pass_through.APP_YEAR as APP_YEAR,
exp_pass_through.APP_MONTH as APP_MONTH,
exp_pass_through.APPS_N as APPS_N,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';