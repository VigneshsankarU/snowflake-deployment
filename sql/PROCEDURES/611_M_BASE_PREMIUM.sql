-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PREMIUM("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_PREMIUM, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PREMIUM AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as STATE_NBR,
$3 as SERVICE_CENTER,
$4 as ACCOUNTING_DT,
$5 as CMPY_ABBREV,
$6 as POLICY_SYMBOL,
$7 as FIELD_TYPE,
$8 as AMOUNT,
$9 as USER_ID,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
PREMIUM.AGENT_NBR,
PREMIUM.STATE_NBR,
PREMIUM.SERVICE_CENTER,
PREMIUM.ACCOUNTING_DT,
PREMIUM.CMPY_ABBREV,
PREMIUM.POLICY_SYMBOL,
PREMIUM.FIELD_TYPE,
PREMIUM.AMOUNT,
PREMIUM.USER_ID
FROM db_t_prod_stag.PREMIUM
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PREMIUM.AGENT_NBR as AGENT_NBR,
SQ_PREMIUM.STATE_NBR as STATE_NBR,
SQ_PREMIUM.SERVICE_CENTER as SERVICE_CENTER,
SQ_PREMIUM.ACCOUNTING_DT as ACCOUNTING_DT,
SQ_PREMIUM.CMPY_ABBREV as CMPY_ABBREV,
SQ_PREMIUM.POLICY_SYMBOL as POLICY_SYMBOL,
SQ_PREMIUM.FIELD_TYPE as FIELD_TYPE,
SQ_PREMIUM.AMOUNT as AMOUNT,
SQ_PREMIUM.USER_ID as USER_ID,
:prcs_id as prcs_id,
SQ_PREMIUM.source_record_id
FROM
SQ_PREMIUM
);


-- Component PREMIUM1, Type TARGET 
INSERT INTO db_t_prod_comn.PREMIUM
(
AGENT_NBR,
STATE_NBR,
SERVICE_CENTER,
ACCOUNTING_DT,
CMPY_ABBREV,
POLICY_SYMBOL,
FIELD_TYPE,
AMOUNT,
USER_ID,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.STATE_NBR as STATE_NBR,
exp_pass_through.SERVICE_CENTER as SERVICE_CENTER,
exp_pass_through.ACCOUNTING_DT as ACCOUNTING_DT,
exp_pass_through.CMPY_ABBREV as CMPY_ABBREV,
exp_pass_through.POLICY_SYMBOL as POLICY_SYMBOL,
exp_pass_through.FIELD_TYPE as FIELD_TYPE,
exp_pass_through.AMOUNT as AMOUNT,
exp_pass_through.USER_ID as USER_ID,
exp_pass_through.prcs_id as PRCS_ID
FROM
exp_pass_through;


END; ';