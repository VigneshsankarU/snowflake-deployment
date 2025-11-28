-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PREM_SUMM_GRP("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_PREM_SUMM_GRP, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PREM_SUMM_GRP AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as ACCOUNTING_DT,
$3 as LINEBUS_GROUP,
$4 as COMMISSION_TYPE,
$5 as AMOUNT,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
PREM_SUMM_GRP.AGENT_NBR,
PREM_SUMM_GRP.ACCOUNTING_DT,
PREM_SUMM_GRP.LINEBUS_GROUP,
PREM_SUMM_GRP.COMMISSION_TYPE,
PREM_SUMM_GRP.AMOUNT
FROM db_t_prod_stag.PREM_SUMM_GRP
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PREM_SUMM_GRP.AGENT_NBR as AGENT_NBR,
SQ_PREM_SUMM_GRP.ACCOUNTING_DT as ACCOUNTING_DT,
SQ_PREM_SUMM_GRP.LINEBUS_GROUP as LINEBUS_GROUP,
SQ_PREM_SUMM_GRP.COMMISSION_TYPE as COMMISSION_TYPE,
SQ_PREM_SUMM_GRP.AMOUNT as AMOUNT,
:PRCS_ID as PRCS_ID,
SQ_PREM_SUMM_GRP.source_record_id
FROM
SQ_PREM_SUMM_GRP
);


-- Component PREM_SUMM_GRP1, Type TARGET 
INSERT INTO db_t_prod_comn.PREM_SUMM_GRP
(
AGENT_NBR,
ACCOUNTING_DT,
LINEBUS_GROUP,
COMMISSION_TYPE,
AMOUNT,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.ACCOUNTING_DT as ACCOUNTING_DT,
exp_pass_through.LINEBUS_GROUP as LINEBUS_GROUP,
exp_pass_through.COMMISSION_TYPE as COMMISSION_TYPE,
exp_pass_through.AMOUNT as AMOUNT,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';