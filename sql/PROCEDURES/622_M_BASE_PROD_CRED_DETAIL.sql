-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PROD_CRED_DETAIL("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_prod_cred_Detail, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prod_cred_Detail AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as POLICY_SYMBOL,
$3 as POLICY_NBR,
$4 as UNIT_NBR,
$5 as EOM_DATE,
$6 as SEQ_NBR,
$7 as LINE_ID,
$8 as STATE,
$9 as REGION,
$10 as DISTRICT,
$11 as COUNTY,
$12 as SVC_CNTR,
$13 as ORIG_REGION,
$14 as ORIG_DISTRICT,
$15 as ORIG_SVC_CNTR,
$16 as APP_CREDIT,
$17 as PREMIUM,
$18 as UPDATE_SOURCE,
$19 as UPDATE_TYPE,
$20 as UPDATE_DATE,
$21 as PAY_AGENT,
$22 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
prod_cred_Detail.AGENT_NBR,
prod_cred_Detail.POLICY_SYMBOL,
prod_cred_Detail.POLICY_NBR,
prod_cred_Detail.UNIT_NBR,
prod_cred_Detail.EOM_DATE,
prod_cred_Detail.SEQ_NBR,
prod_cred_Detail.LINE_ID,
prod_cred_Detail.STATE,
prod_cred_Detail.REGION,
prod_cred_Detail.DISTRICT,
prod_cred_Detail.COUNTY,
prod_cred_Detail.SVC_CNTR,
prod_cred_Detail.ORIG_REGION,
prod_cred_Detail.ORIG_DISTRICT,
prod_cred_Detail.ORIG_SVC_CNTR,
prod_cred_Detail.APP_CREDIT,
prod_cred_Detail.PREMIUM,
prod_cred_Detail.UPDATE_SOURCE,
prod_cred_Detail.UPDATE_TYPE,
prod_cred_Detail.UPDATE_DATE,
prod_cred_Detail.PAY_AGENT
FROM db_t_prod_stag.prod_cred_Detail
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_prod_cred_Detail.AGENT_NBR as AGENT_NBR,
SQ_prod_cred_Detail.POLICY_SYMBOL as POLICY_SYMBOL,
SQ_prod_cred_Detail.POLICY_NBR as POLICY_NBR,
SQ_prod_cred_Detail.UNIT_NBR as UNIT_NBR,
SQ_prod_cred_Detail.EOM_DATE as EOM_DATE,
SQ_prod_cred_Detail.SEQ_NBR as SEQ_NBR,
SQ_prod_cred_Detail.LINE_ID as LINE_ID,
SQ_prod_cred_Detail.STATE as STATE,
SQ_prod_cred_Detail.REGION as REGION,
SQ_prod_cred_Detail.DISTRICT as DISTRICT,
SQ_prod_cred_Detail.COUNTY as COUNTY,
SQ_prod_cred_Detail.SVC_CNTR as SVC_CNTR,
SQ_prod_cred_Detail.ORIG_REGION as ORIG_REGION,
SQ_prod_cred_Detail.ORIG_DISTRICT as ORIG_DISTRICT,
SQ_prod_cred_Detail.ORIG_SVC_CNTR as ORIG_SVC_CNTR,
SQ_prod_cred_Detail.APP_CREDIT as APP_CREDIT,
SQ_prod_cred_Detail.PREMIUM as PREMIUM,
SQ_prod_cred_Detail.UPDATE_SOURCE as UPDATE_SOURCE,
SQ_prod_cred_Detail.UPDATE_TYPE as UPDATE_TYPE,
SQ_prod_cred_Detail.UPDATE_DATE as UPDATE_DATE,
SQ_prod_cred_Detail.PAY_AGENT as PAY_AGENT,
:PRCS_ID as prcs_id,
SQ_prod_cred_Detail.source_record_id
FROM
SQ_prod_cred_Detail
);


-- Component PROD_CRED_DETAIL1, Type TARGET 
INSERT INTO db_t_prod_comn.PROD_CRED_DETAIL
(
AGENT_NBR,
POLICY_SYMBOL,
POLICY_NBR,
UNIT_NBR,
EOM_DATE,
SEQ_NBR,
LINE_ID,
STATE,
REGION,
DISTRICT,
COUNTY,
SVC_CNTR,
ORIG_REGION,
ORIG_DISTRICT,
ORIG_SVC_CNTR,
APP_CREDIT,
PREMIUM,
UPDATE_SOURCE,
UPDATE_TYPE,
UPDATE_DATE,
PAY_AGENT,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.POLICY_SYMBOL as POLICY_SYMBOL,
exp_pass_through.POLICY_NBR as POLICY_NBR,
exp_pass_through.UNIT_NBR as UNIT_NBR,
exp_pass_through.EOM_DATE as EOM_DATE,
exp_pass_through.SEQ_NBR as SEQ_NBR,
exp_pass_through.LINE_ID as LINE_ID,
exp_pass_through.STATE as STATE,
exp_pass_through.REGION as REGION,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.COUNTY as COUNTY,
exp_pass_through.SVC_CNTR as SVC_CNTR,
exp_pass_through.ORIG_REGION as ORIG_REGION,
exp_pass_through.ORIG_DISTRICT as ORIG_DISTRICT,
exp_pass_through.ORIG_SVC_CNTR as ORIG_SVC_CNTR,
exp_pass_through.APP_CREDIT as APP_CREDIT,
exp_pass_through.PREMIUM as PREMIUM,
exp_pass_through.UPDATE_SOURCE as UPDATE_SOURCE,
exp_pass_through.UPDATE_TYPE as UPDATE_TYPE,
exp_pass_through.UPDATE_DATE as UPDATE_DATE,
exp_pass_through.PAY_AGENT as PAY_AGENT,
exp_pass_through.prcs_id as PRCS_ID
FROM
exp_pass_through;


END; ';