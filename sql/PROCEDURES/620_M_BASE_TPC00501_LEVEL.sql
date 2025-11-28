-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TPC00501_LEVEL("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_tpc00501_level, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_tpc00501_level AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT,
$2 as YEAR1,
$3 as MONTH1,
$4 as NB_PREM,
$5 as NB_COMMPD,
$6 as NB_BONUSPD,
$7 as DISTRICT,
$8 as REGION,
$9 as NB_PREM_SELECT,
$10 as AGENT_TYPE,
$11 as PERSISTENCY,
$12 as NB_PAID_SELECT,
$13 as RENEWAL_PREM,
$14 as RENEWAL_PAID,
$15 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
tpc00501_level.AGENT,
tpc00501_level.YEAR1,
tpc00501_level.MONTH1,
tpc00501_level.NB_PREM,
tpc00501_level.NB_COMMPD,
tpc00501_level.NB_BONUSPD,
tpc00501_level.DISTRICT,
tpc00501_level.REGION,
tpc00501_level.NB_PREM_SELECT,
tpc00501_level.AGENT_TYPE,
tpc00501_level.PERSISTENCY,
tpc00501_level.NB_PAID_SELECT,
tpc00501_level.RENEWAL_PREM,
tpc00501_level.RENEWAL_PAID
FROM db_t_prod_stag.tpc00501_level
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_tpc00501_level.AGENT as AGENT,
SQ_tpc00501_level.YEAR1 as YEAR1,
SQ_tpc00501_level.MONTH1 as MONTH1,
SQ_tpc00501_level.NB_PREM as NB_PREM,
SQ_tpc00501_level.NB_COMMPD as NB_COMMPD,
SQ_tpc00501_level.NB_BONUSPD as NB_BONUSPD,
SQ_tpc00501_level.DISTRICT as DISTRICT,
SQ_tpc00501_level.REGION as REGION,
SQ_tpc00501_level.NB_PREM_SELECT as NB_PREM_SELECT,
SQ_tpc00501_level.AGENT_TYPE as AGENT_TYPE,
SQ_tpc00501_level.PERSISTENCY as PERSISTENCY,
SQ_tpc00501_level.NB_PAID_SELECT as NB_PAID_SELECT,
SQ_tpc00501_level.RENEWAL_PREM as RENEWAL_PREM,
SQ_tpc00501_level.RENEWAL_PAID as RENEWAL_PAID,
:prcs_id as prcs_id,
SQ_tpc00501_level.source_record_id
FROM
SQ_tpc00501_level
);


-- Component TPC00501_LEVEL1, Type TARGET 
INSERT INTO db_t_prod_comn.TPC00501_LEVEL
(
AGENT,
YEAR1,
MONTH1,
NB_PREM,
NB_COMMPD,
NB_BONUSPD,
DISTRICT,
REGION,
NB_PREM_SELECT,
AGENT_TYPE,
PERSISTENCY,
NB_PAID_SELECT,
RENEWAL_PREM,
RENEWAL_PAID,
PRCS_ID
)
SELECT
exp_pass_through.AGENT as AGENT,
exp_pass_through.YEAR1 as YEAR1,
exp_pass_through.MONTH1 as MONTH1,
exp_pass_through.NB_PREM as NB_PREM,
exp_pass_through.NB_COMMPD as NB_COMMPD,
exp_pass_through.NB_BONUSPD as NB_BONUSPD,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.REGION as REGION,
exp_pass_through.NB_PREM_SELECT as NB_PREM_SELECT,
exp_pass_through.AGENT_TYPE as AGENT_TYPE,
exp_pass_through.PERSISTENCY as PERSISTENCY,
exp_pass_through.NB_PAID_SELECT as NB_PAID_SELECT,
exp_pass_through.RENEWAL_PREM as RENEWAL_PREM,
exp_pass_through.RENEWAL_PAID as RENEWAL_PAID,
exp_pass_through.prcs_id as PRCS_ID
FROM
exp_pass_through;


END; ';