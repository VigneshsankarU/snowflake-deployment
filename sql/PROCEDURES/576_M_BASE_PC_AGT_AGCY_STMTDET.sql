-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PC_AGT_AGCY_STMTDET("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_PC_AGT_AGCY_STMTDET, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PC_AGT_AGCY_STMTDET AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as ACCOUNTING_DT,
$3 as POLICYHOLDER,
$4 as CARRIER,
$5 as SEQ_NBR,
$6 as AMOUNT,
$7 as CMPY_ABBREV,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
PC_AGT_AGCY_STMTDET.AGENT_NBR,
PC_AGT_AGCY_STMTDET.ACCOUNTING_DT,
PC_AGT_AGCY_STMTDET.POLICYHOLDER,
PC_AGT_AGCY_STMTDET.CARRIER,
PC_AGT_AGCY_STMTDET.SEQ_NBR,
PC_AGT_AGCY_STMTDET.AMOUNT,
PC_AGT_AGCY_STMTDET.CMPY_ABBREV
FROM db_t_prod_stag.PC_AGT_AGCY_STMTDET
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PC_AGT_AGCY_STMTDET.AGENT_NBR as AGENT_NBR,
SQ_PC_AGT_AGCY_STMTDET.ACCOUNTING_DT as ACCOUNTING_DT,
SQ_PC_AGT_AGCY_STMTDET.POLICYHOLDER as POLICYHOLDER,
SQ_PC_AGT_AGCY_STMTDET.CARRIER as CARRIER,
SQ_PC_AGT_AGCY_STMTDET.SEQ_NBR as SEQ_NBR,
SQ_PC_AGT_AGCY_STMTDET.AMOUNT as AMOUNT,
SQ_PC_AGT_AGCY_STMTDET.CMPY_ABBREV as CMPY_ABBREV,
:PRCS_ID as PRCS_ID,
SQ_PC_AGT_AGCY_STMTDET.source_record_id
FROM
SQ_PC_AGT_AGCY_STMTDET
);


-- Component PC_AGT_AGCY_STMTDET1, Type TARGET 
INSERT INTO db_t_prod_comn.PC_AGT_AGCY_STMTDET
(
AGENT_NBR,
ACCOUNTING_DT,
POLICYHOLDER,
CARRIER,
SEQ_NBR,
AMOUNT,
CMPY_ABBREV,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.ACCOUNTING_DT as ACCOUNTING_DT,
exp_pass_through.POLICYHOLDER as POLICYHOLDER,
exp_pass_through.CARRIER as CARRIER,
exp_pass_through.SEQ_NBR as SEQ_NBR,
exp_pass_through.AMOUNT as AMOUNT,
exp_pass_through.CMPY_ABBREV as CMPY_ABBREV,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';