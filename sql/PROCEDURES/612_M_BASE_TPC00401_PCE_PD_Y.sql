-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TPC00401_PCE_PD_Y("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_tpc00401_pce_pd_y, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_tpc00401_pce_pd_y AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as REC_ID,
$2 as CONTRACT,
$3 as AGENT,
$4 as INSURED_NAME,
$5 as MODE1,
$6 as POLICY_TYPE,
$7 as PLAN,
$8 as FILLER,
$9 as ISS_MM,
$10 as ISS_DAY,
$11 as ISS_YR,
$12 as ISS_AGE,
$13 as ENTRY_CODE,
$14 as EXCESS_CODE,
$15 as CR_MM,
$16 as CR_DAY,
$17 as CR_YR,
$18 as COMM_ANLZD_PREM,
$19 as PAID_VOLUME,
$20 as PD_TO_MM,
$21 as PD_TO_DAY,
$22 as PD_TO_YR,
$23 as PHYS_PD_MM,
$24 as PHYS_PD_DAY,
$25 as PHYS_PD_YR,
$26 as LINE_OF_BS_CD,
$27 as APP_CREDIT,
$28 as PAID_ANLZD_PREM,
$29 as EOM_MM,
$30 as EOM_DD,
$31 as EOM_YYYY,
$32 as STATUS,
$33 as STATUS_NEW,
$34 as COVERAGE,
$35 as CONNECT1,
$36 as PD_CRED_MM2,
$37 as PD_CRED_DD2,
$38 as PD_CRED_YY2,
$39 as DISTRICT,
$40 as REGION,
$41 as SERVICE_CENTER,
$42 as ASSOC_AGT,
$43 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
tpc00401_pce_pd_y.REC_ID,
tpc00401_pce_pd_y.CONTRACT,
tpc00401_pce_pd_y.AGENT,
tpc00401_pce_pd_y.INSURED_NAME,
tpc00401_pce_pd_y.MODE1,
tpc00401_pce_pd_y.POLICY_TYPE,
tpc00401_pce_pd_y.PLAN,
tpc00401_pce_pd_y.FILLER,
tpc00401_pce_pd_y.ISS_MM,
tpc00401_pce_pd_y.ISS_DAY,
tpc00401_pce_pd_y.ISS_YR,
tpc00401_pce_pd_y.ISS_AGE,
tpc00401_pce_pd_y.ENTRY_CODE,
tpc00401_pce_pd_y.EXCESS_CODE,
tpc00401_pce_pd_y.CR_MM,
tpc00401_pce_pd_y.CR_DAY,
tpc00401_pce_pd_y.CR_YR,
tpc00401_pce_pd_y.COMM_ANLZD_PREM,
tpc00401_pce_pd_y.PAID_VOLUME,
tpc00401_pce_pd_y.PD_TO_MM,
tpc00401_pce_pd_y.PD_TO_DAY,
tpc00401_pce_pd_y.PD_TO_YR,
tpc00401_pce_pd_y.PHYS_PD_MM,
tpc00401_pce_pd_y.PHYS_PD_DAY,
tpc00401_pce_pd_y.PHYS_PD_YR,
tpc00401_pce_pd_y.LINE_OF_BS_CD,
tpc00401_pce_pd_y.APP_CREDIT,
tpc00401_pce_pd_y.PAID_ANLZD_PREM,
tpc00401_pce_pd_y.EOM_MM,
tpc00401_pce_pd_y.EOM_DD,
tpc00401_pce_pd_y.EOM_YYYY,
tpc00401_pce_pd_y.STATUS,
tpc00401_pce_pd_y.STATUS_NEW,
tpc00401_pce_pd_y.COVERAGE,
tpc00401_pce_pd_y.CONNECT1,
tpc00401_pce_pd_y.PD_CRED_MM2,
tpc00401_pce_pd_y.PD_CRED_DD2,
tpc00401_pce_pd_y.PD_CRED_YY2,
tpc00401_pce_pd_y.DISTRICT,
tpc00401_pce_pd_y.REGION,
tpc00401_pce_pd_y.SERVICE_CENTER,
tpc00401_pce_pd_y.ASSOC_AGT
FROM db_t_prod_stag.tpc00401_pce_pd_y
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_tpc00401_pce_pd_y.REC_ID as REC_ID,
SQ_tpc00401_pce_pd_y.CONTRACT as CONTRACT,
SQ_tpc00401_pce_pd_y.AGENT as AGENT,
SQ_tpc00401_pce_pd_y.INSURED_NAME as INSURED_NAME,
SQ_tpc00401_pce_pd_y.MODE1 as MODE1,
SQ_tpc00401_pce_pd_y.POLICY_TYPE as POLICY_TYPE,
SQ_tpc00401_pce_pd_y.PLAN as PLAN,
SQ_tpc00401_pce_pd_y.FILLER as FILLER,
SQ_tpc00401_pce_pd_y.ISS_MM as ISS_MM,
SQ_tpc00401_pce_pd_y.ISS_DAY as ISS_DAY,
SQ_tpc00401_pce_pd_y.ISS_YR as ISS_YR,
SQ_tpc00401_pce_pd_y.ISS_AGE as ISS_AGE,
SQ_tpc00401_pce_pd_y.ENTRY_CODE as ENTRY_CODE,
SQ_tpc00401_pce_pd_y.EXCESS_CODE as EXCESS_CODE,
SQ_tpc00401_pce_pd_y.CR_MM as CR_MM,
SQ_tpc00401_pce_pd_y.CR_DAY as CR_DAY,
SQ_tpc00401_pce_pd_y.CR_YR as CR_YR,
SQ_tpc00401_pce_pd_y.COMM_ANLZD_PREM as COMM_ANLZD_PREM,
SQ_tpc00401_pce_pd_y.PAID_VOLUME as PAID_VOLUME,
SQ_tpc00401_pce_pd_y.PD_TO_MM as PD_TO_MM,
SQ_tpc00401_pce_pd_y.PD_TO_DAY as PD_TO_DAY,
SQ_tpc00401_pce_pd_y.PD_TO_YR as PD_TO_YR,
SQ_tpc00401_pce_pd_y.PHYS_PD_MM as PHYS_PD_MM,
SQ_tpc00401_pce_pd_y.PHYS_PD_DAY as PHYS_PD_DAY,
SQ_tpc00401_pce_pd_y.PHYS_PD_YR as PHYS_PD_YR,
SQ_tpc00401_pce_pd_y.LINE_OF_BS_CD as LINE_OF_BS_CD,
SQ_tpc00401_pce_pd_y.APP_CREDIT as APP_CREDIT,
SQ_tpc00401_pce_pd_y.PAID_ANLZD_PREM as PAID_ANLZD_PREM,
SQ_tpc00401_pce_pd_y.EOM_MM as EOM_MM,
SQ_tpc00401_pce_pd_y.EOM_DD as EOM_DD,
SQ_tpc00401_pce_pd_y.EOM_YYYY as EOM_YYYY,
SQ_tpc00401_pce_pd_y.STATUS as STATUS,
SQ_tpc00401_pce_pd_y.STATUS_NEW as STATUS_NEW,
SQ_tpc00401_pce_pd_y.COVERAGE as COVERAGE,
SQ_tpc00401_pce_pd_y.CONNECT1 as CONNECT1,
SQ_tpc00401_pce_pd_y.PD_CRED_MM2 as PD_CRED_MM2,
SQ_tpc00401_pce_pd_y.PD_CRED_DD2 as PD_CRED_DD2,
SQ_tpc00401_pce_pd_y.PD_CRED_YY2 as PD_CRED_YY2,
SQ_tpc00401_pce_pd_y.DISTRICT as DISTRICT,
SQ_tpc00401_pce_pd_y.REGION as REGION,
SQ_tpc00401_pce_pd_y.SERVICE_CENTER as SERVICE_CENTER,
SQ_tpc00401_pce_pd_y.ASSOC_AGT as ASSOC_AGT,
:PRCS_ID as PRCS_ID,
SQ_tpc00401_pce_pd_y.source_record_id
FROM
SQ_tpc00401_pce_pd_y
);


-- Component TPC00401_PCE_PD_Y1, Type TARGET 
INSERT INTO db_t_prod_comn.TPC00401_PCE_PD_Y
(
REC_ID,
CONTRACT,
AGENT,
INSURED_NAME,
MODE1,
POLICY_TYPE,
PLAN,
FILLER,
ISS_MM,
ISS_DAY,
ISS_YR,
ISS_AGE,
ENTRY_CODE,
EXCESS_CODE,
CR_MM,
CR_DAY,
CR_YR,
COMM_ANLZD_PREM,
PAID_VOLUME,
PD_TO_MM,
PD_TO_DAY,
PD_TO_YR,
PHYS_PD_MM,
PHYS_PD_DAY,
PHYS_PD_YR,
LINE_OF_BS_CD,
APP_CREDIT,
PAID_ANLZD_PREM,
EOM_MM,
EOM_DD,
EOM_YYYY,
STATUS,
STATUS_NEW,
COVERAGE,
CONNECT1,
PD_CRED_MM2,
PD_CRED_DD2,
PD_CRED_YY2,
DISTRICT,
REGION,
SERVICE_CENTER,
ASSOC_AGT,
PRCS_ID
)
SELECT
exp_pass_through.REC_ID as REC_ID,
exp_pass_through.CONTRACT as CONTRACT,
exp_pass_through.AGENT as AGENT,
exp_pass_through.INSURED_NAME as INSURED_NAME,
exp_pass_through.MODE1 as MODE1,
exp_pass_through.POLICY_TYPE as POLICY_TYPE,
exp_pass_through.PLAN as PLAN,
exp_pass_through.FILLER as FILLER,
exp_pass_through.ISS_MM as ISS_MM,
exp_pass_through.ISS_DAY as ISS_DAY,
exp_pass_through.ISS_YR as ISS_YR,
exp_pass_through.ISS_AGE as ISS_AGE,
exp_pass_through.ENTRY_CODE as ENTRY_CODE,
exp_pass_through.EXCESS_CODE as EXCESS_CODE,
exp_pass_through.CR_MM as CR_MM,
exp_pass_through.CR_DAY as CR_DAY,
exp_pass_through.CR_YR as CR_YR,
exp_pass_through.COMM_ANLZD_PREM as COMM_ANLZD_PREM,
exp_pass_through.PAID_VOLUME as PAID_VOLUME,
exp_pass_through.PD_TO_MM as PD_TO_MM,
exp_pass_through.PD_TO_DAY as PD_TO_DAY,
exp_pass_through.PD_TO_YR as PD_TO_YR,
exp_pass_through.PHYS_PD_MM as PHYS_PD_MM,
exp_pass_through.PHYS_PD_DAY as PHYS_PD_DAY,
exp_pass_through.PHYS_PD_YR as PHYS_PD_YR,
exp_pass_through.LINE_OF_BS_CD as LINE_OF_BS_CD,
exp_pass_through.APP_CREDIT as APP_CREDIT,
exp_pass_through.PAID_ANLZD_PREM as PAID_ANLZD_PREM,
exp_pass_through.EOM_MM as EOM_MM,
exp_pass_through.EOM_DD as EOM_DD,
exp_pass_through.EOM_YYYY as EOM_YYYY,
exp_pass_through.STATUS as STATUS,
exp_pass_through.STATUS_NEW as STATUS_NEW,
exp_pass_through.COVERAGE as COVERAGE,
exp_pass_through.CONNECT1 as CONNECT1,
exp_pass_through.PD_CRED_MM2 as PD_CRED_MM2,
exp_pass_through.PD_CRED_DD2 as PD_CRED_DD2,
exp_pass_through.PD_CRED_YY2 as PD_CRED_YY2,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.REGION as REGION,
exp_pass_through.SERVICE_CENTER as SERVICE_CENTER,
exp_pass_through.ASSOC_AGT as ASSOC_AGT,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';