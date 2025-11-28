-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TPC00201_PRDCRD_Y("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_tpc00201_prdcrd_y, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_tpc00201_prdcrd_y AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RECORD_ID,
$2 as POLICY,
$3 as POLICY_SFX,
$4 as SOURCE1,
$5 as AGENT,
$6 as INSURED_NAME,
$7 as MODE1,
$8 as FORM,
$9 as PLAN1,
$10 as FAM_BUS,
$11 as ISSUE_AGE,
$12 as APP_CREDIT,
$13 as WC_MM,
$14 as WC_DD,
$15 as WC_YY,
$16 as CWA,
$17 as TOT_ANNL_TGT_PRM,
$18 as TOT_VOLUME,
$19 as PREM_CRED_CHANGE,
$20 as VOL_CRED_CHANGE,
$21 as EOM_MM,
$22 as EOM_DD,
$23 as EOM_YYYY,
$24 as CFO_MM,
$25 as CFO_DD,
$26 as CFO_YYYY,
$27 as STATUS,
$28 as CURR_MM,
$29 as CURR_DD,
$30 as CURR_YYYY,
$31 as DISTRICT,
$32 as REGION,
$33 as SERVICE_CENTER,
$34 as STATUS_NEW,
$35 as COVERAGE,
$36 as LINE_OF_BUS,
$37 as POLICY_TYPE,
$38 as PCE_WC_MM,
$39 as PCE_WC_DD,
$40 as PCE_WC_YY,
$41 as CVG_CONNECT,
$42 as E_APP,
$43 as ASSOC_AGT,
$44 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
tpc00201_prdcrd_y.RECORD_ID,
tpc00201_prdcrd_y.POLICY,
tpc00201_prdcrd_y.POLICY_SFX,
tpc00201_prdcrd_y.SOURCE1,
tpc00201_prdcrd_y.AGENT,
tpc00201_prdcrd_y.INSURED_NAME,
tpc00201_prdcrd_y.MODE1,
tpc00201_prdcrd_y.FORM,
tpc00201_prdcrd_y.PLAN1,
tpc00201_prdcrd_y.FAM_BUS,
tpc00201_prdcrd_y.ISSUE_AGE,
tpc00201_prdcrd_y.APP_CREDIT,
tpc00201_prdcrd_y.WC_MM,
tpc00201_prdcrd_y.WC_DD,
tpc00201_prdcrd_y.WC_YY,
tpc00201_prdcrd_y.CWA,
tpc00201_prdcrd_y.TOT_ANNL_TGT_PRM,
tpc00201_prdcrd_y.TOT_VOLUME,
tpc00201_prdcrd_y.PREM_CRED_CHANGE,
tpc00201_prdcrd_y.VOL_CRED_CHANGE,
tpc00201_prdcrd_y.EOM_MM,
tpc00201_prdcrd_y.EOM_DD,
tpc00201_prdcrd_y.EOM_YYYY,
tpc00201_prdcrd_y.CFO_MM,
tpc00201_prdcrd_y.CFO_DD,
tpc00201_prdcrd_y.CFO_YYYY,
tpc00201_prdcrd_y.STATUS,
tpc00201_prdcrd_y.CURR_MM,
tpc00201_prdcrd_y.CURR_DD,
tpc00201_prdcrd_y.CURR_YYYY,
tpc00201_prdcrd_y.DISTRICT,
tpc00201_prdcrd_y.REGION,
tpc00201_prdcrd_y.SERVICE_CENTER,
tpc00201_prdcrd_y.STATUS_NEW,
tpc00201_prdcrd_y.COVERAGE,
tpc00201_prdcrd_y.LINE_OF_BUS,
tpc00201_prdcrd_y.POLICY_TYPE,
tpc00201_prdcrd_y.PCE_WC_MM,
tpc00201_prdcrd_y.PCE_WC_DD,
tpc00201_prdcrd_y.PCE_WC_YY,
tpc00201_prdcrd_y.CVG_CONNECT,
tpc00201_prdcrd_y.E_APP,
tpc00201_prdcrd_y.ASSOC_AGT
FROM db_t_prod_stag.tpc00201_prdcrd_y
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_tpc00201_prdcrd_y.RECORD_ID as RECORD_ID,
SQ_tpc00201_prdcrd_y.POLICY as POLICY,
SQ_tpc00201_prdcrd_y.POLICY_SFX as POLICY_SFX,
SQ_tpc00201_prdcrd_y.SOURCE1 as SOURCE1,
SQ_tpc00201_prdcrd_y.AGENT as AGENT,
SQ_tpc00201_prdcrd_y.INSURED_NAME as INSURED_NAME,
SQ_tpc00201_prdcrd_y.MODE1 as MODE1,
SQ_tpc00201_prdcrd_y.FORM as FORM,
SQ_tpc00201_prdcrd_y.PLAN1 as PLAN,
SQ_tpc00201_prdcrd_y.FAM_BUS as FAM_BUS,
SQ_tpc00201_prdcrd_y.ISSUE_AGE as ISSUE_AGE,
SQ_tpc00201_prdcrd_y.APP_CREDIT as APP_CREDIT,
SQ_tpc00201_prdcrd_y.WC_MM as WC_MM,
SQ_tpc00201_prdcrd_y.WC_DD as WC_DD,
SQ_tpc00201_prdcrd_y.WC_YY as WC_YY,
SQ_tpc00201_prdcrd_y.CWA as CWA,
SQ_tpc00201_prdcrd_y.TOT_ANNL_TGT_PRM as TOT_ANNL_TGT_PRM,
SQ_tpc00201_prdcrd_y.TOT_VOLUME as TOT_VOLUME,
SQ_tpc00201_prdcrd_y.PREM_CRED_CHANGE as PREM_CRED_CHANGE,
SQ_tpc00201_prdcrd_y.VOL_CRED_CHANGE as VOL_CRED_CHANGE,
SQ_tpc00201_prdcrd_y.EOM_MM as EOM_MM,
SQ_tpc00201_prdcrd_y.EOM_DD as EOM_DD,
SQ_tpc00201_prdcrd_y.EOM_YYYY as EOM_YYYY,
SQ_tpc00201_prdcrd_y.CFO_MM as CFO_MM,
SQ_tpc00201_prdcrd_y.CFO_DD as CFO_DD,
SQ_tpc00201_prdcrd_y.CFO_YYYY as CFO_YYYY,
SQ_tpc00201_prdcrd_y.STATUS as STATUS,
SQ_tpc00201_prdcrd_y.CURR_MM as CURR_MM,
SQ_tpc00201_prdcrd_y.CURR_DD as CURR_DD,
SQ_tpc00201_prdcrd_y.CURR_YYYY as CURR_YYYY,
SQ_tpc00201_prdcrd_y.DISTRICT as DISTRICT,
SQ_tpc00201_prdcrd_y.REGION as REGION,
SQ_tpc00201_prdcrd_y.SERVICE_CENTER as SERVICE_CENTER,
SQ_tpc00201_prdcrd_y.STATUS_NEW as STATUS_NEW,
SQ_tpc00201_prdcrd_y.COVERAGE as COVERAGE,
SQ_tpc00201_prdcrd_y.LINE_OF_BUS as LINE_OF_BUS,
SQ_tpc00201_prdcrd_y.POLICY_TYPE as POLICY_TYPE,
SQ_tpc00201_prdcrd_y.PCE_WC_MM as PCE_WC_MM,
SQ_tpc00201_prdcrd_y.PCE_WC_DD as PCE_WC_DD,
SQ_tpc00201_prdcrd_y.PCE_WC_YY as PCE_WC_YY,
SQ_tpc00201_prdcrd_y.CVG_CONNECT as CVG_CONNECT,
SQ_tpc00201_prdcrd_y.E_APP as E_APP,
SQ_tpc00201_prdcrd_y.ASSOC_AGT as ASSOC_AGT,
:PRCS_ID as PRCS_ID,
SQ_tpc00201_prdcrd_y.source_record_id
FROM
SQ_tpc00201_prdcrd_y
);


-- Component TPC00201_PRDCRD_Y1, Type TARGET 
INSERT INTO db_t_prod_comn.TPC00201_PRDCRD_Y
(
RECORD_ID,
POLICY,
POLICY_SFX,
SOURCE1,
AGENT,
INSURED_NAME,
MODE1,
FORM,
PLAN,
FAM_BUS,
ISSUE_AGE,
APP_CREDIT,
WC_MM,
WC_DD,
WC_YY,
CWA,
TOT_ANNL_TGT_PRM,
TOT_VOLUME,
PREM_CRED_CHANGE,
VOL_CRED_CHANGE,
EOM_MM,
EOM_DD,
EOM_YYYY,
CFO_MM,
CFO_DD,
CFO_YYYY,
STATUS,
CURR_MM,
CURR_DD,
CURR_YYYY,
DISTRICT,
REGION,
SERVICE_CENTER,
STATUS_NEW,
COVERAGE,
LINE_OF_BUS,
POLICY_TYPE,
PCE_WC_MM,
PCE_WC_DD,
PCE_WC_YY,
CVG_CONNECT,
E_APP,
ASSOC_AGT,
PRCS_ID
)
SELECT
exp_pass_through.RECORD_ID as RECORD_ID,
exp_pass_through.POLICY as POLICY,
exp_pass_through.POLICY_SFX as POLICY_SFX,
exp_pass_through.SOURCE1 as SOURCE1,
exp_pass_through.AGENT as AGENT,
exp_pass_through.INSURED_NAME as INSURED_NAME,
exp_pass_through.MODE1 as MODE1,
exp_pass_through.FORM as FORM,
exp_pass_through.PLAN as PLAN,
exp_pass_through.FAM_BUS as FAM_BUS,
exp_pass_through.ISSUE_AGE as ISSUE_AGE,
exp_pass_through.APP_CREDIT as APP_CREDIT,
exp_pass_through.WC_MM as WC_MM,
exp_pass_through.WC_DD as WC_DD,
exp_pass_through.WC_YY as WC_YY,
exp_pass_through.CWA as CWA,
exp_pass_through.TOT_ANNL_TGT_PRM as TOT_ANNL_TGT_PRM,
exp_pass_through.TOT_VOLUME as TOT_VOLUME,
exp_pass_through.PREM_CRED_CHANGE as PREM_CRED_CHANGE,
exp_pass_through.VOL_CRED_CHANGE as VOL_CRED_CHANGE,
exp_pass_through.EOM_MM as EOM_MM,
exp_pass_through.EOM_DD as EOM_DD,
exp_pass_through.EOM_YYYY as EOM_YYYY,
exp_pass_through.CFO_MM as CFO_MM,
exp_pass_through.CFO_DD as CFO_DD,
exp_pass_through.CFO_YYYY as CFO_YYYY,
exp_pass_through.STATUS as STATUS,
exp_pass_through.CURR_MM as CURR_MM,
exp_pass_through.CURR_DD as CURR_DD,
exp_pass_through.CURR_YYYY as CURR_YYYY,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.REGION as REGION,
exp_pass_through.SERVICE_CENTER as SERVICE_CENTER,
exp_pass_through.STATUS_NEW as STATUS_NEW,
exp_pass_through.COVERAGE as COVERAGE,
exp_pass_through.LINE_OF_BUS as LINE_OF_BUS,
exp_pass_through.POLICY_TYPE as POLICY_TYPE,
exp_pass_through.PCE_WC_MM as PCE_WC_MM,
exp_pass_through.PCE_WC_DD as PCE_WC_DD,
exp_pass_through.PCE_WC_YY as PCE_WC_YY,
exp_pass_through.CVG_CONNECT as CVG_CONNECT,
exp_pass_through.E_APP as E_APP,
exp_pass_through.ASSOC_AGT as ASSOC_AGT,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';