-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_JOB_SUCCESS_CMPST_MTHLY("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  PRCS_ID STRING;
  SRC_CNT STRING;
  SRC_NM STRING;
  SUB_PRCS_NM STRING;
  TGT_CNT STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  SRC_CNT := public.func_get_scoped_param(:run_id, ''src_cnt'', :workflow_name, :worklet_name, :session_name);
  SRC_NM := public.func_get_scoped_param(:run_id, ''src_nm'', :workflow_name, :worklet_name, :session_name);
  SUB_PRCS_NM := public.func_get_scoped_param(:run_id, ''sub_prcs_nm'', :workflow_name, :worklet_name, :session_name);
  TGT_CNT := public.func_get_scoped_param(:run_id, ''tgt_cnt'', :workflow_name, :worklet_name, :session_name);
 

-- Component src_edw_gen_seq, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_edw_gen_seq AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRCS_STAG,
$2 as PRCS_STS,
$3 as src_extract_date,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT ''COMPSIT'' as  PRCS_STAG, ''SUCCEEDED'' as PRCS_STS 

,Max(SRC_EXTRACT_DT::date) AS SRC_EXTRACT_DT

from DB_T_PROD_CORE.ETL_PRCS_CTRL  

where  PRCS_STAG = ''COMPSIT''

and PRCS_STS=''SUCCEEDED''

and SUB_PRCS_NM= '':SUB_PRCS_NM''





/* FROM DB_SP_PROD.EDW_SEQ_GEN where TGL_TBLNAME=''BASE_PRCS_ID'' */
) SRC
)
);


-- Component exp_prcs_id_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_id_upd AS
(
SELECT
:PRCS_ID as PRCS_ID,
:SUB_PRCS_NM as SUB_PRCS_NM,
:SRC_CNT as SRC_CNT,
:TGT_CNT as TGT_CNT,
last_day ( DATEADD(''month'', + 1, to_date ( TO_CHAR ( src_edw_gen_seq.src_extract_date , ''yyyy-mm-dd'' ) , ''yyyy-mm-dd'' )) ) as V_SRC_EXTRACT_DT,
V_SRC_EXTRACT_DT as SRC_EXTRACT_DT,
:SRC_NM as SRC_NM,
CURRENT_TIMESTAMP as END_DT,
src_edw_gen_seq.PRCS_STAG as PRCS_STAG,
src_edw_gen_seq.PRCS_STS as PRCS_STS,
src_edw_gen_seq.source_record_id
FROM
src_edw_gen_seq
);


-- Component upd_stg_prcs_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_prcs_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_prcs_id_upd.PRCS_ID as PRCS_ID,
exp_prcs_id_upd.END_DT as END_DT,
exp_prcs_id_upd.PRCS_STS as PRCS_STS,
exp_prcs_id_upd.SUB_PRCS_NM as SUB_PRCS_NM,
exp_prcs_id_upd.SRC_CNT as SRC_CNT,
exp_prcs_id_upd.TGT_CNT as TGT_CNT,
exp_prcs_id_upd.PRCS_STAG as PRCS_STAG,
exp_prcs_id_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
exp_prcs_id_upd.SRC_NM as SRC_NM,
1 as UPDATE_STRATEGY_ACTION,
exp_prcs_id_upd.source_record_id as source_record_id
FROM
exp_prcs_id_upd
);


-- Component exp_prcs_tgt_pass_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_tgt_pass_upd AS
(
SELECT
upd_stg_prcs_upd.PRCS_ID as PRCS_ID,
upd_stg_prcs_upd.END_DT as END_DT,
upd_stg_prcs_upd.PRCS_STS as PRCS_STS,
upd_stg_prcs_upd.SUB_PRCS_NM as SUB_PRCS_NM,
upd_stg_prcs_upd.SRC_CNT as SRC_CNT,
upd_stg_prcs_upd.TGT_CNT as TGT_CNT,
upd_stg_prcs_upd.PRCS_STAG as PRCS_STAG,
upd_stg_prcs_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
upd_stg_prcs_upd.SRC_NM as SRC_NM,
upd_stg_prcs_upd.source_record_id
FROM
upd_stg_prcs_upd
);


-- Component etl_prcs_ctrl_upd_1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.ETL_PRCS_CTRL
USING exp_prcs_tgt_pass_upd ON (ETL_PRCS_CTRL.PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID AND ETL_PRCS_CTRL.PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG)
WHEN MATCHED THEN UPDATE
SET
PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID,
PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG,
SRC_NM = exp_prcs_tgt_pass_upd.SRC_NM,
SUB_PRCS_NM = exp_prcs_tgt_pass_upd.SUB_PRCS_NM,
JOB_END_DT = exp_prcs_tgt_pass_upd.END_DT,
SRC_EXTRACT_DT = exp_prcs_tgt_pass_upd.SRC_EXTRACT_DT,
PRCS_STS = exp_prcs_tgt_pass_upd.PRCS_STS,
SRC_CNT = exp_prcs_tgt_pass_upd.SRC_CNT,
TGT_CNT = exp_prcs_tgt_pass_upd.TGT_CNT;


END; ';