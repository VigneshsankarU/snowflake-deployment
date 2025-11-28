-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_RECON_STG_GW_EDW_MTRC_THRSHLD_TO_FILE_OUTPUT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '


DECLARE

  run_id STRING;
  prcs_id int;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
 

-- Component SQ_RECON_MTRC, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_RECON_MTRC AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MTRC,
$2 as FRQCY,
$3 as VALUE_TYPE,
$4 as RECON_MTRC_LAYER,
$5 as SOURCE_MTRC_VAL,
$6 as TARGET_MTRC_VAL,
$7 as DIFF_MTRC_VAL,
$8 as THRSH_VAL,
$9 as THRSH_VAL_CALC_MTHD,
$10 as DIFF_MTRC_VAL_OVR_THRSH,
$11 as PRIOR_DIFF_MTRC_VAL,
$12 as PCT_CHG_DIFF_MTRC_VAL,
$13 as CMPRSN_DT,
$14 as MTRC_ORIGN,
$15 as MTRC_DESC,
$16 as LOAD_DTTM,
$17 as PRCS_ID,
$18 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT MTRC,

FRQCY,

VALUE_TYPE,

RECON_MTRC_LAYER, 

SOURCE_MTRC_VAL,

TARGET_MTRC_VAL,

DIFF_MTRC_VAL,

THRSH_VAL, 

THRSH_VAL_CALC_MTHD,

DIFF_MTRC_VAL_OVR_THRSH, 

PRIOR_DIFF_MTRC_VAL,

PCT_CHG_DIFF_MTRC_VAL,

CMPRSN_DT,

MTRC_ORIGN, 

MTRC_DESC,

LOAD_DTTM,

PRCS_ID

FROM  DB_T_PROD_CORE.RECON_MTRC WHERE RECON_MTRC_LAYER LIKE ''STG_GW%'' AND FRQCY=''DLY'' AND CAST(LOAD_DTTM AS DATE) = CURRENT_DATE

AND CAST(DIFF_MTRC_VAL_OVR_THRSH AS DECIMAL(18,4)) > 0.0000

QUALIFY ROW_NUMBER() OVER (PARTITION BY MTRC,RECON_MTRC_LAYER,FRQCY,VALUE_TYPE ORDER BY CMPRSN_DT DESC)=1
) SRC
)
);


-- Component exp_pass_thru_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_thru_data AS
(
SELECT
SQ_RECON_MTRC.MTRC as MTRC,
SQ_RECON_MTRC.FRQCY as FRQCY,
SQ_RECON_MTRC.VALUE_TYPE as VALUE_TYPE,
SQ_RECON_MTRC.RECON_MTRC_LAYER as RECON_MTRC_LAYER,
SQ_RECON_MTRC.SOURCE_MTRC_VAL as SOURCE_MTRC_VAL,
SQ_RECON_MTRC.TARGET_MTRC_VAL as TARGET_MTRC_VAL,
SQ_RECON_MTRC.DIFF_MTRC_VAL as DIFF_MTRC_VAL,
SQ_RECON_MTRC.THRSH_VAL as THRSH_VAL,
SQ_RECON_MTRC.THRSH_VAL_CALC_MTHD as THRSH_VAL_CALC_MTHD,
SQ_RECON_MTRC.DIFF_MTRC_VAL_OVR_THRSH as DIFF_MTRC_VAL_OVR_THRSH,
SQ_RECON_MTRC.PRIOR_DIFF_MTRC_VAL as PRIOR_DIFF_MTRC_VAL,
SQ_RECON_MTRC.PCT_CHG_DIFF_MTRC_VAL as PCT_CHG_DIFF_MTRC_VAL,
SQ_RECON_MTRC.CMPRSN_DT as CMPRSN_DT,
SQ_RECON_MTRC.MTRC_ORIGN as MTRC_ORIGN,
SQ_RECON_MTRC.MTRC_DESC as MTRC_DESC,
SQ_RECON_MTRC.LOAD_DTTM as LOAD_DTTM,
SQ_RECON_MTRC.PRCS_ID as PRCS_ID,
SQ_RECON_MTRC.source_record_id
FROM
SQ_RECON_MTRC
);


-- Component RECON_MTRC_OUTPUT_FILE_STG_GW_EDW_THRESH, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE RECON_MTRC_OUTPUT_FILE_STG_GW_EDW_THRESH AS
(
SELECT
exp_pass_thru_data.MTRC as MTRC,
exp_pass_thru_data.FRQCY as FRQCY,
exp_pass_thru_data.VALUE_TYPE as VALUE_TYPE,
exp_pass_thru_data.RECON_MTRC_LAYER as RECON_MTRC_LAYER,
exp_pass_thru_data.SOURCE_MTRC_VAL as SOURCE_MTRC_VAL,
exp_pass_thru_data.TARGET_MTRC_VAL as TARGET_MTRC_VAL,
exp_pass_thru_data.DIFF_MTRC_VAL as DIFF_MTRC_VAL,
exp_pass_thru_data.THRSH_VAL as THRSH_VAL,
exp_pass_thru_data.THRSH_VAL_CALC_MTHD as THRSH_VAL_CALC_MTHD,
exp_pass_thru_data.DIFF_MTRC_VAL_OVR_THRSH as DIFF_MTRC_VAL_OVR_THRSH,
exp_pass_thru_data.PRIOR_DIFF_MTRC_VAL as PRIOR_DIFF_MTRC_VAL,
exp_pass_thru_data.PCT_CHG_DIFF_MTRC_VAL as PCT_CHG_DIFF_MTRC_VAL,
exp_pass_thru_data.CMPRSN_DT as CMPRSN_DT,
exp_pass_thru_data.MTRC_ORIGN as MTRC_ORIGN,
exp_pass_thru_data.MTRC_DESC as MTRC_DESC,
exp_pass_thru_data.LOAD_DTTM as LOAD_DTTM,
exp_pass_thru_data.PRCS_ID as PRCS_ID
FROM
exp_pass_thru_data
);


-- Component RECON_MTRC_OUTPUT_FILE_STG_GW_EDW_THRESH, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/recon_mtrc_output_file_stg_gw_edw_thresh.txt
from 
(select *
from RECON_MTRC_OUTPUT_FILE_STG_GW_EDW_THRESH)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;

END; 
';