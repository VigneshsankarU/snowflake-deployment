-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_RECON_FRAUD_EXTRACT_MTRC_TO_FILE_OUTPUT("WORKLET_NAME" VARCHAR)
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
$2 as VALUE_TYPE,
$3 as SOURCE_MTRC_VAL,
$4 as TARGET_MTRC_VAL,
$5 as DIFF_MTRC_VAL,
$6 as CMPRSN_DT,
$7 as LAYER,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

RECON_MTRC.MTRC,

RECON_MTRC.VALUE_TYPE,

RECON_MTRC.SOURCE_MTRC_VAL,

TRIM(RECON_MTRC.TARGET_MTRC_VAL) TARGET_VAL,

RECON_MTRC.DIFF_MTRC_VAL,

RECON_MTRC.CMPRSN_DT,

RECON_MTRC.LAYER

FROM DB_T_PROD_CORE.RECON_MTRC

,(SELECT  MAX(LOAD_DTTM) LOAD_DTTM

FROM DB_T_PROD_CORE.RECON_MTRC

WHERE LAYER LIKE ''%FRAUD%'' 

) AS A

WHERE RECON_MTRC.LOAD_DTTM=A.LOAD_DTTM
) SRC
)
);


-- Component exp_src_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_tgt AS
(
SELECT
SQ_RECON_MTRC.MTRC as MTRC,
SQ_RECON_MTRC.VALUE_TYPE as VALUE_TYPE,
SQ_RECON_MTRC.SOURCE_MTRC_VAL as SOURCE_MTRC_VAL,
SQ_RECON_MTRC.TARGET_MTRC_VAL as TARGET_MTRC_VAL,
SQ_RECON_MTRC.DIFF_MTRC_VAL as DIFF_MTRC_VAL,
SQ_RECON_MTRC.CMPRSN_DT as CMPRSN_DT,
SQ_RECON_MTRC.LAYER as LAYER,
SQ_RECON_MTRC.source_record_id
FROM
SQ_RECON_MTRC
);


-- Component RECON_OUTPUT_FILE, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE RECON_OUTPUT_FILE AS
(
SELECT
exp_src_tgt.MTRC as MTRC,
exp_src_tgt.VALUE_TYPE as VALUE_TYPE,
exp_src_tgt.SOURCE_MTRC_VAL as SOURCE_VAL,
exp_src_tgt.TARGET_MTRC_VAL as TARGET_VAL,
exp_src_tgt.DIFF_MTRC_VAL as DIFFERENCE,
exp_src_tgt.CMPRSN_DT as COMPARISION_DATE,
exp_src_tgt.LAYER as LAYER
FROM
exp_src_tgt
);


-- Component RECON_OUTPUT_FILE, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/recon_output_file.txt
from 
(select *
from RECON_OUTPUT_FILE)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;


END; 
';