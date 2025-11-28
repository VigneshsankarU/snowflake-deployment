-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_RECON_FRAUD_EXTRACT("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_RECON_EXTC_MTRC_INT2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_RECON_EXTC_MTRC_INT2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MTRC,
$2 as VALUE_TYPE,
$3 as CMPRSN_DT,
$4 as SOURCE_CNT,
$5 as TARGET_CNT,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  MTRC,

			 VALUE_TYPE,

			CMPRSN_DT,

			 SUM(CASE WHEN LAYER=''FRAUD-EDW'' THEN MTRC_VAL ELSE 0 END) AS  SOURCE_CNT,

			 SUM(CASE WHEN LAYER=''FRAUD'' THEN MTRC_VAL ELSE  0 END) AS TARGET_CNT

FROM

db_t_prod_core.RECON_EXTC_MTRC_INT

WHERE LAYER IN (''FRAUD'' ,''FRAUD-EDW'')

and CMPRSN_DT = (select max(CMPRSN_DT) 
from db_t_prod_core.RECON_EXTC_MTRC_INT

WHERE LAYER IN (''FRAUD'' ,''FRAUD-EDW''))

GROUP BY 

MTRC,VALUE_TYPE,CMPRSN_DT
) SRC
)
);


-- Component exp_RECON_MTRC1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_RECON_MTRC1 AS
(
SELECT
SQ_RECON_EXTC_MTRC_INT2.MTRC as MTRC,
SQ_RECON_EXTC_MTRC_INT2.VALUE_TYPE as ValueType,
SQ_RECON_EXTC_MTRC_INT2.CMPRSN_DT as CMPRSN_DT,
SQ_RECON_EXTC_MTRC_INT2.SOURCE_CNT as SRC_MetricValue,
SQ_RECON_EXTC_MTRC_INT2.TARGET_CNT as TARGET_MTRC_VAL,
TO_CHAR ( IFNULL(TRY_TO_DECIMAL(SQ_RECON_EXTC_MTRC_INT2.SOURCE_CNT), 0) - IFNULL(TRY_TO_DECIMAL(SQ_RECON_EXTC_MTRC_INT2.TARGET_CNT), 0) ) as DIFF_MTRC_VALUE,
CURRENT_TIMESTAMP as LoadDate,
:PRCS_ID as Process_ID,
SQ_RECON_EXTC_MTRC_INT2.source_record_id
FROM
SQ_RECON_EXTC_MTRC_INT2
);


-- Component RECON_MTRC1, Type TARGET 
INSERT INTO db_t_prod_core.RECON_MTRC
(
MTRC,
VALUE_TYPE,
SOURCE_MTRC_VAL,
TARGET_MTRC_VAL,
DIFF_MTRC_VAL,
CMPRSN_DT,
LOAD_DTTM,
PRCS_ID
)
SELECT
exp_RECON_MTRC1.MTRC as MTRC,
exp_RECON_MTRC1.ValueType as VALUE_TYPE,
exp_RECON_MTRC1.SRC_MetricValue as SOURCE_MTRC_VAL,
exp_RECON_MTRC1.TARGET_MTRC_VAL as TARGET_MTRC_VAL,
exp_RECON_MTRC1.DIFF_MTRC_VALUE as DIFF_MTRC_VAL,
exp_RECON_MTRC1.CMPRSN_DT as CMPRSN_DT,
exp_RECON_MTRC1.LoadDate as LOAD_DTTM,
exp_RECON_MTRC1.Process_ID as PRCS_ID
FROM
exp_RECON_MTRC1;


END; 
';