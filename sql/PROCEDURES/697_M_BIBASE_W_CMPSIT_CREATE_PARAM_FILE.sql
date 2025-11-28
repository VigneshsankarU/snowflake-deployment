-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BIBASE_W_CMPSIT_CREATE_PARAM_FILE("WORKLET_NAME" VARCHAR)
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


-- Component SQ_ETL_PRCS_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ETL_PRCS_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as cal_start_mth_id,
$2 as cal_end_mth_id,
$3 as cal_start_dt,
$4 as cal_end_dt,
$5 as load_start_dt,
$6 as load_end_dt,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

max(accounting_yr*100+accounting_mo) as cal_start_mth_id,

max(accounting_yr*100+accounting_mo) as cal_end_mth_id,

MAX(TO_DATE(CAST(accounting_yr * 100 + accounting_mo AS STRING), ''YYYYMM'')) AS cal_start_dt,

MAX(
  DATEADD(
    DAY, -1,
    DATEADD(
      MONTH, 1,
      TO_DATE(CAST(accounting_yr * 100 + accounting_mo AS STRING), ''YYYYMM'')
    )
  )
) AS cal_end_dt,

  MAX(
    CASE 
      WHEN CAST(from_date AS DATE) <> DATE_TRUNC(''MONTH'', CAST(from_date AS DATE))
      THEN DATEADD(DAY, 1, CAST(from_date AS DATE))
      ELSE CAST(from_date AS DATE)
    END
  ) AS load_start_dt,

max(cast(to_date as date)) as load_end_dt 

from DB_T_CTRL_PROD.ectl_misprem_header
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sessionmerge,
''$CAL_START_MTH_ID='' || LTRIM ( RTRIM ( TO_CHAR ( SQ_ETL_PRCS_CTRL.cal_start_mth_id ) ) ) as out_cal_start_mth_id,
''$CAL_END_MTH_ID='' || LTRIM ( RTRIM ( TO_CHAR ( SQ_ETL_PRCS_CTRL.cal_end_mth_id ) ) ) as out_cal_end_mth_id,
''$CAL_START_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.cal_start_dt , ''YYYY-MM-DD'' ) as out_cal_start_dt,
''$CAL_END_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.cal_end_dt , ''YYYY-MM-DD'' ) as out_cal_end_dt,
''$LOAD_START_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.load_start_dt , ''YYYY-MM-DD'' ) as out_load_start_dt,
''$LOAD_END_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.load_end_dt , ''YYYY-MM-DD'' ) as out_load_end_dt,
SQ_ETL_PRCS_CTRL.cal_end_dt as var_src_extract_dt,
SQ_ETL_PRCS_CTRL.source_record_id
FROM
SQ_ETL_PRCS_CTRL
);


-- Component NRMTRANS, Type NORMALIZER 
/*CREATE OR REPLACE TEMPORARY TABLE NRMTRANS AS
(
SELECT , * FROM
( /* start of inner SQL */
/*SELECT
EXPTRANS.out_global as param_item_in1,
EXPTRANS.out_sessionmerge as param_item_in2,
EXPTRANS.out_cal_start_mth_id as param_item_in3,
EXPTRANS.out_cal_end_mth_id as param_item_in4,
EXPTRANS.out_cal_start_dt as param_item_in5,
EXPTRANS.out_cal_end_dt as param_item_in6,
EXPTRANS.out_load_start_dt as param_item_in7,
EXPTRANS.out_load_end_dt as param_item_in8,
EXPTRANS.source_record_id
FROM
EXPTRANS
/* end of inner SQL */
/*/)
UNPIVOT(param_item) FOR REC_NO IN (param_item_in1 AS REC1, param_item_in2 AS REC2, param_item_in3 AS REC3, param_item_in4 AS REC4, param_item_in5 AS REC5, param_item_in6 AS REC6, param_item_in7 AS REC7, param_item_in8 AS REC8) UNPIVOT_TBL
);
*/

CREATE OR REPLACE TEMPORARY TABLE NRMTRANS AS
SELECT * 
FROM (
    -- Inner SQL
    SELECT
        EXPTRANS.out_global           AS param_item_in1,
        EXPTRANS.out_sessionmerge     AS param_item_in2,
        EXPTRANS.out_cal_start_mth_id AS param_item_in3,
        EXPTRANS.out_cal_end_mth_id   AS param_item_in4,
        EXPTRANS.out_cal_start_dt     AS param_item_in5,
        EXPTRANS.out_cal_end_dt       AS param_item_in6,
        EXPTRANS.out_load_start_dt    AS param_item_in7,
        EXPTRANS.out_load_end_dt      AS param_item_in8,
        EXPTRANS.source_record_id
    FROM EXPTRANS
) src
UNPIVOT(param_item FOR rec_no IN (
    param_item_in1, 
    param_item_in2, 
    param_item_in3, 
    param_item_in4, 
    param_item_in5, 
    param_item_in6, 
    param_item_in7, 
    param_item_in8
)) AS unpvt;



-- Component tgt_edw_shared_append_file, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tgt_edw_shared_append_file AS
(
SELECT
NRMTRANS.param_item as Param_item
FROM
NRMTRANS
);


-- Component tgt_edw_shared_append_file, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/tgt_edw_shared_append_file.txt
from 
(select param_item
from tgt_edw_shared_append_file)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;



END; 
';