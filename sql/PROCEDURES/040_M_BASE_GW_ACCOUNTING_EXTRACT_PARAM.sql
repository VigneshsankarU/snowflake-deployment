-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_GW_ACCOUNTING_EXTRACT_PARAM("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_GW_CLOSEOUT_CTL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GW_CLOSEOUT_CTL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CURRGLMONTH,
$2 as PREVGLMONTH,
$3 as CURRGLYEAR,
$4 as PREVGLYEAR,
$5 as P_EOQ,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT EXTRACT(MONTH,P_EOQ) AS CURRGLMONTH

,EXTRACT(MONTH,P_EOQ) AS PREVGLMONTH

,EXTRACT(YEAR,P_EOQ) AS CURRGLYEAR

,EXTRACT(YEAR,P_EOQ)-1 AS PREVGLYEAR

,LAST_DAY(CAST(P_EOQ AS DATE)) AS P_EOQ

FROM(

SELECT 

MAX(CASE WHEN CLOSEOUT_TYPE=''P'' AND 

ACCOUNTING_YR=CASE WHEN EXTRACT(MONTH,CAST($CURRENT_DATE AS DATE)) IN (0,1,2,3) THEN  EXTRACT(YEAR,CAST($CURRENT_DATE AS DATE))-1 ELSE EXTRACT(YEAR,CAST($CURRENT_DATE AS DATE)) END

AND ACCOUNTING_MO = (CASE WHEN EXTRACT(MONTH,CAST($CURRENT_DATE AS DATE)) IN (0,1,2,3) THEN 12

WHEN EXTRACT(MONTH,CAST($CURRENT_DATE AS DATE)) IN (4,5,6) THEN 3

WHEN EXTRACT(MONTH,CAST($CURRENT_DATE AS DATE)) IN (7,8,9) THEN 6

WHEN EXTRACT(MONTH,CAST($CURRENT_DATE AS DATE)) IN (10,11,12) THEN 9 END) THEN ENDING_TS END) AS P_EOQ

FROM DB_T_PROD_COMN.GW_CLOSEOUT_CTL_DLY 

WHERE CLOSEOUT_TYPE =''P''

AND ACCOUNTING_YR IN (EXTRACT(YEAR,CAST($CURRENT_DATE AS DATE)), EXTRACT(YEAR,CAST($CURRENT_DATE AS DATE))-2, EXTRACT(YEAR,CAST($CURRENT_DATE AS DATE))-1)) AS A
) SRC
)
);


-- Component exp_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data AS
(
SELECT
''[Global]'' as out_Global,
''$CURRGLMONTH='' || CHR ( 39 ) || SQ_GW_CLOSEOUT_CTL.CURRGLMONTH || CHR ( 39 ) as o_CURRGLMONTH,
''$PREVGLMONTH='' || CHR ( 39 ) || SQ_GW_CLOSEOUT_CTL.PREVGLMONTH || CHR ( 39 ) as o_PREVGLMONTH,
''$CURRGLYEAR='' || CHR ( 39 ) || SQ_GW_CLOSEOUT_CTL.CURRGLYEAR || CHR ( 39 ) as o_CURRGLYEAR,
''$PREVGLYEAR='' || CHR ( 39 ) || SQ_GW_CLOSEOUT_CTL.PREVGLYEAR || CHR ( 39 ) as o_PREVGLYEAR,
''$P_EOQ='' || CHR ( 39 ) || SQ_GW_CLOSEOUT_CTL.P_EOQ || CHR ( 39 ) as O_P_EOQ,
SQ_GW_CLOSEOUT_CTL.source_record_id
FROM
SQ_GW_CLOSEOUT_CTL
);


-- Component nrmzr_data, Type NORMALIZER 
/*CREATE OR REPLACE TEMPORARY TABLE nrmzr_data AS
(
SELECT , * FROM
( /* start of inner SQL */
/*SELECT
exp_data.out_Global as out_file_in1,
exp_data.o_CURRGLMONTH as out_file_in2,
exp_data.o_PREVGLMONTH as out_file_in3,
exp_data.o_CURRGLYEAR as out_file_in4,
exp_data.o_PREVGLYEAR as out_file_in5,
exp_data.O_P_EOQ as out_file_in6,
NULL as out_file_in7,
NULL as out_file_in8,
NULL as out_file_in9,
NULL as out_file_in10,
exp_data.source_record_id
FROM
exp_data
--/* end of inner SQL */
/*)
UNPIVOT(out_file) FOR REC_NO IN (out_file_in1 AS REC1, out_file_in2 AS REC2, out_file_in3 AS REC3, out_file_in4 AS REC4, out_file_in5 AS REC5, out_file_in6 AS REC6, out_file_in7 AS REC7, out_file_in8 AS REC8, out_file_in9 AS REC9, out_file_in10 AS REC10) UNPIVOT_TBL
);
*/

CREATE OR REPLACE TEMPORARY TABLE nrmzr_data AS
SELECT *
FROM (
  SELECT
    exp_data.out_Global        AS out_file_in1,
    exp_data.o_CURRGLMONTH     AS out_file_in2,
    exp_data.o_PREVGLMONTH     AS out_file_in3,
    exp_data.o_CURRGLYEAR      AS out_file_in4,
    exp_data.o_PREVGLYEAR      AS out_file_in5,
    exp_data.O_P_EOQ           AS out_file_in6,
    CAST(NULL AS VARCHAR)      AS out_file_in7,
    CAST(NULL AS VARCHAR)      AS out_file_in8,
    CAST(NULL AS VARCHAR)      AS out_file_in9,
    CAST(NULL AS VARCHAR)      AS out_file_in10,
    exp_data.source_record_id
  FROM exp_data
)
UNPIVOT (
  out_file FOR rec_no IN (
    out_file_in1,
    out_file_in2,
    out_file_in3,
    out_file_in4,
    out_file_in5,
    out_file_in6,
    out_file_in7,
    out_file_in8,
    out_file_in9,
    out_file_in10
  )
);




-- Component edw_param_GW_Accounting, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE edw_param_GW_Accounting AS
(
SELECT
nrmzr_data.out_file as out_file
FROM
nrmzr_data
);


-- Component edw_param_GW_Accounting, Type EXPORT_DATA Exporting data
;


END; ';