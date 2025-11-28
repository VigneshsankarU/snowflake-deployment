-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_FNOL_WH_STAG("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component sq_FNOL_WH_STAG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_FNOL_WH_STAG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Claim_Nbr,
$2 as Date_LOADED,
$3 as Date_Report_Produced,
$4 as matched_cd,
$5 as Claim_date,
$6 as Claim_num2,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	
CLAIM_NBR
, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS DATE_LOADED
, CAST(NULL AS TIMESTAMP(6)) AS DATE_REPORT_PRODUCED
, ''N'' AS MATCHED_CD
,  CAST ((CLAIM_DATE ) AS VARCHAR(10)) AS DOL
, CLAIM_NUM2
FROM DB_T_STAG_MEMBXREF_PROD.FNOL_TEMP
) SRC
)
);


-- Component exp_Pass_Through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Pass_Through AS
(
SELECT
sq_FNOL_WH_STAG.Claim_Nbr as Policy_nbr,
sq_FNOL_WH_STAG.Date_LOADED as Date_LOADED,
sq_FNOL_WH_STAG.Date_Report_Produced as Date_Report_Produced,
sq_FNOL_WH_STAG.matched_cd as matched_cd,
sq_FNOL_WH_STAG.Claim_date as DOL,
sq_FNOL_WH_STAG.Claim_num2 as claim_num2,
sq_FNOL_WH_STAG.source_record_id
FROM
sq_FNOL_WH_STAG
);


-- Component FNOL_WH_STAG, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG
(
Policy_nbr,
Date_LOADED,
Date_Report_Produced,
matched_cd,
DOL,
claim_num2
)
SELECT
exp_Pass_Through.Policy_nbr as Policy_nbr,
exp_Pass_Through.Date_LOADED as Date_LOADED,
exp_Pass_Through.Date_Report_Produced as Date_Report_Produced,
exp_Pass_Through.matched_cd as matched_cd,
exp_Pass_Through.DOL as DOL,
exp_Pass_Through.claim_num2 as claim_num2
FROM
exp_Pass_Through;


END; ';