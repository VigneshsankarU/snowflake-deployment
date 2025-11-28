-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_MEMBER_SUMMARY2025("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_ETL_LOAD_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_ETL_LOAD_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as START_DTTM,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CALENDAR_DATE
FROM DB_SP_PROD.calendar
WHERE CALENDAR_DATE > (SELECT COALESCE( MAX(CAL_DATE), CAST(''1992-12-31'' AS DATE)) FROM EVIEWDB_LGCY.MEMBER_SUMMARY)
AND CALENDAR_DATE <= (CURRENT_DATE-1)
ORDER BY 1
/* AND CALENDAR_DATE <= ''2009-01-31'' */
) SRC
)
);


-- Component exp_convert_date, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_convert_date AS
(
SELECT
lpad ( to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_Shortcut_to_ETL_LOAD_CTRL.START_DTTM)) ) , 4 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_Shortcut_to_ETL_LOAD_CTRL.START_DTTM)) ) , 2 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''DD'', TO_TIMESTAMP(SQ_Shortcut_to_ETL_LOAD_CTRL.START_DTTM)) ) , 2 , ''0'' ) as out_DATE,
SQ_Shortcut_to_ETL_LOAD_CTRL.source_record_id
FROM
SQ_Shortcut_to_ETL_LOAD_CTRL
);


-- Component LOAD_MEMBER_SUMMARY, Type STORED_PROCEDURE 
--CREATE OR REPLACE TEMPORARY TABLE LOAD_MEMBER_SUMMARY AS
--(
--CREATE OR REPLACE TEMPORARY TABLE LOAD_MEMBER_SUMMARY AS
--(
-- WARNING: Stored Procedure node is not supported, manual conversion required. Commented call text below:
-- LOAD_MEMBER_SUMMARY
--)
--);


-- Component exp_Check_Status, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Check_Status AS
(
SELECT
LOAD_MEMBER_SUMMARY.STATUS as STATUS,
CASE WHEN LOAD_MEMBER_SUMMARY.STATUS = ''1'' THEN RAISE_ERROR(''Error while executing stored procedure LOAD_MEMBER_SUMMARY'') ELSE $3 END as out_Abort,
LOAD_MEMBER_SUMMARY.source_record_id
FROM
LOAD_MEMBER_SUMMARY
);


-- Component flt_status, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_status AS
(
SELECT
exp_Check_Status.STATUS as STATUS,
exp_Check_Status.source_record_id
FROM
exp_Check_Status
WHERE CASE WHEN exp_Check_Status.STATUS = ''X'' THEN TRUE ELSE FALSE END
);


-- Component Shortcut_to_ETL_LOAD_CTRL1, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL
(
PRCS_NM
)
SELECT
flt_status.STATUS as PRCS_NM
FROM
flt_status;


END; ';