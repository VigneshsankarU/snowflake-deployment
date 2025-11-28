-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_AGG_AVG_DAYS_TO_PAY_AM("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_STG_FED_MSTR_TRG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_TRG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FEED_DT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CALENDAR_DATE

FROM --DB_SP_PROD.CALENDAR
public.sys_calendar

WHERE 

CALENDAR_DATE > 

(SELECT COALESCE( MAX(AM_DATE), CAST(''1992-01-01'' AS DATE)) FROM DB_V_PROD.AGG_AVG_DAYS_TO_PAY_AM)

AND CALENDAR_DATE <= (SELECT FEED_DT FROM DB_T_STAG_PROD.STG_FED_MSTR_TRG

ORDER BY 1)



/* SELECT CALENDAR_DATE */
/* FROM DB_SP_PROD.calendar */
/* WHERE  */
/* CALENDAR_DATE > CAST(''1992-01-01'' AS DATE) */
/* (SELECT COALESCE( MAX(CAL_DATE), CAST(''1993-01-01'' AS DATE)) --FROM MEMBER_SUMMARY) */
/* AND CALENDAR_DATE <= (SELECT FEED_DT FROM --STG_FED_MSTR_TRG) */
/* ORDER BY 1 */
/* AND CALENDAR_DATE <= ''2009-01-31'' */
) SRC
)
);


-- Component exp_last_date, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_last_date AS
(
SELECT
SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT as FEED_DT,
lpad ( to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 4 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 2 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''DD'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 2 , ''0'' ) as out_DATE,
SQ_Shortcut_to_STG_FED_MSTR_TRG.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_TRG
);


-- Component flt_month_end, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_month_end AS
(
SELECT
exp_last_date.FEED_DT as FEED_DT,
exp_last_date.out_DATE as out_DATE,
exp_last_date.source_record_id
FROM
exp_last_date
WHERE CASE WHEN exp_last_date.FEED_DT = last_day ( exp_last_date.FEED_DT ) THEN TRUE ELSE FALSE END
);

-- Loop to call stored procedure AVG_DAYS_TO_PAY_AM for each out_date from flt_month_end
let flt_month_end_sql resultset := (SELECT * FROM flt_month_end);
let cur_prm_date cursor for flt_month_end_sql;
for rec_prm_date IN cur_prm_date 
do
  call DB_SP_PROD.AVG_DAYS_TO_PAY_AM(rec_prm_date.out_DATE, rec_prm_date.source_record_id);
end for;

-- Component AVG_DAYS_TO_PAY_AM, Type STORED_PROCEDURE 
-- add a loop to use out_date from flt_month_end
-- call ALFA_EDW_DEV.DB_SP_PROD.AVG_DAYS_TO_PAY_AM("PRM_DATE" VARCHAR(10), "STATUS" NUMBER(38,0));
/*
CREATE OR REPLACE TEMPORARY TABLE AVG_DAYS_TO_PAY_AM AS
(
CREATE OR REPLACE TEMPORARY TABLE AVG_DAYS_TO_PAY_AM AS
(
-- WARNING: Stored Procedure node is not supported, manual conversion required. Commented call text below:
-- AVG_DAYS_TO_PAY_AM
)
);
*/

-- Component exp_Check_Status, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Check_Status AS
(
SELECT
--AVG_DAYS_TO_PAY_AM.STATUS as STATUS,
0 as status,
--CASE WHEN AVG_DAYS_TO_PAY_AM.STATUS = ''1'' THEN RAISE_ERROR(''Error while executing stored procedure LOAD_MEMBER_SUMMARY'') ELSE $3 END as out_Abort,
--AVG_DAYS_TO_PAY_AM.source_record_id
source_record_id
FROM
flt_month_end --- AVG_DAYS_TO_PAY_AM
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


-- Component Shortcut_to_STG_FED_MSTR_TRG1, Type TARGET 
INSERT INTO DB_T_STAG_PROD.STG_FED_MSTR_TRG
(
REC_COUNT
)
SELECT
flt_status.STATUS as REC_COUNT
FROM
flt_status;


END; ';