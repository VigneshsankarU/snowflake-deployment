-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_MEMBER_SUMMARY("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare 
v_status_cd varchar;
V_PRM_DATE varchar;
 MY_EXCEPTION EXCEPTION(-20002, ''Error while executing stored procedure LOAD_MEMBER_SUMMARY'');
BEGIN 
set v_status_cd:=''2'';
set V_PRM_DATE:=''2025-01-01'';
-- Component SQ_Shortcut_to_STG_FED_MSTR_TRG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_TRG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FEED_DT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CALENDAR_DATE

FROM PUBLIC.SYS_BUSINESSCALENDAR calendar

WHERE CALENDAR_DATE > (SELECT COALESCE( MAX(CAL_DATE), CAST(''1992-12-31'' AS DATE)) FROM DB_T_CORE_PROD.MEMBER_SUMMARY)

AND CALENDAR_DATE <= (SELECT FEED_DT FROM STG_FED_MSTR_TRG)

ORDER BY 1

/* AND CALENDAR_DATE <= ''2009-01-31'' */
) SRC
)
);


-- Component exp_convert_date, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_convert_date AS
(
SELECT
lpad ( to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 4 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 2 , ''0'' ) || ''-'' || lpad ( to_char ( DATE_PART(''DD'', TO_TIMESTAMP(SQ_Shortcut_to_STG_FED_MSTR_TRG.FEED_DT)) ) , 2 , ''0'' ) as out_DATE,
SQ_Shortcut_to_STG_FED_MSTR_TRG.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_TRG
);


-- Component LOAD_MEMBER_SUMMARY, Type STORED_PROCEDURE 
call DB_SP_PROD.LOAD_MEMBER_SUMMARY (:V_PRM_DATE,:v_status_cd);
 CREATE OR REPLACE TEMPORARY TABLE LOAD_MEMBER_SUMMARY AS
 (
 select *, row_number() over (order by 1) AS source_record_id  from DB_T_CORE_PROD.MEMBER_SUMMARY where CAL_DATE=to_date(:V_PRM_DATE)
-- -- WARNING: Stored Procedure node is not supported, manual conversion required. Commented call text below:


);


-- Component exp_Check_Status, Type EXPRESSION 
-- First statement: Create temporary table with the data
CREATE
OR REPLACE TEMPORARY TABLE exp_Check_Status AS
SELECT
  STATUS_CD AS STATUS,
   COUNT(STATUS_CD) as STATUS_CD_count
   --,
 -- source_record_id
FROM
  LOAD_MEMBER_SUMMARY where STATUS_CD=1 group by STATUS_CD;
LET status_count NUMBER := (SELECT STATUS_CD_count                                
FROM exp_Check_Status                                
WHERE STATUS = :v_status_cd);        
IF (status_count > 0) THEN        
RAISE my_exception;    
END IF; 

-- CREATE OR REPLACE TEMPORARY TABLE exp_Check_Status AS
-- (
-- SELECT
--   LOAD_MEMBER_SUMMARY.STATUS_CD AS STATUS,
--   IFF(
--     LOAD_MEMBER_SUMMARY.STATUS_CD = ''1'',
--     SYSTEM$ERROR(
--       ''Error while executing stored procedure LOAD_MEMBER_SUMMARY''
--     ),
--     $3
--   ) AS out_Abort,
--   LOAD_MEMBER_SUMMARY.source_record_id
-- FROM
--   LOAD_MEMBER_SUMMARY
-- );


-- Component flt_status, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_status AS
(
SELECT
exp_Check_Status.STATUS as STATUS
--,
--exp_Check_Status.source_record_id
FROM
exp_Check_Status
WHERE CASE WHEN to_char(exp_Check_Status.STATUS) = ''X'' THEN TRUE ELSE FALSE END
);


-- Component Shortcut_to_STG_FED_MSTR_TRG1, Type TARGET 
INSERT INTO ALFA_EDW_DEV.DB_T_STAG_PROD.STG_FED_MSTR_TRG
(
REC_COUNT
)
SELECT
flt_status.STATUS as REC_COUNT
FROM
flt_status;


END; ';