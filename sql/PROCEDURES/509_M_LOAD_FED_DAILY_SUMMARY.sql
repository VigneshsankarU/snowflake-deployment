-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_FED_DAILY_SUMMARY("RUN_ID" VARCHAR)
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
CREATE
OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_TRG AS
SELECT
  CALENDAR_DATE AS FEED_DT,
  source_record_id
FROM
  (
    SELECT
      SRC.*,
      ROW_NUMBER() OVER (
        ORDER BY
          CALENDAR_DATE
      ) AS source_record_id
    FROM
      (
        SELECT
          a.CALENDAR_DATE,
          b.FEED_IND
        FROM
          PUBLIC.SYS_BUSINESSCALENDAR AS a,
          DB_T_STAG_PROD.stg_fed_mstr_trg AS b
        WHERE
          a.CALENDAR_DATE > (
            SELECT
              COALESCE(MAX(CAL_DATE), CAST(''2009-05-31'' AS DATE))
            FROM
              DB_T_CORE_PROD.FED_DAILY_SUMMARY
          )
          AND a.CALENDAR_DATE <= (
            SELECT
              FEED_DT
            FROM
              STG_FED_MSTR_TRG
          )
          AND b.FEED_IND =''D''
        ORDER BY
          1
      ) AS SRC
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


-- Component LOAD_FED_DAILY_SUMMARY, Type STORED_PROCEDURE 

--ALFA_EDW_DEV.DB_SP_PROD.LOAD_FED_DAILY_SUMMARY

call DB_SP_PROD.LOAD_FED_DAILY_SUMMARY (:V_PRM_DATE,:v_status_cd);
 CREATE OR REPLACE TEMPORARY TABLE LOAD_FED_DAILY_SUMMARY AS
 (
 select *, row_number() over (order by 1) AS source_record_id  from DB_T_CORE_PROD.FED_DAILY_SUMMARY where CAL_DATE=to_date(:V_PRM_DATE)
-- -- WARNING: Stored Procedure node is not supported, manual conversion required. Commented call text below:

--DB_V_PROD.MEMBER_TRANS_v
);


-- CREATE OR REPLACE TEMPORARY TABLE LOAD_FED_DAILY_SUMMARY AS
-- (
-- CREATE OR REPLACE TEMPORARY TABLE LOAD_FED_DAILY_SUMMARY AS
-- (
-- -- WARNING: Stored Procedure node is not supported, manual conversion required. Commented call text below:
-- -- LOAD_FED_DAILY_SUMMARY
-- )
-- );


-- Component exp_Check_Status, Type EXPRESSION 
-- CREATE OR REPLACE TEMPORARY TABLE exp_Check_Status AS
-- (
-- SELECT
-- LOAD_FED_DAILY_SUMMARY.STATUS as STATUS,
-- CASE WHEN LOAD_FED_DAILY_SUMMARY.STATUS = ''1'' THEN RAISE_ERROR(''Error while executing stored procedure LOAD_MEMBER_SUMMARY'') ELSE '''''''' END as out_Abort,
-- LOAD_FED_DAILY_SUMMARY.source_record_id
-- FROM
-- LOAD_FED_DAILY_SUMMARY
-- );

CREATE
OR REPLACE TEMPORARY TABLE exp_Check_Status AS
SELECT
  pac_type AS STATUS,
   COUNT(pac_type) as STATUS_CD_count
   --,
 -- source_record_id
FROM
  LOAD_FED_DAILY_SUMMARY where pac_type=1 group by pac_type;
LET status_count NUMBER := (SELECT STATUS_CD_count                                
FROM exp_Check_Status                                
WHERE STATUS = :v_status_cd);        
IF (status_count > 0) THEN        
RAISE my_exception;    
END IF; 



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
INSERT INTO DB_T_STAG_PROD.STG_FED_MSTR_TRG
(
REC_COUNT
)
SELECT
flt_status.STATUS as REC_COUNT
FROM
flt_status;


END; ';