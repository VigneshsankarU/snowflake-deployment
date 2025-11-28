-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_UPDATE_FNOL_WH_STAG2("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_FNOL_WH_STAG, Type Pre SQL 
update DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG 
set date_report_produced = current_timestamp
where matched_cd = ''Y''
and Date_report_Produced is null;
update DB_T_CTRL_PROD.RTS_CONTROL
from
(select count (*) as rec_ct from DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW ) x
set REC_COUNT =x. rec_ct
where ACTUAL_RUN_TS = (select max(actual_run_ts) from DB_T_CTRL_PROD.rts_control);
update DB_T_CTRL_PROD.RTS_CONTROL
set ACTUAL_END_TS = ACTUAL_BEG_TS
where ACTUAL_RUN_TS = (select max(actual_run_ts) from DB_T_CTRL_PROD.rts_control)
and rec_count = 0;
update DB_T_CTRL_PROD.RTS_CONTROL
--select * from information_schema.tables where table_name=''RTS_CONTROL''
from (SELECT max (create_ts) max_ts from DB_T_STAG_MEMBXREF_PROD.FNOL_TEMP) A
set ACTUAL_END_TS = nvl(a.max_ts,''1900-01-01'') 
where ACTUAL_RUN_TS = (select max(actual_run_ts) from DB_T_CTRL_PROD.rts_control)
and rec_count > 0;
insert into DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW
values
(''ZZZZZZZZ''
, ''ZZZZZZZZZZZZZZZ''
, ''ZZZZZZZZZZZZZZZZZZZZ''
, ''ZZZZZZZZZZZZZZZ''
, current_timestamp
, current_timestamp
, '' '');


-- Component SQ_FNOL_WH_STAG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FNOL_WH_STAG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Policy_nbr,
$2 as Date_LOADED,
$3 as Date_Report_Produced,
$4 as matched_cd,
$5 as DOL,
$6 as claim_num2,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select
	Policy_nbr,
	Date_LOADED,
	Date_Report_Produced,
	matched_cd,
	DOL,
	claim_num2
FROM
	DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG
where
	Date_LOADED <= any (select add_months (current_date,-12) as drop_date from DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG)
) SRC
)
);


-- Component Exp_Pass_Through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_Pass_Through AS
(
SELECT
SQ_FNOL_WH_STAG.Policy_nbr as Policy_nbr,
SQ_FNOL_WH_STAG.Date_LOADED as Date_LOADED,
SQ_FNOL_WH_STAG.Date_Report_Produced as Date_Report_Produced,
SQ_FNOL_WH_STAG.matched_cd as matched_cd,
SQ_FNOL_WH_STAG.DOL as DOL,
SQ_FNOL_WH_STAG.claim_num2 as claim_num2,
SQ_FNOL_WH_STAG.source_record_id
FROM
SQ_FNOL_WH_STAG
);


-- Component FNOL_WH_STAG_ARCH, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG_ARCH
(
Policy_nbr,
Date_LOADED,
Date_Report_Produced,
matched_cd,
DOL,
claim_num2
)
SELECT
Exp_Pass_Through.Policy_nbr as Policy_nbr,
Exp_Pass_Through.Date_LOADED as Date_LOADED,
Exp_Pass_Through.Date_Report_Produced as Date_Report_Produced,
Exp_Pass_Through.matched_cd as matched_cd,
Exp_Pass_Through.DOL as DOL,
Exp_Pass_Through.claim_num2 as claim_num2
FROM
Exp_Pass_Through;


-- Component FNOL_WH_STAG_ARCH, Type Post SQL 
DELETE FROM DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG
where Date_LOADED <= any (select  add_months (current_date,-12) as drop_date from DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG);


END; ';