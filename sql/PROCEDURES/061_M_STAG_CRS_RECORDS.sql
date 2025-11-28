-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CRS_RECORDS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  MAX_UPDATE_TS STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):MAX_UPDATE_TS::STRING
  INTO
    MAX_UPDATE_TS;

-- Component SQ_CRS_Records, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CRS_Records AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as MemberNumber,
$3 as AgentNumber,
$4 as LastReviewDate,
$5 as DocPreparedDate,
$6 as DocCreateUser,
$7 as CreationUID,
$8 as UpdateUID,
$9 as CreationTS,
$10 as UpdateTS,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CRS_Records.ID, CRS_Records.MemberNumber, CRS_Records.AgentNumber, CRS_Records.LastReviewDate, CRS_Records.DocPreparedDate, CRS_Records.DocCreateUser, CRS_Records.CreationUID, CRS_Records.UpdateUID, CRS_Records.CreationTS, CRS_Records.UpdateTS 

FROM

 Mule_Integrations.DB_T_PROD_STAG.CRS_Records

where CRS_Records.UpdateTS > :MAX_UPDATE_TS
) SRC
)
);


-- Component exp_src_to_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_to_tgt_pass AS
(
SELECT
SQ_CRS_Records.ID as ID,
SQ_CRS_Records.MemberNumber as MemberNumber,
SQ_CRS_Records.AgentNumber as AgentNumber,
SQ_CRS_Records.LastReviewDate as LastReviewDate,
SQ_CRS_Records.DocPreparedDate as DocPreparedDate,
SQ_CRS_Records.DocCreateUser as DocCreateUser,
SQ_CRS_Records.CreationUID as CreationUID,
SQ_CRS_Records.UpdateUID as UpdateUID,
SQ_CRS_Records.CreationTS as CreationTS,
SQ_CRS_Records.UpdateTS as UpdateTS,
SQ_CRS_Records.source_record_id
FROM
SQ_CRS_Records
);


-- Component CRS_Records, Type TARGET 
INSERT INTO DB_T_PROD_STAG.CRS_Records
(
ID_stg,
MemberNumber_stg,
AgentNumber_stg,
LastReviewDate_stg,
DocPreparedDate_stg,
DocCreateUser_stg,
CreationUID_stg,
UpdateUID_stg,
CreationTS_stg,
UpdateTS_stg
)
SELECT
exp_src_to_tgt_pass.ID as ID_stg,
exp_src_to_tgt_pass.MemberNumber as MemberNumber_stg,
exp_src_to_tgt_pass.AgentNumber as AgentNumber_stg,
exp_src_to_tgt_pass.LastReviewDate as LastReviewDate_stg,
exp_src_to_tgt_pass.DocPreparedDate as DocPreparedDate_stg,
exp_src_to_tgt_pass.DocCreateUser as DocCreateUser_stg,
exp_src_to_tgt_pass.CreationUID as CreationUID_stg,
exp_src_to_tgt_pass.UpdateUID as UpdateUID_stg,
exp_src_to_tgt_pass.CreationTS as CreationTS_stg,
exp_src_to_tgt_pass.UpdateTS as UpdateTS_stg
FROM
exp_src_to_tgt_pass;


END; ';