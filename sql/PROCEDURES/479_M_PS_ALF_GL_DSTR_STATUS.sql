-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PS_ALF_GL_DSTR_STATUS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_PSXLATITEM, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PSXLATITEM AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FIELDNAME,
$2 as FIELDVALUE,
$3 as EFFDT,
$4 as EFF_STATUS,
$5 as XLATLONGNAME,
$6 as XLATSHORTNAME,
$7 as LASTUPDDTTM,
$8 as LASTUPDOPRID,
$9 as SYNCID,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT FIELDNAME, FIELDVALUE,EFFDT, EFF_STATUS,XLATLONGNAME, XLATSHORTNAME, LASTUPDDTTM, LASTUPDOPRID, SYNCID 

FROM

 PS_DB2_OWNER.PSXLATITEM
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PSXLATITEM.FIELDNAME as FIELDNAME,
SQ_PSXLATITEM.FIELDVALUE as FIELDVALUE,
SQ_PSXLATITEM.EFFDT as EFFDT,
SQ_PSXLATITEM.EFF_STATUS as EFF_STATUS,
SQ_PSXLATITEM.XLATLONGNAME as XLATLONGNAME,
SQ_PSXLATITEM.XLATSHORTNAME as XLATSHORTNAME,
SQ_PSXLATITEM.LASTUPDDTTM as LASTUPDDTTM,
SQ_PSXLATITEM.LASTUPDOPRID as LASTUPDOPRID,
SQ_PSXLATITEM.SYNCID as SYNCID,
SQ_PSXLATITEM.source_record_id
FROM
SQ_PSXLATITEM
);


-- Component PS_ALF_GL_DSTR_STATUS, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.PS_ALF_GL_DSTR_STATUS
(
FIELDNAME,
FIELDVALUE,
EFFDT,
EFF_STATUS,
XLATLONGNAME,
XLATSHORTNAME,
LASTUPDDTTM,
LASTUPDOPRID,
SYNCID
)
SELECT
exp_pass_through.FIELDNAME as FIELDNAME,
exp_pass_through.FIELDVALUE as FIELDVALUE,
exp_pass_through.EFFDT as EFFDT,
exp_pass_through.EFF_STATUS as EFF_STATUS,
exp_pass_through.XLATLONGNAME as XLATLONGNAME,
exp_pass_through.XLATSHORTNAME as XLATSHORTNAME,
exp_pass_through.LASTUPDDTTM as LASTUPDDTTM,
exp_pass_through.LASTUPDOPRID as LASTUPDOPRID,
exp_pass_through.SYNCID as SYNCID
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_PSXLATITEM1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PSXLATITEM1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count

from

PS_DB2_OWNER.psxlatitem
) SRC
)
);


-- Component exp_psxlatitem, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_psxlatitem AS
(
SELECT
SQ_PSXLATITEM1.record_count as record_count,
:FEED_IND as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
filler,
SQ_PSXLATITEM1.source_record_id
FROM
SQ_PSXLATITEM1
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp_psxlatitem.feed_ind as feed_ind,
exp_psxlatitem.feed_dt as feed_dt,
exp_psxlatitem.record_count as record_cnt,
exp_psxlatitem.filler as FIELD1,
exp_psxlatitem.filler as FIELD2,
exp_psxlatitem.filler as FIELD3,
exp_psxlatitem.filler as FIELD4,
exp_psxlatitem.filler as FIELD5,
exp_psxlatitem.filler as FIELD6,
exp_psxlatitem.filler as FIELD7,
exp_psxlatitem.filler as FIELD8,
exp_psxlatitem.filler as FIELD9,
exp_psxlatitem.filler as FIELD10,
exp_psxlatitem.filler as FIELD11,
exp_psxlatitem.filler as FIELD12,
exp_psxlatitem.filler as FIELD13,
exp_psxlatitem.filler as FIELD14,
exp_psxlatitem.filler as FIELD15,
exp_psxlatitem.filler as FIELD16,
exp_psxlatitem.filler as FIELD17,
exp_psxlatitem.filler as FIELD18,
exp_psxlatitem.filler as FIELD19,
exp_psxlatitem.filler as FIELD20,
exp_psxlatitem.filler as FIELD21,
exp_psxlatitem.filler as FIELD22,
exp_psxlatitem.filler as FIELD23,
exp_psxlatitem.filler as FIELD24,
exp_psxlatitem.filler as FIELD25,
exp_psxlatitem.filler as FIELD26,
exp_psxlatitem.filler as FIELD27
FROM
exp_psxlatitem
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;
COPY INTO @my_internal_stage/my_export_folder/TRG_STAG_PS_LOOKUP_
FROM (SELECT * FROM TRG_STAG_PS_LOOKUP)
HEADER = TRUE
OVERWRITE = TRUE;

-- PIPELINE END FOR 2

END; ';