-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PS_ALF_PRODUCT_II("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_PS_ALFXLATVALS, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_ALFXLATVALS AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SETID,
$2 as SOURCE,
$3 as CHARTFIELD,
$4 as ALF_KEY1,
$5 as ALF_KEY2,
$6 as ALF_KEY3,
$7 as ALF_KEY4,
$8 as ALF_KEY5,
$9 as ALF_KEY6,
$10 as ALF_KEY7,
$11 as KEY_01,
$12 as KEY_02,
$13 as KEY_03,
$14 as KEY_04,
$15 as KEY_05,
$16 as KEY_06,
$17 as KEY_07,
$18 as EFFDT,
$19 as EFF_STATUS,
$20 as ALF_MAPPING_TYPE,
$21 as DESCR,
$22 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT SETID, SOURCE, CHARTFIELD,ALF_KEY1, ALF_KEY2, ALF_KEY3,ALF_KEY4,ALF_KEY5,ALF_KEY6,ALF_KEY7, KEY_01,KEY_02,KEY_03,KEY_04, KEY_05, KEY_06, KEY_07, EFFDT, EFF_STATUS, ALF_MAPPING_TYPE, DESCR 

FROM

 PS_DB2_OWNER.PS_ALFXLATVALS
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PS_ALFXLATVALS.SETID as SETID,
SQ_PS_ALFXLATVALS.SOURCE as SOURCE,
SQ_PS_ALFXLATVALS.CHARTFIELD as CHARTFIELD,
SQ_PS_ALFXLATVALS.ALF_KEY1 as ALF_KEY1,
SQ_PS_ALFXLATVALS.ALF_KEY2 as ALF_KEY2,
SQ_PS_ALFXLATVALS.ALF_KEY3 as ALF_KEY3,
SQ_PS_ALFXLATVALS.ALF_KEY4 as ALF_KEY4,
SQ_PS_ALFXLATVALS.ALF_KEY5 as ALF_KEY5,
SQ_PS_ALFXLATVALS.ALF_KEY6 as ALF_KEY6,
SQ_PS_ALFXLATVALS.ALF_KEY7 as ALF_KEY7,
SQ_PS_ALFXLATVALS.KEY_01 as KEY_01,
SQ_PS_ALFXLATVALS.KEY_02 as KEY_02,
SQ_PS_ALFXLATVALS.KEY_03 as KEY_03,
SQ_PS_ALFXLATVALS.KEY_04 as KEY_04,
SQ_PS_ALFXLATVALS.KEY_05 as KEY_05,
SQ_PS_ALFXLATVALS.KEY_06 as KEY_06,
SQ_PS_ALFXLATVALS.KEY_07 as KEY_07,
SQ_PS_ALFXLATVALS.EFFDT as EFFDT,
SQ_PS_ALFXLATVALS.EFF_STATUS as EFF_STATUS,
SQ_PS_ALFXLATVALS.ALF_MAPPING_TYPE as ALF_MAPPING_TYPE,
SQ_PS_ALFXLATVALS.DESCR as DESCR,
SQ_PS_ALFXLATVALS.source_record_id
FROM
SQ_PS_ALFXLATVALS
);


-- Component PS_ALF_PRODUCT_II, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.PS_ALF_PRODUCT_II
(
SETID,
SOURCE,
CHARTFIELD,
ALF_KEY1,
ALF_KEY2,
ALF_KEY3,
ALF_KEY4,
ALF_KEY5,
ALF_KEY6,
ALF_KEY7,
KEY_01,
KEY_02,
KEY_03,
KEY_04,
KEY_05,
KEY_06,
KEY_07,
EFFDT,
EFF_STATUS,
ALF_MAPPING_TYPE,
DESCR
)
SELECT
exp_pass_through.SETID as SETID,
exp_pass_through.SOURCE as SOURCE,
exp_pass_through.CHARTFIELD as CHARTFIELD,
exp_pass_through.ALF_KEY1 as ALF_KEY1,
exp_pass_through.ALF_KEY2 as ALF_KEY2,
exp_pass_through.ALF_KEY3 as ALF_KEY3,
exp_pass_through.ALF_KEY4 as ALF_KEY4,
exp_pass_through.ALF_KEY5 as ALF_KEY5,
exp_pass_through.ALF_KEY6 as ALF_KEY6,
exp_pass_through.ALF_KEY7 as ALF_KEY7,
exp_pass_through.KEY_01 as KEY_01,
exp_pass_through.KEY_02 as KEY_02,
exp_pass_through.KEY_03 as KEY_03,
exp_pass_through.KEY_04 as KEY_04,
exp_pass_through.KEY_05 as KEY_05,
exp_pass_through.KEY_06 as KEY_06,
exp_pass_through.KEY_07 as KEY_07,
exp_pass_through.EFFDT as EFFDT,
exp_pass_through.EFF_STATUS as EFF_STATUS,
exp_pass_through.ALF_MAPPING_TYPE as ALF_MAPPING_TYPE,
exp_pass_through.DESCR as DESCR
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_PS_ALFXLATVALS1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_ALFXLATVALS1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count

from PS_DB2_OWNER.ps_alfxlatvals
) SRC
)
);


-- Component exp_alfxlatvals, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_alfxlatvals AS
(
SELECT
TO_NUMBER(SQ_PS_ALFXLATVALS1.record_count) as o_record_count,
:feed_ind as feed_ind,
to_Char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
filler,
SQ_PS_ALFXLATVALS1.source_record_id
FROM
SQ_PS_ALFXLATVALS1
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp_alfxlatvals.feed_ind as feed_ind,
exp_alfxlatvals.feed_dt as feed_dt,
exp_alfxlatvals.o_record_count as record_cnt,
exp_alfxlatvals.filler as FIELD1,
exp_alfxlatvals.filler as FIELD2,
exp_alfxlatvals.filler as FIELD3,
exp_alfxlatvals.filler as FIELD4,
exp_alfxlatvals.filler as FIELD5,
exp_alfxlatvals.filler as FIELD6,
exp_alfxlatvals.filler as FIELD7,
exp_alfxlatvals.filler as FIELD8,
exp_alfxlatvals.filler as FIELD9,
exp_alfxlatvals.filler as FIELD10,
exp_alfxlatvals.filler as FIELD11,
exp_alfxlatvals.filler as FIELD12,
exp_alfxlatvals.filler as FIELD13,
exp_alfxlatvals.filler as FIELD14,
exp_alfxlatvals.filler as FIELD15,
exp_alfxlatvals.filler as FIELD16,
exp_alfxlatvals.filler as FIELD17,
exp_alfxlatvals.filler as FIELD18,
exp_alfxlatvals.filler as FIELD19,
exp_alfxlatvals.filler as FIELD20,
exp_alfxlatvals.filler as FIELD21,
exp_alfxlatvals.filler as FIELD22,
exp_alfxlatvals.filler as FIELD23,
exp_alfxlatvals.filler as FIELD24,
exp_alfxlatvals.filler as FIELD25,
exp_alfxlatvals.filler as FIELD26,
exp_alfxlatvals.filler as FIELD27
FROM
exp_alfxlatvals
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 2

END; ';