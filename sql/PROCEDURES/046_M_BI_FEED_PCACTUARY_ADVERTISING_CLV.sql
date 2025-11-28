-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_FEED_PCACTUARY_ADVERTISING_CLV("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_BI_FEED_PCActuary_Advertising_CLV_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_BI_FEED_PCActuary_Advertising_CLV
(
FIELD1 varchar(42),
FIELD2 varchar(6),
UW_CMPY varchar(50),
FIELD4 varchar(20),
AMT_TYPE varchar(50),
LLINE_OF_BUSI varchar(50),
STATE varchar(50),
FIELD8 varchar(16),
QTR_ID varchar(50),
FIELD10 varchar(12),
AMT decimal,
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_BI_FEED_PCActuary_Advertising_CLV_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp_BI_FEED_PCAdvertising_Fees, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_BI_FEED_PCAdvertising_Fees AS
(
SELECT
SQ_BI_FEED_PCActuary_Advertising_CLV.UW_CMPY as UW_CMPY,
SQ_BI_FEED_PCActuary_Advertising_CLV.STATE as STATE,
SQ_BI_FEED_PCActuary_Advertising_CLV.LLINE_OF_BUSI as LINE_OF_BUS,
SQ_BI_FEED_PCActuary_Advertising_CLV.AMT_TYPE as AMT_TYPE,
SQ_BI_FEED_PCActuary_Advertising_CLV.AMT as AMT,
substr ( SQ_BI_FEED_PCActuary_Advertising_CLV.QTR_ID , 1 , 3 ) as v_MTH,
''01'' as v_DAY,
substr ( SQ_BI_FEED_PCActuary_Advertising_CLV.QTR_ID , - 2 , 2 ) as v_YEAR,
concat ( concat ( concat ( v_YEAR , ''/'' ) , concat ( v_MTH , ''/'' ) ) , v_DAY ) as v_CONCAT,
LAST_DAY ( TO_DATE ( v_CONCAT , ''YY/MON/DD'' ) ) as o_QTR_ID,
SQ_BI_FEED_PCActuary_Advertising_CLV.source_record_id
FROM
SQ_BI_FEED_PCActuary_Advertising_CLV
);


-- Component BI_FEED_PCActuary_Advertising_CLV1, Type TARGET 
INSERT INTO DB_T_PROD_STAG.BI_FEED_PCActuary_Advertising_CLV
(
UW_CMPY,
STATE,
LINE_OF_BUS,
QTR_ID,
AMT_TYPE,
AMT
)
SELECT
exp_BI_FEED_PCAdvertising_Fees.UW_CMPY as UW_CMPY,
exp_BI_FEED_PCAdvertising_Fees.STATE as STATE,
exp_BI_FEED_PCAdvertising_Fees.LINE_OF_BUS as LINE_OF_BUS,
exp_BI_FEED_PCAdvertising_Fees.o_QTR_ID as QTR_ID,
exp_BI_FEED_PCAdvertising_Fees.AMT_TYPE as AMT_TYPE,
exp_BI_FEED_PCAdvertising_Fees.AMT as AMT
FROM
exp_BI_FEED_PCAdvertising_Fees;


END; ';