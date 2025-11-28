-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ADDRESS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare 
PMMAPPINGNAME varchar;
BEGIN 
PMMAPPINGNAME:=''m_address'';
-- Component SQ_Shortcut_to_STG_FED_MSTR_DELTA, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_DELTA AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ADDRESS_1,
$2 as ADDRESS_2,
$3 as CITY,
$4 as STATE,
$5 as ZIP_CODE,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT STG_FED_MSTR_DELTA.ADDRESS_1, STG_FED_MSTR_DELTA.ADDRESS_2, STG_FED_MSTR_DELTA.CITY, STG_FED_MSTR_DELTA.STATE, STG_FED_MSTR_DELTA.ZIP_CODE 
FROM
 DB_T_STAG_PROD.STG_FED_MSTR_DELTA
WHERE TX_CD <> ''DEL''
) SRC
)
);


-- Component exp_Address, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Address AS
(
SELECT
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.ADDRESS_1 ) ) as out_ADDRESS_1,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.ADDRESS_2 ) ) as out_ADDRESS_2,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.CITY ) ) as out_CITY,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.STATE ) ) as out_STATE,
ltrim ( rtrim ( to_char ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.ZIP_CODE ) ) ) as out_ZIPCODE,
4 as PROJECT_ID,
''Y'' as BATCH_ACTIVE_IND,
:PMMAPPINGNAME as PROGRAM_NM,
''ADDRESS'' as TABLE_NAME,
SQ_Shortcut_to_STG_FED_MSTR_DELTA.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_DELTA
);


-- Component m_lkp_Control_Tables, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_lkp_Control_Tables, mapplet instance m_lkp_Control_Tables;
call m_lkp_Control_Tables(''exp_Address'');

-- Component LKP_ADDRESS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ADDRESS AS
(
SELECT
LKP.DW_ADDR_SKEY,
exp_Address.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Address.source_record_id ORDER BY LKP.DW_ADDR_SKEY asc) RNK
FROM
exp_Address
LEFT JOIN (
SELECT DW_ADDR_SKEY as DW_ADDR_SKEY, 
Trim(ADDR_LINE_1) as ADDR_LINE_1, 
Trim(ADDR_LINE_2) as ADDR_LINE_2, 
Trim(CITY) as CITY, 
Trim(ST_CD) as ST_CD, 
Trim(ZIP_CD) as ZIP_CD 
FROM DB_T_CORE_PROD.ADDRESS
) LKP ON LKP.ADDR_LINE_1 = exp_Address.out_ADDRESS_1 AND LKP.ADDR_LINE_2 = exp_Address.out_ADDRESS_2 AND LKP.CITY = exp_Address.out_CITY AND LKP.ST_CD = exp_Address.out_STATE AND LKP.ZIP_CD = exp_Address.out_ZIPCODE
QUALIFY RNK = 1
);


-- Component rtr_Address_NEW_ADDRESS, Type ROUTER Output Group NEW_ADDRESS
CREATE OR REPLACE TEMPORARY TABLE rtr_Address_NEW_ADDRESS AS
(SELECT
LKP_ADDRESS.DW_ADDR_SKEY as DW_ADDR_SKEY,
exp_Address.out_ADDRESS_1 as ADDRESS_1,
exp_Address.out_ADDRESS_2 as ADDRESS_2,
exp_Address.out_CITY as CITY,
exp_Address.out_STATE as STATE,
exp_Address.out_ZIPCODE as ZIP_CODE,
m_lkp_Control_Tables.mplt_BATCH_ID as mplt_BATCH_ID,
m_lkp_Control_Tables.mplt_PRGM_ID as mplt_PRGM_ID,
m_lkp_Control_Tables.mplt_TABLE_ID as mplt_TABLE_ID,
m_lkp_Control_Tables.mplt_PROJ_ID as mplt_PROJ_ID,
exp_Address.source_record_id
FROM
exp_Address
LEFT JOIN m_lkp_Control_Tables ON exp_Address.source_record_id = m_lkp_Control_Tables.source_record_id
LEFT JOIN LKP_ADDRESS ON m_lkp_Control_Tables.source_record_id = LKP_ADDRESS.source_record_id
WHERE CASE WHEN LKP_ADDRESS.DW_ADDR_SKEY IS NULL THEN TRUE ELSE FALSE END);


-- Component exp_New_Address, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_New_Address AS
(
SELECT
seq_Address_Id.NEXTVAL as ADDRESS_KEY,
ltrim ( rtrim ( rtr_Address_NEW_ADDRESS.ADDRESS_1 ) ) as out_ADDRESS_1,
ltrim ( rtrim ( rtr_Address_NEW_ADDRESS.ADDRESS_2 ) ) as out_ADDRESS_2,
ltrim ( rtrim ( rtr_Address_NEW_ADDRESS.CITY ) ) as out_CITY,
ltrim ( rtrim ( rtr_Address_NEW_ADDRESS.STATE ) ) as out_STATE,
ltrim ( rtrim ( rtr_Address_NEW_ADDRESS.ZIP_CODE ) ) as out_ZIP_CODE,
rtr_Address_NEW_ADDRESS.mplt_BATCH_ID as mplt_BATCH_ID,
rtr_Address_NEW_ADDRESS.mplt_PRGM_ID as mplt_PRGM_ID,
CURRENT_TIMESTAMP as out_INS_TS,
rtr_Address_NEW_ADDRESS.source_record_id
FROM
rtr_Address_NEW_ADDRESS
);


-- Component ADDRESS_INS, Type TARGET 
INSERT INTO DB_T_CORE_PROD.ADDRESS
(
DW_ADDR_SKEY,
ADDR_ID,
ADDR_LINE_1,
ADDR_LINE_2,
CITY,
ST_CD,
ZIP_CD,
ECTL_INS_BATCH_ID,
ECTL_INS_PRGM_ID,
ECTL_ORIG_INS_TS
)
SELECT
exp_New_Address.ADDRESS_KEY as DW_ADDR_SKEY,
exp_New_Address.ADDRESS_KEY as ADDR_ID,
exp_New_Address.out_ADDRESS_1 as ADDR_LINE_1,
exp_New_Address.out_ADDRESS_2 as ADDR_LINE_2,
exp_New_Address.out_CITY as CITY,
exp_New_Address.out_STATE as ST_CD,
exp_New_Address.out_ZIP_CODE as ZIP_CD,
exp_New_Address.mplt_BATCH_ID as ECTL_INS_BATCH_ID,
exp_New_Address.mplt_PRGM_ID as ECTL_INS_PRGM_ID,
exp_New_Address.out_INS_TS as ECTL_ORIG_INS_TS
FROM
exp_New_Address;


END; ';