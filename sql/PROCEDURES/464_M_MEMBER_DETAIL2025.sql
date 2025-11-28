-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_MEMBER_DETAIL2025("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE PMMAPPINGNAME varchar;
BEGIN 

PMMAPPINGNAME:=''M_MEMBER_DETAIL2025''; 

-- Component SQ_Shortcut_to_STG_FED_MSTR_DELTA, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_DELTA AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MEMBER_NBR,
$2 as NAME_1,
$3 as NAME_2,
$4 as ALPHA_CODE,
$5 as TX_CD,
$6 as FEED_DT,
$7 as FEED_IND,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT STG_FED_MSTR_DELTA.MEMBER_NBR, STG_FED_MSTR_DELTA.NAME_1, STG_FED_MSTR_DELTA.NAME_2, STG_FED_MSTR_DELTA.ALPHA_CODE, STG_FED_MSTR_DELTA.TX_CD, STG_FED_MSTR_TRG.FEED_DT , STG_FED_MSTR_TRG.FEED_IND
FROM
 DB_T_STAG_PROD.STG_FED_MSTR_DELTA, DB_T_STAG_PROD.STG_FED_MSTR_TRG
WHERE TX_CD <> ''DEL''
) SRC
)
);


-- Component exp_Source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Source AS
(
SELECT
SQ_Shortcut_to_STG_FED_MSTR_DELTA.MEMBER_NBR as MEMBER_NBR,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.NAME_1 ) ) as out_NAME_1,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.NAME_2 ) ) as out_NAME_2,
ltrim ( rtrim ( SQ_Shortcut_to_STG_FED_MSTR_DELTA.ALPHA_CODE ) ) as out_ALPHA_CODE,
SQ_Shortcut_to_STG_FED_MSTR_DELTA.TX_CD as TX_CD,
SQ_Shortcut_to_STG_FED_MSTR_DELTA.FEED_DT as FEED_DT,
4 as out_PROJ_ID,
4 IN_PROJ_ID1,
4 IN_PROJ_ID,
''Y'' as out_BATCH_IND,
''Y'' IN_BATCH_ACTIVE_IND,
:PMMappingName as out_PRGM_NM,
:PMMappingName IN_PRGM_NM,
''MEMBER_DETAILS'' as out_TABLE_NM,
''MEMBER_DETAILS'' IN_SRC_TABLE_NM,
SQ_Shortcut_to_STG_FED_MSTR_DELTA.FEED_IND as FEED_IND,
SQ_Shortcut_to_STG_FED_MSTR_DELTA.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_DELTA
);


-- Component LKP_MEMBER_MSTR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MEMBER_MSTR AS
(
SELECT
LKP.MEMB_SKEY,
LKP.MEMB_EFF_DT,
exp_Source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Source.source_record_id ORDER BY LKP.MEMB_SKEY asc,LKP.MEMB_NUM asc,LKP.MEMB_EFF_DT asc,LKP.MEMB_EXP_DT asc,LKP.MEMB_EMAIL asc,LKP.MEMB_PHONE asc,LKP.ECTL_INS_BATCH_ID asc,LKP.ECTL_UPD_BATCH_ID asc,LKP.ECTL_INS_PRGM_ID asc,LKP.ECTL_UPD_PRGM_ID asc) RNK
FROM
exp_Source
LEFT JOIN (
SELECT MEMBER_MSTR.MEMB_SKEY as MEMB_SKEY, 
MEMBER_MSTR.MEMB_NUM as MEMB_NUM, 
MEMBER_MSTR.MEMB_EFF_DT as MEMB_EFF_DT, 
MEMBER_MSTR.MEMB_EXP_DT as MEMB_EXP_DT, 
MEMBER_MSTR.MEMB_EMAIL as MEMB_EMAIL, 
MEMBER_MSTR.MEMB_PHONE as MEMB_PHONE, 
MEMBER_MSTR.ECTL_INS_BATCH_ID as ECTL_INS_BATCH_ID, MEMBER_MSTR.ECTL_UPD_BATCH_ID as ECTL_UPD_BATCH_ID, MEMBER_MSTR.ECTL_INS_PRGM_ID as ECTL_INS_PRGM_ID,
MEMBER_MSTR.ECTL_UPD_PRGM_ID as ECTL_UPD_PRGM_ID
FROM DB_T_CORE_PROD.MEMBER_MSTR
WHERE MEMB_EXP_DT = ''9999-12-31''
) LKP ON LKP.MEMB_NUM = exp_Source.MEMBER_NBR
QUALIFY RNK = 1
);


-- Component exp_CHG_EFF_DT, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CHG_EFF_DT AS
(
SELECT
CASE WHEN exp_Source.TX_CD = ''NEW'' THEN LKP_MEMBER_MSTR.MEMB_EFF_DT ELSE CASE WHEN exp_Source.TX_CD = ''UPD'' and exp_Source.FEED_IND = ''X'' THEN to_date ( ''0101'' || to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(exp_Source.FEED_DT)) ) , ''MMDDYYYY'' ) ELSE CASE WHEN exp_Source.TX_CD = ''UPD'' and exp_Source.FEED_IND = ''M'' THEN to_date ( lpad ( to_char ( DATE_PART(''MM'', TO_TIMESTAMP(exp_Source.FEED_DT)) ) , 2 , ''0'' ) || ''01'' || to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(exp_Source.FEED_DT)) ) , ''MMDDYYYY'' ) ELSE exp_Source.FEED_DT END END END as out_CHG_EFF_DT,
exp_Source.source_record_id
FROM
exp_Source
INNER JOIN LKP_MEMBER_MSTR ON exp_Source.source_record_id = LKP_MEMBER_MSTR.source_record_id
);


-- Component LKP_MEMBER_DETAILS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MEMBER_DETAILS AS
(
SELECT
LKP.MEMB_SKEY,
LKP.CHG_EFF_DT,
LKP.NAME1,
LKP.NAME2,
LKP.ALPHA_CODE,
LKP_MEMBER_MSTR.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_MEMBER_MSTR.source_record_id ORDER BY LKP.MEMB_SKEY asc,LKP.CHG_EFF_DT asc,LKP.NAME1 asc,LKP.NAME2 asc,LKP.ALPHA_CODE asc) RNK
FROM
LKP_MEMBER_MSTR
LEFT JOIN (
SELECT
MEMBER_DETAILS.MEMB_SKEY as MEMB_SKEY,
MEMBER_DETAILS.CHG_EFF_DT as CHG_EFF_DT,
Trim(MEMBER_DETAILS.NAME1) as NAME1, 
Trim(MEMBER_DETAILS.NAME2) as NAME2,
Trim(MEMBER_DETAILS.ALPHA_CODE) as ALPHA_CODE
FROM 
DB_T_CORE_PROD.MEMBER_DETAILS
WHERE CHG_EXP_DT = ''9999-12-31''
) LKP ON LKP.MEMB_SKEY = LKP_MEMBER_MSTR.MEMB_SKEY
QUALIFY RNK = 1
);

--select * from MPLT_LKP_CTRL_TBLS_VALIDATION
-- Component m_lkp_Control_Tables, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_lkp_Control_Tables, mapplet instance m_lkp_Control_Tables;
call MPLT_LKP_CTRL_TBLS_VALIDATION(''exp_Source'');


;

-- Component rtr_Member_Details_NEW, Type ROUTER Output Group NEW
create or replace temp table rtr_Member_Details_NEW as
SELECT
LKP_MEMBER_MSTR.MEMB_SKEY as MEMB_SKEY_MMLKP,
LKP_MEMBER_DETAILS.MEMB_SKEY as MEMB_SKEY_MDLKP,
LKP_MEMBER_DETAILS.CHG_EFF_DT as CHG_EFF_DT_MDLKP,
LKP_MEMBER_DETAILS.NAME1 as NAME1_MDLKP,
LKP_MEMBER_DETAILS.NAME2 as NAME2_MDLKP,
LKP_MEMBER_DETAILS.ALPHA_CODE as ALPHA_CODE_MDLKP,
exp_Source.out_NAME_1 as NAME_1_SRC,
exp_Source.out_NAME_2 as NAME_2_SRC,
exp_Source.out_ALPHA_CODE as ALPHA_CODE_SRC,
exp_Source.TX_CD as TX_CD,
exp_CHG_EFF_DT.out_CHG_EFF_DT as CHG_EFF_DT,
m_lkp_Control_Tables.OUT_ECTL_BATCH_ID as mplt_BATCH_ID,
m_lkp_Control_Tables.OUT_ECTL_PRGM_ID as mplt_PRGM_ID,
m_lkp_Control_Tables.OUT_ECTL_TABLE_ID as mplt_TABLE_ID,
exp_Source.source_record_id
FROM
exp_Source
LEFT JOIN LKP_MEMBER_MSTR ON exp_Source.source_record_id = LKP_MEMBER_MSTR.source_record_id
LEFT JOIN exp_CHG_EFF_DT ON LKP_MEMBER_MSTR.source_record_id = exp_CHG_EFF_DT.source_record_id
LEFT JOIN LKP_MEMBER_DETAILS ON exp_CHG_EFF_DT.source_record_id = LKP_MEMBER_DETAILS.source_record_id
LEFT JOIN MPLT_LKP_CTRL_TBLS_VALIDATION m_lkp_Control_Tables ON LKP_MEMBER_DETAILS.source_record_id = m_lkp_Control_Tables.source_record_id
WHERE CASE WHEN LKP_MEMBER_DETAILS.MEMB_SKEY IS NULL or ( exp_Source.TX_CD = ''UPD'' and ( LKP_MEMBER_DETAILS.NAME1 != exp_Source.out_NAME_1 or LKP_MEMBER_DETAILS.NAME2 != exp_Source.out_NAME_2 or LKP_MEMBER_DETAILS.ALPHA_CODE != exp_Source.out_ALPHA_CODE ) ) THEN TRUE ELSE FALSE END 
-- - - CASE WHEN ( exp_Source.TX_CD = ''NEW'' or exp_Source.TX_CD = ''UPD'' ) and ( ltrim ( rtrim ( LKP_MEMBER_DETAILS.NAME1 ) ) != ltrim ( rtrim ( exp_Source.out_NAME_1 ) ) or ltrim ( rtrim ( LKP_MEMBER_DETAILS.NAME2 ) ) != ltrim ( rtrim ( exp_Source.out_NAME_2 ) ) or ltrim ( rtrim ( LKP_MEMBER_DETAILS.ALPHA_CODE ) ) != ltrim ( rtrim ( exp_Source.out_ALPHA_CODE ) ) ) THEN TRUE ELSE FALSE END - - CASE WHEN exp_Source.TX_CD = ''NEW'' or ( exp_Source.TX_CD = ''UPD'' and ( LKP_MEMBER_DETAILS.NAME1 != exp_Source.out_NAME_1 or LKP_MEMBER_DETAILS.NAME2 != exp_Source.out_NAME_2 or LKP_MEMBER_DETAILS.ALPHA_CODE != exp_Source.out_ALPHA_CODE ) ) THEN TRUE ELSE FALSE END
;


-- Component rtr_Member_Details_UPD, Type ROUTER Output Group UPD
create or replace temp table rtr_Member_Details_UPD as 
SELECT
LKP_MEMBER_MSTR.MEMB_SKEY as MEMB_SKEY_MMLKP,
LKP_MEMBER_DETAILS.MEMB_SKEY as MEMB_SKEY_MDLKP,
LKP_MEMBER_DETAILS.CHG_EFF_DT as CHG_EFF_DT_MDLKP,
LKP_MEMBER_DETAILS.NAME1 as NAME1_MDLKP,
LKP_MEMBER_DETAILS.NAME2 as NAME2_MDLKP,
LKP_MEMBER_DETAILS.ALPHA_CODE as ALPHA_CODE_MDLKP,
exp_Source.out_NAME_1 as NAME_1_SRC,
exp_Source.out_NAME_2 as NAME_2_SRC,
exp_Source.out_ALPHA_CODE as ALPHA_CODE_SRC,
exp_Source.TX_CD as TX_CD,
exp_CHG_EFF_DT.out_CHG_EFF_DT as CHG_EFF_DT,
m_lkp_Control_Tables.OUT_ECTL_BATCH_ID as mplt_BATCH_ID,
m_lkp_Control_Tables.OUT_ECTL_PRGM_ID as mplt_PRGM_ID,
m_lkp_Control_Tables.OUT_ECTL_TABLE_ID as mplt_TABLE_ID,


exp_Source.source_record_id
FROM
exp_Source
LEFT JOIN LKP_MEMBER_MSTR ON exp_Source.source_record_id = LKP_MEMBER_MSTR.source_record_id
LEFT JOIN exp_CHG_EFF_DT ON LKP_MEMBER_MSTR.source_record_id = exp_CHG_EFF_DT.source_record_id
LEFT JOIN LKP_MEMBER_DETAILS ON exp_CHG_EFF_DT.source_record_id = LKP_MEMBER_DETAILS.source_record_id
LEFT JOIN MPLT_LKP_CTRL_TBLS_VALIDATION m_lkp_Control_Tables ON LKP_MEMBER_DETAILS.source_record_id = m_lkp_Control_Tables.source_record_id
WHERE CASE WHEN ( exp_Source.TX_CD = ''UPD'' and ( ltrim ( rtrim ( LKP_MEMBER_DETAILS.NAME1 ) ) != ltrim ( rtrim ( exp_Source.out_NAME_1 ) ) or ltrim ( rtrim ( LKP_MEMBER_DETAILS.NAME2 ) ) != ltrim ( rtrim ( exp_Source.out_NAME_2 ) ) or ltrim ( rtrim ( LKP_MEMBER_DETAILS.ALPHA_CODE ) ) != ltrim ( rtrim ( exp_Source.out_ALPHA_CODE ) ) ) ) THEN TRUE ELSE FALSE END;


-- Component upd_Member_Details, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Member_Details AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Member_Details_UPD.MEMB_SKEY_MDLKP as MEMB_SKEY_MDLKP3,
rtr_Member_Details_UPD.CHG_EFF_DT_MDLKP as CHG_EFF_DT_MDLKP3,
rtr_Member_Details_UPD.mplt_BATCH_ID as mplt_BATCH_ID3,
rtr_Member_Details_UPD.mplt_PRGM_ID as mplt_PRGM_ID3,
rtr_Member_Details_UPD.CHG_EFF_DT as CHG_EFF_DT3,
1 as UPDATE_STRATEGY_ACTION,
 SOURCE_RECORD_ID
FROM
rtr_Member_Details_UPD
);


-- Component exp_Member_Details, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Member_Details AS
(
SELECT
rtr_Member_Details_NEW.MEMB_SKEY_MMLKP as MEMB_SKEY,
rtr_Member_Details_NEW.CHG_EFF_DT as CHG_EFF_DT,
to_date ( ''9999-12-31'' , ''YYYY-MM-DD'' ) as out_EXP_DT,
rtr_Member_Details_NEW.NAME_1_SRC as NAME_1,
rtr_Member_Details_NEW.NAME_2_SRC as NAME_2,
rtr_Member_Details_NEW.ALPHA_CODE_SRC as ALPHA_CODE,
rtr_Member_Details_NEW.mplt_BATCH_ID as INS_BATCH_ID,
rtr_Member_Details_NEW.mplt_PRGM_ID as INS_PRGM_ID,
rtr_Member_Details_NEW.source_record_id
FROM
rtr_Member_Details_NEW
);


-- Component MEMBER_DETAILS_INS, Type TARGET 
INSERT INTO DB_T_CORE_PROD.MEMBER_DETAILS
(
MEMB_SKEY,
CHG_EFF_DT,
CHG_EXP_DT,
NAME1,
NAME2,
ALPHA_CODE,
ECTL_INS_BATCH_ID,
ECTL_INS_PRGM_ID
)
SELECT
nvl(exp_Member_Details.MEMB_SKEY,-1) as MEMB_SKEY,
nvl(exp_Member_Details.CHG_EFF_DT,''1900-01-01'') as CHG_EFF_DT,
nvl(exp_Member_Details.out_EXP_DT,''1900-01-01'') as CHG_EXP_DT,
nvl(exp_Member_Details.NAME_1,''-1'') as NAME1,
nvl(exp_Member_Details.NAME_2,''-1'') as NAME2,
nvl(exp_Member_Details.ALPHA_CODE,''-1'') as ALPHA_CODE,
nvl(exp_Member_Details.INS_BATCH_ID,-1) as ECTL_INS_BATCH_ID,
nvl(exp_Member_Details.INS_PRGM_ID,-1) as ECTL_INS_PRGM_ID
FROM
exp_Member_Details;


-- Component exp_Expire_Member_Details, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Expire_Member_Details AS
(
SELECT
upd_Member_Details.MEMB_SKEY_MDLKP3 as MEMB_SKEY,
upd_Member_Details.CHG_EFF_DT_MDLKP3 as CHG_EFF_DT,
to_date( upd_Member_Details.CHG_EFF_DT3)  - 1  as out_EXP_DT,
upd_Member_Details.mplt_BATCH_ID3 as UPD_BATCH_ID,
upd_Member_Details.mplt_PRGM_ID3 as UPD_PRGM_ID,
upd_Member_Details.source_record_id
FROM
upd_Member_Details
);


-- Component MEMBER_DETAILS_EXPIRE, Type TARGET 
MERGE INTO DB_T_CORE_PROD.MEMBER_DETAILS
USING exp_Expire_Member_Details ON (MEMBER_DETAILS.MEMB_SKEY = exp_Expire_Member_Details.MEMB_SKEY AND MEMBER_DETAILS.CHG_EFF_DT = exp_Expire_Member_Details.CHG_EFF_DT)
WHEN MATCHED THEN UPDATE
SET
MEMB_SKEY = exp_Expire_Member_Details.MEMB_SKEY,
CHG_EFF_DT = exp_Expire_Member_Details.CHG_EFF_DT,
CHG_EXP_DT = exp_Expire_Member_Details.out_EXP_DT,
ECTL_UPD_BATCH_ID = exp_Expire_Member_Details.UPD_BATCH_ID,
ECTL_UPD_PRGM_ID = exp_Expire_Member_Details.UPD_PRGM_ID;


END; ';