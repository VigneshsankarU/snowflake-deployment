-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STG_FED_MSTR_CUR("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_SRC_FED_MSTR_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_SRC_FED_MSTR
(
FED_TYPE varchar(1),
MEMBER_NBR integer,
COMMODITY varchar(3),
COUNTY integer,
STATUS integer,
AMOUNT_PAID integer,
PAID_TO_DATE varchar(8),
BATCH_NBR_1 integer,
BATCH_TYPE_1 integer,
BATCH_DATE_1 varchar(8),
BATCH_NBR_2 integer,
BATCH_TYPE_2 integer,
BATCH_DATE_2 varchar(8),
BATCH_NBR_3 integer,
BATCH_TYPE_3 integer,
BATCH_DATE_3 varchar(8),
COUNTY_PAID_DT varchar(8),
FED_SOURCE varchar(1),
OUT_OF_STATE varchar(1),
NAME_1 varchar(25),
NAME_2 varchar(25),
ADDRESS_1 varchar(25),
ADDRESS_2 varchar(25),
ALPHA_CODE varchar(5),
CITY varchar(17),
STATE varchar(2),
ZIP integer,
COUNTY_CHANGE_IND varchar(1),
GW_BILL_REF varchar(11),
GW_DUE_DATE varchar(8),
COMB_BILL_IND varchar(1),
ALLOW_COMBINED_BILLING varchar(1),
FOUR_DAY_LTR_IND varchar(1),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_SRC_FED_MSTR_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp_Trim, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Trim AS
(
SELECT
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.FED_TYPE ) ) as out_FED_TYPE,
SQ_Shortcut_to_SRC_FED_MSTR.MEMBER_NBR as MEMBER_NBR,
SQ_Shortcut_to_SRC_FED_MSTR.COMMODITY as COMMODITY,
SQ_Shortcut_to_SRC_FED_MSTR.COUNTY as COUNTY,
SQ_Shortcut_to_SRC_FED_MSTR.STATUS as STATUS,
SQ_Shortcut_to_SRC_FED_MSTR.AMOUNT_PAID as AMOUNT_PAID,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.PAID_TO_DATE ) ) as out_PAID_TO_DATE,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_NBR_1 as BATCH_NBR_1,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_TYPE_1 as BATCH_TYPE_1,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.BATCH_DATE_1 ) ) as out_BATCH_DATE_1,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_NBR_2 as BATCH_NBR_2,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_TYPE_2 as BATCH_TYPE_2,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.BATCH_DATE_2 ) ) as out_BATCH_DATE_2,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_NBR_3 as BATCH_NBR_3,
SQ_Shortcut_to_SRC_FED_MSTR.BATCH_TYPE_3 as BATCH_TYPE_3,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.BATCH_DATE_3 ) ) as out_BATCH_DATE_3,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.COUNTY_PAID_DT ) ) as out_COUNTY_PAID_DT,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.FED_SOURCE ) ) as out_FED_SOURCE,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.OUT_OF_STATE ) ) as out_OUT_OF_STATE,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.NAME_1 ) ) as out_NAME_1,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.NAME_2 ) ) as out_NAME_2,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.ADDRESS_1 ) ) as out_ADDRESS_1,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.ADDRESS_2 ) ) as out_ADDRESS_2,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.ALPHA_CODE ) ) as out_ALPHA_CODE,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.CITY ) ) as out_CITY,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.STATE ) ) as out_STATE,
SQ_Shortcut_to_SRC_FED_MSTR.ZIP as ZIP,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.COUNTY_CHANGE_IND ) ) as out_COUNTY_CHANGE_IND,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.GW_BILL_REF ) ) as out_GW_BILL_REF,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.GW_DUE_DATE ) ) as out_GW_DUE_DATE,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.COMB_BILL_IND ) ) as out_COMB_BILL_IND,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.ALLOW_COMBINED_BILLING ) ) as out_ALLOW_COMBINED_BILLING,
ltrim ( rtrim ( SQ_Shortcut_to_SRC_FED_MSTR.FOUR_DAY_LTR_IND ) ) as out_FOUR_DAY_LTR_IND,
SQ_Shortcut_to_SRC_FED_MSTR.source_record_id
FROM
SQ_Shortcut_to_SRC_FED_MSTR
);


-- Component STG_FED_MSTR_CUR, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE STG_FED_MSTR_CUR AS
(
SELECT
exp_Trim.out_FED_TYPE as FED_TYPE,
exp_Trim.MEMBER_NBR as MEMBER_NBR,
exp_Trim.COMMODITY as COMMODITY,
exp_Trim.COUNTY as COUNTY,
exp_Trim.STATUS as STATUS,
exp_Trim.AMOUNT_PAID as AMOUNT_PAID,
exp_Trim.out_PAID_TO_DATE as PAID_TO_DATE,
exp_Trim.BATCH_NBR_1 as BATCH_NBR_1,
exp_Trim.BATCH_TYPE_1 as BATCH_TYPE_1,
exp_Trim.out_BATCH_DATE_1 as BATCH_DATE_1,
exp_Trim.BATCH_NBR_2 as BATCH_NBR_2,
exp_Trim.BATCH_TYPE_2 as BATCH_TYPE_2,
exp_Trim.out_BATCH_DATE_2 as BATCH_DATE_2,
exp_Trim.BATCH_NBR_3 as BATCH_NBR_3,
exp_Trim.BATCH_TYPE_3 as BATCH_TYPE_3,
exp_Trim.out_BATCH_DATE_3 as BATCH_DATE_3,
exp_Trim.out_COUNTY_PAID_DT as COPAY_DATE,
exp_Trim.out_FED_SOURCE as FED_SOURCE,
exp_Trim.out_OUT_OF_STATE as OUT_OF_STATE,
exp_Trim.out_NAME_1 as NAME_1,
exp_Trim.out_NAME_2 as NAME_2,
exp_Trim.out_ADDRESS_1 as ADDRESS_1,
exp_Trim.out_ADDRESS_2 as ADDRESS_2,
exp_Trim.out_ALPHA_CODE as ALPHA_CODE,
exp_Trim.out_CITY as CITY,
exp_Trim.out_STATE as STATE,
exp_Trim.ZIP as ZIP_CODE,
exp_Trim.out_COUNTY_CHANGE_IND as COUNTY_CHG_IND,
exp_Trim.out_GW_BILL_REF as GW_BILL_REF,
exp_Trim.out_GW_DUE_DATE as GW_DUE_DATE,
exp_Trim.out_COMB_BILL_IND as COMB_BILL_IND,
exp_Trim.out_ALLOW_COMBINED_BILLING as ALLOW_COMBINED_BILLING,
exp_Trim.out_FOUR_DAY_LTR_IND as FOUR_DAY_LTR_IND
FROM
exp_Trim
);


-- Component STG_FED_MSTR_CUR, Type EXPORT_DATA Exporting data
;


END; ';