-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STG_FED_MSTR_DELTA("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
PMMAPPINGNAME varchar;
BEGIN 
PMMAPPINGNAME:=''abc'';
-- Component SQ_Shortcut_to_STG_FED_MSTR_CUR, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_CUR AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FED_TYPE_cur,
$2 as MEMBER_NBR_cur,
$3 as COMMODITY_cur,
$4 as COUNTY_cur,
$5 as STATUS_cur,
$6 as AMOUNT_PAID_cur,
$7 as PAID_TO_DATE_cur,
$8 as BATCH_NBR_1_cur,
$9 as BATCH_TYPE_1_cur,
$10 as BATCH_DATE_1_cur,
$11 as BATCH_NBR_2_cur,
$12 as BATCH_TYPE_2_cur,
$13 as BATCH_DATE_2_cur,
$14 as BATCH_NBR_3_cur,
$15 as BATCH_TYPE_3_cur,
$16 as BATCH_DATE_3_cur,
$17 as COPAY_DATE_cur,
$18 as FED_SOURCE_cur,
$19 as OUT_OF_STATE_cur,
$20 as NAME_1_cur,
$21 as NAME_2_cur,
$22 as ADDRESS_1_cur,
$23 as ADDRESS_2_cur,
$24 as ALPHA_CODE_cur,
$25 as CITY_cur,
$26 as STATE_cur,
$27 as ZIP_CODE_cur,
$28 as COUNTY_CHG_IND_cur,
$29 as GW_BILL_REF_cur,
$30 as GW_DUE_DATE_cur,
$31 as COMB_BILL_IND_cur,
$32 as ALLOW_COMBINED_BILLING_cur,
$33 as FOUR_DAY_LTR_IND_cur,
$34 as FED_TYPE_prv,
$35 as MEMBER_NBR_prv,
$36 as COMMODITY_prv,
$37 as COUNTY_prv,
$38 as STATUS_prv,
$39 as AMOUNT_PAID_prv,
$40 as PAID_TO_DATE_prv,
$41 as BATCH_NBR_1_prv,
$42 as BATCH_TYPE_1_prv,
$43 as BATCH_DATE_1_prv,
$44 as BATCH_NBR_2_prv,
$45 as BATCH_TYPE_2_prv,
$46 as BATCH_DATE_2_prv,
$47 as BATCH_NBR_3_prv,
$48 as BATCH_TYPE_3_prv,
$49 as BATCH_DATE_3_prv,
$50 as COPAY_DATE_prv,
$51 as FED_SOURCE_prv,
$52 as OUT_OF_STATE_prv,
$53 as NAME_1_prv,
$54 as NAME_2_prv,
$55 as ADDRESS_1_prv,
$56 as ADDRESS_2_prv,
$57 as ALPHA_CODE_prv,
$58 as CITY_prv,
$59 as STATE_prv,
$60 as ZIP_CODE_prv,
$61 as COUNTY_CHG_IND_prv,
$62 as GW_BILL_REF_prv,
$63 as GW_DUE_DATE_prv,
$64 as COMB_BILL_IND_prv,
$65 as ALLOW_COMBINED_BILLING_prv,
$66 as FOUR_DAY_LTR_IND_prv,
$67 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT coalesce(STG_FED_MSTR_CUR.FED_TYPE, '' ''), STG_FED_MSTR_CUR.MEMBER_NBR, coalesce(STG_FED_MSTR_CUR.COMMODITY, '' ''), STG_FED_MSTR_CUR.COUNTY, STG_FED_MSTR_CUR.STATUS, STG_FED_MSTR_CUR.AMOUNT_PAID, 

STG_FED_MSTR_CUR.PAID_TO_DATE, STG_FED_MSTR_CUR.BATCH_NBR_1, STG_FED_MSTR_CUR.BATCH_TYPE_1, STG_FED_MSTR_CUR.BATCH_DATE_1, STG_FED_MSTR_CUR.BATCH_NBR_2, STG_FED_MSTR_CUR.BATCH_TYPE_2, STG_FED_MSTR_CUR.BATCH_DATE_2, 

STG_FED_MSTR_CUR.BATCH_NBR_3, STG_FED_MSTR_CUR.BATCH_TYPE_3, STG_FED_MSTR_CUR.BATCH_DATE_3, STG_FED_MSTR_CUR.COPAY_DATE, coalesce(STG_FED_MSTR_CUR.FED_SOURCE, '' ''), coalesce(STG_FED_MSTR_CUR.OUT_OF_STATE, '' ''), 

coalesce(STG_FED_MSTR_CUR.NAME_1, '' ''), coalesce(STG_FED_MSTR_CUR.NAME_2, '' ''), coalesce(STG_FED_MSTR_CUR.ADDRESS_1, '' ''), coalesce(STG_FED_MSTR_CUR.ADDRESS_2, '' ''), coalesce(STG_FED_MSTR_CUR.ALPHA_CODE, '' ''), coalesce(STG_FED_MSTR_CUR.CITY, '' ''), 

coalesce(STG_FED_MSTR_CUR.STATE, '' ''), STG_FED_MSTR_CUR.ZIP_CODE, coalesce(STG_FED_MSTR_CUR.COUNTY_CHG_IND, '' ''), 

coalesce(STG_FED_MSTR_CUR.GW_BILL_REF, '' ''), STG_FED_MSTR_CUR.GW_DUE_DATE, coalesce(STG_FED_MSTR_CUR.COMB_BILL_IND, '' ''), STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING, coalesce(STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND, '' ''),

coalesce(STG_FED_MSTR_PRV.FED_TYPE, '' ''), STG_FED_MSTR_PRV.MEMBER_NBR, coalesce(STG_FED_MSTR_PRV.COMMODITY, '' ''), STG_FED_MSTR_PRV.COUNTY, 

STG_FED_MSTR_PRV.STATUS, STG_FED_MSTR_PRV.AMOUNT_PAID, STG_FED_MSTR_PRV.PAID_TO_DATE, STG_FED_MSTR_PRV.BATCH_NBR_1, STG_FED_MSTR_PRV.BATCH_TYPE_1, STG_FED_MSTR_PRV.BATCH_DATE_1, STG_FED_MSTR_PRV.BATCH_NBR_2, STG_FED_MSTR_PRV.BATCH_TYPE_2, 

STG_FED_MSTR_PRV.BATCH_DATE_2, STG_FED_MSTR_PRV.BATCH_NBR_3, STG_FED_MSTR_PRV.BATCH_TYPE_3, STG_FED_MSTR_PRV.BATCH_DATE_3, STG_FED_MSTR_PRV.COPAY_DATE, coalesce(STG_FED_MSTR_PRV.FED_SOURCE,'' ''), coalesce(STG_FED_MSTR_PRV.OUT_OF_STATE, '' ''), 

coalesce(STG_FED_MSTR_PRV.NAME_1, '' ''), coalesce(STG_FED_MSTR_PRV.NAME_2, '' ''), coalesce(STG_FED_MSTR_PRV.ADDRESS_1, '' ''), coalesce(STG_FED_MSTR_PRV.ADDRESS_2, '' ''), coalesce(STG_FED_MSTR_PRV.ALPHA_CODE, '' ''), coalesce(STG_FED_MSTR_PRV.CITY, '' ''), coalesce(STG_FED_MSTR_PRV.STATE, '' ''), 

STG_FED_MSTR_PRV.ZIP_CODE, coalesce(STG_FED_MSTR_PRV.COUNTY_CHG_IND, '' ''), 

coalesce(STG_FED_MSTR_PRV.GW_BILL_REF, '' ''), STG_FED_MSTR_PRV.GW_DUE_DATE, coalesce(STG_FED_MSTR_PRV.COMB_BILL_IND, '' ''), STG_FED_MSTR_PRV.ALLOW_COMBINED_BILLING, coalesce(STG_FED_MSTR_PRV.FOUR_DAY_LTR_IND, '' '')

FROM

DB_T_STAG_PROD.STG_FED_MSTR_CUR FULL OUTER JOIN DB_T_STAG_PROD.STG_FED_MSTR_PRV

ON STG_FED_MSTR_CUR.MEMBER_NBR = STG_FED_MSTR_PRV.MEMBER_NBR
) SRC
)
);


-- Component flt_DELTA, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_DELTA AS
(
SELECT
SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_TYPE_cur as FED_TYPE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_cur as MEMBER_NBR_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COMMODITY_cur as COMMODITY_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_cur as COUNTY_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.STATUS_cur as STATUS_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.AMOUNT_PAID_cur as AMOUNT_PAID_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.PAID_TO_DATE_cur as PAID_TO_DATE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_1_cur as BATCH_NBR_1_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_1_cur as BATCH_TYPE_1_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_1_cur as BATCH_DATE_1_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_2_cur as BATCH_NBR_2_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_2_cur as BATCH_TYPE_2_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_2_cur as BATCH_DATE_2_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_3_cur as BATCH_NBR_3_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_3_cur as BATCH_TYPE_3_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_3_cur as BATCH_DATE_3_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COPAY_DATE_cur as COPAY_DATE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_SOURCE_cur as FED_SOURCE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.OUT_OF_STATE_cur as OUT_OF_STATE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_1_cur as NAME_1_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_2_cur as NAME_2_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_1_cur as ADDRESS_1_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_2_cur as ADDRESS_2_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ALPHA_CODE_cur as ALPHA_CODE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.CITY_cur as CITY_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.STATE_cur as STATE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ZIP_CODE_cur as ZIP_CODE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_CHG_IND_cur as COUNTY_CHG_IND_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_cur as GW_BILL_REF_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_cur as GW_DUE_DATE_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_cur as COMB_BILL_IND_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_cur as ALLOW_COMBINED_BILLING_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_cur as FOUR_DAY_LTR_IND_cur,
SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_TYPE_prv as FED_TYPE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_prv as MEMBER_NBR_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COMMODITY_prv as COMMODITY_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_prv as COUNTY_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.STATUS_prv as STATUS_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.AMOUNT_PAID_prv as AMOUNT_PAID_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.PAID_TO_DATE_prv as PAID_TO_DATE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_1_prv as BATCH_NBR_1_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_1_prv as BATCH_TYPE_1_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_1_prv as BATCH_DATE_1_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_2_prv as BATCH_NBR_2_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_2_prv as BATCH_TYPE_2_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_2_prv as BATCH_DATE_2_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_3_prv as BATCH_NBR_3_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_3_prv as BATCH_TYPE_3_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_3_prv as BATCH_DATE_3_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COPAY_DATE_prv as COPAY_DATE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_SOURCE_prv as FED_SOURCE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.OUT_OF_STATE_prv as OUT_OF_STATE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_1_prv as NAME_1_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_2_prv as NAME_2_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_1_prv as ADDRESS_1_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_2_prv as ADDRESS_2_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ALPHA_CODE_prv as ALPHA_CODE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.CITY_prv as CITY_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.STATE_prv as STATE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ZIP_CODE_prv as ZIP_CODE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_CHG_IND_prv as COUNTY_CHG_IND_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_prv as GW_BILL_REF_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_prv as GW_DUE_DATE_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_prv as COMB_BILL_IND_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_prv as ALLOW_COMBINED_BILLING_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_prv as FOUR_DAY_LTR_IND_prv,
SQ_Shortcut_to_STG_FED_MSTR_CUR.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_CUR
WHERE CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_cur IS NULL or SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_prv IS NULL or SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.MEMBER_NBR_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_TYPE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_TYPE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.COMMODITY_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.COMMODITY_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.STATUS_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.STATUS_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.AMOUNT_PAID_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.AMOUNT_PAID_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.PAID_TO_DATE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.PAID_TO_DATE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_1_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_1_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_1_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_1_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_1_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_1_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_2_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_2_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_2_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_2_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_2_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_2_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_3_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_NBR_3_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_3_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_TYPE_3_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_3_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.BATCH_DATE_3_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.COPAY_DATE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.COPAY_DATE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_SOURCE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.FED_SOURCE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.OUT_OF_STATE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.OUT_OF_STATE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_1_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_1_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_2_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.NAME_2_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_1_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_1_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_2_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.ADDRESS_2_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.ALPHA_CODE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.ALPHA_CODE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.CITY_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.CITY_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.STATE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.STATE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.ZIP_CODE_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.ZIP_CODE_prv or SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_CHG_IND_cur != SQ_Shortcut_to_STG_FED_MSTR_CUR.COUNTY_CHG_IND_prv or ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_cur IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_cur END ) <> ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_prv IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_BILL_REF_prv END ) or ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_cur IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_cur END ) <> ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_prv IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.GW_DUE_DATE_prv END ) or ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_cur IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_cur END ) <> ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_prv IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.COMB_BILL_IND_prv END ) or ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_cur IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_cur END ) <> ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_prv IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.ALLOW_COMBINED_BILLING_prv END ) or ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_cur IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_cur END ) <> ( CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_prv IS NULL THEN ''~'' ELSE SQ_Shortcut_to_STG_FED_MSTR_CUR.FOUR_DAY_LTR_IND_prv END ) THEN TRUE ELSE FALSE END
);


-- Component exp_DELTA, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_DELTA AS
(
SELECT
CASE WHEN flt_DELTA.MEMBER_NBR_cur IS NULL THEN flt_DELTA.MEMBER_NBR_prv ELSE flt_DELTA.MEMBER_NBR_cur END as out_MEMBER_NBR,
flt_DELTA.FED_TYPE_cur as FED_TYPE_cur,
flt_DELTA.COMMODITY_cur as COMMODITY_cur,
flt_DELTA.COUNTY_cur as COUNTY_cur,
flt_DELTA.STATUS_cur as STATUS_cur,
flt_DELTA.AMOUNT_PAID_cur as AMOUNT_PAID_cur,
substr ( flt_DELTA.PAID_TO_DATE_cur , 1 , 4 ) as out_PAID_TO_DATE_cur_YYYY,
substr ( flt_DELTA.PAID_TO_DATE_cur , 1 , 4 ) as YYYY,
substr ( flt_DELTA.PAID_TO_DATE_cur , 5 , 2 ) as out_PAID_TO_DATE_cur_MM,
substr ( flt_DELTA.PAID_TO_DATE_cur , 7 , 2 ) as out_PAID_TO_DATE_cur_DD,
substr ( flt_DELTA.PAID_TO_DATE_cur , 5 , 2 ) as MM,
substr ( flt_DELTA.PAID_TO_DATE_cur , 7 , 2 ) as DD,
flt_DELTA.BATCH_NBR_1_cur as BATCH_NBR_1_cur,
flt_DELTA.BATCH_TYPE_1_cur as BATCH_TYPE_1_cur,
flt_DELTA.BATCH_DATE_1_cur as BATCH_DATE_1_cur,
flt_DELTA.BATCH_NBR_2_cur as BATCH_NBR_2_cur,
flt_DELTA.BATCH_TYPE_2_cur as BATCH_TYPE_2_cur,
flt_DELTA.BATCH_DATE_2_cur as BATCH_DATE_2_cur,
flt_DELTA.BATCH_NBR_3_cur as BATCH_NBR_3_cur,
flt_DELTA.BATCH_TYPE_3_cur as BATCH_TYPE_3_cur,
flt_DELTA.BATCH_DATE_3_cur as BATCH_DATE_3_cur,
substr ( flt_DELTA.COPAY_DATE_cur , 1 , 4 ) as out_COPAY_DATE_cur_YYYY,
substr ( flt_DELTA.COPAY_DATE_cur , 5 , 2 ) as out_COPAY_DATE_cur_MM,
substr ( flt_DELTA.COPAY_DATE_cur , 7 , 2 ) as out_COPAY_DATE_cur_DD,
CASE WHEN ltrim ( rtrim ( flt_DELTA.FED_SOURCE_cur ) ) IS NULL THEN null ELSE ltrim ( rtrim ( flt_DELTA.FED_SOURCE_cur ) ) END as out_FED_SOURCE_cur,
CASE WHEN ltrim ( rtrim ( flt_DELTA.OUT_OF_STATE_cur ) ) IS NULL THEN null ELSE ltrim ( rtrim ( flt_DELTA.OUT_OF_STATE_cur ) ) END as out_OUT_OF_STATE_cur,
flt_DELTA.NAME_1_cur as NAME_1_cur,
flt_DELTA.NAME_2_cur as NAME_2_cur,
flt_DELTA.ADDRESS_1_cur as ADDRESS_1_cur,
flt_DELTA.ADDRESS_2_cur as ADDRESS_2_cur,
flt_DELTA.ALPHA_CODE_cur as ALPHA_CODE_cur,
flt_DELTA.CITY_cur as CITY_cur,
flt_DELTA.STATE_cur as STATE_cur,
flt_DELTA.ZIP_CODE_cur as ZIP_CODE_cur,
CASE WHEN ltrim ( rtrim ( flt_DELTA.COUNTY_CHG_IND_cur ) ) IS NULL THEN null ELSE ltrim ( rtrim ( flt_DELTA.COUNTY_CHG_IND_cur ) ) END as out_COUNTY_CHG_IND_cur,
CASE WHEN flt_DELTA.MEMBER_NBR_cur IS NULL THEN ''DEL'' ELSE CASE WHEN flt_DELTA.MEMBER_NBR_prv IS NULL THEN ''NEW'' ELSE ''UPD'' END END as out_TX_CD,
1 as out_JOIN_TRG,
flt_DELTA.GW_BILL_REF_cur as GW_BILL_REF_cur,
CASE WHEN ltrim ( rtrim ( flt_DELTA.GW_DUE_DATE_cur ) ) IS NULL THEN null ELSE ltrim ( rtrim ( flt_DELTA.GW_DUE_DATE_cur ) ) END as out_GW_DUE_DATE_cur,
flt_DELTA.COMB_BILL_IND_cur as COMB_BILL_IND_cur,
flt_DELTA.ALLOW_COMBINED_BILLING_cur as ALLOW_COMBINED_BILLING_cur,
flt_DELTA.FOUR_DAY_LTR_IND_cur as FOUR_DAY_LTR_IND_cur,
flt_DELTA.source_record_id
FROM
flt_DELTA
);


-- Component M_VALIDATE_COPAY_DATE, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_validate_date, mapplet instance M_VALIDATE_COPAY_DATE;
call public.m_validate_date(''exp_DELTA'');

-- Component LKP_TRG, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TRG AS
(
SELECT
LKP.FEED_IND,
LKP.FEED_DT,
exp_DELTA.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_DELTA.source_record_id ORDER BY LKP.TO_JOIN asc,LKP.FEED_IND asc,LKP.FEED_DT asc,LKP.REC_COUNT asc,LKP.DOLLAR_AMT asc) RNK
FROM
exp_DELTA
LEFT JOIN (
SELECT 
1 as "TO_JOIN" ,
STG_FED_MSTR_TRG.FEED_IND as FEED_IND, 
STG_FED_MSTR_TRG.FEED_DT as FEED_DT, 
STG_FED_MSTR_TRG.REC_COUNT as REC_COUNT, 
STG_FED_MSTR_TRG.DOLLAR_AMT as DOLLAR_AMT
FROM DB_T_STAG_PROD.STG_FED_MSTR_TRG
) LKP ON LKP.TO_JOIN = exp_DELTA.out_JOIN_TRG
QUALIFY RNK = 1
);


-- Component flt_Validate_Status_Type_Comm_State_County, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_Validate_Status_Type_Comm_State_County AS
(
SELECT
exp_DELTA.out_MEMBER_NBR as out_MEMBER_NBR,
exp_DELTA.FED_TYPE_cur as FED_TYPE_cur,
exp_DELTA.COMMODITY_cur as COMMODITY_cur,
exp_DELTA.COUNTY_cur as COUNTY_cur,
exp_DELTA.STATUS_cur as STATUS_cur,
exp_DELTA.out_TX_CD as out_TX_CD,
exp_DELTA.source_record_id
FROM
exp_DELTA
WHERE exp_DELTA.out_TX_CD != ''DEL''
);


-- Component exp_Validate, Type EXPRESSION 
CREATE
OR REPLACE TEMPORARY TABLE exp_validate AS
(SELECT
  flt_Validate_Status_Type_Comm_State_County.out_MEMBER_NBR,
  flt_Validate_Status_Type_Comm_State_County.FED_TYPE_cur,
  TRIM(
    SUBSTRING(
      flt_Validate_Status_Type_Comm_State_County.COMMODITY_cur,
      1,
      1
    )
  ) AS COMM1,
  TRIM(
    SUBSTRING(
      flt_Validate_Status_Type_Comm_State_County.COMMODITY_cur,
      2,
      1
    )
  ) AS COMM2,
  TRIM(
    SUBSTRING(
      flt_Validate_Status_Type_Comm_State_County.COMMODITY_cur,
      3,
      1
    )
  ) AS COMM3,
  flt_Validate_Status_Type_Comm_State_County.COUNTY_cur,
  flt_Validate_Status_Type_Comm_State_County.STATUS_cur,
  4 AS out_PROJ_ID,
  ''Y'' AS out_BATCH_IND,
  :PMMappingName AS out_PRGM_NM,
  ''FED_MSTR.TXT'' AS out_TABLE_NM,
  ''AL'' AS out_STATE,
  flt_Validate_Status_Type_Comm_State_County.source_record_id
FROM
  flt_Validate_Status_Type_Comm_State_County);

-- Component LKP_MEMBER_STATUS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MEMBER_STATUS AS
(
SELECT
LKP.STATUS_CD,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.STATUS_CD asc) RNK
FROM
exp_Validate
LEFT JOIN (
SELECT
STATUS_CD
FROM DB_T_SHRD_PROD.MEMBER_STATUS
) LKP ON LKP.STATUS_CD = exp_Validate.STATUS_cur
QUALIFY RNK = 1
);


-- Component exp_DATE_BILLED_PAID_CANCELLED, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_DATE_BILLED_PAID_CANCELLED AS
(
SELECT
lpad ( to_char ( DATE_PART(''YYYY'', TO_TIMESTAMP(LKP_TRG.FEED_DT)) ) , 4 , ''0'' ) || lpad ( to_char ( DATE_PART(''MM'', TO_TIMESTAMP(LKP_TRG.FEED_DT)) ) , 2 , ''0'' ) || lpad ( to_char ( DATE_PART(''DD'', TO_TIMESTAMP(LKP_TRG.FEED_DT)) ) , 2 , ''0'' ) as FEED_DT_S,
CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_1_cur = 888 THEN exp_DELTA.BATCH_DATE_1_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_2_cur = 888 THEN exp_DELTA.BATCH_DATE_2_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_3_cur = 888 THEN exp_DELTA.BATCH_DATE_3_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_1_cur = 900 THEN exp_DELTA.BATCH_DATE_1_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_2_cur = 900 THEN exp_DELTA.BATCH_DATE_2_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_3_cur = 900 THEN exp_DELTA.BATCH_DATE_3_cur ELSE CASE WHEN LKP_TRG.FEED_IND = ''D'' THEN FEED_DT_S ELSE null END END END END END END END as out_DATE_BILLED,
CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_1_cur = 999 THEN exp_DELTA.BATCH_DATE_1_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_2_cur = 999 THEN exp_DELTA.BATCH_DATE_2_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and exp_DELTA.BATCH_NBR_3_cur = 999 THEN exp_DELTA.BATCH_DATE_3_cur ELSE CASE WHEN LKP_TRG.FEED_IND = ''D'' THEN FEED_DT_S ELSE null END END END END as out_DATE_CANCELLED,
CASE WHEN LKP_TRG.FEED_IND != ''D'' and ( exp_DELTA.BATCH_TYPE_1_cur = 19 or exp_DELTA.BATCH_TYPE_1_cur = 20 or exp_DELTA.BATCH_TYPE_1_cur = 21 or exp_DELTA.BATCH_TYPE_1_cur = 28 or exp_DELTA.BATCH_TYPE_1_cur = 29 or exp_DELTA.BATCH_TYPE_1_cur = 30 or exp_DELTA.BATCH_TYPE_1_cur = 98 ) THEN exp_DELTA.BATCH_DATE_1_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and ( exp_DELTA.BATCH_TYPE_2_cur = 19 or exp_DELTA.BATCH_TYPE_2_cur = 20 or exp_DELTA.BATCH_TYPE_2_cur = 21 or exp_DELTA.BATCH_TYPE_2_cur = 28 or exp_DELTA.BATCH_TYPE_2_cur = 29 or exp_DELTA.BATCH_TYPE_2_cur = 30 or exp_DELTA.BATCH_TYPE_2_cur = 98 ) THEN exp_DELTA.BATCH_DATE_2_cur ELSE CASE WHEN LKP_TRG.FEED_IND != ''D'' and ( exp_DELTA.BATCH_TYPE_3_cur = 19 or exp_DELTA.BATCH_TYPE_3_cur = 20 or exp_DELTA.BATCH_TYPE_3_cur = 21 or exp_DELTA.BATCH_TYPE_3_cur = 28 or exp_DELTA.BATCH_TYPE_3_cur = 29 or exp_DELTA.BATCH_TYPE_3_cur = 30 or exp_DELTA.BATCH_TYPE_3_cur = 98 ) THEN exp_DELTA.BATCH_DATE_3_cur ELSE CASE WHEN LKP_TRG.FEED_IND = ''D'' THEN FEED_DT_S ELSE null END END END END as out_DATE_PAID,
exp_DELTA.source_record_id
FROM
exp_DELTA
INNER JOIN LKP_TRG ON exp_DELTA.source_record_id = LKP_TRG.source_record_id
);


-- Component m_validate_paid_to_date, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_validate_date, mapplet instance m_validate_paid_to_date;
call m_validate_date(''exp_DELTA'');

-- Component LKP_COMMODITY2, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_COMMODITY2 AS
(
SELECT
LKP.COMM_CD,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.COMM_CD asc) RNK
FROM
exp_Validate
left join flt_Validate_Status_Type_Comm_State_County a on  exp_Validate.source_record_id=a.source_record_id
LEFT JOIN (
SELECT
COMM_CD
FROM DB_T_SHRD_PROD.COMMODITY
) LKP ON LKP.COMM_CD = SUBSTRING(flt_Validate_Status_Type_Comm_State_County.COMMODITY_cur,3,1)
--exp_Validate.COMM2
QUALIFY RNK = 1
);


  

-- Component Shortcut_to_STG_FED_MSTR_DELTA, Type TARGET 
INSERT INTO DB_T_STAG_PROD.STG_FED_MSTR_DELTA
(
FED_TYPE,
MEMBER_NBR,
COMMODITY,
COUNTY,
STATUS,
AMOUNT_PAID,
PAID_TO_DATE,
BATCH_NBR_1,
BATCH_TYPE_1,
BATCH_DATE_1,
BATCH_NBR_2,
BATCH_TYPE_2,
BATCH_DATE_2,
BATCH_NBR_3,
BATCH_TYPE_3,
BATCH_DATE_3,
COPAY_DATE,
FED_SOURCE,
OUT_OF_STATE,
NAME_1,
NAME_2,
ADDRESS_1,
ADDRESS_2,
ALPHA_CODE,
CITY,
STATE,
ZIP_CODE,
COUNTY_CHG_IND,
TX_CD,
DATE_BILLED,
DATE_PAID,
DATE_CANCELLED,
GW_BILL_REF,
GW_DUE_DATE,
COMB_BILL_IND,
ALLOW_COMBINED_BILLING,
FOUR_DAY_LTR_IND
)
SELECT
exp_DELTA.FED_TYPE_cur as FED_TYPE,
exp_DELTA.out_MEMBER_NBR as MEMBER_NBR,
exp_DELTA.COMMODITY_cur as COMMODITY,
exp_DELTA.COUNTY_cur as COUNTY,
exp_DELTA.STATUS_cur as STATUS,
exp_DELTA.AMOUNT_PAID_cur as AMOUNT_PAID,
m_validate_paid_to_date.out_DATE_S as PAID_TO_DATE,
exp_DELTA.BATCH_NBR_1_cur as BATCH_NBR_1,
exp_DELTA.BATCH_TYPE_1_cur as BATCH_TYPE_1,
exp_DELTA.BATCH_DATE_1_cur as BATCH_DATE_1,
exp_DELTA.BATCH_NBR_2_cur as BATCH_NBR_2,
exp_DELTA.BATCH_TYPE_2_cur as BATCH_TYPE_2,
exp_DELTA.BATCH_DATE_2_cur as BATCH_DATE_2,
exp_DELTA.BATCH_NBR_3_cur as BATCH_NBR_3,
exp_DELTA.BATCH_TYPE_3_cur as BATCH_TYPE_3,
exp_DELTA.BATCH_DATE_3_cur as BATCH_DATE_3,
m_validate_date.out_DATE_S as COPAY_DATE,
exp_DELTA.out_FED_SOURCE_cur as FED_SOURCE,
exp_DELTA.out_OUT_OF_STATE_cur as OUT_OF_STATE,
exp_DELTA.NAME_1_cur as NAME_1,
exp_DELTA.NAME_2_cur as NAME_2,
exp_DELTA.ADDRESS_1_cur as ADDRESS_1,
exp_DELTA.ADDRESS_2_cur as ADDRESS_2,
exp_DELTA.ALPHA_CODE_cur as ALPHA_CODE,
exp_DELTA.CITY_cur as CITY,
exp_DELTA.STATE_cur as STATE,
exp_DELTA.ZIP_CODE_cur as ZIP_CODE,
exp_DELTA.out_COUNTY_CHG_IND_cur as COUNTY_CHG_IND,
exp_DELTA.out_TX_CD as TX_CD,
exp_DATE_BILLED_PAID_CANCELLED.out_DATE_BILLED as DATE_BILLED,
exp_DATE_BILLED_PAID_CANCELLED.out_DATE_PAID as DATE_PAID,
exp_DATE_BILLED_PAID_CANCELLED.out_DATE_CANCELLED as DATE_CANCELLED,
exp_DELTA.GW_BILL_REF_cur as GW_BILL_REF,
exp_DELTA.out_GW_DUE_DATE_cur as GW_DUE_DATE,
exp_DELTA.COMB_BILL_IND_cur as COMB_BILL_IND,
exp_DELTA.ALLOW_COMBINED_BILLING_cur as ALLOW_COMBINED_BILLING,
exp_DELTA.FOUR_DAY_LTR_IND_cur as FOUR_DAY_LTR_IND
FROM
exp_DELTA
INNER JOIN m_validate_date ON exp_DELTA.source_record_id = m_validate_date.source_record_id
INNER JOIN exp_DATE_BILLED_PAID_CANCELLED ON m_validate_date.source_record_id = exp_DATE_BILLED_PAID_CANCELLED.source_record_id
INNER JOIN m_validate_date m_validate_paid_to_date ON exp_DATE_BILLED_PAID_CANCELLED.source_record_id = m_validate_paid_to_date.source_record_id;


-- Component m_lkp_Control_Tables, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_lkp_Control_Tables, mapplet instance m_lkp_Control_Tables;
call m_lkp_Control_Tables(''exp_DELTA'');

-- Component LKP_MEMBER_TYPE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MEMBER_TYPE AS
(
SELECT
LKP.MEMB_TYPE,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.MEMB_TYPE asc) RNK
FROM
exp_Validate
LEFT JOIN (
SELECT
MEMB_TYPE
FROM MEMBER_TYPE
) LKP ON LKP.MEMB_TYPE = exp_Validate.FED_TYPE_cur
QUALIFY RNK = 1
);


-- Component LKP_COMMODITY1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_COMMODITY1 AS
(
SELECT
LKP.COMM_CD,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.COMM_CD asc) RNK
FROM
exp_Validate
LEFT JOIN (
SELECT
COMM_CD
FROM DB_T_SHRD_PROD.COMMODITY
) LKP ON LKP.COMM_CD = exp_Validate.COMM1
QUALIFY RNK = 1
);


-- Component LKP_GEO_COUNTY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_GEO_COUNTY AS
(
SELECT
LKP.COUNTY_CD,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.ST_CD asc,LKP.COUNTY_CD asc,LKP.COUNTY_NM asc) RNK
FROM
exp_Validate
LEFT JOIN (
SELECT
ST_CD,
COUNTY_CD,
COUNTY_NM
FROM GEO_COUNTY
) LKP ON LKP.ST_CD = exp_Validate.out_STATE AND LKP.COUNTY_CD = exp_Validate.COUNTY_cur
QUALIFY RNK = 1
);


-- Component LKP_COMMODITY3, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_COMMODITY3 AS
(
SELECT
LKP.COMM_CD,
exp_Validate.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Validate.source_record_id ORDER BY LKP.COMM_CD asc) RNK
FROM
exp_Validate
LEFT JOIN (
SELECT
COMM_CD
FROM DB_T_SHRD_PROD.COMMODITY
) LKP ON LKP.COMM_CD = exp_Validate.COMM3
QUALIFY RNK = 1
);


-- Component exp_LOOKUP, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_LOOKUP AS
(
SELECT
exp_Validate.out_MEMBER_NBR as out_MEMBER_NBR,
exp_Validate.STATUS_cur as STATUS_cur,
CASE WHEN LKP_MEMBER_STATUS.STATUS_CD = exp_Validate.STATUS_cur THEN ''Y'' ELSE ''N'' END as out_Status_Valid,
''STATUS'' as out_ERR_COL_NM_1,
exp_Validate.STATUS_cur as out_ERR_COL_VAL_1,
''Invalid Status Code ('' || to_char ( exp_Validate.STATUS_cur ) || '') for Member Nbr: '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ERR_COL_DESC_1,
exp_Validate.FED_TYPE_cur as FED_TYPE_cur,
CASE WHEN LKP_MEMBER_TYPE.MEMB_TYPE = exp_Validate.FED_TYPE_cur THEN ''Y'' ELSE ''N'' END as out_Type_Valid,
''MEMBER TYPE'' as out_ERR_COL_NM_2,
exp_Validate.FED_TYPE_cur as out_ERR_COL_VAL_2,
''Invalid Member Type ('' || exp_Validate.FED_TYPE_cur || '') for Member Nbr : '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ERR_COL_DESC_2,
exp_Validate.COMM1 as COMM1,
CASE WHEN LKP_COMMODITY1.COMM_CD = exp_Validate.COMM1 or ltrim ( rtrim ( exp_Validate.COMM1 ) ) = '''' or ltrim ( rtrim ( exp_Validate.COMM1 ) ) IS NULL THEN ''Y'' ELSE ''N'' END as out_COMM1_Valid,
''COMMODITY'' as out_ERR_COL_NM_4,
exp_Validate.COMM1 as out_ERR_COL_VAL_4,
''Invalid Commodity Code1 ('' || exp_Validate.COMM1 || '') for Member Nbr : '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ERR_COL_DESC_4,
exp_Validate.COMM2 as COMM2,
CASE WHEN exp_Validate.COMM2 = LKP_COMMODITY2.COMM_CD or ltrim ( rtrim ( exp_Validate.COMM2 ) ) = '''' or ltrim ( rtrim ( exp_Validate.COMM2 ) ) IS NULL THEN ''Y'' ELSE ''N'' END as out_COMM2_VALID,
''COMMODITY'' as out_ERR_COL_NM_5,
exp_Validate.COMM2 as out_ERR_COL_VAL_5,
''Invalid Commodity Code2 ('' || exp_Validate.COMM2 || '') for Member Nbr : '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ERR_COL_DESC_5,
exp_Validate.COMM3 as COMM3,
CASE WHEN LKP_COMMODITY3.COMM_CD = exp_Validate.COMM3 or ltrim ( rtrim ( exp_Validate.COMM3 ) ) = '''' or ltrim ( rtrim ( exp_Validate.COMM3 ) ) IS NULL THEN ''Y'' ELSE ''N'' END as out_COMM3_Valid,
''COMMODITY'' as out_ERR_COL_NM_6,
exp_Validate.COMM3 as out_ERR_COL_VAL_6,
''Invalid Commodity Code3 ('' || exp_Validate.COMM3 || '') for Member Nbr : '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ERR_COL_DESC_6,
exp_Validate.COUNTY_cur as COUNTY_cur,
CASE WHEN exp_Validate.COUNTY_cur = 68 THEN ''Y'' ELSE CASE WHEN LKP_GEO_COUNTY.COUNTY_CD = to_char ( exp_Validate.COUNTY_cur ) THEN ''Y'' ELSE ''N'' END END as out_County_Valid,
''COUNTY'' as out_ERR_COL_NM_7,
exp_Validate.COUNTY_cur as out_ERR_COL_VAL_7,
''Invalid County Code ('' || to_char ( exp_Validate.COUNTY_cur ) || '') for Member Nbr : '' || to_char ( exp_Validate.out_MEMBER_NBR ) as out_ETT_COL_DESC_7,
m_lkp_Control_Tables.mplt_BATCH_ID as mplt_BATCH_ID,
m_lkp_Control_Tables.mplt_PRGM_ID as mplt_PRGM_ID,
m_lkp_Control_Tables.mplt_TABLE_ID as mplt_TABLE_ID,
m_lkp_Control_Tables.mplt_PROJ_ID as mplt_PROJ_ID,
NULL as out_Temp1,
NULL as out_Temp2,
NULL as out_Temp3,
''Y'' as out_Temp4,
exp_Validate.source_record_id
FROM
exp_Validate
INNER JOIN LKP_MEMBER_STATUS ON exp_Validate.source_record_id = LKP_MEMBER_STATUS.source_record_id
INNER JOIN LKP_COMMODITY2 ON LKP_MEMBER_STATUS.source_record_id = LKP_COMMODITY2.source_record_id
INNER JOIN m_lkp_Control_Tables ON LKP_COMMODITY2.source_record_id = m_lkp_Control_Tables.source_record_id
INNER JOIN LKP_MEMBER_TYPE ON m_lkp_Control_Tables.source_record_id = LKP_MEMBER_TYPE.source_record_id
INNER JOIN LKP_COMMODITY1 ON LKP_MEMBER_TYPE.source_record_id = LKP_COMMODITY1.source_record_id
INNER JOIN LKP_GEO_COUNTY ON LKP_COMMODITY1.source_record_id = LKP_GEO_COUNTY.source_record_id
INNER JOIN LKP_COMMODITY3 ON LKP_GEO_COUNTY.source_record_id = LKP_COMMODITY3.source_record_id
);


CREATE OR REPLACE TEMPORARY TABLE nrm_Errors AS
SELECT
  mplt_BATCH_ID AS BATCH_ID,
  mplt_PROJ_ID AS PROJ_ID,
  mplt_PRGM_ID AS PRGM_ID,
  mplt_TABLE_ID AS SRC_TABLE_ID,
  out_MEMBER_NBR AS SRC_KEY_VAL,
  ''REC1'' AS REC_NO,
  COL_VALID_in1 AS COL_VALID,
  ERR_COL_NM_in1 AS ERR_COL_NM,
  ERR_COL_VAL_in1 AS ERR_COL_VAL,
  ERR_DESC_in1 AS ERR_DESC
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC2'',
  COL_VALID_in2,
  ERR_COL_NM_in2,
  ERR_COL_VAL_in2,
  ERR_DESC_in2
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC3'',
  COL_VALID_in3,
  ERR_COL_NM_in3,
  ERR_COL_VAL_in3,
  ERR_DESC_in3
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC4'',
  COL_VALID_in4,
  ERR_COL_NM_in4,
  ERR_COL_VAL_in4,
  ERR_DESC_in4
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC5'',
  COL_VALID_in5,
  ERR_COL_NM_in5,
  ERR_COL_VAL_in5,
  ERR_DESC_in5
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC6'',
  COL_VALID_in6,
  ERR_COL_NM_in6,
  ERR_COL_VAL_in6,
  ERR_DESC_in6
FROM exp_LOOKUP

UNION ALL
SELECT
  mplt_BATCH_ID,
  mplt_PROJ_ID,
  mplt_PRGM_ID,
  mplt_TABLE_ID,
  out_MEMBER_NBR,
  ''REC7'',
  COL_VALID_in7,
  ERR_COL_NM_in7,
  ERR_COL_VAL_in7,
  ERR_DESC_in7
FROM exp_LOOKUP;

-- Component flt_Errors, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_Errors AS
(
SELECT
nrm_Errors.BATCH_ID as BATCH_ID,
nrm_Errors.PROJ_ID as PROJ_ID,
nrm_Errors.PRGM_ID as PRGM_ID,
nrm_Errors.SRC_TABLE_ID as SRC_TABLE_ID,
nrm_Errors.SRC_KEY_VAL as SRC_KEY_VAL,
nrm_Errors.ERR_COL_NM as ERR_COL_NM,
nrm_Errors.ERR_COL_VAL as ERR_COL_VAL,
nrm_Errors.ERR_DESC as ERR_DESC,
nrm_Errors.COL_VALID as COL_VALID,
nrm_Errors.source_record_id
FROM
nrm_Errors
WHERE nrm_Errors.COL_VALID != ''Y''
);


-- Component exp_Errors, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Errors AS
(
SELECT
flt_Errors.BATCH_ID as BATCH_ID,
flt_Errors.PROJ_ID as PROJ_ID,
flt_Errors.PRGM_ID as PRGM_ID,
flt_Errors.SRC_TABLE_ID as SRC_TABLE_ID,
flt_Errors.SRC_KEY_VAL as SRC_KEY_VAL,
flt_Errors.ERR_COL_NM as ERR_COL_NM,
flt_Errors.ERR_COL_VAL as ERR_COL_VAL,
flt_Errors.ERR_DESC as ERR_DESC,
CURRENT_TIMESTAMP as out_INS_TS,
flt_Errors.source_record_id
FROM
flt_Errors
);


-- Component Shortcut_to_ECTL_ERR_LOG, Type TARGET 
INSERT INTO DB_T_CTRL_FIN_PROD.ECTL_ERR_LOG
(
ECTL_ERR_ID,
ECTL_BATCH_ID,
ECTL_PROJ_ID,
ECTL_PRGM_ID,
ECTL_SRC_TABLE_ID,
ECTL_SRC_KEY_VAL,
ERR_COL_NM,
ERR_COL_VAL,
ERR_DESC,
ERR_TS
)
SELECT
row_number() over (order by 1) as ECTL_ERR_ID,
exp_Errors.BATCH_ID as ECTL_BATCH_ID,
exp_Errors.PROJ_ID as ECTL_PROJ_ID,
exp_Errors.PRGM_ID as ECTL_PRGM_ID,
exp_Errors.SRC_TABLE_ID as ECTL_SRC_TABLE_ID,
exp_Errors.SRC_KEY_VAL as ECTL_SRC_KEY_VAL,
exp_Errors.ERR_COL_NM as ERR_COL_NM,
exp_Errors.ERR_COL_VAL as ERR_COL_VAL,
exp_Errors.ERR_DESC as ERR_DESC,
exp_Errors.out_INS_TS as ERR_TS
FROM
exp_Errors;


END; ';