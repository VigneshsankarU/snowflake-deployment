-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_WRK_FOP_BORDEREAU_EXTRACT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
 DECLARE
run_id STRING;
prcs_id int;
CAL_START_DT date;
CAL_END_DT date;
CAL_END_MTH_ID string;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
CAL_START_DT :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_START_DT'' order by insert_ts desc limit 1);
CAL_END_DT :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_DT'' order by insert_ts desc limit 1);
CAL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_MTH_ID'' order by insert_ts desc limit 1);
  

-- PIPELINE START FOR 2

-- Component SQ_FOP_BORDEREAU1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FOP_BORDEREAU1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TRANS_ID,
$2 as TRANS_PREM_GROSS,
$3 as TRANS_PREM_NET,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	DISTINCT TRANS_ID,TRANS_PREM_GROSS, TRANS_PREM_NET

FROM DB_T_PROD_WRK.FOP_BORDEREAU

WHERE CASE WHEN CAST(TRANS_ENTRY_DT AS DATE )  > CAST          (TRANS_EFF_DT AS DATE) 

	THEN CAST(TRANS_ENTRY_DT AS DATE) ELSE CAST(TRANS_EFF_DT AS DATE) END >=  :CAL_START_DT

AND CASE WHEN CAST(TRANS_ENTRY_DT AS DATE)  > CAST(TRANS_EFF_DT AS DATE) 

	THEN CAST(TRANS_ENTRY_DT AS DATE) ELSE CAST(TRANS_EFF_DT AS DATE) END <=  :CAL_END_DT
) SRC
)
);


-- PIPELINE START FOR 1

-- Component SQ_FOP_BORDEREAU, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FOP_BORDEREAU AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TRANS_ID,
$2 as POL_NBR,
$3 as TRANS_TYPE,
$4 as TRANS_EFF_DT,
$5 as POL_EFF_DT,
$6 as POL_TERM_EXP_DT,
$7 as TRANS_PREM_GROSS,
$8 as TRANS_PREM_NET,
$9 as NAMED_INSRD,
$10 as UW_CMPNY,
$11 as POL_RSK_ST,
$12 as POL_TYPE_DESC,
$13 as TOT_PROP_PREM,
$14 as LOB_CD,
$15 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	DISTINCT TRANS_ID, POL_NBR, TRANS_TYPE, TRANS_EFF_DT,POL_EFF_DT, POL_TERM_EXP_DT, TRANS_PREM_GROSS,

TRANS_PREM_NET, NAMED_INSRD, UW_CMPNY,POL_RSK_ST, POL_TYPE_DESC, TOT_PROP_PREM,LOB_CD

FROM DB_T_PROD_WRK.FOP_BORDEREAU

WHERE CASE WHEN CAST(TRANS_ENTRY_DT AS DATE )  > CAST(TRANS_EFF_DT AS DATE)     

	THEN CAST(TRANS_ENTRY_DT AS DATE) ELSE CAST(TRANS_EFF_DT AS DATE) END >=  :CAL_START_DT

AND CASE WHEN CAST(TRANS_ENTRY_DT AS DATE)  > CAST(TRANS_EFF_DT AS DATE) 

	THEN CAST(TRANS_ENTRY_DT AS DATE) ELSE CAST(TRANS_EFF_DT AS DATE)END  <=  :CAL_END_DT
) SRC
)
);


-- Component exp_fop_agg_bordereau, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_fop_agg_bordereau AS
(
SELECT
SQ_FOP_BORDEREAU1.TRANS_ID as TRANS_ID,
SQ_FOP_BORDEREAU1.TRANS_PREM_GROSS as TRANS_PREM_GROSS,
SQ_FOP_BORDEREAU1.TRANS_PREM_NET as TRANS_PREM_NET,
SQ_FOP_BORDEREAU1.source_record_id
FROM
SQ_FOP_BORDEREAU1
);


-- Component agg_count_TRANS_ID, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE agg_count_TRANS_ID AS
(
SELECT
MIN(exp_fop_agg_bordereau.TRANS_ID) as TRANS_ID,
count(TRANS_ID) as COUNTVAL,
MIN(exp_fop_agg_bordereau.TRANS_PREM_GROSS) as TRANS_PREM_GROSS,
MIN(exp_fop_agg_bordereau.TRANS_PREM_NET) as TRANS_PREM_NET,
sum(TRANS_PREM_GROSS) as SUM_TRANS_PREM_GROSS,
sum(TRANS_PREM_NET) as SUM_TRANS_PREM_NET,
MIN(exp_fop_agg_bordereau.source_record_id) as source_record_id
FROM
exp_fop_agg_bordereau
);


-- Component exp_fop_bordereau_pass_thru, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_fop_bordereau_pass_thru AS
(
SELECT
'' '' as var_SPACES,
''EBF'' as out_PROD_CD,
''5519'' as out_PROD_ID,
RPAD ( SQ_FOP_BORDEREAU.POL_NBR , 20 , '' '' ) as out_POL_NBR,
RPAD ( SQ_FOP_BORDEREAU.TRANS_TYPE , 2 , '' '' ) as out_TRANS_TYPE,
RPAD ( to_char ( SQ_FOP_BORDEREAU.TRANS_EFF_DT , ''YYYYMMDD'' ) , 8 , '' '' ) as out_TRANS_EFF_DT,
RPAD ( to_char ( SQ_FOP_BORDEREAU.POL_EFF_DT , ''YYYYMMDD'' ) , 8 , '' '' ) as out_POL_EFF_DT,
RPAD ( to_char ( SQ_FOP_BORDEREAU.POL_TERM_EXP_DT , ''YYYYMMDD'' ) , 8 , '' '' ) as out_POL_TERM_EXP_DT,
CASE WHEN SQ_FOP_BORDEREAU.TRANS_PREM_GROSS < 0 THEN 1 ELSE 0 END as TPG_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(SQ_FOP_BORDEREAU.TRANS_PREM_GROSS,11,2), 0) ) as var_TRANS_PREM_GROSS,
LPAD ( var_TRANS_PREM_GROSS , 11 , ''0'' ) as var_TRANS_PREM_GROSS_2,
regexp_replace(var_TRANS_PREM_GROSS_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_GROSS_3,
''-'' || substr ( var_TRANS_PREM_GROSS_3 , 2 , 10 ) as var_TRANS_PREM_GROSS_4,
CASE WHEN length ( var_TRANS_PREM_GROSS_2 ) > 0 THEN var_TRANS_PREM_GROSS_2 ELSE ''00000000.00'' END as var_TRANS_PREM_GROSS_5,
CASE WHEN TPG_FLAG = 1 THEN var_TRANS_PREM_GROSS_4 ELSE var_TRANS_PREM_GROSS_5 END as out_TRANS_PREM_GROSS,
CASE WHEN SQ_FOP_BORDEREAU.TRANS_PREM_NET < 0 THEN 1 ELSE 0 END as TPN_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(SQ_FOP_BORDEREAU.TRANS_PREM_NET,11,2), 0) ) as var_TRANS_PREM_NET,
LPAD ( var_TRANS_PREM_NET , 11 , ''0'' ) as var_TRANS_PREM_NET_2,
regexp_replace(var_TRANS_PREM_NET_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_NET_3,
''-'' || substr ( var_TRANS_PREM_NET_3 , 2 , 10 ) as var_TRANS_PREM_NET_4,
CASE WHEN length ( var_TRANS_PREM_NET_2 ) > 0 THEN var_TRANS_PREM_NET_2 ELSE ''00000000.00'' END as var_TRANS_PREM_NET_5,
CASE WHEN TPN_FLAG = 1 THEN var_TRANS_PREM_NET_4 ELSE var_TRANS_PREM_NET_5 END as out_TRANS_PREM_NET,
RPAD ( var_SPACES , 20 , '' '' ) as out_PREV_POL_NBR,
''002'' as out_PROG_ID,
RPAD ( SQ_FOP_BORDEREAU.NAMED_INSRD , 55 , '' '' ) as out_NAME_INSRD,
''1005338'' as out_CNTRCT_NUM,
RPAD ( var_SPACES , 20 , '' '' ) as out_MAIL_CITY,
RPAD ( var_SPACES , 2 , '' '' ) as out_MAIL_ST,
RPAD ( var_SPACES , 10 , '' '' ) as out_MAIL_ZIP,
RPAD ( var_SPACES , 3 , '' '' ) as out_HSB_BRANCH_CD,
RPAD ( var_SPACES , 11 , '' '' ) as out_TOT_PROP_PREM,
RPAD ( var_SPACES , 8 , '' '' ) as out_TRANS_ENTRY_DT,
''013'' as out_COVERAGE_CD,
RPAD ( var_SPACES , 11 , '' '' ) as out_COMMISSION_AMOUNT,
RPAD ( var_SPACES , 5 , '' '' ) as out_COMMISSION_RATE,
RPAD ( var_SPACES , 5 , '' '' ) as out_EB_TREATY_RATE,
RPAD ( var_SPACES , 1 , '' '' ) as FARM_CYBER_IND,
RPAD ( var_SPACES , 2 , '' '' ) as FARM_CYBER_LIMIT_CD,
SQ_FOP_BORDEREAU.source_record_id
FROM
SQ_FOP_BORDEREAU
);


-- Component FOP_Bordereau_File, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FOP_Bordereau_File AS
(
SELECT
exp_fop_bordereau_pass_thru.out_PROD_CD as Product_Code,
exp_fop_bordereau_pass_thru.out_PROD_ID as Company_Product_ID,
exp_fop_bordereau_pass_thru.out_POL_NBR as Policy_Number,
exp_fop_bordereau_pass_thru.out_TRANS_TYPE as Transaction_Code,
exp_fop_bordereau_pass_thru.out_TRANS_EFF_DT as Transaction_Effective_Date,
exp_fop_bordereau_pass_thru.out_POL_EFF_DT as Coverage_Effective_Date,
exp_fop_bordereau_pass_thru.out_POL_TERM_EXP_DT as Coverage_Expiration_Date,
exp_fop_bordereau_pass_thru.out_TRANS_PREM_GROSS as Coverage_Gross_Premium,
exp_fop_bordereau_pass_thru.out_TRANS_PREM_NET as Coverage_Net_Premium,
exp_fop_bordereau_pass_thru.out_PREV_POL_NBR as Previous_Policy_Number,
exp_fop_bordereau_pass_thru.out_PROG_ID as Program_ID,
exp_fop_bordereau_pass_thru.out_NAME_INSRD as Name_Of_Insured,
exp_fop_bordereau_pass_thru.out_CNTRCT_NUM as Contract_Number,
exp_fop_bordereau_pass_thru.out_MAIL_CITY as Mailing_City,
exp_fop_bordereau_pass_thru.out_MAIL_ST as Mailing_State,
exp_fop_bordereau_pass_thru.out_MAIL_ZIP as Mailing_Zip_Code,
exp_fop_bordereau_pass_thru.out_HSB_BRANCH_CD as HSB_Branch_Code,
exp_fop_bordereau_pass_thru.out_TOT_PROP_PREM as Total_Property_Premium,
exp_fop_bordereau_pass_thru.out_TRANS_ENTRY_DT as Transaction_Entry_Date,
exp_fop_bordereau_pass_thru.out_COVERAGE_CD as Coverage_Code,
exp_fop_bordereau_pass_thru.out_COMMISSION_AMOUNT as Commission_Amount,
exp_fop_bordereau_pass_thru.out_COMMISSION_RATE as Commission_Rate,
exp_fop_bordereau_pass_thru.out_EB_TREATY_RATE as EB_Treaty_Rate,
exp_fop_bordereau_pass_thru.FARM_CYBER_IND as Filler_1,
exp_fop_bordereau_pass_thru.FARM_CYBER_LIMIT_CD as Filler_2
FROM
exp_fop_bordereau_pass_thru
);


-- Component FOP_Bordereau_File, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/fop_bordereau_file.txt
from 
(select *
from fop_bordereau_file)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;


-- PIPELINE END FOR 1

-- Component exp_fop_bordereau_pass_thru1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_fop_bordereau_pass_thru1 AS
(
SELECT
''CONTROL'' as C1_Record_Type,
''5519'' as C1_Company_Product_ID,
LPAD ( agg_count_TRANS_ID.COUNTVAL , 7 , ''0'' ) as out_Count_Val,
CASE WHEN agg_count_TRANS_ID.SUM_TRANS_PREM_GROSS < 0 THEN 1 ELSE 0 END as TPG_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(agg_count_TRANS_ID.SUM_TRANS_PREM_GROSS,23,2), 0) ) as var_TRANS_PREM_GROSS,
LPAD ( var_TRANS_PREM_GROSS , 11 , ''0'' ) as var_TRANS_PREM_GROSS_2,
regexp_replace(var_TRANS_PREM_GROSS_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_GROSS_3,
''-'' || substr ( var_TRANS_PREM_GROSS_3 , 2 , 10 ) as var_TRANS_PREM_GROSS_4,
CASE WHEN length ( var_TRANS_PREM_GROSS_2 ) > 0 THEN var_TRANS_PREM_GROSS_2 ELSE ''00000000.00'' END as var_TRANS_PREM_GROSS_5,
CASE WHEN TPG_FLAG = 1 THEN var_TRANS_PREM_GROSS_4 ELSE var_TRANS_PREM_GROSS_5 END as out_SUM_TRANS_PREM_GROSS,
CASE WHEN agg_count_TRANS_ID.SUM_TRANS_PREM_NET < 0 THEN 1 ELSE 0 END as TPN_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(agg_count_TRANS_ID.SUM_TRANS_PREM_NET,23,2), 0) ) as var_TRANS_PREM_NET,
LPAD ( var_TRANS_PREM_NET , 11 , ''0'' ) as var_TRANS_PREM_NET_2,
regexp_replace(var_TRANS_PREM_NET_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_NET_3,
''-'' || substr ( var_TRANS_PREM_NET_3 , 2 , 10 ) as var_TRANS_PREM_NET_4,
CASE WHEN length ( var_TRANS_PREM_NET_2 ) > 0 THEN var_TRANS_PREM_NET_2 ELSE ''00000000.00'' END as var_TRANS_PREM_NET_5,
CASE WHEN TPN_FLAG = 1 THEN var_TRANS_PREM_NET_4 ELSE var_TRANS_PREM_NET_5 END as out_SUM_TRANS_PREM_NET,
:CAL_END_MTH_ID as C1_Reporting_Period,
''08.10'' as C1_Version_Number,
agg_count_TRANS_ID.source_record_id
FROM
agg_count_TRANS_ID
);


-- Component FOB_Bordereau_Trailer, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FOB_Bordereau_Trailer AS
(
SELECT
exp_fop_bordereau_pass_thru1.C1_Record_Type as Record_Type,
exp_fop_bordereau_pass_thru1.C1_Company_Product_ID as Company_Product_ID,
exp_fop_bordereau_pass_thru1.out_Count_Val as Numer_Of_Records,
exp_fop_bordereau_pass_thru1.out_SUM_TRANS_PREM_GROSS as Total_Gross_Premium,
exp_fop_bordereau_pass_thru1.out_SUM_TRANS_PREM_NET as Total_Net_Premium,
exp_fop_bordereau_pass_thru1.C1_Reporting_Period as Reporting_Period,
exp_fop_bordereau_pass_thru1.C1_Version_Number as Version_Number
FROM
exp_fop_bordereau_pass_thru1
);


-- Component FOB_Bordereau_Trailer, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/fob_bordereau_trailer.txt
from 
(select *
from fob_bordereau_trailer)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;


-- PIPELINE END FOR 2

END; ';