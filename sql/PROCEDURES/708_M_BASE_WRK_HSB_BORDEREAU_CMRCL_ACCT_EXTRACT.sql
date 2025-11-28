-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_WRK_HSB_BORDEREAU_CMRCL_ACCT_EXTRACT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
 DECLARE
run_id STRING;
prcs_id int;
CAL_START_DT date;
CAL_END_DT date;
RECORD_TYPE string;
CAL_END_MTH_ID string;
COMPANY_ID string;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
CAL_START_DT :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_START_DT'' order by insert_ts desc limit 1);
CAL_END_DT :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_DT'' order by insert_ts desc limit 1);
RECORD_TYPE :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''RECORD_TYPE'' order by insert_ts desc limit 1);
CAL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_MTH_ID'' order by insert_ts desc limit 1);
COMPANY_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''COMPANY_ID'' order by insert_ts desc limit 1); 

-- PIPELINE START FOR 1

-- Component SQ_src_HSB_BORDEREAU, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_src_HSB_BORDEREAU AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TRANS_ID,
$2 as PROD_CD,
$3 as PROD_ID,
$4 as POL_NBR,
$5 as TRANS_TYPE,
$6 as TRANS_EFF_DT,
$7 as POL_EFF_DT,
$8 as POL_TERM_EXP_DT,
$9 as TRANS_PREM_GROSS,
$10 as TRANS_PREM_NET,
$11 as PREV_POL_NBR,
$12 as PROG_ID,
$13 as NAMED_INSRD,
$14 as CNTRCT_NUM,
$15 as UW_CMPNY,
$16 as POL_RSK_ST,
$17 as POL_TYPE_DESC,
$18 as MAIL_CITY,
$19 as MAIL_ST,
$20 as MAIL_ZIP,
$21 as HSB_BRANCH_CD,
$22 as TOT_PROP_PREM,
$23 as TRANS_ENTRY_DT,
$24 as COVERAGE_CD,
$25 as COMMISSION_RATE,
$26 as EB_TREATY_RATE,
$27 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	HSB_BORDEREAU_CMRCL.TRANS_ID,

		HSB_BORDEREAU_CMRCL.PROD_CD, HSB_BORDEREAU_CMRCL.PROD_ID, HSB_BORDEREAU_CMRCL.POL_NBR,

		HSB_BORDEREAU_CMRCL.TRANS_TYPE, HSB_BORDEREAU_CMRCL.TRANS_EFF_DT,

		HSB_BORDEREAU_CMRCL.POL_EFF_DT, HSB_BORDEREAU_CMRCL.POL_TERM_EXP_DT,

		HSB_BORDEREAU_CMRCL.TRANS_PREM_GROSS, HSB_BORDEREAU_CMRCL.TRANS_PREM_NET,

		HSB_BORDEREAU_CMRCL.PREV_POL_NBR, HSB_BORDEREAU_CMRCL.PROG_ID,

/* EIM-21056_ Removing comma in NAMED_INSRD column as output file is getting generated in .csv file */
/* HSB_BORDEREAU_CMRCL.NAMED_INSRD, */
			CASE 

			WHEN HSB_BORDEREAU_CMRCL.NAMED_INSRD LIKE ''%,%'' THEN REPLACE(HSB_BORDEREAU_CMRCL.NAMED_INSRD,'','','''') ELSE HSB_BORDEREAU_CMRCL.NAMED_INSRD 

		END AS NAMED_INSRD,

		HSB_BORDEREAU_CMRCL.CNTRCT_NUM,

		HSB_BORDEREAU_CMRCL.UW_CMPNY, HSB_BORDEREAU_CMRCL.POL_RSK_ST,

		HSB_BORDEREAU_CMRCL.POL_TYPE_DESC, HSB_BORDEREAU_CMRCL.MAIL_CITY,

		HSB_BORDEREAU_CMRCL.MAIL_ST, HSB_BORDEREAU_CMRCL.MAIL_ZIP, HSB_BORDEREAU_CMRCL.HSB_BRANCH_CD,

		HSB_BORDEREAU_CMRCL.TOT_PROP_PREM, HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT,

		HSB_BORDEREAU_CMRCL.COVERAGE_CD, HSB_BORDEREAU_CMRCL.COMMISSION_RATE,

		HSB_BORDEREAU_CMRCL.EB_TREATY_RATE 

FROM

 DB_T_PROD_WRK.HSB_BORDEREAU_CMRCL

 WHERE

  CASE 

    WHEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE )  > CAST (HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE) 

         THEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)

     ELSE cast(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE)

     END     >=  :CAL_START_DT

   AND

       CASE 

       WHEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)  > CAST(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE) 

          THEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)

       ELSE cast(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE)

       END     <=  :CAL_END_DT
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
'' '' as var_SPACES,
RPAD ( SQ_src_HSB_BORDEREAU.PROD_CD , 3 , '' '' ) as out_PROD_CD,
''4609'' as out_PROD_ID,
RPAD ( SQ_src_HSB_BORDEREAU.POL_NBR , 20 , '' '' ) as out_POL_NBR,
RPAD ( SQ_src_HSB_BORDEREAU.TRANS_TYPE , 2 , '' '' ) as out_TRANS_TYPE,
to_char ( SQ_src_HSB_BORDEREAU.TRANS_EFF_DT , ''YYYYMMDD'' ) as out_TRANS_EFF_DT,
to_char ( SQ_src_HSB_BORDEREAU.POL_EFF_DT , ''YYYYMMDD'' ) as out_POL_EFF_DT,
to_char ( SQ_src_HSB_BORDEREAU.POL_TERM_EXP_DT , ''YYYYMMDD'' ) as out_POL_TERM_EXP_DT,
CASE WHEN SQ_src_HSB_BORDEREAU.TRANS_PREM_GROSS < 0 THEN 1 ELSE 0 END as TPG_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(SQ_src_HSB_BORDEREAU.TRANS_PREM_GROSS,11,2), 0) ) as var_TRANS_PREM_GROSS,
LPAD ( var_TRANS_PREM_GROSS , 11 , ''0'' ) as var_TRANS_PREM_GROSS_2,
regexp_replace(var_TRANS_PREM_GROSS_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_GROSS_3,
''-'' || substr ( var_TRANS_PREM_GROSS_3 , 2 , 10 ) as var_TRANS_PREM_GROSS_4,
CASE WHEN length ( var_TRANS_PREM_GROSS_2 ) > 0 THEN var_TRANS_PREM_GROSS_2 ELSE ''00000000.00'' END as var_TRANS_PREM_GROSS_5,
CASE WHEN TPG_FLAG = 1 THEN var_TRANS_PREM_GROSS_4 ELSE var_TRANS_PREM_GROSS_5 END as out_TRANS_PREM_GROSS,
CASE WHEN SQ_src_HSB_BORDEREAU.TRANS_PREM_NET < 0 THEN 1 ELSE 0 END as TPN_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(SQ_src_HSB_BORDEREAU.TRANS_PREM_NET,11,2), 0) ) as var_TRANS_PREM_NET,
LPAD ( var_TRANS_PREM_NET , 11 , ''0'' ) as var_TRANS_PREM_NET_2,
regexp_replace(var_TRANS_PREM_NET_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_NET_3,
''-'' || substr ( var_TRANS_PREM_NET_3 , 2 , 10 ) as var_TRANS_PREM_NET_4,
CASE WHEN length ( var_TRANS_PREM_NET_2 ) > 0 THEN var_TRANS_PREM_NET_2 ELSE ''00000000.00'' END as var_TRANS_PREM_NET_5,
CASE WHEN TPN_FLAG = 1 THEN var_TRANS_PREM_NET_4 ELSE var_TRANS_PREM_NET_5 END as out_TRANS_PREM_NET,
RPAD ( var_SPACES , 20 , '' '' ) as out_PREV_POL_NBR,
RPAD ( SQ_src_HSB_BORDEREAU.PROG_ID , 3 , '' '' ) as out_PROG_ID,
RPAD ( SQ_src_HSB_BORDEREAU.NAMED_INSRD , 55 , '' '' ) as out_NAME_INSRD,
RPAD ( SQ_src_HSB_BORDEREAU.CNTRCT_NUM , 7 , '' '' ) as out_CNTRCT_NUM,
RPAD ( SQ_src_HSB_BORDEREAU.UW_CMPNY , 3 , '' '' ) as out_UW_CMPNY_ACTG,
RPAD ( SQ_src_HSB_BORDEREAU.POL_RSK_ST , 2 , '' '' ) as out_POL_RSJ_ST_ACTG,
RPAD ( SQ_src_HSB_BORDEREAU.POL_TYPE_DESC , 3 , '' '' ) as out_POL_TYPE_DESC_ACTG,
RPAD ( SQ_src_HSB_BORDEREAU.MAIL_CITY , 20 , '' '' ) as out_MAIL_CITY,
RPAD ( SQ_src_HSB_BORDEREAU.MAIL_ST , 2 , '' '' ) as out_MAIL_ST,
RPAD ( SQ_src_HSB_BORDEREAU.MAIL_ZIP , 10 , '' '' ) as out_MAIL_ZIP,
''000'' as out_HSB_BRANCH_CD,
RPAD ( var_SPACES , 11 , '' '' ) as out_TOT_PROP_PREM,
to_char ( SQ_src_HSB_BORDEREAU.TRANS_ENTRY_DT , ''YYYYMMDD'' ) as out_TRANS_ENTRY_DT,
''013'' as out_COVERAGE_CD,
RPAD ( var_SPACES , 11 , '' '' ) as out_COMMISSION_AMOUNT,
RPAD ( var_SPACES , 5 , '' '' ) as out_COMMISSION_RATE,
SQ_src_HSB_BORDEREAU.EB_TREATY_RATE as EB_TREATY_RATE,
RPAD ( var_SPACES , 1 , '' '' ) as out_Filler_1,
RPAD ( var_SPACES , 2 , '' '' ) as out_Filler_2,
SUBSTR ( ltrim ( rtrim ( SQ_src_HSB_BORDEREAU.POL_TYPE_DESC ) ) , 1 , 3 ) as v_POL_TYPE_DESC,
DECODE ( true , SUBSTR ( v_POL_TYPE_DESC , 1 , 3 ) = ''BUS'' , ''BO'' , SUBSTR ( v_POL_TYPE_DESC , 1 , 3 ) = ''CHU'' , ''BO'' , ''NA'' ) as out_Lob_cd,
SQ_src_HSB_BORDEREAU.source_record_id
FROM
SQ_src_HSB_BORDEREAU
);


-- Component HSB_Bordereau_File, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE HSB_Bordereau_File AS
(
SELECT
EXPTRANS.out_PROD_CD as Product_Code,
EXPTRANS.out_PROD_ID as Company_Product_ID,
EXPTRANS.out_POL_NBR as Policy_Number,
EXPTRANS.out_TRANS_TYPE as Transaction_Code,
EXPTRANS.out_TRANS_EFF_DT as Transaction_Effective_Date,
EXPTRANS.out_POL_EFF_DT as Coverage_Effective_Date,
EXPTRANS.out_POL_TERM_EXP_DT as Coverage_Expiration_Date,
EXPTRANS.out_TRANS_PREM_GROSS as Coverage_Gross_Premium,
EXPTRANS.out_TRANS_PREM_NET as Coverage_Net_Premium,
EXPTRANS.out_PREV_POL_NBR as Previous_Policy_Number,
EXPTRANS.out_PROG_ID as Program_ID,
EXPTRANS.out_NAME_INSRD as Name_Of_Insured,
EXPTRANS.out_CNTRCT_NUM as Contract_Number,
EXPTRANS.out_MAIL_CITY as Mailing_City,
EXPTRANS.out_MAIL_ST as Mailing_State,
EXPTRANS.out_MAIL_ZIP as Mailing_Zip_Code,
EXPTRANS.out_HSB_BRANCH_CD as HSB_Branch_Code,
EXPTRANS.out_TOT_PROP_PREM as Total_Property_Premium,
EXPTRANS.out_TRANS_ENTRY_DT as Transaction_Entry_Date,
EXPTRANS.out_COVERAGE_CD as Coverage_Code,
EXPTRANS.out_COMMISSION_AMOUNT as Commission_Amount,
EXPTRANS.out_COMMISSION_RATE as Commission_Rate,
EXPTRANS.EB_TREATY_RATE as EB_Treaty_Rate,
EXPTRANS.out_Filler_1 as Filler_1,
EXPTRANS.out_Filler_2 as Filler_2,
EXPTRANS.out_UW_CMPNY_ACTG as Underwriting_Company,
EXPTRANS.out_POL_RSJ_ST_ACTG as Policy_Risk_State,
EXPTRANS.out_POL_TYPE_DESC_ACTG as Policy_Type_Description,
EXPTRANS.out_Lob_cd as LOB_Code
FROM
EXPTRANS
);


-- Component HSB_Bordereau_File, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/hsb_bordereau_file.txt
from 
(select *
from hsb_bordereau_file)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;

;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_HSB_BORDEREAU, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_HSB_BORDEREAU AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TRANS_ID,
$2 as PROD_CD,
$3 as PROD_ID,
$4 as POL_NBR,
$5 as TRANS_TYPE,
$6 as TRANS_EFF_DT,
$7 as POL_EFF_DT,
$8 as POL_TERM_EXP_DT,
$9 as TRANS_PREM_GROSS,
$10 as TRANS_PREM_NET,
$11 as PREV_POL_NBR,
$12 as PROG_ID,
$13 as NAMED_INSRD,
$14 as CNTRCT_NUM,
$15 as UW_CMPNY,
$16 as POL_RSK_ST,
$17 as POL_TYPE_DESC,
$18 as MAIL_CITY,
$19 as MAIL_ST,
$20 as MAIL_ZIP,
$21 as HSB_BRANCH_CD,
$22 as TOT_PROP_PREM,
$23 as TRANS_ENTRY_DT,
$24 as COVERAGE_CD,
$25 as COMMISSION_RATE,
$26 as EB_TREATY_RATE,
$27 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT HSB_BORDEREAU_CMRCL.TRANS_ID, HSB_BORDEREAU_CMRCL.PROD_CD, HSB_BORDEREAU_CMRCL.PROD_ID, HSB_BORDEREAU_CMRCL.POL_NBR, HSB_BORDEREAU_CMRCL.TRANS_TYPE, HSB_BORDEREAU_CMRCL.TRANS_EFF_DT, HSB_BORDEREAU_CMRCL.POL_EFF_DT, HSB_BORDEREAU_CMRCL.POL_TERM_EXP_DT, HSB_BORDEREAU_CMRCL.TRANS_PREM_GROSS, HSB_BORDEREAU_CMRCL.TRANS_PREM_NET, HSB_BORDEREAU_CMRCL.PREV_POL_NBR, HSB_BORDEREAU_CMRCL.PROG_ID, HSB_BORDEREAU_CMRCL.NAMED_INSRD, HSB_BORDEREAU_CMRCL.CNTRCT_NUM, HSB_BORDEREAU_CMRCL.UW_CMPNY, HSB_BORDEREAU_CMRCL.POL_RSK_ST, HSB_BORDEREAU_CMRCL.POL_TYPE_DESC, HSB_BORDEREAU_CMRCL.MAIL_CITY, HSB_BORDEREAU_CMRCL.MAIL_ST, HSB_BORDEREAU_CMRCL.MAIL_ZIP, HSB_BORDEREAU_CMRCL.HSB_BRANCH_CD, HSB_BORDEREAU_CMRCL.TOT_PROP_PREM, HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT, HSB_BORDEREAU_CMRCL.COVERAGE_CD, HSB_BORDEREAU_CMRCL.COMMISSION_RATE, HSB_BORDEREAU_CMRCL.EB_TREATY_RATE 

FROM

 DB_T_PROD_WRK.HSB_BORDEREAU_CMRCL

 WHERE

  CASE 

    WHEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE )  > CAST (HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE) 

         THEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)

     ELSE cast(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE)

     END     >=  :CAL_START_DT

   AND

       CASE 

       WHEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)  > CAST(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE) 

          THEN cast(HSB_BORDEREAU_CMRCL.TRANS_ENTRY_DT AS DATE)

       ELSE cast(HSB_BORDEREAU_CMRCL.TRANS_EFF_DT AS DATE)

       END     <=  :CAL_END_DT
) SRC
)
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
SQ_HSB_BORDEREAU.TRANS_ID as TRANS_ID,
SQ_HSB_BORDEREAU.TRANS_PREM_GROSS as TRANS_PREM_GROSS,
SQ_HSB_BORDEREAU.TRANS_PREM_NET as TRANS_PREM_NET,
SQ_HSB_BORDEREAU.source_record_id
FROM
SQ_HSB_BORDEREAU
);


-- Component AGGTRANS, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE AGGTRANS AS
(
SELECT
MIN(EXPTRANS1.TRANS_ID) as TRANS_ID,
count(TRANS_ID) as COUNTVAL,
MIN(EXPTRANS1.TRANS_PREM_GROSS) as TRANS_PREM_GROSS,
MIN(EXPTRANS1.TRANS_PREM_NET) as TRANS_PREM_NET,
sum(TRANS_PREM_GROSS) as SUM_TRANS_PREM_GROSS,
sum(TRANS_PREM_NET) as SUM_TRANS_PREM_NET,
MIN(EXPTRANS1.source_record_id) as source_record_id
FROM
EXPTRANS1
);


-- Component EXPTRANS2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS2 AS
(
SELECT
'' '' as var_SPACES,
LPAD ( AGGTRANS.COUNTVAL , 7 , ''0'' ) as out_Count_Val,
:RECORD_TYPE as C1_Record_Type,
C1_Record_Type as Out_C1_Record_Type,
:COMPANY_ID as C1_Company_Product_ID,
''4609'' as Out_C1_Company_Product_ID,
CASE WHEN AGGTRANS.SUM_TRANS_PREM_GROSS < 0 THEN 1 ELSE 0 END as TPG_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(AGGTRANS.SUM_TRANS_PREM_GROSS,23,2), 0) ) as var_TRANS_PREM_GROSS,
LPAD ( var_TRANS_PREM_GROSS , 11 , ''0'' ) as var_TRANS_PREM_GROSS_2,
regexp_replace(var_TRANS_PREM_GROSS_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_GROSS_3,
''-'' || substr ( var_TRANS_PREM_GROSS_3 , 2 , 10 ) as var_TRANS_PREM_GROSS_4,
CASE WHEN length ( var_TRANS_PREM_GROSS_2 ) > 0 THEN var_TRANS_PREM_GROSS_2 ELSE ''00000000.00'' END as var_TRANS_PREM_GROSS_5,
CASE WHEN TPG_FLAG = 1 THEN var_TRANS_PREM_GROSS_4 ELSE var_TRANS_PREM_GROSS_5 END as out_SUM_TRANS_PREM_GROSS,
CASE WHEN AGGTRANS.SUM_TRANS_PREM_NET < 0 THEN 1 ELSE 0 END as TPN_FLAG,
to_char ( IFNULL(TRY_TO_DECIMAL(AGGTRANS.SUM_TRANS_PREM_NET,23,2), 0) ) as var_TRANS_PREM_NET,
LPAD ( var_TRANS_PREM_NET , 11 , ''0'' ) as var_TRANS_PREM_NET_2,
regexp_replace(var_TRANS_PREM_NET_2,''-'',''0'',1,0,''i'') as var_TRANS_PREM_NET_3,
''-'' || substr ( var_TRANS_PREM_NET_3 , 2 , 10 ) as var_TRANS_PREM_NET_4,
CASE WHEN length ( var_TRANS_PREM_NET_2 ) > 0 THEN var_TRANS_PREM_NET_2 ELSE ''00000000.00'' END as var_TRANS_PREM_NET_5,
CASE WHEN TPN_FLAG = 1 THEN var_TRANS_PREM_NET_4 ELSE var_TRANS_PREM_NET_5 END as out_SUM_TRANS_PREM_NET,
:CAL_END_MTH_ID as C1_Reporting_Period,
''08.07'' as C1_Version_Number,
RPAD ( var_SPACES , 195 , '''' ) as filler,
AGGTRANS.source_record_id
FROM
AGGTRANS
);


-- Component HSB_Bordereau_Trailer, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE HSB_Bordereau_Trailer AS
(
SELECT
EXPTRANS2.Out_C1_Record_Type as Record_Type,
EXPTRANS2.Out_C1_Company_Product_ID as Company_Product_ID,
EXPTRANS2.out_Count_Val as Numer_Of_Records,
EXPTRANS2.out_SUM_TRANS_PREM_GROSS as Total_Gross_Premium,
EXPTRANS2.out_SUM_TRANS_PREM_NET as Total_Net_Premium,
EXPTRANS2.C1_Reporting_Period as Reporting_Period,
EXPTRANS2.C1_Version_Number as Version_Number,
EXPTRANS2.filler as Filler_area
FROM
EXPTRANS2
);


-- Component HSB_Bordereau_Trailer, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/hsb_bordereau_trailer.txt
from 
(select *
from hsb_bordereau_trailer)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;



-- PIPELINE END FOR 2

END; ';