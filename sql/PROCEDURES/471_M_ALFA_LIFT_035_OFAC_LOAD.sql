-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_035_OFAC_LOAD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_OFAC_Stag_DeDup, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_Stag_DeDup AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FirstName,
$2 as LastName,
$3 as FullName,
$4 as AddressLine1,
$5 as AddressLine2,
$6 as AddressLine3,
$7 as City,
$8 as StateCD,
$9 as PostalCode,
$10 as ContactType,
$11 as TaxID,
$12 as ClaimNumber,
$13 as PolicyNumber,
$14 as AccountNumber,
$15 as SourceSystem,
$16 as UpdateTime,
$17 as SourceGroup,
$18 as TaxStatus,
$19 as Country,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
OFAC_Stag_DeDup.FirstName,
OFAC_Stag_DeDup.LastName,
OFAC_Stag_DeDup.FullName,
OFAC_Stag_DeDup.AddressLine1,
OFAC_Stag_DeDup.AddressLine2,
OFAC_Stag_DeDup.AddressLine3,
OFAC_Stag_DeDup.City,
OFAC_Stag_DeDup.StateCD,
OFAC_Stag_DeDup.PostalCode,
OFAC_Stag_DeDup.ContactType,
OFAC_Stag_DeDup.TaxID,
OFAC_Stag_DeDup.ClaimNumber,
OFAC_Stag_DeDup.PolicyNumber,
OFAC_Stag_DeDup.AccountNumber,
OFAC_Stag_DeDup.SourceSystem,
OFAC_Stag_DeDup.UpdateTime,
OFAC_Stag_DeDup.SourceGroup,
OFAC_Stag_DeDup.TaxStatus,
OFAC_Stag_DeDup.Country
FROM DB_T_PROD_STAG.OFAC_Stag_DeDup
) SRC
)
);


-- Component exp_OFAC, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_OFAC AS
(
SELECT
''|'' PIPE,
''|'' as PIPE2,
RPAD ( CASE WHEN SQ_OFAC_Stag_DeDup.FullName IS NULL THEN SQ_OFAC_Stag_DeDup.FirstName || '' '' || SQ_OFAC_Stag_DeDup.LastName ELSE SQ_OFAC_Stag_DeDup.FullName END , 50 , '' '' ) as EXC_NAME,
REPLACE(REPLACE(EXC_NAME,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_EXC_NAME,
ltrim ( rtrim ( v_EXC_NAME ) ) as o_EXC_NAME,
rtrim ( ltrim ( SQ_OFAC_Stag_DeDup.AddressLine1 ) ) as v_AddressLine1,
CONCAT ( '' '' , rtrim ( ltrim ( SQ_OFAC_Stag_DeDup.AddressLine2 ) ) ) as v_AddressLine2,
CASE WHEN SQ_OFAC_Stag_DeDup.SourceSystem = ''BillingCenter'' or SQ_OFAC_Stag_DeDup.SourceSystem = ''ClaimsCenter'' or SQ_OFAC_Stag_DeDup.SourceSystem = ''PolicyCenter'' THEN SQ_OFAC_Stag_DeDup.AddressLine1 ELSE CONCAT ( v_AddressLine1 , v_AddressLine2 ) END as v_AddressLine,
REPLACE(v_AddressLine,''|'',NULL) as v_AddressLine_REPL,
RPAD ( v_AddressLine_REPL , 50 , '' '' ) as v1_AddressLine,
ltrim ( rtrim ( v1_AddressLine ) ) as o_EXC_ADDR,
RPAD ( SQ_OFAC_Stag_DeDup.City , 40 , '' '' ) as EXC_CITY,
ltrim ( rtrim ( EXC_CITY ) ) as o_EXC_CITY,
SQ_OFAC_Stag_DeDup.StateCD as StateCD,
SQ_OFAC_Stag_DeDup.PostalCode as PostalCode,
DECODE ( True , SQ_OFAC_Stag_DeDup.TaxID IS NOT NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''ClaimsCenter'' AND SQ_OFAC_Stag_DeDup.ContactType = ''Company'' AND SQ_OFAC_Stag_DeDup.TaxStatus = ''1'' , CONCAT ( ''C'' , SUBSTR ( SQ_OFAC_Stag_DeDup.TaxID , - 4 , 4 ) ) , SQ_OFAC_Stag_DeDup.TaxID IS NOT NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''ClaimsCenter'' AND SQ_OFAC_Stag_DeDup.ContactType = ''CompanyVendor'' AND SQ_OFAC_Stag_DeDup.TaxStatus = ''1'' , CONCAT ( ''C'' , SUBSTR ( SQ_OFAC_Stag_DeDup.TaxID , - 4 , 4 ) ) , SQ_OFAC_Stag_DeDup.TaxID IS NOT NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''ClaimsCenter'' , SQ_OFAC_Stag_DeDup.ClaimNumber , SQ_OFAC_Stag_DeDup.TaxID IS NOT NULL AND SQ_OFAC_Stag_DeDup.SourceSystem <> ''ClaimsCenter'' , SUBSTR ( SQ_OFAC_Stag_DeDup.TaxID , - 4 , 4 ) , SQ_OFAC_Stag_DeDup.TaxID IS NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''PolicyCenter'' , SQ_OFAC_Stag_DeDup.PolicyNumber , SQ_OFAC_Stag_DeDup.TaxID IS NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''ClaimsCenter'' , SQ_OFAC_Stag_DeDup.ClaimNumber , SQ_OFAC_Stag_DeDup.TaxID IS NULL AND SQ_OFAC_Stag_DeDup.SourceSystem = ''BillingCenter'' , SQ_OFAC_Stag_DeDup.AccountNumber , SQ_OFAC_Stag_DeDup.SourceSystem = ''MB'' , SQ_OFAC_Stag_DeDup.AccountNumber , SQ_OFAC_Stag_DeDup.PolicyNumber ) as EXC_POLICY,
ltrim ( rtrim ( EXC_POLICY ) ) as o_EXC_POLICY,
ltrim ( rtrim ( SQ_OFAC_Stag_DeDup.Country ) ) as o_Country,
SQ_OFAC_Stag_DeDup.source_record_id
FROM
SQ_OFAC_Stag_DeDup
);


-- Component SRTTRANS, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE SRTTRANS AS
(
SELECT
exp_OFAC.PIPE as DELIMETER_PIPE_01,
exp_OFAC.PIPE as DELIMETER_PIPE_02,
exp_OFAC.PIPE as DELIMETER_PIPE_03,
exp_OFAC.PIPE as DELIMETER_PIPE_04,
exp_OFAC.PIPE as DELIMETER_PIPE_05,
exp_OFAC.o_EXC_NAME as EXC_NAME,
exp_OFAC.o_EXC_CITY as EXC_CITY,
exp_OFAC.StateCD as EXC_STATE,
exp_OFAC.PostalCode as EXC_ZIP,
exp_OFAC.PIPE as DELIMETER_PIPE_10,
exp_OFAC.o_Country as EXC_COUNTRY,
exp_OFAC.PIPE as DELIMETER_PIPE_12,
exp_OFAC.o_EXC_POLICY as EXC_POLICY,
exp_OFAC.o_EXC_ADDR as EXC_ADDR,
exp_OFAC.PIPE2 as DELIMETER_PIPE_15,
exp_OFAC.source_record_id
FROM
exp_OFAC
ORDER BY EXC_POLICY 
);


-- Component OFAC_TGT, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE OFAC_TGT AS
(
SELECT
SRTTRANS.DELIMETER_PIPE_01 as DELIMITER_PIPE_01,
SRTTRANS.DELIMETER_PIPE_02 as DELIMITER_PIPE_02,
SRTTRANS.DELIMETER_PIPE_03 as DELIMITER_PIPE_03,
SRTTRANS.DELIMETER_PIPE_04 as DELIMITER_PIPE_04,
SRTTRANS.DELIMETER_PIPE_05 as DELIMITER_PIPE_05,
SRTTRANS.EXC_NAME as EXC_NAME,
SRTTRANS.EXC_CITY as EXC_CITY,
SRTTRANS.EXC_STATE as EXC_STATE,
SRTTRANS.EXC_ZIP as EXC_ZIP,
SRTTRANS.DELIMETER_PIPE_10 as DELIMITER_PIPE_10,
SRTTRANS.DELIMETER_PIPE_10 as DELIMITER_PIPE_11,
SRTTRANS.DELIMETER_PIPE_12 as DELIMITER_PIPE_12,
SRTTRANS.EXC_POLICY as EXC_POLICY,
SRTTRANS.EXC_ADDR as EXC_ADDR,
SRTTRANS.DELIMETER_PIPE_15 as DELIMITER_PIPE_15
FROM
SRTTRANS
);

copy into @my_internal_stage/OFAC_TGT from (select * from OFAC_TGT)
header=true
overwrite=true;

-- Component OFAC_TGT, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_OFAC_Stag_DeDup_HEADER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_Stag_DeDup_HEADER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select count(OFAC.FullName)  As CNT from DB_T_PROD_STAG.OFAC_Stag OFAC
) SRC
)
);


-- Component EXP_HEADER, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_HEADER AS
(
SELECT
''OFAC COMPLIANCE REPORT'' as REPORT_TITLE,
TO_CHAR ( CURRENT_TIMESTAMP , ''YYYYMMDDHHMMSS'' ) as RUNDATE,
SQ_OFAC_Stag_DeDup_HEADER.source_record_id
FROM
SQ_OFAC_Stag_DeDup_HEADER
);


-- Component FF_OFAC_HEADER, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_OFAC_HEADER AS
(
SELECT
EXP_HEADER.REPORT_TITLE as REPORT_TITLE,
EXP_HEADER.RUNDATE as RUNDATE
FROM
EXP_HEADER
);

copy into @my_internal_stage/FF_OFAC_HEADER from (select * from FF_OFAC_HEADER)
header=true
overwrite=true;

-- Component FF_OFAC_HEADER, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 2

-- PIPELINE START FOR 3

-- Component SQ_OFAC_Stag_DeDup_DETAIL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_Stag_DeDup_DETAIL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SourceGroup,
$2 as CNT,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT SRCGroup, Count (*) FROM
(SELECT CASE 
WHEN OFAC.SourceGroup = ''EMPLOYEE'' THEN ''Total HR Records'' 
WHEN OFAC.SourceGroup = ''VENDOR'' THEN ''Total Vendor Records'' 
WHEN OFAC.SourceGroup = ''PANDC'' THEN ''Total P&C Records'' 
WHEN OFAC.SourceGroup = ''IRS'' THEN ''Total IRS Records'' 
WHEN OFAC.SourceGroup = ''ING_POL_LIST'' THEN ''Total DB_T_STAG_MEMBXREF_PROD.Life Records'' 
/* WHEN OFAC.SourceGroup = ''GW'' THEN ''Total Guidewire Records''  */
WHEN OFAC.SourceGroup = ''MB'' THEN ''Total MonthlyBilling Records'' 
WHEN OFAC.SourceGroup = ''GW'' AND OFAC.SourceSystem = ''ClaimsCenter'' THEN ''Total Guidewire CC Records'' 
WHEN OFAC.SourceGroup = ''GW'' AND OFAC.SourceSystem = ''PolicyCenter'' THEN ''Total Guidewire PC Records'' 
WHEN OFAC.SourceGroup = ''GW'' AND OFAC.SourceSystem = ''BillingCenter'' THEN ''Total Guidewire BC Records'' 
ELSE ''UNK''
END AS SRCGroup
FROM DB_T_PROD_STAG.OFAC_Stag_DeDup OFAC 
       ) SRC
GROUP BY SRCGroup
) SRC
)
);


-- Component EXP_DETAIL, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_DETAIL AS
(
SELECT
SQ_OFAC_Stag_DeDup_DETAIL.SourceGroup as REPORT_TITLE,
TO_NUMBER(SQ_OFAC_Stag_DeDup_DETAIL.CNT) as o_CNT,
SQ_OFAC_Stag_DeDup_DETAIL.source_record_id
FROM
SQ_OFAC_Stag_DeDup_DETAIL
);


-- Component FF_OFAC_DETAIL, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_OFAC_DETAIL AS
(
SELECT
EXP_DETAIL.REPORT_TITLE as HEADER,
EXP_DETAIL.o_CNT as TOTAL_RECS
FROM
EXP_DETAIL
);

copy into @my_internal_stage/FF_OFAC_DETAIL from (select * from FF_OFAC_DETAIL)
header=true
overwrite=true;

-- Component FF_OFAC_DETAIL, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 3

-- PIPELINE START FOR 4

-- Component SQOFAC_Stag_DeDup_TRAILER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQOFAC_Stag_DeDup_TRAILER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select count(OFAC.SourceGroup)  As CNT from DB_T_PROD_STAG.OFAC_Stag_DeDup OFAC
) SRC
)
);


-- Component EXP_TRAILER, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_TRAILER AS
(
SELECT
''TOTAL RECORDS'' as TRAILER_HEADER,
TO_NUMBER(SQOFAC_Stag_DeDup_TRAILER.CNT) as o_CNT,
SQOFAC_Stag_DeDup_TRAILER.source_record_id
FROM
SQOFAC_Stag_DeDup_TRAILER
);


-- Component FF_OFAC_TRAILER, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_OFAC_TRAILER AS
(
SELECT
EXP_TRAILER.TRAILER_HEADER as TRAILER_HEADER,
EXP_TRAILER.o_CNT as TOTAL_RECS
FROM
EXP_TRAILER
);

copy into @my_internal_stage/FF_OFAC_TRAILER from (select * from FF_OFAC_TRAILER)
header=true
overwrite=true;

-- Component FF_OFAC_TRAILER, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 4

END; ';