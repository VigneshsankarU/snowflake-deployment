-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_MRUA("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE RUN_DATE date;
BEGIN 

RUN_DATE:=(select current_date); 

-- PIPELINE START FOR 1

-- Component SQ_MRUA_HEADER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MRUA_HEADER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as dummy_count
from DB_T_PROD_STAG.pc_policyperiod 
where 1=2
) SRC
)
);


-- Component EXP_MRUA_HEADER1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_MRUA_HEADER1 AS
(
SELECT
''Company_Name'' as Company_Name,
''Policy_Suffix'' as Policy_Suffix,
''NAIC_Code'' as NAIC_Code,
''Policy_Type'' as Policy_Type,
''Policy_Number'' as Policy_Number,
''Primary_Insrd_Name'' as Primary_Insrd_Name,
''Address_Line'' as Address_Line,
''City'' as City,
''County'' as County,
''Zip_Code'' as Zip_Code,
''ProtClassCode'' as ProtClassCode,
''Period_Start'' as Period_Start,
''Period_End'' as Period_End,
''Prem_Trans_Eff_Date'' as Prem_Trans_Eff_Date,
''Prem_Trans_Type'' as Prem_Trans_Type,
''Prem_Trans_Amt'' as Prem_Trans_Amt,
''Job_Number'' as Job_Number,
SQ_MRUA_HEADER.source_record_id
FROM
SQ_MRUA_HEADER
);


-- Component FF_MRUA_HEADER, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_MRUA_HEADER AS
(
SELECT
EXP_MRUA_HEADER1.Company_Name as Company_Name,
EXP_MRUA_HEADER1.Policy_Suffix as Policy_Suffix,
EXP_MRUA_HEADER1.NAIC_Code as NAIC_Code,
EXP_MRUA_HEADER1.Policy_Type as Policy_Type,
EXP_MRUA_HEADER1.Policy_Number as Policy_Number,
EXP_MRUA_HEADER1.Primary_Insrd_Name as Primary_Insrd_Name,
EXP_MRUA_HEADER1.Address_Line as Address_Line,
EXP_MRUA_HEADER1.City as City,
EXP_MRUA_HEADER1.County as County,
EXP_MRUA_HEADER1.Zip_Code as Zip_Code,
EXP_MRUA_HEADER1.ProtClassCode as ProtClassCode,
EXP_MRUA_HEADER1.Period_Start as Period_Start,
EXP_MRUA_HEADER1.Period_End as Period_End,
EXP_MRUA_HEADER1.Prem_Trans_Eff_Date as Prem_Trans_Eff_Date,
EXP_MRUA_HEADER1.Prem_Trans_Type as Prem_Trans_Type,
EXP_MRUA_HEADER1.Prem_Trans_Amt as Prem_Trans_Amt,
EXP_MRUA_HEADER1.Job_Number as Job_Number
FROM
EXP_MRUA_HEADER1
);

copy into @my_internal_stage/FF_MRUA_HEADER from (select * from FF_MRUA_HEADER)
header=true
overwrite=true;

-- Component FF_MRUA_HEADER, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_MRUA_DETAIL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MRUA_DETAIL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Company_Name,
$2 as Policy_Suffix,
$3 as NAIC_Code,
$4 as Policy_Type,
$5 as Policy_Number,
$6 as Primary_Insrd_Name,
$7 as Address_Line,
$8 as City,
$9 as County,
$10 as Zip_Code,
$11 as ProtClassCode,
$12 as Period_Start,
$13 as Period_End,
$14 as Prem_Trans_Eff_Date,
$15 as Prem_Trans_Type,
$16 as Job_Number,
$17 as Prem_Trans_Amt,
$18 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT "Company_Name" 
	, Policy_Suffix
	, "NAIC_Code"
	, TYPECODE_stg AS "Policy_Type"
	, PolicyNumber_stg AS Policy_Number
	, primaryinsuredname_stg AS Primary_Insrd_Name
	, addressline1_stg AS Address_Line
	, city_stg AS City
	, county_stg AS County
	, postalcode_stg AS Zip_Code
	, cast(ProtClassCode as int) AS ProtClassCode
	, cast(periodstart_stg as date) AS Period_Start
	, cast(periodend_stg as date) AS Period_End
	, cast("Premium Transaction Effective Date_stg" as date) AS Prem_Trans_Eff_Date
	, JOB_stg AS "Prem_Trans_Type"
	, jobnumber_stg AS Job_Number
	, SUM(Amount_stg) AS "Prem_Trans_Amt"

FROM (

SELECT DISTINCT PolicyNumber_stg
    , TermNumber_stg
    , pp.ModelNumber_stg
    , jobtl.name_stg
    , pp.PeriodStart_stg
    , pp.PeriodEnd_stg
    , pt.ConfirmationDate_alfa_stg
    , pp.ModelDate_stg
    , pp.CancellationDate_stg
    , hotl.typecode_stg AS "Policy Type_stg"
    , pp.primaryinsuredname_stg
    , CASE WHEN ploc.AddressLine2Internal_stg IS NULL THEN ploc.AddressLine1Internal_stg
        ELSE ploc.AddressLine1Internal_stg || '' '' || ploc.AddressLine2Internal_stg 
        END AS addressline1_stg
    , ploc.countyinternal_stg AS county_stg
    , ploc.cityinternal_stg AS city_stg
    , LEFT(ploc.postalcodeinternaldenorm_stg,5) AS postalcode_stg
    , ROOFYEAR_ALFA_stg
    , hotl2.yearbuilt_stg
    , pol.OriginalEffectiveDate_stg
    , addrtl.name_stg AS AddressType_stg,pp.id_stg
    , n.DESCRIPTION_stg "State_stg"
	, CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''H''
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''F''
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''T''
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''SM''
        END AS Policy_Suffix
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''HO''
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''SF''
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''MH''
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''
        END AS TYPECODE_stg
    , hotl.TYPECODE_stg AS HOTYPECODE_stg
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''
        ELSE uwco.Name_stg
        END AS "NAIC_Code"
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''
        ELSE uwco.Name_stg
        END AS "Company_Name"
    , pp.TotalPremiumRPT_stg AS "Premium_stg"
    , job.closedate_stg
    , COALESCE(hotsc.Amount_stg,0) AS Amount_stg
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''
        WHEN jobtl.TYPECODE_stg=''Reinstatement'' THEN ''REINSTATEMENT''
        ELSE jobtl.TYPECODE_stg
        END AS JOB_stg
    , etlpat.name_stg AS "wind"
    , zz.BooleanTerm1_stg
    , exc.PatternCode_stg
    , jobnumber_stg
    , hotsc.id_stg AS hotscID
    , holoc.DwellingProtectionClassCode_stg AS ProtClassCode
FROM (select * from DB_T_PROD_STAG.pc_policyperiod limit 1) pp
    LEFT JOIN DB_T_PROD_STAG.pcx_hotransaction_hoe hotsc ON hotsc.BranchID_stg = pp.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_holocation_hoe holoc ON holoc.BranchID_stg = pp.ID_stg 
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.branchid_stg=pp.id_stg 
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation ploc ON ploc.BranchID_stg = pp.ID_stg 
       AND ploc.FixedID_stg = holoc.PolicyLocation_stg 
       AND ploc.expirationdate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP ON plocBOP.BranchID_stg = pp.ID_stg  
       AND plocBOP.FixedID_stg = bp7loc.Location_stg 
       AND plocBOP.expirationdate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg
       AND pl.ExpirationDate_stg IS NULL 
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg
    LEFT  JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = cast(pp.basestate_stg as string)
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg
    LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg
       AND hotl2.ExpirationDate_stg IS NULL 
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol on pol.id_stg = pp.PolicyID_stg
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = addr.addr_id
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = ploc.Stateinternal_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=ploc.addresstypeinternal_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg
       AND zz.ExpirationDate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.ID_stg = bld.BranchID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON bld.ID_stg = exc.Building_stg
WHERE pp.status_stg = 9 
    AND (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' AND (zz.BooleanTerm1_stg=0 or zz.BooleanTerm1_stg IS NULL))
    AND n.DESCRIPTION_stg=''Mississippi''
    AND ((hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') OR hotl.TYPECODE_stg IS NULL) OR (bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') OR bp7.TYPECODE_stg IS NULL))
    AND (hotl.TYPECODE_stg IS NOT NULL OR bp7.TYPECODE_stg IS NOT NULL)
    AND (hotl.TYPECODE_stg NOT IN(''PAF'',''CPL'') OR hotl.TYPECODE_stg IS NULL)
    AND ((cast(pp.ModelDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND CAST(pp.EditEffectiveDate_stg as date) BETWEEN trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' )
    OR (cast(pp.ModelDate_stg AS DATE)  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' and trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' AND CAST(pp.EditEffectiveDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'')
    OR (cast(pp.ModelDate_stg AS DATE) between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' and trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' AND CAST(pp.EditEffectiveDate_stg AS DATE) BETWEEN trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' )
    OR (cast(pp.ModelDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' 
    AND exists
(  
select *
from DB_T_PROD_STAG.pc_policyperiod pp1
join DB_T_PROD_STAG.pc_job j1 on j1.ID_stg = pp1.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl1 on jtl1.ID_stg = j1.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt1 on pt1.ID_stg = pp1.PolicyTermID_stg
where pp1.PolicyNumber_stg = pp.PolicyNumber_stg
and cast(pp1.ModelDate_stg as date) = cast(pp.ModelDate_stg as date)
and cast(pp1.PeriodStart_stg as date) = cast(pp.EditEffectiveDate_stg as date)
and jtl1.Name_stg = jobtl.Name_stg
and exists 
(
select *
from DB_T_PROD_STAG.pc_policyperiod pp2
join DB_T_PROD_STAG.pc_job j2 on j2.ID_stg = pp2.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl2 on jtl2.ID_stg = j2.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt2 on pt2.ID_stg = pp2.PolicyTermID_stg
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl1.Name_stg = ''Renewal''
and cast(ConfirmationDate_alfa_stg as date) > trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-2)||''-12-''||''31''
/* year for date was 2022 */
)
and status_stg = 9
)))
and not exists 
(  
select *
from DB_T_PROD_STAG.pc_policyperiod pp1
join DB_T_PROD_STAG.pc_job j1 on j1.ID_stg = pp1.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl1 on jtl1.ID_stg = j1.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt1 on pt1.ID_stg = pp1.PolicyTermID_stg
where pp1.PolicyNumber_stg = pp.PolicyNumber_stg
and cast(pp1.ModelDate_stg as date) = cast(pp.ModelDate_stg as date)
and cast(pp1.EditEffectiveDate_stg as date) = cast(pp.EditEffectiveDate_stg as date)
and jtl1.Name_stg = jobtl.Name_stg
and exists 
(
select *
from DB_T_PROD_STAG.pc_policyperiod pp2
join DB_T_PROD_STAG.pc_job j2 on j2.ID_stg = pp2.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl2 on jtl2.ID_stg = j2.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt2 on pt2.ID_stg = pp2.PolicyTermID_stg
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl2.Name_stg = ''Renewal''
and status_stg = 9
        and ((ConfirmationDate_alfa_stg is NULL) or (cast(ConfirmationDate_alfa_stg as date) > trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''))
))

) cte

WHERE TYPECODE_stg IN (''HO'',''MH'')

GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix, JOB_stg, 
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", 
"NAIC_Code", "Company_Name", BooleanTerm1_stg, jobnumber_stg, ProtClassCode

ORDER BY PolicyNumber_stg, SUM(Amount_stg)
--, "Premium Transaction Effective Date_stg"
--HAVING Prem_Trans_Amt <> 0
) SRC
)
);


-- Component EXP_MRUA_DETAIL, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_MRUA_DETAIL AS
(
SELECT
SQ_MRUA_DETAIL.Company_Name as Company_Name,
SQ_MRUA_DETAIL.Policy_Suffix as Policy_Suffix,
SQ_MRUA_DETAIL.NAIC_Code as NAIC_Code,
SQ_MRUA_DETAIL.Policy_Type as Policy_Type,
SQ_MRUA_DETAIL.Policy_Number as Policy_Number,
REPLACE(SQ_MRUA_DETAIL.Primary_Insrd_Name,'','',NULL) as o_Primary_Insrd_Name,
REPLACE(SQ_MRUA_DETAIL.Address_Line,'','',NULL) as o_Address_Line,
SQ_MRUA_DETAIL.City as City,
SQ_MRUA_DETAIL.County as County,
SQ_MRUA_DETAIL.Zip_Code as Zip_Code,
SQ_MRUA_DETAIL.ProtClassCode as ProtClassCode,
SQ_MRUA_DETAIL.Period_Start as Period_Start,
SQ_MRUA_DETAIL.Period_End as Period_End,
SQ_MRUA_DETAIL.Prem_Trans_Eff_Date as Prem_Trans_Eff_Date,
SQ_MRUA_DETAIL.Prem_Trans_Type as Prem_Trans_Type,
to_char ( IFNULL(TRY_TO_DECIMAL( cast(SQ_MRUA_DETAIL.Prem_Trans_Amt as string) ), 0) ) as o_Prem_Trans_Amt,
SQ_MRUA_DETAIL.Job_Number as Job_Number,
SQ_MRUA_DETAIL.source_record_id
FROM
SQ_MRUA_DETAIL
);


-- Component FF_MRUA_DETAIL, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_MRUA_DETAIL AS
(
SELECT
EXP_MRUA_DETAIL.Company_Name as Company_Name,
EXP_MRUA_DETAIL.Policy_Suffix as Policy_Suffix,
EXP_MRUA_DETAIL.NAIC_Code as NAIC_Code,
EXP_MRUA_DETAIL.Policy_Type as Policy_Type,
EXP_MRUA_DETAIL.Policy_Number as Policy_Number,
EXP_MRUA_DETAIL.o_Primary_Insrd_Name as Primary_Insrd_Name,
EXP_MRUA_DETAIL.o_Address_Line as Address_Line,
EXP_MRUA_DETAIL.City as City,
EXP_MRUA_DETAIL.County as County,
EXP_MRUA_DETAIL.Zip_Code as Zip_Code,
EXP_MRUA_DETAIL.ProtClassCode as ProtClassCode,
EXP_MRUA_DETAIL.Period_Start as Period_Start,
EXP_MRUA_DETAIL.Period_End as Period_End,
EXP_MRUA_DETAIL.Prem_Trans_Eff_Date as Prem_Trans_Eff_Date,
EXP_MRUA_DETAIL.Prem_Trans_Type as Prem_Trans_Type,
EXP_MRUA_DETAIL.o_Prem_Trans_Amt as Prem_Trans_Amt,
EXP_MRUA_DETAIL.Job_Number as Job_Number
FROM
EXP_MRUA_DETAIL
);

copy into @my_internal_stage/FF_MRUA_DETAIL from (select * from FF_MRUA_DETAIL)
header=true
overwrite=true;

-- Component FF_MRUA_DETAIL, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 2

-- PIPELINE START FOR 3

-- Component SQ_MRUA_TRAILER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MRUA_TRAILER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Total_Prem_Amt,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select sum(a."Prem_Trans_Amt") as Total_Prem_Amt
from
(SELECT DISTINCT "Company_Name" 
	, Policy_Suffix
	, "NAIC_Code"
	, TYPECODE_stg AS "Policy_Type"
	, PolicyNumber_stg AS Policy_Number
	, primaryinsuredname_stg AS Primary_Insrd_Name
	, addressline1_stg AS Address_Line
	, city_stg AS City
	, county_stg AS County
	, postalcode_stg AS Zip_Code
	, cast(ProtClassCode as int) AS ProtClassCode
	, cast(periodstart_stg as date) AS Period_Start
	, cast(periodend_stg as date) AS Period_End
	, cast("Premium Transaction Effective Date_stg" as date) AS Prem_Trans_Eff_Date
	, JOB_stg AS "Prem_Trans_Type"
	, jobnumber_stg AS Job_Number
	, SUM(Amount_stg) AS "Prem_Trans_Amt"

FROM (

SELECT DISTINCT PolicyNumber_stg
    , TermNumber_stg
    , pp.ModelNumber_stg
    , jobtl.name_stg
    , pp.PeriodStart_stg
    , pp.PeriodEnd_stg
    , pt.ConfirmationDate_alfa_stg
    , pp.ModelDate_stg
    , pp.CancellationDate_stg
    , hotl.typecode_stg AS "Policy Type_stg"
    , pp.primaryinsuredname_stg
    , CASE WHEN ploc.AddressLine2Internal_stg IS NULL THEN ploc.AddressLine1Internal_stg
        ELSE ploc.AddressLine1Internal_stg || '' '' || ploc.AddressLine2Internal_stg 
        END AS addressline1_stg
    , ploc.countyinternal_stg AS county_stg
    , ploc.cityinternal_stg AS city_stg
    , LEFT(ploc.postalcodeinternaldenorm_stg,5) AS postalcode_stg
    , ROOFYEAR_ALFA_stg
    , hotl2.yearbuilt_stg
    , pol.OriginalEffectiveDate_stg
    , addrtl.name_stg AS AddressType_stg,pp.id_stg
    , n.DESCRIPTION_stg "State_stg"
	, CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''H''
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''F''
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''T''
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''SM''
        END AS Policy_Suffix
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''HO''
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''SF''
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''MH''
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''
        END AS TYPECODE_stg
    , hotl.TYPECODE_stg AS HOTYPECODE_stg
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''
        ELSE uwco.Name_stg
        END AS "NAIC_Code"
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''
        ELSE uwco.Name_stg
        END AS "Company_Name"
    , pp.TotalPremiumRPT_stg AS "Premium_stg"
    , job.closedate_stg
    , COALESCE(hotsc.Amount_stg,0) AS Amount_stg
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''
        WHEN jobtl.TYPECODE_stg=''Reinstatement'' THEN ''REINSTATEMENT''
        ELSE jobtl.TYPECODE_stg
        END AS JOB_stg
    , etlpat.name_stg AS "wind"
    , zz.BooleanTerm1_stg
    , exc.PatternCode_stg
    , jobnumber_stg
    , hotsc.id_stg AS hotscID
    , holoc.DwellingProtectionClassCode_stg AS ProtClassCode
FROM DB_T_PROD_STAG.pc_policyperiod pp
    LEFT JOIN DB_T_PROD_STAG.pcx_hotransaction_hoe hotsc ON hotsc.BranchID_stg = pp.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_holocation_hoe holoc ON holoc.BranchID_stg = pp.ID_stg 
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.branchid_stg=pp.id_stg 
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation ploc ON ploc.BranchID_stg = pp.ID_stg 
       AND ploc.FixedID_stg = holoc.PolicyLocation_stg 
       AND ploc.expirationdate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP ON plocBOP.BranchID_stg = pp.ID_stg  
       AND plocBOP.FixedID_stg = bp7loc.Location_stg 
       AND plocBOP.expirationdate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg
       AND pl.ExpirationDate_stg IS NULL 
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg
    LEFT  JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = cast(pp.basestate_stg as string)
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg
    LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg
       AND hotl2.ExpirationDate_stg IS NULL 
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol on pol.id_stg = pp.PolicyID_stg
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = addr.addr_id
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = ploc.Stateinternal_stg
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=ploc.addresstypeinternal_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg
       AND zz.ExpirationDate_stg IS NULL
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.ID_stg = bld.BranchID_stg
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON bld.ID_stg = exc.Building_stg
WHERE pp.status_stg = 9 
    AND (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' AND (zz.BooleanTerm1_stg=0 or zz.BooleanTerm1_stg IS NULL))
    AND n.DESCRIPTION_stg=''Mississippi''
    AND ((hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') OR hotl.TYPECODE_stg IS NULL) OR (bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') OR bp7.TYPECODE_stg IS NULL))
    AND (hotl.TYPECODE_stg IS NOT NULL OR bp7.TYPECODE_stg IS NOT NULL)
    AND (hotl.TYPECODE_stg NOT IN(''PAF'',''CPL'') OR hotl.TYPECODE_stg IS NULL)
    AND ((cast(pp.ModelDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND CAST(pp.EditEffectiveDate_stg as date) BETWEEN trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' )
    OR (cast(pp.ModelDate_stg AS DATE)  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' and trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' AND CAST(pp.EditEffectiveDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'')
    OR (cast(pp.ModelDate_stg AS DATE) between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' and trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' AND CAST(pp.EditEffectiveDate_stg AS DATE) BETWEEN trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' AND trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' )
    OR (cast(pp.ModelDate_stg AS DATE) < trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01'' 
    AND exists
(  
select *
from DB_T_PROD_STAG.pc_policyperiod pp1
join DB_T_PROD_STAG.pc_job j1 on j1.ID_stg = pp1.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl1 on jtl1.ID_stg = j1.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt1 on pt1.ID_stg = pp1.PolicyTermID_stg
where pp1.PolicyNumber_stg = pp.PolicyNumber_stg
and cast(pp1.ModelDate_stg as date) = cast(pp.ModelDate_stg as date)
and cast(pp1.PeriodStart_stg as date) = cast(pp.EditEffectiveDate_stg as date)
and jtl1.Name_stg = jobtl.Name_stg
and exists 
(
select *
from DB_T_PROD_STAG.pc_policyperiod pp2
join DB_T_PROD_STAG.pc_job j2 on j2.ID_stg = pp2.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl2 on jtl2.ID_stg = j2.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt2 on pt2.ID_stg = pp2.PolicyTermID_stg
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl1.Name_stg = ''Renewal''
and cast(ConfirmationDate_alfa_stg as date) > trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-2)||''-12-''||''31''
/* year for date was 2022 */
)
and status_stg = 9
)))
and not exists 
(  
select *
from DB_T_PROD_STAG.pc_policyperiod pp1
join DB_T_PROD_STAG.pc_job j1 on j1.ID_stg = pp1.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl1 on jtl1.ID_stg = j1.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt1 on pt1.ID_stg = pp1.PolicyTermID_stg
where pp1.PolicyNumber_stg = pp.PolicyNumber_stg
and cast(pp1.ModelDate_stg as date) = cast(pp.ModelDate_stg as date)
and cast(pp1.EditEffectiveDate_stg as date) = cast(pp.EditEffectiveDate_stg as date)
and jtl1.Name_stg = jobtl.Name_stg
and exists 
(
select *
from DB_T_PROD_STAG.pc_policyperiod pp2
join DB_T_PROD_STAG.pc_job j2 on j2.ID_stg = pp2.JobID_stg
join DB_T_PROD_STAG.pctl_job jtl2 on jtl2.ID_stg = j2.Subtype_stg
join DB_T_PROD_STAG.pc_policyterm pt2 on pt2.ID_stg = pp2.PolicyTermID_stg
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl2.Name_stg = ''Renewal''
and status_stg = 9
        and ((ConfirmationDate_alfa_stg is NULL) or (cast(ConfirmationDate_alfa_stg as date) > trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''))
))

) cte

WHERE TYPECODE_stg IN (''HO'',''MH'')

GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix, JOB_stg, 
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", 
"NAIC_Code", "Company_Name", BooleanTerm1_stg, jobnumber_stg, ProtClassCode

/* ORDER BY PolicyNumber_stg, SUM(Amount_stg), "Premium Transaction Effective Date_stg" */
HAVING "Prem_Trans_Amt" <> 0) a
) SRC
)
);


-- Component EXP_MRUA_TRAILER, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_MRUA_TRAILER AS
(
SELECT
'',,,,,,,,,,,,,,TOTAL'' as TOTAL,
to_char ( IFNULL(TRY_TO_DECIMAL( cast(SQ_MRUA_TRAILER.Total_Prem_Amt as string) ), 0) ) as o_Total_Prem_Amount,
SQ_MRUA_TRAILER.source_record_id
FROM
SQ_MRUA_TRAILER
);


-- Component FF_TRL_MRUA, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_TRL_MRUA AS
(
SELECT
EXP_MRUA_TRAILER.TOTAL as TOTAL,
EXP_MRUA_TRAILER.o_Total_Prem_Amount as Total_Prem_Amount
FROM
EXP_MRUA_TRAILER
);

copy into @my_internal_stage/FF_TRL_MRUA from (select * from FF_TRL_MRUA)
header=true
overwrite=true;

-- Component FF_TRL_MRUA, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 3

END; ';