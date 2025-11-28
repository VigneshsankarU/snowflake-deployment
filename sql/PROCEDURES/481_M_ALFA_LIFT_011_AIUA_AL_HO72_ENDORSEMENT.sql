-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_AIUA_AL_HO72_ENDORSEMENT("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE PC_EOY varchar;
PC_BOY varchar;
BEGIN 

PC_EOY:=''1900-01-01''; 
PC_BOY:=''1900-01-01''; 

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UW_Company,
$2 as PolicyNumber,
$3 as Premium,
$4 as PolicyType,
$5 as State,
$6 as County,
$7 as ModelDate,
$8 as EffectiveDate,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT UWCompany,POLICYNUMBER_STG as PolicyName, SUM(writtenPremium) AS Premium,PolicyType,State,County,
MODELDATE_STG AS ModelDate, editeffectivedate_stg AS EffectiveDate 
FROM (
select DISTINCT  MODELDATE_STG,editeffectivedate_stg,( 

--case when 1 = ( select  1 
--where   exists( select  pc_policyperiod2.policynumber_stg from    DB_T_PROD_STAG.pc_policyperiod pc_policyperiod2 
--join DB_T_PROD_STAG.pc_policyterm pt2 on pt2.ID_stg = pc_policyperiod2.PolicyTermID_stg 
--join DB_T_PROD_STAG.pc_policyline on pc_policyperiod2.id_stg = pc_policyline.BranchID_stg 
--join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pc_policyline.HOPolicyType_stg = pctl_hopolicytype_hoe.ID_stg 
--and pctl_hopolicytype_hoe.TypeCode_stg like ''HO%'' 
--join DB_T_PROD_STAG.pc_job job2 on job2.ID_stg = pc_policyperiod2.jobID_stg 
--join DB_T_PROD_STAG.pctl_job pctl_job2 on pctl_job2.ID_stg = job2.Subtype_stg where   pctl_job2.Name_stg = ''Renewal'' 
--and (pt.ConfirmationDate_alfa_stg > :PC_EOY or pt.ConfirmationDate_alfa_stg is NULL) 
--and pc_policyperiod2.PolicyNumber_stg = pp.PolicyNumber_stg and pc_policyperiod2.TermNumber_stg = pp.TermNumber_stg )) then 0 
--else pp.TransactionPremiumRPT_stg end

nvl(pp.TransactionPremiumRPT_stg,0)
) as writtenPremium,
pp.POLICYNUMBER_stg, pp.id_stg, pchh.TypeCode_stg as PolicyType,jd.TypeCode_stg as State,
UWC.publicid_stg as UWCompany,pl.CountyInternal_stg as County
FROM    DB_T_PROD_STAG.pcx_hotransaction_hoe PHTH 
JOIN DB_T_PROD_STAG.pcx_homeownerscost_hoe PHCH ON PHTH.HomeownersCost_stg = PHCH.ID_stg 
JOIN DB_T_PROD_STAG.pc_policyperiod pp ON PHTH.BRANCHID_stg =PP.ID_stg 
JOIN DB_T_PROD_STAG.PC_UWCOMPANY UWC ON pp.UWCOMPANY_stg=UWC.ID_stg 
JOIN DB_T_PROD_STAG.PCTL_JURISDICTION JD ON pp.BASESTATE_stg=JD.ID_stg 
join DB_T_PROD_STAG.pcx_dwelling_hoe pdh on pdh.BranchID_stg=pp.ID_stg and pdh.expirationdate_stg is null 
left JOIN DB_T_PROD_STAG.PCTL_PERILTYPE_ALFA PERIL ON PERILTYPE_ALFA_stg=PERIL.ID_stg 
left JOIN DB_T_PROD_STAG.pctl_dwellingoccupancytype_hoe OCC_TYPE ON PDH.OCCUPANCY_stg =OCC_TYPE.ID_stg 
left join DB_T_PROD_STAG.pctl_dwellingusage_hoe pcdh on pcdh.id_stg =pdh.DwellingUsage_stg 
join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha on pha.BranchID_stg = pp.ID_stg
join  DB_T_PROD_STAG.pcx_homeownerslinecov_hoe phh on phh.BranchID_stg = pp.ID_stg
left join  DB_T_PROD_STAG.pc_effectivedatedfields eff   on eff.BranchID_stg = pp.ID_stg and eff.expirationdate_stg is null
join DB_T_PROD_STAG.pc_policylocation pl on eff.primarylocation_stg = pl.id_stg
join DB_T_PROD_STAG.pc_policyline ppl1 on pp.id_stg = ppl1.BranchID_stg  
left join DB_T_PROD_STAG.pc_formpattern c on c.clausepatterncode_stg = phh.patterncode_stg
left join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = phh.branchid_stg
LEFT JOIN DB_T_PROD_STAG.pctl_constructiontype_hoe PCT ON PCT.ID_stg=constructiontype_stg 
join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pp.status_stg=pps.ID_stg and pps.typecode_stg=''Bound'' 
join DB_T_PROD_STAG.pc_policyterm pt on pt.ID_stg = pp.PolicyTermID_stg 
join DB_T_PROD_STAG.pc_policyline ppl on pp.id_stg = ppl.BranchID_stg 
join DB_T_PROD_STAG.pctl_hopolicytype_hoe pchh on ppl.HOPolicyType_stg = pchh.ID_stg and pchh.TypeCode_stg like ''HO%'' 
and Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 
    then pp.EditEffectiveDate_stg 
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 
 else pp.ModelDate_stg end  between :PC_BOY and :PC_EOY 
        AND JD.TYPECODE_STG =''AL'' and d.FormNumber_stg=''HO72'')A GROUP BY 1,2,4,5,6,7,8
HAVING SUM(writtenPremium)<>0
) SRC
)
);


-- Component exp_Pass_Through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Pass_Through AS
(
SELECT
SQ_pc_policyperiod.UW_Company as UW_Company,
SQ_pc_policyperiod.PolicyNumber as PolicyNumber,
SQ_pc_policyperiod.Premium as Premium,
SQ_pc_policyperiod.PolicyType as PolicyType,
SQ_pc_policyperiod.State as State,
SQ_pc_policyperiod.County as County,
SQ_pc_policyperiod.ModelDate as ModelDate,
SQ_pc_policyperiod.EffectiveDate as EffectiveDate,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component srt_PolicyNumber, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE srt_PolicyNumber AS
(
SELECT
exp_Pass_Through.UW_Company as UW_Company,
exp_Pass_Through.PolicyNumber as PolicyNumber,
exp_Pass_Through.Premium as Premium,
exp_Pass_Through.PolicyType as PolicyType,
exp_Pass_Through.State as State,
exp_Pass_Through.County as County,
exp_Pass_Through.ModelDate as ModelDate,
exp_Pass_Through.EffectiveDate as EffectiveDate,
exp_Pass_Through.source_record_id
FROM
exp_Pass_Through
ORDER BY PolicyNumber 
);


-- Component FF_AIUA_AL_HO72_Endorsement, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_AIUA_AL_HO72_Endorsement AS
(
SELECT
srt_PolicyNumber.UW_Company as UW_Company,
srt_PolicyNumber.PolicyNumber as PolicyName,
srt_PolicyNumber.Premium as Premium,
srt_PolicyNumber.PolicyType as PolicyType,
srt_PolicyNumber.State as State,
srt_PolicyNumber.County as County,
srt_PolicyNumber.ModelDate as ModelDate,
srt_PolicyNumber.EffectiveDate as EffectiveDate
FROM
srt_PolicyNumber
);

copy into @my_internal_stage/FF_AIUA_AL_HO72_Endorsement from (select * from FF_AIUA_AL_HO72_Endorsement)
header=true
overwrite=true;

-- Component FF_AIUA_AL_HO72_Endorsement, Type EXPORT_DATA Exporting data
;


END; ';