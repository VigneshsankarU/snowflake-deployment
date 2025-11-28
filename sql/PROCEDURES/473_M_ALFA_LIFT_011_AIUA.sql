-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_AIUA("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE SRC_STATUS_CD_ERR varchar;
SRC_STATUS_CD_PRC varchar;
BEGIN 

SRC_STATUS_CD_ERR:='' ''; 
SRC_STATUS_CD_PRC:='' ''; 

-- Component SQ_AIUA, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AIUA AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicyNumber,
$2 as PolicyType,
$3 as FirstName,
$4 as LastName,
$5 as CompanyName,
$6 as City,
$7 as PostalCode,
$8 as Location,
$9 as Acct_date,
$10 as Eff_Date,
$11 as Exp_Date,
$12 as Ann_Wrt_Prem,
$13 as Chg_Add_Prem,
$14 as Chg_Ret_Prem,
$15 as Can_Ret_Prem,
$16 as TransactionType,
$17 as UWcompanyCode,
$18 as Prem_Trans_Eff_Date,
$19 as jobnumber_stg,
$20 as LOB,
$21 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH dates as (select
distinct cast(extract(year from 
CASE WHEN LKPTBL.StringValue_STG=''0'' OR LKPTBL.StringValue_STG IS NULL  THEN CURRENT_TIMESTAMP ELSE 
CAST(CAST(current_date+cast(StringValue_STG as decimal(35,0))/(3600*24*1000) AS VARCHAR(10) ) || '' ''||
CAST(lpad(cast(extract(hour from current_timestamp)+1 as varchar(2)),2,''0'')||'':''||
lpad(cast(extract(MINUTE from current_timestamp) as varchar(2)),2,''0'') ||'':''||
lpad(cast(extract(SECOND from current_timestamp) as varchar(2)),2,''0'') AS VARCHAR(8)) AS TIMESTAMP(6)) 
-- -36 -- - interval ''36'' second 
END)
-- -1
||''-01-01'' as date) AS firstdayofyear,
cast(extract(year from 
CASE WHEN LKPTBL.StringValue_STG=''0'' OR LKPTBL.StringValue_STG IS NULL  THEN CURRENT_TIMESTAMP ELSE 
CAST(CAST(current_date+cast(StringValue_STG as decimal(35,0))/(3600*24*1000) AS VARCHAR(10) ) || '' ''||
CAST(lpad(cast(extract(hour from current_timestamp)+1 as varchar(2)),2,''0'')||'':''||
lpad(cast(extract(MINUTE from current_timestamp) as varchar(2)),2,''0'') ||'':''||
lpad(cast(extract(SECOND from current_timestamp) as varchar(2)),2,''0'') AS VARCHAR(8)) AS TIMESTAMP(6))
-- -36 -- - interval ''36'' second 
END)
-- -1
||''-12-31'' as date) AS lastdayofyear,
extract(year from 
CASE WHEN LKPTBL.StringValue_STG=''0'' OR LKPTBL.StringValue_STG IS NULL  THEN CURRENT_TIMESTAMP ELSE 
CAST(CAST(current_date+cast(StringValue_STG as decimal(35,0))/(3600*24*1000) AS VARCHAR(10) ) || '' ''||
CAST(lpad(cast(extract(hour from current_timestamp)+1 as varchar(2)),2,''0'')||'':''||
lpad(cast(extract(MINUTE from current_timestamp) as varchar(2)),2,''0'') ||'':''||
lpad(cast(extract(SECOND from current_timestamp) as varchar(2)),2,''0'') AS VARCHAR(8)) AS TIMESTAMP(6))
-- -36 -- - interval ''36'' second 
END)
-- -1 
AS lastyear
FROM DB_T_PROD_STAG.pc_parameter LKPTBL WHERE ParameterName_STG = ''TestingClock:CurrentTime''  )



select 
PolicyNumber,
PolicyType, 
FirstName,
LastName,
CompanyName,
City,
PostalCode,
Location, 
ACCT_Date,
eff_date,
exp_date, 
case when TransactionType in (''R'',''N'') then Premium_stg else NULL end as Ann_Wrt_Prem, 
case when TransactionType in (''RI'',''E'') and Premium_stg>0 then Premium_stg else NULL end as Chg_Add_Prem, 
case when TransactionType=''E'' and Premium_stg<0  then Premium_stg else NULL end as Chg_Ret_Prem, 
case when TransactionType=''C'' then Premium_stg else NULL end as Can_Ret_Prem, 
TransactionType,
UWcompanyCode,
case when TransactionType in (''R'',''N'') then NULL else Prem_Trans_Eff_dt end as  Prem_Trans_Eff_dt ,
jobnumber_stg ,
LOB
from
(
select
Prem_Trans_Eff_dt,
addressline1 as Location, 
city, 
postalcode as PostalCode, 
PolicyNumber_stg as PolicyNumber,
firstname_stg as FirstName,
lastname_stg as LastName,
CAST(null AS DECIMAL(15,3)) as PolicySuffix, 
TYPECODE as PolicyType,
sum(Amount) as Premium_stg, 
JOB as TransactionType,                                 
cast(periodstart_stg as date) as eff_date, 
cast(periodend_stg as date) as exp_date, 
ACCT_DT as ACCT_Date,
CompanyName,                                
null NAICCode, 
CompanyCode as UWcompanyCode, 
jobnumber_stg,
LOB  
from (                              
	select distinct                                 
		PolicyNumber_stg, 
		TermNumber_stg,                               
		pp.ModelNumber_stg, 
		jobtl.name_stg, 
		pp.PeriodStart_stg, 
		pp.PeriodEnd_stg, 
		pt.ConfirmationDate_alfa_stg, 
		pp.ModelDate_stg,                               
		pp.CancellationDate_stg, 
		hotl.typecode_stg as "Policy Type", 
		cnt.firstname_stg,
		cnt.lastname_stg,
		pp.primaryinsuredname_stg as CompanyName,                                 
		case when ploc.AddressLine2Internal_stg is null then ploc.AddressLine1Internal_stg                              
		else ploc.AddressLine1Internal_stg || '' '' || ploc.AddressLine2Internal_stg                                
		end as addressline1,                                             
		ploc.countyinternal_stg as county,                              
		ploc.cityinternal_stg as city,                              
		ploc.postalcodeinternal_stg as postalcode,                              
		ROOFYEAR_ALFA_stg, 
		hotl2.yearbuilt_stg,                                 
		pol.OriginalEffectiveDate_stg,                              
		addrtl.name_stg as AddressType,                             
		pp.id_stg,      hotsc.BranchID_stg,                            
		n.DESCRIPTION_stg "State",                              
		case when hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'') then ''3''                              
		when hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') then ''1''                                
		when bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') then ''4''                             
		end as TYPECODE,
		case when hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') then ''HO''
		when hotl.TYPECODE_stg in(''MH3'',''MH4'',''MH7'',''MH9'') then ''MH''                              
		when hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') then ''SF''                                
		when bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') then ''SM''                             
		end as LOB,
		hotl.TYPECODE_stg as HOTYPECODE, 
		bp7.TYPECODE_stg as BOPTYPECODE,                             
		case when uwco.Name_stg=''Alfa Mutual Insurance Company'' then ''19135''                               
		when uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' then ''19143''                               
		when uwco.Name_stg=''Alfa Mutual General Insurance Company'' then ''19151''                                
		when uwco.Name_stg=''Alfa Insurance Corporation'' then ''22330''                               
		when uwco.Name_stg=''Alfa General Insurance Corporation'' then ''41661''                               
		else uwco.Name_stg                             
		end as "NAICCode",                                 
		case when uwco.Name_stg=''Alfa Mutual Insurance Company'' then ''AMI''                                                                
		when uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' then ''AMF''                                                             
		when uwco.Name_stg=''Alfa Mutual General Insurance Company'' then ''AMG''                                                               
		when uwco.Name_stg=''Alfa General Insurance Corporation'' then ''AGI''                                                               
		else uwco.Name_stg                                                              
		end as CompanyCode,                            
		pp.TotalPremiumRPT_stg as "Premium", 
		job.closedate_stg,                                 
		Coalesce(hotsc.Amount_stg,0) as Amount,                                
		pp.editeffectivedate_stg as Prem_Trans_Eff_dt,                               
		case when jobtl.TYPECODE_stg=''Cancellation'' then ''C''                               
			when jobtl.TYPECODE_stg=''PolicyChange'' then ''E''                           
			when jobtl.TYPECODE_stg=''Submission'' then ''N''                         
			when jobtl.TYPECODE_stg in(''Renewal'',''Rewrite'') then ''R''                          
			when jobtl.TYPECODE_stg in(''Reinstatement'') then ''RI''                          
			else jobtl.TYPECODE_stg                           
		end as JOB,
		case when (coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
					cast(''1900-01-01 00:00:00.000000'' as timestamp))>pp.editeffectivedate_stg and coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
					cast(''1900-01-01 00:00:00.000000'' as timestamp))>pp.ModelDate_stg) then pt.ConfirmationDate_alfa_stg
			when (pp.editeffectivedate_stg>coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
					cast(''1900-01-01 00:00:00.000000'' as timestamp)) and pp.editeffectivedate_stg>pp.ModelDate_stg) then pp.editeffectivedate_stg
			when (pp.ModelDate_stg>pp.editeffectivedate_stg and pp.ModelDate_stg>coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
					cast(''1900-01-01 00:00:00.000000'' as timestamp))) then pp.ModelDate_stg
		else null end as ACCT_DT,
		etlpat.name_stg as "wind",
		zz.BooleanTerm1_stg,
		exc.PatternCode_stg,
		jobnumber_stg,
		hotsc.id_stg as hotscID   
from (select *	from DB_T_PROD_STAG.pc_policyperiod limit 1) pp                             
left join DB_T_PROD_STAG.pcx_hotransaction_hoe hotsc on hotsc.BranchID_stg = pp.ID_stg                                                           
left join DB_T_PROD_STAG.pc_effectivedatedfields edf on edf.BranchID_stg = pp.ID_stg                               
left join DB_T_PROD_STAG.pcx_holocation_hoe holoc on holoc.BranchID_stg = pp.ID_stg                             
left join DB_T_PROD_STAG.pcx_bp7location bp7loc on bp7loc.branchid_stg=pp.id_stg                               
left join DB_T_PROD_STAG.pc_policylocation ploc on ploc.BranchID_stg = pp.ID_stg and ploc.FixedID_stg = holoc.PolicyLocation_stg and ploc.expirationdate_stg is null                               
left join DB_T_PROD_STAG.pc_policylocation plocBOP on plocBOP.BranchID_stg = pp.ID_stg and plocBOP.FixedID_stg = bp7loc.Location_stg and plocBOP.expirationdate_stg is null                                
left join DB_T_PROD_STAG.pc_job job on job.id_stg = pp.JobID_stg                                
left join DB_T_PROD_STAG.pctl_job jobtl on jobtl.id_stg = job.Subtype_stg                               
left join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL                            
left join DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl on hotl.id_stg = pl.HOPolicyType_stg                                
left join DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 on bp7.id_stg = pl.BP7PolicyType_alfa_stg                               
--select * from DB_T_SHRD_PROD.state
left join DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state on state.st_cd = cast(pp.basestate_stg as string)                              
left join DB_T_PROD_STAG.pc_uwcompany uwco on uwco.id_stg = pp.UWCompany_stg                               
left join DB_T_PROD_STAG.pc_policyterm pt on pt.id_stg = pp.PolicyTermID_stg                              
left Join DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 on hotl2.branchid_stg = pp.ID_stg and hotl2.ExpirationDate_stg is NULL                             
left join DB_T_PROD_STAG.pc_policy pol on pol.id_stg = pp.PolicyID_stg
left join DB_T_PROD_STAG.pc_policyaddress padd on padd.BranchID_stg = pp.id_stg                         
left join DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr on padd.address_stg = addr.addr_id                           
left join DB_T_PROD_STAG.pctl_state n on n.id_stg = ploc.Stateinternal_stg                        
left join DB_T_PROD_STAG.pctl_addresstype addrtl on addrtl.ID_stg=ploc.addresstypeinternal_stg                           
left join DB_T_PROD_STAG.pcx_dwellingcov_hoe zz on zz.branchid_stg=pp.id_stg and zz.ExpirationDate_stg is NULL                               
left join DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg
left join DB_T_PROD_STAG.pc_etlcovtermpattern etlpat on etlpat.ClausePatternID_stg=ETL.ID_stg
left join DB_T_PROD_STAG.pcx_bp7building bld on pp.ID_stg = bld.BranchID_stg
left join DB_T_PROD_STAG.pcx_bp7buildingexcl exc on bld.ID_stg = exc.Building_stg   
LEFT JOIN DB_T_PROD_STAG.pc_contact cnt on PP.PNIContactDenorm_stg = cnt.id_stg  
join dates on 1=1
where pp.status_stg = 9                                                            
and hotsc.id_stg is not null                                   
and (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' and (zz.BooleanTerm1_stg=0 or zz.BooleanTerm1_stg is null))                               
and (ploc.postalcodeinternal_stg not like''36502%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36505%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36521%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36522%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36550%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36560%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36562%'' or ploc.postalcodeinternal_stg is null)                               
and (ploc.postalcodeinternal_stg not like''36579%'' or ploc.postalcodeinternal_stg is null)                                  
and n.DESCRIPTION_stg=''Alabama''                             
and ploc.countyinternal_stg in (''MOBILE'',''BALDWIN'') 
and (hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'')  or bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') )                                
and ((hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') or hotl.TYPECODE_stg is null) or (bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') or bp7.TYPECODE_stg is null))                                
and (ploc.Latitude_stg<=31 or ploc.Latitude_stg is null)                                
and (plocBOP.Latitude_stg<=31 or plocBOP.Latitude_stg is null)
and (hotl.TYPECODE_stg is not null or bp7.TYPECODE_stg is not null)
and (hotl.TYPECODE_stg not in(''PAF'',''CPL'') or hotl.TYPECODE_stg is null)
and ploc.locationnum_stg=(select min(ploc2.locationnum_stg) from DB_T_PROD_STAG.pc_policyperiod pp2                                
	left join DB_T_PROD_STAG.pc_job j2 on j2.ID_stg = pp2.JobID_stg                                
	left join DB_T_PROD_STAG.pcx_holocation_hoe holoc2 on holoc2.BranchID_stg = pp2.ID_stg
	left join DB_T_PROD_STAG.pc_policylocation ploc2 on ploc2.BranchID_stg = pp2.ID_stg and ploc2.FixedID_stg = holoc2.PolicyLocation_stg and ploc2.expirationdate_stg is null                             
	where pp2.TermNumber_stg = pp.TermNumber_stg                                
	and j2.jobnumber_stg=job.jobnumber_stg)
	and pp.PeriodStart_stg >=DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
  --  -1 -- -interval ''1'' year
and ((cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))                               
	or (cast(pp.ModelDate_stg as date)  between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))and cast(pp.EditEffectiveDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)))                             
	or (cast(pp.ModelDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))                              
	or (cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))                               
	and exists                              
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
			join dates on 1=1
			where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
			and pp2.TermNumber_stg = pp1.TermNumber_stg
			and jtl1.Name_stg = ''Renewal''                               
			and cast(ConfirmationDate_alfa_stg as date) >= DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))                              
		)
	and status_stg = 9                              
	)
))                             
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
join dates on 1=1
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl2.Name_stg = ''Renewal''                               
and status_stg = 9                              
and ((ConfirmationDate_alfa_stg is NULL) or (cast(ConfirmationDate_alfa_stg as date) >  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))))                               
))                                               
) cte                               
group by PolicyNumber,
Location, 
city, 
PostalCode, 
FirstName,
LastName,
PolicySuffix, 
PolicyType,
TransactionType,                                 
eff_date, 
exp_date, 
Prem_Trans_Eff_dt,
ACCT_Date,
CompanyName,
NAICCode, 
UWcompanyCode, 
jobnumber_stg,
LOB
)abc 
union
select 
PolicyNumber,
PolicyType,
FirstName,
LastName,
CompanyName,
city, 
PostalCode,
Location,   
acct_date,
eff_date, 
end_date,
case when TransactionType in (''R'',''N'') then Premium_stg else NULL end as Ann_Wrt_Prem, 
case when TransactionType in (''RI'',''E'') and Premium_stg>0 then Premium_stg else NULL end as Chg_Add_Prem, 
case when TransactionType=''E'' and Premium_stg<0  then Premium_stg else NULL end as Chg_Ret_Prem, 
case when TransactionType=''C'' then Premium_stg else NULL end as Can_Ret_Prem, 
TransactionType,                                                                 
UWcompanyCode,
case when TransactionType in (''R'',''N'') then NULL else Prem_Trans_Eff_dt end as  Prem_Trans_Eff_dt ,
jobnumber_stg,
LOB
 from
(
select 
PolicyNumber,
TYPECODE_stg as PolicyType,
firstname_stg as FirstName,
lastname_stg as LastName,
CompanyName,
Street as Location, 
CAST(null AS DECIMAL(15,3)) PolicySuffix, 
city_stg as city, 
postalcode_stg as PostalCode, 
acct_dt as acct_date,
Prem_Trans_Eff_dt as eff_date, 
periodend_stg as end_date,
sum(Amount_stg) as Premium_stg, 
JOB_stg as TransactionType,                                                                 
CompanyCode as UWcompanyCode,
lob,
Prem_Trans_Eff_dt   ,
jobnumber_stg
from (  
select distinct                                                                 
case when plocBOP.AddressLine2Internal_stg is null then plocBOP.AddressLine1Internal_stg                                                                
else plocBOP.AddressLine1Internal_stg || '' '' || plocBOP.AddressLine2Internal_stg                                                                
end as Street,                                                                
pp.PolicyNumber_stg as PolicyNumber, 
TermNumber_stg,                                                                
pp.ModelNumber_stg, 
jobtl.name_stg, 
pp.PeriodStart_stg, 
pp.PeriodEnd_stg, 
pt.ConfirmationDate_alfa_stg, 
pp.ModelDate_stg,                                                               
pp.CancellationDate_stg, 
hotl.typecode_stg as "Policy Type_stg", 
pp.primaryinsuredname_stg as CompanyName, 
cnt.firstname_stg,
cnt.lastname_stg,
plocBOP.countyinternal_stg as county_stg,                                                               
plocBOP.cityinternal_stg as city_stg,                                                               
left(plocBOP.postalcodeinternaldenorm_stg,5) as postalcode_stg,                                                             
ROOFYEAR_ALFA_stg, 
hotl2.yearbuilt_stg,                                                                 
pol.OriginalEffectiveDate_stg,                                                              
addrtl.name_stg as AddressType_stg,
pp.id_stg as ppID,                                                               
n.DESCRIPTION_stg "State_stg",                                                              
 case when hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'') then ''3''                                                              
when hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') then ''1''                                                             
when bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') then ''4''                                                              
end as TYPECODE_stg,
  case when hotl.TYPECODE_stg in(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') then ''HO''
  when hotl.TYPECODE_stg in(''MH3'',''MH4'',''MH7'',''MH9'') then ''MH''                              
 when hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') then ''SF''                                
 when bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') then ''SM''                             
 end as LOB,
hotl.TYPECODE_stg as HOTYPECODE_stg, 
bp7.TYPECODE_stg as BOPTYPECODE_stg,                                                              
case when uwco.Name_stg=''Alfa Mutual Insurance Company'' then ''19135''                                                                
when uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' then ''19143''                                                                
when uwco.Name_stg=''Alfa Mutual General Insurance Company'' then ''19151''                                                             
when uwco.Name_stg=''Alfa Insurance Corporation'' then ''22330''                                                                
when uwco.Name_stg=''Alfa General Insurance Corporation'' then ''41661''                                                                
else uwco.Name_stg                                                              
end as "NAICCode_stg",                                                              
case when uwco.Name_stg=''Alfa Mutual Insurance Company'' then ''AMI''                                                                
when uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' then ''AMF''                                                             
when uwco.Name_stg=''Alfa Mutual General Insurance Company'' then ''AMG''                                                               
when uwco.Name_stg=''Alfa General Insurance Corporation'' then ''AGI''                                                               
else uwco.Name_stg                                                              
end as CompanyCode,                                                              
pp.TotalPremiumRPT_stg as "Premium_stg", 
job.closedate_stg,                                                                                                                              
case                                                              
  when bldcov.building_stg = bld.fixedid_stg then Coalesce(hotsc.Amount_stg,0)                                                              
  when cls.building_stg = bld.fixedid_stg then Coalesce(hotsc.Amount_stg,0)                                                              
  when loccov.location_stg=bp7loc.fixedid_stg then Coalesce(hotsc.Amount_stg,0)                                                                                                                           
  when schd.id_stg is not null and bp7loc.location_stg=edf.PrimaryLocation_stg then Coalesce(hotsc.Amount_stg,0)                                                             
  when bp7loc.location_stg=edf.PrimaryLocation_stg and linecov.id_stg is not null                                                             
  then Coalesce(hotsc.Amount_stg,0)                                                              
  else  0                                                              
end as Amount_stg,                                                             
case when bldcov.building_stg = bld.fixedid_stg then Coalesce(hotsc.Amount_stg,0) else 0 end as Line1,                                                             
case when cls.building_stg = bld.fixedid_stg then Coalesce(hotsc.Amount_stg,0) else 0 end as Line2,                                                             
case when loccov.location_stg=bp7loc.fixedid_stg then Coalesce(hotsc.Amount_stg,0) else 0 end as Line3,                                                                                                                           
case 
  when bp7loc.location_stg=edf.PrimaryLocation_stg and linecov.id_stg is not null                                                                
  then Coalesce(hotsc.Amount_stg,0) else 0 end as Line4,                                                             
case when schd.id_stg is not null and bp7loc.location_stg=edf.PrimaryLocation_stg then Coalesce(hotsc.Amount_stg,0) end as line5,                                                               
Coalesce(hotsc.Amount_stg,0) as amountALL,                                                             
pp.editeffectivedate_stg as Prem_Trans_Eff_dt, 
case when (coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
            cast(''1900-01-01 00:00:00.000000'' as timestamp))>pp.editeffectivedate_stg and coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
            cast(''1900-01-01 00:00:00.000000'' as timestamp))>pp.ModelDate_stg) then pt.ConfirmationDate_alfa_stg
	 when (pp.editeffectivedate_stg>coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
            cast(''1900-01-01 00:00:00.000000'' as timestamp)) and pp.editeffectivedate_stg>pp.ModelDate_stg) then pp.editeffectivedate_stg
	 when (pp.ModelDate_stg>pp.editeffectivedate_stg and pp.ModelDate_stg>coalesce(cast(PT.ConfirmationDate_alfa_stg as timestamp),
            cast(''1900-01-01 00:00:00.000000'' as timestamp))) then pp.ModelDate_stg
else null end as ACCT_DT,
case when jobtl.TYPECODE_stg=''Cancellation'' then ''C''
      when jobtl.TYPECODE_stg=''PolicyChange'' then ''E''
      when jobtl.TYPECODE_stg=''Submission'' then ''N''
      when jobtl.TYPECODE_stg in (''Renewal'',''Rewrite'') then ''R''
      when jobtl.TYPECODE_stg in (''Reinstatement'') then ''RI''
      else jobtl.TYPECODE_stg
end as JOB_stg
, job.jobnumber_stg
, hotsc.id_stg as hotscID
from DB_T_PROD_STAG.pc_policyperiod pp
left join DB_T_PROD_STAG.pc_effectivedatedfields edf on edf.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pcx_bp7building bld on pp.id_stg = bld.branchid_stg and bld.expirationdate_stg is null
left join DB_T_PROD_STAG.pcx_bp7location bp7loc on bp7loc.id_stg=bld.location_stg
left join DB_T_PROD_STAG.pc_policylocation plocBOP on plocBOP.ID_stg = bp7loc.location_stg
left join DB_T_PROD_STAG.pc_job job on job.id_stg = pp.JobID_stg
left join DB_T_PROD_STAG.pctl_job jobtl on jobtl.id_stg = job.Subtype_stg
left join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL
left join DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl on hotl.id_stg = pl.HOPolicyType_stg
left join DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 on bp7.id_stg = pl.BP7PolicyType_alfa_stg
left join DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state on state.st_cd = cast(pp.basestate_stg as string)
left join DB_T_PROD_STAG.pc_uwcompany uwco on uwco.id_stg = pp.UWCompany_stg
left join DB_T_PROD_STAG.pc_policyterm pt on pt.id_stg = pp.PolicyTermID_stg
left Join DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 on hotl2.branchid_stg = pp.ID_stg and hotl2.ExpirationDate_stg is NULL
--select * from DB_V_PROD_BASE.addr
left join DB_T_PROD_STAG.pc_policy pol on pol.id_stg = pp.PolicyID_stg
left join DB_T_PROD_STAG.pc_policyaddress padd on padd.BranchID_stg = pp.id_stg
left join DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr on padd.address_stg = addr.addr_id
left join DB_T_PROD_STAG.pctl_state n on n.id_stg = plocBOP.Stateinternal_stg
left join DB_T_PROD_STAG.pctl_addresstype addrtl on addrtl.ID_stg=plocBOP.addresstypeinternal_stg
left join DB_T_PROD_STAG.pcx_dwellingcov_hoe zz on zz.branchid_stg=pp.id_stg and zz.ExpirationDate_stg is NULL
left join DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg
left join DB_T_PROD_STAG.pc_etlcovtermpattern etlpat on etlpat.ClausePatternID_stg=ETL.ID_stg
left join DB_T_PROD_STAG.pc_address addr2 on addr2.ID_stg = plocBOP.AccountLocation_stg
left join DB_T_PROD_STAG.pc_policycontactrole pcr on pcr.BranchID_stg = pp.id_stg
left join DB_T_PROD_STAG.pc_contact cnt on cnt.id_stg = PP.PNIContactDenorm_stg
left join DB_T_PROD_STAG.pc_address adr on adr.id_stg = cnt.PrimaryAddressID_stg
left join DB_T_PROD_STAG.pcx_bp7buildingexcl exc on  exc.building_stg = bld.id_stg
left join DB_T_PROD_STAG.pcx_bp7buildingexcl exc2 on exc2.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pcx_bp7transaction hotsc on hotsc.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pcx_bp7cost e on e.id_stg = hotsc.bp7cost_stg
left join DB_T_PROD_STAG.pcx_bp7buildingcov BldCov on BldCov.id_stg = e.BuildingCov_stg and bldcov.building_stg=bld.fixedid_stg
left join DB_T_PROD_STAG.pcx_bp7linecov Linecov on Linecov.id_stg = e.LineCoverage_stg
left join DB_T_PROD_STAG.pcx_bp7locationcov LocCov on LocCov.id_stg =e.locationcov_stg
left join DB_T_PROD_STAG.pcx_bp7classificationcov classcov on classcov.id_stg =e.classificationcov_stg
left join DB_T_PROD_STAG.pcx_bp7classification cls on classcov.Classification_stg = cls.id_stg and bld.fixedid_stg=cls.building_stg
left join DB_T_PROD_STAG.pcx_bp7locschedcovitem schditem on schditem.Schedule_stg = LocCov.FixedID_stg
left join DB_T_PROD_STAG.pcx_bp7locschedcovitemcov schd on schd.id_stg = e.LocSchedCovItemCov_stg
left join DB_T_PROD_STAG.pctl_bp7whatisinsured_alfa WIS on WIS.id_stg=pl.BP7WhatIsInsured_alfa_stg
join dates on 1=1
where pp.status_stg = 9
and WIS.name_stg<>''Liability Only''
and bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'')
and (plocBOP.postalcodeinternaldenorm_stg not like''36502%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36505%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36521%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36522%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36550%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36560%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36562%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and (plocBOP.postalcodeinternaldenorm_stg not like''36579%'' or plocBOP.postalcodeinternaldenorm_stg is null)
and n.DESCRIPTION_stg=''Alabama''
and plocBOP.countyinternal_stg in (''MOBILE'',''BALDWIN'')
and pp.PeriodStart_stg >=DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
-- -1 -- -interval ''1'' year
and ((cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date)  between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))and cast(pp.EditEffectiveDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
and exists
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
join dates on 1=1
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl1.Name_stg = ''Renewal''
and cast(ConfirmationDate_alfa_stg as date) >= DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
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
join dates on 1=1
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl2.Name_stg = ''Renewal''
and status_stg = 9
and ((ConfirmationDate_alfa_stg is NULL) or (cast(ConfirmationDate_alfa_stg as date) >  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))))
))
and not exists
(
select distinct
plocBOP2.AddressLine1Internal_stg
, job2.jobnumber_stg
from DB_T_PROD_STAG.pc_policyperiod pp
left join DB_T_PROD_STAG.pc_effectivedatedfields edf on edf.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pcx_bp7buildingexcl exc on exc.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pcx_bp7building bld on exc.building_stg = bld.id_stg
left join DB_T_PROD_STAG.pcx_bp7location bp7loc on bp7loc.id_stg=bld.location_stg
left join DB_T_PROD_STAG.pc_policylocation plocBOP2 on plocBOP2.ID_stg = bp7loc.location_stg
left join DB_T_PROD_STAG.pcx_bp7transaction hotsc on hotsc.BranchID_stg = pp.ID_stg
left join DB_T_PROD_STAG.pc_job job2 on job2.id_stg = pp.JobID_stg
left join DB_T_PROD_STAG.pctl_job jobtl on jobtl.id_stg = job2.Subtype_stg
left join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL
left join DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl on hotl.id_stg = pl.HOPolicyType_stg
left join DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 on bp7.id_stg = pl.BP7PolicyType_alfa_stg
left join DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state on state.st_cd = cast(pp.basestate_stg as string)
left join DB_T_PROD_STAG.pc_uwcompany uwco on uwco.id_stg = pp.UWCompany_stg
left join DB_T_PROD_STAG.pc_policyterm pt on pt.id_stg = pp.PolicyTermID_stg
left Join DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 on hotl2.branchid_stg = pp.ID_stg and hotl2.ExpirationDate_stg is NULL
left join DB_T_PROD_STAG.pc_policy pol on pol.id_stg = pp.PolicyID_stg
left join DB_T_PROD_STAG.pc_policyaddress padd on padd.BranchID_stg = pp.id_stg
left join DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr on padd.address_stg = addr.addr_id
left join DB_T_PROD_STAG.pctl_state n on n.id_stg = plocBOP2.Stateinternal_stg
left join DB_T_PROD_STAG.pctl_addresstype addrtl on addrtl.ID_stg=plocBOP2.addresstypeinternal_stg
left join DB_T_PROD_STAG.pcx_dwellingcov_hoe zz on zz.branchid_stg=pp.id_stg and zz.ExpirationDate_stg is NULL
left join DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg
left join DB_T_PROD_STAG.pc_etlcovtermpattern etlpat on etlpat.ClausePatternID_stg=ETL.ID_stg
left join DB_T_PROD_STAG.pcx_bp7locationcov loccov on loccov.BranchID_stg = pp.ID_stg and loccov.Location_stg = bp7loc.FixedID_stg
left join DB_T_PROD_STAG.pc_etlclausepattern ETL2 ON ETL2.PATTERNID_stg = loccov.PatternCode_stg
left join DB_T_PROD_STAG.pc_etlcovtermpattern etlpat2 on etlpat2.ClausePatternID_stg=ETL2.ID_stg
left join DB_T_PROD_STAG.pc_address addr2 on addr2.ID_stg = plocBOP2.AccountLocation_stg
left join DB_T_PROD_STAG.pc_policycontactrole pcr on pcr.BranchID_stg = pp.id_stg
left join DB_T_PROD_STAG.pc_contact cnt on cnt.id_stg = pcr.ContactDenorm_stg
left join DB_T_PROD_STAG.pc_address adr on adr.id_stg = cnt.PrimaryAddressID_stg
join dates on 1=1
where pp.status_stg = 9
and bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'')
and (plocBOP2.postalcodeinternaldenorm_stg not like''36502%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36505%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36521%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36522%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36550%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36560%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36562%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and (plocBOP2.postalcodeinternaldenorm_stg not like''36579%'' or plocBOP2.postalcodeinternaldenorm_stg is null)
and n.DESCRIPTION_stg=''Alabama''
and plocBOP2.countyinternal_stg in (''MOBILE'',''BALDWIN'')
and ((cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date)  between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))and cast(pp.EditEffectiveDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)) and cast(pp.EditEffectiveDate_stg as date) between DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp)) and  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp)))
or (cast(pp.ModelDate_stg as date) < DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
and exists
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
join dates on 1=1
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl1.Name_stg = ''Renewal''
and cast(ConfirmationDate_alfa_stg as date) >= DATE_TRUNC(''dd'', cast(dates.firstdayofyear as timestamp))
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
join dates on 1=1
where pp2.PolicyNumber_stg = pp1.PolicyNumber_stg
and pp2.TermNumber_stg = pp1.TermNumber_stg
and jtl2.Name_stg = ''Renewal''
and status_stg = 9
and ((ConfirmationDate_alfa_stg is NULL) or (cast(ConfirmationDate_alfa_stg as date) >  DATE_TRUNC(''dd'', cast(dates.lastdayofyear as timestamp))))
))
and plocBOP2.AddressLine1Internal_stg=plocBOP.AddressLine1Internal_stg
and job2.jobnumber_stg=job.jobnumber_stg)
) cte
group by
PolicyNumber,
PolicyType,
FirstName,
LastName,
CompanyName,
Location, 
PolicySuffix, 
city, 
PostalCode, 
acct_date,
eff_date, 
end_date, 
TransactionType,                                                                 
UWcompanyCode,
lob,
Prem_Trans_Eff_dt,
jobnumber_stg
) abc
) SRC
)
);


-- Component EXP_AIUA_DETAIL1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_AIUA_DETAIL1 AS
(
SELECT
NULL as ID,
RPAD ( SQ_AIUA.PolicyNumber , 25 , '' '' ) as out_PolicyNumber,
RPAD ( '' '' , 3 , '' '' ) as out_PolicySuffix,
LPAD ( to_char ( CASE WHEN SQ_AIUA.PolicyType IS NULL THEN 0 ELSE SQ_AIUA.PolicyType END ) , 1 , 0 ) as out_PolicyType,
RPAD ( CASE WHEN SQ_AIUA.LastName IS NULL or SQ_AIUA.LastName = '''' THEN SQ_AIUA.CompanyName ELSE SQ_AIUA.LastName END , 50 , '' '' ) as OUT_LastName,
RPAD ( CASE WHEN SQ_AIUA.FirstName IS NULL THEN '''' ELSE SQ_AIUA.FirstName END , 50 , '' '' ) as OUT_FirstName,
RPAD ( '' '' , 10 , '' '' ) as OUT_House_No,
RPAD ( '' '' , 1 , '' '' ) as OUT_Post,
SQ_AIUA.Location as Out_Street,
RPAD ( '' '' , 4 , '' '' ) as Out_Suffix,
RPAD ( SQ_AIUA.City , 25 , '' '' ) as Out_City,
RPAD ( SQ_AIUA.PostalCode , 5 , '' '' ) as Out_Zipcode,
to_char ( SQ_AIUA.Acct_date , ''MM/DD/YY'' ) as out_AcctDate,
to_char ( SQ_AIUA.Eff_Date , ''MM/DD/YY'' ) as out_Eff_Date,
to_char ( SQ_AIUA.Exp_Date , ''MM/DD/YY'' ) as out_Exp_Date,
to_char ( Round ( CASE WHEN SQ_AIUA.Ann_Wrt_Prem IS NULL THEN 0 ELSE SQ_AIUA.Ann_Wrt_Prem END , 2 ) ) as out_Ann_Wrt_Prem,
to_char ( Round ( CASE WHEN SQ_AIUA.Chg_Add_Prem IS NULL THEN 0 ELSE SQ_AIUA.Chg_Add_Prem END , 2 ) ) as out_Chg_Add_Prem,
to_char ( Round ( CASE WHEN SQ_AIUA.Chg_Ret_Prem IS NULL THEN 0 ELSE SQ_AIUA.Chg_Ret_Prem END , 2 ) ) as out_Chg_Ret_Prem,
to_char ( Round ( CASE WHEN SQ_AIUA.Can_Ret_Prem IS NULL THEN 0 ELSE SQ_AIUA.Can_Ret_Prem END , 2 ) ) as out_Can_Ret_Prem,
DECODE ( TRUE , SQ_AIUA.TransactionType = ''N'' , ''NB  '' , SQ_AIUA.TransactionType = ''R'' or SQ_AIUA.TransactionType = ''RI'' , ''RNL '' , SQ_AIUA.TransactionType = ''E'' , ''END '' , ''CANC'' ) as out_Transaction_Type,
CASE WHEN SQ_AIUA.UWcompanyCode IS NULL THEN RPAD ( '' '' , 3 , '' '' ) ELSE RPAD ( SQ_AIUA.UWcompanyCode , 3 , '' '' ) END as out_Company_Code,
CASE WHEN SQ_AIUA.PolicyNumber IS NULL THEN SRC_STATUS_CD_ERR ELSE SRC_STATUS_CD_PRC END as StatusCode,
to_char ( SQ_AIUA.Prem_Trans_Eff_Date , ''MM/DD/YY'' ) as OUT_Prem_Eff_Trans_Date,
RPAD ( SQ_AIUA.LOB , 20 , '' '' ) as LOB_o,
SQ_AIUA.source_record_id
FROM
SQ_AIUA
);


-- Component RTR_VALIDATION1_AIUA, Type ROUTER Output Group AIUA
SELECT
EXP_AIUA_DETAIL1.ID as ID,
EXP_AIUA_DETAIL1.out_PolicyNumber as out_PolicyNumber,
EXP_AIUA_DETAIL1.out_PolicySuffix as out_PolicySuffix,
EXP_AIUA_DETAIL1.out_PolicyType as out_PolicyType,
EXP_AIUA_DETAIL1.OUT_LastName as OUT_LastName,
EXP_AIUA_DETAIL1.OUT_FirstName as OUT_FirstName,
EXP_AIUA_DETAIL1.OUT_House_No as OUT_House_No,
EXP_AIUA_DETAIL1.OUT_Post as OUT_Post,
EXP_AIUA_DETAIL1.Out_Street as Out_Street,
EXP_AIUA_DETAIL1.Out_Suffix as Out_Suffix,
EXP_AIUA_DETAIL1.Out_City as Out_City,
EXP_AIUA_DETAIL1.Out_Zipcode as Out_Zipcode,
EXP_AIUA_DETAIL1.out_AcctDate as out_AcctDate,
EXP_AIUA_DETAIL1.out_Eff_Date as out_Eff_Date,
EXP_AIUA_DETAIL1.out_Exp_Date as out_Exp_Date,
EXP_AIUA_DETAIL1.out_Ann_Wrt_Prem as out_Ann_Wrt_Prem,
EXP_AIUA_DETAIL1.out_Chg_Add_Prem as out_Chg_Add_Prem,
EXP_AIUA_DETAIL1.out_Chg_Ret_Prem as out_Chg_Ret_Prem,
EXP_AIUA_DETAIL1.out_Can_Ret_Prem as out_Can_Ret_Prem,
EXP_AIUA_DETAIL1.out_Transaction_Type as out_Transaction_Type,
EXP_AIUA_DETAIL1.out_Company_Code as out_Company_Code,
EXP_AIUA_DETAIL1.LOB_o as out_LOB,
EXP_AIUA_DETAIL1.StatusCode as StatusCode,
EXP_AIUA_DETAIL1.OUT_Prem_Eff_Trans_Date as out_Prem_Trans_Eff_Date,
EXP_AIUA_DETAIL1.source_record_id
FROM
EXP_AIUA_DETAIL1
WHERE EXP_AIUA_DETAIL1.StatusCode = SRC_STATUS_CD_PRC;


-- Component FF_AIUA_DETAIL1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_AIUA_DETAIL1 AS
(
SELECT
RTR_VALIDATION1_AIUA.out_PolicyNumber as POL_NUMBER,
RTR_VALIDATION1_AIUA.out_PolicySuffix as POL_SUFFIX,
RTR_VALIDATION1_AIUA.out_PolicyType as POL_NAME,
RTR_VALIDATION1_AIUA.OUT_LastName as LAST_NAME,
RTR_VALIDATION1_AIUA.OUT_FirstName as FIRST_NAME,
RTR_VALIDATION1_AIUA.OUT_House_No as HOUSE_NO,
RTR_VALIDATION1_AIUA.OUT_Post as POST,
RTR_VALIDATION1_AIUA.Out_Street as STREET,
RTR_VALIDATION1_AIUA.Out_Suffix as SUFFIX,
RTR_VALIDATION1_AIUA.Out_City as CITY,
RTR_VALIDATION1_AIUA.Out_Zipcode as ZIP,
RTR_VALIDATION1_AIUA.out_AcctDate as ACCT_DATE,
RTR_VALIDATION1_AIUA.out_Eff_Date as EFF_DATE,
RTR_VALIDATION1_AIUA.out_Exp_Date as EXP_DATE,
RTR_VALIDATION1_AIUA.out_Ann_Wrt_Prem as ANN_WRT_PREM,
RTR_VALIDATION1_AIUA.out_Chg_Add_Prem as CHG_ADD_PREM,
RTR_VALIDATION1_AIUA.out_Chg_Ret_Prem as CHG_RET_PREM,
RTR_VALIDATION1_AIUA.out_Can_Ret_Prem as CAN_RET_PREM,
RTR_VALIDATION1_AIUA.out_Transaction_Type as TRANSACTION_TYPE,
RTR_VALIDATION1_AIUA.out_Company_Code as COMPANY_Code,
RTR_VALIDATION1_AIUA.out_LOB as LOB,
RTR_VALIDATION1_AIUA.out_Prem_Trans_Eff_Date as PREM_TRANS_EFF_DATE
FROM
RTR_VALIDATION1_AIUA
);

copy into @my_internal_stage/FF_AIUA_DETAIL1 from (select * from FF_AIUA_DETAIL1 limit 1)
header=true
overwrite=true;

-- Component FF_AIUA_DETAIL1, Type EXPORT_DATA Exporting data
;


END; ';