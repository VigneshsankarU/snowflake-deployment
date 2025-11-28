-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_MWUA("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE RUN_DATE date;
BEGIN 

set RUN_DATE:=''1900-01-01''; 

-- PIPELINE START FOR 1

-- Component SQ_MWUA_HEADER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MWUA_HEADER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select count(Policy_Number)  from (SELECT DISTINCT "Company Name_stg" AS "Company_Name"								
    , Policy_Suffix_stg AS "Policy_Suffix"								
    , "NAICCode_stg" AS "NAIC_Code"								
    , TYPECODE_stg AS "Policy_Type"								
    , PolicyNumber_stg AS "Policy_Number"								
    , primaryinsuredname_stg AS "Pimary_Insrd_Name"								
    , addressline1_stg AS "Address_Line"								
    , city_stg AS "City"								
    , county_stg AS "County"								
    , postalcode_stg AS "Zip_Code"								
    , periodstart_stg AS "Period_Start_Date" 								
    , periodend_stg AS "Period_End_Date"								
    , "Premium Transaction Effective Date_stg" AS "Prem_Trans_Eff_Date"								
    , JOB_stg AS "Prem_Trans_Type"								
    , SUM(Amount_stg) AS "Prem_Trans_Amt"								
    , jobnumber_stg AS "Job_number"								
								
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
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''HO''								
        WHEN hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''SF''								
        WHEN hotl.typecode_stg in (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''MH''								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''								
        END AS TYPECODE_stg								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''								
        ELSE uwco.Name_stg								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''								
        ELSE uwco.Name_stg								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , COALESCE(hotsc.Amount_stg,0) AS Amount_stg								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''								
        WHEN jobtl.TYPECODE_stg=''Reinstatement'' THEN ''REINSTATEMENT''								
        WHEN jobtl.TYPECODE_stg=''Rewrite'' THEN ''REWRITE''								
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
       AND plocBOP.expirationdate_stg is null								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg								
       AND pl.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg								
       AND hotl2.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = ploc.Stateinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=ploc.addresstypeinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg								
       AND zz.ExpirationDate_stg IS NULL								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL  ON ETL.PATTERNID_stg = zz.PatternCode_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.ID_stg = bld.BranchID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON bld.ID_stg = exc.Building_stg								
WHERE pp.status_stg = 9 								
    AND (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' 								
    AND (zz.BooleanTerm1_stg=0 OR zz.BooleanTerm1_stg IS NULL))								
    AND n.DESCRIPTION_stg=''Mississippi''								
    AND ((hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') OR hotl.TYPECODE_stg IS NULL) OR (bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'') OR bp7.TYPECODE_stg IS NULL))								
    AND (hotl.TYPECODE_stg IS NOT NULL OR bp7.TYPECODE_stg IS NOT NULL)								
    AND (hotl.TYPECODE_stg NOT IN(''PAF'',''CPL'') OR hotl.TYPECODE_stg IS NULL)								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31'' 								
								
) cte								
								
WHERE TYPECODE_stg IN (''HO'',''MH'',''SF'',''CMP'')								
    AND county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								
								
GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg, 								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", "NAICCode_stg", "Company Name_stg", BooleanTerm1_stg, jobnumber_stg, ProtClassCode								
								
								
								
UNION								
								
/* BOP/CHURCH								 */
SELECT DISTINCT "Company Name_stg" 								
    , Policy_Suffix_stg								
    , "NAICCode_stg"								
    , TYPECODE_stg AS "Policy Type_stg"								
    , PolicyNumber_stg								
    , primaryinsuredname_stg								
    , addressline1_stg								
    , city_stg								
    , county_stg								
    , postalcode_stg AS ZIP_stg								
/* , ProtClassCode								 */
    , periodstart_stg								
    , periodend_stg								
    , "Premium Transaction Effective Date_stg"								
    , JOB_stg AS "Premium Transaction_stg"								
    , SUM(Amount_stg) AS "Premium_stg"								
    , jobnumber_stg                                                    								
FROM (                                                              								
                                                                								
SELECT DISTINCT pp.PolicyNumber_stg								
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
    , CASE WHEN plocBOP.AddressLine2Internal_stg IS NULL THEN plocBOP.AddressLine1Internal_stg                                                                								
        ELSE plocBOP.AddressLine1Internal_stg || '' '' || plocBOP.AddressLine2Internal_stg                                                                								
        END AS addressline1_stg								
    , plocBOP.countyinternal_stg AS county_stg								
    , plocBOP.cityinternal_stg AS city_stg								
    , LEFT(plocBOP.postalcodeinternaldenorm_stg,5) AS postalcode_stg								
    , ROOFYEAR_ALFA_stg								
    , hotl2.yearbuilt_stg								
    , pol.OriginalEffectiveDate_stg								
    , addrtl.name_stg AS AddressType_stg								
    , pp.id_stg AS ppID								
    , n.DESCRIPTION_stg "State_stg"								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''H''								
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''F''								
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''T''								
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''SM''								
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN (''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'') THEN ''HO''                                                              								
        WHEN hotl.TYPECODE_stg IN (''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''FAL''                                                             								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''                                                              								
        END AS TYPECODE_stg                                                             								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''                                                             								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''                                                                								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''                                                                								
        ELSE uwco.Name_stg                                                              								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''                                                             								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''                                                               								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''                                                               								
        ELSE uwco.Name_stg                                                              								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0)                                                             								
        WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL                                                             								
        THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        ELSE  0                                                             								
        END AS Amount_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line1								
    , CASE WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line2								
    , CASE WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line3								
    , CASE WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line4								
    , CASE WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0) END AS line5								
    , COALESCE(hotsc.Amount_stg,0) AS amountALL								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''                                                                								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''                                                               								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''                                                                								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''                                                              								
        ELSE jobtl.TYPECODE_stg                                                               								
        END AS JOB_stg                                                              								
    , job.jobnumber_stg                                                             								
    , hotsc.id_stg as hotscID                                                               								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.id_stg = bld.branchid_stg AND bld.expirationdate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP ON plocBOP.ID_stg = bp7loc.location_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
        AND pl.ExpirationDate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
        AND hotl2.ExpirationDate_stg IS NULL                                                                 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP.Stateinternal_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP.addresstypeinternal_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
        AND zz.ExpirationDate_stg IS NULL                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP.AccountLocation_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.building_stg = bld.id_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc2 ON exc2.BranchID_stg = pp.ID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7cost e ON e.id_stg = hotsc.bp7cost_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingcov BldCov ON BldCov.id_stg = e.BuildingCov_stg 								
        AND bldcov.building_stg=bld.fixedid_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7linecov Linecov ON Linecov.id_stg = e.LineCoverage_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov LocCov ON LocCov.id_stg =e.locationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classificationcov classcov ON classcov.id_stg =e.classificationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classification cls ON classcov.Classification_stg = cls.id_stg AND bld.fixedid_stg=cls.building_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitem schditem ON schditem.Schedule_stg = LocCov.FixedID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitemcov schd ON schd.id_stg = e.LocSchedCovItemCov_stg                                                               								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg=''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''                                                         								
								
AND NOT EXISTS(                                                             								
                                                                								
SELECT DISTINCT plocBOP2.AddressLine1Internal_stg                                                                 								
    , job2.jobnumber_stg                                                                								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON exc.building_stg = bld.id_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP2 ON plocBOP2.ID_stg = bp7loc.location_stg                                                           								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_job job2 ON job2.id_stg = pp.JobID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job2.Subtype_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
    AND pl.ExpirationDate_stg IS NULL                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
    AND hotl2.ExpirationDate_stg IS NULL                                                                 								
LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP2.Stateinternal_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP2.addresstypeinternal_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
    AND zz.ExpirationDate_stg IS NULL                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov loccov ON loccov.BranchID_stg = pp.ID_stg 								
    AND loccov.Location_stg = bp7loc.FixedID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL2 ON ETL2.PATTERNID_stg = loccov.PatternCode_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat2 ON etlpat2.ClausePatternID_stg=ETL2.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP2.AccountLocation_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
                                                                								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND exc.expirationdate_stg IS NULL                                                              								
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg = ''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''                                                          								
AND plocBOP2.AddressLine1Internal_stg=plocBOP.AddressLine1Internal_stg                                                              								
AND job2.jobnumber_stg=job.jobnumber_stg)                                                                                                                            								
) cte                                                               								
                                                                 								
WHERE county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								

GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg,                                                               								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", /* Longitude, Latitude,                                                                								 */
"NAICCode_stg", "Company Name_stg", /*BooleanTerm1_stg,*/ jobnumber_stg/* , LocationNum_stg, LocationNum    */

                                                                								
) mwua							
where Prem_Trans_Amt <> 0.00
) SRC
)
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
''Company_Name'' as Company_Name,
''Policy_Suffix'' as Policy_Suffix,
''NAIC_Code'' as NAIC_Code,
''Policy_Type'' as Policy_Type,
Policy_Number as Policy_Number,
''Pimary_Insrd_Name'' as Pimary_Insrd_Name,
''Address_Line'' as Address_Line,
''City'' as City,
''County'' as County,
''Zip_Code'' as Zip_Code,
''Period_Start_Date'' as Period_Start_Date,
''Period_End_Date'' as Period_End_Date,
''Prem_Trans_Eff_Date'' as Prem_Trans_Eff_Date,
''Prem_Trans_Type'' as Prem_Trans_Type,
''Prem_Trans_Amt'' as Prem_Trans_Amt,
''Job_number'' as Job_number,
SQ_MWUA_HEADER.source_record_id
FROM
SQ_MWUA_HEADER
);


-- Component FF_MWUA_HEADER, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_MWUA_HEADER AS
(
SELECT
EXPTRANS1.Company_Name as Company_Name,
EXPTRANS1.Policy_Suffix as Policy_Suffix,
EXPTRANS1.NAIC_Code as NAIC_Code,
EXPTRANS1.Policy_Type as Policy_Type,
EXPTRANS1.Policy_Number as Policy_Number,
EXPTRANS1.Pimary_Insrd_Name as Pimary_Insrd_Name,
EXPTRANS1.Address_Line as Address_Line,
EXPTRANS1.City as City,
EXPTRANS1.County as County,
EXPTRANS1.Zip_Code as Zip_Code,
EXPTRANS1.Period_Start_Date as Period_Start_Date,
EXPTRANS1.Period_End_Date as Period_End_Date,
EXPTRANS1.Prem_Trans_Eff_Date as Prem_Trans_Eff_Date,
EXPTRANS1.Prem_Trans_Type as Prem_Trans_Type,
EXPTRANS1.Prem_Trans_Amt as Prem_Trans_Amt,
EXPTRANS1.Job_number as Job_number
FROM
EXPTRANS1
);


-- Component FF_MWUA_HEADER, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/FF_MWUA_HEADER_
FROM (SELECT * FROM FF_MWUA_HEADER)
HEADER = TRUE
OVERWRITE = TRUE;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_MWUA_DETAIL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MWUA_DETAIL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Company_Name,
$2 as Policy_Suffix,
$3 as NAIC_Code,
$4 as Policy_Type,
$5 as Policy_Number,
$6 as Pimary_Insrd_Name,
$7 as Address_Line,
$8 as City,
$9 as County,
$10 as Zip_Code,
$11 as Period_Start_Date,
$12 as Period_End_Date,
$13 as Prem_Trans_Eff_Date,
$14 as Prem_Trans_Type,
$15 as Prem_Trans_Amt,
$16 as Job_number,
$17 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select * from (SELECT DISTINCT "Company Name_stg" AS "Company_Name"								
    , Policy_Suffix_stg AS "Policy_Suffix"								
    , "NAICCode_stg" AS "NAIC_Code"								
    , TYPECODE_stg AS "Policy_Type"								
    , PolicyNumber_stg AS "Policy_Number"								
    , primaryinsuredname_stg AS "Pimary_Insrd_Name"								
    , addressline1_stg AS "Address_Line"								
    , city_stg AS "City"								
    , county_stg AS "County"								
    , postalcode_stg AS "Zip_Code"								
    , periodstart_stg AS "Period_Start_Date" 								
    , periodend_stg AS "Period_End_Date"								
    , "Premium Transaction Effective Date_stg" AS "Prem_Trans_Eff_Date"								
    , JOB_stg AS "Prem_Trans_Type"								
    , SUM(Amount_stg) AS "Prem_Trans_Amt"								
    , jobnumber_stg AS "Job_number"								
								
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
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''HO''								
        WHEN hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''SF''								
        WHEN hotl.typecode_stg in (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''MH''								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''								
        END AS TYPECODE_stg								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''								
        ELSE uwco.Name_stg								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''								
        ELSE uwco.Name_stg								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , COALESCE(hotsc.Amount_stg,0) AS Amount_stg								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''								
        WHEN jobtl.TYPECODE_stg=''Reinstatement'' THEN ''REINSTATEMENT''								
        WHEN jobtl.TYPECODE_stg=''Rewrite'' THEN ''REWRITE''								
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
       AND plocBOP.expirationdate_stg is null								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg								
       AND pl.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg								
       AND hotl2.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = ploc.Stateinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=ploc.addresstypeinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg								
       AND zz.ExpirationDate_stg IS NULL								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL  ON ETL.PATTERNID_stg = zz.PatternCode_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.ID_stg = bld.BranchID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON bld.ID_stg = exc.Building_stg								
WHERE pp.status_stg = 9 								
    AND (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' 								
    AND (zz.BooleanTerm1_stg=0 OR zz.BooleanTerm1_stg IS NULL))								
    AND n.DESCRIPTION_stg=''Mississippi''								
    AND ((hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') OR hotl.TYPECODE_stg IS NULL) OR (bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'') OR bp7.TYPECODE_stg IS NULL))								
    AND (hotl.TYPECODE_stg IS NOT NULL OR bp7.TYPECODE_stg IS NOT NULL)								
    AND (hotl.TYPECODE_stg NOT IN(''PAF'',''CPL'') OR hotl.TYPECODE_stg IS NULL)								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''    								
								
) cte								
								
WHERE TYPECODE_stg IN (''HO'',''MH'',''SF'',''CMP'')								
    AND county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								
								
GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg, 								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", "NAICCode_stg", "Company Name_stg", BooleanTerm1_stg, jobnumber_stg, ProtClassCode								
								
								
								
UNION								
								
/* BOP/CHURCH								 */
SELECT DISTINCT "Company Name_stg" 								
    , Policy_Suffix_stg								
    , "NAICCode_stg"								
    , TYPECODE_stg AS "Policy Type_stg"								
    , PolicyNumber_stg								
    , primaryinsuredname_stg								
    , addressline1_stg								
    , city_stg								
    , county_stg								
    , postalcode_stg AS ZIP_stg								
/* , ProtClassCode								 */
    , periodstart_stg								
    , periodend_stg								
    , "Premium Transaction Effective Date_stg"								
    , JOB_stg AS "Premium Transaction_stg"								
    , SUM(Amount_stg) AS "Premium_stg"								
    , jobnumber_stg                                                    								
FROM (                                                              								
                                                                								
SELECT DISTINCT pp.PolicyNumber_stg								
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
    , CASE WHEN plocBOP.AddressLine2Internal_stg IS NULL THEN plocBOP.AddressLine1Internal_stg                                                                								
        ELSE plocBOP.AddressLine1Internal_stg || '' '' || plocBOP.AddressLine2Internal_stg                                                                								
        END AS addressline1_stg								
    , plocBOP.countyinternal_stg AS county_stg								
    , plocBOP.cityinternal_stg AS city_stg								
    , LEFT(plocBOP.postalcodeinternaldenorm_stg,5) AS postalcode_stg								
    , ROOFYEAR_ALFA_stg								
    , hotl2.yearbuilt_stg								
    , pol.OriginalEffectiveDate_stg								
    , addrtl.name_stg AS AddressType_stg								
    , pp.id_stg AS ppID								
    , n.DESCRIPTION_stg "State_stg"								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''H''								
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''F''								
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''T''								
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''SM''								
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN (''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'') THEN ''HO''                                                              								
        WHEN hotl.TYPECODE_stg IN (''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''FAL''                                                             								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''                                                              								
        END AS TYPECODE_stg                                                             								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''                                                             								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''                                                                								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''                                                                								
        ELSE uwco.Name_stg                                                              								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''                                                             								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''                                                               								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''                                                               								
        ELSE uwco.Name_stg                                                              								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0)                                                             								
        WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL                                                             								
        THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        ELSE  0                                                             								
        END AS Amount_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line1								
    , CASE WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line2								
    , CASE WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line3								
    , CASE WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line4								
    , CASE WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0) END AS line5								
    , COALESCE(hotsc.Amount_stg,0) AS amountALL								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''                                                                								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''                                                               								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''                                                                								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''                                                              								
        ELSE jobtl.TYPECODE_stg                                                               								
        END AS JOB_stg                                                              								
    , job.jobnumber_stg                                                             								
    , hotsc.id_stg as hotscID                                                               								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.id_stg = bld.branchid_stg AND bld.expirationdate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP ON plocBOP.ID_stg = bp7loc.location_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
        AND pl.ExpirationDate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
        AND hotl2.ExpirationDate_stg IS NULL                                                                 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP.Stateinternal_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP.addresstypeinternal_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
        AND zz.ExpirationDate_stg IS NULL                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP.AccountLocation_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.building_stg = bld.id_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc2 ON exc2.BranchID_stg = pp.ID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7cost e ON e.id_stg = hotsc.bp7cost_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingcov BldCov ON BldCov.id_stg = e.BuildingCov_stg 								
        AND bldcov.building_stg=bld.fixedid_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7linecov Linecov ON Linecov.id_stg = e.LineCoverage_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov LocCov ON LocCov.id_stg =e.locationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classificationcov classcov ON classcov.id_stg =e.classificationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classification cls ON classcov.Classification_stg = cls.id_stg AND bld.fixedid_stg=cls.building_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitem schditem ON schditem.Schedule_stg = LocCov.FixedID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitemcov schd ON schd.id_stg = e.LocSchedCovItemCov_stg                                                               								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg=''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''                                                                								
								
AND NOT EXISTS(                                                             								
                                                                								
SELECT DISTINCT plocBOP2.AddressLine1Internal_stg                                                                 								
    , job2.jobnumber_stg                                                                								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON exc.building_stg = bld.id_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP2 ON plocBOP2.ID_stg = bp7loc.location_stg                                                           								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_job job2 ON job2.id_stg = pp.JobID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job2.Subtype_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
    AND pl.ExpirationDate_stg IS NULL                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
    AND hotl2.ExpirationDate_stg IS NULL                                                                 								
LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP2.Stateinternal_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP2.addresstypeinternal_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
    AND zz.ExpirationDate_stg IS NULL                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov loccov ON loccov.BranchID_stg = pp.ID_stg 								
    AND loccov.Location_stg = bp7loc.FixedID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL2 ON ETL2.PATTERNID_stg = loccov.PatternCode_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat2 ON etlpat2.ClausePatternID_stg=ETL2.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP2.AccountLocation_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
                                                                								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND exc.expirationdate_stg IS NULL                                                              								
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg = ''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''                                                                  								
AND plocBOP2.AddressLine1Internal_stg=plocBOP.AddressLine1Internal_stg                                                              								
AND job2.jobnumber_stg=job.jobnumber_stg)                                                                                                                            								
) cte                                                               								
                                                                 								
WHERE county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								

GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg,                                                               								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", /* Longitude, Latitude,                                                                								 */
"NAICCode_stg", "Company Name_stg", /*BooleanTerm1_stg,*/ jobnumber_stg/* , LocationNum_stg, LocationNum    */

                                                                								
) mwua							
where Prem_Trans_Amt <> 0.00
ORDER BY Company_Name,Policy_Suffix,NAIC_Code,Policy_Type,Policy_Number,Pimary_Insrd_Name,Address_Line,City,County,Zip_Code,Period_Start_Date,Period_End_Date,Prem_Trans_Eff_Date,Prem_Trans_Type,Prem_Trans_Amt,Job_number
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_MWUA_DETAIL.Company_Name as Company_Name,
SQ_MWUA_DETAIL.Policy_Suffix as Policy_Suffix,
SQ_MWUA_DETAIL.NAIC_Code as NAIC_Code,
SQ_MWUA_DETAIL.Policy_Type as Policy_Type,
SQ_MWUA_DETAIL.Policy_Number as Policy_Number,
SQ_MWUA_DETAIL.Pimary_Insrd_Name as Pimary_Insrd_Name,
SQ_MWUA_DETAIL.Address_Line as Address_Line,
SQ_MWUA_DETAIL.City as City,
SQ_MWUA_DETAIL.County as County,
SQ_MWUA_DETAIL.Zip_Code as Zip_Code,
substr ( to_char ( SQ_MWUA_DETAIL.Period_Start_Date ) , 0 , 10 ) as o_Period_Start_Date,
substr ( to_char ( SQ_MWUA_DETAIL.Period_End_Date ) , 0 , 10 ) as o_Period_End_Date,
substr ( to_char ( SQ_MWUA_DETAIL.Prem_Trans_Eff_Date ) , 0 , 10 ) as o_Prem_Trans_Eff_Date,
SQ_MWUA_DETAIL.Prem_Trans_Type as Prem_Trans_Type,
to_char ( IFNULL(TRY_TO_DECIMAL(SQ_MWUA_DETAIL.Prem_Trans_Amt), 0) ) as o_Prem_Trans_Amt,
SQ_MWUA_DETAIL.Job_number as Job_number,
SQ_MWUA_DETAIL.source_record_id
FROM
SQ_MWUA_DETAIL
);


-- Component FF_MWUA_DETAIL, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_MWUA_DETAIL AS
(
SELECT
EXPTRANS.Company_Name as Company_Name,
EXPTRANS.Policy_Suffix as Policy_Suffix,
EXPTRANS.NAIC_Code as NAIC_Code,
EXPTRANS.Policy_Type as Policy_Type,
EXPTRANS.Policy_Number as Policy_Number,
EXPTRANS.Pimary_Insrd_Name as Pimary_Insrd_Name,
EXPTRANS.Address_Line as Address_Line,
EXPTRANS.City as City,
EXPTRANS.County as County,
EXPTRANS.Zip_Code as Zip_Code,
EXPTRANS.o_Period_Start_Date as Period_Start_Date,
EXPTRANS.o_Period_End_Date as Period_End_Date,
EXPTRANS.o_Prem_Trans_Eff_Date as Prem_Trans_Eff_Date,
EXPTRANS.Prem_Trans_Type as Prem_Trans_Type,
EXPTRANS.o_Prem_Trans_Amt as Prem_Trans_Amt,
EXPTRANS.Job_number as Job_number
FROM
EXPTRANS
);


-- Component FF_MWUA_DETAIL, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/FF_MWUA_DETAIL_
FROM (SELECT * FROM FF_MWUA_DETAIL)
HEADER = TRUE
OVERWRITE = TRUE;


-- PIPELINE END FOR 2

-- PIPELINE START FOR 3

-- Component SQ_MWUA_TRAILER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MWUA_TRAILER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Prem_Trans_Amt,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select Sum(Prem_Trans_Amt)  from (SELECT DISTINCT "Company Name_stg" AS "Company_Name"								
    , Policy_Suffix_stg AS "Policy_Suffix"								
    , "NAICCode_stg" AS "NAIC_Code"								
    , TYPECODE_stg AS "Policy_Type"								
    , PolicyNumber_stg AS "Policy_Number"								
    , primaryinsuredname_stg AS "Pimary_Insrd_Name"								
    , addressline1_stg AS "Address_Line"								
    , city_stg AS "City"								
    , county_stg AS "County"								
    , postalcode_stg AS "Zip_Code"								
    , periodstart_stg AS "Period_Start_Date" 								
    , periodend_stg AS "Period_End_Date"								
    , "Premium Transaction Effective Date_stg" AS "Prem_Trans_Eff_Date"								
    , JOB_stg AS "Prem_Trans_Type"								
    , SUM(Amount_stg) AS "Prem_Trans_Amt"								
    , jobnumber_stg AS "Job_number"								
								
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
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''HO''								
        WHEN hotl.TYPECODE_stg in(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''SF''								
        WHEN hotl.typecode_stg in (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''MH''								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''								
        END AS TYPECODE_stg								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''								
        ELSE uwco.Name_stg								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''								
        ELSE uwco.Name_stg								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , COALESCE(hotsc.Amount_stg,0) AS Amount_stg								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''								
        WHEN jobtl.TYPECODE_stg=''Reinstatement'' THEN ''REINSTATEMENT''								
        WHEN jobtl.TYPECODE_stg=''Rewrite'' THEN ''REWRITE''								
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
       AND plocBOP.expirationdate_stg is null								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg								
       AND pl.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg								
       AND hotl2.ExpirationDate_stg IS NULL 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = ploc.Stateinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=ploc.addresstypeinternal_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg								
       AND zz.ExpirationDate_stg IS NULL								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL  ON ETL.PATTERNID_stg = zz.PatternCode_stg								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.ID_stg = bld.BranchID_stg								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON bld.ID_stg = exc.Building_stg								
WHERE pp.status_stg = 9 								
    AND (etlpat.name_stg =''Is Wind, Windstorm and Hail Excluded?'' 								
    AND (zz.BooleanTerm1_stg=0 OR zz.BooleanTerm1_stg IS NULL))								
    AND n.DESCRIPTION_stg=''Mississippi''								
    AND ((hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'',''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') OR hotl.TYPECODE_stg IS NULL) OR (bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'') OR bp7.TYPECODE_stg IS NULL))								
    AND (hotl.TYPECODE_stg IS NOT NULL OR bp7.TYPECODE_stg IS NOT NULL)								
    AND (hotl.TYPECODE_stg NOT IN(''PAF'',''CPL'') OR hotl.TYPECODE_stg IS NULL)								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''   								
								
) cte								
								
WHERE TYPECODE_stg IN (''HO'',''MH'',''SF'',''CMP'')								
    AND county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								
								
GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg, 								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", "NAICCode_stg", "Company Name_stg", BooleanTerm1_stg, jobnumber_stg, ProtClassCode								
								
								
								
UNION								
								
/* BOP/CHURCH								 */
SELECT DISTINCT "Company Name_stg" 								
    , Policy_Suffix_stg								
    , "NAICCode_stg"								
    , TYPECODE_stg AS "Policy Type_stg"								
    , PolicyNumber_stg								
    , primaryinsuredname_stg								
    , addressline1_stg								
    , city_stg								
    , county_stg								
    , postalcode_stg AS ZIP_stg								
/* , ProtClassCode								 */
    , periodstart_stg								
    , periodend_stg								
    , "Premium Transaction Effective Date_stg"								
    , JOB_stg AS "Premium Transaction_stg"								
    , SUM(Amount_stg) AS "Premium_stg"								
    , jobnumber_stg                                                    								
FROM (                                                              								
                                                                								
SELECT DISTINCT pp.PolicyNumber_stg								
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
    , CASE WHEN plocBOP.AddressLine2Internal_stg IS NULL THEN plocBOP.AddressLine1Internal_stg                                                                								
        ELSE plocBOP.AddressLine1Internal_stg || '' '' || plocBOP.AddressLine2Internal_stg                                                                								
        END AS addressline1_stg								
    , plocBOP.countyinternal_stg AS county_stg								
    , plocBOP.cityinternal_stg AS city_stg								
    , LEFT(plocBOP.postalcodeinternaldenorm_stg,5) AS postalcode_stg								
    , ROOFYEAR_ALFA_stg								
    , hotl2.yearbuilt_stg								
    , pol.OriginalEffectiveDate_stg								
    , addrtl.name_stg AS AddressType_stg								
    , pp.id_stg AS ppID								
    , n.DESCRIPTION_stg "State_stg"								
    , CASE WHEN hotl.TYPECODE_stg IN(''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'') THEN ''H''								
        WHEN hotl.TYPECODE_stg IN(''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''F''								
        WHEN hotl.typecode_stg IN (''MH3'',''MH4'',''MH7'',''MH9'') THEN ''T''								
        WHEN bp7.TYPECODE_stg IN(''BUSINESSOWNERS'',''CHURCH'') THEN ''SM''								
        END AS Policy_Suffix_stg								
    , CASE WHEN hotl.TYPECODE_stg IN (''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'') THEN ''HO''                                                              								
        WHEN hotl.TYPECODE_stg IN (''SF1'',''SF2'',''SF3'',''SF4'',''SF5'') THEN ''FAL''                                                             								
        WHEN bp7.TYPECODE_stg in(''BUSINESSOWNERS'',''CHURCH'') THEN ''CMP''                                                              								
        END AS TYPECODE_stg                                                             								
    , hotl.TYPECODE_stg AS HOTYPECODE_stg								
    , bp7.TYPECODE_stg AS BOPTYPECODE_stg								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''19135''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''19143''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''19151''                                                             								
        WHEN uwco.Name_stg=''Alfa Insurance Corporation'' THEN ''22330''                                                                								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''41661''                                                                								
        ELSE uwco.Name_stg                                                              								
        END AS "NAICCode_stg"								
    , CASE WHEN uwco.Name_stg=''Alfa Mutual Insurance Company'' THEN ''Alfa Mutual Insurance''                                                                								
        WHEN uwco.Name_stg=''Alfa Mutual DB_T_STAG_MEMBXREF_PROD.Fire Insurance Company'' THEN ''Alfa Mutual Fire''                                                             								
        WHEN uwco.Name_stg=''Alfa Mutual General Insurance Company'' THEN ''Alfa Mutual General''                                                               								
        WHEN uwco.Name_stg=''Alfa General Insurance Corporation'' THEN ''Alfa General Insurance''                                                               								
        ELSE uwco.Name_stg                                                              								
        END AS "Company Name_stg"								
    , pp.TotalPremiumRPT_stg AS "Premium_stg"								
    , job.closedate_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0)                                                             								
        WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL                                                             								
        THEN COALESCE(hotsc.Amount_stg,0)                                                              								
        ELSE  0                                                             								
        END AS Amount_stg								
    , CASE WHEN bldcov.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line1								
    , CASE WHEN cls.building_stg = bld.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line2								
    , CASE WHEN loccov.location_stg=bp7loc.fixedid_stg THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line3								
    , CASE WHEN bp7loc.location_stg=edf.PrimaryLocation_stg AND linecov.id_stg IS NOT NULL THEN COALESCE(hotsc.Amount_stg,0) ELSE 0 END AS Line4								
    , CASE WHEN schd.id_stg IS NOT NULL AND bp7loc.location_stg=edf.PrimaryLocation_stg THEN COALESCE(hotsc.Amount_stg,0) END AS line5								
    , COALESCE(hotsc.Amount_stg,0) AS amountALL								
    , pp.editeffectivedate_stg AS "Premium Transaction Effective Date_stg"								
    , CASE WHEN jobtl.TYPECODE_stg=''Cancellation'' THEN ''CANCELLATION''                                                                								
        WHEN jobtl.TYPECODE_stg=''PolicyChange'' THEN ''POLICY CHANGE''                                                               								
        WHEN jobtl.TYPECODE_stg=''Submission'' THEN ''SUBMISSION''                                                                								
        WHEN jobtl.TYPECODE_stg=''Renewal'' THEN ''RENEWAL''                                                              								
        ELSE jobtl.TYPECODE_stg                                                               								
        END AS JOB_stg                                                              								
    , job.jobnumber_stg                                                             								
    , hotsc.id_stg as hotscID                                                               								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON pp.id_stg = bld.branchid_stg AND bld.expirationdate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP ON plocBOP.ID_stg = bp7loc.location_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_job job ON job.id_stg = pp.JobID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job.Subtype_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
        AND pl.ExpirationDate_stg IS NULL                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
    LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
        AND hotl2.ExpirationDate_stg IS NULL                                                                 								
    LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
    LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP.Stateinternal_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP.addresstypeinternal_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
        AND zz.ExpirationDate_stg IS NULL                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP.AccountLocation_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.building_stg = bld.id_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc2 ON exc2.BranchID_stg = pp.ID_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7cost e ON e.id_stg = hotsc.bp7cost_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingcov BldCov ON BldCov.id_stg = e.BuildingCov_stg 								
        AND bldcov.building_stg=bld.fixedid_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7linecov Linecov ON Linecov.id_stg = e.LineCoverage_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov LocCov ON LocCov.id_stg =e.locationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classificationcov classcov ON classcov.id_stg =e.classificationcov_stg                                                             								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7classification cls ON classcov.Classification_stg = cls.id_stg AND bld.fixedid_stg=cls.building_stg                                                                								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitem schditem ON schditem.Schedule_stg = LocCov.FixedID_stg                                                              								
    LEFT JOIN DB_T_PROD_STAG.pcx_bp7locschedcovitemcov schd ON schd.id_stg = e.LocSchedCovItemCov_stg                                                               								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg=''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''                                                                								
								
AND NOT EXISTS(                                                             								
                                                                								
SELECT DISTINCT plocBOP2.AddressLine1Internal_stg                                                                 								
    , job2.jobnumber_stg                                                                								
FROM DB_T_PROD_STAG.pc_policyperiod pp                                                              								
                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_effectivedatedfields edf ON edf.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7buildingexcl exc ON exc.BranchID_stg = pp.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7building bld ON exc.building_stg = bld.id_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7location bp7loc ON bp7loc.id_stg=bld.location_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policylocation plocBOP2 ON plocBOP2.ID_stg = bp7loc.location_stg                                                           								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7transaction hotsc ON hotsc.BranchID_stg = pp.ID_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pc_job job2 ON job2.id_stg = pp.JobID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pctl_job jobtl ON jobtl.id_stg = job2.Subtype_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_policyline pl ON pl.BranchID_stg = pp.ID_stg                                                              								
    AND pl.ExpirationDate_stg IS NULL                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_hopolicytype_hoe hotl ON hotl.id_stg = pl.HOPolicyType_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 ON bp7.id_stg = pl.BP7PolicyType_alfa_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_jurisdiction left join DB_T_SHRD_PROD.state ON state.st_cd = pp.basestate_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_uwcompany uwco ON uwco.id_stg = pp.UWCompany_stg                                                               								
LEFT  JOIN DB_T_PROD_STAG.pc_policyterm pt ON pt.id_stg = pp.PolicyTermID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_Dwelling_HOE hotl2 ON hotl2.branchid_stg = pp.ID_stg                                                               								
    AND hotl2.ExpirationDate_stg IS NULL                                                                 								
LEFT JOIN DB_T_PROD_STAG.pc_policy pol ON pol.id_stg = pp.PolicyID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_policyaddress padd ON padd.BranchID_stg = pp.id_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_address left join DB_V_PROD_BASE.addr ON padd.address_stg = ADDR.ADDR_ID                                                               								
LEFT JOIN DB_T_PROD_STAG.pctl_state n ON n.id_stg = plocBOP2.Stateinternal_stg                                                                								
LEFT JOIN DB_T_PROD_STAG.pctl_addresstype addrtl ON addrtl.ID_stg=plocBOP2.addresstypeinternal_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe zz ON zz.branchid_stg=pp.id_stg                                                                								
    AND zz.ExpirationDate_stg IS NULL                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL ON ETL.PATTERNID_stg = zz.PatternCode_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat ON etlpat.ClausePatternID_stg=ETL.ID_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pcx_bp7locationcov loccov ON loccov.BranchID_stg = pp.ID_stg 								
    AND loccov.Location_stg = bp7loc.FixedID_stg                                                             								
LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern ETL2 ON ETL2.PATTERNID_stg = loccov.PatternCode_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_etlcovtermpattern etlpat2 ON etlpat2.ClausePatternID_stg=ETL2.ID_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_address addr2 ON addr2.ID_stg = plocBOP2.AccountLocation_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_policycontactrole pcr ON pcr.BranchID_stg = pp.id_stg                                                               								
LEFT JOIN DB_T_PROD_STAG.pc_contact cnt ON cnt.id_stg = pcr.ContactDenorm_stg                                                              								
LEFT JOIN DB_T_PROD_STAG.pc_address adr ON adr.id_stg = cnt.PrimaryAddressID_stg                                                               								
                                                                								
WHERE pp.status_stg = 9 /*  bound                                                                								 */
    AND exc.expirationdate_stg IS NULL                                                              								
    AND bp7.TYPECODE_stg IN (''BUSINESSOWNERS'',''CHURCH'')                                                              								
    AND n.DESCRIPTION_stg = ''Mississippi''                                                             								
    AND Case when pp.EditEffectiveDate_stg >= pp.ModelDate_stg 								
AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),CAST(''1900-01-01 00:00:00.000000''AS timestamp)) 								
    then pp.EditEffectiveDate_stg 								
 when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp), CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg 								
            THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),    CAST(''1900-01-01 00:00:00.000000'' as timestamp)) 								
 else CAST(pp.ModelDate_stg AS DATE)  end  between trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-01-''||''01''  and  trim(EXTRACT(year FROM TO_DATE(:RUN_DATE))-1)||''-12-''||''31''
AND plocBOP2.AddressLine1Internal_stg=plocBOP.AddressLine1Internal_stg                                                              								
AND job2.jobnumber_stg=job.jobnumber_stg)                                                                                                                            								
) cte                                                               								
                                                                 								
WHERE county_stg IN (''GEORGE'',''HANCOCK'',''HARRISON'',''JACKSON'',''PEARL RIVER'',''STONE'')								

GROUP BY primaryinsuredname_stg, addressline1_stg, city_stg, county_stg, postalcode_stg, PolicyNumber_stg, TYPECODE_stg, Policy_Suffix_stg, JOB_stg,                                                               								
periodstart_stg, periodend_stg, "Premium Transaction Effective Date_stg", /* Longitude, Latitude,                                                                								 */
"NAICCode_stg", "Company Name_stg", /*BooleanTerm1_stg,*/ jobnumber_stg/* , LocationNum_stg, LocationNum    */

                                                                								
) mwua							
where Prem_Trans_Amt <> 0.00
) SRC
)
);


-- Component EXPTRANS2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS2 AS
(
SELECT
SQ_MWUA_TRAILER.Prem_Trans_Amt as Prem_Trans_Amt,
''TOTAL'' as TOTAL,
Dummy,
SQ_MWUA_TRAILER.source_record_id
FROM
SQ_MWUA_TRAILER
);


-- Component FF_TRL_MWUA, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_TRL_MWUA AS
(
SELECT
EXPTRANS2.Dummy as Company_Name,
EXPTRANS2.Dummy as Policy_Suffix,
EXPTRANS2.Dummy as NAIC_Code,
EXPTRANS2.Dummy as Policy_Type,
EXPTRANS2.Dummy as Policy_Number,
EXPTRANS2.Dummy as Pimary_Insrd_Name,
EXPTRANS2.Dummy as Address_Line,
EXPTRANS2.Dummy as City,
EXPTRANS2.Dummy as County,
EXPTRANS2.Dummy as Zip_Code,
EXPTRANS2.Dummy as Period_Start_Date,
EXPTRANS2.Dummy as Period_End_Date,
EXPTRANS2.Dummy as Prem_Trans_Eff_Date,
EXPTRANS2.TOTAL as Prem_Trans_Type,
EXPTRANS2.Prem_Trans_Amt as Prem_Trans_Amt,
EXPTRANS2.Dummy as Job_number
FROM
EXPTRANS2
);


-- Component FF_TRL_MWUA, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/FF_TRL_MWUA_
FROM (SELECT * FROM FF_TRL_MWUA)
HEADER = TRUE
OVERWRITE = TRUE;


-- PIPELINE END FOR 3

END; ';