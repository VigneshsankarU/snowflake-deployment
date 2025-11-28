-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_ASSET_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1; 

-- Component LKP_PRTY_ASSET_ID, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 

FROM DB_T_PROD_CORE.PRTY_ASSET 

--QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'', ''pctl_bp7classificationproperty.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_CNTRCT_ROLE_SBTYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_quotation_asset_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_quotation_asset_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as policynumber,
$2 as asset_start_dt,
$3 as partyassetid,
$4 as partyassettype,
$5 as agreementtype,
$6 as agmt_asset_ref_num,
$7 as nk_PublicID,
$8 as SYS_SRC_CD,
$9 as BRANCHNUMBER,
$10 as updatetime,
$11 as Rank,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct  A.* ,rank() over (partition by  PartyAssetID_stg, partyassettype_stg,nk_PublicID_stg,BRANCHNUMBER_stg
, SYS_SRC_CD_stg
order by asset_start_dt_stg ) Rnk 
from(

SELECT distinct quotation_asset_x.policynumber_stg, cast (quotation_asset_x.asset_start_dt_stg as date) as asset_start_dt_stg ,/*  quotation_asset_x.asset_end_dt,  */
Cast(quotation_asset_x.partyassetid_stg AS VARCHAR(100)) as PartyAssetID_stg, quotation_asset_x.partyassettype_stg, quotation_asset_x.agreementtype_stg, 

quotation_asset_x.agmt_asset_ref_num_stg, quotation_asset_x.nk_PublicID_stg,

''SRC_SYS4'' as SYS_SRC_CD_stg,
BRANCHNUMBER_stg,quotation_asset_x.updatetime_stg



FROM

 (SELECT distinct 

	pc_policyperiod.PolicyNumber_stg, 

	pcx_Dwelling_HOE.fixedid_stg as partyassetid_stg,

	cast(''PRTY_ASSET_SBTYPE5'' AS VARCHAR(100))as partyassettype_stg,

	cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(100))as agreementtype_stg,

	case 

	when (pcx_Dwelling_HOE.EffectiveDate_stg is null) 

	then pc_policyperiod.EditEffectiveDate_stg 

	else pcx_Dwelling_HOE.EffectiveDate_stg

	end as asset_start_dt_stg, 

	cast(null as varchar(100))AGMT_ASSET_REF_NUM_stg,

	pc_job.jobnumber_stg as nk_PublicID_stg ,

	pc_policyperiod.BranchNumber_stg,
    ''SRC_SYS4'' as SYS_SRC_CD_stg,

	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod 

join DB_T_PROD_STAG.pcx_Dwelling_HOE 

on pc_PolicyPeriod.ID_stg = pcx_Dwelling_HOE.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pcx_Dwelling_HOE.fixedid_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_Dwelling_HOE.ExpirationDate_stg is null
qualify row_number() over(partition by policynumber_stg, PartyAssetID_stg, partyassettype_stg, agreementtype_stg,  nk_PublicID_Stg 
, SYS_SRC_CD_stg
,BRANCHNUMBER_stg order by asset_start_dt_stg desc) =1


UNION

/**Dwelling Personal Property and Other Structure**/

SELECT distinct 

	pc_policyperiod.PolicyNumber_stg, 

	a.id_Stg as partyassetid_stg,

	assettype_stg as partyassettype_stg,

	classification_code_stg as agreementtype_stg,

	case 

	when (pcx_holineschedcovitem_alfa.EffectiveDate_stg is null) 

	then pc_policyperiod.EditEffectiveDate_stg

	else pcx_holineschedcovitem_alfa.EffectiveDate_stg  

	end as asset_start_dt_stg,

	cast(null as varchar(100)) AGMT_ASSET_REF_NUM_stg,

	pc_job.jobnumber_stg as nk_PublicID_stg,

	pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod 

inner join (

	select 

		pcx_holineschedcovitem_alfa.FixedID_stg as id_stg, 

		pcx_holineschedcovitem_alfa.branchid_stg,

		max(

			case 

			when pcx_holineschcovitemcov_alfa.PatternCode_stg in (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

			then ''PRTY_ASSET_SBTYPE5'' 

			when pcx_holineschcovitemcov_alfa.PatternCode_stg = ''HOSI_ScheduledPropertyItem_alfa'' 

			then ''PRTY_ASSET_SBTYPE7''	

			else ''UNK'' 

			end 

		) as assettype_stg,

		max(ChoiceTerm1_stg) as classification_code_stg,

		max(LineSchCovItemCov_ClausePattern.Name_stg) as asset_name_stg,

		max(

			case pc_etlcovtermpattern.PatternID_stg 

			when ''HOSI_ScheduledPropertyItemArticleDescr_alfa'' 

			then StringTerm1_stg  

			end 

		) as prop_Description_stg

	from DB_T_PROD_STAG.pcx_holineschedcovitem_alfa

    inner join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

	on pcx_holineschedcovitem_alfa.id_stg = cast(pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg as int)

	inner join DB_T_PROD_STAG.pc_etlclausepattern LineSchCovItemCov_ClausePattern 

	on pcx_holineschcovitemcov_alfa.PatternCode_stg = LineSchCovItemCov_ClausePattern.PatternID_stg

	inner join DB_T_PROD_STAG.pc_etlcovtermpattern 

	on LineSchCovItemCov_ClausePattern.id_stg = pc_etlcovtermpattern.ClausePatternID_stg

where 

pcx_holineschcovitemcov_alfa.PatternCode_stg in (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'')

group by pcx_holineschedcovitem_alfa.FixedID_stg, pcx_holineschedcovitem_alfa.branchid_stg

) a 

on pc_PolicyPeriod.ID_stg = a.BranchID_stg

join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa 

on pc_PolicyPeriod.ID_stg = pcx_holineschedcovitem_alfa.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pcx_holineschedcovitem_alfa.fixedid_stg is not null

and a.classification_code_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_holineschedcovitem_alfa.ExpirationDate_stg is null



UNION

/***Agreement and its related Asset Information for Vehicle***/

SELECT distinct 

	pc_policyperiod.PolicyNumber_stg, 

	pc_personalvehicle.fixedid_stg as partyassetid_stg,

	''PRTY_ASSET_SBTYPE4''  as partyassettype_stg,

	''PRTY_ASSET_CLASFCN3''  as agreementtype_stg,

	case 

	when (pc_personalvehicle.EffectiveDate_stg is null) 

	then pc_policyperiod.EditEffectiveDate_stg

	else pc_personalvehicle.EffectiveDate_Stg 

	end as asset_start_dt_stg,

	cast(pc_personalvehicle.VehicleNumber_stg as varchar(100))as AGMT_ASSET_REF_NUM_stg,

	pc_job.jobnumber_stg as nk_PublicID_stg,

	pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pc_personalvehicle  

on pc_PolicyPeriod.ID_stg = pc_personalvehicle.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pc_personalvehicle.fixedid_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pc_personalvehicle.ExpirationDate_stg is null



UNION

/***Agreement and its related Asset Information for DB_T_CORE_DM_PROD.Coverage for watercraft motor***/

SELECT distinct 

	pc_policyperiod.PolicyNumber_stg, 

	pcx_pawatercraftmotor_alfa.fixedid_stg as partyassetid_stg,

	''PRTY_ASSET_SBTYPE4'' as partyassettype_stg,

	''PRTY_ASSET_CLASFCN4'' as agreementtype_stg,

	case 

	when (pcx_pawatercraftmotor_alfa.EffectiveDate_stg is null) 

	then pc_policyperiod.EditEffectiveDate_stg 

	else pcx_pawatercraftmotor_alfa.EffectiveDate_stg 

	end as asset_start_dt_stg,

	cast(null as varchar(100))AGMT_ASSET_REF_NUM_stg,

	pc_job.jobnumber_stg as nk_PublicID_stg,

	pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod 

join DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa  

on pc_PolicyPeriod.ID_stg = pcx_pawatercraftmotor_alfa.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_Stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_pawatercraftmotor_alfa.ExpirationDate_stg is null



UNION

/***Agreement and its related Asset Information for DB_T_CORE_DM_PROD.Coverage for watercraft trailer***/

SELECT distinct 

	pc_policyperiod.PolicyNumber_stg, 

	pcx_pawatercrafttrailer_alfa.fixedid_stg as partyassetid_stg,

	''PRTY_ASSET_SBTYPE4'' as partyassettype_stg,

	''PRTY_ASSET_CLASFCN5'' as agreementtype_stg,

	case 

	when (pcx_pawatercrafttrailer_alfa.EffectiveDate_stg is null) 

	then pc_policyperiod.EditEffectiveDate_stg 

	else pcx_pawatercrafttrailer_alfa.EffectiveDate_stg  

	end as asset_start_dt_stg,

	cast(null as varchar(100))AGMT_ASSET_REF_NUM_stg,

	pc_job.jobnumber_stg as nk_PublicID_stg,

	pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod 

join DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa  

on pc_PolicyPeriod.ID_stg = pcx_pawatercrafttrailer_alfa.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pcx_pawatercrafttrailer_alfa.fixedid_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_pawatercrafttrailer_alfa.ExpirationDate_stg is null



UNION

select

    pc_policyperiod.PolicyNumber_stg, 

    pcx_bp7classification.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE13'' as partyassettype_stg,

    pctl_bp7classificationproperty.TYPECODE_stg as agreementtype_stg,

    case 

    when (pcx_bp7classification.EffectiveDate_stg is null) 

    then pc_policyperiod.EditEffectiveDate_stg

    else pcx_bp7classification.EffectiveDate_stg

    end as asset_start_dt_stg, 

    cast(null as varchar(100))AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(current_timestamp as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_bp7classification

on pc_PolicyPeriod.ID_stg = pcx_bp7classification.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_PolicyPeriod.JobID_stg = pc_job.id_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_bp7classificationproperty 

on pcx_bp7classification.bp7classpropertytype_stg = pctl_bp7classificationproperty.ID_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_bp7classification.FixedID_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_bp7classification.ExpirationDate_stg is null 

/*EIM-36975*/

UNION

select distinct

    pc_policyperiod.PolicyNumber_stg, 

    pcx_bp7building.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE32'' as partyassettype_stg,

    ''PRTY_ASSET_CLASFCN10'' as agreementtype_stg,

    pc_policyperiod.EditEffectiveDate_stg asset_start_dt_stg, 

    cast(null as varchar(100))AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
	cast(pc_PolicyPeriod.UpdateTime_stg as TIMESTAMP(6))updatetime_stg

from DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_bp7building

on pc_PolicyPeriod.ID_stg = pcx_bp7building.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_PolicyPeriod.JobID_stg = pc_job.id_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg



where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_bp7building.FixedID_stg is not null

and pc_policyperiod.updatetime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

and pcx_bp7building.ExpirationDate_stg is null

/*EIM-36975*/



union



/* FOPDwelling */
SELECT distinct 

    pc_policyperiod.PolicyNumber_stg, 

    pcx_fopdwelling.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE37''  as partyassettype_stg,

    ''PRTY_ASSET_CLASFCN15''  as agreementtype_stg,

    case 

    when (pcx_fopdwelling.EffectiveDate_stg is null) 

    then pc_policyperiod.EditEffectiveDate_stg

    else pcx_fopdwelling.EffectiveDate_stg 

    end as asset_start_dt_stg,

    cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
        cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopdwelling.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopdwelling.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopdwelling  

on pc_PolicyPeriod.ID_stg = pcx_fopdwelling.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pcx_fopdwelling.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopdwelling.updatetime_stg > (:start_dttm)

    and pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopdwelling.ExpirationDate_stg is null or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopdwelling.createtime_stg desc)=1

union

/* FOPOutbuilding */
SELECT distinct 

    pc_policyperiod.PolicyNumber_stg, 

    pcx_fopoutbuilding.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE36''  as partyassettype_stg,

    ''PRTY_ASSET_CLASFCN13''  as agreementtype_stg,

    case 

    when (pcx_fopoutbuilding.EffectiveDate_stg is null) 

    then pc_policyperiod.EditEffectiveDate_stg

    else pcx_fopoutbuilding.EffectiveDate_stg

    end as asset_start_dt_stg,

    cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
    cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopoutbuilding.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopoutbuilding.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopoutbuilding 

on pc_PolicyPeriod.ID_stg = pcx_fopoutbuilding.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

and pcx_fopoutbuilding.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopoutbuilding.updatetime_stg > (:start_dttm)

    and pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopoutbuilding.ExpirationDate_stg is null or pcx_fopoutbuilding.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopoutbuilding.createtime_stg desc)=1

union

/* FOPFeedAndSeed */
SELECT distinct

pc_policyperiod.PolicyNumber_stg,

pcx_fopfeedandseed.fixedid_stg as partyassetid_stg,

''PRTY_ASSET_SBTYPE33''  as partyassettype_stg,

''PRTY_ASSET_CLASFCN11''  as agreementtype_stg,

case

when (pcx_fopfeedandseed.EffectiveDate_stg is null)

then pc_policyperiod.EditEffectiveDate_stg

else pcx_fopfeedandseed.EffectiveDate_stg

end as asset_start_dt_stg,

cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

pc_job.jobnumber_stg as nk_PublicID_stg,

pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopfeedandseed.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopfeedandseed.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopfeedandseed on pc_PolicyPeriod.ID_stg = pcx_fopfeedandseed.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_fopfeedandseed.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopfeedandseed.updatetime_stg > (:start_dttm)

    and pcx_fopfeedandseed.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopfeedandseed.ExpirationDate_stg is null or pcx_fopfeedandseed.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopfeedandseed.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopfeedandseed.createtime_stg desc)=1

union

/* FOPMachinery */
SELECT distinct

pc_policyperiod.PolicyNumber_stg,

pcx_fopmachinery.fixedid_stg as partyassetid_stg,

''PRTY_ASSET_SBTYPE34''  as partyassettype_stg,

''PRTY_ASSET_CLASFCN12''  as agreementtype_stg,

case

when (pcx_fopmachinery.EffectiveDate_stg is null)

then pc_policyperiod.EditEffectiveDate_stg

else pcx_fopmachinery.EffectiveDate_stg

end as asset_start_dt_stg,

cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

pc_job.jobnumber_stg as nk_PublicID_stg,

pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopmachinery.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopmachinery.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopmachinery on pc_PolicyPeriod.ID_stg = pcx_fopmachinery.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_fopmachinery.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopmachinery.updatetime_stg > (:start_dttm)

    and pcx_fopmachinery.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopmachinery.ExpirationDate_stg is null or pcx_fopmachinery.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopmachinery.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopmachinery.createtime_stg desc)=1

union

/* FOPLivestock */
SELECT distinct

pc_policyperiod.PolicyNumber_stg,

pcx_foplivestock.fixedid_stg as partyassetid_stg,

''PRTY_ASSET_SBTYPE35''  as partyassettype_stg,

''PRTY_ASSET_CLASFCN14''  as agreementtype_stg,

case

when (pcx_foplivestock.EffectiveDate_stg is null)

then pc_policyperiod.EditEffectiveDate_stg

else pcx_foplivestock.EffectiveDate_stg

end as asset_start_dt_stg,

cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

pc_job.jobnumber_stg as nk_PublicID_stg,

pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
cast(case

        when pc_policyperiod.updatetime_stg > pcx_foplivestock.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_foplivestock.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_foplivestock on pc_PolicyPeriod.ID_stg = pcx_foplivestock.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_foplivestock.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_foplivestock.updatetime_stg > (:start_dttm)

    and pcx_foplivestock.UpdateTime_stg <= (:end_dttm)))

and (pcx_foplivestock.ExpirationDate_stg is null or pcx_foplivestock.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_foplivestock.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_foplivestock.createtime_stg desc)=1

union

/* FOPDwellingScheduleCovItem */
SELECT distinct

pc_policyperiod.PolicyNumber_stg,

pcx_fopdwellingschdcovitem.fixedid_stg as partyassetid_stg,

''PRTY_ASSET_SBTYPE38''  as partyassettype_stg,

''PRTY_ASSET_CLASFCN16''  as agreementtype_stg,

case

when (pcx_fopdwellingschdcovitem.EffectiveDate_stg is null)

then pc_policyperiod.EditEffectiveDate_stg

else pcx_fopdwellingschdcovitem.EffectiveDate_stg

end as asset_start_dt_stg,

cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

pc_job.jobnumber_stg as nk_PublicID_stg,

pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopdwellingschdcovitem.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopdwellingschdcovitem.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopdwellingschdcovitem on pc_PolicyPeriod.ID_stg = pcx_fopdwellingschdcovitem.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_fopdwellingschdcovitem.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopdwellingschdcovitem.updatetime_stg > (:start_dttm)

    and pcx_fopdwellingschdcovitem.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopdwellingschdcovitem.ExpirationDate_stg is null or pcx_fopdwellingschdcovitem.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopdwellingschdcovitem.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopdwellingschdcovitem.createtime_stg desc)=1

union

/* DB_T_PROD_STAG.pcx_fopdwellingschdexclitem */
SELECT distinct

pc_policyperiod.PolicyNumber_stg,

pcx_fopdwellingschdexclitem.fixedid_stg as partyassetid_stg,

''PRTY_ASSET_SBTYPE40''  as partyassettype_stg,

''PRTY_ASSET_CLASFCN18''  as agreementtype_stg,

case

when (pcx_fopdwellingschdexclitem.EffectiveDate_stg is null)

then pc_policyperiod.EditEffectiveDate_stg

else pcx_fopdwellingschdexclitem.EffectiveDate_stg

end as asset_start_dt_stg,

cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

pc_job.jobnumber_stg as nk_PublicID_stg,

pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
cast(case

        when pc_policyperiod.updatetime_stg > pcx_fopdwellingschdexclitem.updatetime_stg

        then pc_policyperiod.updatetime_stg

        else pcx_fopdwellingschdexclitem.updatetime_stg

        end 

        as TIMESTAMP(6))updatetime

from DB_T_PROD_STAG.pc_PolicyPeriod

join DB_T_PROD_STAG.pcx_fopdwellingschdexclitem on pc_PolicyPeriod.ID_stg = pcx_fopdwellingschdexclitem.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pcx_fopdwellingschdexclitem.fixedid_stg is not null

and ((pc_policyperiod.updatetime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopdwellingschdexclitem.updatetime_stg > (:start_dttm)

    and pcx_fopdwellingschdexclitem.UpdateTime_stg <= (:end_dttm)))

and (pcx_fopdwellingschdexclitem.ExpirationDate_stg is null or pcx_fopdwellingschdexclitem.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopdwellingschdexclitem.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopdwellingschdexclitem.createtime_stg desc)=1

union

/* DB_T_PROD_STAG.pcx_fopfarmownerslischdcovitem */
    SELECT distinct

    pc_policyperiod.PolicyNumber_stg,

    pcx_fopfarmownerslischdcovitem.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE41''  as partyassettype_stg,

    ''PRTY_ASSET_CLASFCN19''  as agreementtype_stg,

    case

    when (pcx_fopfarmownerslischdcovitem.EffectiveDate_stg is null)

    then pc_policyperiod.EditEffectiveDate_stg

    else pcx_fopfarmownerslischdcovitem.EffectiveDate_stg

    end as asset_start_dt_stg,

    cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
    cast(case

            when pc_policyperiod.updatetime_stg > pcx_fopfarmownerslischdcovitem.updatetime_stg

            then pc_policyperiod.updatetime_stg

            else pcx_fopfarmownerslischdcovitem.updatetime_stg

            end 

            as TIMESTAMP(6))updatetime

    from DB_T_PROD_STAG.pc_PolicyPeriod

    join DB_T_PROD_STAG.pcx_fopfarmownerslischdcovitem on pc_PolicyPeriod.ID_stg = pcx_fopfarmownerslischdcovitem.BranchID_stg

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

    where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

    and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

    and pcx_fopfarmownerslischdcovitem.fixedid_stg is not null

    and ((pc_policyperiod.updatetime_stg > (:start_dttm)

        and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopfarmownerslischdcovitem.updatetime_stg > (:start_dttm)

        and pcx_fopfarmownerslischdcovitem.UpdateTime_stg <= (:end_dttm)))

    and (pcx_fopfarmownerslischdcovitem.ExpirationDate_stg is null or pcx_fopfarmownerslischdcovitem.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

	qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopfarmownerslischdcovitem.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopfarmownerslischdcovitem.createtime_stg desc)=1

    union

/* DB_T_PROD_STAG.pcx_fopliabilityschdcovitem */
    SELECT distinct

    pc_policyperiod.PolicyNumber_stg,

    pcx_fopliabilityschdcovitem.fixedid_stg as partyassetid_stg,

    ''PRTY_ASSET_SBTYPE42''  as partyassettype_stg,

    ''PRTY_ASSET_CLASFCN20''  as agreementtype_stg,

    case

    when (pcx_fopliabilityschdcovitem.EffectiveDate_stg is null)

    then pc_policyperiod.EditEffectiveDate_stg

    else pcx_fopliabilityschdcovitem.EffectiveDate_stg

    end as asset_start_dt_stg,

    cast(null as varchar(100))as AGMT_ASSET_REF_NUM_stg,

    pc_job.jobnumber_stg as nk_PublicID_stg,

    pc_policyperiod.BranchNumber_stg,
''SRC_SYS4'' as SYS_SRC_CD_stg,
    cast(case

            when pc_policyperiod.updatetime_stg > pcx_fopliabilityschdcovitem.updatetime_stg

            then pc_policyperiod.updatetime_stg

            else pcx_fopliabilityschdcovitem.updatetime_stg

            end 

            as TIMESTAMP(6))updatetime

    from DB_T_PROD_STAG.pc_PolicyPeriod

    join DB_T_PROD_STAG.pcx_fopliabilityschdcovitem on pc_PolicyPeriod.ID_stg = pcx_fopliabilityschdcovitem.BranchID_stg

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg

    where pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

    and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

    and pcx_fopliabilityschdcovitem.fixedid_stg is not null

    and ((pc_policyperiod.updatetime_stg > (:start_dttm)

        and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) or (pcx_fopliabilityschdcovitem.updatetime_stg > (:start_dttm)

        and pcx_fopliabilityschdcovitem.UpdateTime_stg <= (:end_dttm)))

    and (pcx_fopliabilityschdcovitem.ExpirationDate_stg is null or pcx_fopliabilityschdcovitem.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)

	qualify Row_Number() over(partition by PolicyNumber_stg,partyassetid_stg,nk_PublicID_stg,BranchNumber_stg order by Coalesce(pcx_fopliabilityschdcovitem.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, updatetime desc, pcx_fopliabilityschdcovitem.createtime_stg desc)=1

)

quotation_asset_x where partyassetid_stg is not null ) as A
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
SQ_quotation_asset_x.Rank as Rank,
SQ_quotation_asset_x.policynumber as policynumber,
SQ_quotation_asset_x.asset_start_dt as asset_start_dt,
NULL as asset_end_dt,
SQ_quotation_asset_x.partyassetid as partyassetid,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_asset_subtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_clasfn_cd,
NULL as Class_cd,
SQ_quotation_asset_x.agmt_asset_ref_num as VehicleNumber,
SQ_quotation_asset_x.nk_PublicID as nk_PublicID,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as OUT_SRC_CD,
SQ_quotation_asset_x.BRANCHNUMBER as BRANCHNUMBER,
SQ_quotation_asset_x.updatetime as updatetime,
SQ_quotation_asset_x.source_record_id,
row_number() over (partition by SQ_quotation_asset_x.source_record_id order by SQ_quotation_asset_x.source_record_id) as RNK
FROM
SQ_quotation_asset_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_quotation_asset_x.partyassettype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_quotation_asset_x.partyassettype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_quotation_asset_x.agreementtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_quotation_asset_x.agreementtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_quotation_asset_x.SYS_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_from_source.nk_PublicID as nk_PublicID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.NK_JOB_NBR asc,LKP.VERS_NBR asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR, INSRNC_QUOTN.SRC_SYS_CD AS SRC_SYS_CD FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_from_source.nk_PublicID AND LKP.VERS_NBR = exp_pass_from_source.BRANCHNUMBER AND LKP.SRC_SYS_CD = exp_pass_from_source.OUT_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID1 AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM DB_T_PROD_CORE.PRTY_ASSET 
/* QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1 */
WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_pass_from_source.partyassetid AND LKP.PRTY_ASSET_SBTYPE_CD = exp_pass_from_source.out_asset_subtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_pass_from_source.out_clasfn_cd
QUALIFY RNK = 1
);


-- Component exp_data_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_trans AS
(
SELECT
exp_pass_from_source.Rank as Rank,
exp_pass_from_source.policynumber as policynumber,
exp_pass_from_source.partyassetid as v_partyassetid,
exp_pass_from_source.asset_start_dt as asset_start_dt,
exp_pass_from_source.asset_end_dt as asset_end_dt,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as v_prty_sbtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as v_prty_clsfctn_cd,
exp_pass_from_source.Class_cd as Class_cd,
exp_pass_from_source.OUT_SRC_CD as OUT_SRC_CD,
LKP_5.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ as v_prty_asset_id,
LKP_INSRNC_QUOTN.QUOTN_ID as v_quotn_id,
v_quotn_id as out_quotn_id,
CASE WHEN exp_pass_from_source.asset_start_dt IS NULL THEN CURRENT_TIMESTAMP ELSE exp_pass_from_source.asset_start_dt END as out_start_dt,
exp_pass_from_source.VehicleNumber as VehicleNumber,
to_char ( exp_pass_from_source.VehicleNumber ) as VehicleNumber1,
exp_pass_from_source.nk_PublicID as nk_PublicID,
LKP_PRTY_ASSET_ID1.PRTY_ASSET_ID as PRTY_ASSET_ID,
CASE WHEN exp_pass_from_source.updatetime IS NULL THEN TO_DATE ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE exp_pass_from_source.updatetime END as out_TRANS_STRT_DTTM,
to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM1,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE */ as in_ASSET_CNTRCT_ROLE_SUBTYPE_CD,
:PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK2
FROM
exp_pass_from_source
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_from_source.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID1 ON LKP_INSRNC_QUOTN.source_record_id = LKP_PRTY_ASSET_ID1.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.out_asset_subtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source.out_asset_subtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_pass_from_source.out_clasfn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = exp_pass_from_source.out_clasfn_cd
LEFT JOIN LKP_PRTY_ASSET_ID LKP_5 ON LKP_5.ASSET_HOST_ID_VAL = v_partyassetid AND LKP_5.PRTY_ASSET_SBTYPE_CD = v_prty_sbtype_cd AND LKP_5.PRTY_ASSET_CLASFCN_CD = v_prty_clsfctn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
QUALIFY RNK2 = 1
);


-- Component LKP_QUOTN_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_ASSET AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.QUOTN_ID,
LKP.QUOTN_ASSET_STRT_DTTM,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.QUOTN_ID asc,LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD asc,LKP.QUOTN_ASSET_STRT_DTTM asc) RNK
FROM
exp_data_trans
LEFT JOIN (
SELECT QUOTN_ASSET.QUOTN_ASSET_STRT_DTTM as QUOTN_ASSET_STRT_DTTM,  QUOTN_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, QUOTN_ASSET.QUOTN_ID as QUOTN_ID, QUOTN_ASSET.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD 
FROM DB_T_PROD_CORE.QUOTN_ASSET 
/* QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID, QUOTN_ID, ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY EDW_END_DTTM desc) = 1 */
WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''
) LKP ON LKP.PRTY_ASSET_ID = exp_data_trans.PRTY_ASSET_ID AND LKP.QUOTN_ID = exp_data_trans.out_quotn_id AND LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_data_trans.in_ASSET_CNTRCT_ROLE_SUBTYPE_CD
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
exp_data_trans.Rank as Rank,
exp_data_trans.in_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_data_trans.in_EDW_END_DTTM as out_EDW_END_DTTM,
exp_data_trans.out_quotn_id as in_QUOTN_ID,
exp_data_trans.PRTY_ASSET_ID as in_PARTY_ASSET_ID,
exp_data_trans.out_start_dt as asset_start_dt,
exp_data_trans.asset_end_dt as asset_end_dt,
exp_data_trans.out_TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
exp_data_trans.VehicleNumber as VehicleNumber,
MD5 ( TO_CHAR ( LKP_QUOTN_ASSET.QUOTN_ASSET_STRT_DTTM ) ) as var_orig_chksm,
MD5 ( to_char ( exp_data_trans.out_start_dt ) ) as var_calc_chksm,
exp_data_trans.TRANS_END_DTTM1 as TRANS_END_DTTM1,
exp_data_trans.in_ASSET_CNTRCT_ROLE_SUBTYPE_CD as ASSET_CNTRCT_ROLE_SUBTYPE_CD,
exp_data_trans.in_PRCS_ID as PRCS_ID,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_data_trans.source_record_id
FROM
exp_data_trans
INNER JOIN LKP_QUOTN_ASSET ON exp_data_trans.source_record_id = LKP_QUOTN_ASSET.source_record_id
);


-- Component router_ins_out_agmt_asset_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE router_ins_out_agmt_asset_INSERT AS
(SELECT
exp_ins_upd.Rank as Rank,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.in_QUOTN_ID as in_QUOTN_ID,
exp_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
exp_ins_upd.asset_start_dt as asset_start_dt,
exp_ins_upd.asset_end_dt as asset_end_dt,
exp_ins_upd.VehicleNumber as VehicleNumber,
exp_ins_upd.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM1 as TRANS_END_DTTM1,
exp_ins_upd.ASSET_CNTRCT_ROLE_SUBTYPE_CD as in_ASSET_CNTRCT_ROLE_SUBTYPE_CD,
exp_ins_upd.PRCS_ID as in_PRCS_ID,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE ( exp_ins_upd.out_ins_upd = ''I'' or exp_ins_upd.out_ins_upd = ''U'' ) and exp_ins_upd.in_PARTY_ASSET_ID > 0 and exp_ins_upd.in_QUOTN_ID > 0 
-- in_out_router_flag = ''INS'' and in_AGMT_ID IS NOT NULL and exp_ins_upd.in_PARTY_ASSET_ID IS NOT NULL
);


-- Component EXP_ins_AGMT_ASSET, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_ins_AGMT_ASSET AS
(
SELECT
router_ins_out_agmt_asset_INSERT.in_ASSET_CNTRCT_ROLE_SUBTYPE_CD as ASSET_CONTRACT_SBTYPE,
router_ins_out_agmt_asset_INSERT.in_QUOTN_ID as in_AGMT_ID,
router_ins_out_agmt_asset_INSERT.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
router_ins_out_agmt_asset_INSERT.asset_start_dt as asset_start_dt,
router_ins_out_agmt_asset_INSERT.asset_end_dt as asset_end_dt,
router_ins_out_agmt_asset_INSERT.in_PRCS_ID as PROCESS_ID,
router_ins_out_agmt_asset_INSERT.VehicleNumber as VehicleNumber,
router_ins_out_agmt_asset_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
router_ins_out_agmt_asset_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
router_ins_out_agmt_asset_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
router_ins_out_agmt_asset_INSERT.TRANS_END_DTTM1 as TRANS_END_DTTM11,
DATEADD (
  SECOND,
  (2 * (router_ins_out_agmt_asset_INSERT.Rank - 1)),
  CURRENT_TIMESTAMP()
) as OUT_EDW_START_DT,
router_ins_out_agmt_asset_INSERT.source_record_id
FROM
router_ins_out_agmt_asset_INSERT
);


-- Component upd_agmt_asset_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
EXP_ins_AGMT_ASSET.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
EXP_ins_AGMT_ASSET.in_AGMT_ID as in_QUOTN_ID,
EXP_ins_AGMT_ASSET.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
EXP_ins_AGMT_ASSET.asset_start_dt as asset_start_dt,
EXP_ins_AGMT_ASSET.asset_end_dt as asset_end_dt,
EXP_ins_AGMT_ASSET.PROCESS_ID as PROCESS_ID,
NULL as agreementtype,
EXP_ins_AGMT_ASSET.VehicleNumber as VehicleNumber,
EXP_ins_AGMT_ASSET.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM1,
EXP_ins_AGMT_ASSET.out_EDW_END_DTTM1 as out_EDW_END_DTTM1,
EXP_ins_AGMT_ASSET.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
EXP_ins_AGMT_ASSET.TRANS_END_DTTM11 as TRANS_END_DTTM11,
EXP_ins_AGMT_ASSET.OUT_EDW_START_DT as OUT_EDW_START_DT,
0 as UPDATE_STRATEGY_ACTION,EXP_ins_AGMT_ASSET.source_record_id
FROM
EXP_ins_AGMT_ASSET
);


-- Component QUOTN_ASSET_new_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET
(
PRTY_ASSET_ID,
QUOTN_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
QUOTN_ASSET_REF_NUM,
QUOTN_ASSET_STRT_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
upd_agmt_asset_ins.in_PARTY_ASSET_ID as PRTY_ASSET_ID,
upd_agmt_asset_ins.in_QUOTN_ID as QUOTN_ID,
upd_agmt_asset_ins.ASSET_CONTRACT_SBTYPE as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_agmt_asset_ins.VehicleNumber as QUOTN_ASSET_REF_NUM,
upd_agmt_asset_ins.asset_start_dt as QUOTN_ASSET_STRT_DTTM,
upd_agmt_asset_ins.PROCESS_ID as PRCS_ID,
upd_agmt_asset_ins.OUT_EDW_START_DT as EDW_STRT_DTTM,
upd_agmt_asset_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
upd_agmt_asset_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
upd_agmt_asset_ins.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
upd_agmt_asset_ins;


-- Component QUOTN_ASSET_new_ins, Type Post SQL 
UPDATE DB_T_PROD_CORE.QUOTN_ASSET FROM  

(

SELECT	distinct PRTY_ASSET_ID,EDW_STRT_DTTM, ASSET_CNTRCT_ROLE_SBTYPE_CD, QUOTN_ID, 

max(EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID,QUOTN_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID,QUOTN_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM DB_T_PROD_CORE.QUOTN_ASSET 

)  A

set TRANS_END_DTTM=  A.lead, 

EDW_END_DTTM=A.lead1

where  QUOTN_ASSET.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_ASSET.PRTY_ASSET_ID=A.PRTY_ASSET_ID

AND QUOTN_ASSET.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD

AND QUOTN_ASSET.QUOTN_ID=A.QUOTN_ID

and QUOTN_ASSET.TRANS_STRT_DTTM <>QUOTN_ASSET.TRANS_END_DTTM

and lead is not null;



/*

UPDATE  DB_T_PROD_CORE.QUOTN_ASSET  FROM  

(

SELECT	distinct PRTY_ASSET_ID,QUOTN_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD,EDW_STRT_DTTM, TRANS_STRT_DTTM, QUOTN_ASSET_STRT_DTTM

FROM	DB_T_PROD_CORE.QUOTN_ASSET 

WHERE EDW_END_DTTM=TO_TIMESTAMP(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID,QUOTN_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD  ORDER BY QUOTN_ASSET_STRT_DTTM DESC) >1

)  A

SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1 SECOND'', 

TRANS_END_DTTM=A.TRANS_STRT_DTTM+ INTERVAL ''1 SECOND'' 

WHERE  QUOTN_ASSET.PRTY_ASSET_ID=A.PRTY_ASSET_ID

AND  QUOTN_ASSET.EDW_STRT_DTTM=A.EDW_STRT_DTTM

AND QUOTN_ASSET.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD

AND QUOTN_ASSET.QUOTN_ID=A.QUOTN_ID

AND QUOTN_ASSET.QUOTN_ASSET_STRT_DTTM=A.QUOTN_ASSET_STRT_DTTM

*/


END; ';