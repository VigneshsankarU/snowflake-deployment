-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_FEAT_MTRC_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
    start_dttm timestamp;
    end_dttm timestamp;
    prcs_id integer;

BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;

-- Component LKP_TERADATA_ETL_REF_INSRNC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_INSRNC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_plcy_asset_cvge_mtrc_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_plcy_asset_cvge_mtrc_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as cov_type_cd,
$2 as earnings_as_of_dttm,
$3 as amount,
$4 as Insrnc_Mtrc_Type_Cd,
$5 as Jobnumber,
$6 as Branchnumber,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

cast(pc_plcy_writtn_prem_x.cov_type_cd as varchar(100)), 

pc_plcy_writtn_prem_x.Updatetime, 

sum(pc_plcy_writtn_prem_x.amount), 

pc_plcy_writtn_prem_x.Insrnc_Mtrc_Type_Cd,

Jobnumber,

Branchnumber 

FROM

 (

 select 

 pc_policyperiod.PublicID_stg PublicID, 

/* pc_patransaction.PublicID, */
ExpandedCostTable.personalvehicleID as fixedid,

''PRTY_ASSET_SBTYPE4''  as asset_type,

''PRTY_ASSET_CLASFCN3'' as classification_code,

''GWPC'' as asset_src_cd,

ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD,

pc_policyperiod.UpdateTime_stg   as UpdateTime,

pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Insrnc_Mtrc_Type_Cd,

pc_patransaction.Amount_stg as amount,

0 as CVGE_CNT,

pctl_policyperiodstatus.TYPECODE_stg as PolicyperiodStatus,

pc_job.JobNumber_stg as JobNumber,

pc_policyperiod.branchnumber_stg as branchnumber,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,

pctl_job.TYPECODE_stg as jobtype

from DB_T_PROD_STAG.pc_patransaction 

  join

  (

  select pc_policyperiod.PolicyNumber_stg, pc_personalvehiclecov.PersonalVehicle_stg PersonalVehicleID,

  case when pc_pacost.PersonalVehicleCov_stg is not null then ''pc_personalvehicle''

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then ''pc_policyline''

   when pc_pacost.PersonalAutoCov_stg is not null then ''pc_policyline''

  end as Table_Name_For_FixedID,





  case when pc_pacost.PersonalVehicleCov_stg is not null then pc_personalvehiclecov.PatternCode_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then PACov_alfa.PatternCode_stg

  end as Coverable_or_PolicyLine_CovPattern,



  case when pc_pacost.PersonalVehicleCov_stg is not null then pc_personalvehiclecov.PatternCode_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then PACov_alfa.PatternCode_stg

  end as Coverable_or_PolicyLine_CovName,



  pc_pacost.*



from DB_T_PROD_STAG.pc_pacost 

join DB_T_PROD_STAG.pc_policyperiod on pc_pacost.BranchID_stg=pc_policyperiod.ID_stg   /*  and  pc_policyperiod.PublicID=''sitpcnew:100184'' */
  /*Add unit-level coverages for auto*/

  left join (select distinct pc_personalvehiclecov.PatternCode_stg, pc_personalvehiclecov.FixedID_stg, 

  pc_policyperiod.PolicyNumber_stg, pc_personalvehiclecov.PersonalVehicle_stg

                   from DB_T_PROD_STAG.pc_personalvehiclecov, DB_T_PROD_STAG.pc_policyperiod

                   where pc_personalvehiclecov.branchid_stg=pc_policyperiod.ID_stg

                   and pc_personalvehiclecov.ExpirationDate_stg is null) pc_personalvehiclecov 

                   on pc_pacost.PersonalVehicleCov_stg = pc_personalvehiclecov.FixedID_stg  and pc_personalvehiclecov.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg





  /*Add policy-level coverages for auto*/

  left join DB_T_PROD_STAG.pc_personalautocov PACov_alfa on pc_pacost.PersonalAutoCov_alfa_stg = PACov_alfa.FixedID_stg and PACov_alfa.BranchID_stg=pc_policyperiod.ID_stg and PACov_alfa.ExpirationDate_stg is null

   left join DB_T_PROD_STAG.pc_policyline PALine_unit_alfa on PACov_alfa.PALine_stg = PALine_unit_alfa.id_stg

  





  ) ExpandedCostTable on pc_patransaction.pacost_stg = expandedcosttable.ID_stg   







  left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_pacost on ExpandedCostTable.Subtype_stg = pctl_pacost.ID_stg

  left join DB_T_PROD_STAG.pctl_periltype_alfa AutoPerilType on ExpandedCostTable.PerilType_alfa_stg = AutoPerilType.ID_stg 



   join DB_T_PROD_STAG.pc_policyperiod on pc_patransaction.BranchID_stg = pc_policyperiod.ID_stg and ExpandedCostTable.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg 

   and pc_patransaction.BranchID_stg=pc_policyperiod.ID_stg 

   left join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.status_stg=pctl_policyperiodstatus.ID_stg

    join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg  

    left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg    

   left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg    and pc_policyline.ExpirationDate_stg is null  

    left join DB_T_PROD_STAG.pctl_papolicytype_alfa on pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.ID_stg

   left join DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg = pc_policy.id_stg

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg = pc_account.ID_stg   

where pctl_chargepattern.name_stg = ''Premium''

and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern like ''PA%''

and pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

/* and pctl_policyperiodstatus.TYPECODE_stg=''Bound'' */


UNION 



select pc_policyperiod.PublicID_stg policyperiodid, 

coalesce(ExpandedHOCostTable.DwellingID,ExpandedHOCostTable.ScheduledItemID) as Prty_asset_id,

''PRTY_ASSET_SBTYPE5''  as asset_type,

''PRTY_ASSET_CLASFCN1'' as classification_code,

''GWPC'' as asset_src_cd,

ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern as Cov_Type_CD,/* featid */
pc_policyperiod.UpdateTime_stg   as UpdateTime,

pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

SUM(pcx_hotransaction_hoe.Amount_stg) as PREMIUM_TRANS_AMT,

0 as CVGE_CNT,

pctl_policyperiodstatus.TYPECODE_stg,

pc_job.JobNumber_stg,

pc_policyperiod.branchnumber_stg ,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,

pctl_job.TYPECODE_stg as jobtype



from DB_T_PROD_STAG.pcx_hotransaction_hoe



  join

  

  (

  select pc_policyperiod.PolicyNumber_stg, pcx_dwellingcov_hoe.dwelling_stg as DwellingID, pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg ScheduledItemID,

  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then ''pcx_dwelling_hoe''

   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then ''pcx_holineschedcovitem_alfa''

   when pcx_homeownerscost_hoe.HomeownersLineCov_stg is not null then ''pc_policyline''

  end as Table_Name_For_FixedID,



  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then pcx_dwellingcov_hoe.PatternCode_stg

   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then pcx_holineschcovitemcov_alfa.PatternCode_stg  

   when pcx_homeownerscost_hoe.HomeownersLineCov_stg is not null then pcx_homeownerslinecov_hoe.PatternCode_stg

  end as Coverable_or_PolicyLine_CovPattern,



  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then pcx_dwellingcov_hoe.PatternCode_stg

   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then pcx_holineschcovitemcov_alfa.PatternCode_stg

   when pcx_homeownerscost_hoe.HomeownersLineCov_stg is not null then pcx_homeownerslinecov_hoe.PatternCode_stg

  end as Coverable_or_PolicyLine_CovName,



  pcx_homeownerscost_hoe.*



  from DB_T_PROD_STAG.pcx_homeownerscost_hoe

join DB_T_PROD_STAG.pc_policyperiod on pcx_homeownerscost_hoe.BranchID_stg=pc_policyperiod.ID_stg   /*  and  pc_policyperiod.PublicID=''sitpcnew:100184'' */
   /*Add unit-level coverages for homeowners*/

   left join ( select distinct pcx_dwellingcov_hoe.PatternCode_stg, pcx_dwellingcov_hoe.FixedID_stg, 

   pc_policyperiod.PolicyNumber_stg, pcx_dwellingcov_hoe.Dwelling_stg

                   from DB_T_PROD_STAG.pcx_dwellingcov_hoe, DB_T_PROD_STAG.pc_policyperiod

                   where pcx_dwellingcov_hoe.branchid_stg=pc_policyperiod.ID_stg 

                   and pcx_dwellingcov_hoe.ExpirationDate_stg is null )  as pcx_dwellingcov_hoe 

                    on pcx_homeownerscost_hoe.DwellingCov_stg = pcx_dwellingcov_hoe.FixedID_stg  

					and pcx_dwellingcov_hoe.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg



   left join (select distinct pcx_holineschcovitemcov_alfa.PatternCode_stg, pcx_holineschcovitemcov_alfa.FixedID_stg, pc_policyperiod.PolicyNumber_stg, pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg

                   from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa, DB_T_PROD_STAG.pc_policyperiod

                   where pcx_holineschcovitemcov_alfa.branchid_stg=pc_policyperiod.ID_stg

                   and pcx_holineschcovitemcov_alfa.ExpirationDate_stg is null) as pcx_holineschcovitemcov_alfa 

                    on pcx_homeownerscost_hoe.SchedItemCov_stg = pcx_holineschcovitemcov_alfa.FixedID_stg 

					and pcx_holineschcovitemcov_alfa.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg





   /*Add policy-level coverages for homeowners*/ 

   left join DB_T_PROD_STAG.pcx_homeownerslinecov_hoe on pcx_homeownerscost_hoe.HomeownersLineCov_stg = pcx_homeownerslinecov_hoe.ID_stg and pcx_homeownerslinecov_hoe.ExpirationDate_stg is null



  ) ExpandedHOCostTable on pcx_hotransaction_hoe.HomeownersCost_stg = ExpandedHOCostTable.ID_stg





  left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedHOCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_homeownerscost_hoe on ExpandedHOCostTable.Subtype_stg = pctl_homeownerscost_hoe.ID_stg

  left join DB_T_PROD_STAG.pctl_periltype_alfa HOPerilType on ExpandedHOCostTable.PerilType_alfa_stg = HOPerilType.ID_stg



  join DB_T_PROD_STAG.pc_policyperiod on pcx_hotransaction_hoe.BranchID_stg = pc_policyperiod.ID_stg and ExpandedHOCostTable.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg

    join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg

    left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg

    left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg  and pc_policyline.ExpirationDate_stg is null  

    left join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pc_policyline.HOPolicyType_stg = pctl_hopolicytype_hoe.ID_stg

   left join DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg = pc_policy.id_stg

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg = pc_account.ID_stg

    left join DB_T_PROD_STAG.pctl_sectiontype_alfa on ExpandedHOCostTable.SectionType_alfa_stg=pctl_sectiontype_alfa.ID_stg

    join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.Status_stg=pctl_policyperiodstatus.ID_stg

where pctl_chargepattern.name_stg = ''Premium''  

And pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)



group by pc_policyperiod.PublicID_stg , 

coalesce(ExpandedHOCostTable.DwellingID,ExpandedHOCostTable.ScheduledItemID),

ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern,pctl_policyperiodstatus.TYPECODE_stg,

pc_job.JobNumber_stg,

pc_policyperiod.branchnumber_stg,

pc_policyperiod.UpdateTime_stg,

pc_policyperiod.EditEffectiveDate_stg, pctl_job.TYPECODE_stg 





UNION





/**********************************************  POLTRM **************************************************************/





select 

''POLTRM'' as PolicyPeriodID,

ExpandedCostTable.personalvehicleID,

''PRTY_ASSET_SBTYPE4''  as asset_type,

''PRTY_ASSET_CLASFCN3'' as classification_code,

''GWPC'' as asset_src_cd,

ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD,

max(pc_patransaction.UpdateTime_stg)   as UpdateTime,

max(pc_policyperiod.EditEffectiveDate_stg) as EditEffectiveDate,

cast(''INSRNC_MTRC_TYPE5'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

Sum(pc_patransaction.Amount_stg) as PREMIUM_TRANS_AMT,

0 as CVGE_CNT,

pctl_policyperiodstatus.TYPECODE_stg,

pc_policyperiod.Policynumber_stg,

pc_policyperiod.TermNumber_stg,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,

pctl_job.TYPECODE_stg as jobtype



from DB_T_PROD_STAG.pc_patransaction 

 left join

  (

  select pc_personalvehicle.FixedID_stg PersonalVehicleID,

  case when pc_pacost.PersonalVehicleCov_stg is not null then ''pc_personalvehicle''

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then ''pc_policyline''

   when pc_pacost.PersonalAutoCov_stg is not null then ''pc_policyline''

  end as Table_Name_For_FixedID,



  case when pc_pacost.PersonalVehicleCov_stg is not null then pc_personalvehicle.FixedID_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then PALine_unit_alfa.FixedID_stg

   when pc_pacost.PersonalAutoCov_stg is not null then PALine_unit_OOTB.FixedID_stg

  end as Coverable_or_PolicyLine_FixedID,



  case when pc_pacost.PersonalVehicleCov_stg is not null then VehCovPattern.PatternID_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then AutoCovPattern_alfa.PatternID_stg

   when pc_pacost.PersonalAutoCov_stg is not null then AutoCovPattern_OOTB.PatternID_stg

  end as Coverable_or_PolicyLine_CovPattern,



  case when pc_pacost.PersonalVehicleCov_stg is not null then VehCovPattern.Name_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then AutoCovPattern_alfa.Name_stg

   when pc_pacost.PersonalAutoCov_stg is not null then AutoCovPattern_OOTB.Name_stg

  end as Coverable_or_PolicyLine_CovName,



  case when pc_pacost.PersonalVehicleCov_stg is not null then pctl_vehicletype.TYPECODE_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then ''PersonalAutoLine''

   when pc_pacost.PersonalAutoCov_stg is not null then ''PersonalAutoLine''

  end as VehTypeCode,



  case when pc_pacost.PersonalVehicleCov_stg is not null then pctl_vehicletype.NAME_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then ''PersonalAutoLine''

   when pc_pacost.PersonalAutoCov_stg is not null then ''PersonalAutoLine''

  end as VehTypeName,



  pc_pacost.*

  from DB_T_PROD_STAG.pc_pacost

  /*Add unit-level coverages for auto*/

  left join DB_T_PROD_STAG.pc_personalvehiclecov on pc_pacost.PersonalVehicleCov_stg = pc_personalvehiclecov.id_stg

   left join DB_T_PROD_STAG.pc_personalvehicle on pc_personalvehiclecov.PersonalVehicle_stg = pc_personalvehicle.id_stg

    left join DB_T_PROD_STAG.pctl_vehicletype on pc_personalvehicle.VehicleType_stg = pctl_vehicletype.ID_stg

   left join DB_T_PROD_STAG.pc_etlclausepattern VehCovPattern on pc_personalvehiclecov.PatternCode_stg = VehCovPattern.PatternID_stg



  /*Add policy-level coverages for auto*/

  left join DB_T_PROD_STAG.pc_personalautocov PACov_alfa on pc_pacost.PersonalAutoCov_alfa_stg = PACov_alfa.id_stg

   left join DB_T_PROD_STAG.pc_policyline PALine_unit_alfa on PACov_alfa.PALine_stg = PALine_unit_alfa.id_stg

   left join DB_T_PROD_STAG.pc_etlclausepattern AutoCovPattern_alfa on PACov_alfa.PatternCode_stg = AutoCovPattern_alfa.PatternID_stg



  left join DB_T_PROD_STAG.pc_personalautocov PACov_OOTB on pc_pacost.personalautocov_stg = PACov_OOTB.id_stg

   left join DB_T_PROD_STAG.pc_policyline PALine_unit_OOTB on PACov_OOTB.PALine_stg = PALine_unit_OOTB.id_stg

   left join DB_T_PROD_STAG.pc_etlclausepattern AutoCovPattern_OOTB on PACov_OOTB.PatternCode_stg = AutoCovPattern_OOTB.PatternID_stg

   

  ) ExpandedCostTable on pc_patransaction.pacost_stg = expandedcosttable.fixedid_stg





  left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedCostTable.ChargePattern_stg= pctl_chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_pacost on ExpandedCostTable.Subtype_stg = pctl_pacost.ID_stg

  left join DB_T_PROD_STAG.pctl_periltype_alfa AutoPerilType on ExpandedCostTable.PerilType_alfa_stg = AutoPerilType.ID_stg



  left join DB_T_PROD_STAG.pc_policyperiod on pc_patransaction.BranchID_stg = pc_policyperiod.ID_stg

   left join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.status_stg=pctl_policyperiodstatus.ID_stg

   left join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg

    left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg

   left join DB_T_PROD_STAG.pc_producercode on pc_policyperiod.ProducerCodeOfRecordID_stg = pc_producercode.ID_stg

   left join DB_T_PROD_STAG.pc_uwcompany on pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

   left join DB_T_PROD_STAG.pctl_jurisdiction on pc_policyperiod.BaseState_stg = pctl_jurisdiction.ID_stg

   left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg

    left join DB_T_PROD_STAG.pctl_papolicytype_alfa on pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.ID_stg

   left join DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg = pc_policy.id_stg

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg= pc_account.ID_stg

where pctl_chargepattern.name_stg = ''Premium''

and pctl_policyperiodstatus.TYPECODE_stg=''Bound''

And pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)



group by personalvehicleID,Coverable_or_PolicyLine_CovPattern,pctl_policyperiodstatus.TYPECODE_stg,policynumber_stg,TermNumber_stg, pctl_job.TYPECODE_stg 









UNION



select 

      pc_policyperiod.PublicID_stg policyperiodid,

      ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID as Prty_asset_id,

      max(case when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ''PRTY_ASSET_SBTYPE13'' else NULL end) as asset_type,

      max(case when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ExpandedCostTable.class_t else NULL end) as classification_code,

      ''GWPC'' as asset_src_cd,

      ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD,

      pc_policyperiod.UpdateTime_stg as UpdateTime,

      pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,

      cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

      SUM(tr.Amount_stg) as PREMIUM_TRANS_AMT,

      0 as CVGE_CNT,

      pctl_policyperiodstatus.TYPECODE_stg,

      pc_job.JobNumber_stg,

      pc_policyperiod.branchnumber_stg,

      (:start_dttm) as start_dttm,

      (:end_dttm) as end_dttm,

      pctl_job.TYPECODE_stg as jobtype



from DB_T_PROD_STAG.pcx_bp7transaction tr



left join (

      select

             case 

             when cost.BuildingCov_stg is not null then ''pcx_bp7building''

             when cost.LocationCov_stg is not null then ''pcx_bp7location''

             when cost.ClassificationCov_stg is not null then ''pcx_bp7classification''

             when cost.LineCoverage_stg is not null then ''pc_policyline''

             when cost.LocSchedCovItemCov_stg is not null then ''pcx_bp7locschedcovitemcov''

             when cost.BldgSchedCovItemCov_stg is not null then ''pcx_bp7bldgschedcovitemcov''

             when cost.LineSchedCovItemCov_stg is not null then ''pcx_bp7lineschedcovitemcov''

             end as Table_Name_For_FixedID,

             

             case

             when cost.BuildingCov_stg is not null then bcov.Building_stg

             when cost.LocationCov_stg is not null then lcov.Location_stg

             when cost.ClassificationCov_stg is not null then ccov.Classification_stg

             when cost.LineCoverage_stg is not null then licov.BP7Line_stg

             when cost.LocSchedCovItemCov_stg is not null then lscov.LocSchedCovItem_stg

             when cost.BldgSchedCovItemCov_stg is not null then bscov.BldgSchedCovItem_stg

             when cost.LineSchedCovItemCov_stg is not null then liscov.LineSchedCovItem_stg

             end as Coverable_or_PolicyLine_PartyAssetID,

             

             case

             when cost.BuildingCov_stg is not null then bcov.PatternCode_stg

             when cost.LocationCov_stg is not null then lcov.PatternCode_stg

             when cost.ClassificationCov_stg is not null then ccov.PatternCode_stg

             when cost.LineCoverage_stg is not null then licov.PatternCode_stg

             when cost.LocSchedCovItemCov_stg is not null then lscov.PatternCode_stg

             when cost.BldgSchedCovItemCov_stg is not null then bscov.PatternCode_stg

             when cost.LineSchedCovItemCov_stg is not null then liscov.PatternCode_stg

             end as Coverable_or_PolicyLine_CovPattern,

             

             cost.ID_stg,

             cost.ChargePattern_stg,

             cp.TYPECODE_stg as class_t

             

      from DB_T_PROD_STAG.pcx_bp7cost cost



/* Building DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7buildingcov bcov on cost.BuildingCov_stg = bcov.ID_stg

      

/* Location DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7locationcov lcov on cost.LocationCov_stg = lcov.ID_stg

      

/* Classification DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7classificationcov ccov on cost.ClassificationCov_stg = ccov.ID_stg

      left join DB_T_PROD_STAG.pcx_bp7classification c on ccov.Classification_stg = c.ID_stg

      left join DB_T_PROD_STAG.pctl_bp7classificationproperty cp on c.bp7classpropertytype_stg = cp.ID_stg

      

/* Line DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7linecov licov on cost.LineCoverage_stg = licov.ID_stg



/* Location Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7locschedcovitemcov lscov on cost.LocSchedCovItemCov_stg = lscov.ID_stg



/* Building Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bscov on cost.BldgSchedCovItemCov_stg = bscov.ID_stg



/* Line Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov liscov on cost.LineSchedCovItemCov_stg = liscov.ID_stg

) ExpandedCostTable 

on tr.BP7Cost_stg = ExpandedCostTable.ID_stg

      

left join DB_T_PROD_STAG.pctl_chargepattern 

on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg



left join DB_T_PROD_STAG.pc_policyperiod 

on tr.BranchID_stg = pc_policyperiod.ID_stg



left join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg



left join DB_T_PROD_STAG.pc_job 

on pc_policyperiod.JobID_stg = pc_job.ID_stg



left join DB_T_PROD_STAG.pctl_job 

on pc_job.Subtype_stg = pctl_job.ID_stg  



where 

      pctl_chargepattern.name_stg = ''Premium''

      and pctl_policyperiodstatus.TYPECODE_stg=''Bound''

      and pc_policyperiod.UpdateTime_stg > (:start_dttm)

      and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

      and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern is not null



group by

      pc_policyperiod.PublicID_stg,

      ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID,

      ExpandedCostTable.Coverable_or_PolicyLine_CovPattern,

      pc_policyperiod.UpdateTime_stg,

      pc_policyperiod.EditEffectiveDate_stg,

      pctl_policyperiodstatus.TYPECODE_stg,

      pc_job.JobNumber_stg,

      pc_policyperiod.branchnumber_stg,

      pctl_job.TYPECODE_stg



UNION



select 

      ''POLTRM'' policyperiodid,

      ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID as Prty_asset_id,

      max(case when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ''PRTY_ASSET_SBTYPE13'' else NULL end) as asset_type,

      max(case when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ExpandedCostTable.class_t else NULL end) as classification_code,

      ''GWPC'' as asset_src_cd,

      ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD,

      max(pc_policyperiod.UpdateTime_stg) as UpdateTime,

      max(pc_policyperiod.EditEffectiveDate_stg) as EditEffectiveDate,

      cast(''INSRNC_MTRC_TYPE5'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

      SUM(tr.Amount_stg) as PREMIUM_TRANS_AMT,

      0 as CVGE_CNT,

      pctl_policyperiodstatus.TYPECODE_stg,

      pc_job.JobNumber_stg,

      pc_policyperiod.branchnumber_stg,

      (:start_dttm) as start_dttm,

      (:end_dttm) as end_dttm,

      pctl_job.TYPECODE_stg as jobtype



from DB_T_PROD_STAG.pcx_bp7transaction tr



left join (

      select

             case 

             when cost.BuildingCov_stg is not null then ''pcx_bp7building''

             when cost.LocationCov_stg is not null then ''pcx_bp7location''

             when cost.ClassificationCov_stg is not null then ''pcx_bp7classification''

             when cost.LineCoverage_stg is not null then ''pc_policyline''

             when cost.LocSchedCovItemCov_stg is not null then ''pcx_bp7locschedcovitemcov''

             when cost.BldgSchedCovItemCov_stg is not null then ''pcx_bp7bldgschedcovitemcov''

             when cost.LineSchedCovItemCov_stg is not null then ''pcx_bp7lineschedcovitemcov''

             end as Table_Name_For_FixedID,

             

             case

             when cost.BuildingCov_stg is not null then bcov.Building_stg

             when cost.LocationCov_stg is not null then lcov.Location_stg

             when cost.ClassificationCov_stg is not null then ccov.Classification_stg

             when cost.LineCoverage_stg is not null then licov.BP7Line_stg

             when cost.LocSchedCovItemCov_stg is not null then lscov.LocSchedCovItem_stg

             when cost.BldgSchedCovItemCov_stg is not null then bscov.BldgSchedCovItem_stg

             when cost.LineSchedCovItemCov_stg is not null then liscov.LineSchedCovItem_stg

             end as Coverable_or_PolicyLine_PartyAssetID,

             

             case

             when cost.BuildingCov_stg is not null then bcov.PatternCode_stg

             when cost.LocationCov_stg is not null then lcov.PatternCode_stg

             when cost.ClassificationCov_stg is not null then ccov.PatternCode_stg

             when cost.LineCoverage_stg is not null then licov.PatternCode_stg

             when cost.LocSchedCovItemCov_stg is not null then lscov.PatternCode_stg

             when cost.BldgSchedCovItemCov_stg is not null then bscov.PatternCode_stg

             when cost.LineSchedCovItemCov_stg is not null then liscov.PatternCode_stg

             end as Coverable_or_PolicyLine_CovPattern,

             

             cost.ID_stg,

             cost.ChargePattern_stg,

             cp.TYPECODE_stg as class_t

             

      from DB_T_PROD_STAG.pcx_bp7cost cost



/* Building DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7buildingcov bcov on cost.BuildingCov_stg = bcov.ID_stg

      

/* Location DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7locationcov lcov on cost.LocationCov_stg = lcov.ID_stg

      

/* Classification DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7classificationcov ccov on cost.ClassificationCov_stg = ccov.ID_stg

      left join DB_T_PROD_STAG.pcx_bp7classification c on ccov.Classification_stg = c.ID_stg

      left join DB_T_PROD_STAG.pctl_bp7classificationproperty cp on c.bp7classpropertytype_stg = cp.ID_stg

      

/* Line DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7linecov licov on cost.LineCoverage_stg = licov.ID_stg



/* Location Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7locschedcovitemcov lscov on cost.LocSchedCovItemCov_stg = lscov.ID_stg



/* Building Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bscov on cost.BldgSchedCovItemCov_stg = bscov.ID_stg



/* Line Scheduled Item DB_T_CORE_DM_PROD.Coverage */
      left join DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov liscov on cost.LineSchedCovItemCov_stg = liscov.ID_stg

) ExpandedCostTable

on tr.BP7Cost_stg = ExpandedCostTable.ID_stg

      

left join DB_T_PROD_STAG.pctl_chargepattern 

on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg



left join DB_T_PROD_STAG.pc_policyperiod 

on tr.BranchID_stg = pc_policyperiod.ID_stg



left join DB_T_PROD_STAG.pctl_policyperiodstatus 

on pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg



left join DB_T_PROD_STAG.pc_job 

on pc_policyperiod.JobID_stg = pc_job.ID_stg



left join DB_T_PROD_STAG.pctl_job 

on pc_job.Subtype_stg = pctl_job.ID_stg



where pctl_chargepattern.name_stg = ''Premium''

      and pc_policyperiod.UpdateTime_stg > (:start_dttm)

      and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

      and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

      and pctl_job.TYPECODE_stg in (''Submission'', ''PolicyChange'', ''Renewal'')

      and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern is not null 

group by

      ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID,

      ExpandedCostTable.Coverable_or_PolicyLine_CovPattern,

      pctl_policyperiodstatus.TYPECODE_stg,

      pc_job.JobNumber_stg,

      pc_policyperiod.branchnumber_stg,

      pctl_job.TYPECODE_stg

	  

UNION

/* EIM-48975 - FARM CHANGES*/

select * from (

select distinct 

pp.publicid_stg policyperiodid,

null as Prty_asset_id,

CAST(Null AS VARCHAR(50)) as asset_type,

CAST(Null AS VARCHAR(50)) as classification_code,

''GWPC'' as asset_src_cd,

ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD,

max(tr.UpdateTime_stg) as UpdateTime, /* EIM-49941 */
max(pp.EditEffectiveDate_stg) as EditEffectiveDate,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

SUM(tr.Amount_stg) as PREMIUM_TRANS_AMT,

0 as CVGE_CNT,

pps.TYPECODE_stg as PolicyperiodStatus,

pj.JobNumber_stg,

pp.branchnumber_stg,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,

pcj.TYPECODE_stg as jobtype

from DB_T_PROD_STAG.pcx_foptransaction tr



left join (

    select

        case

        when cost.FOPDwellingCov_stg is not null then ''pcx_fopdwelling''

        when cost.FOPFarmownersLineCov_stg is not null then ''pcx_fopfarmownersline''

        when cost.FOPDwellingSchCovItemCov_stg is not null then ''pcx_fopdwellingschcovitemcov''

        when cost.FOPOutbuildingCov_stg is not null then ''pcx_fopoutbuildingcov''

        when cost.FOPLivestockCov_stg is not null then ''pcx_foplivestockcov''

		when cost.FOPMachineryCov_stg is not null then ''pcx_fopmachinerycov''

		when cost.FOPFeedAndSeedCov_stg is not null then ''pcx_fopfeedandseedcov''

		when cost.FOPFarmownersLineSchCovItemCov_stg is not null then ''pcx_fopfarmownersschcovitemcov''

		when cost.FOPBlanketCov_stg is not null then ''pcx_fopblanketcov''

		when cost.FOPLiabilityCov_stg is not null then ''pcx_fopliabilitycov''

		when cost.FOPLiabilitySchCovItemCov_stg is not null then ''pcx_fopliabilityschcovitemcov''

        end as Table_Name_For_FixedID,

        case

        when cost.FOPDwellingCov_stg is not null then bcov.PatternCode_stg

        when cost.FOPFarmownersLineCov_stg is not null then licov.PatternCode_stg

        when cost.FOPDwellingSchCovItemCov_stg is not null then dwelschcov.PatternCode_stg

        when cost.FOPOutbuildingCov_stg is not null then outcov.PatternCode_stg

        when cost.FOPLivestockCov_stg is not null then lscov.PatternCode_stg

        when cost.FOPMachineryCov_stg is not null then maccov.PatternCode_stg

        when cost.FOPFeedAndSeedCov_stg is not null then feedcov.PatternCode_stg

		when cost.FOPFarmownersLineSchCovItemCov_stg is not null then liscov.PatternCode_stg

		when cost.FOPLiabilityCov_stg is not null then liacov.PatternCode_stg

		when cost.FOPBlanketCov_stg is not null then blacov.PatternCode_stg

		when cost.FOPLiabilitySchCovItemCov_stg is not null then liaschcov.PatternCode_stg

        end as Coverable_or_PolicyLine_CovPattern,

       

        cost.ID_stg,

        cost.ChargePattern_stg

       

    from DB_T_PROD_STAG.pcx_fopcost cost

join DB_T_PROD_STAG.pc_policyperiod pp on cost.BranchID_stg=pp.ID_stg 

/* Dwelling Coverage-- */
left join ( select distinct dwell.PatternCode_stg, dwell.FixedID_stg, pp.PolicyNumber_stg,

   dwell.Dwelling_stg,dwell.branchid_stg from DB_T_PROD_STAG.pcx_fopdwellingcov dwell 

   join DB_T_PROD_STAG.pc_policyperiod pp  on dwell.branchid_stg=pp.ID_stg

 qualify row_number() over (partition by PatternCode_stg, dwell.FixedID_stg,branchid_stg order by coalesce(dwell.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,dwell.updatetime_stg desc,dwell.createtime_stg desc)=1

)  as bcov

   on cost.FOPDwellingCov_stg = bcov.FixedID_stg   and cost.branchid_stg = bcov.branchid_stg

/* Dwelling Scheduled Item Coverage-- */
left join (select distinct dwelschcov.PatternCode_stg, dwelschcov.FixedID_stg, pp.PolicyNumber_stg,

   dwelschcov.FOPDwellingScheduleCovItem_stg,dwelschcov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov dwelschcov 

	join DB_T_PROD_STAG.pc_policyperiod pp on dwelschcov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, dwelschcov.FixedID_stg,branchid_stg order by coalesce(dwelschcov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,dwelschcov.updatetime_stg desc,dwelschcov.createtime_stg desc)=1

) as dwelschcov

    on cost.FOPDwellingSchCovItemCov_stg = dwelschcov.FixedID_stg and dwelschcov.PolicyNumber_stg=pp.PolicyNumber_stg

	and cost.branchid_stg = dwelschcov.branchid_stg

/* Outbuilding Coverage-- */
left join ( select distinct outcov.PatternCode_stg, outcov.FixedID_stg, pp.PolicyNumber_stg,

   outcov.Outbuilding_stg,outcov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopoutbuildingcov outcov join DB_T_PROD_STAG.pc_policyperiod pp

    on outcov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, outcov.FixedID_stg,branchid_stg order by coalesce(outcov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,outcov.updatetime_stg desc,outcov.createtime_stg desc)=1

)  as outcov 

	on cost.FOPOutbuildingCov_stg = outcov.FixedID_stg    and cost.branchid_stg = outcov.branchid_stg

/* Line DB_T_CORE_DM_PROD.Coverage */
left join ( select distinct licov.PatternCode_stg, licov.FixedID_stg, pp.PolicyNumber_stg,

   licov.FarmownersLine_stg,licov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopfarmownerslinecov licov join DB_T_PROD_STAG.pc_policyperiod pp

    on licov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, licov.FixedID_stg,branchid_stg order by coalesce(licov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,licov.updatetime_stg desc,licov.createtime_stg desc)=1

	)  as licov

    on cost.FOPFarmownersLineCov_stg = licov.FixedID_stg   and cost.branchid_stg = licov.branchid_stg

/* Livestock Coverage-- */
left join ( select distinct lscov.PatternCode_stg, lscov.FixedID_stg, pp.PolicyNumber_stg,

   lscov.Livestock_stg,lscov.branchid_stg

    from DB_T_PROD_STAG.pcx_foplivestockcov lscov join DB_T_PROD_STAG.pc_policyperiod pp

    on lscov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, lscov.FixedID_stg,branchid_stg order by coalesce(lscov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,lscov.updatetime_stg desc,lscov.createtime_stg desc)=1

)  as lscov

    on cost.FOPLivestockCov_stg = lscov.FixedID_stg   and cost.branchid_stg = lscov.branchid_stg

/* Machinery Coverage-- */
left join ( select distinct maccov.PatternCode_stg, maccov.FixedID_stg, pp.PolicyNumber_stg,

   maccov.Machinery_stg,maccov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopmachinerycov maccov join DB_T_PROD_STAG.pc_policyperiod pp

    on maccov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, maccov.FixedID_stg,branchid_stg order by coalesce(maccov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,maccov.updatetime_stg desc,maccov.createtime_stg desc)=1

)  as maccov

    on cost.FOPMachineryCov_stg = maccov.FixedID_stg  and cost.branchid_stg = maccov.branchid_stg

/* Feed and Seed Coverage-- */
left join ( select distinct feedcov.PatternCode_stg, feedcov.FixedID_stg, pp.PolicyNumber_stg,

   feedcov.FeedAndSeed_stg,feedcov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopfeedandseedcov feedcov join DB_T_PROD_STAG.pc_policyperiod pp

    on feedcov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, feedcov.FixedID_stg,branchid_stg order by coalesce(feedcov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,feedcov.updatetime_stg desc,feedcov.createtime_stg desc)=1

)  as feedcov

    on cost.FOPFeedAndSeedCov_stg = feedcov.FixedID_stg   and cost.branchid_stg = feedcov.branchid_stg

/* Line Scheduled Item Coverage-- */
left join (select distinct liscov.PatternCode_stg,liscov.FixedID_stg, pp.PolicyNumber_stg,

	liscov.FOPFarmownersLiScheduleCovItem_stg,liscov.branchid_stg

     from  DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov liscov  join DB_T_PROD_STAG.pc_policyperiod pp

     on  liscov.branchid_stg=pp.ID_stg

     qualify row_number() over (partition by PatternCode_stg, liscov.FixedID_stg,branchid_stg order by coalesce(liscov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,liscov.updatetime_stg desc,liscov.createtime_stg desc)=1

) as liscov 	

on cost.FOPFarmownersLineSchCovItemCov_stg = liscov.Fixedid_stg and  liscov.PolicyNumber_stg=pp.PolicyNumber_stg

and cost.branchid_stg = liscov.branchid_stg

/*  Liability DB_T_CORE_DM_PROD.Coverage */
left join ( select distinct liacov.PatternCode_stg, liacov.FixedID_stg, pp.PolicyNumber_stg,

   liacov.Liability_stg,liacov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopliabilitycov liacov join DB_T_PROD_STAG.pc_policyperiod pp

    on liacov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, liacov.FixedID_stg,branchid_stg order by coalesce(liacov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,liacov.updatetime_stg desc,liacov.createtime_stg desc)=1

)  as liacov

    on cost.FOPLiabilityCov_stg = liacov.FixedID_stg   and cost.branchid_stg = liacov.branchid_stg

/* Blanket DB_T_CORE_DM_PROD.Coverage */
left join ( select distinct blacov.PatternCode_stg, blacov.FixedID_stg, pp.PolicyNumber_stg,

   blacov.Blanket_stg,blacov.branchid_stg

    from DB_T_PROD_STAG.pcx_fopblanketcov blacov join DB_T_PROD_STAG.pc_policyperiod pp

    on blacov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, blacov.FixedID_stg,branchid_stg order by coalesce(blacov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,blacov.updatetime_stg desc,blacov.createtime_stg desc)=1

)  as blacov

    on cost.FOPBlanketCov_stg = blacov.FixedID_stg   and cost.branchid_stg = blacov.branchid_stg

/* Liability Scheduled Item Coverage-- */
left join (select distinct liaschcov.PatternCode_stg, liaschcov.FixedID_stg, pp.PolicyNumber_stg,

   liaschcov.FOPLiabilityScheduleCovItem_stg,liaschcov.branchid_stg

    from  DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov liaschcov  join DB_T_PROD_STAG.pc_policyperiod pp

    on  liaschcov.branchid_stg=pp.ID_stg

    qualify row_number() over (partition by PatternCode_stg, liaschcov.FixedID_stg,branchid_stg order by coalesce(liaschcov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,liaschcov.updatetime_stg desc,liaschcov.createtime_stg desc)=1

) as  liaschcov 

     on cost.FOPLiabilitySchCovItemCov_stg = liaschcov.FixedID_stg 	 and  liaschcov.PolicyNumber_stg=pp.PolicyNumber_stg

	 and cost.branchid_stg = liaschcov.branchid_stg



) ExpandedCostTable

on tr.cost_stg = ExpandedCostTable.ID_stg

   

left join DB_T_PROD_STAG.pctl_chargepattern pcp on ExpandedCostTable.ChargePattern_stg = pcp.ID_stg

left join DB_T_PROD_STAG.pc_policyperiod pp on tr.BranchID_stg = pp.ID_stg

left join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.ID_stg = pp.Status_stg

left join DB_T_PROD_STAG.pc_job  pj on pp.JobID_stg = pj.ID_stg

left join DB_T_PROD_STAG.pctl_job  pcj on pj.Subtype_stg = pcj.ID_stg 



where pcp.name_stg = ''Premium''

    and ((pp.UpdateTime_stg > (:start_dttm) and pp.UpdateTime_stg <= (:end_dttm))

or (tr.UpdateTime_stg > (:start_dttm) and tr.UpdateTime_stg <= (:end_dttm)))/*  EIM-49941 */
      and pps.TYPECODE_stg <> ''Temporary'' and pcj.TYPECODE_stg in (''Submission'', ''PolicyChange'', ''Renewal'')

      and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern is not null

group by

    pp.PublicID_stg,

    ExpandedCostTable.Coverable_or_PolicyLine_CovPattern,

    tr.UpdateTime_stg,

    pp.EditEffectiveDate_stg,

    pps.TYPECODE_stg,

    pj.JobNumber_stg,

    pp.branchnumber_stg,

    pcj.TYPECODE_stg) as a 

	QUALIFY ROW_NUMBER() OVER(                                                              

PARTITION BY policyperiodid,Cov_Type_CD,PolicyperiodStatus,JobNumber_stg,branchnumber_stg,jobtype

ORDER BY updatetime DESC,EditEffectiveDate DESC) = 1 /* EIM-49941                   */
 )pc_plcy_writtn_prem_x

where  pc_plcy_writtn_prem_x.fixedid  is null

and PublicID <> ''POLTRM''

and Jobtype  IN (''Submission'',''PolicyChange'',''Renewal'') AND PolicyperiodStatus <> ''Temporary''

 group by cov_type_cd,Updatetime,Insrnc_Mtrc_Type_Cd,Jobnumber,Branchnumber
) SRC
)
);


-- Component exp_pass_frm_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_frm_source AS
(
SELECT
rtrim ( ltrim ( SQ_pc_plcy_asset_cvge_mtrc_x.cov_type_cd ) ) as COV_TYPE_CD_OUT,
SQ_pc_plcy_asset_cvge_mtrc_x.Insrnc_Mtrc_Type_Cd as INSRNC_MTRC_TYPE_CD,
SQ_pc_plcy_asset_cvge_mtrc_x.amount as amount,
SQ_pc_plcy_asset_cvge_mtrc_x.earnings_as_of_dttm as EARNINGS_AS_OF_DT,
SQ_pc_plcy_asset_cvge_mtrc_x.Jobnumber as Jobnumber,
SQ_pc_plcy_asset_cvge_mtrc_x.Branchnumber as Branchnumber,
SQ_pc_plcy_asset_cvge_mtrc_x.source_record_id
FROM
SQ_pc_plcy_asset_cvge_mtrc_x
);


-- Component LKP_FEAT_PLCY_ASSE_CVGE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT_PLCY_ASSE_CVGE AS
(
SELECT
LKP.FEAT_ID,
exp_pass_frm_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY LKP.FEAT_ID asc) RNK
FROM
exp_pass_frm_source
LEFT JOIN (
SELECT
FEAT_ID,
NK_SRC_KEY
FROM db_t_prod_core.FEAT
) LKP ON LKP.NK_SRC_KEY = exp_pass_frm_source.COV_TYPE_CD_OUT
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_frm_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_source.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_frm_source
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM db_t_prod_core.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_frm_source.Jobnumber AND LKP.VERS_NBR = exp_pass_frm_source.Branchnumber
QUALIFY RNK = 1
);


-- Component exp_data_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_trans AS
(
SELECT
CASE WHEN LKP_FEAT_PLCY_ASSE_CVGE.FEAT_ID IS NULL THEN 9999 ELSE LKP_FEAT_PLCY_ASSE_CVGE.FEAT_ID END as o_FEAT_ID,
exp_pass_frm_source.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
exp_pass_frm_source.amount as amount,
''UNK'' as ASSET_CNTRCT_ROLE_SBTYPE_CD,
CASE WHEN LKP_INSRNC_QUOTN.QUOTN_ID IS NULL THEN 9999 ELSE LKP_INSRNC_QUOTN.QUOTN_ID END as o_QUOTN_ID,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_INSRNC_CD */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_INSRNC_CD */ END as out_INSRNC_MTRC_TYPE_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
exp_pass_frm_source.source_record_id,
row_number() over (partition by exp_pass_frm_source.source_record_id order by exp_pass_frm_source.source_record_id) as RNK
FROM
exp_pass_frm_source
INNER JOIN LKP_FEAT_PLCY_ASSE_CVGE ON exp_pass_frm_source.source_record_id = LKP_FEAT_PLCY_ASSE_CVGE.source_record_id
INNER JOIN LKP_INSRNC_QUOTN ON LKP_FEAT_PLCY_ASSE_CVGE.source_record_id = LKP_INSRNC_QUOTN.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_INSRNC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_frm_source.INSRNC_MTRC_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_INSRNC_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_frm_source.INSRNC_MTRC_TYPE_CD
QUALIFY row_number() over (partition by exp_pass_frm_source.source_record_id order by exp_pass_frm_source.source_record_id) 
= 1
);


-- Component LKP_QUOTN_FEAT_MTRC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_FEAT_MTRC AS
(
SELECT
LKP.QUOTN_ID,
LKP.FEAT_ID,
LKP.QUOTN_FEAT_MTRC_AMT,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.FEAT_ID asc,LKP.INSRNC_MTRC_TYPE_CD asc,LKP.QUOTN_FEAT_MTRC_AMT asc) RNK
FROM
exp_data_trans
LEFT JOIN (
SELECT QUOTN_FEAT_MTRC.QUOTN_FEAT_MTRC_AMT as QUOTN_FEAT_MTRC_AMT, QUOTN_FEAT_MTRC.QUOTN_ID as QUOTN_ID, QUOTN_FEAT_MTRC.FEAT_ID as FEAT_ID, QUOTN_FEAT_MTRC.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD FROM db_t_prod_core.QUOTN_FEAT_MTRC QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  FEAT_ID,QUOTN_ID,INSRNC_MTRC_TYPE_CD 
ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.QUOTN_ID = exp_data_trans.o_QUOTN_ID AND LKP.FEAT_ID = exp_data_trans.o_FEAT_ID AND LKP.INSRNC_MTRC_TYPE_CD = exp_data_trans.out_INSRNC_MTRC_TYPE_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.FEAT_ID asc,LKP.INSRNC_MTRC_TYPE_CD asc,LKP.QUOTN_FEAT_MTRC_AMT asc) 
= 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
exp_data_trans.o_FEAT_ID as in_FEAT_ID,
exp_data_trans.o_QUOTN_ID as in_QUOTN_ID,
exp_data_trans.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
exp_data_trans.amount as amount,
exp_data_trans.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_trans.EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_trans.out_PRCS_ID as out_PRCS_ID,
LKP_QUOTN_FEAT_MTRC.FEAT_ID as lkp_FEAT_ID,
LKP_QUOTN_FEAT_MTRC.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_FEAT_MTRC.QUOTN_FEAT_MTRC_AMT as lkp_QUOTN_ASSET_CVGE_AMT1,
CASE WHEN LKP_QUOTN_FEAT_MTRC.FEAT_ID IS NULL THEN ''I'' ELSE ( CASE WHEN exp_data_trans.amount <> LKP_QUOTN_FEAT_MTRC.QUOTN_FEAT_MTRC_AMT THEN ''U'' ELSE ''R'' END ) END as Flag,
exp_data_trans.out_INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_data_trans.source_record_id
FROM
exp_data_trans
INNER JOIN LKP_QUOTN_FEAT_MTRC ON exp_data_trans.source_record_id = LKP_QUOTN_FEAT_MTRC.source_record_id
);


-- Component rtr_insupd_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insupd_Insert AS
SELECT
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_QUOTN_ID as in_QUOTN_ID,
exp_ins_upd.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
exp_ins_upd.amount as amount,
exp_ins_upd.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_ins_upd.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_trans.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.out_PRCS_ID as out_PRCS_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_ins_upd.lkp_QUOTN_ASSET_CVGE_AMT1 as lkp_PLCY_ASSET_CVGE_AMT1,
exp_ins_upd.Flag as Flag,
exp_ins_upd.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_data_trans.source_record_id
FROM
exp_data_trans
LEFT JOIN exp_ins_upd ON exp_data_trans.source_record_id = exp_ins_upd.source_record_id
WHERE exp_ins_upd.Flag = ''I'' or exp_ins_upd.Flag = ''U'';


-- Component upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insupd_Insert.in_FEAT_ID as in_FEAT_ID,
rtr_insupd_Insert.in_QUOTN_ID as in_QUOTN_ID,
rtr_insupd_Insert.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
rtr_insupd_Insert.amount as amount,
rtr_insupd_Insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insupd_Insert.in_EDW_END_DTTM as in_EDW_END_DTTM,
rtr_insupd_Insert.out_PRCS_ID as out_PRCS_ID,
rtr_insupd_Insert.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
rtr_insupd_Insert.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD1,
0 as UPDATE_STRATEGY_ACTION,
source_record_id,
FROM
rtr_insupd_Insert
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_insert.in_FEAT_ID as in_FEAT_ID,
upd_insert.in_QUOTN_ID as in_QUOTN_ID,
upd_insert.amount as amount,
upd_insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
upd_insert.in_EDW_END_DTTM as in_EDW_END_DTTM,
upd_insert.out_PRCS_ID as out_PRCS_ID,
upd_insert.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
upd_insert.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
upd_insert.INSRNC_MTRC_TYPE_CD1 as INSRNC_MTRC_TYPE_CD1,
upd_insert.source_record_id
FROM
upd_insert
);


-- Component QUOTN_FEAT_MTRC, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_FEAT_MTRC
(
QUOTN_ID,
FEAT_ID,
QUOTN_FEAT_ROLE_CD,
QUOTN_FEAT_STRT_DTTM,
INSRNC_MTRC_TYPE_CD,
QUOTN_FEAT_MTRC_STRT_DTTM,
QUOTN_FEAT_MTRC_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins.in_QUOTN_ID as QUOTN_ID,
exp_pass_to_tgt_ins.in_FEAT_ID as FEAT_ID,
exp_pass_to_tgt_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as QUOTN_FEAT_ROLE_CD,
exp_pass_to_tgt_ins.EARNINGS_AS_OF_DT as QUOTN_FEAT_STRT_DTTM,
exp_pass_to_tgt_ins.INSRNC_MTRC_TYPE_CD1 as INSRNC_MTRC_TYPE_CD,
exp_pass_to_tgt_ins.EARNINGS_AS_OF_DT as QUOTN_FEAT_MTRC_STRT_DTTM,
exp_pass_to_tgt_ins.amount as QUOTN_FEAT_MTRC_AMT,
exp_pass_to_tgt_ins.out_PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.in_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_ins.EARNINGS_AS_OF_DT as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component QUOTN_FEAT_MTRC, Type Post SQL 
UPDATE db_t_prod_core.QUOTN_FEAT_MTRC 
set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

FROM

(SELECT	distinct QUOTN_ID,FEAT_ID,INSRNC_MTRC_TYPE_CD,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,INSRNC_MTRC_TYPE_CD ORDER by EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

 as lead1,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,INSRNC_MTRC_TYPE_CD ORDER by EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM	db_t_prod_core.QUOTN_FEAT_MTRC

 ) a


where  QUOTN_FEAT_MTRC.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_FEAT_MTRC.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_FEAT_MTRC.FEAT_ID=A.FEAT_ID

AND QUOTN_FEAT_MTRC.INSRNC_MTRC_TYPE_CD=A.INSRNC_MTRC_TYPE_CD

and lead1 is not null;


END; ';