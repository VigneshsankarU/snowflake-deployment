-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_ASSET_FEAT_MTRC_INSUPD("RUN_ID" VARCHAR)
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


-- Component SQ_pc_plcy_asset_cvge_mtrc_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_plcy_asset_cvge_mtrc_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FEAT_ID,
$2 as QUOTN_ID,
$3 as PRTY_ASSET_ID,
$4 as INSRNC_MTRC_TYPE_CD,
$5 as UpdateTime,
$6 as busn_dt,
$7 as Amount,
$8 as ASSET_CNTRCT_ROLE_SBTYPE_CD,
$9 as TGT_PRTY_ASSET_ID,
$10 as TGT_QUOTN_ID,
$11 as TGT_FEAT_ID,
$12 as TGT_INSRNC_MTRC_TYPE_CD,
$13 as TGT_QAF_MTRC_AMT,
$14 as TGT_EDW_STRT_DTTM,
$15 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

SQ1.FEAT_ID,

SQ1.QUOTN_ID,

SQ1.PRTY_ASSET_ID,

SQ1.INSRNC_MTRC_TYPE_CD,

SQ1.UpdateTime,

SQ1.BUSN_DT,

SQ1.Amount,

SQ1.ASSET_CNTRCT_ROLE_SBTYPE_CD,

SQ1.TGT_PRTY_ASSET_ID,

SQ1.TGT_QUOTN_ID,

SQ1.TGT_FEAT_ID,

SQ1.TGT_INSRNC_MTRC_TYPE_CD,

SQ1.TGT_QAF_MTRC_AMT,/* EIM-49963 */
SQ1.TGT_EDW_STRT_DTTM/* EIM-49963 */
from (

select 

fixed_id,

asset_type_stg,

classification_code_stg,

COV_TYPE_CD,

UpdateTime_stg as UpdateTime,

busn_dt,

PREMIUM_TRANS_AMT_stg as Amount,

Inscrn_Mtrc_Type_CD_stg,

Jobnumber_stg,

Branchnumber_stg,



/*lkp_teradata_etl_xref_asset_clasfcn*/

CASE WHEN XLAT_ASSET_CLASSIFICATION.TGT_IDNTFTN_VAL is null then ''UNK'' 

ELSE  XLAT_ASSET_CLASSIFICATION.TGT_IDNTFTN_VAL END  as ASSET_CLASFCN,



/*lkp_insurance_cd*/

CASE WHEN XLAT_INSURANCE_CD.TGT_IDNTFTN_VAL is null then ''UNK'' 

ELSE  XLAT_INSURANCE_CD.TGT_IDNTFTN_VAL END  as INSRNC_MTRC_TYPE_CD,





/*lkp_teradata_etl_xref_asset_sbtype*/

CASE WHEN XLAT_ASSET_SBTYPE.TGT_IDNTFTN_VAL is null then ''UNK'' 

ELSE  XLAT_ASSET_SBTYPE.TGT_IDNTFTN_VAL END  as ASSET_SBTYPE,



/*lkp_feat*/

FEAT.FEAT_ID as FEAT_ID,

/*lkp_INSRNC_QUOTN*/

INSRNC_QUOTN.QUOTN_ID as QUOTN_ID,

/*lkp_prty_asset*/

PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID,

/*LKP_TGT_QUOTN_ASSET*/

TGT_QUOTN_ASSET.PRTY_ASSET_ID1 as TGT_PRTY_ASSET_ID,

TGT_QUOTN_ASSET.QUOTN_ID1 as TGT_QUOTN_ID,

TGT_QUOTN_ASSET.FEAT_ID1 as TGT_FEAT_ID,

TGT_QUOTN_ASSET.INSRNC_MTRC_TYPE_CD1 as TGT_INSRNC_MTRC_TYPE_CD,

TGT_QUOTN_ASSET.QAF_MTRC_AMT1 as TGT_QAF_MTRC_AMT,/* EIM-49963 */
TGT_QUOTN_ASSET.EDW_STRT_DTTM1 as TGT_EDW_STRT_DTTM,/* EIM-49963 */
''UNK'' as ASSET_CNTRCT_ROLE_SBTYPE_CD,



/*flag*/

CASE WHEN TGT_FEAT_ID is null then ''|'' ELSE ''U'' END as flag



from (

/*source_query*/



SELECT TO_CHAR(CAST(pc_plcy_writtn_prem_x.Prty_asset_id_stg as NUMBER))as fixed_id  , 

pc_plcy_writtn_prem_x.asset_type_stg as asset_type_stg, 

pc_plcy_writtn_prem_x.classification_code_stg as classification_code_stg, 

TRIM(pc_plcy_writtn_prem_x.COV_TYPE_CD_stg) as COV_TYPE_CD, 

pc_plcy_writtn_prem_x.UpdateTime_stg as UpdateTime_stg, 

pc_plcy_writtn_prem_x.busn_dt as busn_dt,

sum(pc_plcy_writtn_prem_x.PREMIUM_TRANS_AMT_stg) as PREMIUM_TRANS_AMT_stg, 

pc_plcy_writtn_prem_x.Inscrn_Mtrc_Type_CD_stg as Inscrn_Mtrc_Type_CD_stg,

Jobnumber_stg,

Branchnumber_stg

FROM

(

select cast( ExpandedCostTable.personalvehicleID_stg as varchar(100)) as Prty_asset_id_stg,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as asset_type_stg,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code_stg,

cast(ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg as varchar(100))  as COV_TYPE_CD_stg,

pc_policyperiod.UpdateTime_stg   as UpdateTime_stg,

pc_policyperiod.UpdateTime_stg  as busn_dt,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

pc_patransaction.Amount_stg as PREMIUM_TRANS_AMT_stg,

pc_job.JobNumber_stg,

cast(pc_policyperiod.branchnumber_stg as varchar(100)) as branchnumber_stg

from DB_T_PROD_STAG.pc_patransaction 

  join

  (

  select   pc_pcv.PersonalVehicle_stg PersonalVehicleID_stg,

  case when pc_pacost.PersonalVehicleCov_stg is not null then pc_pcv.PatternCode_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then PACov_alfa.PatternCode_stg

  end as Coverable_or_PolicyLine_CovPattern_stg, pc_policyperiod.PolicyNumber_stg,

  pc_pacost.id_stg,pc_pacost.ChargePattern_stg,pc_pacost.Subtype_stg,pc_pacost.PerilType_alfa_stg

from DB_T_PROD_STAG.pc_pacost 

join DB_T_PROD_STAG.pc_policyperiod on pc_pacost.BranchID_stg=pc_policyperiod.ID_stg  /*  and  pc_policyperiod.PublicID=''sitpcnew:100184'' */
  /*Add unit-level coverages for auto*/

  left join ( select distinct pc_personalvehiclecov.PatternCode_stg, pc_personalvehiclecov.FixedID_stg, pc_policyperiod.PolicyNumber_stg, pc_personalvehiclecov.PersonalVehicle_stg

            from DB_T_PROD_STAG.pc_personalvehiclecov inner join DB_T_PROD_STAG.pc_policyperiod

            on pc_personalvehiclecov.branchid_stg=pc_policyperiod.ID_stg

            and pc_personalvehiclecov.ExpirationDate_stg is null) as pc_pcv 

            on pc_pacost.PersonalVehicleCov_stg = pc_pcv.FixedID_stg  and pc_pcv.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg

         /*Add policy-level coverages for auto*/

  left join DB_T_PROD_STAG.pc_personalautocov PACov_alfa on pc_pacost.PersonalAutoCov_alfa_stg = PACov_alfa.FixedID_stg and PACov_alfa.BranchID_stg=pc_policyperiod.ID_stg and PACov_alfa.ExpirationDate_stg is null

   left join DB_T_PROD_STAG.pc_policyline PALine_unit_alfa on PACov_alfa.PALine_stg = PALine_unit_alfa.id_stg

  ) ExpandedCostTable on pc_patransaction.pacost_stg = expandedcosttable.ID_stg  

 left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_pacost on ExpandedCostTable.Subtype_stg = pctl_pacost.ID_stg

  left join DB_T_PROD_STAG.pctl_periltype_alfa AutoPerilType on ExpandedCostTable.PerilType_alfa_stg = AutoPerilType.ID_stg 

   join DB_T_PROD_STAG.pc_policyperiod on pc_patransaction.BranchID_stg = pc_policyperiod.ID_stg and ExpandedCostTable.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg and pc_patransaction.BranchID_stg=pc_policyperiod.ID_stg 

   left join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.status_stg=pctl_policyperiodstatus.ID_stg

    join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg  

    left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg    

   left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg    and pc_policyline.ExpirationDate_stg is null  

    left join DB_T_PROD_STAG.pctl_papolicytype_alfa on pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.ID_stg

   left join DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg = pc_policy.id_stg

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg = pc_account.ID_stg   

where pctl_chargepattern.name_stg = ''Premium''

and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg like ''PA%''

and pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

union



select   

 cast(coalesce(ExpandedHOCostTable.DwellingID_stg,ExpandedHOCostTable.ScheduledItemID_stg) as varchar(100)) as Prty_asset_id_stg,

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) as asset_type_stg,

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100)) as classification_code_stg,

cast(ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern_stg as varchar(100)) as Cov_Type_CD_stg,/* featid */
pc_policyperiod.UpdateTime_stg   as UpdateTime_stg,

pc_policyperiod.UpdateTime_stg  as busn_dt,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

SUM(pcx_hotransaction_hoe.Amount_stg) as PREMIUM_TRANS_AMT_stg,

pc_job.JobNumber_stg,

cast(pc_policyperiod.branchnumber_stg as  varchar(100)) as branchnumber_stg

from DB_T_PROD_STAG.pcx_hotransaction_hoe

  join

  (

  select pc_policyperiod.PolicyNumber_stg, pcx_dwellingcov_hoe.dwelling_stg as DwellingID_stg, pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg ScheduledItemID_stg,

    case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then pcx_dwellingcov_hoe.PatternCode_stg

   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then pcx_holineschcovitemcov_alfa.PatternCode_stg 

   when pcx_homeownerscost_hoe.HomeownersLineCov_stg is not null then pcx_homeownerslinecov_hoe.PatternCode_stg

  end as Coverable_or_PolicyLine_CovPattern_stg,

pcx_homeownerscost_hoe.ID_stg,pcx_homeownerscost_hoe.ChargePattern_stg,pcx_homeownerscost_hoe.Subtype_stg,pcx_homeownerscost_hoe.PerilType_alfa_stg ,pcx_homeownerscost_hoe.SectionType_alfa_stg

  from DB_T_PROD_STAG.pcx_homeownerscost_hoe

join DB_T_PROD_STAG.pc_policyperiod on pcx_homeownerscost_hoe.BranchID_stg=pc_policyperiod.ID_stg   /*  and  pc_policyperiod.PublicID=''sitpcnew:100184'' */
   /*Add unit-level coverages for homeowners*/

   left join ( select distinct pcx_dwellingcov_hoe.PatternCode_stg, pcx_dwellingcov_hoe.FixedID_stg, pc_policyperiod.PolicyNumber_stg, pcx_dwellingcov_hoe.Dwelling_stg

            from DB_T_PROD_STAG.pcx_dwellingcov_hoe, DB_T_PROD_STAG.pc_policyperiod

            where pcx_dwellingcov_hoe.branchid_stg=pc_policyperiod.ID_stg 

            and pcx_dwellingcov_hoe.ExpirationDate_stg is null )  as pcx_dwellingcov_hoe 

             on pcx_homeownerscost_hoe.DwellingCov_stg = pcx_dwellingcov_hoe.FixedID_stg  and pcx_dwellingcov_hoe.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg

    left join (select distinct pcx_holineschcovitemcov_alfa.PatternCode_stg, pcx_holineschcovitemcov_alfa.FixedID_stg, pc_policyperiod.PolicyNumber_stg, pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg

            from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa, DB_T_PROD_STAG.pc_policyperiod

            where pcx_holineschcovitemcov_alfa.branchid_stg=pc_policyperiod.ID_stg 

            and pcx_holineschcovitemcov_alfa.ExpirationDate_stg is null) as pcx_holineschcovitemcov_alfa 

             on pcx_homeownerscost_hoe.SchedItemCov_stg = pcx_holineschcovitemcov_alfa.FixedID_stg and pcx_holineschcovitemcov_alfa.PolicyNumber_stg=pc_policyperiod.PolicyNumber_stg

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



group by  

coalesce(ExpandedHOCostTable.DwellingID_stg,ExpandedHOCostTable.ScheduledItemID_stg),

ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern_stg,

pc_job.JobNumber_stg,

pc_policyperiod.branchnumber_stg,

pc_policyperiod.UpdateTime_stg, busn_dt

,pctl_policyperiodstatus.TYPECODE_stg,

pctl_job.TYPECODE_stg







union



/**********************************************  POLTRM **************************************************************/

select 

  cast(ExpandedCostTable.personalvehicleID_stg as  varchar(100))  AS Prty_asset_id_stg ,

cast(''PRTY_ASSET_SBTYPE4'' as  varchar(100)) as asset_type_stg,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code_stg,

cast( ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg as  varchar(100))  as COV_TYPE_CD_stg,

max(pc_patransaction.UpdateTime_stg)   as UpdateTime_stg,

pc_policyperiod.UpdateTime_stg  as busn_dt,

cast(''INSRNC_MTRC_TYPE5'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

Sum(pc_patransaction.Amount_stg) as PREMIUM_TRANS_AMT_stg,

pc_policyperiod.Policynumber_stg as jobnumber_stg,

cast(pc_policyperiod.TermNumber_stg as  varchar(100)) as branchnumber_stg 

 

from DB_T_PROD_STAG.pc_patransaction 

 left join

  (

  select pc_personalvehicle.FixedID_stg PersonalVehicleID_stg,

   case when pc_pacost.PersonalVehicleCov_stg is not null then VehCovPattern.PatternID_stg

   when pc_pacost.PersonalAutoCov_alfa_stg is not null then AutoCovPattern_alfa.PatternID_stg

   when pc_pacost.PersonalAutoCov_stg is not null then AutoCovPattern_OOTB.PatternID_stg

  end as Coverable_or_PolicyLine_CovPattern_stg

  , pc_pacost.*  

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

   WHERE  PersonalVehicleID_stg IS NOT NULL

  ) ExpandedCostTable on pc_patransaction.pacost_stg = expandedcosttable.fixedid_stg





  left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

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

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg = pc_account.ID_stg

where pctl_chargepattern.name_stg = ''Premium''

and pctl_policyperiodstatus.TYPECODE_stg=''Bound''

And pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)



group by personalvehicleID_stg,Coverable_or_PolicyLine_CovPattern_stg, policynumber_stg,TermNumber_stg,pctl_policyperiodstatus.TYPECODE_stg,pctl_job.TYPECODE_stg,busn_dt 



/* order by pc_policyperiod.id , pc_job.ID */


union



select 

 cast(coalesce(ExpandedHOCostTable.DwellingID_stg,ExpandedHOCostTable.ScheduledItemID_stg) as  varchar(100)) AS  Prty_asset_id_stg ,

cast(''PRTY_ASSET_SBTYPE5'' as  varchar(100)) as asset_type_stg,

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code_stg,

cast(ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern_stg as  varchar(100))as Cov_Type_CD_stg,/* featid */
max(pcx_hotransaction_hoe.UpdateTime_stg)   as UpdateTime_stg,

pc_policyperiod.UpdateTime_stg  as busn_dt,

cast(''INSRNC_MTRC_TYPE5'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

SUM(pcx_hotransaction_hoe.Amount_stg) as PREMIUM_TRANS_AMT_stg,

pc_policyperiod.Policynumber_stg as JobNumber_stg ,

cast(pc_policyperiod.TermNumber_stg as  varchar(100)) as branchnumber_stg



from DB_T_PROD_STAG.pcx_hotransaction_hoe

  left join

  

  (

  select 

  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then DwellingCovPattern.PatternID_stg

   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then SchedItemCovPattern.PatternID_stg  

   when pcx_homeownerscost_hoe.HomeownersLineCov_stg is not null then HOLineCovPattern.PatternID_stg 

  end as Coverable_or_PolicyLine_CovPattern_stg,

  pcx_homeownerscost_hoe.ID_stg, 

  pcx_homeownerscost_hoe.ChargePattern_stg,pcx_homeownerscost_hoe.Subtype_stg,pcx_homeownerscost_hoe.PerilType_alfa_stg,pcx_homeownerscost_hoe.SectionType_alfa_stg,

  pcx_dwelling_hoe.ID_stg DwellingID_stg,

pcx_holineschedcovitem_alfa.ID_stg ScheduledItemID_stg/* ,pcx_dwelling_hoe.FixedID Prty_asset_id */




  from DB_T_PROD_STAG.pcx_homeownerscost_hoe

   /*Add unit-level coverages for homeowners*/

   left join DB_T_PROD_STAG.pcx_dwellingcov_hoe on pcx_homeownerscost_hoe.DwellingCov_stg = pcx_dwellingcov_hoe.id_stg

    left join DB_T_PROD_STAG.pcx_dwelling_hoe on pcx_dwellingcov_hoe.Dwelling_stg = pcx_dwelling_hoe.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern DwellingCovPattern on pcx_dwellingcov_hoe.PatternCode_stg = DwellingCovPattern.PatternID_stg



   left join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa on pcx_homeownerscost_hoe.SchedItemCov_stg = pcx_holineschcovitemcov_alfa.ID_stg

    left join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa on pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg = pcx_holineschedcovitem_alfa.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern SchedItemCovPattern on pcx_holineschcovitemcov_alfa.PatternCode_stg = SchedItemCovPattern.PatternID_stg



   /*Add policy-level coverages for homeowners*/ 

   left join DB_T_PROD_STAG.pcx_homeownerslinecov_hoe on pcx_homeownerscost_hoe.HomeownersLineCov_stg = pcx_homeownerslinecov_hoe.ID_stg

    left join DB_T_PROD_STAG.pc_policyline HOLine_Unit on pcx_homeownerslinecov_hoe.HOLine_stg = HOLine_Unit.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern HOLineCovPattern on pcx_homeownerslinecov_hoe.PatternCode_stg = HOLineCovPattern.PatternID_stg

  ) ExpandedHOCostTable on pcx_hotransaction_hoe.HomeownersCost_stg  = ExpandedHOCostTable.ID_stg



  left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedHOCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_pacost on ExpandedHOCostTable.Subtype_stg = pctl_pacost.ID_stg

  left join DB_T_PROD_STAG.pctl_periltype_alfa HOPerilType on ExpandedHOCostTable.PerilType_alfa_stg = HOPerilType.ID_stg



  left join DB_T_PROD_STAG.pc_policyperiod on pcx_hotransaction_hoe.BranchID_stg = pc_policyperiod.ID_stg

   left join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg

    left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg

   left join DB_T_PROD_STAG.pc_producercode on pc_policyperiod.ProducerCodeOfRecordID_stg = pc_producercode.ID_stg

   left join DB_T_PROD_STAG.pc_uwcompany on pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

   left join DB_T_PROD_STAG.pctl_jurisdiction on pc_policyperiod.BaseState_stg= pctl_jurisdiction.ID_stg

   left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg

    left join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pc_policyline.HOPolicyType_stg = pctl_hopolicytype_hoe.ID_stg

   left join DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg = pc_policy.id_stg

    left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID_stg = pc_account.ID_stg

    left join DB_T_PROD_STAG.pctl_sectiontype_alfa on ExpandedHOCostTable.SectionType_alfa_stg=pctl_sectiontype_alfa.ID_stg

    join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.Status_stg=pctl_policyperiodstatus.ID_stg

where pctl_chargepattern.name_stg = ''Premium'' 

and pctl_policyperiodstatus.TYPECODE_stg not in(''Temporary'')

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') 

And pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

group by 

coalesce(ExpandedHOCostTable.DwellingID_stg,ExpandedHOCostTable.ScheduledItemID_stg),

ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern_stg,

pc_policyperiod.Policynumber_stg,

pc_policyperiod.TermNumber_stg,busn_dt

, pctl_job.TYPECODE_stg,pctl_policyperiodstatus.TYPECODE_stg 

UNION

select  cast( ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID_stg as  varchar(100))   as Prty_asset_id_stg,

    cast(max(case when ExpandedCostTable.Table_Name_For_FixedID_stg = ''pcx_bp7classification'' then ''PRTY_ASSET_SBTYPE13'' else NULL end)as varchar(100)) as asset_type_stg,

    cast(max(case when ExpandedCostTable.Table_Name_For_FixedID_stg = ''pcx_bp7classification'' then ExpandedCostTable.class_stg else NULL end) as  varchar(100)) as classification_code_stg,

     cast(ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg as varchar(100)) as COV_TYPE_CD_stg,

    pc_policyperiod.UpdateTime_stg as UpdateTime_stg,

	pc_policyperiod.UpdateTime_stg  as busn_dt,

    cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

    SUM(tr.Amount_stg) as PREMIUM_TRANS_AMT_stg,

    pc_job.JobNumber_stg,

    cast( pc_policyperiod.branchnumber_stg as  varchar(100)) as branchnumber_stg



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

        end as Table_Name_For_FixedID_stg,

        

        case

        when cost.BuildingCov_stg is not null then bcov.Building_stg

        when cost.LocationCov_stg is not null then lcov.Location_stg

        when cost.ClassificationCov_stg is not null then ccov.Classification_stg

        when cost.LineCoverage_stg is not null then licov.BP7Line_stg

        when cost.LocSchedCovItemCov_stg is not null then lscov.LocSchedCovItem_stg

        when cost.BldgSchedCovItemCov_stg is not null then bscov.BldgSchedCovItem_stg

        when cost.LineSchedCovItemCov_stg is not null then liscov.LineSchedCovItem_stg

        end as Coverable_or_PolicyLine_PartyAssetID_stg,

        

        case

        when cost.BuildingCov_stg is not null then bcov.PatternCode_stg

        when cost.LocationCov_stg is not null then lcov.PatternCode_stg

        when cost.ClassificationCov_stg is not null then ccov.PatternCode_stg

        when cost.LineCoverage_stg is not null then licov.PatternCode_stg

        when cost.LocSchedCovItemCov_stg is not null then lscov.PatternCode_stg

        when cost.BldgSchedCovItemCov_stg is not null then bscov.PatternCode_stg

        when cost.LineSchedCovItemCov_stg is not null then liscov.PatternCode_stg

        end as Coverable_or_PolicyLine_CovPattern_stg,

        

        cost.ID_stg,

        cost.ChargePattern_stg,

        cp.TYPECODE_stg as class_stg

        

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

left join DB_T_PROD_STAG.pctl_chargepattern on ExpandedCostTable.ChargePattern_stg = pctl_chargepattern.ID_stg

left join DB_T_PROD_STAG.pc_policyperiod on tr.BranchID_stg = pc_policyperiod.ID_stg

left join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

left join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg

left join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg  



where 

    pctl_chargepattern.name_stg = ''Premium''

    and pctl_policyperiodstatus.TYPECODE_stg=''Bound''

    and pc_policyperiod.UpdateTime_stg > (:start_dttm)

    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

    and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg is not null



group by

    ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID_stg,

    ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg,

    pc_policyperiod.UpdateTime_stg,busn_dt,

    pc_job.JobNumber_stg,

    pc_policyperiod.branchnumber_stg, 

    pctl_job.TYPECODE_stg,

    pctl_policyperiodstatus.TYPECODE_stg

    

    

    union

    select

     cast(ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID_stg as  varchar(100))   as Prty_asset_id_stg,

    cast(max(case when ExpandedCostTable.Table_Name_For_FixedID_stg = ''pcx_bp7classification'' then ''PRTY_ASSET_SBTYPE13'' else NULL end) as varchar(100))as asset_type_stg,

    cast(max(case when ExpandedCostTable.Table_Name_For_FixedID_stg = ''pcx_bp7classification'' then ExpandedCostTable.class_stg else NULL end) as  varchar(100)) as classification_code_stg,

  cast(ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg as varchar(100)) as COV_TYPE_CD_stg,

    max(pc_policyperiod.UpdateTime_stg) as UpdateTime_stg,

	pc_policyperiod.UpdateTime_stg  as busn_dt,

    cast(''INSRNC_MTRC_TYPE5'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,

    SUM(tr.Amount_stg) as PREMIUM_TRANS_AMT_stg,

    pc_job.JobNumber_stg,

    cast(pc_policyperiod.branchnumber_stg   as  varchar(100)) as branchnumber_stg

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

        end as Table_Name_For_FixedID_stg,

        

        case

        when cost.BuildingCov_stg is not null then bcov.Building_stg

        when cost.LocationCov_stg is not null then lcov.Location_stg

        when cost.ClassificationCov_stg is not null then ccov.Classification_stg

        when cost.LineCoverage_stg is not null then licov.BP7Line_stg

        when cost.LocSchedCovItemCov_stg is not null then lscov.LocSchedCovItem_stg

        when cost.BldgSchedCovItemCov_stg is not null then bscov.BldgSchedCovItem_stg

        when cost.LineSchedCovItemCov_stg is not null then liscov.LineSchedCovItem_stg

        end as Coverable_or_PolicyLine_PartyAssetID_stg,

        

        case

        when cost.BuildingCov_stg is not null then bcov.PatternCode_stg

        when cost.LocationCov_stg is not null then lcov.PatternCode_stg

        when cost.ClassificationCov_stg is not null then ccov.PatternCode_stg 

        when cost.LineCoverage_stg is not null then licov.PatternCode_stg

        when cost.LocSchedCovItemCov_stg is not null then lscov.PatternCode_stg

        when cost.BldgSchedCovItemCov_stg is not null then bscov.PatternCode_stg

        when cost.LineSchedCovItemCov_stg is not null then liscov.PatternCode_stg

        end as Coverable_or_PolicyLine_CovPattern_stg,

        

        cost.ID_stg,

        cost.ChargePattern_stg,

        cp.TYPECODE_stg as class_stg

        

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

    and pctl_policyperiodstatus.TYPECODE_stg not in(''Temporary'')

    and pctl_job.TYPECODE_stg in (''Submission'', ''PolicyChange'', ''Renewal'')

    and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg is not null 

group by

    ExpandedCostTable.Coverable_or_PolicyLine_PartyAssetID_stg,

    ExpandedCostTable.Coverable_or_PolicyLine_CovPattern_stg,

    pc_job.JobNumber_stg,busn_dt,

    pc_policyperiod.branchnumber_stg ,  pctl_job.TYPECODE_stg,pctl_policyperiodstatus.TYPECODE_stg



union



/***EIM-48972 Farm changes**/

SELECT Prty_asset_id_stg,asset_type_stg,classification_code_stg,COV_TYPE_CD_stg,UpdateTime_stg,busn_dt,Inscrn_Mtrc_Type_CD_stg,PREMIUM_TRANS_AMT_stg,Jobnumber_stg,branchnumber_stg 

FROM(

SELECT cast(COALESCE(ExpandedFarmCostTable.DwellingID,ExpandedFarmCostTable.OutbuildingID,ExpandedFarmCostTable.LivestockID,ExpandedFarmCostTable.MachineryID,ExpandedFarmCostTable.FeedAndSeedID,

ExpandedFarmCostTable.DwellScheduledItemID,ExpandedFarmCostTable.FarmScheduledItemID,ExpandedFarmCostTable.LiabScheduledItemID

) as varchar(100)) as Prty_asset_id_stg,  

cast(max(case when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopdwellingcov'' then ''PRTY_ASSET_SBTYPE37''

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopdwellingschcovitemcov'' then ''PRTY_ASSET_SBTYPE38''

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopoutbuildingcov'' then ''PRTY_ASSET_SBTYPE36'' 

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_foplivestockcov'' then ''PRTY_ASSET_SBTYPE35'' 

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopmachinerycov'' then ''PRTY_ASSET_SBTYPE34'' 

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopfeedandseedcov'' then ''PRTY_ASSET_SBTYPE33'' 

     when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopfarmownersschcovitemcov'' then ''PRTY_ASSET_SBTYPE41''

	 when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopliabilityschcovitemcov'' then ''PRTY_ASSET_SBTYPE42''

     else NULL end) as varchar(100)) as asset_type_stg,                                          

cast(max(case when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopdwellingcov'' then ''PRTY_ASSET_CLASFCN15'' 

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopdwellingschcovitemcov'' then ''PRTY_ASSET_CLASFCN16''

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopoutbuildingcov'' then ''PRTY_ASSET_CLASFCN13''

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_foplivestockcov'' then ''PRTY_ASSET_CLASFCN14''

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopmachinerycov'' then ''PRTY_ASSET_CLASFCN12''

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopfeedandseedcov'' then ''PRTY_ASSET_CLASFCN11''

    when ExpandedFarmCostTable.Table_Name_For_FixedID  =''pcx_fopfarmownersschcovitemcov'' then ''PRTY_ASSET_CLASFCN19'' 

	when ExpandedFarmCostTable.Table_Name_For_FixedID = ''pcx_fopliabilityschcovitemcov'' then ''PRTY_ASSET_CLASFCN20''

    else NULL end) as varchar(100)) as classification_code_stg,  

ExpandedFarmCostTable.Coverable_or_PolicyLine_CovPattern as COV_TYPE_CD_stg,                                           

foptrans.UpdateTime_stg  as UpdateTime_stg,  /* EIM-49963 */
ppa.editeffectivedate_stg  as busn_dt,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD_stg,                                                                             

SUM(foptrans.Amount_stg) as PREMIUM_TRANS_AMT_stg ,                                                                            

job.JobNumber_stg as Jobnumber_stg,

cast(ppa.branchnumber_stg as  varchar(100)) as branchnumber_stg                                      

from DB_T_PROD_STAG.pcx_foptransaction foptrans

join

  ( Select distinct pp.PolicyNumber_stg,

  dwcov.dwelling_stg as DwellingID,

  outbuildingcov.Outbuilding_stg as OutbuildingID,

  livestockcov.Livestock_stg as LivestockID,

  machinerycov.Machinery_stg as MachineryID,

  fopfeedandseedcov.FeedAndSeed_stg as FeedAndSeedID,

  dwellschcov.FOPDwellingScheduleCovItem_stg as DwellScheduledItemID,

  farmschcov.FOPFarmownersLiScheduleCovItem_stg as FarmScheduledItemID,

  liabcov.FOPLiabilityScheduleCovItem_stg as LiabScheduledItemID,

  /** Fixed id *****/

          case

        when fopcost.FOPDwellingCov_stg is not null then ''pcx_fopdwellingcov''

        when fopcost.FOPOutbuildingCov_stg is not null then ''pcx_fopoutbuildingcov''

        when fopcost.FOPLivestockCov_stg is not null then ''pcx_foplivestockcov''

        when fopcost.FOPMachineryCov_stg is not null then ''pcx_fopmachinerycov''

        when fopcost.FOPFeedAndSeedCov_stg is not null then ''pcx_fopfeedandseedcov''

		when fopcost.FOPDwellingSchCovItemCov_stg is not null then ''pcx_fopdwellingschcovitemcov''

        when fopcost.FOPFarmownersLineSchCovItemCov_stg is not null then ''pcx_fopfarmownersschcovitemcov''

        when fopcost.FOPLiabilitySchCovItemCov_stg  is not null then ''pcx_fopliabilityschcovitemcov''

        end as Table_Name_For_FixedID,

  /***feat_id ****/

  case when fopcost.FOPDwellingCov_stg is not null then dwcov.PatternCode_stg

   when fopcost.FOPOutbuildingCov_stg is not null then outbuildingcov.PatternCode_stg

   when fopcost.FOPLivestockCov_stg is not null then livestockcov.PatternCode_stg

   when fopcost.FOPMachineryCov_stg is not null then machinerycov.PatternCode_stg

   when fopcost.FOPFeedAndSeedCov_stg is not null then fopfeedandseedcov.PatternCode_stg

   when fopcost.FOPDwellingSchCovItemCov_stg is not null then dwellschcov.PatternCode_stg

   when fopcost.FOPFarmownersLineSchCovItemCov_stg is not null then farmschcov.PatternCode_stg

   when fopcost.FOPLiabilitySchCovItemCov_stg is not null then liabcov.PatternCode_stg

  end as Coverable_or_PolicyLine_CovPattern,

  fopcost.ChargePattern_stg,fopcost.Subtype_stg,

  fopcost.ID_stg

  from DB_T_PROD_STAG.pcx_fopcost fopcost

   join DB_T_PROD_STAG.pc_policyperiod pp on fopcost.BranchID_stg=pp.ID_stg 

   left join ( select distinct dwellcov.PatternCode_stg, dwellcov.FixedID_stg, pp.PolicyNumber_stg,

   dwellcov.Dwelling_stg,dwellcov.branchid_stg

            from DB_T_PROD_STAG.pcx_fopdwellingcov dwellcov join DB_T_PROD_STAG.pc_policyperiod pp

            on dwellcov.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by dwellcov.branchid_stg , dwellcov.FixedID_stg 

order by coalesce(dwellcov.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,dwellcov.updatetime_stg desc,dwellcov.createtime_stg desc)=1

)  as dwcov

             on fopcost.FOPDwellingCov_stg = dwcov.FixedID_stg 

  and fopcost.branchid_stg = dwcov.branchid_stg

  and dwcov.PolicyNumber_stg=pp.PolicyNumber_stg



    left join ( select distinct outbuilding.PatternCode_stg, outbuilding.FixedID_stg, pp.PolicyNumber_stg,

   outbuilding.Outbuilding_stg,outbuilding.branchid_stg

            from DB_T_PROD_STAG.pcx_fopoutbuildingcov outbuilding join DB_T_PROD_STAG.pc_policyperiod pp

            on outbuilding.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by outbuilding.branchid_stg , outbuilding.FixedID_stg 

order by coalesce(outbuilding.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,outbuilding.updatetime_stg desc,outbuilding.createtime_stg desc)=1

)  as outbuildingcov

             on fopcost.FOPOutbuildingCov_stg = outbuildingcov.FixedID_stg 

  and fopcost.branchid_stg = outbuildingcov.branchid_stg 

  and outbuildingcov.PolicyNumber_stg=pp.PolicyNumber_stg



	left join ( select distinct livestock.PatternCode_stg, livestock.FixedID_stg, pp.PolicyNumber_stg,

	   livestock.Livestock_stg,livestock.branchid_stg

	            from DB_T_PROD_STAG.pcx_foplivestockcov livestock join DB_T_PROD_STAG.pc_policyperiod pp

	            on livestock.branchid_stg=pp.ID_stg

				qualify row_number() over(partition by livestock.branchid_stg , livestock.FixedID_stg 

order by coalesce(livestock.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,livestock.updatetime_stg desc,livestock.createtime_stg desc)=1

	)  as livestockcov

             on fopcost.FOPLivestockCov_stg = livestockcov.FixedID_stg 

  and fopcost.branchid_stg = livestockcov.branchid_stg

   and livestockcov.PolicyNumber_stg=pp.PolicyNumber_stg



  left join ( select distinct machinery.PatternCode_stg, machinery.FixedID_stg, pp.PolicyNumber_stg,

   machinery.Machinery_stg,machinery.branchid_stg

            from DB_T_PROD_STAG.pcx_fopmachinerycov machinery join DB_T_PROD_STAG.pc_policyperiod pp

            on machinery.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by machinery.branchid_stg , machinery.FixedID_stg 

order by coalesce(machinery.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,machinery.updatetime_stg desc,machinery.createtime_stg desc)=1

)  as machinerycov

             on fopcost.FOPMachineryCov_stg = machinerycov.FixedID_stg 

  and fopcost.branchid_stg = machinerycov.branchid_stg 

  and machinerycov.PolicyNumber_stg=pp.PolicyNumber_stg



	left join ( select distinct fopfeedandseed.PatternCode_stg, fopfeedandseed.FixedID_stg, pp.PolicyNumber_stg,

	   fopfeedandseed.FeedAndSeed_stg,fopfeedandseed.branchid_stg

	            from DB_T_PROD_STAG.pcx_fopfeedandseedcov fopfeedandseed join DB_T_PROD_STAG.pc_policyperiod pp

	            on fopfeedandseed.branchid_stg=pp.ID_stg

				qualify row_number() over(partition by fopfeedandseed.branchid_stg , fopfeedandseed.FixedID_stg 

order by coalesce(fopfeedandseed.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,fopfeedandseed.updatetime_stg desc,fopfeedandseed.createtime_stg desc)=1

	)  as fopfeedandseedcov

	             on fopcost.FOPFeedAndSeedCov_stg = fopfeedandseedcov.FixedID_stg 

	  and fopcost.branchid_stg = fopfeedandseedcov.branchid_stg

	  and fopfeedandseedcov.PolicyNumber_stg=pp.PolicyNumber_stg



   left join (select distinct dwellsch.PatternCode_stg, dwellsch.FixedID_stg, pp.PolicyNumber_stg,

   dwellsch.FOPDwellingScheduleCovItem_stg,dwellsch.branchid_stg

            from DB_T_PROD_STAG.pcx_fopdwellingschcovitemcov dwellsch join DB_T_PROD_STAG.pc_policyperiod pp

            on dwellsch.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by dwellsch.branchid_stg , dwellsch.FixedID_stg 

order by coalesce(dwellsch.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,dwellsch.updatetime_stg desc,dwellsch.createtime_stg desc)=1

			) as dwellschcov

             on fopcost.FOPDwellingSchCovItemCov_stg = dwellschcov.FixedID_stg 

			 and dwellschcov.PolicyNumber_stg=pp.PolicyNumber_stg

			 and fopcost.branchid_stg = dwellschcov.branchid_stg



   left join (select distinct liab.PatternCode_stg, liab.FixedID_stg, pp.PolicyNumber_stg,

   liab.FOPLiabilityScheduleCovItem_stg,liab.branchid_stg

            from  DB_T_PROD_STAG.pcx_fopliabilityschcovitemcov liab  join DB_T_PROD_STAG.pc_policyperiod pp

            on  liab.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by liab.branchid_stg , liab.FixedID_stg 

order by coalesce(liab.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,liab.updatetime_stg desc,liab.createtime_stg desc)=1

			) as  liabcov 

             on fopcost.FOPLiabilitySchCovItemCov_stg = liabcov.FixedID_stg 

			 and  liabcov.PolicyNumber_stg=pp.PolicyNumber_stg

			 and fopcost.branchid_stg = liabcov.branchid_stg



   left join (select distinct farmsch.PatternCode_stg,farmsch.FixedID_stg, pp.PolicyNumber_stg,

   farmsch.FOPFarmownersLiScheduleCovItem_stg,farmsch.branchid_stg

            from  DB_T_PROD_STAG.pcx_fopfarmownersschcovitemcov farmsch  join DB_T_PROD_STAG.pc_policyperiod pp

            on  farmsch.branchid_stg=pp.ID_stg

			qualify row_number() over(partition by farmsch.branchid_stg , farmsch.FixedID_stg 

order by coalesce(farmsch.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,farmsch.updatetime_stg desc,farmsch.createtime_stg desc)=1

   ) as farmschcov 	

   on fopcost.FOPFarmownersLineSchCovItemCov_stg = farmschcov.Fixedid_stg 

   and  farmschcov.PolicyNumber_stg=pp.PolicyNumber_stg

   and fopcost.branchid_stg = farmschcov.branchid_stg

   

   ) ExpandedFarmCostTable on foptrans.Cost_stg = ExpandedFarmCostTable.ID_stg



  left join DB_T_PROD_STAG.pctl_chargepattern chargepattern on ExpandedFarmCostTable.ChargePattern_stg = chargepattern.ID_stg

  left join DB_T_PROD_STAG.pctl_fopcost pctl_cost ON ExpandedFarmCostTable.subtype_stg = pctl_cost.ID_stg

  join DB_T_PROD_STAG.pc_policyperiod  ppa on foptrans.BranchID_stg = ppa.ID_stg and ExpandedFarmCostTable.PolicyNumber_stg=ppa.PolicyNumber_stg

  join DB_T_PROD_STAG.pc_job job on ppa.JobID_stg = job.ID_stg

  left join DB_T_PROD_STAG.pctl_job tl_job on job.Subtype_stg = tl_job.ID_stg

  left join DB_T_PROD_STAG.pc_policyline policyline on ppa.id_stg = policyline.BranchID_stg  and policyline.ExpirationDate_stg is null 

  left join DB_T_PROD_STAG.pctl_foppolicytype foppolicy ON policyline.FOPPolicyType_stg = foppolicy.ID_stg 

  left join DB_T_PROD_STAG.pc_policy policy on ppa.PolicyID_stg = policy.id_stg 

  left join DB_T_PROD_STAG.pc_account acc on policy.accountid_stg = acc.id_stg  

  join DB_T_PROD_STAG.pctl_policyperiodstatus pp_status on ppa.Status_stg=pp_status.ID_stg

  where chargepattern.name_stg = ''Premium'' 

  and ExpandedFarmCostTable.Coverable_or_PolicyLine_CovPattern is not null 

  and ((ppa.UpdateTime_stg > (:start_dttm)

  and ppa.UpdateTime_stg <= (:end_dttm))

  or (foptrans.UpdateTime_stg > (:start_dttm)

and foptrans.UpdateTime_stg <= (:end_dttm)))/* EIM-49963 */
 

group by  

COALESCE(ExpandedFarmCostTable.DwellingID,ExpandedFarmCostTable.OutbuildingID,ExpandedFarmCostTable.LivestockID,ExpandedFarmCostTable.MachineryID,ExpandedFarmCostTable.FeedAndSeedID,

ExpandedFarmCostTable.DwellScheduledItemID,ExpandedFarmCostTable.FarmScheduledItemID,ExpandedFarmCostTable.LiabScheduledItemID),

ExpandedFarmCostTable.Coverable_or_PolicyLine_CovPattern,

job.JobNumber_stg,

ppa.branchnumber_stg,

foptrans.UpdateTime_stg,/* EIM-49963 */
ppa.editeffectivedate_stg,

pp_status.TYPECODE_stg,

tl_job.TYPECODE_stg

)farm

QUALIFY ROW_NUMBER() OVER( PARTITION BY Prty_asset_id_stg,asset_type_stg,classification_code_stg,COV_TYPE_CD_stg,Inscrn_Mtrc_Type_CD_stg,Jobnumber_stg,Branchnumber_stg                                   					

ORDER BY updatetime_stg DESC,busn_dt DESC) = 1 /* EIM-49963					 */
)

pc_plcy_writtn_prem_x

where pc_plcy_writtn_prem_x.Prty_asset_id_stg  is not null 

group by Prty_asset_id_stg,asset_type_stg,classification_code_stg,COV_TYPE_CD_stg,UpdateTime_stg,busn_dt,Inscrn_Mtrc_Type_CD_stg,Jobnumber_stg,Branchnumber_stg

) SQ



LEFT OUTER JOIN(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_INSURANCE_CD

        on SQ.Inscrn_Mtrc_Type_CD_stg=XLAT_INSURANCE_CD.SRC_IDNTFTN_VAL

        

LEFT OUTER JOIN(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

/* AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'') */
        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_ASSET_CLASSIFICATION

        on SQ.classification_code_stg=XLAT_ASSET_CLASSIFICATION.SRC_IDNTFTN_VAL



LEFT OUTER JOIN(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_ASSET_SBTYPE

        ON SQ.asset_type_stg=XLAT_ASSET_SBTYPE.SRC_IDNTFTN_VAL

        

LEFT OUTER JOIN(select distinct

        FEAT_ID,

        NK_SRC_KEY

from DB_T_PROD_CORE.FEAT) FEAT

ON TRIM(SQ.COV_TYPE_CD)=FEAT.NK_SRC_KEY



LEFT OUTER JOIN(SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, 

INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN

QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1) 

INSRNC_QUOTN

ON SQ.Jobnumber_stg=INSRNC_QUOTN.NK_JOB_NBR

and SQ.Branchnumber_stg=INSRNC_QUOTN.VERS_NBR



LEFT OUTER JOIN(SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, 

PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, 

PRTY_ASSET.ASSET_DESC as ASSET_DESC, 

PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, 

PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, 

PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, 

PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, 

PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, 

PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, 

PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD,

PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 

FROM DB_T_PROD_CORE.PRTY_ASSET 

QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1) PRTY_ASSET

ON SQ.fixed_id=PRTY_ASSET.ASSET_HOST_ID_VAL

AND ASSET_SBTYPE=PRTY_ASSET.PRTY_ASSET_SBTYPE_CD

AND ASSET_CLASFCN=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD



LEFT OUTER JOIN(SELECT QUOTN_ASSET_FEAT_MTRC.PRTY_ASSET_ID as PRTY_ASSET_ID1, 

QUOTN_ASSET_FEAT_MTRC.QUOTN_ID as QUOTN_ID1, 

QUOTN_ASSET_FEAT_MTRC.FEAT_ID as FEAT_ID1, 

QUOTN_ASSET_FEAT_MTRC.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD1,

QUOTN_ASSET_FEAT_MTRC.QAF_MTRC_AMT as QAF_MTRC_AMT1,/* EIM-49963 */
QUOTN_ASSET_FEAT_MTRC.EDW_STRT_DTTM as EDW_STRT_DTTM1/* EIM-49963 */
FROM DB_T_PROD_CORE.QUOTN_ASSET_FEAT_MTRC

QUALIFY ROW_NUMBER() OVER(PARTITION BY  PRTY_ASSET_ID,QUOTN_ID,INSRNC_MTRC_TYPE_CD,FEAT_ID ORDER BY EDW_END_DTTM DESC) = 1) TGT_QUOTN_ASSET

on FEAT_ID=TGT_QUOTN_ASSET.FEAT_ID1

AND PRTY_ASSET_ID=TGT_QUOTN_ASSET.PRTY_ASSET_ID1

AND QUOTN_ID=TGT_QUOTN_ASSET.QUOTN_ID1

AND INSRNC_MTRC_TYPE_CD=TGT_QUOTN_ASSET.INSRNC_MTRC_TYPE_CD1)SQ1/* ------ */
) SRC
)
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
SQ_pc_plcy_asset_cvge_mtrc_x.FEAT_ID as FEAT_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.QUOTN_ID as QUOTN_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
SQ_pc_plcy_asset_cvge_mtrc_x.UpdateTime as EARNINGS_AS_OF_DT,
SQ_pc_plcy_asset_cvge_mtrc_x.Amount as amount,
SQ_pc_plcy_asset_cvge_mtrc_x.busn_dt as busn_dt,
SQ_pc_plcy_asset_cvge_mtrc_x.PRTY_ASSET_ID as PRTY_ASSET_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_FEAT_ID as TGT_FEAT_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_QUOTN_ID as TGT_QUOTN_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
NULL as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
SQ_pc_plcy_asset_cvge_mtrc_x.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_INSRNC_MTRC_TYPE_CD as TGT_INSRNC_MTRC_TYPE_CD,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_QAF_MTRC_AMT as lkp_PLCY_ASSET_CVGE_AMT1,
SQ_pc_plcy_asset_cvge_mtrc_x.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
CASE WHEN SQ_pc_plcy_asset_cvge_mtrc_x.TGT_FEAT_ID IS NULL THEN ''I'' ELSE ( CASE WHEN SQ_pc_plcy_asset_cvge_mtrc_x.Amount <> SQ_pc_plcy_asset_cvge_mtrc_x.TGT_QAF_MTRC_AMT THEN ''U'' ELSE ''R'' END ) END as Flag,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
SQ_pc_plcy_asset_cvge_mtrc_x.source_record_id
FROM
SQ_pc_plcy_asset_cvge_mtrc_x
);


-- Component rtr_insupd_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insupd_Insert AS
SELECT
exp_ins_upd.FEAT_ID as FEAT_ID,
exp_ins_upd.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd.QUOTN_ID as QUOTN_ID,
exp_ins_upd.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins_upd.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
exp_ins_upd.amount as amount,
exp_ins_upd.busn_dt as busn_dt,
exp_ins_upd.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as in_EDW_END_DTTM,
exp_ins_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.out_PRCS_ID as out_PRCS_ID,
exp_ins_upd.TGT_FEAT_ID as TGT_FEAT_ID,
exp_ins_upd.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_ins_upd.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
exp_ins_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.TGT_INSRNC_MTRC_TYPE_CD as TGT_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_PLCY_ASSET_CVGE_AMT1 as lkp_PLCY_ASSET_CVGE_AMT1,
exp_ins_upd.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp_ins_upd.Flag as Flag,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.Flag = ''I'' and exp_ins_upd.FEAT_ID IS NOT NULL and exp_ins_upd.PRTY_ASSET_ID IS NOT NULL and exp_ins_upd.QUOTN_ID IS NOT NULL;


-- Component rtr_insupd_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_insupd_Update AS
SELECT
exp_ins_upd.FEAT_ID as FEAT_ID,
exp_ins_upd.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd.QUOTN_ID as QUOTN_ID,
exp_ins_upd.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins_upd.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
exp_ins_upd.amount as amount,
exp_ins_upd.busn_dt as busn_dt,
exp_ins_upd.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as in_EDW_END_DTTM,
exp_ins_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.out_PRCS_ID as out_PRCS_ID,
exp_ins_upd.TGT_FEAT_ID as TGT_FEAT_ID,
exp_ins_upd.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_ins_upd.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
exp_ins_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.TGT_INSRNC_MTRC_TYPE_CD as TGT_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_PLCY_ASSET_CVGE_AMT1 as lkp_PLCY_ASSET_CVGE_AMT1,
exp_ins_upd.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp_ins_upd.Flag as Flag,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.Flag = ''U'' and exp_ins_upd.FEAT_ID IS NOT NULL and exp_ins_upd.PRTY_ASSET_ID IS NOT NULL and exp_ins_upd.QUOTN_ID IS NOT NULL;


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insupd_Update.TGT_FEAT_ID as TGT_FEAT_ID,
rtr_insupd_Update.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
rtr_insupd_Update.TGT_QUOTN_ID as TGT_QUOTN_ID,
rtr_insupd_Update.TGT_INSRNC_MTRC_TYPE_CD as TGT_INSRNC_MTRC_TYPE_CD,
rtr_insupd_Update.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
rtr_insupd_Update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
rtr_insupd_Update.out_PRCS_ID as out_PRCS_ID,
rtr_insupd_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insupd_Update.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
1 as UPDATE_STRATEGY_ACTION,
rtr_insupd_Update.source_record_id
FROM
rtr_insupd_Update
);


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
upd_update.TGT_FEAT_ID as TGT_FEAT_ID,
upd_update.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
upd_update.TGT_QUOTN_ID as TGT_QUOTN_ID,
upd_update.TGT_INSRNC_MTRC_TYPE_CD as TGT_INSRNC_MTRC_TYPE_CD,
upd_update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
dateadd (second, -1,  upd_update.in_EDW_STRT_DTTM ) as o_EDW_END_DTTM1,
dateadd (second, -1,  upd_update.EARNINGS_AS_OF_DT ) as TRNS_END_DATE,
upd_update.source_record_id
FROM
upd_update
);


-- Component upd_update_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insupd_Update.FEAT_ID as FEAT_ID,
rtr_insupd_Update.PRTY_ASSET_ID as PRTY_ASSET_ID,
rtr_insupd_Update.QUOTN_ID as QUOTN_ID,
rtr_insupd_Update.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
rtr_insupd_Update.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
rtr_insupd_Update.amount as amount,
rtr_insupd_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insupd_Update.in_EDW_END_DTTM as in_EDW_END_DTTM,
rtr_insupd_Update.out_PRCS_ID as out_PRCS_ID,
rtr_insupd_Update.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
rtr_insupd_Update.busn_dt as busn_dt3,
0 as UPDATE_STRATEGY_ACTION,
rtr_insupd_Update.source_record_id
FROM
rtr_insupd_Update
);


-- Component upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insupd_Insert.FEAT_ID as FEAT_ID,
rtr_insupd_Insert.PRTY_ASSET_ID as PRTY_ASSET_ID,
rtr_insupd_Insert.QUOTN_ID as QUOTN_ID,
rtr_insupd_Insert.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
rtr_insupd_Insert.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
rtr_insupd_Insert.amount as amount,
rtr_insupd_Insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insupd_Insert.in_EDW_END_DTTM as in_EDW_END_DTTM,
rtr_insupd_Insert.out_PRCS_ID as out_PRCS_ID,
rtr_insupd_Insert.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
rtr_insupd_Insert.busn_dt as busn_dt1,
0 as UPDATE_STRATEGY_ACTION,
rtr_insupd_Insert.source_record_id
FROM
rtr_insupd_Insert
);


-- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
(
SELECT
upd_update_ins.FEAT_ID as FEAT_ID,
upd_update_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_update_ins.QUOTN_ID as QUOTN_ID,
upd_update_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
upd_update_ins.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
upd_update_ins.amount as amount,
upd_update_ins.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
upd_update_ins.in_EDW_END_DTTM as in_EDW_END_DTTM,
upd_update_ins.out_PRCS_ID as out_PRCS_ID,
upd_update_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
upd_update_ins.busn_dt3 as busn_dt3,
upd_update_ins.source_record_id
FROM
upd_update_ins
);


-- Component QUOTN_ASSET_FEAT_MTRC2, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_MTRC
USING exp_pass_to_tgt_upd ON (QUOTN_ASSET_FEAT_MTRC.PRTY_ASSET_ID = exp_pass_to_tgt_upd.TGT_PRTY_ASSET_ID AND QUOTN_ASSET_FEAT_MTRC.QUOTN_ID = exp_pass_to_tgt_upd.TGT_QUOTN_ID AND QUOTN_ASSET_FEAT_MTRC.FEAT_ID = exp_pass_to_tgt_upd.TGT_FEAT_ID AND QUOTN_ASSET_FEAT_MTRC.INSRNC_MTRC_TYPE_CD = exp_pass_to_tgt_upd.TGT_INSRNC_MTRC_TYPE_CD AND QUOTN_ASSET_FEAT_MTRC.EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM1)
WHEN MATCHED THEN UPDATE
SET
PRTY_ASSET_ID = exp_pass_to_tgt_upd.TGT_PRTY_ASSET_ID,
QUOTN_ID = exp_pass_to_tgt_upd.TGT_QUOTN_ID,
FEAT_ID = exp_pass_to_tgt_upd.TGT_FEAT_ID,
INSRNC_MTRC_TYPE_CD = exp_pass_to_tgt_upd.TGT_INSRNC_MTRC_TYPE_CD,
EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM1,
EDW_END_DTTM = exp_pass_to_tgt_upd.o_EDW_END_DTTM1,
TRANS_END_DTTM = exp_pass_to_tgt_upd.TRNS_END_DATE;


-- Component QUOTN_ASSET_FEAT_MTRC1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_MTRC
(
PRTY_ASSET_ID,
QUOTN_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
QUOTN_ASSET_STRT_DTTM,
FEAT_ID,
QUOTN_ASSET_FEAT_STRT_DTTM,
INSRNC_MTRC_TYPE_CD,
QAF_MTRC_STRT_DTTM,
QAF_MTRC_END_DTTM,
QAF_MTRC_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_upd_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_pass_to_tgt_upd_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_tgt_upd_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_to_tgt_upd_ins.busn_dt3 as QUOTN_ASSET_STRT_DTTM,
exp_pass_to_tgt_upd_ins.FEAT_ID as FEAT_ID,
exp_pass_to_tgt_upd_ins.busn_dt3 as QUOTN_ASSET_FEAT_STRT_DTTM,
exp_pass_to_tgt_upd_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_pass_to_tgt_upd_ins.busn_dt3 as QAF_MTRC_STRT_DTTM,
exp_pass_to_tgt_upd_ins.in_EDW_END_DTTM as QAF_MTRC_END_DTTM,
exp_pass_to_tgt_upd_ins.amount as QAF_MTRC_AMT,
exp_pass_to_tgt_upd_ins.out_PRCS_ID as PRCS_ID,
exp_pass_to_tgt_upd_ins.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_upd_ins.in_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_upd_ins.EARNINGS_AS_OF_DT as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_upd_ins;


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_insert.FEAT_ID as FEAT_ID,
upd_insert.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_insert.QUOTN_ID as QUOTN_ID,
upd_insert.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
upd_insert.amount as amount,
upd_insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
upd_insert.in_EDW_END_DTTM as in_EDW_END_DTTM,
upd_insert.out_PRCS_ID as out_PRCS_ID,
upd_insert.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as ASSET_CNTRCT_ROLE_SBTYPE_CD1,
upd_insert.EARNINGS_AS_OF_DT as EARNINGS_AS_OF_DT,
upd_insert.busn_dt1 as busn_dt1,
upd_insert.source_record_id
FROM
upd_insert
);


-- Component QUOTN_ASSET_FEAT_MTRC, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_MTRC
(
PRTY_ASSET_ID,
QUOTN_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
QUOTN_ASSET_STRT_DTTM,
FEAT_ID,
QUOTN_ASSET_FEAT_STRT_DTTM,
INSRNC_MTRC_TYPE_CD,
QAF_MTRC_STRT_DTTM,
QAF_MTRC_END_DTTM,
QAF_MTRC_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_pass_to_tgt_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_tgt_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD1 as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_to_tgt_ins.busn_dt1 as QUOTN_ASSET_STRT_DTTM,
exp_pass_to_tgt_ins.FEAT_ID as FEAT_ID,
exp_pass_to_tgt_ins.busn_dt1 as QUOTN_ASSET_FEAT_STRT_DTTM,
exp_pass_to_tgt_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_pass_to_tgt_ins.busn_dt1 as QAF_MTRC_STRT_DTTM,
exp_pass_to_tgt_ins.in_EDW_END_DTTM as QAF_MTRC_END_DTTM,
exp_pass_to_tgt_ins.amount as QAF_MTRC_AMT,
exp_pass_to_tgt_ins.out_PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.in_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_ins.EARNINGS_AS_OF_DT as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins;


END; ';