-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_FEAT_PERIL_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''ASSET_CNTRCT_ROLE_SBTYPE''

         		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PLCY_SECTN_TYPE'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LKP_AGMT_ID,
$2 as LKP_PRTY_ASSET_ID,
$3 as LKP_FEAT_ID,
$4 as LKP_AGMT_ASSET_FEAT_STRT_DTTM,
$5 as LKP_RTG_PERIL_TYPE_CD,
$6 as LKP_EDW_STRT_DTTM,
$7 as AGMT_ID,
$8 as PRTY_ASSET_ID,
$9 as FEAT_ID,
$10 as Inscrn_Mtrc_Type_CD,
$11 as RTG_PERIL_TYPE_CD,
$12 as Amount,
$13 as Section_type,
$14 as Earnings_as_of_dt,
$15 as TRANS_STRT_DTTM,
$16 as TRANS_END_DTTM,
$17 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH out_loop AS (

SELECT	DISTINCT PublicID,FixedID,Asset_sbtype,Asset_Classification_code,Feat_NKsrckey,Inscrn_Mtrc_Type_CD,Peril_type,Amount,Section_type,Earnings_as_of_dt,TRANS_STRT_DTTM,cast(null as timestamp(6)) as TRANS_END_DTTM 

from(

select 

cast(PublicID as varchar(100)) as PublicID,

cast(FixedID as varchar(100)) as FixedID,

cast(max(case when Table_Name_For_FixedID=''pcx_dwelling_hoe'' then ''PRTY_ASSET_SBTYPE5''

when Table_Name_For_FixedID=''pcx_holineschedcovitem_alfa'' and 

Cov_Type_CD in (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') then ''PRTY_ASSET_SBTYPE5'' 

when Table_Name_For_FixedID=''pcx_holineschedcovitem_alfa'' and 

Cov_Type_CD =''HOSI_ScheduledPropertyItem_alfa'' then ''PRTY_ASSET_SBTYPE7'' 

else NULL  end) as varchar(50)) as Asset_sbtype,

cast(max(case when Table_Name_For_FixedID=''pcx_dwelling_hoe'' then ''PRTY_ASSET_CLASFCN1''

when Table_Name_For_FixedID=''pcx_holineschedcovitem_alfa'' and 

Cov_Type_CD in (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'',''pcx_holineschedcovitem_alfa'')  

then class_code else NULL  end) as varchar(50)) as Asset_Classification_code,

cast(Cov_Type_CD as varchar(100)) as Feat_NKsrckey,

cast(Inscrn_Mtrc_Type_CD as varchar(100)) as Inscrn_Mtrc_Type_CD,

cast(Peril_type as varchar(100)) as Peril_type,

cast(sum(Amount) as decimal(18,4)) as Amount,

cast(Section_type as varchar(100)) as Section_type,

cast(Earnings_as_of_dt as timestamp(6)) as Earnings_as_of_dt,

cast(TRANS_STRT_DTTM as timestamp(6)) as TRANS_STRT_DTTM,

cast(TYPECODE as varchar(100)) as TYPECODE

from

(/* Modified Query */
select distinct 

 pp.PublicID_stg PublicID, 

ExpandedHOCostTable.Coverable_or_PolicyLine_FixedID as FixedID,

ExpandedHOCostTable.Table_Name_For_FixedID,

/* ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern, */
class_code,

/* ''GWPC'' as asset_src_cd,  */
ExpandedHOCostTable.Coverable_or_PolicyLine_CovPattern as Cov_Type_CD,/* featid */
pcsa.typecode_stg Section_type,

HOPerilType.TYPECODE_stg as Peril_type,

pp.UpdateTime_stg   as TRANS_STRT_DTTM,

pp.EditEffectiveDate_stg as Earnings_as_of_dt,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

pcxhh.Amount_stg as Amount,

/* 0 as CVGE_CNT, */
ppsts.TYPECODE_stg as TYPECODE

/* pj.JobNumber_STG AS JobNumber, */
/* pp.branchnumber_STG AS branchnumber */


from DB_T_PROD_STAG.pcx_hotransaction_hoe pcxhh

 left join DB_T_PROD_STAG.pc_policyperiod pp on pcxhh.BranchID_stg = pp.ID_stg

   join

  (

  select distinct  

  case when pcxhh1.DwellingCov_stg is not null then ''pcx_dwelling_hoe''

   when pcxhh1.SchedItemCov_stg is not null then ''pcx_holineschedcovitem_alfa''

   when pcxhh1.HomeownersLineCov_stg is not null then ''pc_policyline''

  end as Table_Name_For_FixedID,



  case when pcxhh1.DwellingCov_stg is not null then pdh.FixedID_stg

   when pcxhh1.SchedItemCov_stg is not null then phcov.FixedID_stg

   when pcxhh1.HomeownersLineCov_stg is not null then HOLine_Unit.FixedID_stg

  end as Coverable_or_PolicyLine_FixedID,



  case when pcxhh1.DwellingCov_stg is not null then DwellingCovPattern.PatternID_stg

   when pcxhh1.SchedItemCov_stg is not null then SchedItemCovPattern.PatternID_stg  

   when pcxhh1.HomeownersLineCov_stg is not null then HOLineCovPattern.PatternID_stg 

  end as Coverable_or_PolicyLine_CovPattern,

  

  /*case when pcxhh1.DwellingCov_stg is not null then ''Dwelling_HOE''

   when pcxhh1.SchedItemCov_stg is not null then ''HOLineSchCovItem_alfa''

   when pcxhh1.HomeownersLineCov_stg is not null then ''HomeownersLine_HOE'' 

  end as UnitTypeCode,



  case when pcxhh1.DwellingCov_stg is not null then ''Dwelling at Dwelling Location''

   when pcxhh1.SchedItemCov_stg is not null then ''Scheduled Item''

   when pcxhh1.HomeownersLineCov_stg is not null then ''Property Line''

  end as UnitTypeName,*/

	phitem.ChoiceTerm1_stg as class_code,

pcxhh1.ID_stg                /* --newly added EIM_32692 */
	,pcxhh1.ChargePattern_stg      

	,pcxhh1.PerilType_alfa_stg    

	,pcxhh1.SectionType_alfa_stg  

/* ,pcxhh1.* --------changed as part perf tuning task EIM_32692 */


  from DB_T_PROD_STAG.pcx_homeownerscost_hoe pcxhh1

   /*Add unit-level coverages for homeowners*/

   left join DB_T_PROD_STAG.pcx_dwellingcov_hoe pcxdh on pcxhh1.DwellingCov_stg  = pcxdh.FixedID_stg and pcxdh.BranchID_stg = pcxhh1.branchid_stg

    left join DB_T_PROD_STAG.pcx_dwelling_hoe pdh on pcxdh.Dwelling_stg = pdh.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern DwellingCovPattern on pcxdh.PatternCode_stg = DwellingCovPattern.PatternID_stg



   left join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa phitem on pcxhh1.SchedItemCov_stg  = phitem.FixedID_stg and phitem.BranchID_stg = pcxhh1.branchid_stg

    left join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa phcov on phitem.HOLineSchCovItem_stg = phcov.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern SchedItemCovPattern on phitem.PatternCode_stg = SchedItemCovPattern.PatternID_stg



   /*Add policy-level coverages for homeowners*/ 

   left join DB_T_PROD_STAG.pcx_homeownerslinecov_hoe pclineov on pcxhh1.HomeownersLineCov_stg = pclineov.FixedID_stg and pclineov.BranchID_stg = pcxhh1.branchid_stg

    left join DB_T_PROD_STAG.pc_policyline HOLine_Unit on pclineov.HOLine_stg = HOLine_Unit.ID_stg

    left join DB_T_PROD_STAG.pc_etlclausepattern HOLineCovPattern on pclineov.PatternCode_stg = HOLineCovPattern.PatternID_stg

) ExpandedHOCostTable on pcxhh.HomeownersCost_stg = ExpandedHOCostTable.ID_stg /*  and pp.id = ExpandedHOCostTable.BranchID */


  left join DB_T_PROD_STAG.pctl_chargepattern pctlc on ExpandedHOCostTable.ChargePattern_stg = pctlc.ID_stg

/* left join DB_T_PROD_STAG.pctl_pacost pctlp on ExpandedHOCostTable.Subtype_stg = pctlp.ID_stg */
  join DB_T_PROD_STAG.pctl_periltype_alfa HOPerilType on ExpandedHOCostTable.PerilType_alfa_stg = HOPerilType.ID_stg

   left join DB_T_PROD_STAG.pctl_policyperiodstatus ppsts on ppsts.ID_stg=pp.Status_stg

  /* left join DB_T_PROD_STAG.pc_job pj on pp.JobID_stg = pj.ID_stg

    left join DB_T_PROD_STAG.pctl_job pcj on pj.Subtype_stg = pcj.ID_stg

   left join DB_T_PROD_STAG.pc_producercode pcp on pp.ProducerCodeOfRecordID_stg= pcp.ID_stg

   left join DB_T_PROD_STAG.pc_uwcompany pcuw on pp.UWCompany_stg = pcuw.ID_stg

   left join DB_T_PROD_STAG.pctl_jurisdiction pcj1 on pp.BaseState_stg = pcj1.ID_stg

   left join DB_T_PROD_STAG.pc_policyline ppline on pp.id_stg = ppline.BranchID_stg

   left join DB_T_PROD_STAG.pctl_hopolicytype_hoe pchopol on ppline.HOPolicyType_stg = pchopol.ID_stg

   left join DB_T_PROD_STAG.pc_policy pol on pp.PolicyID_stg = pol.id_stg

left join DB_T_PROD_STAG.pc_account pa on pol.AccountID_stg = pa.ID_stg*/ /* changed as part perf tuning task EIM_32692 */
    left join DB_T_PROD_STAG.pctl_sectiontype_alfa pcsa on ExpandedHOCostTable.SectionType_alfa_stg=pcsa.ID_stg

where pctlc.name_stg = ''Premium''  

and HOPerilType.TYPECODE_stg is not null               /* rated by peril */
/* and  (pcj.TYPECODE =''AL'' and pchopol.TYPECODE= ''HO3'' )         --Alabama Homeowner */
and pp.UpdateTime_stg > (:Start_dttm)

	and  pp.UpdateTime_stg <= (:End_dttm)  )a

group by 

PublicID,

FixedID,

Cov_Type_CD,

Inscrn_Mtrc_Type_CD,

Peril_type,

Section_type,

Earnings_as_of_dt,

TRANS_STRT_DTTM,

TYPECODE

union

select cast(pp.PublicID_stg as varchar(100)) as PublicID,

cast(ExpandedCostTable.Coverable_or_PolicyLine_FixedID as varchar(100)) as FixedID,

cast(max(case

		when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ''PRTY_ASSET_SBTYPE13''

		else NULL

	end) as varchar(50)) as Asset_sbtype,

cast(max(case

		when ExpandedCostTable.Table_Name_For_FixedID = ''pcx_bp7classification'' then ExpandedCostTable.class_code

		else NULL

	end) as varchar(50)) as Asset_Classification_code,

cast(ExpandedCostTable.Coverable_or_PolicyLine_CovPattern as varchar(100)) as Feat_NKsrckey,

cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

cast(null as varchar(100)) as Peril_type,

cast(SUM(tr.Amount_stg) as decimal(18,4)) as Amount,

cast(pcsa.typecode_stg as varchar(100)) as Section_type, 

cast(pp.EditEffectiveDate_stg as timestamp(6)) as Earnings_as_of_dt,

cast(pp.UpdateTime_stg as timestamp(6)) as TRANS_STRT_DTTM,

cast(ppsts.TYPECODE_stg  as varchar(100)) as TYPECODE

from  DB_T_PROD_STAG.pcx_bp7transaction tr

left join(

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

			when cost.BuildingCov_stg is not null then b.FixedID_stg

			when cost.LocationCov_stg is not null then l.FixedID_stg

			when cost.ClassificationCov_stg is not null then c.FixedID_stg

			when cost.LineCoverage_stg is not null then bp7line.FixedID_stg

			when cost.LocSchedCovItemCov_stg is not null then ls.FixedID_stg

			when cost.BldgSchedCovItemCov_stg is not null then bs.FixedID_stg

			when cost.LineSchedCovItemCov_stg is not null then lis.FixedID_stg

		end as Coverable_or_PolicyLine_FixedID,

		

		case

			when cost.BuildingCov_stg is not null then bpattern.PatternID_stg

			when cost.LocationCov_stg is not null then lpattern.PatternID_stg

			when cost.ClassificationCov_stg is not null then cpattern.PatternID_stg

			when cost.LineCoverage_stg is not null then lipattern.PatternID_stg

			when cost.LocSchedCovItemCov_stg is not null then lspattern.PatternID_stg

			when cost.BldgSchedCovItemCov_stg is not null then bspattern.PatternID_stg

			when cost.LineSchedCovItemCov_stg is not null then lispattern.PatternID_stg

		end as Coverable_or_PolicyLine_CovPattern,

		

		/*case

			when cost.BuildingCov_stg is not null then bpattern.Name_stg

			when cost.LocationCov_stg is not null then lpattern.Name_stg

			when cost.ClassificationCov_stg is not null then cpattern.Name_stg

			when cost.LineCoverage_stg is not null then lipattern.Name_stg

			when cost.LocSchedCovItemCov_stg is not null then lspattern.Name_stg

			when cost.BldgSchedCovItemCov_stg is not null then bspattern.Name_stg

			when cost.LineSchedCovItemCov_stg is not null then lispattern.Name_stg

		end as Coverable_or_PolicyLine_CovName,

		null as UnitTypeCode,

null as UnitTypeName,*/ /* -changed as part perf tuning task EIM_32692 */
		cost.ID_stg,

		cost.ChargePattern_STG,

		cp.TYPECODE_STG as class_code,

		cost.SectionType_alfa_STG

		

	from DB_T_PROD_STAG.pcx_bp7cost cost

/* Building DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7buildingcov bcov on cost.BuildingCov_stg = bcov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7building b on bcov.Building_stg = b.id_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern bpattern on bcov.PatternCode_stg = bpattern.PatternID_stg

	

/* Location DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7locationcov lcov on cost.LocationCov_stg = lcov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7location l on lcov.Location_stg = l.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern lpattern on lcov.PatternCode_stg = lpattern.PatternID_stg

	

/* Classification DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7classificationcov ccov on cost.ClassificationCov_stg = ccov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7classification c on ccov.Classification_stg = c.ID_stg

	left join DB_T_PROD_STAG.pctl_bp7classificationproperty cp on c.bp7classpropertytype_stg = cp.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern cpattern on ccov.PatternCode_stg= cpattern.PatternID_stg

	

/* Line DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7linecov licov on cost.LineCoverage_stg = licov.ID_stg

	left join DB_T_PROD_STAG.pc_policyline bp7line on licov.BP7Line_stg = bp7line.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern lipattern on licov.PatternCode_stg = lipattern.PatternID_stg



/* Location Scheduled Item DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7locschedcovitemcov lscov on cost.LocSchedCovItemCov_stg = lscov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7locschedcovitem ls on lscov.LocSchedCovItem_stg = ls.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern lspattern on lscov.PatternCode_stg = lspattern.PatternID_stg



/* Building Scheduled Item DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bscov on cost.BldgSchedCovItemCov_stg = bscov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7bldgschedcovitem bs on bscov.BldgSchedCovItem_stg = bs.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern bspattern on bscov.PatternCode_stg = bspattern.PatternID_stg



/* Line Scheduled Item DB_T_CORE_DM_PROD.Coverage */
	left join DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov liscov on cost.LineSchedCovItemCov_stg = liscov.ID_stg

	left join DB_T_PROD_STAG.pcx_bp7lineschedcovitem lis on liscov.LineSchedCovItem_stg = lis.ID_stg

	left join DB_T_PROD_STAG.pc_etlclausepattern lispattern on liscov.PatternCode_stg = lispattern.PatternID_stg

	) ExpandedCostTable on tr.BP7Cost_stg = ExpandedCostTable.ID_stg

	

left join DB_T_PROD_STAG.pctl_chargepattern pcc on ExpandedCostTable.ChargePattern_stg = pcc.ID_stg

left join DB_T_PROD_STAG.pc_policyperiod pp on tr.BranchID_stg = pp.ID_stg

left join DB_T_PROD_STAG.pctl_policyperiodstatus ppsts on ppsts.ID_stg=pp.Status_stg

/* left join DB_T_PROD_STAG.pc_job pcj on pp.JobID_stg = pcj.ID_stg */
left join DB_T_PROD_STAG.pctl_sectiontype_alfa pcsa on ExpandedCostTable.SectionType_alfa_stg=pcsa.ID_stg

where pcc.name_stg = ''Premium''

and pp.UpdateTime_stg > (:Start_dttm)

and pp.UpdateTime_stg <= (:End_dttm)

and ExpandedCostTable.Coverable_or_PolicyLine_CovPattern is not null

group by

pp.PublicID_stg,

ExpandedCostTable.Coverable_or_PolicyLine_FixedID,

ExpandedCostTable.Coverable_or_PolicyLine_CovPattern,

pcsa.typecode_stg,

pp.UpdateTime_stg,

pp.EditEffectiveDate_stg,

ppsts.TYPECODE_stg) as a

where	Asset_sbtype  IS NOT NULL  

	and Asset_Classification_code is not null 

	and typecode=''BOUND''

)



SELECT LKP_AGMT_ASSET_FEAT_PERIL.AGMT_ID as LKP_AGMT_ID, LKP_AGMT_ASSET_FEAT_PERIL.PRTY_ASSET_ID as LKP_PRTY_ASSET_ID, LKP_AGMT_ASSET_FEAT_PERIL.FEAT_ID as LKP_FEAT_ID, LKP_AGMT_ASSET_FEAT_PERIL.AGMT_ASSET_FEAT_STRT_DTTM as LKP_AGMT_ASSET_FEAT_STRT_DTTM, LKP_AGMT_ASSET_FEAT_PERIL.RTG_PERIL_TYPE_CD as LKP_RTG_PERIL_TYPE_CD, LKP_AGMT_ASSET_FEAT_PERIL.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,

SQ.AGMT_ID,SQ.PRTY_ASSET_ID,SQ.FEAT_ID,Inscrn_Mtrc_Type_CD,SQ.RTG_PERIL_TYPE_CD,Amount,Section_type,Earnings_as_of_dt,TRANS_STRT_DTTM,TRANS_END_DTTM FROM (

SELECT LKP_AGMT_PPV.AGMT_ID,lkp_prty_asset_id.PRTY_ASSET_ID,lkp_feat.FEAT_ID,Inscrn_Mtrc_Type_CD,case when XLAT_SRC.TGT_IDNTFTN_VAL is null then ''UNK'' else XLAT_SRC.TGT_IDNTFTN_VAL end as RTG_PERIL_TYPE_CD,

Amount,Section_type,Earnings_as_of_dt,TRANS_STRT_DTTM,TRANS_END_DTTM FROM (



SELECT * FROM out_loop

	

LEFT OUTER JOIN (SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL1, TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL1 

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE

ON LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE.SRC_IDNTFTN_VAL1 = out_loop.Asset_sbtype



LEFT OUTER JOIN ( SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL2, TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL2 

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

/* AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'') */
AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN

ON LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN.SRC_IDNTFTN_VAL2 = out_loop.Asset_Classification_code



LEFT OUTER JOIN (SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL, TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''RTG_PERIL_TYPE'' AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL

ON LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL.SRC_IDNTFTN_VAL = Peril_type

) XLAT_SRC



LEFT OUTER JOIN (SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.FEAT 

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') lkp_feat

ON lkp_feat.NK_SRC_KEY = XLAT_SRC.Feat_NKsrckey



LEFT OUTER JOIN (SELECT	AGMT.AGMT_ID as AGMT_ID, AGMT.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.AGMT 

where agmt_type_cd= ''PPV'' AND CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') LKP_AGMT_PPV

ON LKP_AGMT_PPV.NK_SRC_KEY = XLAT_SRC.PublicID



LEFT OUTER JOIN (SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 

FROM DB_T_PROD_CORE.PRTY_ASSET 

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') lkp_prty_asset_id

ON lkp_prty_asset_id.ASSET_HOST_ID_VAL = XLAT_SRC.FixedID

AND lkp_prty_asset_id.PRTY_ASSET_SBTYPE_CD = XLAT_SRC.TGT_IDNTFTN_VAL1

AND lkp_prty_asset_id.PRTY_ASSET_CLASFCN_CD = XLAT_SRC.TGT_IDNTFTN_VAL2

) SQ



LEFT OUTER JOIN (SELECT AGMT_ASSET_FEAT_PERIL.AGMT_ID AS AGMT_ID, AGMT_ASSET_FEAT_PERIL.FEAT_ID AS FEAT_ID, AGMT_ASSET_FEAT_PERIL.PRTY_ASSET_ID AS PRTY_ASSET_ID, AGMT_ASSET_FEAT_PERIL.AGMT_ASSET_FEAT_STRT_DTTM AS AGMT_ASSET_FEAT_STRT_DTTM, 

 AGMT_ASSET_FEAT_PERIL.RTG_PERIL_TYPE_CD AS RTG_PERIL_TYPE_CD, AGMT_ASSET_FEAT_PERIL.EDW_STRT_DTTM AS EDW_STRT_DTTM

FROM DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL 

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') LKP_AGMT_ASSET_FEAT_PERIL

ON LKP_AGMT_ASSET_FEAT_PERIL.RTG_PERIL_TYPE_CD = SQ.RTG_PERIL_TYPE_CD

AND LKP_AGMT_ASSET_FEAT_PERIL.AGMT_ID = SQ.AGMT_ID

AND LKP_AGMT_ASSET_FEAT_PERIL.FEAT_ID = SQ.FEAT_ID

AND LKP_AGMT_ASSET_FEAT_PERIL.PRTY_ASSET_ID = SQ.PRTY_ASSET_ID
) SRC
)
);


-- Component exp_pass_frm_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_frm_src AS
(
SELECT
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_AGMT_ID as LKP_AGMT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_PRTY_ASSET_ID as LKP_PRTY_ASSET_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_FEAT_ID as LKP_FEAT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_RTG_PERIL_TYPE_CD as LKP_RTG_PERIL_TYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_AGMT_ASSET_FEAT_STRT_DTTM as LKP_AGMT_ASSET_FEAT_STRT_DTTM,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.AGMT_ID as AGMT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.PRTY_ASSET_ID as PRTY_ASSET_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.FEAT_ID as FEAT_ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE */ as ASSET_CNTRCT_ROLE_SBTYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Earnings_as_of_dt as Earnings_as_of_dt1,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
CASE WHEN LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */ END as Section_type1,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Amount as Amount,
CASE WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM END as TRANS_STRT_DTTM1,
CASE WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM END as TRANS_END_DTTM1,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Earnings_as_of_dt as Earnings_as_of_dt,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id,
row_number() over (partition by SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id order by SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id) as RNK
FROM
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Section_type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Section_type
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
exp_pass_frm_src.LKP_AGMT_ID as lkp_AGMT_ID,
exp_pass_frm_src.LKP_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_pass_frm_src.LKP_FEAT_ID as lkp_FEAT_ID,
exp_pass_frm_src.LKP_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_pass_frm_src.LKP_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_pass_frm_src.LKP_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
MD5 ( exp_pass_frm_src.LKP_RTG_PERIL_TYPE_CD || TO_CHAR ( exp_pass_frm_src.LKP_AGMT_ASSET_FEAT_STRT_DTTM ) ) as lkp_checksum,
exp_pass_frm_src.FEAT_ID as in_FEAT_ID,
exp_pass_frm_src.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_pass_frm_src.AGMT_ID as in_AGMT_ID,
exp_pass_frm_src.RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_pass_frm_src.ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_frm_src.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_pass_frm_src.Section_type1 as Section_type,
exp_pass_frm_src.Earnings_as_of_dt as Earnings_as_of_dt,
exp_pass_frm_src.Amount as Amount,
MD5 ( exp_pass_frm_src.RTG_PERIL_TYPE_CD || TO_CHAR ( exp_pass_frm_src.Earnings_as_of_dt1 ) ) as in_checksum,
exp_pass_frm_src.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_frm_src.TRANS_END_DTTM1 as TRANS_END_DTTM,
CASE WHEN lkp_checksum IS NULL THEN ''I'' ELSE ( CASE WHEN lkp_checksum <> in_checksum THEN ''U'' ELSE ''R'' END ) END as ins_upd_flag,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) as AGMT_ASSET_STRT_DTTM,
:PRCS_ID as PRCS_ID,
exp_pass_frm_src.source_record_id
FROM
exp_pass_frm_src
);


-- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table RTRTRANS_INSERT AS
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''I'' and exp_ins_upd.in_FEAT_ID IS NOT NULL and exp_ins_upd.in_PRTY_ASSET_ID IS NOT NULL and exp_ins_upd.in_AGMT_ID IS NOT NULL;


-- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
create or replace temporary table RTRTRANS_UPDATE as
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''U'' and exp_ins_upd.in_FEAT_ID IS NOT NULL and exp_ins_upd.in_PRTY_ASSET_ID IS NOT NULL and exp_ins_upd.in_AGMT_ID IS NOT NULL;


-- Component exp_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_upd AS
(
SELECT
RTRTRANS_UPDATE.lkp_AGMT_ID as lkp_AGMT_ID3,
RTRTRANS_UPDATE.lkp_FEAT_ID as lkp_FEAT_ID3,
RTRTRANS_UPDATE.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID3,
RTRTRANS_UPDATE.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD3,
RTRTRANS_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
DATEADD(''second'', - 1, RTRTRANS_UPDATE.EDW_STRT_DTTM) as EDW_END_DTTM,
DATEADD(''second'', - 1, RTRTRANS_UPDATE.TRANS_STRT_DTTM) as TRANS_END_DTTM,
RTRTRANS_UPDATE.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component AGMT_ASSET_FEAT_PERIL_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_FEAT_STRT_DTTM,
RTG_PERIL_TYPE_CD,
AAF_PERIL_STRT_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
RTRTRANS_INSERT.in_AGMT_ID as AGMT_ID,
RTRTRANS_INSERT.in_FEAT_ID as FEAT_ID,
RTRTRANS_INSERT.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_INSERT.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
RTRTRANS_INSERT.Earnings_as_of_dt4 as AGMT_ASSET_FEAT_STRT_DTTM,
RTRTRANS_INSERT.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as AAF_PERIL_STRT_DTTM,
RTRTRANS_INSERT.PRCS_ID as PRCS_ID,
RTRTRANS_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_INSERT.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
RTRTRANS_INSERT;


-- Component exp_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins AS
(
SELECT
RTRTRANS_UPDATE.in_AGMT_ID as AGMT_ID,
RTRTRANS_UPDATE.in_FEAT_ID as FEAT_ID,
RTRTRANS_UPDATE.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_UPDATE.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt,
RTRTRANS_UPDATE.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt1,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt2,
RTRTRANS_UPDATE.Section_type as AGMT_SECTN_CD,
NULL as INSRNC_MTRC_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt as Earnings_as_of_dt3,
RTRTRANS_UPDATE.Amount as AGMT_ASSET_FEAT_PERIL_AMT,
RTRTRANS_UPDATE.PRCS_ID as PRCS_ID,
RTRTRANS_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_UPDATE.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_UPDATE.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
RTRTRANS_UPDATE.TRANS_END_DTTM as TRANS_END_DTTM,
RTRTRANS_UPDATE.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_ins.AGMT_ID as AGMT_ID,
exp_ins.FEAT_ID as FEAT_ID,
exp_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
exp_ins.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_ins.Earnings_as_of_dt2 as Earnings_as_of_dt2,
exp_ins.AGMT_SECTN_CD as AGMT_SECTN_CD,
exp_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins.Earnings_as_of_dt3 as Earnings_as_of_dt3,
exp_ins.AGMT_ASSET_FEAT_PERIL_AMT as AGMT_ASSET_FEAT_PERIL_AMT,
exp_ins.PRCS_ID as PRCS_ID,
exp_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins.AGMT_ASSET_STRT_DTTM3 as AGMT_ASSET_STRT_DTTM3,
0 as UPDATE_STRATEGY_ACTION
FROM
exp_ins
);


-- Component update_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE update_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_upd.lkp_AGMT_ID3 as lkp_AGMT_ID3,
exp_upd.lkp_FEAT_ID3 as lkp_FEAT_ID3,
exp_upd.lkp_PRTY_ASSET_ID3 as lkp_PRTY_ASSET_ID3,
exp_upd.lkp_RTG_PERIL_TYPE_CD3 as lkp_RTG_PERIL_TYPE_CD3,
exp_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
exp_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM3 as lkp_AGMT_ASSET_FEAT_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_upd
);


-- Component AGMT_ASSET_FEAT_PERIL_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_FEAT_STRT_DTTM,
RTG_PERIL_TYPE_CD,
AAF_PERIL_STRT_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
upd_ins.AGMT_ID as AGMT_ID,
upd_ins.FEAT_ID as FEAT_ID,
upd_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as AGMT_ASSET_STRT_DTTM,
upd_ins.Earnings_as_of_dt1 as AGMT_ASSET_FEAT_STRT_DTTM,
upd_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as AAF_PERIL_STRT_DTTM,
upd_ins.PRCS_ID as PRCS_ID,
upd_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins.EDW_END_DTTM as EDW_END_DTTM,
upd_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
upd_ins;


-- Component AGMT_ASSET_FEAT_PERIL_upd_upd, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL
USING update_upd ON 
(UPDATE_STRATEGY_ACTION = 1 
AND AGMT_ASSET_FEAT_PERIL.AGMT_ID = update_upd.lkp_AGMT_ID3 
AND AGMT_ASSET_FEAT_PERIL.FEAT_ID = update_upd.lkp_FEAT_ID3 
AND AGMT_ASSET_FEAT_PERIL.PRTY_ASSET_ID = update_upd.lkp_PRTY_ASSET_ID3 
AND AGMT_ASSET_FEAT_PERIL.RTG_PERIL_TYPE_CD = update_upd.lkp_RTG_PERIL_TYPE_CD3 
AND AGMT_ASSET_FEAT_PERIL.EDW_STRT_DTTM = update_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = update_upd.EDW_END_DTTM,
TRANS_STRT_DTTM = update_upd.TRANS_END_DTTM
;


END; ';