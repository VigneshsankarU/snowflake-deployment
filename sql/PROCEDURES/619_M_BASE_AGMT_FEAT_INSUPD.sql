-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_FEAT_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_pc_agmt_feat_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_agmt_feat_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TG_AGMT_ID,
$2 as TG_FEAT_ID,
$3 as TG_AGMT_FEAT_ROLE_CD,
$4 as TG_AGMT_FEAT_START_DTTM,
$5 as TG_AGMT_FEAT_END_DTTM,
$6 as TG_AGMT_FEAT_AMT,
$7 as TG_AGMT_FEAT_RATE,
$8 as TG_AGMT_FEAT_QTY,
$9 as TG_AGMT_FEAT_NUM,
$10 as TG_VAL_TYPE_CD,
$11 as TG_EDW_STRT_DTTM,
$12 as TG_EDW_END_DTTM,
$13 as AGMT_ID,
$14 as FEAT_ID,
$15 as SRC_AGMT_FEAT_ROLE_CD,
$16 as SRC_AGMT_FEAT_STRT_DT,
$17 as SRC_AGMT_FEAT_AMT,
$18 as SRC_FEAT_RATE,
$19 as FEAT_QTY,
$20 as FEAT_NUM,
$21 as AGMT_FEAT_DT,
$22 as SRC_FEAT_TXT,
$23 as SRC_AGMT_FEAT_IND,
$24 as Eligible,
$25 as ENDDATE,
$26 as FEAT_EFFECT_TYPE_CD,
$27 as VAL_TYP_CD,
$28 as SRC_TRANS_START_DTTM,
$29 as RETIRED,
$30 as SOURCEDATA,
$31 as TARGETDATA,
$32 as INSUPD,
$33 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH /*EIM-48781-FARM CHANGES BEGINS*/

farm_temp as (select  distinct pp.PolicyNumber_stg, 

case when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg  else polcov.EffectiveDate_stg end as startdate, 

case when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg  else polcov.ExpirationDate_stg end as enddate, 

case when polcov.typ = ''MODIFIER'' then pe.PatternID_stg  else pc.PatternID_stg end as nk_public_id, 

case when polcov.typ = ''EXCLUSION'' then ''CLAUSE''  else polcov.typ end as FEAT_SBTYPE_CD, 

cast(ratemodifier as varchar(255)) as feat_amt,

cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,

pp.PublicID_stg, pp.Createtime_stg, polcov.feat_rate, pp.updatetime_stg,

pp.Retired_stg, pda.typecode_stg as feat_effect_type_cd, cast(null as varchar(255)) as feat_val,

cast(null as varchar(255)) as feat_CovTermType, 

 ( :start_dttm) as start_dttm,

( :end_dttm) as end_dttm,  

cast(polcov.Eligible as varchar(10)) as Eligible 

from (select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

cast(''EXCLUSION'' as varchar(250)) as typ, EffectiveDate_stg,

ExpirationDate_stg, cast(NULL as varchar(255)) as ratemodifier,

cast(NULL as varchar(255)) AS DiscountSurcharge_alfa, cast(NULL as varchar(255)) as feat_rate,

cast(NULL as varchar(250)) as Eligible 

from DB_T_PROD_STAG.pcx_fopfarmownerslineexcl fexcl

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fexcl.BranchID_stg 

where  (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= (:end_dttm) 

 qualify row_number() over (partition by BranchID_stg,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,fexcl.updatetime_stg desc,fexcl.createtime_stg desc)=1

  union 

select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

cast( case when fop.Eligible_stg= 1 THEN fop.RateModifier_stg ELSE 0 end as varchar(255)) as feat_rate, 

cast(Eligible_stg as varchar(10)) as Eligible 

from DB_T_PROD_STAG.pcx_foplinemod fop 

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg

where  (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

  and pp.updatetime_stg > ( :start_dttm)  and pp.updatetime_stg <= ( :end_dttm) 

  qualify row_number() over (partition by BranchID_stg,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,fop.updatetime_stg desc,fop.createtime_stg desc)=1

 ) polcov 

left join DB_T_PROD_STAG.pc_etlmodifierpattern pe on pe.patternid_stg = polcov.PatternCode_stg and polcov.typ = ''MODIFIER'' 

left join DB_T_PROD_STAG.pc_etlclausepattern pc  on pc.patternid_stg = polcov.PatternCode_stg and polcov.typ = ''EXCLUSION'' 

inner join (  select  cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

createtime_stg, updatetime_stg,Retired_stg from DB_T_PROD_STAG.pc_policyperiod ) pp on pp.id = polcov.BranchID 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.id_stg = pp.Status_stg 

inner join DB_T_PROD_STAG.pc_job pj on pj.id_stg = pp.JobID_stg 

inner join DB_T_PROD_STAG.pctl_job pcj on pcj.id_stg=pj.Subtype_stg 

LEFT JOIN DB_T_PROD_STAG.pctl_discountsurcharge_alfa pda ON polcov.DiscountSurcharge_alfa = pda.ID_stg 

where   (pc.Name_stg not like''%ZZ%'' or pe.Name_stg not like''%ZZ%'') and pps.TYPECODE_stg = ''Bound'' 

 and pp.updatetime_stg > ( :start_dttm)  and pp.updatetime_stg <= ( :end_dttm)     

 union

 select distinct pp.PolicyNumber_stg, 

case when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg else polcov.EffectiveDate_stg end startdate, 

case when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg else polcov.ExpirationDate_stg end enddate, 

case when covterm.CovTermType = ''Package'' then package.packagePatternID 

     when covterm.CovTermType = ''Option'' and polcov.val is not null then optn.optionPatternID 

 when covterm.CovTermType = ''Clause'' then covterm.clausePatternID else covterm.covtermPatternID end nk_public_id, 

case when covterm.CovTermType = ''Package'' then cast (''PACKAGE'' as varchar (50)) 

 when covterm.CovTermType = ''Option'' and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) 

 when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) else cast (''COVTERM'' as varchar (50)) end FEAT_SBTYPE_CD, 

case when covterm.CovTermType = ''Option'' and optn.ValueType = ''Money'' then optn.Value1 

 when covterm.CovTermType <> ''Option'' then polcov.val end feat_amt, 

case when optn.ValueType = ''count'' then optn.Value1 end feat_qty, 

case when optn.ValueType in (''days'', ''hours'', ''other'') then optn.value1 end feat_num, 

pp.PublicID_stg, 

pp.Createtime_stg, 

case when optn.ValueType=''Percent'' then optn.Value1 end feat_rate, 

polcov.updatetime_stg, 

pp.Retired_stg AS Retired,

cast (null as varchar(50)) feat_effect_type_cd, 

polcov.val feat_val,

covterm.CovTermType feat_CovTermType,

 ( :start_dttm) start_dttm,

( :end_dttm) end_dttm,  

cast(NULL as varchar(10)) as Eligible 

from (/* DB_T_PROD_STAG.pcx_fopfarmownerslinecov */
select * from (select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''StringTerm1'' AS VARCHAR(250)) as columnname, cast(StringTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where StringTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''BooleanTerm1'' AS VARCHAR(250)) as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where BooleanTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

UNION

select  distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg 

from  DB_T_PROD_STAG.pcx_fopfarmownerslinecov  fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

  and pp.updatetime_stg > ( :start_dttm)   and pp.updatetime_stg <= ( :end_dttm) 

 where (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

 and ChoiceTerm1Avl_stg is null 

  and DirectTerm1Avl_stg is null 

  and StringTerm1Avl_stg is null 

  and BooleanTerm1Avl_stg is null 

) as fopline 

qualify row_number() over (partition by BranchID,patterncode_stg,columnname  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1



union

/* DB_T_PROD_STAG.pcx_fopliabilitycov */
select * from (select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm4'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm4Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm5'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm5Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''StringTerm1'' AS VARCHAR(250)) as columnname, cast(StringTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where StringTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''StringTerm2'' AS VARCHAR(250)) as columnname, cast(StringTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where StringTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''StringTerm3'' AS VARCHAR(250)) as columnname, cast(StringTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where StringTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

and ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null and ChoiceTerm4Avl_stg is null

and ChoiceTerm5Avl_stg is null and StringTerm1Avl_stg is null and StringTerm2Avl_stg is null and StringTerm3Avl_stg is null

and DirectTerm1Avl_stg is null

/* liabexcl */
union

select  distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fexcl.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilityexcl  fexcl

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fexcl.BranchID_stg 

where  ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)

 union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilityexcl fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

and ChoiceTerm1Avl_stg is null

 ) as fopliab

qualify row_number() over (partition by BranchID,patterncode_stg,columnname  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1



union

/* DB_T_PROD_STAG.pcx_fopblanketcov */
select * from (select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where ChoiceTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg) 

union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > modeldate_stg) 

and ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null

and DirectTerm1Avl_stg is null

) as fopblank

qualify row_number() over (partition by BranchID,patterncode_stg,columnname  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1

) polcov 

inner join ( select cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

 PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

 Createtime_stg, updatetime_stg, Retired_stg from db_t_prod_stag.pc_policyperiod) pp on pp.id = polcov.BranchID 

left join ( select pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

 pcv.ColumnName_stg as columnname, pcv.CovTermType_stg as covtermtype,

 pcl.name_stg clausename from DB_T_PROD_STAG.pc_etlclausepattern pcl 

 join DB_T_PROD_STAG.pc_etlcovtermpattern pcv on pcl.id_stg = pcv.ClausePatternID_stg 

 union 

 select pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

 coalesce(pcv.ColumnName_stg,''Clause'') columnname, coalesce(pcv.CovTermType_stg,

 ''Clause'') covtermtype, pcl.name_stg clausename from DB_T_PROD_STAG.pc_etlclausepattern pcl 

 left join ( select * from DB_T_PROD_STAG.pc_etlcovtermpattern where  Name_stg not like ''ZZ%'' ) pcv on pcv.ClausePatternID_stg = pcl.ID_stg 

 where  pcl.Name_stg not like ''ZZ%'' and pcv.Name_stg is null 

 and OwningEntityType_stg in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',''FOPFarmownersLine'',''FOPFarmownersLineScheduleCovItem

'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',''FOPMachinery'',''FOPOutbuilding'') ) covterm 

 on covterm.clausePatternID = polcov.PatternCode_stg and covterm.ColumnName = polcov.columnname 

left outer join ( select pcv.PatternID_stg packagePatternID, pcv.PackageCode_stg cov_id,

 pcv.PackageCode_stg name1 from DB_T_PROD_STAG.pc_etlcovtermpackage pcv) package 

 on package.packagePatternID = polcov.val 

left outer join ( select pct.PatternID_stg optionPatternID, pct.optioncode_stg name1,

 cast(pct.value_stg as varchar(255)) as value1, pcv.ValueType_stg as ValueType 

 from DB_T_PROD_STAG.pc_etlcovtermpattern pcv 

 inner join DB_T_PROD_STAG.pc_etlcovtermoption pct on pcv.id_stg = pct.CoverageTermPatternID_stg ) optn on optn.optionPatternID = polcov.val 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.id_stg = pp.Status_stg 

inner join DB_T_PROD_STAG.pc_job pj on pj.id_stg = pp.JobID_stg 

inner join DB_T_PROD_STAG.pctl_job pcj on pcj.id_stg = pj.Subtype_stg 

where  covterm.clausename not like''%ZZ%'' and pps.TYPECODE_stg = ''Bound'' 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)

 UNION

/* ENDORSEMENTS */
select  distinct pp.PolicyNumber_stg, 

case when d.EffectiveDate_stg is null then pp.PeriodStart_stg else d.EffectiveDate_stg  end as startdate, 

case when d.ExpirationDate_stg is null then pp.PeriodEnd_stg else d.ExpirationDate_stg end as enddate,

d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

from  DB_T_PROD_STAG.pc_policyperiod pp 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps  on pp.status_stg = pps.id_stg 

left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.branchid_stg = pp.id_stg 

join DB_T_PROD_STAG.pcx_fopfarmownerslinecov a  on pp.id_stg = a.branchid_stg 

join DB_T_PROD_STAG.pc_formpattern c  on c.clausepatterncode_stg = a.patterncode_stg 

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg 

inner join DB_T_PROD_STAG.pctl_documenttype pd on pd.id_stg = c.DocumentType_stg 

where pd.typecode_stg = ''endorsement_alfa'' and c.Retired_stg = 0 

and d.RemovedOrSuperseded_stg is null and pps.typecode_stg = ''Bound'' 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)

and(a.EffectiveDate_stg is null or (a.EffectiveDate_stg > pp.ModelDate_stg and a.EffectiveDate_stg <> a.ExpirationDate_stg)) 

union

select  distinct pp.PolicyNumber_stg, 

case when d.EffectiveDate_stg is null then pp.PeriodStart_stg else d.EffectiveDate_stg  end as startdate, 

case when d.ExpirationDate_stg is null then pp.PeriodEnd_stg else d.ExpirationDate_stg end as enddate,

d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

from  DB_T_PROD_STAG.pc_policyperiod pp 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps  on pp.status_stg = pps.id_stg 

left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.branchid_stg = pp.id_stg 

join DB_T_PROD_STAG.pcx_fopliabilitycov a  on pp.id_stg = a.branchid_stg 

join DB_T_PROD_STAG.pc_formpattern c  on c.clausepatterncode_stg = a.patterncode_stg 

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg 

inner join DB_T_PROD_STAG.pctl_documenttype pd on pd.id_stg = c.DocumentType_stg 

where pd.typecode_stg = ''endorsement_alfa'' and c.Retired_stg = 0 

and d.RemovedOrSuperseded_stg is null and pps.typecode_stg = ''Bound'' 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)

and(a.EffectiveDate_stg is null or (a.EffectiveDate_stg > pp.ModelDate_stg and a.EffectiveDate_stg <> a.ExpirationDate_stg))

union

select  distinct pp.PolicyNumber_stg, 

case when d.EffectiveDate_stg is null then pp.PeriodStart_stg else d.EffectiveDate_stg  end as startdate, 

case when d.ExpirationDate_stg is null then pp.PeriodEnd_stg else d.ExpirationDate_stg end as enddate,

d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

from  DB_T_PROD_STAG.pc_policyperiod pp 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps  on pp.status_stg = pps.id_stg 

left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.branchid_stg = pp.id_stg 

join DB_T_PROD_STAG.pcx_fopblanketcov a  on pp.id_stg = a.branchid_stg 

join DB_T_PROD_STAG.pc_formpattern c  on c.clausepatterncode_stg = a.patterncode_stg 

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg 

inner join DB_T_PROD_STAG.pctl_documenttype pd on pd.id_stg = c.DocumentType_stg 

where pd.typecode_stg = ''endorsement_alfa'' and c.Retired_stg = 0 

and d.RemovedOrSuperseded_stg is null and pps.typecode_stg = ''Bound'' 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)

and(a.EffectiveDate_stg is null or (a.EffectiveDate_stg > pp.ModelDate_stg and a.EffectiveDate_stg <> a.ExpirationDate_stg))) /*EIM-48781-FARM CHANGES ENDS*/

, Umbrella_temp as (/* DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov */
select  CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalumbrellalinecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinecov.updatetime_stg

    from    

    DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalumbrellalinecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null

        

union 

        select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalumbrellalinecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalumbrellalinecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

        

        union 

        select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalumbrellalinecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalumbrellalinecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

        

        union 

        select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalumbrellalinecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg= pcx_puppersonalumbrellalinecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg is null

    AND DirectTerm1Avl_stg is null 

	and StringTerm1Avl_stg is null

/* DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl */
    union

    select  CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    

    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg> ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null

        union

        

        select  CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    

    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null

        union

        select  CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    

    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null

        

        union

        select  CAST(''ChoiceTerm4'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    

    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg= 1 

        and ExpirationDate_stg is null

        

        union       

        select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

        union       

        select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

        

        union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_puppersonalschexclitemexcl.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as booleanterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg=1 

        and ExpirationDate_stg is null 

    union

    select  ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

        where DateTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

 UNION

 

 select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

        where StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

 UNION

 

 select ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalschexclitemexcl.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_puppersonalschexclitemexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalschexclitemexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg is null 

        and ChoiceTerm2Avl_stg is null 

        and ChoiceTerm3Avl_stg is null 

        and ChoiceTerm4Avl_stg is null 

        and DirectTerm1Avl_stg is null 

        and DirectTerm2_stg is null

        and BooleanTerm1Avl_stg is null 

        and DateTerm1_stg is null

		and StringTerm1Avl_stg is null

        and ExpirationDate_stg is null 

        union

/* - DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl */
select  CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_puppersonalumbrellalinexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalumbrellalinexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm)

		where ChoiceTerm1Avl_stg=1 and ExpirationDate_stg is null 

		union

		select ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

             pcx_puppersonalumbrellalinexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinexcl.updatetime_stg

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_puppersonalumbrellalinexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm)

		where ChoiceTerm1Avl_stg is null and ExpirationDate_stg is null

            )



, Dwelling_Temp as ( 

    select  CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm3'' as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm4'' as columnname, cast(DirectTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_dwellingcov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_dwellingcov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg is null 

        and ChoiceTerm2Avl_stg is null 

        and ChoiceTerm3Avl_stg is null 

        and ChoiceTerm4Avl_stg is null 

        and ChoiceTerm5Avl_stg is null 

        and DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and DirectTerm3Avl_stg is null 

        and DirectTerm4Avl_stg is null 

        and BooleanTerm2Avl_stg is null 

        and BooleanTerm1Avl_stg is null 

        and ExpirationDate_stg is null ), LINECOV_TEMP AS ( 

    select  cast(''ChoiceTerm1'' as varchar(50)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg =1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm2'' as columnname, StringTerm2_stg as val, patterncode_stg,

            cast(BranchID_stg as varchar(255)) as BranchID, pcx_homeownerslinecov_hoe.createtime_stg,

            EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg is null 

        and ChoiceTerm2Avl_stg is null 

        and ChoiceTerm3Avl_stg is null 

        and ChoiceTerm4Avl_stg is null 

        and ChoiceTerm5Avl_stg is null 

        and DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and BooleanTerm1Avl_stg is null 

        and BooleanTerm2Avl_stg is null 

        and StringTerm1Avl_stg is null 

        and StringTerm2Avl_stg is null 

        and ExpirationDate_stg is null ), COVITEM_TEMP AS ( 

    select  cast(''ChoiceTerm1'' as varchar(50))as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            cast(patterncode_stg as varchar(250))patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            cast(pe.PatternID_stg as varchar(255)) as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm3'' as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and BooleanTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm4'' as columnname, cast(BooleanTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and BooleanTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm5'' as columnname, cast(BooleanTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and BooleanTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm2'' as columnname, cast(StringTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and StringTerm2Avl_stg= 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm3'' as columnname, cast(StringTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_STG 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and StringTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm4'' as columnname, cast(StringTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_STG 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and StringTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_STG 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and DateTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm4'' as columnname, cast(DateTerm4_stg as varchar(255)) val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and DateTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

            pe.PatternID_stg as patternid, pha.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pha.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    left join DB_T_PROD_STAG.pc_etlclausepattern pe 

        on pe.PatternID_stg = pha.PatternCode_stg 

    where   pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

            ''HOSI_ScheduledPropertyItem_alfa'', ''HOSI_SpecificOtherStructureExclItem_alfa'') 

        and ChoiceTerm1Avl_stg is null 

        and ChoiceTerm2Avl_stg is null 

        and ChoiceTerm3Avl_stg is null 

        and ChoiceTerm4Avl_stg is null 

        and ChoiceTerm5Avl_stg is null 

        and ChoiceTerm6Avl_stg is null 

        and DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and BooleanTerm1Avl_stg is null 

        and BooleanTerm2Avl_stg is null 

        and BooleanTerm3Avl_stg is null 

        and BooleanTerm4Avl_stg is null 

        and BooleanTerm5Avl_stg is null 

        and StringTerm1Avl_stg is null 

        and StringTerm2Avl_stg is null 

        and StringTerm3Avl_stg is null 

        and StringTerm4Avl_stg is null 

        and DateTerm1Avl_stg is null 

        and DateTerm4Avl_stg is null 

        and ExpirationDate_stg is null 

    union 

    select  

            case 

                when t.seq1 = 1 then ''BooleanTerm1'' 

                when t.seq1 = 2 then ''BooleanTerm2'' 

                when t.seq1 = 3 then ''ChoiceTerm1'' 

                when t.seq1 = 4 then ''DateTerm1'' 

                when t.seq1 = 5 then ''DateTerm2'' 

                when t.seq1 = 6 then ''DirectTerm1'' 

                when t.seq1 = 7 then ''DirectTerm2'' 

                when t.seq1 = 8 then ''StringTerm1'' 

                when t.seq1 = 9 then ''StringTerm2'' 

                when t.seq1 = 10 then ''StringTerm2'' 

            end as columnname, 

            case 

                when t.seq1 = 1 then cast(h.BooleanTerm1_stg as varchar(255)) 

                when t.seq1 = 2 then cast(h.BooleanTerm2_stg as varchar(255)) 

                when t.seq1 = 3 then cast(h.ChoiceTerm1_stg as varchar(255)) 

                when t.seq1 = 4 then cast(h.DateTerm1_stg as varchar(255)) 

                when t.seq1 = 5 then cast(h.DateTerm2_stg as varchar(255)) 

                when t.seq1 = 6 then cast(h.DirectTerm1_stg as varchar(255)) 

                when t.seq1 = 7 then cast(h.DirectTerm2_stg as varchar(255)) 

                when t.seq1 = 8 then cast(h.StringTerm1_stg as varchar(255)) 

                when t.seq1 = 9 then cast(h.StringTerm2_stg as varchar(255)) 

                when t.seq1 = 10 then cast(h.StringTerm2_stg as varchar(255)) 

            end as val, patterncode_stg, cast(branchid_stg as varchar(255)) as branchid,

            h.createtime_stg, effectiveDate_stg, expirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, h.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineexcl_hoe h 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = h.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    inner join ( 

        select  1 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  2 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  3 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  4 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  5 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  6 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  7 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  8 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  9 as seq1 

        from    DB_T_PROD_STAG.pcx_holineexcl_hoe 

        union 

        select  10 as seq1 

        from    db_t_prod_stag.pcx_holineexcl_hoe) as t 

        on 1 = 1 

    where   h.expirationdate_stg is null 

        and ( 

            case 

                when t.seq1 = 1 then cast(h.BooleanTerm1_stg as varchar(255)) 

                when t.seq1 = 2 then cast(h.BooleanTerm2_stg as varchar(255)) 

                when t.seq1 = 3 then cast(h.ChoiceTerm1_stg as varchar(255)) 

                when t.seq1 = 4 then cast(h.DateTerm1_stg as varchar(255)) 

                when t.seq1 = 5 then cast(h.DateTerm2_stg as varchar(255)) 

                when t.seq1 = 6 then cast(h.DirectTerm1_stg as varchar(255)) 

                when t.seq1 = 7 then cast(h.DirectTerm2_stg as varchar(255)) 

                when t.seq1 = 8 then cast(h.StringTerm1_stg as varchar(255)) 

                when t.seq1 = 9 then cast(h.StringTerm2_stg as varchar(255)) 

                when t.seq1 = 10 then cast(h.StringTerm2_stg as varchar(255)) end) is not null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) val,

            patterncode_stg, cast(branchid_stg as varchar(255)) as branchid,

            h.createtime_stg, effectiveDate_stg, expirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid, h.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_holineexcl_hoe h 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = h.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   coalesce(BooleanTerm1Avl_stg,BooleanTerm2Avl_stg,ChoiceTerm1Avl_stg,

            DateTerm1Avl_stg,DateTerm2Avl_stg,DirectTerm1Avl_stg,DirectTerm2Avl_stg,

            StringTerm1Avl_stg,StringTerm2Avl_stg) is null 

        and expirationdate_stg is null 

    group by patterncode_stg, branchid_stg, h.createtime_stg, effectiveDate_stg,

            expirationDate_stg, h.updatetime_stg ) , Personal_Vehicle as ( 

    select  cast(''ChoiceTerm1'' as varchar(250))as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pc_personalvehiclecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalvehiclecov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalvehiclecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalvehiclecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg is null 

        and ChoiceTerm2Avl_stg is null 

        and DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and BooleanTerm1Avl_stg is null 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm1'' as columnname,cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname,cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm2, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname,cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm3, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname,cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm4, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname,cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm5, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm6'' as columnname,cast(ChoiceTerm6_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm6, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm6Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm7'' as columnname,cast(ChoiceTerm7_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm7, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm7Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm8'' as columnname,cast(ChoiceTerm8_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm8, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm8Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as booleanterm1, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as booleanterm2, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm3'' as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as booleanterm3, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm3Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm4'' as columnname, cast(BooleanTerm4_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

            pc_personalautocov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

            cast(null as varchar(255)) as booleanterm4, cast(null as varchar(255)) as patternid,

            pc_personalautocov.updatetime_stg 

    from    DB_T_PROD_STAG.pc_personalautocov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pc_personalautocov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm4Avl_stg=1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawatercraftmotorcov_alfa.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawatercraftmotorcov_alfa.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawatercraftmotorcov_alfa.createtime_stg, EffectiveDate_stg,

            ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

            cast(null as varchar(255)) as patternid,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255))as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawctrailercov_alfa.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_pawctrailercov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawctrailercov_alfa.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_pawctrailercov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            pcx_pawctrailercov_alfa.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_pawctrailercov_alfa.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg is null 

        and DirectTerm2Avl_stg is null 

        and ExpirationDate_stg is null ) , MASTER_Temp AS ( 

    select  distinct pp.PolicyNumber_stg, 

            case 

                when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else polcov.EffectiveDate_stg 

            end startdate, 

            case 

                when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else polcov.ExpirationDate_stg 

            end enddate, 

            case 

                when covterm.CovTermType = ''Package'' then package.packagePatternID 

                when covterm.CovTermType = ''Option'' 

        and polcov.val is not null then optn.optionPatternID 

                when covterm.CovTermType = ''Clause'' then covterm.clausePatternID 

                else covterm.covtermPatternID 

            end nk_public_id, 

            case 

                when covterm.CovTermType = ''Package'' then cast (''PACKAGE'' as varchar (50)) 

                when covterm.CovTermType = ''Option'' 

        and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) 

                when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) 

                else cast (''COVTERM'' as varchar (50)) 

            end FEAT_SBTYPE_CD, 

            case 

                when covterm.CovTermType = ''Option'' 

        and optn.ValueType = ''money'' then optn.Value1 

                when covterm.CovTermType <> ''Option'' then polcov.val 

            end feat_amt, 

            case 

                when optn.ValueType = ''count'' then optn.Value1 

            end feat_qty, 

            case 

                when optn.ValueType in (''days'', ''hours'', ''other'') then optn.value1 

            end feat_num, pp.PublicID_stg, pp.Createtime_stg, 

            case 

                when optn.ValueType=''percent'' then optn.Value1 

            end feat_rate, polcov.updatetime_stg, pp.Retired_stg AS Retired,

            cast (null as varchar(50)) feat_effect_type_cd, polcov.val feat_val,

            covterm.CovTermType feat_CovTermType, ( :start_dttm) start_dttm,

            ( :end_dttm) end_dttm, cast(NULL as varchar(10)) as Eligible 

    from    ( 

    select * from 

                Umbrella_temp

         union all

        select  * 

        from    Dwelling_Temp 

        union all 

        select  * 

        from    Personal_Vehicle 

        union 

        select  * 

        from    COVITEM_TEMP 

        union all 

        select  * 

        from    LINECOV_TEMP ) polcov 

    inner join ( 

        select  cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

                PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

                Createtime_stg, updatetime_stg, Retired_stg 

        from    db_t_prod_stag.pc_policyperiod) pp 

        on pp.id = polcov.BranchID 

    left join ( 

        select  pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

                pcv.ColumnName_stg as columnname, pcv.CovTermType_stg as covtermtype,

                pcl.name_stg clausename 

        from    DB_T_PROD_STAG.pc_etlclausepattern pcl join DB_T_PROD_STAG.pc_etlcovtermpattern pcv 

            on pcl.id_stg = pcv.ClausePatternID_stg 

        union 

        select  pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

                coalesce(pcv.ColumnName_stg,''Clause'') columnname, coalesce(pcv.CovTermType_stg,

                ''Clause'') covtermtype, pcl.name_stg clausename 

        from    DB_T_PROD_STAG.pc_etlclausepattern pcl 

        left join ( 

            select  * 

            from    DB_T_PROD_STAG.pc_etlcovtermpattern 

            where   Name_stg not like ''ZZ%'' ) pcv 

            on pcv.ClausePatternID_stg = pcl.ID_stg 

        where   pcl.Name_stg not like ''ZZ%'' 

            and pcv.Name_stg is null 

            and OwningEntityType_stg in (''HOLineSchCovItem_alfa'',''HomeownersLine_HOE'',

                ''Dwelling_HOE'', ''PersonalVehicle'', ''PersonalAutoLine'',''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') ) covterm 

        on covterm.clausePatternID = polcov.PatternCode_stg 

        and covterm.ColumnName = polcov.columnname 

    left outer join ( 

        select  pcv.PatternID_stg packagePatternID, pcv.PackageCode_stg cov_id,

                pcv.PackageCode_stg name1 

        from    DB_T_PROD_STAG.pc_etlcovtermpackage pcv) package 

        on package.packagePatternID = polcov.val 

    left outer join ( 

        select  pct.PatternID_stg optionPatternID, pct.optioncode_stg name1,

                cast(pct.value_stg as varchar(255)) as value1, pcv.ValueType_stg as ValueType 

        from    DB_T_PROD_STAG.pc_etlcovtermpattern pcv 

        inner join DB_T_PROD_STAG.pc_etlcovtermoption pct 

            on pcv.id_stg = pct.CoverageTermPatternID_stg ) optn 

        on optn.optionPatternID = polcov.val 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pps.id_stg = pp.Status_stg 

    inner join DB_T_PROD_STAG.pc_job pj 

        on pj.id_stg = pp.JobID_stg 

    inner join DB_T_PROD_STAG.pctl_job pcj 

        on pcj.id_stg = pj.Subtype_stg 

    where   covterm.clausename not like''%ZZ%'' 

        and pps.TYPECODE_stg = ''Bound'' 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) ), Modifiers_Temp AS ( 

    select  pp.PolicyNumber_stg, 

            case 

                when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else polcov.EffectiveDate_stg 

            end as startdate, 

            case 

                when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else polcov.ExpirationDate_stg 

            end as enddate, 

            case 

                when polcov.typ = ''MODIFIER'' then pe.PatternID_stg 

                else pc.PatternID_stg 

            end as nk_public_id, 

            case 

                when polcov.typ = ''EXCLUSION'' then ''CLAUSE'' 

                else polcov.typ 

            end as FEAT_SBTYPE_CD, cast(ratemodifier as varchar(255)) as feat_amt,

            cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,

            pp.PublicID_stg, pp.Createtime_stg, polcov.feat_rate, pp.updatetime_stg,

            pp.Retired_stg, pda.typecode_stg as feat_effect_type_cd, cast(null as varchar(255)) as feat_val,

            cast(null as varchar(255)) as feat_CovTermType, ( :start_dttm) as start_dttm,

            ( :end_dttm) as end_dttm, cast(polcov.Eligible as varchar(10)) as Eligible 

    from    ( /**************Modifiers*****************************/ 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            cast(''EXCLUSION'' as varchar(250)) as typ, EffectiveDate_stg,

            ExpirationDate_stg, cast(NULL as varchar(255)) as ratemodifier,

            cast(NULL as varchar(255)) AS DiscountSurcharge_alfa, cast(NULL as varchar(255)) as feat_rate,

            cast(NULL as varchar(250)) as Eligible 

    from    DB_T_PROD_STAG.pcx_pavehicleexclusion_alfa 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_pavehicleexclusion_alfa.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            ''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

            cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

            cast( 

            case 

                when phh.Eligible_stg= 1 THEN phh.RateModifier_stg 

                ELSE 0 

            end as varchar(255)) as feat_rate, cast(Eligible_stg as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pcx_homodifier_hoe phh 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = phh.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            ''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

            cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

            cast( 

            case 

                when pdh.Eligible_stg = 1 THEN pdh.RateModifier_stg 

                ELSE 0 

            end as varchar(255))as feat_rate, cast(Eligible_stg as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pcx_dwellingmodifier_hoe pdh 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pdh.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            ''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

            cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

            cast( 

            case 

                when pvp.Eligible_stg = 1 THEN pvp.RateModifier_stg 

                ELSE 0 

            end as varchar(255)) as feat_rate, cast(Eligible_stg as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_pavehmodifier pvp 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pvp.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            ''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

            cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

            cast( 

            case 

                when pmp.Eligible_stg = 1 THEN pmp.RateModifier_stg 

                ELSE 0 

            end as varchar(255)) as feat_rate, cast(Eligible_stg as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_pamodifier pmp 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pmp.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union 

    select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

            ''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

            cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

            cast( 

            case 

                when pb.Eligible_stg = 1 THEN pb.RateModifier_stg 

                ELSE 0 

            end as varchar(255)) as feat_rate, cast(Eligible_stg as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pcx_bp7linemod pb 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pb.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) /**************************************************/ /**********************Exclusions are at clauselevel******************/ /**************************************************/ ) polcov 

    left join DB_T_PROD_STAG.pc_etlmodifierpattern pe 

        on pe.patternid_stg = polcov.PatternCode_stg 

        and polcov.typ = ''MODIFIER'' 

    left join DB_T_PROD_STAG.pc_etlclausepattern pc 

        on pc.patternid_stg = polcov.PatternCode_stg 

        and polcov.typ = ''EXCLUSION'' 

    inner join ( 

        select  cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

                PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

                createtime_stg, updatetime_stg,Retired_stg 

        from    DB_T_PROD_STAG.pc_policyperiod ) pp 

        on pp.id = polcov.BranchID 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pps.id_stg = pp.Status_stg 

    inner join DB_T_PROD_STAG.pc_job pj 

        on pj.id_stg = pp.JobID_stg 

    inner join DB_T_PROD_STAG.pctl_job pcj 

        on pcj.id_stg=pj.Subtype_stg 

    LEFT JOIN DB_T_PROD_STAG.pctl_discountsurcharge_alfa pda 

        ON polcov.DiscountSurcharge_alfa = pda.ID_stg 

    where   (pc.Name_stg not like''%ZZ%'' 

        or pe.Name_stg not like''%ZZ%'') 

        and pps.TYPECODE_stg = ''Bound'' 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) /*Fix for Defect: 13484*/ 

    UNION /*Endorsemnets 1*/ 

    select  distinct pp.PolicyNumber_stg, 

            case 

                when d.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else d.EffectiveDate_stg 

            end as startdate, 

            case 

                when d.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else d.ExpirationDate_stg 

            end as enddate, d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

            cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

            cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

            cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

            cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

            cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

            ( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_policyperiod pp 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pp.status_stg = pps.id_stg 

    left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff 

        on eff.branchid_stg = pp.id_stg join DB_T_PROD_STAG.pcx_bp7linecov a 

        on pp.id_stg = a.branchid_stg join DB_T_PROD_STAG.pc_formpattern c 

        on c.clausepatterncode_stg = a.patterncode_stg join DB_T_PROD_STAG.pc_form d 

        on d.formpatterncode_stg = c.code_stg 

        and d.branchid_stg = a.branchid_stg join DB_T_PROD_STAG.pc_etlclausepattern e 

        on e.patternid_stg = a.patterncode_stg 

    inner join DB_T_PROD_STAG.pctl_documenttype pd 

        on pd.id_stg = c.DocumentType_stg 

    where   pd.typecode_stg = ''endorsement_alfa'' 

        and c.Retired_stg = 0 /*EIM-33716*/ 

        and d.RemovedOrSuperseded_stg is null /*EIM-33716*//*and pfa.id_stg is not null */ /*is it required*/ 

        and pps.typecode_stg = ''Bound'' /*and eff.ExpirationDate_stg is null */ 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) /*Fix for Defect: 13484 Ends*/ 

        and(a.EffectiveDate_stg is null 

        or (a.EffectiveDate_stg > pp.ModelDate_stg 

        and a.EffectiveDate_stg <> a.ExpirationDate_stg)) 

    UNION /*Endorsemnets 2*/ 

    select  distinct pp.PolicyNumber_stg, 

            case 

                when d.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else d.EffectiveDate_stg 

            end as startdate, 

            case 

                when d.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else d.ExpirationDate_stg 

            end as enddate, d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

            cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

            cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

            cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

            cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

            cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

            ( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_policyperiod pp 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pp.status_stg = pps.id_stg 

    left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff 

        on eff.branchid_stg = pp.id_stg join DB_T_PROD_STAG.pcx_homeownerslinecov_hoe a 

        on pp.id_stg = a.branchid_stg join DB_T_PROD_STAG.pc_formpattern c 

        on c.clausepatterncode_stg = a.patterncode_stg join DB_T_PROD_STAG.pc_form d 

        on d.formpatterncode_stg = c.code_stg 

        and d.branchid_stg = a.branchid_stg join DB_T_PROD_STAG.pc_etlclausepattern e 

        on e.patternid_stg = a.patterncode_stg 

    inner join DB_T_PROD_STAG.pctl_documenttype pd 

        on pd.id_stg = c.DocumentType_stg 

    where   pd.typecode_stg = ''endorsement_alfa'' 

        and c.Retired_stg = 0 /*EIM-33716*/ 

        and d.RemovedOrSuperseded_stg is null /*EIM-33716*//*and pfa.id_stg is not null */ /*is it required*/ 

        and pps.typecode_stg = ''Bound'' /*and eff.ExpirationDate_stg is null */ 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) /*Fix for Defect: 13484 Ends*/ 

        and(a.EffectiveDate_stg is null 

        or (a.EffectiveDate_stg > pp.ModelDate_stg 

        and a.EffectiveDate_stg <> a.ExpirationDate_stg)) 

    UNION /*Endorsemnets 3*/ 

    select  distinct pp.PolicyNumber_stg, 

            case 

                when d.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else d.EffectiveDate_stg 

            end as startdate, 

            case 

                when d.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else d.ExpirationDate_stg 

            end as enddate, d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

            cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

            cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

            cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

            cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

            cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

            ( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_policyperiod pp 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pp.status_stg = pps.id_stg 

    left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff 

        on eff.branchid_stg = pp.id_stg join DB_T_PROD_STAG.pc_personalautocov a 

        on pp.id_stg = a.branchid_stg join DB_T_PROD_STAG.pc_formpattern c 

        on c.clausepatterncode_stg = a.patterncode_stg join DB_T_PROD_STAG.pc_form d 

        on d.formpatterncode_stg = c.code_stg 

        and d.branchid_stg = a.branchid_stg join DB_T_PROD_STAG.pc_etlclausepattern e 

        on e.patternid_stg = a.patterncode_stg 

    inner join DB_T_PROD_STAG.pctl_documenttype pd 

        on pd.id_stg = c.DocumentType_stg 

    where   pd.typecode_stg = ''endorsement_alfa'' 

        and c.Retired_stg = 0 /*EIM-33716*/ 

        and d.RemovedOrSuperseded_stg is null /*EIM-33716*//*and pfa.id_stg is not null */ /*is it required*/ 

        and pps.typecode_stg = ''Bound'' /*and eff.ExpirationDate_stg is null*/ 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) /*Fix for Defect: 13484 Ends*/ 

        and(a.EffectiveDate_stg is null 

        or (a.EffectiveDate_stg > pp.ModelDate_stg 

        and a.EffectiveDate_stg <> a.ExpirationDate_stg))

        UNION /*Endorsemnets 4*/ 

        select  distinct pp.PolicyNumber_stg, 

            case 

                when d.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else d.EffectiveDate_stg 

            end as startdate, 

            case 

                when d.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else d.ExpirationDate_stg 

            end as enddate, d.FormPatternCode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

            cast(0 as varchar(50)) as feat_amt, cast(0 as varchar(50))as feat_qty,

            cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.Createtime_stg,

            cast(null as varchar(50)) as feat_rate, pp.updatetime_stg, pp.Retired_stg,

            cast(null as varchar(50)) as feat_effect_type_cd, cast(null as varchar(50)) as feat_val,

            cast(null as varchar(50)) as feat_CovTermType, ( :start_dttm) as start_dttm,

            ( :end_dttm) as end_dttm, cast(null as varchar(10)) as Eligible 

    from    DB_T_PROD_STAG.pc_policyperiod pp 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pp.status_stg = pps.id_stg 

    left outer join DB_T_PROD_STAG.pc_effectivedatedfields eff 

        on eff.branchid_stg = pp.id_stg join DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov a 

        on pp.id_stg = a.branchid_stg join DB_T_PROD_STAG.pc_formpattern c 

        on c.clausepatterncode_stg = a.patterncode_stg join DB_T_PROD_STAG.pc_form d 

        on d.formpatterncode_stg = c.code_stg 

        and d.branchid_stg = a.branchid_stg join DB_T_PROD_STAG.pc_etlclausepattern e 

        on e.patternid_stg = a.patterncode_stg 

    inner join DB_T_PROD_STAG.pctl_documenttype pd 

        on pd.id_stg = c.DocumentType_stg 

    where   pd.typecode_stg = ''endorsement_alfa'' 

        and c.Retired_stg = 0 

        and d.RemovedOrSuperseded_stg is null 

        and pps.typecode_stg = ''Bound'' 

      and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

        and(a.EffectiveDate_stg is null 

        or (a.EffectiveDate_stg > pp.ModelDate_stg 

        and a.EffectiveDate_stg <> a.ExpirationDate_stg)) 



        ), Personal_Temp AS ( 

    select  distinct pp.PolicyNumber_stg ,pp.PeriodStart_stg as startdate ,

            pp.PeriodEnd_stg as enddate ,covterm.PatternID ,''CL'' as FEAT_SBTYPE_CD ,

            cast( 

            case 

                when covterm.CovTermType=''Package'' then cast(package.Value1 as varchar(255)) 

                when covterm.CovTermType=''Option'' 

        and optn.ValueType=''money'' then cast(optn.Value1 as varchar(255)) 

                when covterm.CovTermType<>''Option'' then polcov.val 

            end as varchar(255))as AGMT_FEAT_TO_AMT , cast( 

            case 

                when optn.ValueType=''count'' then optn.value1 

            end as varchar(255)) as AGMT_FEAT_QTY , cast( 

            case 

                when optn.ValueType in (''days'',''hours'',''other'') then optn.value1 

            end as varchar(255)) as AGMT_FEAT_NUM ,pp.PublicID_stg ,pp.Createtime_stg ,

            cast( 

            case 

                when optn.ValueType=''percent'' then optn.value1 

            end as varchar(255)) as AGMT_FEAT_RATE ,pp.UpdateTime_stg ,pp.Retired_stg AS Retired ,

            cast (null as varchar (50)) feat_effect_type_cd ,polcov.columnname ,

            covterm.CovTermType feat_CovTermType, ( :start_dttm) start_dttm,

            ( :end_dttm) end_dttm, cast (null as varchar (50)) as Eligible 

    from    (/*****************************pc_personalautocov*********************************************************/ 

    select  ''ChoiceTerm1'' as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID 

    from    DB_T_PROD_STAG.pc_personalautocov /*******************************************************************************************************/ ) polcov 

    inner join ( 

        select  pcl.CoverageSubtype_stg,pct.covtermtype_stg as CovTermType,

                pcl.Name_stg as clausename,pct.name_stg as covname,pct.ColumnName_stg,

                pcl.clausetype_stg, pcl.PatternID_stg as PatternID,pcl.OwningEntityType_stg 

        from    DB_T_PROD_STAG.pc_etlclausepattern pcl 

        left join DB_T_PROD_STAG.pc_etlcovtermpattern pct 

            on pcl.id_stg=pct.ClausePatternID_stg ) covterm 

        on covterm.PatternID=polcov.PatternCode_stg 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on cast(pp.id_stg as varchar(255))=polcov.BranchID 

    left outer join ( 

        select  pcp.PublicID_stg,pcp.PackageCode_stg as cov_id,pcp.PackageCode_stg||''-''||pct.Name_stg as name1,

                pct.Value_stg as value1 

        from    DB_T_PROD_STAG.pc_etlpackterm pct 

        inner join DB_T_PROD_STAG.pc_etlcovtermpackage pcp 

            on pcp.Id_stg=pct.CovTermPackID_stg ) package 

        on package.PublicID_stg=polcov.val 

    left outer join ( 

        select  pco.PublicID_stg, pco.OptionCode_stg as name1, pco.Value_stg as value1,

                pcp.ValueType_stg as ValueType 

        from    DB_T_PROD_STAG.pc_etlcovtermpattern pcp 

        inner join DB_T_PROD_STAG.pc_etlcovtermoption pco 

            on pcp.id_stg = pco.CoverageTermPatternID_stg) optn 

        on optn.PublicID_stg = polcov.val 

    where   pp.PolicyNumber_stg is not null 

        and covterm.clausename not like''%ZZ%'' 

        and covterm.PatternID <> ''PAADD_alfa'' 

        and covterm.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',

            ''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',

            ''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') /* and pp.MostRecentModel_stg=1 *//*EIM-35326*/ 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) ), BP7Line_Temp AS ( 

    select  distinct pp.PolicyNumber_stg, 

            case 

                when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg 

                else polcov.EffectiveDate_stg 

            end startdate, 

            case 

                when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg 

                else polcov.ExpirationDate_stg 

            end enddate, 

            case 

                when covterm.CovTermType = ''Package'' then package.packagePatternID 

                when covterm.CovTermType = ''Option'' 

        and polcov.val is not null then optn.optionPatternID 

                when covterm.CovTermType = ''Clause'' then covterm.clausePatternID 

                else covterm.covtermPatternID 

            end nk_public_id, 

            case 

                when covterm.CovTermType = ''Package'' then cast (''PACKAGE'' as varchar (50)) 

                when covterm.CovTermType = ''Option'' 

        and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) 

                when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) 

                else cast (''COVTERM'' as varchar (50)) 

            end FEAT_SBTYPE_CD, 

            case 

                when covterm.CovTermType = ''Option'' 

        and optn.ValueType = ''money'' then optn.Value1 

                when covterm.CovTermType <> ''Option'' then polcov.val 

            end feat_amt, 

            case 

                when optn.ValueType = ''count'' then optn.Value1 

            end feat_qty, 

            case 

                when optn.ValueType in (''days'', ''hours'', ''other'') then optn.value1 

            end feat_num, pp.PublicID_stg, pp.Createtime_stg, 

            case 

                when optn.ValueType=''percent'' then optn.Value1 

            end feat_rate, polcov.updatetime_stg, pp.Retired_stg AS Retired,

            cast (null as varchar (50)) feat_effect_type_cd, polcov.val feat_val,

            covterm.CovTermType feat_CovTermType, ( :start_dttm) start_dttm,

            ( :end_dttm) end_dttm, cast(NULL as varchar(10)) as Eligible 

    from    ( /*pcx_bp7linecov*/ 

    select  cast(''ChoiceTerm1'' as varchar(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, cast(''bp7line'' as varchar(250)) as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as choiceterm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm6'' as columnname, cast(ChoiceTerm6_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm6, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm6Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm7'' as columnname, cast(ChoiceTerm7_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm7, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm7Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm3'' as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm4'' as columnname, cast(DirectTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm5'' as columnname, cast(DirectTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm6'' as columnname, cast(DirectTerm6_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm6, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm6Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm7'' as columnname, cast(DirectTerm7_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm7, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm7Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm8'' as columnname, cast(DirectTerm8_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm8, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm8Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm9'' as columnname, cast(DirectTerm9_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm9, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm9Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm10'' as columnname, cast(DirectTerm10_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm10, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm10Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm3'' as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm4'' as columnname, cast(BooleanTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm5'' as columnname, cast(BooleanTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm6'' as columnname, cast(BooleanTerm6_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm6, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm6Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm7'' as columnname, cast(BooleanTerm7_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm7, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm7Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm8'' as columnname, cast(BooleanTerm8_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm8, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm8Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm9'' as columnname, cast(BooleanTerm9_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm9, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm9Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm10'' as columnname, cast(BooleanTerm10_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm10, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm10Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm11'' as columnname, cast(BooleanTerm11_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm11, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm11Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm12'' as columnname, cast(BooleanTerm12_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm12, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm12Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm13'' as columnname, cast(BooleanTerm13_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm13, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm13Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm14'' as columnname, cast(BooleanTerm14_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm14, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm14Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm2'' as columnname, cast(StringTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm3'' as columnname, cast(StringTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm4'' as columnname, cast(StringTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm5'' as columnname, cast(StringTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm6'' as columnname, cast(StringTerm6_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm6, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm6Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm7'' as columnname, cast(StringTerm7_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm7, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm7Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm8'' as columnname, cast(StringTerm8_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm8, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm8Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm9'' as columnname, cast(StringTerm9_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm9, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm9Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm10'' as columnname, cast(StringTerm10_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm10, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm10Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm11'' as columnname, cast(StringTerm11_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm11, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm11Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm12'' as columnname, cast(StringTerm12_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm12, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm12Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm13'' as columnname, cast(StringTerm13_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm13, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm13Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm14'' as columnname, cast(StringTerm14_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm14, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm14Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm15'' as columnname, cast(StringTerm15_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm15, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm15Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm2'' as columnname, cast(DateTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''PositiveIntTerm1'' as columnname, cast(PositiveIntTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as PositiveIntTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   PositiveIntTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''PositiveIntTerm2'' as columnname, cast(PositiveIntTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as PositiveIntTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   PositiveIntTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''PositiveIntTerm3'' as columnname, cast(PositiveIntTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as PositiveIntTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   PositiveIntTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(NULL as varchar(255)) val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as Clause, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecov.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecov 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecov.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union /*pcx_bp7lineexcel*/ 

    select  ''ChoiceTerm1'' as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as choiceterm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm3'' as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm3'' as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm2'' as columnname, cast(StringTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm3'' as columnname, cast(StringTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm4'' as columnname, cast(StringTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm2'' as columnname, cast(DateTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(NULL as varchar(255)) val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7lineexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as Clause, cast(NULL as varchar(255)) as patternid,

            pcx_bp7lineexcl. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7lineexcl 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7lineexcl.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    union /*pcx_bp7linecond*/ 

    select  ''ChoiceTerm1'' as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as choiceterm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as ChoiceTerm5, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   ChoiceTerm5Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DirectTerm3'' as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DirectTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DirectTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''BooleanTerm3'' as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as BooleanTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   BooleanTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm2'' as columnname, cast(StringTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm3'' as columnname, cast(StringTerm3_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond. createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm3, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm3Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''StringTerm4'' as columnname, cast(StringTerm4_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as StringTerm4, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   StringTerm4Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm1, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm1Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''DateTerm2'' as columnname, cast(DateTerm2_stg as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as DateTerm2, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond.updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) 

    where   DateTerm2Avl_stg = 1 

        and ExpirationDate_stg is null 

    union 

    select  ''Clause'' as columnname, cast(null as varchar(255)) val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

            cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

            pcx_bp7linecond.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(NULL as varchar(255)) as Clause, cast(NULL as varchar(255)) as patternid,

            pcx_bp7linecond. updatetime_stg 

    from    DB_T_PROD_STAG.pcx_bp7linecond 

    inner join DB_T_PROD_STAG.pc_policyperiod pp 

        on pp.id_stg = pcx_bp7linecond.BranchID_stg 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) ) polcov 

    inner join ( 

        select  cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

                PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

                createtime_stg, updatetime_stg, Retired_stg 

        from    db_t_prod_stag.pc_policyperiod) pp 

        on pp.id= polcov.BranchID 

    left join ( 

        select  pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

                pcv.ColumnName_stg as Columnname, pcv.CovTermType_stg as CovtermType,

                pcl.name_stg as clausename 

        from    DB_T_PROD_STAG.pc_etlclausepattern pcl join DB_T_PROD_STAG.pc_etlcovtermpattern pcv 

            on pcl.id_stg = pcv.ClausePatternID_stg 

        union 

        select  pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

                coalesce(pcv.ColumnName_stg,''Clause'') columnname, coalesce(pcv.CovTermType_stg,

                ''Clause'') covtermtype, pcl.name_stg clausename 

        from    DB_T_PROD_STAG.pc_etlclausepattern pcl 

        left join ( 

            select  * 

            from    DB_T_PROD_STAG.pc_etlcovtermpattern 

            where   Name_stg not like ''ZZ%'') pcv 

            on pcv.ClausePatternID_stg = pcl.ID_stg 

        where   pcl.Name_stg not like ''ZZ%'' 

            and pcv.Name_stg is null 

            and pcl.OwningEntityType_stg in (''BP7BusinessOwnersLine'') ) covterm 

        on covterm.clausePatternID = polcov.patterncode_stg 

        and covterm.ColumnName = polcov.columnname 

    left outer join ( 

        select  pcp.PatternID_stg packagePatternID, pcp.PackageCode_stg cov_id,

                pcp.PackageCode_stg name1 

        from    DB_T_PROD_STAG.pc_etlcovtermpackage pcp) package 

        on package.packagePatternID = polcov.val 

    left outer join ( 

        select  pco.PatternID_stg optionPatternID, pco.optioncode_stg name1,

                cast(pco.value_stg as varchar(255)) as value1, pcp.ValueType_stg as ValueType 

        from    DB_T_PROD_STAG.pc_etlcovtermpattern pcp 

        inner join DB_T_PROD_STAG.pc_etlcovtermoption pco 

            on pcp.id_stg = pco.CoverageTermPatternID_stg ) optn 

        on optn.optionPatternID = polcov.val 

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

        on pps.id_stg = pp.Status_stg 

    inner join DB_T_PROD_STAG.pc_job pj 

        on pj.id_stg = pp.JobID_stg 

    inner join DB_T_PROD_STAG.pctl_job pcj 

        on pcj.id_stg = pj.Subtype_stg 

    where   covterm.clausename not like''%ZZ%'' 

        and pps.TYPECODE_stg = ''Bound'' 

        and pp.updatetime_stg > ( :start_dttm) 

        and pp.updatetime_stg <= ( :end_dttm) ) 

Select  DISTINCT CR_AGMT_FEAT.AGMT_ID As TG_AGMT_ID, CR_AGMT_FEAT.FEAT_ID As TG_FEAT_ID,

        CR_AGMT_FEAT.AGMT_FEAT_ROLE_CD As TG_AGMT_FEAT_ROLE_CD, CR_AGMT_FEAT.AGMT_FEAT_STRT_DTTM As TG_AGMT_FEAT_START_DTTM,

        CR_AGMT_FEAT.AGMT_FEAT_END_DTTM As TG_AGMT_FEAT_END_DTTM, CR_AGMT_FEAT.AGMT_FEAT_AMT As TG_AGMT_FEAT_AMT,

        CR_AGMT_FEAT.AGMT_FEAT_RATE As TG_AGMT_FEAT_RATE, CR_AGMT_FEAT.AGMT_FEAT_QTY As TG_AGMT_FEAT_QTY,

        CR_AGMT_FEAT.AGMT_FEAT_NUM As TG_AGMT_FEAT_NUM, CR_AGMT_FEAT.VAL_TYPE_CD AS TG_VAL_TYPE_CD,

        CR_AGMT_FEAT.EDW_STRT_DTTM As TG_EDW_STRT_DTTM, CR_AGMT_FEAT.EDW_END_DTTM As TG_EDW_END_DTTM,

        XLAT_SRC.AGMT_ID As SRC_AGMT_ID, XLAT_SRC.FEAT_ID As SRC_FEAT_ID,

        XLAT_SRC.AGMT_FEAT_ROLE_CD As SRC_AGMT_FEAT_ROLE_CD, XLAT_SRC.SRC_STRT_DT As SRC_AGMT_FEAT_STRT_DT,

        XLAT_SRC.AGMT_FEAT_AMT As SRC_AGMT_FEAT_AMT, XLAT_SRC.SRC_FEAT_RATE As SRC_FEAT_RATE,

        XLAT_SRC.SRC_FEAT_QTY As SRC_FEAT_QTY, XLAT_SRC.SRC_FEAT_NUM As SRC_FEAT_NUM,

        XLAT_SRC.AGMT_FEAT_DT As SRC_AGMT_FEAT_DT, XLAT_SRC.SRC_FEAT_TXT As SRC_FEAT_TXT,

        XLAT_SRC.AGMT_FEAT_IND As SRC_AGMT_FEAT_IND, XLAT_SRC.SRC_Eligible As SRC_Eligible,

        XLAT_SRC.ENDDATE As SRC_END_DT, XLAT_SRC.FEET_EFFECT_TYPE_CD As SRC_FEET_EFFECT_TYPE_CD,

        XLAT_SRC.AGMT_VAL_TYP_CD As SRC_AGMT_VAL_TYP_CD, XLAT_SRC.UPDTAETIME As SRC_TRANS_START_DTTM,

        XLAT_SRC.RETIRED As SRC_RETIRED, CAST(( TRIM(COALESCE(CAST(XLAT_SRC.AGMT_FEAT_AMT AS DECIMAL(18,4)),0)) || TRIM(COALESCE(CAST(XLAT_SRC.SRC_FEAT_RATE AS DECIMAL(15,12)),0)) || TRIM(COALESCE(CAST(XLAT_SRC.SRC_FEAT_QTY AS DECIMAL(18,4)),0)) || TRIM(COALESCE(XLAT_SRC.SRC_FEAT_NUM,0)) || TRIM(COALESCE(XLAT_SRC.AGMT_VAL_TYP_CD,0)) || TRIM(COALESCE(XLAT_SRC.FEET_EFFECT_TYPE_CD,0)) || TRIM(COALESCE(XLAT_SRC.SRC_FEAT_TXT,0)) || CAST(to_char(XLAT_SRC.SRC_STRT_DT , ''YYYYMMDD'') AS VARCHAR(10)) || CAST(to_char(SRC_END_DT ,''YYYYMMDD'') AS VARCHAR(10)) || TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_IND,0)) || TRIM(COALESCE(SRC_Eligible, 0))) As Varchar(1100)) as SourceData,

        CAST((TRIM(COALESCE(CR_AGMT_FEAT.AGMT_FEAT_AMT,0)) || TRIM(COALESCE(CR_AGMT_FEAT.AGMT_FEAT_RATE,0)) || TRIM(COALESCE(CR_AGMT_FEAT.AGMT_FEAT_QTY,0)) || TRIM(COALESCE(CR_AGMT_FEAT.AGMT_FEAT_NUM,0)) || TRIM(COALESCE(CR_AGMT_FEAT.VAL_TYPE_CD,0)) || TRIM(COALESCE(CR_AGMT_FEAT.FEAT_EFECT_TYPE_CD,0)) || TRIM(COALESCE( 

        case 

            when CR_AGMT_FEAT.AGMT_FEAT_TXT='''' then ''0'' 

            else CR_AGMT_FEAT.AGMT_FEAT_TXT 

        end ,0)) || CAST(to_char(CR_AGMT_FEAT.AGMT_FEAT_STRT_DTTM,''YYYYMMDD'') AS VARCHAR(10)) || CAST(to_char(CR_AGMT_FEAT.AGMT_FEAT_END_DTTM , ''YYYYMMDD'') AS VARCHAR(10)) || TRIM(COALESCE( 

        case 

            when CR_AGMT_FEAT.AGMT_FEAT_IND='''' then ''0'' 

            else CR_AGMT_FEAT.AGMT_FEAT_IND 

        end,0)) || TRIM(COALESCE(CR_AGMT_FEAT.FEAT_ELGBL_IND, 0))) As Varchar(1100)) as TargetData,

        case 

            when CR_AGMT_FEAT.AGMT_ID IS NULL 

    AND ( XLAT_SRC.AGMT_ID) is not null 

    and (XLAT_SRC.FEAT_ID) is not null then ''I'' 

            when CR_AGMT_FEAT.AGMT_ID IS NOT NULL 

    AND CR_AGMT_FEAT.FEAT_ID IS NOT NULL 

    AND SourceData <> TargetData 

    AND ( XLAT_SRC.AGMT_ID) is not null 

    and (XLAT_SRC.FEAT_ID) is not null THEN ''U'' 

            when CR_AGMT_FEAT.AGMT_ID IS NOT NULL 

    AND CR_AGMT_FEAT.FEAT_ID IS NOT NULL 

    AND SourceData = TargetData 

    AND ( XLAT_SRC.AGMT_ID) is not null 

    and (XLAT_SRC.FEAT_ID) is not null then ''R'' 

        end as ins_upd_flag 

from    ( 

    Select  DISTINCT CR_AGMT.AGMT_ID, CR_FEAT.FEAT_ID, COALESCE(SRC.UPDTAETIME,

            CAST(''1900-01-01'' AS DATE )) AS UPDTAETIME, 

            Case 

                When (SRC.FEAT_ROLE_CD IS NULL) 

        OR (LENGTH(SRC.FEAT_ROLE_CD) = 0) 

        OR TRIM( XLAT_AGMT_FEAT_ROLE_CD.TGT_IDNTFTN_VAL) IS NULL THEN ''UNK'' 

                Else TRIM(XLAT_AGMT_FEAT_ROLE_CD.TGT_IDNTFTN_VAL) 

            END AS AGMT_FEAT_ROLE_CD, SRC.STARTDATE AS SRC_STRT_DT, 

            Case 

                When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''direct'' THEN CAST(SRC.FEAT_VAL AS DECIMAL(18,4)) 

                Else NULL 

            End AS AGMT_FEAT_AMT, 

            Case 

                When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''datetime'' 

        and TRIM(FEAT_VAL) IS NULL THEN NULL 

                When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''datetime'' 

        and TRIM(FEAT_VAL) IS NOT NULL THEN CAST(SUBSTR(COALESCE(TRIM(FEAT_VAL),

            ''1900-01-01''),1,10) As Varchar(20)) 

                Else NULL 

            End AS AGMT_FEAT_DT, to_timestamp_ntz(:end_dttm , ''YYYY-MM-DDBHH:MI:SS.FF6'' ) AS EDW_END_DT,

            XLAT_SRC_CD.TGT_IDNTFTN_VAL As SRC_CD, COALESCE(XLAT_FEAT_EFFECT_TYPE_CD.TGT_IDNTFTN_VAL,''UNK'') AS Feet_effect_type_Cd, 
            XLAT_TYPE_CD.TGT_IDNTFTN_VAL As AGMT_VAL_TYP_CD,

            Case 

                When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE)) = ''shorttext'') 

        or (LOWER(TRIM(SRC.FEAT_COVTERMTYPE)) =''typekey'') Then TRIM(SRC.FEAT_VAL) 

                Else Cast(NULL As Varchar(20)) 

            End As SRC_FEAT_TXT, 

            Case 

                When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE)) =''bit'') Then TRIM(SRC.FEAT_VAL) 

                Else Cast(NULL As Varchar(20)) 

            End As AGMT_FEAT_IND, SRC.FEAT_RATE As SRC_FEAT_RATE, SRC.FEAT_QTY As SRC_FEAT_QTY,

            SRC.FEAT_NUM As SRC_FEAT_NUM, SRC.ENDDATE, SRC.Eligible As SRC_Eligible,

            SRC.RETIRED As RETIRED 

    FROM    ( 

        select  POLICYNUMBER, STARTDATE, ENDDATE, NK_PUBLIC_ID, FEAT_SBTYPE_CD,

                FEAT_AMT, FEAT_QTY, FEAT_NUM, PUBLICID, CREATETIME, FEAT_RATE,

                SRC_CD, val_typ_cd, UPDTAETIME, Retired, FEAT_ROLE_CD, feat_effect_type_cd,

                FEAT_VAL, FEAT_COVTERMTYPE, Eligible 

        FROM    ( 

            select  distinct PolicyNumber_stg as POLICYNUMBER, STARTDATE, ENDDATE,

                    NK_PUBLIC_ID, FEAT_SBTYPE_CD, cast(( 

                    case 

                        when a.feat_amt like ''..%'' then a.feat_amt 

                        when (a.feat_amt like ''.%a%'' 

                or a.feat_amt like ''.%e%'' 

                or a.feat_amt like ''.%i%'' 

                or a.feat_amt like ''.%o%'' 

                or a.feat_val like ''.%u%'') then a.feat_amt 

                        when a.feat_amt like ''.%'' then ''0''||a.feat_amt 

                        else a.feat_amt end) as varchar(60)) as FEAT_AMT, FEAT_QTY, cast(( 

                    case 

                        when a.FEAT_NUM like ''..%'' then a.FEAT_NUM 

                        when a.FEAT_NUM like ''.%'' then ''0''||a.FEAT_NUM 

                        else a.FEAT_NUM end) as varchar(60)) as FEAT_NUM, PublicID_stg as PUBLICID,

                    Createtime_stg as CREATETIME , FEAT_RATE, ''SRC_SYS4'' AS SRC_CD,

                    CAST('''' AS VARCHAR(20)) AS val_typ_cd, updatetime_stg as UPDTAETIME,

                    Retired AS Retired, CAST(''AGMT_FEAT_ROLE_TYPE6'' AS VARCHAR(20)) AS feat_role_cd,

                    CAST(feat_effect_type_cd as VARCHAR(50)) as feat_effect_type_cd,

                    cast(( 

                    case 

                        when a.feat_val like ''..%'' then a.feat_val 

                        when (a.feat_val like ''.%a%'' 

                or a.feat_val like ''.%e%'' 

                or a.feat_val like ''.%i%'' 

                or a.feat_val like ''.%o%'' 

                or a.feat_val like ''.%u%'') then a.feat_val 

                        when a.feat_val like ''.%'' then ''0''||a.feat_val 

                        else a.feat_val end) as varchar(255)) as feat_val, FEAT_COVTERMTYPE,

                    case 

                        when cast(Eligible as varchar(10))=''0'' then ''F'' 

                        WHEN cast(Eligible as varchar(10))=''1'' THEN ''T'' 

                        ELSE NULL 

                    END AS Eligible 

            from    ( 

                select  * 

                from    MASTER_Temp 

                UNION ALL 

                select  * 

                from    Modifiers_Temp 

                UNION ALL 

                select  * 

                from    Personal_Temp 

                UNION ALL 

                select  * 

                from    BP7Line_Temp

                 UNION ALL 

                 /*EIM-48781-FARM CHANGES */

                select  * 

                from    farm_temp

                ) as a 

            union 

            SELECT  CAST (NULL AS VARCHAR (60)) AS POLICYNUMBER, cc.effectivedate_stg AS STARTDATE,

                    to_date (''01/01/1900'' , ''MM/DD/YYYY'' ) AS ENDDATE,

                    CAST (cov.typecode_stg AS VARCHAR (60)) AS NK_PUBLIC_ID, CAST (''CL'' AS VARCHAR (60)) AS FEAT_SBTYPE_CD,

                    CAST (NULL AS VARCHAR (60)) AS FEAT_AMT, CAST (''0'' AS VARCHAR (60)) AS FEAT_QTY,

                    CAST (''0'' AS VARCHAR (60)) AS FEAT_NUM, CAST (cp.id_stg AS VARCHAR (60)) AS PUBLICID,

                    to_date (NULL ,''MM/DD/YYYY'' ) AS CREATETIME, CAST (''0'' AS VARCHAR (60)) AS FEAT_RATE,

                    ''SRC_SYS6'' AS SRC_CD, cb.Name_stg AS val_typ_cd, cp.updatetime_stg,

                    cp.Retired_stg AS Retired, CAST(''AGMT_FEAT_ROLE_TYPE6'' AS VARCHAR(20)) AS feat_role_cd,

                    CAST (NULL AS VARCHAR(50) ) AS feat_effect_type_cd, CAST (NULL AS VARCHAR(255) ) AS FEAT_VAL,

                    CAST (NULL AS VARCHAR(255) ) AS FEAT_COVTERMTYPE, CAST (NULL AS VARCHAR(255) ) AS Eligible 

            FROM    DB_T_PROD_STAG.cc_policy cp 

            INNER JOIN ( 

                SELECT  distinct PolicyID_stg,CoverageBasis_stg,Type_stg,effectivedate_stg 

                FROM    DB_T_PROD_STAG.cc_coverage 

                where   UpdateTime_stg > ( :start_dttm) 

                    AND UpdateTime_stg <= ( :end_dttm)) cc 

                ON cp.id_stg=cc.PolicyID_stg 

            INNER JOIN DB_T_PROD_STAG.cctl_coveragetype cov 

                ON cov.id_stg=cc.Type_stg 

            INNER JOIN DB_T_PROD_STAG.cctl_coveragebasis cb 

                ON cc.coveragebasis_stg=cb.ID_stg 

            WHERE   cp.verified_stg= 0 

                and cp.UpdateTime_stg > ( :start_dttm) 

                and cp.UpdateTime_stg <= ( :end_dttm)) a 

        QUALIFY ROW_NUMBER() OVER( 

        PARTITION BY PUBLICID,NK_PUBLIC_ID,FEAT_SBTYPE_CD 

        ORDER BY ENDDATE desc,startdate desc, UPDTAETIME desc) = 1 ) as Src 

    LEFT OUTER JOIN ( 

        SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

        FROM    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

        WHERE   TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''AGMT_FEAT_ROLE_TYPE'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'' ) 

            AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )XLAT_AGMT_FEAT_ROLE_CD 

        ON XLAT_AGMT_FEAT_ROLE_CD.SRC_IDNTFTN_VAL = TRIM(SRC.feat_role_cd) 

    LEFT OUTER JOIN ( 

        SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

        FROM    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

        WHERE   TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

            AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) xlat_feat_sbtype_cd 

        ON xlat_feat_sbtype_cd.SRC_IDNTFTN_VAL= ( 

            Case 

                When SRC.FEAT_SBTYPE_CD = ''MODIFIER'' THEN ''FEAT_SBTYPE11'' 

                When SRC.FEAT_SBTYPE_CD = ''OPTIONS'' THEN ''FEAT_SBTYPE8'' 

                When SRC.FEAT_SBTYPE_CD = ''COVTERM'' THEN ''FEAT_SBTYPE6'' 

                When SRC.FEAT_SBTYPE_CD = ''CLAUSE'' THEN ''FEAT_SBTYPE7'' 

                When SRC.FEAT_SBTYPE_CD = ''PACKAGE'' THEN ''FEAT_SBTYPE9'' 

                When SRC.FEAT_SBTYPE_CD = ''CL'' THEN ''FEAT_SBTYPE7'' 

                When SRC.FEAT_SBTYPE_CD = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15'' End ) 

    LEFT OUTER JOIN ( 

        SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

        FROM    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

        WHERE   TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'' ) 

            AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )XLAT_SRC_CD 

        ON XLAT_SRC_CD.SRC_IDNTFTN_VAL = TRIM(SRC.SRC_CD) 

    LEFT OUTER JOIN ( 

        SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

        FROM    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

        WHERE   TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''VAL_TYPE'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_coveragebasis.name'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'' ) 

            AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )XLAT_TYPE_CD 

        ON XLAT_TYPE_CD.SRC_IDNTFTN_VAL=TRIM(SRC.val_typ_cd) 

    LEFT OUTER JOIN ( 

        SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

        FROM    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

        WHERE   TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_EFECT_TYPE'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_discountsurcharge_alfa.typecode'' 

            AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

            AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )XLAT_FEAT_EFFECT_TYPE_CD 

        ON xlat_feat_effect_type_cd.SRC_IDNTFTN_VAL=TRIM(SRC.feat_effect_type_cd) 

    INNER JOIN ( 

        SELECT  FEAT.FEAT_ID,FEAT.FEAT_SBTYPE_CD,FEAT.NK_SRC_KEY 

        FROM    DB_T_PROD_CORE.FEAT 

        WHERE   CAST(EDW_END_DTTM AS DATE)=''9999-12-31'' ) As CR_FEAT 

        ON CR_FEAT.NK_SRC_KEY = SRC.NK_PUBLIC_ID 

        AND CR_FEAT.FEAT_SBTYPE_CD = COALESCE( XLAT_FEAT_SBTYPE_CD.TGT_IDNTFTN_VAL,

            ''UNK'') 

    INNER JOIN ( 

        SELECT  AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM,AGMT.NK_SRC_KEY,AGMT.AGMT_TYPE_CD 

        FROM    DB_T_PROD_CORE.AGMT 

        WHERE   CAST(EDW_END_DTTM AS DATE)=''9999-12-31'' 

            and AGMT_TYPE_CD = ''PPV'' ) As CR_AGMT 

        ON CR_AGMT.NK_SRC_KEY =SRC.PUBLICID ) as XLAT_SRC 

LEFT OUTER JOIN ( 

    SELECT  AGMT_FEAT_END_DTTM, AGMT_FEAT_AMT, AGMT_FEAT_RATE, AGMT_FEAT_QTY,

            AGMT_FEAT_NUM, VAL_TYPE_CD, FEAT_EFECT_TYPE_CD, AGMT_FEAT_TXT,

            AGMT_FEAT_IND, FEAT_ELGBL_IND,EDW_STRT_DTTM, EDW_END_DTTM, AGMT_ID,

            FEAT_ID, AGMT_FEAT_ROLE_CD, AGMT_FEAT_STRT_DTTM 

    FROM    DB_T_PROD_CORE.AGMT_FEAT 

    QUALIFY ROW_NUMBER() OVER( 

    PARTITION BY AGMT_ID,FEAT_ID,AGMT_FEAT_ROLE_CD 

    ORDER BY EDW_END_DTTM DESC) = 1) As CR_AGMT_FEAT 

    ON XLAT_SRC.AGMT_ID = CR_AGMT_FEAT.AGMT_ID 

    AND XLAT_SRC.FEAT_ID = CR_AGMT_FEAT.FEAT_ID 

    AND XLAT_SRC.AGMT_FEAT_ROLE_CD = CR_AGMT_FEAT.AGMT_FEAT_ROLE_CD 

where   ins_upd_flag in (''I'',''U'') 

    or (ins_upd_flag=''R'' 

    and cast(CR_AGMT_FEAT.EDW_END_DTTM as date)<>''9999-12-31'')

/* -- */
) SRC
)
);


-- Component exp_insupd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insupd AS
(
SELECT
SQ_pc_agmt_feat_x.TG_AGMT_ID as LKP_AGMT_ID,
SQ_pc_agmt_feat_x.TG_FEAT_ID as LKP_FEAT_ID,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_ROLE_CD as LKP_AGMT_FEAT_ROLE_CD,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_START_DTTM as LKP_AGMT_FEAT_STRT_DT,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_END_DTTM as LKP_AGMT_FEAT_END_DT,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_AMT as LKP_AGMT_FEAT_AMT,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_RATE as LKP_AGMT_FEAT_RATE,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_QTY as LKP_AGMT_FEAT_QTY,
SQ_pc_agmt_feat_x.TG_AGMT_FEAT_NUM as LKP_AGMT_FEAT_NUM,
SQ_pc_agmt_feat_x.TG_VAL_TYPE_CD as LKP_VAL_TYPE_CD,
SQ_pc_agmt_feat_x.TG_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
SQ_pc_agmt_feat_x.TG_EDW_END_DTTM as LKP_EDW_END_DTTM,
SQ_pc_agmt_feat_x.AGMT_ID as AGMT_ID,
SQ_pc_agmt_feat_x.FEAT_ID as FEAT_ID,
SQ_pc_agmt_feat_x.SRC_AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
SQ_pc_agmt_feat_x.SRC_AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
NULL as OVRDN_FEAT_ID,
SQ_pc_agmt_feat_x.SRC_AGMT_FEAT_AMT as AGMT_FEAT_AMT,
NULL as AGMT_FEAT_TO_AMT,
SQ_pc_agmt_feat_x.SRC_FEAT_RATE as AGMT_FEAT_RATE,
SQ_pc_agmt_feat_x.FEAT_QTY as AGMT_FEAT_QTY,
SQ_pc_agmt_feat_x.FEAT_NUM as AGMT_FEAT_NUM,
SQ_pc_agmt_feat_x.AGMT_FEAT_DT as AGMT_FEAT_DT,
''UNK'' as AGMT_FEAT_UOM_CD,
NULL as INT_RATE_INDX_CD,
NULL as CURY_CD,
SQ_pc_agmt_feat_x.SRC_FEAT_TXT as AGMT_FEAT_TXT,
SQ_pc_agmt_feat_x.SRC_AGMT_FEAT_IND as AGMT_FEAT_IND,
SQ_pc_agmt_feat_x.Eligible as Eligible,
:PRCS_ID as out_PRCS_ID,
''UNK'' as UOM_TYPE_CD,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_timestamp_ntz( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
SQ_pc_agmt_feat_x.ENDDATE as AGMT_FEAT_END_DT,
SQ_pc_agmt_feat_x.FEAT_EFFECT_TYPE_CD as out_feat_effect_type_cd,
SQ_pc_agmt_feat_x.VAL_TYP_CD as out_VAL_TYP_CD,
NULL as NewLookupRow,
SQ_pc_agmt_feat_x.INSUPD as out_ins,
SQ_pc_agmt_feat_x.SRC_TRANS_START_DTTM as TRANS_STRT_DTTM,
to_timestamp_ntz( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
SQ_pc_agmt_feat_x.RETIRED as Retired,
SQ_pc_agmt_feat_x.source_record_id
FROM
SQ_pc_agmt_feat_x
);


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
SELECT
exp_insupd.LKP_AGMT_ID as LKP_AGMT_ID,
exp_insupd.LKP_FEAT_ID as LKP_FEAT_ID,
exp_insupd.LKP_AGMT_FEAT_ROLE_CD as LKP_AGMT_FEAT_ROLE_CD,
exp_insupd.LKP_AGMT_FEAT_STRT_DT as LKP_AGMT_FEAT_STRT_DT,
exp_insupd.LKP_AGMT_FEAT_END_DT as LKP_AGMT_FEAT_END_DT,
exp_insupd.LKP_AGMT_FEAT_AMT as LKP_AGMT_FEAT_AMT,
exp_insupd.LKP_AGMT_FEAT_RATE as LKP_AGMT_FEAT_RATE,
exp_insupd.LKP_AGMT_FEAT_QTY as LKP_AGMT_FEAT_QTY,
exp_insupd.LKP_AGMT_FEAT_NUM as LKP_AGMT_FEAT_NUM,
exp_insupd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_insupd.AGMT_ID as AGMT_ID,
exp_insupd.FEAT_ID as FEAT_ID,
exp_insupd.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
exp_insupd.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_insupd.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
exp_insupd.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_insupd.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
exp_insupd.AGMT_FEAT_TO_AMT as AGMT_FEAT_TO_AMT,
exp_insupd.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
exp_insupd.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
exp_insupd.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
exp_insupd.AGMT_FEAT_DT as AGMT_FEAT_DT,
exp_insupd.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
exp_insupd.INT_RATE_INDX_CD as INT_RATE_INDX_CD,
exp_insupd.CURY_CD as CURY_CD,
exp_insupd.out_PRCS_ID as PRCS_ID,
exp_insupd.UOM_TYPE_CD as UOM_TYPE_CD,
exp_insupd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_insupd.out_ins as out_ins,
NULL as out_upd,
NULL as o_ORIG_CHKSM1,
NULL as o_CALC_CHKSM1,
exp_insupd.LKP_EDW_STRT_DTTM as EDW_STRT_DTTM_upd,
exp_insupd.out_VAL_TYP_CD as out_VAL_TYP_CD,
exp_insupd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_insupd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_insupd.NewLookupRow as NewLookupRow,
exp_insupd.LKP_VAL_TYPE_CD as LKP_VAL_TYPE_CD,
exp_insupd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insupd.Retired as Retired,
NULL as out_trans_end_dttm,
exp_insupd.out_feat_effect_type_cd as out_feat_effect_type_cd,
exp_insupd.AGMT_FEAT_TXT as AGMT_FEAT_TXT,
exp_insupd.AGMT_FEAT_IND as AGMT_FEAT_IND,
exp_insupd.Eligible as Eligible,
exp_insupd.source_record_id
FROM
exp_insupd
WHERE ( exp_insupd.out_ins = ''I'' ) AND exp_insupd.AGMT_ID IS NOT NULL and exp_insupd.FEAT_ID IS NOT NULL OR ( exp_insupd.Retired = 0 AND exp_insupd.LKP_EDW_END_DTTM != to_timestamp_ntz( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AND exp_insupd.AGMT_ID IS NOT NULL and exp_insupd.FEAT_ID IS NOT NULL ) or ( exp_insupd.out_ins = ''U'' AND exp_insupd.AGMT_ID IS NOT NULL and exp_insupd.FEAT_ID IS NOT NULL AND exp_insupd.LKP_EDW_END_DTTM = to_timestamp_ntz( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) );


-- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
create or replace temporary table rtr_ins_upd_RETIRE as
SELECT
exp_insupd.LKP_AGMT_ID as LKP_AGMT_ID,
exp_insupd.LKP_FEAT_ID as LKP_FEAT_ID,
exp_insupd.LKP_AGMT_FEAT_ROLE_CD as LKP_AGMT_FEAT_ROLE_CD,
exp_insupd.LKP_AGMT_FEAT_STRT_DT as LKP_AGMT_FEAT_STRT_DT,
exp_insupd.LKP_AGMT_FEAT_END_DT as LKP_AGMT_FEAT_END_DT,
exp_insupd.LKP_AGMT_FEAT_AMT as LKP_AGMT_FEAT_AMT,
exp_insupd.LKP_AGMT_FEAT_RATE as LKP_AGMT_FEAT_RATE,
exp_insupd.LKP_AGMT_FEAT_QTY as LKP_AGMT_FEAT_QTY,
exp_insupd.LKP_AGMT_FEAT_NUM as LKP_AGMT_FEAT_NUM,
exp_insupd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_insupd.AGMT_ID as AGMT_ID,
exp_insupd.FEAT_ID as FEAT_ID,
exp_insupd.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
exp_insupd.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_insupd.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
exp_insupd.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_insupd.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
exp_insupd.AGMT_FEAT_TO_AMT as AGMT_FEAT_TO_AMT,
exp_insupd.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
exp_insupd.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
exp_insupd.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
exp_insupd.AGMT_FEAT_DT as AGMT_FEAT_DT,
exp_insupd.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
exp_insupd.INT_RATE_INDX_CD as INT_RATE_INDX_CD,
exp_insupd.CURY_CD as CURY_CD,
exp_insupd.out_PRCS_ID as PRCS_ID,
exp_insupd.UOM_TYPE_CD as UOM_TYPE_CD,
exp_insupd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_insupd.out_ins as out_ins,
NULL as out_upd,
NULL as o_ORIG_CHKSM1,
NULL as o_CALC_CHKSM1,
exp_insupd.LKP_EDW_STRT_DTTM as EDW_STRT_DTTM_upd,
exp_insupd.out_VAL_TYP_CD as out_VAL_TYP_CD,
exp_insupd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_insupd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_insupd.NewLookupRow as NewLookupRow,
exp_insupd.LKP_VAL_TYPE_CD as LKP_VAL_TYPE_CD,
exp_insupd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insupd.Retired as Retired,
NULL as out_trans_end_dttm,
exp_insupd.out_feat_effect_type_cd as out_feat_effect_type_cd,
exp_insupd.AGMT_FEAT_TXT as AGMT_FEAT_TXT,
exp_insupd.AGMT_FEAT_IND as AGMT_FEAT_IND,
exp_insupd.Eligible as Eligible,
exp_insupd.source_record_id
FROM
exp_insupd
WHERE exp_insupd.out_ins = ''R'' and exp_insupd.Retired != 0 and exp_insupd.LKP_EDW_END_DTTM = to_timestamp_ntz( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AND exp_insupd.AGMT_ID IS NOT NULL and exp_insupd.FEAT_ID IS NOT NULL;


-- Component upd_stg_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_INSERT.AGMT_ID as AGMT_ID,
rtr_ins_upd_INSERT.FEAT_ID as FEAT_ID,
rtr_ins_upd_INSERT.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
rtr_ins_upd_INSERT.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
rtr_ins_upd_INSERT.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
rtr_ins_upd_INSERT.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
rtr_ins_upd_INSERT.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
rtr_ins_upd_INSERT.AGMT_FEAT_TO_AMT as AGMT_FEAT_TO_AMT,
rtr_ins_upd_INSERT.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
rtr_ins_upd_INSERT.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
rtr_ins_upd_INSERT.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
rtr_ins_upd_INSERT.AGMT_FEAT_DT as AGMT_FEAT_DT,
rtr_ins_upd_INSERT.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
rtr_ins_upd_INSERT.INT_RATE_INDX_CD as INT_RATE_INDX_CD,
rtr_ins_upd_INSERT.CURY_CD as CURY_CD,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd_INSERT.UOM_TYPE_CD as UOM_TYPE_CD,
rtr_ins_upd_INSERT.out_VAL_TYP_CD as out_VAL_TYP_CD1,
rtr_ins_upd_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
rtr_ins_upd_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_ins_upd_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_ins_upd_INSERT.TRANS_END_DTTM as TRANS_END_DTTM1,
rtr_ins_upd_INSERT.Retired as Retired1,
rtr_ins_upd_INSERT.out_feat_effect_type_cd as out_feat_effect_type_cd1,
rtr_ins_upd_INSERT.AGMT_FEAT_TXT as AGMT_FEAT_TXT1,
rtr_ins_upd_INSERT.AGMT_FEAT_IND as AGMT_FEAT_IND1,
rtr_ins_upd_INSERT.Eligible as Eligible1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_INSERT.source_record_id as source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component upd_stg_upd_retire_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_retire_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_RETIRE.LKP_AGMT_ID as AGMT_ID,
rtr_ins_upd_RETIRE.LKP_FEAT_ID as FEAT_ID,
rtr_ins_upd_RETIRE.LKP_AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
rtr_ins_upd_RETIRE.LKP_AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
rtr_ins_upd_RETIRE.PRCS_ID as PRCS_ID,
NULL as out_EDW_STRT_DTTM3,
rtr_ins_upd_RETIRE.EDW_STRT_DTTM_upd as EDW_STRT_DTTM_upd3,
NULL as Retired3,
NULL as LKP_EDW_END_DTTM3,
rtr_ins_upd_RETIRE.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_RETIRE.source_record_id as source_record_id
FROM
rtr_ins_upd_RETIRE
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_stg_ins.AGMT_ID as AGMT_ID,
upd_stg_ins.FEAT_ID as FEAT_ID,
upd_stg_ins.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
upd_stg_ins.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
upd_stg_ins.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
upd_stg_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
upd_stg_ins.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
upd_stg_ins.AGMT_FEAT_TO_AMT as AGMT_FEAT_TO_AMT,
upd_stg_ins.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
upd_stg_ins.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
upd_stg_ins.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
upd_stg_ins.AGMT_FEAT_DT as AGMT_FEAT_DT,
upd_stg_ins.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
upd_stg_ins.INT_RATE_INDX_CD as INT_RATE_INDX_CD,
upd_stg_ins.CURY_CD as CURY_CD,
upd_stg_ins.PRCS_ID as PRCS_ID,
upd_stg_ins.UOM_TYPE_CD as UOM_TYPE_CD,
upd_stg_ins.out_VAL_TYP_CD1 as out_VAL_TYP_CD1,
upd_stg_ins.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM1,
CASE WHEN upd_stg_ins.Retired1 != 0 THEN upd_stg_ins.out_EDW_STRT_DTTM1 ELSE upd_stg_ins.out_EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_stg_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
CASE WHEN upd_stg_ins.Retired1 != 0 THEN upd_stg_ins.TRANS_STRT_DTTM1 ELSE upd_stg_ins.TRANS_END_DTTM1 END as o_TRANS_END_DTTM,
upd_stg_ins.out_feat_effect_type_cd1 as out_feat_effect_type_cd1,
upd_stg_ins.AGMT_FEAT_TXT1 as AGMT_FEAT_TXT1,
upd_stg_ins.AGMT_FEAT_IND1 as AGMT_FEAT_IND1,
upd_stg_ins.Eligible1 as Eligible1,
upd_stg_ins.source_record_id
FROM
upd_stg_ins
);


-- Component exp_pass_to_target_upd__retire_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd__retire_rejected AS
(
SELECT
upd_stg_upd_retire_rejected.AGMT_ID as AGMT_ID,
upd_stg_upd_retire_rejected.FEAT_ID as FEAT_ID,
upd_stg_upd_retire_rejected.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
CURRENT_TIMESTAMP as out_EDW_END_DTTM,
upd_stg_upd_retire_rejected.EDW_STRT_DTTM_upd3 as EDW_STRT_DTTM_upd3,
upd_stg_upd_retire_rejected.TRANS_STRT_DTTM4 as out_trans_end_dttm4,
upd_stg_upd_retire_rejected.source_record_id
FROM
upd_stg_upd_retire_rejected
);


-- Component AGMT_FEAT_insert_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_FEAT
(
AGMT_ID,
FEAT_ID,
AGMT_FEAT_ROLE_CD,
AGMT_FEAT_STRT_DTTM,
AGMT_FEAT_END_DTTM,
OVRDN_FEAT_ID,
AGMT_FEAT_AMT,
AGMT_FEAT_TO_AMT,
AGMT_FEAT_RATE,
AGMT_FEAT_QTY,
AGMT_FEAT_NUM,
AGMT_FEAT_DT,
AGMT_FEAT_UOM_CD,
INT_RATE_INDX_CD,
CURY_CD,
UOM_TYPE_CD,
VAL_TYPE_CD,
FEAT_EFECT_TYPE_CD,
AGMT_FEAT_TXT,
AGMT_FEAT_IND,
FEAT_ELGBL_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.AGMT_ID as AGMT_ID,
exp_pass_to_target_ins.FEAT_ID as FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
exp_pass_to_target_ins.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DTTM,
exp_pass_to_target_ins.AGMT_FEAT_END_DT as AGMT_FEAT_END_DTTM,
exp_pass_to_target_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
exp_pass_to_target_ins.AGMT_FEAT_TO_AMT as AGMT_FEAT_TO_AMT,
exp_pass_to_target_ins.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
exp_pass_to_target_ins.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
exp_pass_to_target_ins.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
exp_pass_to_target_ins.AGMT_FEAT_DT as AGMT_FEAT_DT,
exp_pass_to_target_ins.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
exp_pass_to_target_ins.INT_RATE_INDX_CD as INT_RATE_INDX_CD,
exp_pass_to_target_ins.CURY_CD as CURY_CD,
exp_pass_to_target_ins.UOM_TYPE_CD as UOM_TYPE_CD,
exp_pass_to_target_ins.out_VAL_TYP_CD1 as VAL_TYPE_CD,
exp_pass_to_target_ins.out_feat_effect_type_cd1 as FEAT_EFECT_TYPE_CD,
exp_pass_to_target_ins.AGMT_FEAT_TXT1 as AGMT_FEAT_TXT,
exp_pass_to_target_ins.AGMT_FEAT_IND1 as AGMT_FEAT_IND,
exp_pass_to_target_ins.Eligible1 as FEAT_ELGBL_IND,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component AGMT_FEAT_upd_retire_rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_FEAT
USING exp_pass_to_target_upd__retire_rejected ON (AGMT_FEAT.AGMT_ID = exp_pass_to_target_upd__retire_rejected.AGMT_ID AND AGMT_FEAT.FEAT_ID = exp_pass_to_target_upd__retire_rejected.FEAT_ID AND AGMT_FEAT.AGMT_FEAT_ROLE_CD = exp_pass_to_target_upd__retire_rejected.AGMT_FEAT_ROLE_CD AND AGMT_FEAT.EDW_STRT_DTTM = exp_pass_to_target_upd__retire_rejected.EDW_STRT_DTTM_upd3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_target_upd__retire_rejected.AGMT_ID,
FEAT_ID = exp_pass_to_target_upd__retire_rejected.FEAT_ID,
AGMT_FEAT_ROLE_CD = exp_pass_to_target_upd__retire_rejected.AGMT_FEAT_ROLE_CD,
EDW_STRT_DTTM = exp_pass_to_target_upd__retire_rejected.EDW_STRT_DTTM_upd3,
EDW_END_DTTM = exp_pass_to_target_upd__retire_rejected.out_EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_target_upd__retire_rejected.out_trans_end_dttm4;


-- Component AGMT_FEAT_upd_retire_rejected, Type Post SQL 
UPDATE db_t_prod_core.AGMT_FEAT
SET EDW_END_DTTM = A.lead1
, TRANS_END_DTTM= A.lead2
FROM 

(

SELECT distinct AGMT_ID,FEAT_ID,AGMT_FEAT_ROLE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,AGMT_FEAT_ROLE_CD ORDER BY

EDW_STRT_DTTM ASC rows between 1 following

and 1 following) - INTERVAL ''1 SECOND'' as lead1

,max(TRANS_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,AGMT_FEAT_ROLE_CD ORDER BY

TRANS_STRT_DTTM ASC rows between 1 following

and 1 following) - INTERVAL ''1 SECOND'' as lead2

FROM db_t_prod_core.AGMT_FEAT

) A

WHERE

AGMT_FEAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND AGMT_FEAT.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

AND AGMT_FEAT.AGMT_ID = A.AGMT_ID

AND AGMT_FEAT.FEAT_ID = A.FEAT_ID

AND AGMT_FEAT.AGMT_FEAT_ROLE_CD = A.AGMT_FEAT_ROLE_CD

AND CAST(AGMT_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''

AND CAST(AGMT_FEAT.TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null and lead2 is not null;


END; ';