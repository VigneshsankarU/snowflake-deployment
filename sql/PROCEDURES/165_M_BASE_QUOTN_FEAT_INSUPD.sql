-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_FEAT_INSUPD("RUN_ID" VARCHAR)
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

-- PIPELINE START FOR 2

-- Component SQ_pc_quotation_feat_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_quotation_feat_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LKP_QUOTN_ID,
$2 as LKP_FEAT_ID,
$3 as LKP_AGMT_FEAT_ROLE_CD,
$4 as LKP_AGMT_FEAT_STRT_DT,
$5 as QUOTN_ID,
$6 as FEAT_ID,
$7 as AGMT_FEAT_ROLE_CD,
$8 as AGMT_FEAT_STRT_DT,
$9 as AGMT_FEAT_END_DT,
$10 as OVRDN_FEAT_ID,
$11 as AGMT_FEAT_AMT,
$12 as AGMT_FEAT_RATE,
$13 as AGMT_FEAT_QTY,
$14 as AGMT_FEAT_NUM,
$15 as AGMT_FEAT_UOM_CD,
$16 as CURY_CD,
$17 as PRCS_ID,
$18 as UOM_TYPE_CD,
$19 as LKP_AGMT_FEAT_END_DT,
$20 as LKP_AGMT_FEAT_AMT,
$21 as LKP_AGMT_FEAT_RATE,
$22 as LKP_AGMT_FEAT_QTY,
$23 as LKP_AGMT_FEAT_NUM,
$24 as SOURCE_DATA,
$25 as TARGET_DATA,
$26 as ins_upd_flag,
$27 as EDW_STRT_DTTM_upd,
$28 as TRANS_STRT_DTTM,
$29 as out_feat_effect_type_cd,
$30 as AGMT_FEAT_TXT,
$31 as AGMT_FEAT_DT,
$32 as AGMT_FEAT_IND,
$33 as Eligible,
$34 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH FARM_TEMP AS (/* EIM-48793 - QUTON_FEAT - FARM CHANGES BEGINS*/

select  distinct pj.JobNumber_stg,pp.BranchNumber_stg,

case when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg  else polcov.EffectiveDate_stg end as startdate, 

case when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg  else polcov.ExpirationDate_stg end as enddate, 

case when polcov.typ = ''MODIFIER'' then pe.PatternID_stg  else pc.PatternID_stg end as nk_public_id, 

case when polcov.typ = ''EXCLUSION'' then ''CLAUSE''  else polcov.typ end as FEAT_SBTYPE_CD, 

cast(ratemodifier as varchar(255)) as feat_amt,

cast(0 as varchar(50))as feat_qty, 

cast(0 as varchar(50)) as feat_num,

pp.PublicID_stg, pp.Createtime_stg, polcov.feat_rate, pp.updatetime_stg,

pda.typecode_stg as feat_effect_type_cd, cast(null as varchar(255)) as feat_val,

cast(null as varchar(255)) as feat_CovTermType, 

 (:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,  

cast(polcov.Eligible as varchar(10)) as Eligible 

from ( select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

cast(''EXCLUSION'' as varchar(250)) as typ, EffectiveDate_stg,

ExpirationDate_stg, cast(NULL as varchar(255)) as ratemodifier,

cast(NULL as varchar(255)) AS DiscountSurcharge_alfa, cast(NULL as varchar(255)) as feat_rate,

cast(NULL as varchar(250)) as Eligible 

from DB_T_PROD_STAG.pcx_fopfarmownerslineexcl fexcl

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fexcl.BranchID_stg 

where  (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm) 

 qualify row_number() over (partition by BranchID_stg,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,fexcl.updatetime_stg desc,fexcl.createtime_stg desc)=1

 union

 select  patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

''MODIFIER'' as typ, EffectiveDate_stg, ExpirationDate_stg, cast(ratemodifier_stg as varchar(255)) as ratemodifier,

cast(DiscountSurcharge_alfa_stg as varchar(255)) as DiscountSurcharge_alfa,

cast( case when fop.Eligible_stg= 1 THEN fop.RateModifier_stg ELSE 0 end as varchar(255)) as feat_rate, 

cast(Eligible_stg as varchar(10)) as Eligible 

from DB_T_PROD_STAG.pcx_foplinemod fop 

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg

where  (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

and pp.updatetime_stg > (:start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

qualify row_number() over (partition by BranchID_stg,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,fop.updatetime_stg desc,fop.createtime_stg desc)=1

 ) polcov 

left join DB_T_PROD_STAG.pc_etlmodifierpattern pe on pe.patternid_stg = polcov.PatternCode_stg and polcov.typ = ''MODIFIER'' 

left join DB_T_PROD_STAG.pc_etlclausepattern pc  on pc.patternid_stg = polcov.PatternCode_stg and polcov.typ = ''EXCLUSION'' 

inner join (  select  cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,BranchNumber_stg,

createtime_stg, updatetime_stg,Retired_stg from DB_T_PROD_STAG.PC_POLICYPERIOD ) pp on pp.id = polcov.BranchID 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.id_stg = pp.Status_stg 

inner join DB_T_PROD_STAG.pc_job pj on pj.id_stg = pp.JobID_stg 

inner join DB_T_PROD_STAG.pctl_job pcj on pcj.id_stg=pj.Subtype_stg 

LEFT JOIN DB_T_PROD_STAG.pctl_discountsurcharge_alfa pda ON polcov.DiscountSurcharge_alfa = pda.ID_stg 

where   (pc.Name_stg not like''%ZZ%'' or pe.Name_stg not like''%ZZ%'') and pcj.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

and pps.TYPECODE_stg <> ''Temporary'' and pp.updatetime_stg> (:start_dttm) and pp.updatetime_stg <= (:end_dttm)     

 union

 select distinct pj.JobNumber_stg,pp.BranchNumber_stg,

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

cast (null as varchar(50)) feat_effect_type_cd, 

polcov.val feat_val,

covterm.CovTermType feat_CovTermType,

 (:start_dttm) start_dttm,

(:end_dttm) end_dttm,  

cast(NULL as varchar(10)) as Eligible 

from (/* DB_T_PROD_STAG.pcx_fopfarmownerslinecov */
select * from (

select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''StringTerm1'' AS VARCHAR(250)) as columnname, cast(StringTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where StringTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''BooleanTerm1'' AS VARCHAR(250)) as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where BooleanTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

UNION

select  distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg 

from  DB_T_PROD_STAG.pcx_fopfarmownerslinecov  fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

  and pp.updatetime_stg > ( :start_dttm)   and pp.updatetime_stg <= ( :end_dttm) 

 where (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

 and ChoiceTerm1Avl_stg is null 

  and DirectTerm1Avl_stg is null 

  and StringTerm1Avl_stg is null 

  and BooleanTerm1Avl_stg is null 

) as folc 

qualify row_number() over (partition by columnname,BranchID,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1



union

/* DB_T_PROD_STAG.pcx_fopliabilitycov */
select * from (

select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm4'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm4Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm5'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm5Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''StringTerm1'' AS VARCHAR(250)) as columnname, cast(StringTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where StringTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''StringTerm2'' AS VARCHAR(250)) as columnname, cast(StringTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where StringTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''StringTerm3'' AS VARCHAR(250)) as columnname, cast(StringTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where StringTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilitycov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

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

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fexcl.BranchID_stg 

where  ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

 and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)

 union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopliabilityexcl fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

and ChoiceTerm1Avl_stg is null

) as flc 

qualify row_number() over (partition by columnname,BranchID,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1



union

/* DB_T_PROD_STAG.pcx_fopblanketcov */
select * from (

select distinct CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm2Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where ChoiceTerm3Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''DirectTerm1'' AS VARCHAR(250)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)  

where DirectTerm1Avl_stg = 1 and (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg)

union

select distinct CAST(''Clause'' AS VARCHAR(250)) as columnname, cast(null as varchar(255)) as val,

cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

fop.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255)) as choiceterm1, 

cast(null as varchar(255)) as patternid,fop.updatetime_stg

from DB_T_PROD_STAG.pcx_fopblanketcov fop

inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = fop.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)  

where (ExpirationDate_stg is null or ExpirationDate_stg > editeffectivedate_stg) 

and ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null

and DirectTerm1Avl_stg is null

) as fbc

qualify row_number() over (partition by columnname,BranchID,patterncode_stg  order by coalesce(ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,updatetime_stg desc,createtime_stg desc)=1

) polcov 

inner join ( select cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,

 PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,BranchNumber_stg,

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

where  covterm.clausename not like''%ZZ%'' and pcj.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

 and pps.TYPECODE_stg<>''Temporary'' and  pp.updatetime_stg > (:start_dttm) and pp.updatetime_stg <= (:end_dttm)

 UNION

/* ENDORSEMENTS */
select distinct cast(jobnumber_stg as varchar(100))as JOBNUMBER_STG,pp.branchnumber_stg,  

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,  

pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,  

 '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 pho.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,  

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible

from DB_T_PROD_STAG.pcx_fopfarmownerslinecov pho   

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp  on pp.id_stg = pho.branchid_stg   

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg

 and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = pho.patterncode_stg

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = pho.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = pho.patterncode_stg  

 where  (pho.EffectiveDate_stg is null or (pho.EffectiveDate_stg > pp.editeffectivedate_stg  and pho.EffectiveDate_stg <> pho.ExpirationDate_stg))   

 and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null  

 union

select distinct cast(jobnumber_stg as varchar(100))as JOBNUMBER_STG,pp.branchnumber_stg,  

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,  

pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,  

 '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 pho.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,  

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible

from DB_T_PROD_STAG.pcx_fopliabilitycov pho   

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp  on pp.id_stg = pho.branchid_stg   

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg

 and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = pho.patterncode_stg

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = pho.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = pho.patterncode_stg  

 where  (pho.EffectiveDate_stg is null or (pho.EffectiveDate_stg > pp.editeffectivedate_stg  and pho.EffectiveDate_stg <> pho.ExpirationDate_stg))   

 and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null  

 union

 select distinct cast(jobnumber_stg as varchar(100))as JOBNUMBER_STG,pp.branchnumber_stg,  

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,  

pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,  

 '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 pho.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,  

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible 

from DB_T_PROD_STAG.pcx_fopblanketcov pho   

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp  on pp.id_stg = pho.branchid_stg   

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg

 and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = pho.patterncode_stg

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = pho.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = pho.patterncode_stg  

 where  (pho.EffectiveDate_stg is null or (pho.EffectiveDate_stg > pp.editeffectivedate_stg  and pho.EffectiveDate_stg <> pho.ExpirationDate_stg))   

 and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null  

 /* EIM-48793 - QUTON_FEAT - FARM CHANGES ENDS*/),SET1 as ( 

 select distinct pc_job.JobNumber_stg,pc_policyperiod.BranchNumber_stg,

 case when polcov.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg else polcov.EffectiveDate_stg end as startdate, 

 case when polcov.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg else polcov.ExpirationDate_stg end as enddate, 

 case when covterm.CovTermType =''Package'' then package.packagePatternID when covterm.CovTermType=''Option'' and polcov.val is not null then optn.optionPatternID when covterm.CovTermType=''Clause'' then covterm.clausePatternID else covterm.covtermPatternID end as nk_public_id, 

 case when covterm.CovTermType=''Package'' then cast (''PACKAGE'' as varchar (50)) when covterm.CovTermType=''Option'' and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) else cast ( ''COVTERM'' as varchar (50)) end as FEAT_SBTYPE_CD, 

 case when covterm.CovTermType=''Option'' and optn.ValueType=''money'' then optn.Value1 when covterm.CovTermType<>''Option'' then polcov.val end as feat_amt , 

 case when optn.ValueType=''count'' then optn.Value1 end as feat_qty , 

 case when optn.ValueType in (''days'',''hours'',''other'') then optn.value1 end as feat_num, pc_policyperiod.PublicID_stg,pc_policyperiod.Createtime_stg,

 case when optn.ValueType=''percent'' then optn.Value1  end as feat_rate, polcov.updatetime_stg,cast (null as varchar (50)) feat_effect_type_cd,

 polcov.val as feat_val,covterm.CovTermType as feat_CovTermType,

 (:start_dttm) as start_dttm,(:end_dttm) as end_dttm,cast(NULL as varchar(10)) as Eligible 

 from ( /****************pcx_dwellingcov_hoe****************************/ 

 select cast(''ChoiceTerm1'' as varchar(50)) as columnname, CAST(ChoiceTerm1_stg AS VARCHAR(255)) as val,

 cast(patterncode_stg as varchar(255)) aS patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey, ''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg, EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm1Avl_stg=1 and ExpirationDate_stg is null and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm2'' as varchar(50)) as columnname, CAST(ChoiceTerm2_stg AS VARCHAR(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg ,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm3'' as varchar(50)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm3Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm4'' as varchar(50)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm5'' as varchar(50)) as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm5Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm3'' as varchar(50)) as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm3Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm4'' as varchar(50)) as columnname, cast(DirectTerm4_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm1'' as varchar(50)) as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast(''BooleanTerm2'' as varchar(50)) as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(Dwelling_stg as varchar(255)) as assetkey,''dwelling_hoe'' as assettype,

 pcx_dwellingcov_hoe.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_dwellingcov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_dwellingcov_hoe 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_dwellingcov_hoe.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null and ChoiceTerm4Avl_stg is null 

 and ChoiceTerm5Avl_stg is null and DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null and DirectTerm3Avl_stg is null 

 and DirectTerm4Avl_stg is null and BooleanTerm2Avl_stg is null and BooleanTerm1Avl_stg is null and ExpirationDate_stg is null 

 union /*HOLINECOV*/ /*eim-34802*/ 

 select cast(''ChoiceTerm1'' as varchar(50)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''ChoiceTerm2'' as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm2Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select ''ChoiceTerm3'' as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''ChoiceTerm4'' as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm4Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''ChoiceTerm5'' as columnname, cast(ChoiceTerm5_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm5Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where DirectTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where DirectTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where BooleanTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''BooleanTerm2'' as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where BooleanTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchID,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where StringTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''StringTerm2'' as columnname, StringTerm2_stg as val, patterncode_stg,

 cast(BranchID_stg as varchar(255)) as BranchID, cast(HOLINE_STG as varchar(255)) as assetkey,

 ''HomeownersLine_HOE'' as assettype, pcx_homeownerslinecov_hoe.createtime_stg,

 EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where StringTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''Clause'' as columnname, cast(null as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(HOLINE_STG as varchar(255)) as assetkey,''HomeownersLine_HOE'' as assettype,

 pcx_homeownerslinecov_hoe.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, pcx_homeownerslinecov_hoe.updatetime_stg 

 from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pcx_homeownerslinecov_hoe.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null and ChoiceTerm4Avl_stg is null 

 and ChoiceTerm5Avl_stg is null and DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null and BooleanTerm1Avl_stg is null 

 and BooleanTerm2Avl_stg is null and StringTerm1Avl_stg is null and StringTerm2Avl_stg is null and ExpirationDate_stg is null 

 UNION /**************BP7Lines*****************************/ 

 select cast(''ChoiceTerm1'' as varchar(50)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm2''as varchar(50)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm2, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD 

 on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''ChoiceTerm3'' as varchar(50)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm3, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm4'' as varchar(50)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm4, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm4Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm5'' as varchar(50)) as columnname,cast(ChoiceTerm5_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm5, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm5Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm6'' as varchar(50)) as columnname, cast(ChoiceTerm6_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm6, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm6Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm7'' as varchar(50)) as columnname, cast(ChoiceTerm7_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm7, cast(null as varchar(255)) as patternid ,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm7Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm1, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm2, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm3'' as varchar(50)) as columnname, cast(DirectTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm3, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm4'' as varchar(50)) as columnname, cast(DirectTerm4_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm4, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm4Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm5'' as varchar(50)) as columnname, cast(DirectTerm5_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm5, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm5Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm6'' as varchar(50)) as columnname, cast(DirectTerm6_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm6, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm6Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm7'' as varchar(50)) as columnname,cast(DirectTerm7_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm7, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm7Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm8'' as varchar(50)) as columnname, cast(DirectTerm8_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm8, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm8Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm9'' as varchar(50)) as columnname,cast(DirectTerm9_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm9, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm9Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm10'' as varchar(50)) as columnname, cast(DirectTerm10_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DirectTerm10, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm10Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm1'' as varchar(50)) as columnname,cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm1, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''BooleanTerm2'' as varchar(50)) as columnname,cast(BooleanTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm2, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm3'' as varchar(50)) as columnname,cast(BooleanTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm3, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm4'' as varchar(50)) as columnname,cast(BooleanTerm4_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm4, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm4Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm5'' as varchar(50)) as columnname, cast(BooleanTerm5_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm5, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm5Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm6'' as varchar(50)) as columnname, cast(BooleanTerm6_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm6, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm6Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm7'' as varchar(50)) as columnname,cast(BooleanTerm7_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm7, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm7Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm8'' as varchar(50)) as columnname,cast(BooleanTerm8_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm8, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm8Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm9'' as varchar(50)) as columnname, cast(BooleanTerm9_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm9,cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm9Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm10'' as varchar(50)) as columnname, cast(BooleanTerm10_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm10, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm10Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm11'' as varchar(50)) as columnname,cast(BooleanTerm11_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm11, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm11Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm12'' as varchar(50)) as columnname, cast(BooleanTerm12_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm12, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm12Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm13'' as varchar(50)) as columnname, cast(BooleanTerm13_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm13, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm13Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''BooleanTerm14'' as varchar(50)) as columnname, cast(BooleanTerm14_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as BooleanTerm14, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where BooleanTerm14Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm1''as varchar(50)) as columnname,cast(StringTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm1, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm2''as varchar(50)) as columnname,cast(StringTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm2, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm3''as varchar(50)) as columnname, cast(StringTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm3, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm4''as varchar(50)) as columnname,cast(StringTerm4_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm4, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm4Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm5''as varchar(50)) as columnname, cast(StringTerm5_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm5, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm5Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm6''as varchar(50)) as columnname, cast(StringTerm6_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm6, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm6Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm7''as varchar(50)) as columnname, cast(StringTerm7_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm7, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm7Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm8''as varchar(50)) as columnname, cast(StringTerm8_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm8, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm8Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm9''as varchar(50)) as columnname, cast(StringTerm9_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm9, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm9Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm10''as varchar(50)) as columnname, cast(StringTerm10_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm10, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm10Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm11''as varchar(50)) as columnname,cast(StringTerm11_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm11, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm11Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm12''as varchar(50)) as columnname, cast(StringTerm12_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm12, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm12Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm13''as varchar(50)) as columnname, cast(StringTerm13_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm13, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm13Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm14''as varchar(50)) as columnname,cast(StringTerm14_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm14, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm14Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select Cast(''StringTerm15''as varchar(50)) as columnname, cast(StringTerm15_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as StringTerm15, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where StringTerm15Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DateTerm1'' as varchar(50)) as columnname, cast(DateTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DateTerm1, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DateTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''DateTerm2'' as varchar(50)) as columnname, cast(DateTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as DateTerm2, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov. updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DateTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''PositiveIntTerm1'' as varchar(50)) as columnname, cast(PositiveIntTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as PositiveIntTerm1, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where PositiveIntTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''PositiveIntTerm2'' as varchar(50)) as columnname,cast(PositiveIntTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as PositiveIntTerm2, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where PositiveIntTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''PositiveIntTerm3'' as varchar(50)) as columnname,cast(PositiveIntTerm3_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as PositiveIntTerm3, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where PositiveIntTerm3Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select cast(''Clause'' as varchar(50)) as columnname,cast(null as varchar(255)) val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 cast(BP7Line_stg as varchar(255)) as assetkey, ''bp7line'' as assettype,

 pcx_bp7linecov.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as Clause, cast(null as varchar(255)) as patternid,

 pcx_bp7linecov.updatetime_stg 

 from DB_T_PROD_STAG.pcx_bp7linecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_bp7linecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm)

 union /*******************************************pcx_holineschcovitemcov_alfa***************/ 

 select cast(''ChoiceTerm1'' as varchar(50)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg, pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and ChoiceTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm2'' as varchar(50)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg, pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and ChoiceTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm3'' as varchar(50)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and ChoiceTerm3Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm4'' as varchar(50)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and ChoiceTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and DirectTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and DirectTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm1'' as varchar(50)) as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and BooleanTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm2'' as varchar(50)) as columnname, cast(BooleanTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and BooleanTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm3'' as varchar(50)) as columnname, cast(BooleanTerm3_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and BooleanTerm3Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm4'' as varchar(50)) as columnname, cast(BooleanTerm4_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and BooleanTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm5'' as varchar(50)) as columnname, cast(BooleanTerm5_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and BooleanTerm5Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''StringTerm1'' as varchar(50)) as columnname, cast(StringTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg ,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and StringTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''StringTerm2'' as varchar(50)) as columnname, cast(StringTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and StringTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''StringTerm3'' as varchar(50)) as columnname, cast(StringTerm3_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and StringTerm3Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''StringTerm4'' as varchar(50)) as columnname, cast(StringTerm4_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and StringTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DateTerm1'' as varchar(50)) as columnname, cast(DateTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and DateTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DateTerm4'' as varchar(50)) as columnname, cast(DateTerm4_stg as varchar(255)) val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg ,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and DateTerm4Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (HOLineSchCovItem_stg as varchar(255)) as assetkey,''holineschedcovitem_alfa'' as assettype,

 pcx_holineschcovitemcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg ,ChoiceTerm1_stg , pc_etlclausepattern.PatternID_stg as patternid ,

 pcx_holineschcovitemcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_holineschcovitemcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg 

 where pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'', ''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

 and ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null and ChoiceTerm4Avl_stg is null 

 and ChoiceTerm5Avl_stg is null and ChoiceTerm6Avl_stg is null and DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null 

 and BooleanTerm1Avl_stg is null and BooleanTerm2Avl_stg is null and BooleanTerm3Avl_stg is null and BooleanTerm4Avl_stg is null 

 and BooleanTerm5Avl_stg is null and StringTerm1Avl_stg is null and StringTerm2Avl_stg is null and StringTerm3Avl_stg is null and StringTerm4Avl_stg is null and DateTerm1Avl_stg is null 

 and DateTerm4Avl_stg is null and ExpirationDate_stg is null /*Added as part of ticket EIM-16675*/ 

 union 

 select case 

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

 cast(HOLine_stg as varchar(255)) AS assetkey, ''pcx_holineexcl_hoe'' as assettype,

 h.createtime_stg, effectiveDate_stg, h.expirationDate_stg, choiceterm1_stg,

 cast(null as varchar(255)) as patternid, h.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe h 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = h.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 inner join ( 

 select 1 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 2 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 3 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 4 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 5 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 6 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 7 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 8 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 9 as seq1 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe 

 union 

 select 10 as seq1 

 from db_t_prod_stag.pcx_holineexcl_hoe) as t 

 on 1 = 1 

 where h.expirationdate_stg is null 

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

 select cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as branchid,

 cast(HOLine_stg as varchar(255)) AS assetkey, ''pcx_holineexcl_hoe'' as assettype,

 H.createtime_stg,h.effectiveDate_stg, h.expirationDate_stg, cast(null as varchar(255)) as choiceterm1_stg,

 cast(null as varchar(255)) as patternid, H.updatetime_stg 

 from DB_T_PROD_STAG.pcx_holineexcl_hoe h 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD A ON A.ID_stg=H.BRANCHID_stg 

 and a.updatetime_stg> (:start_dttm) and a.updatetime_stg <= (:end_dttm) 

 where coalesce(BooleanTerm1Avl_stg,BooleanTerm2Avl_stg,ChoiceTerm1Avl_stg, DateTerm1Avl_stg,DateTerm2Avl_stg,DirectTerm1Avl_stg,DirectTerm2Avl_stg, StringTerm1Avl_stg,StringTerm2Avl_stg) is null and expirationdate_stg is null 

 group by patterncode_stg, branchid_stg, h.HOLine_stg,h.createtime_stg, effectiveDate_stg, expirationDate_stg,h.updatetime_stg ) polcov 

 left join ( select pc_etlclausepattern.PatternID_stg as clausePatternID, pc_etlcovtermpattern.PatternID_stg as covtermPatternID,

 pc_etlcovtermpattern.ColumnName_stg, pc_etlcovtermpattern.CovTermType_stg as CovTermType,

 pc_etlclausepattern.name_stg as clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern 

 join DB_T_PROD_STAG.pc_etlcovtermpattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg 

 union 

 select pc_etlclausepattern.PatternID_stg as clausePatternID, pc_etlcovtermpattern.PatternID_stg as covtermPatternID,

 coalesce(pc_etlcovtermpattern.ColumnName_stg,''Clause'') as columnname,

 coalesce(pc_etlcovtermpattern.CovTermType_stg,''Clause'') as covtermtype,

 pc_etlclausepattern.name_stg as clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern 

 left join ( select * from DB_T_PROD_STAG.pc_etlcovtermpattern where Name_stg not like ''ZZ%'') pc_etlcovtermpattern on pc_etlcovtermpattern.ClausePatternID_stg=pc_etlclausepattern.ID_stg 

 where pc_etlclausepattern.Name_stg not like ''ZZ%'' and pc_etlcovtermpattern.Name_stg is null 

/* -PMOP-54882-PERSONALUMBRELLA CHANGES - added DB_T_STAG_MEMBXREF_PROD.umbrella in owningentitytype_stg-------  */
 and OwningEntityType_stg in (''HOLineSchCovItem_alfa'',''HomeownersLine_HOE'', ''Dwelling_HOE'', ''PersonalVehicle'', ''PersonalAutoLine'' ,''BP7BusinessOwnersLine'',''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'' ) ) covterm 

 on covterm.clausePatternID=polcov.PatternCode_stg and covterm.ColumnName_stg=polcov.columnname 

 inner join ( select cast(id_stg as varchar(255)) as id,PolicyNumber_stg,PeriodStart_stg,

 PeriodEnd_stg,MostRecentModel_stg,Status_stg,JobID_stg ,PublicID_stg, Createtime_stg ,updatetime_stg,BranchNumber_stg 

 from DB_T_PROD_STAG.PC_POLICYPERIOD ) PC_POLICYPERIOD on pc_policyperiod.id = polcov.BranchID 

 left outer join ( select pc_etlcovtermpackage.PatternID_stg as packagePatternID,

 pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name 

 from DB_T_PROD_STAG.pc_etlcovtermpackage ) package on package.packagePatternID=polcov.val 

 left outer join ( 

 select pc_etlcovtermoption.PatternID_stg as optionPatternID, pc_etlcovtermoption.optioncode_stg as name,

 cast(pc_etlcovtermoption.value_stg as varchar(255)) as value1,

 pc_etlcovtermpattern.ValueType_stg as ValueType 

 from DB_T_PROD_STAG.pc_etlcovtermpattern 

 inner join DB_T_PROD_STAG.pc_etlcovtermoption on pc_etlcovtermpattern.id_stg=pc_etlcovtermoption.CoverageTermPatternID_stg ) optn on optn.optionPatternID=polcov.val 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg 

 inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg 

 inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg 

 where covterm.clausename not like''%ZZ%'' and pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

 and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary'' and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) ),

 SET2 AS ( select distinct pc_job.JobNumber_stg,pc_policyperiod.BranchNumber_stg,

 case when polcov.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg else polcov.EffectiveDate_stg end as startdate, 

 case when polcov.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg else polcov.ExpirationDate_stg end as enddate, 

 case when covterm.CovTermType =''Package'' then package.packagePatternID when covterm.CovTermType=''Option'' and polcov.val is not null then optn.optionPatternID 

 when covterm.CovTermType=''Clause'' then covterm.clausePatternID else covterm.covtermPatternID end as nk_public_id, 

 case when covterm.CovTermType=''Package'' then cast (''PACKAGE'' as varchar (50)) when covterm.CovTermType=''Option'' and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) 

 when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) else cast ( ''COVTERM'' as varchar (50)) end as FEAT_SBTYPE_CD, 

 case when covterm.CovTermType=''Option'' and optn.ValueType=''money'' then optn.Value1 when covterm.CovTermType<>''Option'' then polcov.val end as feat_amt , 

 case when optn.ValueType=''count'' then optn.Value1 end as feat_qty , 

 case 

 when optn.ValueType in (''days'',''hours'',''other'') then optn.value1 end as feat_num, 

 pc_policyperiod.PublicID_stg,pc_policyperiod.Createtime_stg,

 case when optn.ValueType=''percent'' then optn.Value1 end as feat_rate, 

 polcov.updatetime_stg,cast (null as varchar (50)) feat_effect_type_cd,

 polcov.val as feat_val,covterm.CovTermType as feat_CovTermType,

 (:start_dttm) as start_dttm,(:end_dttm) as end_dttm,cast(NULL as varchar(10)) as Eligible 

 from ( /*****************************pc_personalvehiclecov************************************************/ 

 select cast(''ChoiceTerm1'' as varchar(50)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pc_personalvehiclecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm1Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select cast(''ChoiceTerm2'' as varchar(50)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pc_personalvehiclecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm2Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pc_personalvehiclecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pc_personalvehiclecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm2Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select Cast (''BooleanTerm1'' as varchar(50)) as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 where BooleanTerm1Avl_stg =1 and ExpirationDate_stg is null 

 union 

 select cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PersonalVehicle_stg as varchar(255)) as assetkey,''personalvehicle'' as assettype,

 pc_personalvehiclecov.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid ,

 pc_personalvehiclecov.updatetime_stg 

 from DB_T_PROD_STAG.pc_personalvehiclecov 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pc_personalvehiclecov.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null 

 and BooleanTerm1Avl_stg is null and ExpirationDate_stg is null 

 union /*****************************pcx_pawatercraftmotorcov_alfa*********************************************/ 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PAWatercraftMotor_alfa_stg as varchar(255)) assetkey,''pawatercraftmotor_alfa'' as assettype,

 pcx_pawatercraftmotorcov_alfa.createtime_stg,EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PAWatercraftMotor_alfa_stg as varchar(255)) as assetkey,

 ''pawatercraftmotor_alfa'' as assettype,pcx_pawatercraftmotorcov_alfa.createtime_stg,

 EffectiveDate_stg,ExpirationDate_stg , cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast (PAWatercraftMotor_alfa_stg as varchar(255)) as assetkey,

 ''pawatercraftmotor_alfa'' as assettype,pcx_pawatercraftmotorcov_alfa.createtime_stg,

 EffectiveDate_stg,ExpirationDate_stg , cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawatercraftmotorcov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawatercraftmotorcov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawatercraftmotorcov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null and ExpirationDate_stg is null 

 union /*****************************pcx_pawctrailercov_alfa************************************************/ 

 select cast(''DirectTerm1'' as varchar(50)) as columnname, cast(DirectTerm1_stg as varchar(255))as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(PAWatercraftTrailer_alfa_stg as varchar(255)) as assetkey,

 ''pawatercrafttrailer_alfa'' as assettype,pcx_pawctrailercov_alfa.createtime_stg,

 EffectiveDate_stg,ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawctrailercov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''DirectTerm2'' as varchar(50)) as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(PAWatercraftTrailer_alfa_stg as varchar(255)) as assetkey,

 ''pawatercrafttrailer_alfa'' as assettype,pcx_pawctrailercov_alfa.createtime_stg,

 EffectiveDate_stg,ExpirationDate_stg , cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawctrailercov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm2Avl_stg=1 and ExpirationDate_stg is null 

 union 

 select cast(''Clause'' as varchar(50)) as columnname, cast(null as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchId,

 cast(PAWatercraftTrailer_alfa_stg as varchar(255)) as assetkey,

 ''pawatercrafttrailer_alfa'' as assettype,pcx_pawctrailercov_alfa.createtime_stg,

 EffectiveDate_stg,ExpirationDate_stg , cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid ,pcx_pawctrailercov_alfa.updatetime_stg 

 from DB_T_PROD_STAG.pcx_pawctrailercov_alfa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pcx_pawctrailercov_alfa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where DirectTerm1Avl_stg is null and DirectTerm2Avl_stg is null and ExpirationDate_stg is null

 /***********DB_T_PROD_STAG.pcx_pavehicleexclusion_alfa (Vehicle exclusions are at Clause level)***************/ ) polcov 

 left join ( select pc_etlclausepattern.PatternID_stg as clausePatternID, pc_etlcovtermpattern.PatternID_stg as covtermPatternID,

 pc_etlcovtermpattern.ColumnName_stg, pc_etlcovtermpattern.CovTermType_stg as CovTermType,

 pc_etlclausepattern.name_stg as clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern 

 join DB_T_PROD_STAG.pc_etlcovtermpattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg 

 union 

 select pc_etlclausepattern.PatternID_stg as clausePatternID, pc_etlcovtermpattern.PatternID_stg as covtermPatternID,

 coalesce(pc_etlcovtermpattern.ColumnName_stg,''Clause'') as columnname,

 coalesce(pc_etlcovtermpattern.CovTermType_stg,''Clause'') as covtermtype,

 pc_etlclausepattern.name_stg as clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern 

 left join ( select * from DB_T_PROD_STAG.pc_etlcovtermpattern where Name_stg not like ''ZZ%'') pc_etlcovtermpattern 

 on pc_etlcovtermpattern.ClausePatternID_stg=pc_etlclausepattern.ID_stg 

 where pc_etlclausepattern.Name_stg not like ''ZZ%'' and pc_etlcovtermpattern.Name_stg is null 

 and OwningEntityType_stg in (''HOLineSchCovItem_alfa'',''HomeownersLine_HOE'', ''Dwelling_HOE'', ''PersonalVehicle'', ''PersonalAutoLine'' ,''BP7BusinessOwnersLine'' ) ) covterm 

 on covterm.clausePatternID=polcov.PatternCode_stg and covterm.ColumnName_stg=polcov.columnname 

 inner join ( select cast(id_stg as varchar(255)) as id,PolicyNumber_stg,PeriodStart_stg,

 PeriodEnd_stg,MostRecentModel_stg,Status_stg,JobID_stg ,PublicID_stg, Createtime_stg ,updatetime_stg,BranchNumber_stg 

 from DB_T_PROD_STAG.PC_POLICYPERIOD ) PC_POLICYPERIOD on pc_policyperiod.id = polcov.BranchID 

 left outer join ( select pc_etlcovtermpackage.PatternID_stg as packagePatternID,

 pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name 

 from DB_T_PROD_STAG.pc_etlcovtermpackage ) package on package.packagePatternID=polcov.val 

 left outer join ( select pc_etlcovtermoption.PatternID_stg as optionPatternID, pc_etlcovtermoption.optioncode_stg as name,

 cast(pc_etlcovtermoption.value_stg as varchar(255)) as value1,

 pc_etlcovtermpattern.ValueType_stg as ValueType 

 from DB_T_PROD_STAG.pc_etlcovtermpattern 

 inner join DB_T_PROD_STAG.pc_etlcovtermoption on pc_etlcovtermpattern.id_stg=pc_etlcovtermoption.CoverageTermPatternID_stg ) optn on optn.optionPatternID=polcov.val 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg 

 inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg 

 inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg 

 where covterm.clausename not like''%ZZ%'' and pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

 and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary'' and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) ), 

 SET3 AS ( select pj.JobNumber_stg, pp.branchnumber_stg, 

 case when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg else polcov.EffectiveDate_stg end as startdate, 

 case when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg else polcov.ExpirationDate_stg end as enddate , 

 case when polcov.typ=''MODIFIER'' then pcm.PatternID_stg else pcetl.PatternID_stg end as nk_public_id , 

 case when polcov.typ=''EXCLUSION'' then ''CLAUSE'' else polcov.typ end as FEAT_SBTYPE_CD, '''' as feat_amt, cast(0 as varchar(50))as feat_qty,

 cast(0 as varchar(50)) as feat_num, pp.PublicID_stg, pp.createtime_stg, cast(polcov.feat_rate as varchar(50)) as feat_rate, polcov.updatetime_stg,

 cast(pcd.typecode_stg as varchar(50)) as feat_effect_type_cd, cast(null as varchar(64))as feat_val, cast(null as varchar(255)) as feat_CovTermType,

 (:start_dttm) as start_dttm,(:end_dttm) as end_dttm, cast(polcov.Eligible as varchar(10)) as Eligible 

 from ( /**************Modifiers*****************************/ 

 select cast(patterncode_stg as varchar(50)) as patterncode_stg ,

 cast(BranchID_stg as varchar(255)) as BranchID, ''EXCLUSION'' as typ,

 EffectiveDate_stg,ExpirationDate_stg,ppa.updatetime_stg,cast(NULL as integer) AS DiscountSurcharge_alfa_stg,

 cast(NULL as decimal(14,4))as feat_rate, cast(NULL as integer) as Eligible 

 from DB_T_PROD_STAG.pcx_pavehicleexclusion_alfa ppa 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = ppa.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ExpirationDate_stg is null 

 union 

 select cast(patterncode_stg as varchar(50)) as patterncode_stg,

 cast(BranchID_stg as varchar(255)) as BranchID,''MODIFIER'' as typ,

 EffectiveDate_stg,ExpirationDate_stg,phh.updatetime_stg,DiscountSurcharge_alfa_stg,

 case when phh.Eligible_stg=1 THEN phh.RateModifier_stg ELSE 0 end as feat_rate ,Eligible_stg 

 from DB_T_PROD_STAG.pcx_homodifier_hoe phh 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = phh.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ExpirationDate_stg is null 

 union 

 select cast(patterncode_stg as varchar(50)) as patterncode_stg,

 cast(BranchID_stg as varchar(255)) as BranchID,''MODIFIER'' as typ,

 EffectiveDate_stg,ExpirationDate_stg,pdh.updatetime_stg,DiscountSurcharge_alfa_stg,

 case when pdh.Eligible_stg=1 THEN pdh.RateModifier_stg ELSE 0 end as feat_rate ,Eligible_stg 

 from DB_T_PROD_STAG.pcx_dwellingmodifier_hoe pdh 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pdh.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ExpirationDate_stg is null 

 union 

 select cast(patterncode_stg as varchar(50)) as patterncode_stg,

 cast(BranchID_stg as varchar(255)) as BranchID,''MODIFIER'' as typ,

 EffectiveDate_stg,ExpirationDate_stg,pv.updatetime_stg,DiscountSurcharge_alfa_stg,

 case when pv.Eligible_stg=1 THEN pv.RateModifier_stg ELSE 0 end as feat_rate ,Eligible_stg 

 from DB_T_PROD_STAG.pc_pavehmodifier pv 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pv.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ExpirationDate_stg is null 

 union 

 select cast(patterncode_stg as varchar(50)) as patterncode_stg,

 cast(BranchID_stg as varchar(255)) as BranchID,''MODIFIER'' as typ,

 EffectiveDate_stg,ExpirationDate_stg,pm.updatetime_stg,DiscountSurcharge_alfa_stg,

 case when pm.Eligible_stg=1 THEN pm.RateModifier_stg ELSE 0 end as feat_rate ,Eligible_stg 

 from DB_T_PROD_STAG.pc_pamodifier pm 

 JOIN DB_T_PROD_STAG.PC_POLICYPERIOD on pc_policyperiod.id_stg = pm.BranchID_stg 

 and pc_policyperiod.updatetime_stg> (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm) 

 where ExpirationDate_stg is null

 /**********************Exclusions are at clauselevel******************/) polcov 

 left join DB_T_PROD_STAG.pc_etlmodifierpattern pcm on pcm.patternid_stg=polcov.patterncode_stg and polcov.typ=''MODIFIER'' 

 left join DB_T_PROD_STAG.pc_etlclausepattern pcetl on pcetl.patternid_stg=polcov.patterncode_stg and polcov.typ=''EXCLUSION'' 

 inner join ( select cast(id_stg as varchar(255)) as id,PolicyNumber_stg,PeriodStart_stg,

 PeriodEnd_stg,MostRecentModel_stg,Status_stg,JobID_stg,PublicID_stg, BranchNumber_stg,createtime_stg,updatetime_stg 

 from DB_T_PROD_STAG.PC_POLICYPERIOD ) pp on pp.id = polcov.BranchID 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.id_stg=pp.Status_stg 

 inner join DB_T_PROD_STAG.pc_job pj on pj.id_stg=pp.JobID_stg 

 inner join DB_T_PROD_STAG.pctl_job pcj on pcj.id_stg=pj.Subtype_stg 

 LEFT JOIN DB_T_PROD_STAG.pctl_discountsurcharge_alfa pcd ON polcov.DiscountSurcharge_alfa_stg=pcd.ID_stg 

 where (pcetl.Name_stg not like''%ZZ%'' or pcm.Name_stg not like''%ZZ%'' ) 

 and pcj.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') and pps.TYPECODE_stg <> ''Temporary'' 

 and pp.updatetime_stg> (:start_dttm) and pp.updatetime_stg <= (:end_dttm) ), 

 SET4 AS ( select distinct cast(jobnumber_stg as varchar(100))as JOBNUMBER_STG,pp.branchnumber_stg,  

/* -bp7line----  */
 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,  pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,  

 '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 pb.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,  

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible   

 from  DB_T_PROD_STAG.pcx_bp7linecov pb  

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pb.branchid_stg 

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg

 and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = pb.patterncode_stg  

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = pb.branchid_stg   

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = pb.patterncode_stg   

 where  (pb.EffectiveDate_stg is null OR (pb.EffectiveDate_stg > pp.ModelDate_stg and pb.EffectiveDate_stg <> pb.ExpirationDate_stg))  

 and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null 

 union   

/* ----homeownersline-----    */
 select distinct cast(jobnumber_stg as varchar(100))as ID, pp.branchnumber_stg,   

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

  case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,   

 pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,   '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 pho.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,   

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible   

  from DB_T_PROD_STAG.pcx_homeownerslinecov_hoe pho   

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp  on pp.id_stg = pho.branchid_stg   

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg

 and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = pho.patterncode_stg

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = pho.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = pho.patterncode_stg  

 where  (pho.EffectiveDate_stg is null or (pho.EffectiveDate_stg > pp.ModelDate_stg  and pho.EffectiveDate_stg <> pho.ExpirationDate_stg))   

 and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null  

 union   

/* -------personalautocov-------   */
 select distinct cast(jobnumber_stg as varchar(100))as ID, pp.branchnumber_stg,   

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

  case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,   

 pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,   '''' as feat_amt,cast(0 as varchar(50))as feat_qty, cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 ppa.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,   

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible   

 from DB_T_PROD_STAG.pc_personalautocov ppa  

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = ppa.branchid_stg 

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = ppa.patterncode_stg 

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = ppa.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = ppa.patterncode_stg  

 where  (ppa.EffectiveDate_stg is null or (ppa.EffectiveDate_stg > pp.ModelDate_stg  and ppa.EffectiveDate_stg <> ppa.ExpirationDate_stg))   

  and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null

   union   

/* -------PMOP-54882-PERSONALUMBRELLA CHANGES-------   */
 select distinct cast(jobnumber_stg as varchar(100))as ID, pp.branchnumber_stg,   

 case when pf.EffectiveDate_stg is null then pp.PeriodStart_stg else pf.EffectiveDate_stg end as startdate,   

  case when pf.ExpirationDate_stg is null then pp.PeriodEnd_stg else pf.ExpirationDate_stg   end as enddate ,   

 pf.Formpatterncode_stg as nk_public_id, ''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,   '''' as feat_amt,cast(0 as varchar(50))as feat_qty,

 cast(0 as varchar(50)) as feat_num,   

 pp.PublicID_stg,pp.createtime_stg,cast(cast(0 as decimal(20,4))as varchar(50)) as feat_rate,   

 ppa.updatetime_stg,cast(null as varchar(50)) as feat_effect_type_cd,   

 cast(null as varchar(64)) as feat_val,cast(null as varchar(255)) as feat_CovTermType,   

 ( :start_dttm) as start_dttm,(:end_dttm) as end_dttm,   

 cast(null as varchar(10)) as Eligible   

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov ppa  

 join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = ppa.branchid_stg 

 join DB_T_PROD_STAG.pc_job pj on pp.jobid_stg =pj.id_stg and pp.updatetime_stg> ( :start_dttm)  and pp.updatetime_stg <= (:end_dttm)  

 join DB_T_PROD_STAG.pc_formpattern pfp on pfp.clausepatterncode_stg = ppa.patterncode_stg 

 inner join DB_T_PROD_STAG.pctl_documenttype pd   on pd.id_stg = pfp.DocumentType_stg and pd.typecode_stg = ''endorsement_alfa'' 

 join DB_T_PROD_STAG.pc_form pf on pf.formpatterncode_stg = pfp.code_stg  and pf.branchid_stg = ppa.branchid_stg  

 join DB_T_PROD_STAG.pc_etlclausepattern pec  on pec.patternid_stg = ppa.patterncode_stg  

 where  (ppa.EffectiveDate_stg is null or (ppa.EffectiveDate_stg > pp.ModelDate_stg  and ppa.EffectiveDate_stg <> ppa.ExpirationDate_stg))   

  and pp.status_stg <> 2 and pf.RemovedorSuperseded_stg is null   ) ,

/* -------PMOP-54882-PERSONALUMBRELLA CHANGES Added new with clause-------  */
  SET5 as (select distinct pj.JobNumber_stg,pp.BranchNumber_stg,

 case when polcov.EffectiveDate_stg is null then pp.PeriodStart_stg else polcov.EffectiveDate_stg end startdate, 

 case when polcov.ExpirationDate_stg is null then pp.PeriodEnd_stg else polcov.ExpirationDate_stg end enddate, 

 case when covterm.CovTermType =''Package'' then package.packagePatternID when covterm.CovTermType=''Option'' and polcov.val is not null then optn.optionPatternID when covterm.CovTermType=''Clause'' then covterm.clausePatternID else covterm.covtermPatternID end as nk_public_id, 

 case when covterm.CovTermType=''Package'' then cast (''PACKAGE'' as varchar (50)) when covterm.CovTermType=''Option'' and polcov.val is not null then cast (''OPTIONS'' as varchar(50)) when covterm.CovTermType=''Clause'' then cast(''CLAUSE'' as varchar(50)) else cast ( ''COVTERM'' as varchar (50)) end as FEAT_SBTYPE_CD, 

 case when covterm.CovTermType = ''Direct'' then polcov.val else NULL end as feat_amt,

 case when optn.ValueType = ''count'' then optn.Value1 end feat_qty, 

 case when optn.ValueType in (''days'', ''hours'', ''other'') then optn.value1 end feat_num, 

 pp.PublicID_stg, pp.Createtime_stg, 

 case when optn.ValueType=''percent'' then optn.Value1 end feat_rate, 

 polcov.updatetime_stg, cast (null as varchar(50)) feat_effect_type_cd, polcov.val as feat_val,

 covterm.CovTermType feat_CovTermType, ( :start_dttm) start_dttm,

 ( :end_dttm) end_dttm, cast(NULL as varchar(10)) as Eligible 

 from ( 

 select CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg = 1 and ExpirationDate_stg is null

union 

 select ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where DirectTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union

 /*********EIM-49741 Added as part of farm***********/

 select ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where StringTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''Clause'' as columnname, cast(null as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinecov a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg= a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg is null AND DirectTerm1Avl_stg is null AND StringTerm1Avl_stg is null

union

/* DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl  */
select CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg = 1 and ExpirationDate_stg is null

 union

 select CAST(''ChoiceTerm2'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm2_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm2Avl_stg = 1 and ExpirationDate_stg is null

 union

 select CAST(''ChoiceTerm3'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm3_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm3Avl_stg = 1 and ExpirationDate_stg is null

 union

 select CAST(''ChoiceTerm4'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm4_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm4Avl_stg= 1 and ExpirationDate_stg is null

 union 

 select ''DirectTerm1'' as columnname, cast(DirectTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where DirectTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''DirectTerm2'' as columnname, cast(DirectTerm2_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where DirectTerm2Avl_stg = 1 and ExpirationDate_stg is null 

 union 

 select ''BooleanTerm1'' as columnname, cast(BooleanTerm1_stg as varchar(255)) as val,

 patterncode_stg,cast(BranchID_stg as varchar(255)) as BranchID,

 a.createtime_stg,EffectiveDate_stg,ExpirationDate_stg,

 cast(null as varchar(255)) as booleanterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where BooleanTerm1Avl_stg=1 and ExpirationDate_stg is null 

 union

 select ''DateTerm1'' as columnname, cast(DateTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, ChoiceTerm1_stg,

 pe.PatternID_stg as patternid, pha.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl pha 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pha.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern pe on pe.PatternID_stg = pha.PatternCode_stg 

 where DateTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 union

/**********EIM-49741 Added as part of farm************/

 select ''StringTerm1'' as columnname, cast(StringTerm1_stg as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 pha.createtime_stg, EffectiveDate_stg, ExpirationDate_stg, StringTerm1_stg,

 pe.PatternID_stg as patternid, pha.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl pha 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = pha.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 left join DB_T_PROD_STAG.pc_etlclausepattern pe on pe.PatternID_stg = pha.PatternCode_stg 

 where StringTerm1Avl_stg = 1 and ExpirationDate_stg is null 

 UNION

 select ''Clause'' as columnname, cast(null as varchar(255)) as val,

 patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg,

 ExpirationDate_stg, cast(null as varchar(255)) as choiceterm1,

 cast(null as varchar(255)) as patternid, a.updatetime_stg 

 from DB_T_PROD_STAG.pcx_puppersonalschexclitemexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg is null and ChoiceTerm2Avl_stg is null and ChoiceTerm3Avl_stg is null and ChoiceTerm4Avl_stg is null 

 and DirectTerm1Avl_stg is null and DirectTerm2_stg is null and BooleanTerm1Avl_stg is null and DateTerm1_stg is null and StringTerm1Avl_stg is null

 and ExpirationDate_stg is null 

union

/* - DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl   */
 /*********EIM-49741 Added as part of farm***********/

   select CAST(''ChoiceTerm1'' AS VARCHAR(250)) as columnname, cast(ChoiceTerm1_stg as varchar(255)) as val,

 cast(patterncode_stg as varchar(250)) patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

 a.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

 cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid, a.updatetime_stg

 from DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl a 

 inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp on pp.id_stg = a.BranchID_stg 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

 where ChoiceTerm1Avl_stg = 1 and ExpirationDate_stg is null

 

 union

 

  select ''Clause'' as columnname, cast(null as varchar(255)) as val,

            patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId,

             pcx_puppersonalumbrellalinexcl.createtime_stg, EffectiveDate_stg, ExpirationDate_stg,

            cast(null as varchar(255)) as choiceterm1, cast(null as varchar(255)) as patternid,

            pcx_puppersonalumbrellalinexcl.updatetime_stg

    from    DB_T_PROD_STAG.pcx_puppersonalumbrellalinexcl

    inner join DB_T_PROD_STAG.PC_POLICYPERIOD pp 

        on pp.id_stg = pcx_puppersonalumbrellalinexcl.BranchID_stg 

and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm) 

where ChoiceTerm1Avl_stg is null

) polcov 

 inner join ( 

 select cast(id_stg as varchar(255)) as id, PolicyNumber_stg, PeriodStart_stg,branchnumber_stg,

 PeriodEnd_stg, MostRecentModel_stg, Status_stg, JobID_stg, PublicID_stg,

 Createtime_stg, updatetime_stg, Retired_stg from db_t_prod_stag.pc_policyperiod) pp on pp.id = polcov.BranchID 

 left join ( select pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

 pcv.ColumnName_stg as columnname, pcv.CovTermType_stg as covtermtype, pcl.name_stg clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern pcl 

 join DB_T_PROD_STAG.pc_etlcovtermpattern pcv on pcl.id_stg = pcv.ClausePatternID_stg 

 union 

 select pcl.PatternID_stg clausePatternID, pcv.PatternID_stg covtermPatternID,

 coalesce(pcv.ColumnName_stg,''Clause'') columnname, coalesce(pcv.CovTermType_stg, ''Clause'') covtermtype, pcl.name_stg clausename 

 from DB_T_PROD_STAG.pc_etlclausepattern pcl 

 left join ( select * from DB_T_PROD_STAG.pc_etlcovtermpattern where Name_stg not like ''ZZ%'' ) pcv on pcv.ClausePatternID_stg = pcl.ID_stg 

 where pcl.Name_stg not like ''ZZ%'' and pcv.Name_stg is null ) covterm 

 on covterm.clausePatternID = polcov.PatternCode_stg and covterm.ColumnName = polcov.columnname 

 left outer join ( select pcv.PatternID_stg packagePatternID, pcv.PackageCode_stg cov_id, pcv.PackageCode_stg name1 from DB_T_PROD_STAG.pc_etlcovtermpackage pcv) package on package.packagePatternID = polcov.val 

 left outer join ( select pct.PatternID_stg optionPatternID, pct.optioncode_stg name1,

 cast(pct.value_stg as varchar(255)) as value1, pcv.ValueType_stg as ValueType from DB_T_PROD_STAG.pc_etlcovtermpattern pcv 

 inner join DB_T_PROD_STAG.pc_etlcovtermoption pct on pcv.id_stg = pct.CoverageTermPatternID_stg ) optn on optn.optionPatternID = polcov.val 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.id_stg = pp.Status_stg 

 inner join DB_T_PROD_STAG.pc_job pj on pj.id_stg = pp.JobID_stg 

 inner join DB_T_PROD_STAG.pctl_job pcj on pcj.id_stg = pj.Subtype_stg 

 where covterm.clausename not like''%ZZ%'' 

 and pp.updatetime_stg > ( :start_dttm) and pp.updatetime_stg <= ( :end_dttm)

 and pcj.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') and pps.TYPECODE_stg<>''Temporary'' )

Select * 

FROM ( 

 select QUOTATION_FEAT_lkp.QUOTN_ID AS LKP_QUOTN_ID, QUOTATION_FEAT_lkp.FEAT_ID AS LKP_FEAT_ID,

 QUOTATION_FEAT_lkp.QUOTN_FEAT_ROLE_CD AS LKP_AGMT_FEAT_ROLE_CD,

 QUOTATION_FEAT_lkp.QUOTN_FEAT_STRT_DTTM AS LKP_AGMT_FEAT_STRT_DT,

 XLAT_SRC.QUOTN_ID as QUOTN_ID, XLAT_SRC.FEAT_ID as FEAT_ID, XLAT_SRC.AGMT_FEAT_ROLE_CD,

 XLAT_SRC.AGMT_FEAT_STRT_DT, XLAT_SRC.AGMT_FEAT_END_DT, XLAT_SRC.OVRDN_FEAT_ID,

 XLAT_SRC.AGMT_FEAT_AMT, XLAT_SRC.AGMT_FEAT_RATE, XLAT_SRC.AGMT_FEAT_QTY,

 XLAT_SRC.AGMT_FEAT_NUM, XLAT_SRC.AGMT_FEAT_UOM_CD, XLAT_SRC.CURY_CD,

/* PMOP-54882  */
 :PRCS_ID AS PRCS_ID,

 XLAT_SRC.UOM_TYPE_CD, QUOTATION_FEAT_lkp.QUOTN_FEAT_END_DTTM AS LKP_AGMT_FEAT_END_DT,

 QUOTATION_FEAT_lkp.QUOTN_FEAT_AMT AS LKP_AGMT_FEAT_AMT, QUOTATION_FEAT_lkp.QUOTN_FEAT_RATE AS LKP_AGMT_FEAT_RATE,

 QUOTATION_FEAT_lkp.QUOTN_FEAT_QTY AS LKP_AGMT_FEAT_QTY, QUOTATION_FEAT_lkp.QUOTN_FEAT_NUM AS LKP_AGMT_FEAT_NUM,
 CAST(TRIM(CAST (XLAT_SRC.AGMT_FEAT_END_DT AS DATE )) || TRIM(CAST (XLAT_SRC.AGMT_FEAT_STRT_DT AS DATE )) || TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_ROLE_CD,0)) || TRIM(CAST(COALESCE(XLAT_SRC.AGMT_FEAT_AMT,0) AS DECIMAL(18,4))) || TRIM(CAST (COALESCE(XLAT_SRC.AGMT_FEAT_QTY,0) AS DECIMAL(18,4))) || TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_NUM,0)) || /*TRIM(COALESCE(XLAT_SRC.OVRDN_FEAT_ID,0)) ||*/ TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_UOM_CD,0)) || TRIM(COALESCE(XLAT_SRC.CURY_CD,0)) || TRIM(COALESCE(XLAT_SRC.UOM_TYPE_CD,0)) || TRIM(CAST(COALESCE(XLAT_SRC.AGMT_FEAT_RATE,0) AS DECIMAL(15,12))) || TRIM(COALESCE(XLAT_SRC.out_feat_effect_type_cd,0)) || TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_TXT,0)) ||TRIM(COALESCE(CAST(XLAT_SRC.AGMT_FEAT_DT AS VARCHAR(30)),0)) || TRIM(COALESCE(XLAT_SRC.AGMT_FEAT_IND,0)) || TRIM(COALESCE(XLAT_SRC.Eligible,0)) AS VARCHAR(1100)) AS SOURCE_DATA,

 CAST(TRIM(CAST (LKP_AGMT_FEAT_END_DT AS DATE )) || TRIM(CAST (LKP_AGMT_FEAT_STRT_DT AS DATE )) || TRIM(COALESCE(LKP_AGMT_FEAT_ROLE_CD,0)) || TRIM(COALESCE(LKP_AGMT_FEAT_AMT,0)) || TRIM(COALESCE(LKP_AGMT_FEAT_QTY,0)) || TRIM(COALESCE(LKP_AGMT_FEAT_NUM,0)) || /*TRIM(COALESCE(QUOTATION_FEAT_lkp.OVRDN_FEAT_ID,0)) ||*/ TRIM(COALESCE(QUOTATION_FEAT_lkp.QUOTN_FEAT_UOM_CD,0)) || TRIM(COALESCE(QUOTATION_FEAT_lkp.CURY_CD,0)) || TRIM(COALESCE(QUOTATION_FEAT_lkp.UOM_TYPE_CD,0)) || TRIM(COALESCE(LKP_AGMT_FEAT_RATE,0)) || TRIM(COALESCE(QUOTATION_FEAT_lkp.FEAT_EFECT_TYPE_CD,0)) || TRIM(COALESCE(NULLIF(QUOTATION_FEAT_lkp.QUOTN_FEAT_TXT,

 ''''),0)) || TRIM(COALESCE(CAST(QUOTATION_FEAT_lkp.QUOTN_FEAT_DT AS VARCHAR(30)),0)) || TRIM(COALESCE(NULLIF(QUOTATION_FEAT_lkp.QUOTN_FEAT_IND,

 ''''),0)) || TRIM(COALESCE(QUOTATION_FEAT_lkp.FEAT_ELGBL_IND,0)) AS VARCHAR(1100)) AS TARGET_DATA,

 case  when QUOTATION_FEAT_lkp.QUOTN_ID IS NULL then ''I'' 

 when QUOTATION_FEAT_lkp.QUOTN_ID IS NOT NULL  AND QUOTATION_FEAT_lkp. FEAT_ID IS NOT NULL  AND SOURCE_DATA <> TARGET_DATA THEN ''U'' 

 when QUOTATION_FEAT_lkp.QUOTN_ID IS NOT NULL  AND QUOTATION_FEAT_lkp. FEAT_ID IS NOT NULL  AND SOURCE_DATA = TARGET_DATA then ''R''  end as ins_upd_flag, QUOTATION_FEAT_lkp.EDW_STRT_DTTM AS EDW_STRT_DTTM_upd,

 XLAT_SRC.TRANS_STRT_DTTM, XLAT_SRC.out_feat_effect_type_cd, XLAT_SRC.AGMT_FEAT_TXT, XLAT_SRC.AGMT_FEAT_DT, XLAT_SRC.AGMT_FEAT_IND, XLAT_SRC.Eligible 

 from (  SELECT INSRNC_QUOTN_lkp.QUOTN_ID, FEAT_lkp.FEAT_ID, SRC.JOBNUMBER, FEAT_lkp.feat_sbtype_cd, 

 CAST( SRC.BRANCHNUMBER AS INTEGER) AS BRANCHNUMBER, CAST (COALESCE(TRIM(CAST(SRC.STARTDATE AS VARCHAR(10))),

 ''1900-01-01'') AS DATE ) AS AGMT_FEAT_STRT_DT,

 CAST (COALESCE(TRIM(CAST(SRC.ENDDATE AS VARCHAR(30))),:end_dttm ) AS TIMESTAMP  ) AS AGMT_FEAT_END_DT,

 SRC.NK_PUBLIC_ID, 

 CASE  When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''direct'' THEN CAST(SRC.FEAT_VAL AS DECIMAL(18,4))  Else NULL  End AS AGMT_FEAT_AMT, 

 SRC.FEAT_QTY AS AGMT_FEAT_QTY, SRC.FEAT_NUM AS AGMT_FEAT_NUM,

 SRC.PUBLICID AS PUBLIC_ID, CAST(SRC.FEAT_RATE AS DECIMAL(15,12)) AS AGMT_FEAT_RATE,

 COALESCE(SRC.UPDTAETIME ,CAST(''01/01/1900'' AS DATE  )) AS TRANS_STRT_DTTM,

 SRC.SYS_SRC_CD AS SRC_CD, SRC.FEAT_VAL, SRC.FEAT_COVTERMTYPE,

 SRC.ELIGIBLE, ''QUOTE'' AS AGMT_FEAT_ROLE_CD, ''UNK'' AS AGMT_FEAT_UOM_CD,

 ''UNK'' AS CURY_CD, ''UNK'' AS UOM_TYPE_CD, CAST (NULL AS BIGINT) AS OVRDN_FEAT_ID,

 COALESCE(XLAT_FEAT_EFFECT_TYPE_CD.TGT_IDNTFTN_VAL,''UNK'') AS out_feat_effect_type_cd,

 CASE  When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) IN ( ''shorttext'' ,''Typekey'') THEN CAST(SRC.FEAT_VAL AS VARCHAR(250))  End AS AGMT_FEAT_TXT, 

 CASE  When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) =''datetime''  AND TRIM(FEAT_VAL) IS NOT NULL THEN CAST(SUBSTR(TRIM(COALESCE(SRC.FEAT_VAL, ''1900-01-01'')),1,10) AS DATE  )  ELSE NULL  End AS AGMT_FEAT_DT, 

 CASE  When (LOWER(TRIM(SRC.FEAT_COVTERMTYPE))) = ''bit'' THEN TRIM(SRC.FEAT_VAL)  End AS AGMT_FEAT_IND 

 FROM (  select jobnumber_stg as JOBNUMBER, cast(BranchNumber_stg as varchar(255)) as BRANCHNUMBER,

 STARTDATE, cast(:end_dttm AS TIMESTAMP(6)) as ENDDATE, NK_PUBLIC_ID, FEAT_SBTYPE_CD, 

 cast((case  when a.feat_amt like ''..%'' then a.feat_amt 

 when (a.feat_amt like ''.%a%''  or a.feat_amt like ''.%e%''  or a.feat_amt like ''.%i%''  or a.feat_amt like ''.%o%''  or a.feat_val like ''.%u%'') then a.feat_amt 

 when a.feat_amt like ''.%'' then ''0''||a.feat_amt  else a.feat_amt end) as varchar(60)) as FEAT_AMT, FEAT_QTY,

 cast((  case  when a.FEAT_NUM like ''..%'' then a.FEAT_NUM  when a.FEAT_NUM like ''.%'' then ''0''||a.FEAT_NUM  else a.FEAT_NUM end) as varchar(60)) as FEAT_NUM, 

 PublicID_stg as PUBLICID, FEAT_RATE, updatetime_stg as UPDTAETIME, ''SRC_SYS4'' AS SYS_SRC_CD,

 feat_effect_type_cd, cast((  case  when a.feat_val like ''..%'' then a.feat_val 

 when (a.feat_val like ''.%a%''  or a.feat_val like ''.%e%''  or a.feat_val like ''.%i%''  or a.feat_val like ''.%o%''  or a.feat_val like ''.%u%'') then a.feat_val 

 when a.feat_val like ''.%'' then ''0''||a.feat_val  else a.feat_val end) as varchar(255)) as feat_val, feat_covtermtype ,

 case  when cast(Eligible as varchar(10))=''0'' then ''F'' 

 WHEN cast(Eligible as varchar(10))=''1'' THEN ''T''  ELSE NULL  END AS Eligible 

 FROM (  Select *  from SET1 

 union all 

 Select *  from SET2 /*Fix for Defect:13484*/ 

 union all 

 Select *  from SET3 

 union all 

 Select *  from SET4 

/* -------PMOP-54882- Added Union to bring in PERSONALUMBRELLA CHANGES-------  */
 union all 

  Select *  from SET5

  /* EIM-48793 - QUTON_FEAT - FARM CHANGES BEGINS*/

  union all 

  Select *  from FARM_TEMP

  ) as a 

 qualify ROW_NUMBER() OVER(  partition by JOBNUMBER_stg,BRANCHNUMBER_stg,FEAT_SBTYPE_CD, NK_PUBLIC_ID  order by updatetime_stg desc) =1 ) AS SRC 

 LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_SRC_CD 

 ON XLAT_SRC_CD.SRC_IDNTFTN_VAL = SRC.SYS_SRC_CD  AND XLAT_SRC_CD.TGT_IDNTFTN_NM= ''SRC_SYS'' 

 AND XLAT_SRC_CD.SRC_IDNTFTN_NM= ''derived''  AND XLAT_SRC_CD.SRC_IDNTFTN_SYS in (''DS'' )  AND XLAT_SRC_CD.EXPN_DT=''9999-12-31'' 

 LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_FEAT_EFFECT_TYPE_CD 

 ON XLAT_FEAT_EFFECT_TYPE_CD.SRC_IDNTFTN_VAL = SRC.FEAT_EFFECT_TYPE_CD 

 AND XLAT_FEAT_EFFECT_TYPE_CD.TGT_IDNTFTN_NM= ''FEAT_EFECT_TYPE'' 

 AND XLAT_FEAT_EFFECT_TYPE_CD.SRC_IDNTFTN_NM= ''pctl_discountsurcharge_alfa.typecode'' 

 AND XLAT_FEAT_EFFECT_TYPE_CD.SRC_IDNTFTN_SYS=''GW'' 

 AND XLAT_FEAT_EFFECT_TYPE_CD.EXPN_DT=''9999-12-31'' 

 LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_FEAT_SBTYPE_CD 

 ON XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_VAL = (  Case  When SRC.FEAT_SBTYPE_CD = ''MODIFIER'' THEN ''FEAT_SBTYPE11'' 

 When SRC.FEAT_SBTYPE_CD = ''OPTIONS'' THEN ''FEAT_SBTYPE8'' 

 When SRC.FEAT_SBTYPE_CD = ''COVTERM'' THEN ''FEAT_SBTYPE6'' 

 When SRC.FEAT_SBTYPE_CD = ''CLAUSE'' THEN ''FEAT_SBTYPE7'' 

 When SRC.FEAT_SBTYPE_CD = ''PACKAGE'' THEN ''FEAT_SBTYPE9'' 

 When SRC.FEAT_SBTYPE_CD = ''CL'' THEN ''FEAT_SBTYPE7'' 

 When SRC.FEAT_SBTYPE_CD = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15'' End) 

 AND XLAT_FEAT_SBTYPE_CD.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

 AND XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_NM= ''derived'' 

 AND XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_SYS in (''DS'' ) 

 AND XLAT_FEAT_SBTYPE_CD.EXPN_DT=''9999-12-31'' 

 LEFT OUTER JOIN (  SELECT INSRNC_QUOTN.QUOTN_ID as QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR as NK_JOB_NBR,

 INSRNC_QUOTN.VERS_NBR as VERS_NBR, INSRNC_QUOTN.SRC_SYS_CD as SRC_SYS_CD 

 FROM DB_T_PROD_CORE.INSRNC_QUOTN 

 QUALIFY ROW_NUMBER () OVER (  PARTITION BY NK_JOB_NBR,VERS_NBR  ORDER BY edw_end_dttm DESC)=1) AS INSRNC_QUOTN_LKP 

 ON INSRNC_QUOTN_lkp.NK_JOB_NBR = SRC.JOBNUMBER 

 AND INSRNC_QUOTN_lkp.VERS_NBR= CAST( SRC.BRANCHNUMBER AS INTEGER) 

 AND INSRNC_QUOTN_lkp.SRC_SYS_CD=''GWPC'' 

 LEFT OUTER JOIN (  SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY 

 FROM DB_T_PROD_CORE.FEAT 

 QUALIFY ROW_NUMBER () OVER (  PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  ORDER BY edw_end_dttm DESC)=1) as FEAT_LKP 

 ON FEAT_lkp. FEAT_SBTYPE_CD =COALESCE( XLAT_FEAT_SBTYPE_CD .TGT_IDNTFTN_VAL, ''UNK'') 

 AND FEAT_lkp.NK_SRC_KEY = SRC.NK_PUBLIC_ID )AS XLAT_SRC 

 LEFT OUTER JOIN (  SELECT QUOTN_FEAT.QUOTN_FEAT_STRT_DTTM as QUOTN_FEAT_STRT_DTTM,

 QUOTN_FEAT.QUOTN_FEAT_END_DTTM as QUOTN_FEAT_END_DTTM, QUOTN_FEAT.QUOTN_FEAT_UOM_CD as QUOTN_FEAT_UOM_CD,

 QUOTN_FEAT.QUOTN_FEAT_AMT as QUOTN_FEAT_AMT, QUOTN_FEAT.QUOTN_FEAT_RATE as QUOTN_FEAT_RATE,

 QUOTN_FEAT.QUOTN_FEAT_QTY as QUOTN_FEAT_QTY, QUOTN_FEAT.QUOTN_FEAT_NUM as QUOTN_FEAT_NUM,

 QUOTN_FEAT.CURY_CD as CURY_CD, QUOTN_FEAT.UOM_TYPE_CD as UOM_TYPE_CD,

 QUOTN_FEAT.FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD, QUOTN_FEAT.QUOTN_FEAT_TXT as QUOTN_FEAT_TXT,

 QUOTN_FEAT.QUOTN_FEAT_DT as QUOTN_FEAT_DT, QUOTN_FEAT.QUOTN_FEAT_IND as QUOTN_FEAT_IND,

 QUOTN_FEAT.FEAT_ELGBL_IND as FEAT_ELGBL_IND, QUOTN_FEAT.EDW_STRT_DTTM as EDW_STRT_DTTM,

 QUOTN_FEAT.QUOTN_ID as QUOTN_ID, QUOTN_FEAT.FEAT_ID as FEAT_ID,

 QUOTN_FEAT. QUOTN_FEAT_ROLE_CD as QUOTN_FEAT_ROLE_CD 

 FROM DB_T_PROD_CORE.QUOTN_FEAT 

 QUALIFY ROW_NUMBER() OVER(  PARTITION BY QUOTN_ID, FEAT_ID, QUOTN_FEAT_ROLE_CD  ORDER BY EDW_END_DTTM desc) = 1 ) QUOTATION_FEAT_LKP 

 ON QUOTATION_FEAT_lkp.QUOTN_ID = XLAT_SRC.QUOTN_ID 

 AND QUOTATION_FEAT_lkp. FEAT_ID = XLAT_SRC.FEAT_ID 

 AND QUOTATION_FEAT_lkp.QUOTN_FEAT_ROLE_CD =XLAT_SRC.AGMT_FEAT_ROLE_CD ) As Master 

Where Master.INS_UPD_FLAG in (''I'',''U'') and quotn_id is not null and feat_id is not null
) SRC
)
);


-- PIPELINE START FOR 1

-- Component SQ_pc_quotation_feat_x1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_quotation_feat_x1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JOBNUMBER,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct jobnumber_stg from DB_T_PROD_STAG.pc_job where 1=2
) SRC
)
);


-- Component exp_insupd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insupd AS
(
SELECT
SQ_pc_quotation_feat_x.LKP_QUOTN_ID as LKP_QUOTN_ID,
SQ_pc_quotation_feat_x.LKP_FEAT_ID as LKP_FEAT_ID,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_ROLE_CD as LKP_AGMT_FEAT_ROLE_CD,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_STRT_DT as LKP_AGMT_FEAT_STRT_DT,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_END_DT as LKP_AGMT_FEAT_END_DT,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_AMT as LKP_AGMT_FEAT_AMT,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_RATE as LKP_AGMT_FEAT_RATE,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_QTY as LKP_AGMT_FEAT_QTY,
SQ_pc_quotation_feat_x.LKP_AGMT_FEAT_NUM as LKP_AGMT_FEAT_NUM,
SQ_pc_quotation_feat_x.EDW_STRT_DTTM_upd as LKP_EDW_STRT_DTTM,
SQ_pc_quotation_feat_x.QUOTN_ID as QUOTN_ID,
SQ_pc_quotation_feat_x.FEAT_ID as FEAT_ID,
SQ_pc_quotation_feat_x.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
SQ_pc_quotation_feat_x.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
SQ_pc_quotation_feat_x.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
SQ_pc_quotation_feat_x.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
SQ_pc_quotation_feat_x.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
SQ_pc_quotation_feat_x.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
SQ_pc_quotation_feat_x.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
SQ_pc_quotation_feat_x.AGMT_FEAT_UOM_CD as QUOTN_FEAT_UOM_CD,
SQ_pc_quotation_feat_x.CURY_CD as CURY_CD,
SQ_pc_quotation_feat_x.PRCS_ID as out_PRCS_ID,
SQ_pc_quotation_feat_x.UOM_TYPE_CD as UOM_TYPE_CD,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
SQ_pc_quotation_feat_x.out_feat_effect_type_cd as out_feat_effect_type_cd,
SQ_pc_quotation_feat_x.AGMT_FEAT_TXT as AGMT_FEAT_TXT,
SQ_pc_quotation_feat_x.AGMT_FEAT_DT as AGMT_FEAT_DT,
SQ_pc_quotation_feat_x.AGMT_FEAT_IND as AGMT_FEAT_IND,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
SQ_pc_quotation_feat_x.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
SQ_pc_quotation_feat_x.Eligible as Eligible,
SQ_pc_quotation_feat_x.ins_upd_flag as out_ins_upd,
SQ_pc_quotation_feat_x.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
SQ_pc_quotation_feat_x.source_record_id
FROM
SQ_pc_quotation_feat_x
);


-- Component QUOTN_FEAT_ins_new1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_FEAT
(
QUOTN_ID
)
SELECT
SQ_pc_quotation_feat_x1.JOBNUMBER as QUOTN_ID
FROM
SQ_pc_quotation_feat_x1;


-- PIPELINE END FOR 1
-- Component QUOTN_FEAT_ins_new1, Type Post SQL 
UPDATE db_t_prod_core.QUOTN_FEAT 
set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

FROM

(SELECT	distinct QUOTN_ID,FEAT_ID,QUOTN_FEAT_ROLE_CD,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,QUOTN_FEAT_ROLE_CD ORDER by EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,QUOTN_FEAT_ROLE_CD ORDER by EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM	db_t_prod_core.QUOTN_FEAT

 ) a

where  QUOTN_FEAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_FEAT.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_FEAT.FEAT_ID=A.FEAT_ID

AND QUOTN_FEAT.QUOTN_FEAT_ROLE_CD=A.QUOTN_FEAT_ROLE_CD

and lead1 is not null;


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
SELECT
exp_insupd.LKP_QUOTN_ID as LKP_AGMT_ID,
exp_insupd.LKP_FEAT_ID as LKP_FEAT_ID,
exp_insupd.LKP_AGMT_FEAT_ROLE_CD as LKP_AGMT_FEAT_ROLE_CD,
exp_insupd.LKP_AGMT_FEAT_STRT_DT as LKP_AGMT_FEAT_STRT_DT,
exp_insupd.QUOTN_ID as AGMT_ID,
exp_insupd.FEAT_ID as FEAT_ID,
exp_insupd.AGMT_FEAT_ROLE_CD as AGMT_FEAT_ROLE_CD,
exp_insupd.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_insupd.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
exp_insupd.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_insupd.AGMT_FEAT_AMT as AGMT_FEAT_AMT,
exp_insupd.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
exp_insupd.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
exp_insupd.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
exp_insupd.QUOTN_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
exp_insupd.CURY_CD as CURY_CD,
exp_insupd.out_PRCS_ID as PRCS_ID,
exp_insupd.UOM_TYPE_CD as UOM_TYPE_CD,
exp_insupd.LKP_AGMT_FEAT_END_DT as LKP_AGMT_FEAT_END_DT,
exp_insupd.LKP_AGMT_FEAT_AMT as LKP_AGMT_FEAT_AMT,
exp_insupd.LKP_AGMT_FEAT_RATE as LKP_AGMT_FEAT_RATE,
exp_insupd.LKP_AGMT_FEAT_QTY as LKP_AGMT_FEAT_QTY,
exp_insupd.LKP_AGMT_FEAT_NUM as LKP_AGMT_FEAT_NUM,
exp_insupd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_insupd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_insupd.out_ins_upd as out_ins_upd,
exp_insupd.LKP_EDW_STRT_DTTM as EDW_STRT_DTTM_upd,
NULL as out_VAL_TYP_CD,
exp_insupd.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_insupd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_insupd.out_feat_effect_type_cd as out_feat_effect_type_cd,
exp_insupd.AGMT_FEAT_TXT as AGMT_FEAT_TXT,
exp_insupd.AGMT_FEAT_DT as AGMT_FEAT_DT,
exp_insupd.AGMT_FEAT_IND as AGMT_FEAT_IND,
exp_insupd.Eligible as Eligible,
exp_insupd.source_record_id
FROM
exp_insupd
WHERE ( exp_insupd.out_ins_upd = ''I'' OR exp_insupd.out_ins_upd = ''U'' ) AND exp_insupd.QUOTN_ID IS NOT NULL and exp_insupd.FEAT_ID IS NOT NULL;


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
rtr_ins_upd_INSERT.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
rtr_ins_upd_INSERT.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
rtr_ins_upd_INSERT.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
rtr_ins_upd_INSERT.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
rtr_ins_upd_INSERT.CURY_CD as CURY_CD,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd_INSERT.UOM_TYPE_CD as UOM_TYPE_CD,
rtr_ins_upd_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
rtr_ins_upd_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_ins_upd_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_ins_upd_INSERT.TRANS_END_DTTM as TRANS_END_DTTM11,
rtr_ins_upd_INSERT.out_feat_effect_type_cd as out_feat_effect_type_cd1,
rtr_ins_upd_INSERT.AGMT_FEAT_TXT as AGMT_FEAT_TXT1,
rtr_ins_upd_INSERT.AGMT_FEAT_DT as AGMT_FEAT_DT,
rtr_ins_upd_INSERT.AGMT_FEAT_IND as AGMT_FEAT_IND1,
rtr_ins_upd_INSERT.Eligible as Eligible1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_INSERT.source_record_id
FROM
rtr_ins_upd_INSERT
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
upd_stg_ins.AGMT_FEAT_RATE as AGMT_FEAT_RATE,
upd_stg_ins.AGMT_FEAT_QTY as AGMT_FEAT_QTY,
upd_stg_ins.AGMT_FEAT_NUM as AGMT_FEAT_NUM,
upd_stg_ins.AGMT_FEAT_UOM_CD as AGMT_FEAT_UOM_CD,
upd_stg_ins.CURY_CD as CURY_CD,
upd_stg_ins.PRCS_ID as PRCS_ID,
upd_stg_ins.UOM_TYPE_CD as UOM_TYPE_CD,
upd_stg_ins.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM1,
upd_stg_ins.out_EDW_END_DTTM1 as out_EDW_END_DTTM1,
upd_stg_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_stg_ins.out_feat_effect_type_cd1 as out_feat_effect_type_cd1,
upd_stg_ins.AGMT_FEAT_TXT1 as AGMT_FEAT_TXT1,
upd_stg_ins.AGMT_FEAT_DT as AGMT_FEAT_DT,
upd_stg_ins.AGMT_FEAT_IND1 as AGMT_FEAT_IND1,
upd_stg_ins.Eligible1 as Eligible1,
upd_stg_ins.source_record_id
FROM
upd_stg_ins
);


-- Component QUOTN_FEAT_ins_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_FEAT
(
QUOTN_ID,
FEAT_ID,
QUOTN_FEAT_ROLE_CD,
QUOTN_FEAT_STRT_DTTM,
QUOTN_FEAT_END_DTTM,
OVRDN_FEAT_ID,
QUOTN_FEAT_UOM_CD,
QUOTN_FEAT_AMT,
QUOTN_FEAT_RATE,
QUOTN_FEAT_QTY,
QUOTN_FEAT_NUM,
CURY_CD,
UOM_TYPE_CD,
FEAT_EFECT_TYPE_CD,
QUOTN_FEAT_TXT,
QUOTN_FEAT_DT,
QUOTN_FEAT_IND,
FEAT_ELGBL_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_target_ins.AGMT_ID as QUOTN_ID,
exp_pass_to_target_ins.FEAT_ID as FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_ROLE_CD as QUOTN_FEAT_ROLE_CD,
exp_pass_to_target_ins.AGMT_FEAT_STRT_DT as QUOTN_FEAT_STRT_DTTM,
exp_pass_to_target_ins.AGMT_FEAT_END_DT as QUOTN_FEAT_END_DTTM,
exp_pass_to_target_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_UOM_CD as QUOTN_FEAT_UOM_CD,
exp_pass_to_target_ins.AGMT_FEAT_AMT as QUOTN_FEAT_AMT,
exp_pass_to_target_ins.AGMT_FEAT_RATE as QUOTN_FEAT_RATE,
exp_pass_to_target_ins.AGMT_FEAT_QTY as QUOTN_FEAT_QTY,
exp_pass_to_target_ins.AGMT_FEAT_NUM as QUOTN_FEAT_NUM,
exp_pass_to_target_ins.CURY_CD as CURY_CD,
exp_pass_to_target_ins.UOM_TYPE_CD as UOM_TYPE_CD,
exp_pass_to_target_ins.out_feat_effect_type_cd1 as FEAT_EFECT_TYPE_CD,
exp_pass_to_target_ins.AGMT_FEAT_TXT1 as QUOTN_FEAT_TXT,
exp_pass_to_target_ins.AGMT_FEAT_DT as QUOTN_FEAT_DT,
exp_pass_to_target_ins.AGMT_FEAT_IND1 as QUOTN_FEAT_IND,
exp_pass_to_target_ins.Eligible1 as FEAT_ELGBL_IND,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM
FROM
exp_pass_to_target_ins;


-- PIPELINE END FOR 2

END; ';