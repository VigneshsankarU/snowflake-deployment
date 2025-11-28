-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_CLM_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_agmt_clm, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_agmt_clm AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PUBLIC_ID,
$2 as CLAIM_NUMBER,
$3 as AGMT_CLM_RLTNSHP_TYPE,
$4 as AGMT_CLM_RLTNSHP_STRT_DT,
$5 as AGMT_CLM_RLTNSHP_END_DT,
$6 as CLM_SRC_CD,
$7 as AGMT_SRC_CD,
$8 as UPDATETIME,
$9 as RETIRED,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

DISTINCT

CAST (AGMT_CLM.publicid AS VARCHAR (64)) AS PUBLIC_ID,

AGMT_CLM.claimnumber,

CAST (AGMT_CLM_RLTNSHP_TYPE AS VARCHAR (60)) AS AGMT_CLM_RLTNSHP_TYPE,

(CAST (AGMT_CLM.AGMT_CLM_RLTNSHP_STRT_DT AS VARCHAR (60))) AS AGMT_CLM_RLTNSHP_STRT_DT,

(CAST (AGMT_CLM.AGMT_CLM_RLTNSHP_END_DT AS VARCHAR (60))) AS AGMT_CLM_RLTNSHP_END_DT,

AGMT_CLM.CLM_SRC_CD, 

AGMT_CLM.AGMT_SRC_CD,

AGMT_CLM.UPDATETIME,

AGMT_CLM.RETIRED

FROM(select distinct case when clm.ver_unver=''verified'' then pp.PublicID_stg else cast(clm.PolicySystemPeriodID as varchar(64)) end as publicid,

clm.claimnumber,

AGMT_CLM_RLTNSHP_TYPE,

case when clm.createtime is null then cast(''1900-01-01 00:00:00.000000'' as timestamp(6)) else clm.createtime end as AGMT_CLM_RLTNSHP_STRT_DT,

case when clm.closedate is null then cast(''9999-12-31 00:00:00.000000'' as timestamp(6)) else clm.closedate end as  AGMT_CLM_RLTNSHP_END_DT,

''SRC_SYS6'' as CLM_SRC_CD, 

''SRC_SYS4'' as AGMT_SRC_CD,

case when clm.updateime is null then cast(''1900-01-01 00:00:00.000000'' as timestamp(6)) else clm.updateime end as updatetime,

clm.retired,PeriodStart_stg

from (

select  distinct cc_claim.ClaimNumber_stg as ClaimNumber, 

case when (cc_policy.UpdateTime_stg> cc_claim.UpdateTime_stg) then cc_policy.UpdateTime_stg else cc_claim.updatetime_stg end updateime,

cc_policy.PolicySystemPeriodID_stg as PolicySystemPeriodID,

cc_claim.lossdate_stg as lossdate,

cc_claim.createtime_stg as createtime,

cc_claim.CloseDate_stg as closedate,

case when cc_claim.Retired_stg=0 and cc_policy.Retired_stg=0 then 0 else 1 end as retired,

''PLCYCLM'' as AGMT_CLM_RLTNSHP_TYPE,

''verified'' as ver_unver,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from (select cc_claim.* 
from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate 
on cc_claim.State_stg= cctl_claimstate.id_stg 
where cctl_claimstate.name_stg <> ''Draft'') cc_claim, DB_T_PROD_STAG.cc_policy

where cc_claim.PolicyID_stg=cc_policy.ID_stg

and cc_claim.ClaimNumber_stg is not null

and cc_policy.PolicyNumber_stg is not null 

and cc_policy.PolicySystemPeriodID_stg is not null 

and cc_policy.verified_stg = 1

and(( cc_claim.UpdateTime_stg > (:start_dttm) AND cc_claim.UpdateTime_stg <= (:end_dttm) )

or ( cc_policy.UpdateTime_stg > (:start_dttm) AND cc_policy.UpdateTime_stg <= (:end_dttm)))



union 



select distinct cc_claim.ClaimNumber_stg as ClaimNumber, 

case when (cc_policy.UpdateTime_stg> cc_claim.UpdateTime_stg) then cc_policy.UpdateTime_stg else cc_claim.UpdateTime_stg end updateime,

cc_policy.id_stg as PolicySystemPeriodID,

cc_claim.lossdate_stg as lossdate,

cc_claim.createtime_stg as createtime,

cc_claim.CloseDate_stg as closedate,

case when cc_claim.Retired_stg=0 and cc_policy.Retired_stg=0 then 0 else 1 end as retired,

''PLCYCLM'' as AGMT_CLM_RLTNSHP_TYPE,

''unverified'' as ver_unver,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

  from DB_T_PROD_STAG.cc_policy  
  inner join (select cc_claim.* 
  from DB_T_PROD_STAG.cc_claim 
  inner join DB_T_PROD_STAG.cctl_claimstate on 
  cc_claim.State_stg= cctl_claimstate.id_stg 
  where cctl_claimstate.name_stg <> ''Draft'') cc_claim on 
  cc_policy.id_stg=cc_claim.PolicyID_stg

where (cc_policy.verified_stg = 0  and coalesce(cc_policy.legacypolind_alfa_stg,0)<>1)and(( cc_claim.UpdateTime_stg > (:start_dttm) AND cc_claim.UpdateTime_stg <= (:end_dttm) )

or ( cc_policy.UpdateTime_stg > (:start_dttm) AND cc_policy.UpdateTime_stg <= (:end_dttm)))



union



select distinct cc_claim.ClaimNumber_stg as ClaimNumber, 

case when (cc_policy.UpdateTime_stg> cc_claim.UpdateTime_stg) then cc_policy.UpdateTime_stg else cc_claim.UpdateTime_stg end updateime,

cc_policy.id_stg as PolicySystemPeriodID,

cc_claim.lossdate_stg as lossdate,

cc_claim.createtime_stg as createtime,

cc_claim.CloseDate_stg as closedate,

case when cc_claim.Retired_stg=0 and cc_policy.Retired_stg=0 then 0 else 1 end as retired,

''PLCYCLM'' as AGMT_CLM_RLTNSHP_TYPE,

''unverified'' as ver_unver,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

 

 from DB_T_PROD_STAG.cc_policy  
 inner join (select cc_claim.* 
 from DB_T_PROD_STAG.cc_claim 
 inner join DB_T_PROD_STAG.cctl_claimstate on 
 cc_claim.State_stg= cctl_claimstate.id_stg 
 where cctl_claimstate.name_stg <> ''Draft'') cc_claim on 
 cc_policy.id_stg=cc_claim.PolicyID_stg

where coalesce(cc_policy.legacypolind_alfa_stg,0)=1  and(( cc_claim.UpdateTime_stg > (:start_dttm) AND cc_claim.UpdateTime_stg <= (:end_dttm) )

or ( cc_policy.UpdateTime_stg > (:start_dttm) AND cc_policy.UpdateTime_stg <= (:end_dttm)))) as clm

left join (select Publicid_stg as PublicID_stg,PeriodID_stg as ID_stg,updatetime_stg as updatetime_stg,EditEffectiveDate_stg as periodstart_stg, 

case when (MODEL_END_DATE_stg is null and CancellationDate_stg is not  null) then CancellationDate_stg 

 when (MODEL_END_DATE_stg is null and CancellationDate_stg is   null) then periodend_stg 

else MODEL_END_DATE_stg end  as  periodend_stg 

FROM 

(SELECT POLICYNUMBER_stg, PeriodID_stg,EditEffectiveDate_stg,termnumber_stg,modelnumber_stg,publicid_stg,PeriodEnd_stg,CancellationDate_stg,Updatetime_stg,

LEAD(EditEffectiveDate_stg,1, NULL) OVER (PARTITION BY POLICYNUMBER_stg,TERMNUMBER_stg,PERIODID_stg ORDER BY MODELNUMBER_stg) MODEL_END_DATE_stg,

ROW_NUMBER() OVER (PARTITION BY POLICYNUMBER_stg,TERMNUMBER_stg ORDER BY MODELNUMBER_stg desc) r

from DB_T_PROD_STAG.pc_policyperiod,
DB_T_PROD_STAG.pctl_policyperiodstatus 
where 

pc_policyperiod.status_stg =pctl_policyperiodstatus.id_stg

and  pctl_policyperiodstatus.typecode_stg =''Bound''

) a 

) pp on pp.ID_stg = clm.PolicySystemPeriodID AND pp.PeriodStart_stg <= clm.lossdate AND pp.PeriodEnd_stg >= clm.lossdate

) as AGMT_CLM 
where AGMT_CLM.publicid is not null
qualify row_number() over(partition by AGMT_CLM.claimnumber order by AGMT_CLM.UPDATETIME desc,PeriodStart_stg asc)=1 

) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_agmt_clm.PUBLIC_ID as PUBLIC_ID,
sq_agmt_clm.CLAIM_NUMBER as CLAIM_NUMBER,
sq_agmt_clm.AGMT_CLM_RLTNSHP_TYPE as AGMT_CLM_RLTNSHP_TYPE,
sq_agmt_clm.AGMT_CLM_RLTNSHP_STRT_DT as AGMT_CLM_RLTNSHP_STRT_DT,
CASE WHEN sq_agmt_clm.AGMT_CLM_RLTNSHP_END_DT IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE sq_agmt_clm.AGMT_CLM_RLTNSHP_END_DT END as o_AGMT_CLM_RLTNSHP_END_DT,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_CLM_SRC_CD,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_AGMT_SRC_CD,
''PPV'' as AGMT_TYPE_CD,
sq_agmt_clm.UPDATETIME as UPDATETIME,
sq_agmt_clm.RETIRED as RETIRED,
sq_agmt_clm.source_record_id,
row_number() over (partition by sq_agmt_clm.source_record_id order by sq_agmt_clm.source_record_id) as RNK
FROM
sq_agmt_clm
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_agmt_clm.CLM_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_agmt_clm.AGMT_SRC_CD
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source.PUBLIC_ID as PUBLIC_ID,
exp_pass_from_source.CLAIM_NUMBER as CLAIM_NUMBER,
exp_pass_from_source.AGMT_CLM_RLTNSHP_TYPE as AGMT_CLM_RLTNSHP_TYPE,
exp_pass_from_source.AGMT_CLM_RLTNSHP_STRT_DT as AGMT_CLM_RLTNSHP_STRT_DT,
exp_pass_from_source.o_AGMT_CLM_RLTNSHP_END_DT as AGMT_CLM_RLTNSHP_END_DT,
:PRCS_ID as out_PRCS_ID,
exp_pass_from_source.out_CLM_SRC_CD as out_CLM_SRC_CD,
exp_pass_from_source.AGMT_TYPE_CD as AGMT_TYPE_CD,
exp_pass_from_source.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
exp_pass_from_source.UPDATETIME as TRANS_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
exp_pass_from_source.RETIRED as RETIRED,
exp_pass_from_source.source_record_id
FROM
exp_pass_from_source
);


-- Component LKP_AGMT_POL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_POL AS
(
SELECT
LKP.AGMT_ID,
LKP.TRANS_STRT_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD 
FROM db_t_prod_core.agmt 
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation.PUBLIC_ID AND LKP.AGMT_TYPE_CD = exp_data_transformation.AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
LKP.TRANS_STRT_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD 
FROM db_t_prod_core.CLM  
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_data_transformation.CLAIM_NUMBER AND LKP.SRC_SYS_CD = exp_data_transformation.out_CLM_SRC_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) = 1
);


-- Component LKP_AGMT_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_CLM AS
(
SELECT
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.AGMT_CLM_RLTNSHP_STRT_DTTM,
LKP.AGMT_CLM_RLTNSHP_END_DTTM,
LKP.TRANS_STRT_DTTM,
LKP.AGMT_ID,
LKP.CLM_ID,
LKP_AGMT_POL.AGMT_ID as in_AGMT_ID,
LKP_CLM.CLM_ID as in_CLM_ID,
exp_data_transformation.AGMT_CLM_RLTNSHP_TYPE as in_AGMT_CLM_RLTNSHP_TYPE_CD,
exp_data_transformation.AGMT_CLM_RLTNSHP_STRT_DT as in_AGMT_CLM_RLTNSHP_STRT_DTTM,
exp_data_transformation.out_PRCS_ID as out_PRCS_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.AGMT_CLM_RLTNSHP_STRT_DTTM asc,LKP.AGMT_CLM_RLTNSHP_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.AGMT_ID asc,LKP.CLM_ID asc) RNK
FROM
exp_data_transformation
INNER JOIN LKP_AGMT_POL ON exp_data_transformation.source_record_id = LKP_AGMT_POL.source_record_id
INNER JOIN LKP_CLM ON LKP_AGMT_POL.source_record_id = LKP_CLM.source_record_id
LEFT JOIN (
SELECT	AGMT_CLM.EDW_STRT_DTTM as EDW_STRT_DTTM,
		AGMT_CLM.EDW_END_DTTM as EDW_END_DTTM, 
		AGMT_CLM.AGMT_CLM_RLTNSHP_STRT_DTTM as AGMT_CLM_RLTNSHP_STRT_DTTM,
		AGMT_CLM.AGMT_CLM_RLTNSHP_END_DTTM as AGMT_CLM_RLTNSHP_END_DTTM,
		AGMT_CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, 
		AGMT_CLM.AGMT_ID as AGMT_ID,
		AGMT_CLM.CLM_ID as CLM_ID 
FROM	db_t_prod_core.AGMT_CLM AGMT_CLM
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  CLM_ID 
ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_ID = LKP_CLM.CLM_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.AGMT_CLM_RLTNSHP_STRT_DTTM asc,LKP.AGMT_CLM_RLTNSHP_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.AGMT_ID asc,LKP.CLM_ID asc)  = 1
);


-- Component exp_cdc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cdc AS
(
SELECT
NULL as NewLookupRow,
LKP_AGMT_CLM.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_CLM.in_AGMT_ID as in_AGMT_ID,
LKP_AGMT_CLM.in_CLM_ID as in_CLM_ID,
LKP_AGMT_CLM.in_AGMT_CLM_RLTNSHP_TYPE_CD as in_AGMT_CLM_RLTNSHP_TYPE_CD,
LKP_AGMT_CLM.in_AGMT_CLM_RLTNSHP_STRT_DTTM as in_AGMT_CLM_RLTNSHP_STRT_DT,
exp_data_transformation.AGMT_CLM_RLTNSHP_END_DT as in_AGMT_CLM_RLTNSHP_END_DT,
LKP_AGMT_CLM.out_PRCS_ID as out_PRCS_ID,
LKP_AGMT_CLM.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
CURRENT_TIMESTAMP as EDW_START_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_data_transformation.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.TRANS_END_DTTM as TRANS_END_DTTM,
MD5 ( TO_CHAR ( LKP_AGMT_CLM.in_AGMT_CLM_RLTNSHP_STRT_DTTM ) || TO_CHAR ( exp_data_transformation.AGMT_CLM_RLTNSHP_END_DT ) || TO_CHAR ( LKP_AGMT_CLM.in_AGMT_ID ) ) as v_MD5_src,
MD5 ( TO_CHAR ( LKP_AGMT_CLM.AGMT_CLM_RLTNSHP_STRT_DTTM ) || TO_CHAR ( LKP_AGMT_CLM.AGMT_CLM_RLTNSHP_END_DTTM ) || LKP_AGMT_CLM.AGMT_ID ) as v_MD5_tgt,
CASE WHEN v_MD5_tgt IS NULL THEN ''I'' ELSE CASE WHEN ( ( v_MD5_tgt != v_MD5_src ) and exp_data_transformation.TRANS_STRT_DTTM > LKP_AGMT_CLM.TRANS_STRT_DTTM ) THEN ''U'' ELSE ''R'' END END as ins_upd_flag,
exp_data_transformation.RETIRED as RETIRED,
LKP_AGMT_CLM.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_AGMT_CLM.CLM_ID as LKP_CLM_ID,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_AGMT_CLM ON exp_data_transformation.source_record_id = LKP_AGMT_CLM.source_record_id
);


-- Component rtr_INSRNC_AGMT_LOB_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_INSRNC_AGMT_LOB_INSERT as
SELECT
exp_cdc.NewLookupRow as NewLookupRow,
exp_cdc.lkp_AGMT_ID as old_AGMT_ID,
exp_cdc.in_AGMT_ID as AGMT_ID,
exp_cdc.in_CLM_ID as CLM_ID,
exp_cdc.in_AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
exp_cdc.in_AGMT_CLM_RLTNSHP_STRT_DT as STRT_DTTM,
exp_cdc.in_AGMT_CLM_RLTNSHP_END_DT as END_DTTM,
exp_cdc.out_PRCS_ID as PRCS_ID,
exp_cdc.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_cdc.EDW_START_DTTM as EDW_START_DTTM,
exp_cdc.EDW_END_DTTM as EDW_END_DTTM,
exp_cdc.ins_upd_flag as ins_upd_flag,
exp_cdc.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_cdc.TRANS_END_DTTM as TRANS_END_DTTM,
exp_cdc.RETIRED as RETIRED,
exp_cdc.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cdc.LKP_CLM_ID as LKP_CLM_ID,
exp_cdc.source_record_id
FROM
exp_cdc
WHERE exp_cdc.ins_upd_flag = ''I'' and exp_cdc.in_CLM_ID IS NOT NULL and exp_cdc.in_AGMT_ID IS NOT NULL OR ( exp_cdc.lkp_EDW_END_DTTM != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_cdc.RETIRED = 0 and exp_cdc.in_AGMT_ID IS NOT NULL and exp_cdc.in_CLM_ID IS NOT NULL ) OR exp_cdc.ins_upd_flag = ''U'' AND exp_cdc.lkp_EDW_END_DTTM = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_cdc.in_AGMT_ID IS NOT NULL and exp_cdc.in_CLM_ID IS NOT NULL;


-- Component rtr_INSRNC_AGMT_LOB_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table rtr_INSRNC_AGMT_LOB_RETIRED as
SELECT
exp_cdc.NewLookupRow as NewLookupRow,
exp_cdc.lkp_AGMT_ID as old_AGMT_ID,
exp_cdc.in_AGMT_ID as AGMT_ID,
exp_cdc.in_CLM_ID as CLM_ID,
exp_cdc.in_AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
exp_cdc.in_AGMT_CLM_RLTNSHP_STRT_DT as STRT_DTTM,
exp_cdc.in_AGMT_CLM_RLTNSHP_END_DT as END_DTTM,
exp_cdc.out_PRCS_ID as PRCS_ID,
exp_cdc.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_cdc.EDW_START_DTTM as EDW_START_DTTM,
exp_cdc.EDW_END_DTTM as EDW_END_DTTM,
exp_cdc.ins_upd_flag as ins_upd_flag,
exp_cdc.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_cdc.TRANS_END_DTTM as TRANS_END_DTTM,
exp_cdc.RETIRED as RETIRED,
exp_cdc.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cdc.LKP_CLM_ID as LKP_CLM_ID,
exp_cdc.source_record_id
FROM
exp_cdc
WHERE exp_cdc.ins_upd_flag = ''R'' and exp_cdc.RETIRED != 0 and exp_cdc.lkp_EDW_END_DTTM = to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) and exp_cdc.in_AGMT_ID IS NOT NULL and exp_cdc.in_CLM_ID IS NOT NULL;


-- Component upd_stg_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_INSRNC_AGMT_LOB_RETIRED.AGMT_ID as AGMT_ID,
rtr_INSRNC_AGMT_LOB_RETIRED.LKP_CLM_ID as CLM_ID,
rtr_INSRNC_AGMT_LOB_RETIRED.AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
rtr_INSRNC_AGMT_LOB_RETIRED.PRCS_ID as PRCS_ID,
rtr_INSRNC_AGMT_LOB_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_INSRNC_AGMT_LOB_RETIRED.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_INSRNC_AGMT_LOB_RETIRED.source_record_id
FROM
rtr_INSRNC_AGMT_LOB_RETIRED
);


-- Component exp_pass_to_target_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired AS
(
SELECT
upd_stg_upd_retired.CLM_ID as CLM_ID,
upd_stg_upd_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as EDW_END_DTTM_exp3,
upd_stg_upd_retired.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM4,
upd_stg_upd_retired.source_record_id
FROM
upd_stg_upd_retired
);


-- Component upd_stg_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_INSRNC_AGMT_LOB_INSERT.AGMT_ID as AGMT_ID,
rtr_INSRNC_AGMT_LOB_INSERT.CLM_ID as CLM_ID,
rtr_INSRNC_AGMT_LOB_INSERT.AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
rtr_INSRNC_AGMT_LOB_INSERT.STRT_DTTM as STRT_DTTM,
rtr_INSRNC_AGMT_LOB_INSERT.PRCS_ID as PRCS_ID,
rtr_INSRNC_AGMT_LOB_INSERT.EDW_START_DTTM as EDW_START_DTTM1,
rtr_INSRNC_AGMT_LOB_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_INSRNC_AGMT_LOB_INSERT.END_DTTM as o_RLTNSHP_ENDDT1,
rtr_INSRNC_AGMT_LOB_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_INSRNC_AGMT_LOB_INSERT.TRANS_END_DTTM as TRANS_END_DTTM1,
rtr_INSRNC_AGMT_LOB_INSERT.RETIRED as RETIRED1,
0 as UPDATE_STRATEGY_ACTION,
rtr_INSRNC_AGMT_LOB_INSERT.source_record_id as source_record_id
FROM
rtr_INSRNC_AGMT_LOB_INSERT
);


-- Component tgt_AGMT_CLM_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_CLM
USING exp_pass_to_target_upd_retired ON (AGMT_CLM.CLM_ID = exp_pass_to_target_upd_retired.CLM_ID AND AGMT_CLM.EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_pass_to_target_upd_retired.CLM_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_retired.EDW_END_DTTM_exp3,
TRANS_END_DTTM = exp_pass_to_target_upd_retired.TRANS_STRT_DTTM4;


-- Component tgt_AGMT_CLM_upd_retired, Type Post SQL 
UPDATE  db_t_prod_core.AGMT_CLM  
set EDW_END_DTTM=A.lead1
, TRANS_END_DTTM=a.lead2
FROM
(SELECT	distinct  CLM_ID,EDW_STRT_DTTM, TRANS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by CLM_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

 as lead1

,max(TRANS_STRT_DTTM) over (partition by CLM_ID ORDER BY EDW_STRT_DTTM  ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''

 as lead2

FROM	db_t_prod_core.AGMT_CLM

group by CLM_ID,EDW_STRT_DTTM, TRANS_STRT_DTTM

 )  a


where  AGMT_CLM.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and AGMT_CLM.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

and AGMT_CLM.CLM_ID=A.CLM_ID 

and CAST(AGMT_CLM.EDW_END_DTTM AS DATE)=''9999-12-31''

and CAST(AGMT_CLM.TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null and lead2 is not null;


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_stg_ins.AGMT_ID as AGMT_ID,
upd_stg_ins.CLM_ID as CLM_ID,
upd_stg_ins.AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
upd_stg_ins.STRT_DTTM as AGMT_CLM_RLTNSHP_STRT_DT,
upd_stg_ins.o_RLTNSHP_ENDDT1 as AGMT_CLM_RLTNSHP_END_DT,
upd_stg_ins.PRCS_ID as PRCS_ID,
upd_stg_ins.EDW_START_DTTM1 as EDW_START_DTTM1,
upd_stg_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
CASE WHEN upd_stg_ins.RETIRED1 != 0 THEN upd_stg_ins.TRANS_STRT_DTTM1 ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM11,
CASE WHEN upd_stg_ins.RETIRED1 = 0 THEN upd_stg_ins.EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as EDW_END_DTTM,
upd_stg_ins.source_record_id
FROM
upd_stg_ins
);


-- Component tgt_AGMT_CLM_Insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_CLM
(
AGMT_ID,
CLM_ID,
AGMT_CLM_RLTNSHP_TYPE_CD,
AGMT_CLM_RLTNSHP_STRT_DTTM,
AGMT_CLM_RLTNSHP_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.AGMT_ID as AGMT_ID,
exp_pass_to_target_ins.CLM_ID as CLM_ID,
exp_pass_to_target_ins.AGMT_CLM_RLTNSHP_TYPE_CD as AGMT_CLM_RLTNSHP_TYPE_CD,
exp_pass_to_target_ins.AGMT_CLM_RLTNSHP_STRT_DT as AGMT_CLM_RLTNSHP_STRT_DTTM,
exp_pass_to_target_ins.AGMT_CLM_RLTNSHP_END_DT as AGMT_CLM_RLTNSHP_END_DTTM,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.EDW_START_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


END; ';