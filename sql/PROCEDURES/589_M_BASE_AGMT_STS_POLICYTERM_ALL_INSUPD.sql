-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STS_POLICYTERM_ALL_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  ETL_LOAD_DTTM STRING;
  PRCS_ID STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  ETL_LOAD_DTTM := public.func_get_scoped_param(:run_id, ''etl_load_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
 

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 
SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL,
TGT_IDNTFTN_NM AS TGT_IDNTFTN_NM,
TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
FROM 
db_t_prod_core.TERADATA_ETL_REF_XLAT 
WHERE 
TGT_IDNTFTN_NM IN (''AGMT_STS_RSN_TYPE'',''AGMT_STS_TYPE'',''AGMT_STS_SRC_TYPE'' ) 
AND SRC_IDNTFTN_NM in (''pctl_reasoncode.typecode'',''pctl_reasoncode.TYPECODE'', 
''out_EDWPolicyStatus_PC.PolicyStatus'',
''pctl_cancellationsource.typecode'')  
AND SRC_IDNTFTN_SYS=''GW'' AND EXPN_DT=''9999-12-31''
);


-- Component sq_pc_policyterm_status_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_policyterm_status_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as policynumber,
$2 as termnumber,
$3 as policystatus,
$4 as policystatus_dttm,
$5 as cancellationsource,
$6 as cancellationreason,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select

pps.PolicyNumber,

/* pps.AccountNumber, */
pps.TermNumber,

/* pps.PolicyPeriodID, */
/* pps.PolicyEffectiveDate, */
/* pps.PolicyExpirationDate, */
pps.PolicyStatus,

cast(:ETL_LOAD_DTTM as timestamp(6)) PolicyStatus_dttm,

cast(case when pps.PolicyStatus=''CANCELED'' and pp.cancellation_source is not null then pp.cancellation_source else ''UNK'' end as varchar(256)) as cancellationsource,

cast(case when pps.PolicyStatus=''CANCELED'' and pp.cancellation_reason is not null then pp.cancellation_reason else ''UNK'' end as varchar(50)) as cancellationreason

from

(select 

pc_policyperiod.PolicyNumber_stg as PolicyNumber,

pc_account.AccountNumber_stg as AccountNumber,

pc_policyperiod.TermNumber_stg as TermNumber,

pc_policyperiod.PublicID_stg as PolicyPeriodID,

pc_policyperiod.EditEffectiveDate_stg as PolicyEffectiveDate,

pc_policyperiod.PeriodEnd_stg as PolicyExpirationDate,

CASE                                       

WHEN pc_policyperiod.CancellationDate_stg IS NOT NULL AND cast(pc_policyperiod.CancellationDate_stg as date) <= cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) THEN ''CANCELED'' 

WHEN pc_policyperiod.CancellationDate_stg IS NOT NULL AND cast(pc_policyperiod.CancellationDate_stg as date) > cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) AND

       cast(pc_policyperiod.EditEffectiveDate_stg as date)<= cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) THEN ''IN FORCE''                       

WHEN cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) > cast(pc_policyperiod.PeriodStart_stg as date) AND pc_policyterm.Bound_stg = 0 THEN 

	(CASE WHEN (pctl_papolicytype_alfa.ID_stg IS NOT NULL and pctl_job.Typecode_stg = ''Renewal'' 

	and pctl_policyperiodsrctype_alfa.Typecode_stg=''AutoConverted'' and pcx_migrationpolinfo_ext.LegacyDueDate_alfa_stg IS NOT NULL 

	and (cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) - cast(pcx_migrationpolinfo_ext.LegacyDueDate_alfa_stg as date) between 0 and 30)) THEN ''IN FORCE'' ELSE ''RENEWAL LAPSED'' END)

WHEN cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) <= cast(pc_policyperiod.PeriodStart_stg as date) AND pc_policyterm.Bound_stg = 0 THEN ''PENDING CONFIRMATION'' 

WHEN cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) < cast(pc_policyperiod.PeriodStart_stg as date) AND pc_policyterm.Bound_stg = 1 

AND (pctl_job.Typecode_stg = ''Renewal'' or (E.Typecode_stg=''Renewal'' and cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) < cast(C.PeriodStart_stg as date ) )) THEN ''CONFIRMED''          

WHEN cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) < cast(pc_policyperiod.PeriodStart_stg as date) AND pc_policyterm.Bound_stg = 1 AND ((pctl_job.Typecode_stg<>''Renewal'')) THEN ''SCHEDULED''

WHEN cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) >= cast(pc_policyperiod.PeriodStart_stg as date) AND cast(cast(:ETL_LOAD_DTTM as timestamp(6)) as date) >= cast(pc_policyperiod.PeriodEnd_stg as date) 

AND pc_policyterm.Bound_stg = 1 THEN ''EXPIRED'' ELSE ''IN FORCE'' END AS PolicyStatus

from db_t_prod_stag.pc_policyperiod  

join db_t_prod_stag.pc_policy on pc_policyperiod.PolicyID_stg=pc_policy.ID_stg

join db_t_prod_stag.pc_policyterm on pc_policyperiod.PolicyTermID_stg=pc_policyterm.ID_stg

join db_t_prod_stag.pc_account on pc_policy.AccountID_stg=pc_account.ID_stg

JOIN db_t_prod_stag.pc_job on pc_job.ID_stg=pc_policyperiod.JobID_stg

JOIN db_t_prod_stag.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

left JOIN db_t_prod_stag.pctl_policyperiodsrctype_alfa on pctl_policyperiodsrctype_alfa.ID_stg=pc_policyperiod.PolicyPeriodSource_stg

left JOIN db_t_prod_stag.pcx_migrationpolinfo_ext on pc_policyperiod.PolicyNumber_stg = pcx_migrationpolinfo_ext.legacypolicynumber_stg 

JOIN db_t_prod_stag.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg 

left JOIN db_t_prod_stag.pc_policyperiod B on pc_policyperiod.BasedonID_stg=B.ID_stg

JOIN db_t_prod_stag.pc_policyline on pc_policyline.BranchID_stg=pc_policyperiod.ID_stg

left JOIN db_t_prod_stag.pctl_papolicytype_alfa on pctl_papolicytype_alfa.ID_stg=pc_policyline.PAPolicyType_alfa_stg

left JOIN db_t_prod_stag.pc_policyperiod C on C.PeriodID_stg=pc_policyperiod.PeriodID_stg and C.ModelNumber_stg=1

JOIN db_t_prod_stag.pc_job D on D.ID_stg=C.JobID_stg

JOIN db_t_prod_stag.pctl_job E on E.ID_stg=D.Subtype_stg

WHERE pc_policyperiod.MostRecentModel_stg=1 and pctl_policyperiodstatus.Typecode_stg=''Bound''

and (pc_policyline.EffectiveDate_stg is NULL or cast(pc_policyline.EffectiveDate_stg as timestamp(6)) <= cast(:ETL_LOAD_DTTM as timestamp(6)))

and (pc_policyline.ExpirationDate_stg is NULL or cast(pc_policyline.ExpirationDate_stg as timestamp(6))>= cast(:ETL_LOAD_DTTM as timestamp(6)))

) pps

/*  lkp_pc_policyperiod */
left join

(

select 

a.PolicyNumber_stg PolicyNumber,

a.termnumber_stg termnumber,

e.TYPECODE_stg as cancellation_source,

f.Typecode_stg as cancellation_reason

/* ,rank() over(partition by periodid_stg order by case when editeffectivedate_stg > modeldate_stg then editeffectivedate_stg else modeldate_stg end desc) as rnk  */
from 

db_t_prod_stag.pc_policyperiod as a

inner join db_t_prod_stag.pctl_policyperiodstatus b on a.status_stg = b.ID_stg

inner join db_t_prod_stag.pc_job c on a.jobid_stg = c.ID_stg

inner join db_t_prod_stag.pctl_job d on c.subtype_stg = d.ID_stg 

left outer join db_t_prod_stag.pctl_cancellationsource e on  c.Source_stg= e.ID_stg

left outer join db_t_prod_stag.pctl_reasoncode f on c.CancelReasonCode_stg=f.ID_stg

where lower(b.Typecode_stg) = ''bound'' and lower(d.Typecode_stg) = ''cancellation''

and cast(case when editeffectivedate_stg > modeldate_stg then editeffectivedate_stg else modeldate_stg end as timestamp(6)) <= cast(:ETL_LOAD_DTTM  as timestamp(6))

qualify row_number() over(partition by a.PolicyNumber_stg,a.termnumber_stg, a.periodid_stg order by case when a.editeffectivedate_stg > modeldate_stg then editeffectivedate_stg else modeldate_stg end desc, modeldate_stg desc) = 1

) pp

on pp.PolicyNumber = pps.policyNumber and pp.termNumber = pps.termNumber

order by pp.PolicyNumber,pp.TermNumber
) SRC
)
);


-- Component exp_data_cleansing_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_cleansing_transformation AS
(
SELECT
sq_pc_policyterm_status_x.policynumber as host_agmt_num,
sq_pc_policyterm_status_x.termnumber as term_num,
sq_pc_policyterm_status_x.policystatus_dttm as agmt_sts_strt_dttm,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as out_agmt_sts_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as out_agmt_sts_rsn_cd,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as out_agmt_sts_src_type_cd,
sq_pc_policyterm_status_x.source_record_id,
row_number() over (partition by sq_pc_policyterm_status_x.source_record_id order by sq_pc_policyterm_status_x.source_record_id) as RNK
FROM
sq_pc_policyterm_status_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.policystatus AND LKP_1.TGT_IDNTFTN_NM = ''AGMT_STS_TYPE''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.policystatus AND LKP_2.TGT_IDNTFTN_NM = ''AGMT_STS_TYPE''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.cancellationreason AND LKP_3.TGT_IDNTFTN_NM = ''AGMT_STS_RSN_TYPE''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.cancellationreason AND LKP_4.TGT_IDNTFTN_NM = ''AGMT_STS_RSN_TYPE''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.cancellationsource AND LKP_5.TGT_IDNTFTN_NM = ''AGMT_STS_SRC_TYPE''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_pc_policyterm_status_x.cancellationsource AND LKP_6.TGT_IDNTFTN_NM = ''AGMT_STS_SRC_TYPE''
QUALIFY row_number() over (partition by sq_pc_policyterm_status_x.source_record_id order by sq_pc_policyterm_status_x.source_record_id) = 1
);


-- Component LKP_AGMT_TERM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_TERM AS
(
SELECT
LKP.agmt_id,
exp_data_cleansing_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_cleansing_transformation.source_record_id ORDER BY LKP.agmt_id asc) RNK
FROM
exp_data_cleansing_transformation
LEFT JOIN (
select 
agmt_id as agmt_id,
host_agmt_num as host_agmt_num,
term_num as term_num 
from db_t_prod_core.agmt  
where agmt_type_cd = ''POLTRM'' 
group by 1,2,3
) LKP ON LKP.host_agmt_num = exp_data_cleansing_transformation.host_agmt_num AND LKP.term_num = exp_data_cleansing_transformation.term_num
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_cleansing_transformation.source_record_id ORDER BY LKP.agmt_id asc) = 1
);


-- Component LKP_AGMT_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_STS AS
(
SELECT
LKP.agmt_id,
exp_data_cleansing_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_cleansing_transformation.source_record_id ORDER BY LKP.agmt_id asc) RNK
FROM
exp_data_cleansing_transformation
INNER JOIN LKP_AGMT_TERM ON exp_data_cleansing_transformation.source_record_id = LKP_AGMT_TERM.source_record_id
LEFT JOIN (
SELECT 
AGMT_ID AS AGMT_ID,
AGMT_STS_CD AS AGMT_STS_CD, 
AGMT_STS_RSN_CD AS AGMT_STS_RSN_CD,
AGMT_STS_SRC_TYPE_CD AS AGMT_STS_SRC_TYPE_CD 
FROM db_t_prod_core.AGMT_STS 
WHERE AGMT_STS_CD <> ''CNFRMDDT'' 
AND AGMT_ID IN (SELECT AGMT_ID FROM db_t_prod_core.AGMT WHERE AGMT_TYPE_CD = ''POLTRM'' GROUP BY AGMT_ID) 
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_ID ORDER BY AGMT_STS_STRT_DTTM DESC) = 1
) LKP ON LKP.agmt_id = LKP_AGMT_TERM.agmt_id AND LKP.agmt_sts_cd = exp_data_cleansing_transformation.out_agmt_sts_cd AND LKP.agmt_sts_rsn_cd = exp_data_cleansing_transformation.out_agmt_sts_rsn_cd AND LKP.agmt_sts_src_type_cd = exp_data_cleansing_transformation.out_agmt_sts_src_type_cd
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_cleansing_transformation.source_record_id ORDER BY LKP.agmt_id asc) = 1
);


-- Component fil_pass_new_data, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_pass_new_data AS
(
SELECT
LKP_AGMT_STS.agmt_id as agmt_id_lkp,
LKP_AGMT_TERM.agmt_id as agmt_id_src,
exp_data_cleansing_transformation.agmt_sts_strt_dttm as agmt_sts_strt_dttm,
exp_data_cleansing_transformation.out_agmt_sts_cd as agmt_sts_cd,
exp_data_cleansing_transformation.out_agmt_sts_rsn_cd as agmt_sts_rsn_cd,
exp_data_cleansing_transformation.out_agmt_sts_src_type_cd as agmt_sts_src_type_cd,
exp_data_cleansing_transformation.source_record_id
FROM
exp_data_cleansing_transformation
LEFT JOIN LKP_AGMT_TERM ON exp_data_cleansing_transformation.source_record_id = LKP_AGMT_TERM.source_record_id
LEFT JOIN LKP_AGMT_STS ON LKP_AGMT_TERM.source_record_id = LKP_AGMT_STS.source_record_id
WHERE LKP_AGMT_STS.agmt_id IS NULL and LKP_AGMT_TERM.agmt_id IS NOT NULL
);


-- Component exp_assign_static_target_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_assign_static_target_data AS
(
SELECT
fil_pass_new_data.agmt_id_src as agmt_id_src,
fil_pass_new_data.agmt_sts_strt_dttm as agmt_sts_strt_dttm,
fil_pass_new_data.agmt_sts_cd as agmt_sts_cd,
fil_pass_new_data.agmt_sts_rsn_cd as agmt_sts_rsn_cd,
fil_pass_new_data.agmt_sts_src_type_cd as agmt_sts_src_type_cd,
:PRCS_ID as out_prcs_id,
CURRENT_TIMESTAMP as out_edw_strt_dttm,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_edw_end_dttm,
fil_pass_new_data.source_record_id
FROM
fil_pass_new_data
);


-- Component agmt_sts_ins, Type TARGET 
INSERT INTO db_t_prod_core.AGMT_STS
(
AGMT_ID,
AGMT_STS_CD,
AGMT_STS_STRT_DTTM,
AGMT_STS_RSN_CD,
AGMT_STS_END_DTTM,
AGMT_STS_SRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_assign_static_target_data.agmt_id_src as AGMT_ID,
exp_assign_static_target_data.agmt_sts_cd as AGMT_STS_CD,
exp_assign_static_target_data.agmt_sts_strt_dttm as AGMT_STS_STRT_DTTM,
exp_assign_static_target_data.agmt_sts_rsn_cd as AGMT_STS_RSN_CD,
exp_assign_static_target_data.out_edw_end_dttm as AGMT_STS_END_DTTM,
exp_assign_static_target_data.agmt_sts_src_type_cd as AGMT_STS_SRC_TYPE_CD,
exp_assign_static_target_data.out_prcs_id as PRCS_ID,
exp_assign_static_target_data.out_edw_strt_dttm as EDW_STRT_DTTM,
exp_assign_static_target_data.out_edw_end_dttm as EDW_END_DTTM
FROM
exp_assign_static_target_data;


-- Component agmt_sts_ins, Type Post SQL 
update db_t_prod_core.agmt_sts as a   
set edw_end_dttm = b.edw_end_dttm_new 
  from
(

select agmt_id, agmt_sts_cd, agmt_sts_strt_dttm, edw_strt_dttm,edw_end_dttm,edw_end_dttm_new

from 

(select 

agmt_id,

agmt_sts_cd,

agmt_sts_strt_dttm,

edw_strt_dttm,

edw_end_dttm,

row_number() over(partition by agmt_id order by agmt_sts_strt_dttm desc, edw_strt_dttm desc) as rnk,

/*case when rnk =1 then edw_end_dttm else 

max(edw_strt_dttm) over(partition by agmt_id order by agmt_sts_strt_dttm desc, edw_strt_dttm desc rows between 1 preceding and 1 preceding) - interval ''1'' second 

end as edw_end_dttm_new */
CASE
    WHEN rnk = 1                                       -- first‑row flag
         THEN edw_end_dttm                             -- keep existing end
    ELSE                                               -- all other rows
         ( MAX(edw_strt_dttm) OVER (                   -- previous row’s start
                 PARTITION BY agmt_id
                 ORDER BY agmt_sts_strt_dttm DESC ,
                          edw_strt_dttm      DESC
                 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
           )
           - INTERVAL ''1 second''                       -- back off 1 second
         )
END  AS edw_end_dttm_new

from db_t_prod_core.agmt_sts 

where agmt_sts_cd <> ''CNFRMDDT'' 

and agmt_id in (select agmt_id from db_t_prod_core.agmt where agmt_type_cd = ''poltrm'' group by agmt_id)

) rvd_upd where edw_end_dttm<> edw_end_dttm_new

qualify row_number() over (partition by agmt_id,agmt_sts_cd,agmt_sts_strt_dttm,edw_strt_dttm,edw_end_dttm order by edw_end_dttm_new desc)=1

) as b

where 

a.agmt_id = b.agmt_id and

a.agmt_sts_cd = b.agmt_sts_cd and

a.agmt_sts_strt_dttm = b.agmt_sts_strt_dttm and

a.edw_strt_dttm = b.edw_strt_dttm and

a.edw_end_dttm = b.edw_end_dttm;
  

END; ';