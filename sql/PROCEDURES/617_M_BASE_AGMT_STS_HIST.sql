-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STS_HIST("WORKLET_NAME" VARCHAR)
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
$7 as load_dttm,
$8 as source_record_id
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

cast(case when pps.PolicyStatus=''CANCELED'' and pp.cancellation_reason is not null then pp.cancellation_reason else ''UNK'' end as varchar(50)) as cancellationreason,

current_timestamp as Load_dttm

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

e.name_stg as cancellation_source,

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


-- Component exp_passthrough, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_passthrough AS
(
SELECT
sq_pc_policyterm_status_x.policynumber as policynumber,
sq_pc_policyterm_status_x.termnumber as termnumber,
sq_pc_policyterm_status_x.policystatus as policystatus,
sq_pc_policyterm_status_x.policystatus_dttm as policystatus_dttm,
sq_pc_policyterm_status_x.cancellationsource as cancellationsource,
sq_pc_policyterm_status_x.cancellationreason as cancellationreason,
sq_pc_policyterm_status_x.load_dttm as load_dttm,
:PRCS_ID as prcs_id,
sq_pc_policyterm_status_x.source_record_id
FROM
sq_pc_policyterm_status_x
);


-- Component AGMT_STS_STAGHIST, Type TARGET 
INSERT INTO db_t_prod_stag.agmt_sts_staghist
(
PolicyNumber_stg,
TermNumber_stg,
PolicyStatus_stg,
PolicyStatus_dttm_stg,
cancellationsource_stg,
cancellationreason_stg,
load_dttm_stg,
prcs_id
)
SELECT
exp_passthrough.policynumber as PolicyNumber_stg,
exp_passthrough.termnumber as TermNumber_stg,
exp_passthrough.policystatus as PolicyStatus_stg,
exp_passthrough.policystatus_dttm as PolicyStatus_dttm_stg,
exp_passthrough.cancellationsource as cancellationsource_stg,
exp_passthrough.cancellationreason as cancellationreason_stg,
exp_passthrough.load_dttm as load_dttm_stg,
exp_passthrough.prcs_id as prcs_id
FROM
exp_passthrough;


END; ';