-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NRPOL_MEMB_INFORCE_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicyNumber_stg,
$2 as Mem_nbr,
$3 as State,
$4 as pol_type,
$5 as Status,
$6 as non_renew_dt,
$7 as Date_paid,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct

pp.PolicyNumber_stg as PolicyNumber_stg,

edf.ClientId_alfa_stg as Mem_nbr,

jur.TypeCode_stg as State,

case when pl.PAPolicyType_alfa_stg  is not NULL then pa.TYPECODE_stg

     when pl.HOPolicyType_stg       is not NULL then ho.TypeCode_stg

     when pl.BP7PolicyType_alfa_stg is not NULL then bp7.TypeCode_stg

end as pol_type,

case when pp.Status_stg = 9 then ''Non-Renewed'' 

else pps.TypeCode_stg 

end as Status,

case when pps.TypeCode_stg in (''Bound'') then cast(pp.PeriodEnd_stg as date) 

else cast(pp.PeriodStart_stg as date) 

end as non_renew_dt,

Date_paid

from DB_T_PROD_STAG.pc_policyperiod pp

join DB_T_PROD_STAG.pc_job job on job.ID_stg = pp.JobID_stg

join DB_T_PROD_STAG.pctl_job jobtl on jobtl.ID_stg = job.Subtype_stg

join DB_T_PROD_STAG.pc_policyterm pt on pt.ID_stg = pp.PolicyTermID_stg

join DB_T_PROD_STAG.pctl_nonrenewalcode nrc on pt.NonRenewReason_stg = nrc.ID_stg

join DB_T_PROD_STAG.pctl_policyperiodstatus pps on pps.ID_stg = pp.Status_stg

join DB_T_PROD_STAG.pc_effectivedatedfields edf on edf.BranchID_stg = pp.ID_stg and edf.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL

left join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

left join DB_T_PROD_STAG.pctl_hopolicytype_hoe ho on ho.ID_stg = pl.HOPolicyType_stg

left join DB_T_PROD_STAG.pctl_bp7policytype_alfa bp7 on bp7.ID_stg = pl.BP7PolicyType_alfa_stg

join DB_T_PROD_STAG.pctl_jurisdiction jur on jur.ID_stg = pp.BaseState_stg

join DB_T_PROD_STAG.pc_policyperiod pp1 on pp1.PolicyNumber_stg = pp.PolicyNumber_stg and pp.PeriodStart_stg = pp1.PeriodStart_stg

join DB_T_CORE_PROD.member_mstr mm on mm.memb_num = edf.ClientId_alfa_stg

    and mm.memb_exp_dt = ''9999-12-31 23:59:59.999999''

join DB_T_CORE_PROD.member_trans mt on mt.memb_skey = mm.memb_skey

    and chg_exp_dt = ''9999-12-31 23:59:59.999999''

where nrc.TYPECODE_stg = ''inactivemembership''

and (pp.status_stg = 9

and pp.PeriodEnd_stg < CURRENT_TIMESTAMP

and pp.TermNumber_stg =

						(select max(pp2.TermNumber_stg)

						from DB_T_PROD_STAG.pc_policyperiod pp2

						where pp2.PolicyNumber_stg = pp.PolicyNumber_stg

						and pp2.Status_stg = 9

						)

and pp.ModelNumber_stg =

						(select max(pp2.ModelNumber_stg)

						from DB_T_PROD_STAG.pc_policyperiod pp2

						where pp2.PolicyNumber_stg = pp.PolicyNumber_stg

						and pp2.TermNumber_stg = pp.TermNumber_stg

						and pp2.Status_stg = 9

						)

and pp.CancellationDate_stg is NULL

or pp.status_stg = 10001)

and bp7.ID_stg is NULL

and status_cd not in (1, 3)

and date_paid - non_renew_dt between -360 and 30

order by non_renew_dt
) SRC
)
);


-- Component exp_GATHER, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_GATHER AS
(
SELECT
SQ_pc_policyperiod.PolicyNumber_stg as PolicyNumber_Stg,
SQ_pc_policyperiod.Mem_nbr as Mem_nbr,
SQ_pc_policyperiod.State as State,
SQ_pc_policyperiod.pol_type as Pol_type,
SQ_pc_policyperiod.Status as Status,
TO_CHAR ( SQ_pc_policyperiod.non_renew_dt ) as Non_renew_dt_o,
TO_CHAR ( SQ_pc_policyperiod.Date_paid ) as Date_paid_o,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component NRPol_Memb_Inforce, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE NRPol_Memb_Inforce AS
(
SELECT
exp_GATHER.PolicyNumber_Stg as PolicyNumber,
exp_GATHER.Mem_nbr as Mem_nbr,
exp_GATHER.State as State,
exp_GATHER.Pol_type as Pol_type,
exp_GATHER.Status as Status,
exp_GATHER.Non_renew_dt_o as Non_renew_dt,
exp_GATHER.Date_paid_o as Date_Paid
FROM
exp_GATHER
);


-- Component NRPol_Memb_Inforce, Type EXPORT_DATA Exporting data
;


END; ';