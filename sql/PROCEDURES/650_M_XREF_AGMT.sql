-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_AGMT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');


 

-- Component LKP_TGT_XREF_AGMT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TGT_XREF_AGMT AS
(
SELECT
AGMT_ID,
NK_SRC_KEY,
TERM_NUM,
AGMT_TYPE_CD
FROM db_t_prod_core.DIR_AGMT
);


-- Component sq_xref_agmt, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_xref_agmt AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as NK_SRC_KEY,
$2 as TERM_NUM,
$3 as AGMT_TYPE_CD,
$4 as SRC_SYS_CD,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT nk_src_key,termnumber,agmt_type_code,src_sys_cd,sort FROM 

(

/*  union 01 */
SELECT DISTINCT

	prd.PolicyNumber_stg AS nk_src_key,

	NULL AS termnumber,

	CAST(''POL''AS VARCHAR(60)) AS agmt_type_code,

	CAST(''GWPC'' AS VARCHAR(50)) AS src_sys_cd,

	CAST(1 AS INTEGER) AS sort

FROM DB_T_PROD_STAG.pc_policy p  

inner join DB_T_PROD_STAG.pc_policyperiod prd on p.id_stg=prd.PolicyId_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus ps on ps.id_stg=prd.Status_stg 

where prd.PolicyNumber_stg IS NOT NULL 

and lower(ps.TYPECODE_stg) =''bound'' 

and p.UpdateTime_stg > (:start_dttm)

and p.UpdateTime_stg <= (:end_dttm)



UNION ALL

/*  union 02 */
SELECT DISTINCT

	pc_account.AccountNumber_stg,

	NULL,

	CAST(''ACT''AS VARCHAR(60)) AS agmt_type_code,

	CAST(''GWPC'' AS VARCHAR(50)) AS src_sys_cd,

	CAST(2 AS INTEGER) AS sort

FROM

 	DB_T_PROD_STAG.pc_account

WHERE

	AccountNumber_stg IS NOT NULL

AND pc_account.UpdateTime_stg > (:start_dttm)

and pc_account.UpdateTime_stg <= (:end_dttm)





UNION ALL

/*  union 03  */
SELECT bc_account.AccountNumber_stg as AccountNumber,

    NULL,

    CAST(''ACT'' AS VARCHAR(60)) AS  agmt_type_code,

    CAST(''GWBC'' AS VARCHAR(50)) AS src_sys_cd,

    CAST(2 AS INTEGER) AS sort

FROM DB_T_PROD_STAG.bc_account 

LEFT OUTER JOIN DB_T_PROD_STAG.pc_account 

on bc_account.AccountNumber_stg = pc_account.AccountNumber_stg

 where bc_account.AccountNumber_stg IS NOT NULL 

 AND  pc_account.AccountNumber_stg IS NULL 

 AND bc_account.UpdateTime_stg > (:start_dttm)

 and bc_account.UpdateTime_stg <= (:end_dttm) 





UNION ALL

/*  union 04 */
SELECT distinct 

	pc_policyperiod.PolicyNumber_stg,

	pc_policyperiod.TermNumber_stg,

	CAST(''POLTRM'' AS VARCHAR(60)) AS  agmt_type_code,

	CAST(''GWPC'' AS VARCHAR(50)) AS src_sys_cd,

	CAST(3 AS INTEGER) AS sort

from DB_T_PROD_STAG.pc_policyperiod 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

 on pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg

WHERE lower(pctl_policyperiodstatus.typecode_stg) =''bound'' and mostrecentmodel_stg=1

 and PolicyNumber_stg IS NOT NULL

 and pc_policyperiod.UpdateTime_stg > (:start_dttm)

 and pc_policyperiod.UpdateTime_stg <= (:end_dttm)



UNION ALL

/*  union 05 */
SELECT distinct   

  cast(pc_policyperiod.Publicid_stg as varchar(64)) as Publicid_stg,	/*EIM-37477*/							

  NULL,

  CAST(''PPV'' AS VARCHAR(60)) AS  agmt_type_code,

  CAST(''GWPC'' AS VARCHAR(50)) AS SRC_SYS_CD,

  CAST(4 AS INTEGER) AS sort

 from DB_T_PROD_STAG.pc_policyperiod 

 inner join  DB_T_PROD_STAG.pc_policy on pc_policy.ID_stg=pc_policyperiod.PolicyID_stg

 inner  join  DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg=pc_job.ID_stg

 inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

 inner join  DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.Status_stg=pctl_policyperiodstatus.id_stg

left outer join DB_T_PROD_STAG.pc_effectivedatedfields on pc_effectivedatedfields.branchid_stg=pc_policyperiod.id_stg

Where lower(pctl_policyperiodstatus.typecode_stg) =''bound''

and pc_effectivedatedfields.ExpirationDate_stg is null

AND  pc_policyperiod.PublicId_stg  IS NOT NULL

and  pc_policyperiod.UpdateTime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

	

UNION ALL

/*  union 06 */
select 

cast (cc_policy.id_stg as varchar (60)) as Publicid_stg,

NULL,

CAST(''PPV'' AS VARCHAR(60)) AS  agmt_type_code,

CAST(''GWCC'' AS VARCHAR(50)) AS SRC_SYS_CD,

CAST(4 AS INTEGER) AS sort

from DB_T_PROD_STAG.cc_policy 

left outer join  DB_T_PROD_STAG.cctl_policystatus on cctl_policystatus.id_stg=cc_policy.Status_stg

/*  normal unverified DB_T_STAG_DM_PROD.claims */
WHERE cc_policy.UpdateTime_stg > (:start_dttm)

and cc_policy.UpdateTime_stg <= (:end_dttm)

and PublicId_stg IS NOT NULL

and ((cc_policy.verified_stg = 0 and coalesce(cc_policy.LegacyPolInd_alfa_stg,0) <> 1 ) or coalesce(cc_policy.LegacyPolInd_alfa_stg,0) = 1)



UNION ALL

/*  union 07 */
select  distinct x.BillingReferenceNumber_Alfa AS BillingReferenceNumber_Alfa,

NULL,

	CAST(''INV'' AS VARCHAR(60)) AS  agmt_type_code,

	CAST(''GWBC'' AS VARCHAR(50)) AS src_sys_cd,

	CAST(5 AS INTEGER) AS sort

from (

select distinct bc_invoicestream.BillingReferenceNumber_Alfa_stg AS BillingReferenceNumber_Alfa

from DB_T_PROD_STAG.bc_policyperiod

inner join DB_T_PROD_STAG.bc_invoicestream on bc_policyperiod.PrimaryInvoiceStream_alfa_stg=bc_invoicestream.id_stg 

where (bc_invoicestream.updatetime_stg> (:start_dttm) AND bc_invoicestream.updatetime_stg <= (:end_dttm))

OR (bc_policyperiod.updatetime_stg> (:start_dttm) AND bc_policyperiod.updatetime_stg <= (:end_dttm))

UNION ALL

select distinct bc_invoicestream.BillingReferenceNumber_Alfa_stg AS BillingReferenceNumber_Alfa

from  DB_T_PROD_STAG.bc_invoicestream inner join  DB_T_PROD_STAG.bc_account on  bc_account.id_stg = bc_invoicestream.AccountID_stg 

left outer join DB_T_PROD_STAG.bc_policy on bc_invoicestream.policyid_stg=bc_policy.id_stg

left outer join DB_T_PROD_STAG.bc_policyperiod on bc_policyperiod.PolicyID_stg=bc_policy.id_stg

where (bc_invoicestream.updatetime_stg> (:start_dttm) AND bc_invoicestream.updatetime_stg <= (:end_dttm) )

OR (bc_policyperiod.updatetime_stg> (:start_dttm) AND bc_policyperiod.updatetime_stg <= (:end_dttm))

) x where BillingReferenceNumber_Alfa IS NOT NULL 

) TMP

ORDER BY 5 ASC
) SRC
)
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
sq_xref_agmt.NK_SRC_KEY as NK_SRC_KEY,
sq_xref_agmt.TERM_NUM as TERM_NUM,
sq_xref_agmt.AGMT_TYPE_CD as AGMT_TYPE_CD,
DECODE ( TRUE , LKP_1.AGMT_ID /* replaced lookup LKP_TGT_XREF_AGMT */ IS NOT NULL , ''R'' , ''I'' ) as ins_rej_flg,
sq_xref_agmt.SRC_SYS_CD as SRC_SYS_CD,
CURRENT_TIMESTAMP as LOAD_DTTM,
sq_xref_agmt.source_record_id,
row_number() over (partition by sq_xref_agmt.source_record_id order by sq_xref_agmt.source_record_id) as RNK
FROM
sq_xref_agmt
LEFT JOIN LKP_TGT_XREF_AGMT LKP_1 ON LKP_1.NK_SRC_KEY = sq_xref_agmt.NK_SRC_KEY AND coalesce(LKP_1.TERM_NUM,0) = coalesce(sq_xref_agmt.TERM_NUM,0) AND LKP_1.AGMT_TYPE_CD = sq_xref_agmt.AGMT_TYPE_CD
/*QUALIFY RNK = 1*/
);


-- Component flt_ins_rej_agmt_id, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_ins_rej_agmt_id AS
(
SELECT
exp_pass_to_target.NK_SRC_KEY as NK_SRC_KEY,
exp_pass_to_target.TERM_NUM as TERM_NUM,
exp_pass_to_target.AGMT_TYPE_CD as AGMT_TYPE_CD,
exp_pass_to_target.ins_rej_flg as ins_rej_flg,
exp_pass_to_target.SRC_SYS_CD as SRC_SYS_CD,
exp_pass_to_target.LOAD_DTTM as LOAD_DTTM,
exp_pass_to_target.source_record_id
FROM
exp_pass_to_target
WHERE exp_pass_to_target.ins_rej_flg = ''I''
);


-- Component DIR_AGMT, Type TARGET 
INSERT INTO DB_T_PROD_CORE.DIR_AGMT
(
AGMT_ID,
NK_SRC_KEY,
TERM_NUM,
AGMT_TYPE_CD,
SRC_SYS_CD,
LOAD_DTTM
)
SELECT
public.seq_agmt_id.nextval  as AGMT_ID,
flt_ins_rej_agmt_id.NK_SRC_KEY as NK_SRC_KEY,
flt_ins_rej_agmt_id.TERM_NUM as TERM_NUM,
flt_ins_rej_agmt_id.AGMT_TYPE_CD as AGMT_TYPE_CD,
flt_ins_rej_agmt_id.SRC_SYS_CD as SRC_SYS_CD,
flt_ins_rej_agmt_id.LOAD_DTTM as LOAD_DTTM
FROM
flt_ins_rej_agmt_id;


END; 
';