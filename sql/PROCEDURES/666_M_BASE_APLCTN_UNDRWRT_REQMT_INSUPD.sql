-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_UNDRWRT_REQMT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 DECLARE
  END_DTTM STRING;
  PRCS_ID int;
  START_DTTM STRING;
  run_id STRING;
  --workflow_name STRING;
  --session_name STRING;
BEGIN
/*  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
*/

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


-- Component SQ_pc_uwissuehistory, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_uwissuehistory AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Rank,
$2 as UpdateTime,
$3 as JobNumber,
$4 as TYPECODE_job,
$5 as Code_uwissuetype,
$6 as CreateTime,
$7 as Reqmt_UpdateTime,
$8 as SYS_SRC_CD,
$9 as PublicID_Update,
$10 as PRTY_APLCTN_ROLE_CD,
$11 as busn_start_dt,
$12 as busn_end_dt,
$13 as uwissueblockingpoint_Typecode,
$14 as IssueKey,
$15 as LKP_APLCTN_TYPE_CD,
$16 as LKP_CODE_UWISSUE_TYPE,
$17 as LKP_SYS_SRC_CD,
$18 as LKP_UWISSUEBLOCKPOINT_TYPECD,
$19 as LKP_PRTY_APLCTN_ROLE_CD,
$20 as LKP_INDIV_PRTY_ID,
$21 as LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
$22 as LKP_APLCTN_ID,
$23 as TGT_LKP_APLCTN_ID,
$24 as TGT_LKP_UNDRWRTG_REQMT_TYPE_CD,
$25 as TGT_LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
$26 as TGT_LKP_PRTY_APLCTN_ROLE_CD,
$27 as TGT_LKP_REQMT_CRT_DTTM,
$28 as TGT_LKP_REQMT_UPDT_DTTM,
$29 as TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM,
$30 as TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM,
$31 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select Rnk, updatetime, JobNumber, TYPECODE_job,Code_uwissuetype,created_date,

timestamp1,SRC_SYS,PublicID_Update,

PRTY_APLCTN_ROLE_CD,busn_start_dt,busn_end_dt,

uwissueblockingpoint_Typecode,issuekey

,LKP_APLCTN_TYPE_CD

,LKP_CODE_UWISSUE_TYPE 

,LKP_SYS_SRC_CD

,LKP_UWISSUEBLOCKPOINT_TYPECD

,LKP_PRTY_APLCTN_ROLE_CD

,LKP_INDIV_PRTY_ID

,LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

,LKP_APLCTN_ID

,TGT_lkp.APLCTN_ID AS TGT_LKP_APLCTN_ID

,TGT_lkp.UNDRWRTG_REQMT_TYPE_CD AS TGT_LKP_UNDRWRTG_REQMT_TYPE_CD

,TGT_lkp.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID AS TGT_LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

,TGT_lkp.PRTY_APLCTN_ROLE_CD1 AS TGT_LKP_PRTY_APLCTN_ROLE_CD

,TGT_lkp.REQMT_CRT_DTTM AS TGT_LKP_REQMT_CRT_DTTM

,TGT_lkp.REQMT_UPDT_DTTM AS TGT_LKP_REQMT_UPDT_DTTM

,TGT_lkp.APLCTN_UNDRWRT_REQMT_STRT_DTTM AS TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM

,TGT_lkp.APLCTN_UNDRWRT_REQMT_END_DTTM AS TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM



from (



select Rnk, updatetime, JobNumber, TYPECODE_job,Code_uwissuetype,created_date,

timestamp1,SRC_SYS,PublicID_Update,

PRTY_APLCTN_ROLE_CD,busn_start_dt,busn_end_dt,

uwissueblockingpoint_Typecode,issuekey

,LKP_APLCTN_TYPE_CD

,LKP_CODE_UWISSUE_TYPE 

,LKP_SYS_SRC_CD

,LKP_UWISSUEBLOCKPOINT_TYPECD

,LKP_PRTY_APLCTN_ROLE_CD

,LKP_INDIV_PRTY_ID

,LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

,APLCTN_ID.APLCTN_ID AS LKP_APLCTN_ID 

from (



select Rnk, updatetime, JobNumber, TYPECODE_job,Code_uwissuetype,created_date,

timestamp1,SRC_SYS,PublicID_Update,

PRTY_APLCTN_ROLE_CD,busn_start_dt,busn_end_dt,

uwissueblockingpoint_Typecode,issuekey

/* LKP_APLCTN_TYPE_CD  */
, Case When TRIM( XLAT_APLCTN_TYPE_CD.TGT_IDNTFTN_VAL) IS NULL THEN ''UNK''

 Else TRIM(XLAT_APLCTN_TYPE_CD.TGT_IDNTFTN_VAL)

 END AS LKP_APLCTN_TYPE_CD

/* LKP_CODE_UWISSUE_TYPE */
, Case When TRIM( XLAT_CODE_UWISSUE_TYPE.TGT_IDNTFTN_VAL) IS NULL THEN ''UNK''

 Else TRIM(XLAT_CODE_UWISSUE_TYPE.TGT_IDNTFTN_VAL)

 END AS LKP_CODE_UWISSUE_TYPE 

/* LKP_SYS_SRC_CD */
 ,TRIM(XLAT_SYS_SRC_CD.TGT_IDNTFTN_VAL)

 AS LKP_SYS_SRC_CD

/* LKP_UWISSUEBLOCKPOINT_TYPECD */
 , Case When TRIM(XLAT_UWISSUEBLOCKPOINT_TYPECD.TGT_IDNTFTN_VAL) IS NULL THEN ''UNK''

 Else TRIM(XLAT_UWISSUEBLOCKPOINT_TYPECD.TGT_IDNTFTN_VAL)

 END AS LKP_UWISSUEBLOCKPOINT_TYPECD 

/* LKP_PRTY_APLCTN_ROLE_CD */
 ,TRIM(XLAT_PRTY_APLCTN_ROLE_CD.TGT_IDNTFTN_VAL)

 AS LKP_PRTY_APLCTN_ROLE_CD

/* LKP_INDIV_PRTY_ID */
 ,TRIM(INDIV_PRTY_ID.INDIV_PRTY_ID)

 AS LKP_INDIV_PRTY_ID

/* LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID */
 ,APLCTN_UNDRWRT_REQMT_ISSU_KEY.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID 

 AS LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

from (



select Rnk, updatetime, JobNumber, TYPECODE_job,Code_uwissuetype,created_date,

timestamp1,SRC_SYS,PublicID_Update,

PRTY_APLCTN_ROLE_CD,busn_start_dt,busn_end_dt,

uwissueblockingpoint_Typecode,issuekey

from (

(

select distinct dense_rank() over (partition by  JOBNUMBER,TYPECODE_job,Code_uwissuetype

/*  ,uwissueblockingpoint_Typecode ,PublicID_Update */
order by busn_start_dt, updatetime) Rnk, b.* from

( select updatetime, JobNumber,  TYPECODE_job,Code_uwissuetype,created_date,timestamp1,SRC_SYS,PublicID_Update,PRTY_APLCTN_ROLE_CD,busn_start_dt,busn_end_dt,uwissueblockingpoint_Typecode,issuekey from 

( SELECT  DISTINCT pc_uwissuehistory.updatetime, pc_uwissuehistory.JobNumber, pc_uwissuehistory.TYPECODE_job, pc_uwissuehistory.Code_uwissuetype,

pc_uwissuehistory.createtime as created_date

,to_date(''9999-12-31'',''yyyy-mm-dd'')  as timestamp1,''SRC_SYS4'' as SRC_SYS,

pc_uwissuehistory.PublicID_Update,

''PRTY_APLCTN_ROLE3'' as PRTY_APLCTN_ROLE_CD,

 pc_uwissuehistory.createtime as busn_start_dt,

 pc_policyperiod.periodend  as busn_end_dt,uwissueblockingpoint_Typecode,pc_uwissuehistory.issuekey as issuekey

FROM  ( SELECT  pc_uwissuehistory.UpdateTime_stg as UpdateTime,

		pc_uwissuehistory.PolicyPeriodID_stg as policyperiodid,

		pc_job.JobNumber_stg as JobNumber,

		pctl_job.TYPECODE_stg as TYPECODE_job,

		pc_uwissuetype.Code_stg as Code_uwissuetype,

		pc_uwissuehistory.CreateTime_stg as CreateTime,

		UpdateContact.PublicID_stg AS PublicID_Update, 

		pctl_uwissueblockingpoint.Typecode_stg as uwissueblockingpoint_Typecode,

		pc_uwissuehistory.IssueKey_stg as IssueKey	

FROM    db_t_prod_stag.pc_uwissuehistory

        LEFT JOIN db_t_prod_stag.pc_policyperiod ON pc_policyperiod.id_stg = pc_uwissuehistory.PolicyPeriodid_stg

        LEFT JOIN db_t_prod_stag.pc_job ON pc_job.id_stg = pc_policyperiod.Jobid_stg

        LEFT JOIN db_t_prod_stag.pctl_job ON pc_job.Subtype_stg = pctl_job.id_stg

        LEFT JOIN db_t_prod_stag.pc_uwissuetype ON pc_uwissuehistory.IssueTypeid_stg = pc_uwissuetype.id_stg

        LEFT JOIN db_t_prod_stag.pc_user UpdateUser ON UpdateUser.id_stg = pc_uwissuehistory.UpdateUserid_stg

        LEFT JOIN db_t_prod_stag.pc_contact UpdateContact ON UpdateContact.id_stg = UpdateUser.Contactid_stg

		left join db_t_prod_stag.pctl_uwissueblockingpoint on pc_uwissuetype.BlockingPoint_stg = pctl_uwissueblockingpoint.id_stg

		WHERE pc_uwissuehistory.UpdateTime_stg > (:Start_dttm)

and pc_uwissuehistory.UpdateTime_stg <= (:End_dttm)

)pc_uwissuehistory

join ( SELECT distinct pc_policyperiod.PeriodEnd_stg as periodend, pc_policyperiod.ID_stg as id

from db_t_prod_stag.pc_policyperiod

WHERE pc_policyperiod.UpdateTime_stg > (:Start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:End_dttm)

) pc_policyperiod 

on pc_uwissuehistory.policyperiodid=pc_policyperiod.id

WHERE

 pc_uwissuehistory.TYPECODE_job in (''Submission'',''PolicyChange'',''Renewal'')			



UNION

		

SELECT DISTINCT updatetime, PCJOB_JOBNUMBER, PCTLJOB_TYPECODE,PC_UWISSUETYPE_CODE,PCUWREFRSN_CREATETIME,UPDATETIME,

''SRC_SYS4'' as SRC_SYS,

pccontact_publicid as PublicID_Update,

''PRTY_APLCTN_ROLE3'' as PRTY_APLCTN_ROLE_CD,

case when PCUWREFRSN_CREATETIME is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else PCUWREFRSN_CREATETIME end as busn_start_dt,

to_date (''1900-01-01'' , ''yyyy-mm-dd'') as busn_end_dt,cast(null  as varchar(50))as uwissueblockingpoint_Typecode,'''' as issuekey

FROM ( select pc_uwreferralreason.CreateTime_stg as PCUWREFRSN_CREATETIME

,pc_job.JobNumber_stg as PCJOB_JOBNUMBER

,pctl_job.TYPECODE_stg as PCTLJOB_TYPECODE

,pc_contact.PublicID_stg as pccontact_publicid

,pc_uwissuetype.Code_stg as PC_UWISSUETYPE_CODE

,pc_uwreferralreason.Updatetime_stg as UPDATETIME

from  db_t_prod_stag.pc_uwreferralreason

left join db_t_prod_stag.pc_uwissuetype on pc_uwreferralreason.IssueTypeID_stg=pc_uwissuetype.ID_stg

left join db_t_prod_stag.pc_policy on pc_policy.ID_stg=pc_uwreferralreason.Policy_stg

left join db_t_prod_stag.pc_policyperiod on pc_policyperiod.PolicyID_stg=pc_policy.ID_stg

left join db_t_prod_stag.pc_job on pc_job.ID_stg=pc_policyperiod.JobID_stg

left join db_t_prod_stag.pctl_job on pc_job.Subtype_stg=pctl_job.ID_stg

left join db_t_prod_stag.pc_user on pc_user.ID_stg=pc_uwreferralreason.CreateUserID_stg

left join db_t_prod_stag.pc_contact on pc_contact.ID_stg=pc_user.ContactID_stg

where pc_uwreferralreason.UpdateTime_stg> (:Start_dttm)

	and pc_uwreferralreason.UpdateTime_stg <= (:End_dttm)

)pc_aplctn_undrwrt_reqmt_x

WHERE PCTLJOB_TYPECODE in (''Submission'',''PolicyChange'',''Renewal'')

) as x

)b

) SQ)

QUALIFY ROW_NUMBER() OVER(PARTITION BY JobNumber,  TYPECODE_job, Code_uwissuetype,  PublicID_Update, PRTY_APLCTN_ROLE_CD,uwissueblockingpoint_Typecode ,lower(issuekey) --(NOT CASESPECIFIC)

ORDER BY busn_start_dt desc, updatetime desc) = 1

) as SRC



/* TYPECODE_job */
LEFT OUTER JOIN 

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT
	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''APLCTN_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_job.Typecode''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_APLCTN_TYPE_CD 

ON XLAT_APLCTN_TYPE_CD.SRC_IDNTFTN_VAL  = TRIM(SRC.TYPECODE_job)



/* Code_uwissuetype */
LEFT OUTER JOIN 

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT
	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''UNDRWRTG_REQMT_TYPE'' 

    AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pc_uwissuetype.code'' 

    AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

		qualify row_number() over (partition by SRC_IDNTFTN_VAL order by expn_dt desc,eff_dt desc)=1

)XLAT_CODE_UWISSUE_TYPE 

ON XLAT_CODE_UWISSUE_TYPE.SRC_IDNTFTN_VAL  = TRIM(SRC.Code_uwissuetype)



/* SRC_SYS_CD */
LEFT OUTER JOIN 

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_SYS_SRC_CD 

ON TRIM(XLAT_SYS_SRC_CD.SRC_IDNTFTN_VAL)  = TRIM(SRC.SRC_SYS)



/* lkp_uwissueblockingpoint_Typecode */
LEFT OUTER JOIN 

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT
	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''APLCTN_BLCKG_TYPE'' 

    AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_uwissueblockingpoint.typecode'' 

    AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_UWISSUEBLOCKPOINT_TYPECD

ON TRIM(XLAT_UWISSUEBLOCKPOINT_TYPECD.SRC_IDNTFTN_VAL)  = TRIM(SRC.uwissueblockingpoint_Typecode)



/* PRTY_APLCTN_ROLE_CD */
LEFT OUTER JOIN 

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT
	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_APLCTN_ROLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_PRTY_APLCTN_ROLE_CD 

ON XLAT_PRTY_APLCTN_ROLE_CD.SRC_IDNTFTN_VAL  = TRIM(SRC.PRTY_APLCTN_ROLE_CD)



/* INDIV_PRTY_ID */
LEFT OUTER JOIN 

(

SELECT 

	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 

	INDIV.NK_PUBLC_ID as NK_PUBLC_ID  

FROM 

	--EVIEWDB_EDW.INDIV
	db_t_prod_core.INDIV

WHERE 

	INDIV.NK_PUBLC_ID IS NOT NULL

	AND INDIV.EDW_END_DTTM=to_timestamp_ntz(''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'')

)INDIV_PRTY_ID 

ON INDIV_PRTY_ID.NK_PUBLC_ID  = TRIM(SRC.PublicID_Update)



/* APLCTN_UNDRWRT_REQ_ISSU_KEY_ID */
LEFT OUTER JOIN 

(

SELECT 

	APLCTN_UNDRWRT_REQMT_ISSU_KEY.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID, 

	UPPER(APLCTN_UNDRWRT_REQMT_ISSU_KEY.HOST_ISSU_KEY) as HOST_ISSU_KEY  

FROM 

	--EVIEWDB_EDW.APLCTN_UNDRWRT_REQMT_ISSU_KEY
	db_t_prod_core.APLCTN_UNDRWRT_REQMT_ISSU_KEY

)APLCTN_UNDRWRT_REQMT_ISSU_KEY 

ON TRIM(APLCTN_UNDRWRT_REQMT_ISSU_KEY.HOST_ISSU_KEY)  = TRIM(UPPER(SRC.issuekey))

)as SRC1



/* APLCTN_ID */
LEFT OUTER JOIN 

(

SELECT 

	APLCTN.APLCTN_ID as APLCTN_ID, 

	APLCTN.HOST_APLCTN_ID as HOST_APLCTN_ID,  

	APLCTN.SRC_SYS_CD as SRC_SYS_CD, 

	APLCTN.APLCTN_TYPE_CD as APLCTN_TYPE_CD,

	APLCTN.EDW_END_DTTM as EDW_END_DTTM

FROM 

	--EVIEWDB_EDW.APLCTN
	db_t_prod_core.APLCTN

/* WHERE 	APLCTN.EDW_END_DTTM=CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP FORMAT ''YYYY-MM-DDBHH:MI:SS.S(6)'') */
QUALIFY ROW_NUMBER () OVER (partition by HOST_APLCTN_ID,SRC_SYS_CD order by EDW_END_DTTM desc)=1

)APLCTN_ID 

ON APLCTN_ID.HOST_APLCTN_ID  = TRIM(SRC1.JobNumber)

AND APLCTN_ID.SRC_SYS_CD  = TRIM(SRC1.LKP_SYS_SRC_CD)

AND APLCTN_ID.APLCTN_TYPE_CD  = TRIM(SRC1.LKP_APLCTN_TYPE_CD)

/* QUALIFY ROW_NUMBER () OVER (partition by HOST_APLCTN_ID,SRC_SYS_CD order by EDW_END_DTTM desc)=1 */
)as SRC2



/* TGT_LKP */
LEFT OUTER JOIN 

(

SELECT 

	APLCTN_UNDRWRT_REQMT.APLCTN_ID as APLCTN_ID, 

	APLCTN_UNDRWRT_REQMT.UNDRWRTG_REQMT_TYPE_CD as UNDRWRTG_REQMT_TYPE_CD,

	APLCTN_UNDRWRT_REQMT.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,

	APLCTN_UNDRWRT_REQMT.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD1,

	APLCTN_UNDRWRT_REQMT.REQMT_CRT_DTTM as REQMT_CRT_DTTM,

	APLCTN_UNDRWRT_REQMT.REQMT_UPDT_DTTM as REQMT_UPDT_DTTM,

	APLCTN_UNDRWRT_REQMT.APLCTN_UNDRWRT_REQMT_STRT_DTTM as APLCTN_UNDRWRT_REQMT_STRT_DTTM,

	APLCTN_UNDRWRT_REQMT.APLCTN_UNDRWRT_REQMT_END_DTTM as APLCTN_UNDRWRT_REQMT_END_DTTM,	

	APLCTN_UNDRWRT_REQMT.EDW_END_DTTM as EDW_END_DTTM

FROM 

	--EVIEWDB_EDW.APLCTN_UNDRWRT_REQMT
	db_t_prod_core.APLCTN_UNDRWRT_REQMT

/* WHERE 	APLCTN_UNDRWRT_REQMT.EDW_END_DTTM=CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP FORMAT ''YYYY-MM-DDBHH:MI:SS.S(6)'') */
QUALIFY	ROW_NUMBER() OVER(PARTITION BY APLCTN_ID, UNDRWRTG_REQMT_TYPE_CD,APLCTN_UNDRWRT_REQ_ISSU_KEY_ID ORDER BY EDW_END_DTTM desc) = 1

)TGT_LKP 

ON TGT_lkp.APLCTN_ID  = SRC2.LKP_APLCTN_ID

AND TGT_lkp.UNDRWRTG_REQMT_TYPE_CD = TRIM(SRC2.LKP_CODE_UWISSUE_TYPE)

AND TGT_lkp.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID = SRC2.LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

/* QUALIFY	ROW_NUMBER() OVER(PARTITION BY APLCTN_ID, UNDRWRTG_REQMT_TYPE_CD,APLCTN_UNDRWRT_REQ_ISSU_KEY_ID ORDER BY EDW_END_DTTM desc) = 1 */
order by LKP_APLCTN_ID, LKP_CODE_UWISSUE_TYPE , LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID, updatetime
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_uwissuehistory.CreateTime as CreateTime,
SQ_pc_uwissuehistory.Reqmt_UpdateTime as Reqmt_UpdateTime,
SQ_pc_uwissuehistory.UpdateTime as UpdateTime,
CASE WHEN SQ_pc_uwissuehistory.busn_start_dt IS NULL THEN to_date (''01/01/1900'' , ''mm/dd/yyyy'') ELSE SQ_pc_uwissuehistory.busn_start_dt END as o_busn_start_dt,
CASE WHEN SQ_pc_uwissuehistory.busn_end_dt IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE SQ_pc_uwissuehistory.busn_end_dt END as o_busn_end_dt,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp_ntz (''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
SQ_pc_uwissuehistory.Rank as Rank,
SQ_pc_uwissuehistory.LKP_CODE_UWISSUE_TYPE as LKP_CODE_UWISSUE_TYPE,
SQ_pc_uwissuehistory.LKP_UWISSUEBLOCKPOINT_TYPECD as LKP_UWISSUEBLOCKPOINT_TYPECD,
SQ_pc_uwissuehistory.LKP_PRTY_APLCTN_ROLE_CD as LKP_PRTY_APLCTN_ROLE_CD,
SQ_pc_uwissuehistory.LKP_INDIV_PRTY_ID as LKP_INDIV_PRTY_ID,
SQ_pc_uwissuehistory.LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
SQ_pc_uwissuehistory.LKP_APLCTN_ID as LKP_APLCTN_ID,
SQ_pc_uwissuehistory.TGT_LKP_APLCTN_ID as TGT_LKP_APLCTN_ID,
SQ_pc_uwissuehistory.TGT_LKP_UNDRWRTG_REQMT_TYPE_CD as TGT_LKP_UNDRWRTG_REQMT_TYPE_CD,
SQ_pc_uwissuehistory.TGT_LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as TGT_LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
SQ_pc_uwissuehistory.TGT_LKP_PRTY_APLCTN_ROLE_CD as TGT_LKP_PRTY_APLCTN_ROLE_CD,
SQ_pc_uwissuehistory.TGT_LKP_REQMT_CRT_DTTM as TGT_LKP_REQMT_CRT_DTTM,
SQ_pc_uwissuehistory.TGT_LKP_REQMT_UPDT_DTTM as TGT_LKP_REQMT_UPDT_DTTM,
SQ_pc_uwissuehistory.TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM as TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM,
SQ_pc_uwissuehistory.TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM as TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM,
SQ_pc_uwissuehistory.source_record_id
FROM
SQ_pc_uwissuehistory
);


-- Component exp_calc_insert_update_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_calc_insert_update_flag AS
(
SELECT
exp_pass_from_src.TGT_LKP_APLCTN_ID as lkp_APLCTN_ID,
exp_pass_from_src.TGT_LKP_UNDRWRTG_REQMT_TYPE_CD as lkp_UNDRWRTG_REQMT_TYPE_CD,
exp_pass_from_src.TGT_LKP_PRTY_APLCTN_ROLE_CD as lkp_PRTY_APLCTN_ROLE_CD,
exp_pass_from_src.LKP_APLCTN_ID as in_APLCTN_ID,
exp_pass_from_src.LKP_CODE_UWISSUE_TYPE as in_Code_uwissuetype,
exp_pass_from_src.LKP_PRTY_APLCTN_ROLE_CD as in_PRTY_APLCTN_ROLE_CD,
CASE WHEN exp_pass_from_src.LKP_PRTY_APLCTN_ROLE_CD IS NULL THEN ''0'' ELSE exp_pass_from_src.LKP_PRTY_APLCTN_ROLE_CD END as v_in_PRTY_APLCTN_ROLE_CD,
CASE WHEN exp_pass_from_src.TGT_LKP_PRTY_APLCTN_ROLE_CD IS NULL THEN ''0'' ELSE exp_pass_from_src.TGT_LKP_PRTY_APLCTN_ROLE_CD END as v_lkp_PRTY_APLCTN_ROLE_CD,
:PRCS_ID as out_PRCS_ID,
CASE WHEN exp_pass_from_src.TGT_LKP_APLCTN_ID IS NULL THEN 1 ELSE 0 END as InsertFlag,
CASE WHEN exp_pass_from_src.TGT_LKP_APLCTN_ID IS NOT NULL AND ( v_lkp_PRTY_APLCTN_ROLE_CD <> v_in_PRTY_APLCTN_ROLE_CD ) THEN 1 ELSE 0 END as UpdateFlag,
exp_pass_from_src.CreateTime as CreateTime,
exp_pass_from_src.UpdateTime as UpdateTime,
exp_pass_from_src.Reqmt_UpdateTime as Reqmt_UpdateTime,
exp_pass_from_src.LKP_INDIV_PRTY_ID as INDIV_PRTY_ID,
exp_pass_from_src.o_busn_start_dt as busn_start_dt,
exp_pass_from_src.o_busn_end_dt as busn_end_dt,
exp_pass_from_src.LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
exp_pass_from_src.TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM as LKP_APLCTN_UNDRWRT_REQMT_STRT_DT,
exp_pass_from_src.TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM as LKP_APLCTN_UNDRWRT_REQMT_END_DT,
exp_pass_from_src.TGT_LKP_REQMT_CRT_DTTM as LKP_REQMT_CRT_DTTM,
exp_pass_from_src.TGT_LKP_REQMT_UPDT_DTTM as LKP_REQMT_UPDT_DTTM,
exp_pass_from_src.TGT_LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
md5 ( to_char ( exp_pass_from_src.TGT_LKP_APLCTN_UNDRWRT_REQMT_STRT_DTTM , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.TGT_LKP_APLCTN_UNDRWRT_REQMT_END_DTTM , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.TGT_LKP_REQMT_CRT_DTTM , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.TGT_LKP_REQMT_UPDT_DTTM , ''yyyy-mm-dd'' ) || ltrim ( rtrim ( exp_pass_from_src.TGT_LKP_PRTY_APLCTN_ROLE_CD ) ) ) as CHKSUM_LKP,
MD5 ( to_char ( exp_pass_from_src.o_busn_start_dt , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.o_busn_end_dt , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.CreateTime , ''yyyy-mm-dd'' ) || to_char ( exp_pass_from_src.Reqmt_UpdateTime , ''yyyy-mm-dd'' ) || ltrim ( rtrim ( exp_pass_from_src.LKP_PRTY_APLCTN_ROLE_CD ) ) ) as CHKSUM_INP,
NULL as flag,
exp_pass_from_src.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_from_src.EDW_END_DTTM as EDW_END_DTTM,
DATEADD(''SECOND'', - 1, exp_pass_from_src.EDW_STRT_DTTM) as EDW_expiry,
exp_pass_from_src.LKP_UWISSUEBLOCKPOINT_TYPECD as uwissueblockingpoint_Typecode1,
exp_pass_from_src.Rank as Rank,
CASE WHEN CHKSUM_LKP IS NULL THEN ''I'' ELSE CASE WHEN CHKSUM_LKP != CHKSUM_INP THEN ''U'' ELSE ''R'' END END as o_Ins_Upd,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
);


-- Component rtr_aplctn_undrwrt_reqmt_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_aplctn_undrwrt_reqmt_INSERT as
(
SELECT
exp_calc_insert_update_flag.InsertFlag as InsertFlag,
exp_calc_insert_update_flag.UpdateFlag as UpdateFlag,
exp_calc_insert_update_flag.in_APLCTN_ID as in_APLCTN_ID,
exp_calc_insert_update_flag.in_Code_uwissuetype as in_Code_uwissuetype,
exp_calc_insert_update_flag.in_PRTY_APLCTN_ROLE_CD as in_PRTY_APLCTN_ROLE_CD,
exp_calc_insert_update_flag.out_PRCS_ID as out_PRCS_ID,
exp_calc_insert_update_flag.lkp_APLCTN_ID as lkp_APLCTN_ID,
exp_calc_insert_update_flag.lkp_UNDRWRTG_REQMT_TYPE_CD as lkp_UNDRWRTG_REQMT_TYPE_CD,
exp_calc_insert_update_flag.CreateTime as CreateTime,
exp_calc_insert_update_flag.UpdateTime as UpdateTime,
exp_calc_insert_update_flag.Reqmt_UpdateTime as Reqmt_Updt_Dttm,
exp_calc_insert_update_flag.INDIV_PRTY_ID as INDIV_PRTY_ID,
exp_calc_insert_update_flag.busn_start_dt as busn_start_dt,
exp_calc_insert_update_flag.busn_end_dt as busn_end_dt,
exp_calc_insert_update_flag.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
exp_calc_insert_update_flag.flag as flag,
exp_calc_insert_update_flag.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_calc_insert_update_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_calc_insert_update_flag.EDW_expiry as EDW_expiry,
exp_calc_insert_update_flag.LKP_APLCTN_UNDRWRT_REQMT_STRT_DT as LKP_APLCTN_UNDRWRT_REQMT_STRT_DT,
exp_calc_insert_update_flag.LKP_APLCTN_UNDRWRT_REQMT_END_DT as LKP_APLCTN_UNDRWRT_REQMT_END_DT,
exp_calc_insert_update_flag.LKP_REQMT_CRT_DTTM as LKP_REQMT_CRT_DTTM,
exp_calc_insert_update_flag.LKP_REQMT_UPDT_DTTM as LKP_REQMT_UPDT_DTTM,
exp_calc_insert_update_flag.lkp_PRTY_APLCTN_ROLE_CD as lkp_PRTY_APLCTN_ROLE_CD,
exp_calc_insert_update_flag.LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as LKP_APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
exp_calc_insert_update_flag.uwissueblockingpoint_Typecode1 as uwissueblockingpoint_Typecode1,
exp_calc_insert_update_flag.o_Ins_Upd as o_Ins_Upd,
exp_calc_insert_update_flag.Rank as Rank,
exp_calc_insert_update_flag.source_record_id
FROM
exp_calc_insert_update_flag
WHERE exp_calc_insert_update_flag.o_Ins_Upd = ''U'' AND exp_calc_insert_update_flag.in_APLCTN_ID IS NOT NULL AND exp_calc_insert_update_flag.INDIV_PRTY_ID IS NOT NULL AND exp_calc_insert_update_flag.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID IS NOT NULL or exp_calc_insert_update_flag.o_Ins_Upd = ''I'' AND exp_calc_insert_update_flag.in_APLCTN_ID IS NOT NULL AND exp_calc_insert_update_flag.INDIV_PRTY_ID IS NOT NULL AND exp_calc_insert_update_flag.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID IS NOT NULL
 );


-- Component upd_aplctn_undrwrt_reqmt_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_aplctn_undrwrt_reqmt_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_aplctn_undrwrt_reqmt_INSERT.in_APLCTN_ID as APLCTN_ID,
rtr_aplctn_undrwrt_reqmt_INSERT.in_Code_uwissuetype as UNDRWRTG_REQMT_TYPE_CD,
rtr_aplctn_undrwrt_reqmt_INSERT.in_PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
rtr_aplctn_undrwrt_reqmt_INSERT.out_PRCS_ID as PRCS_ID,
rtr_aplctn_undrwrt_reqmt_INSERT.CreateTime as REQMT_CRT_DTTM,
rtr_aplctn_undrwrt_reqmt_INSERT.Reqmt_Updt_Dttm as Reqmt_Updt_Dttm,
rtr_aplctn_undrwrt_reqmt_INSERT.INDIV_PRTY_ID as INDIV_PRTY_ID1,
rtr_aplctn_undrwrt_reqmt_INSERT.busn_start_dt as busn_start_dt1,
rtr_aplctn_undrwrt_reqmt_INSERT.busn_end_dt as busn_end_dt1,
rtr_aplctn_undrwrt_reqmt_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_aplctn_undrwrt_reqmt_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_aplctn_undrwrt_reqmt_INSERT.uwissueblockingpoint_Typecode1 as uwissueblockingpoint_Typecode11,
rtr_aplctn_undrwrt_reqmt_INSERT.Rank as Rank1,
rtr_aplctn_undrwrt_reqmt_INSERT.UpdateTime as UpdateTime1,
rtr_aplctn_undrwrt_reqmt_INSERT.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID1,
0 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_aplctn_undrwrt_reqmt_INSERT
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_aplctn_undrwrt_reqmt_ins.APLCTN_ID as APLCTN_ID,
upd_aplctn_undrwrt_reqmt_ins.UNDRWRTG_REQMT_TYPE_CD as UNDRWRTG_REQMT_TYPE_CD,
upd_aplctn_undrwrt_reqmt_ins.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
upd_aplctn_undrwrt_reqmt_ins.PRCS_ID as PRCS_ID,
upd_aplctn_undrwrt_reqmt_ins.REQMT_CRT_DTTM as REQMT_CRT_DTTM,
upd_aplctn_undrwrt_reqmt_ins.Reqmt_Updt_Dttm as Reqmt_Updt_Dttm,
upd_aplctn_undrwrt_reqmt_ins.INDIV_PRTY_ID1 as INDIV_PRTY_ID1,
upd_aplctn_undrwrt_reqmt_ins.busn_start_dt1 as busn_start_dt1,
upd_aplctn_undrwrt_reqmt_ins.busn_end_dt1 as busn_end_dt1,
DATEADD(''SECOND'', ( 2 * ( upd_aplctn_undrwrt_reqmt_ins.Rank1 - 1 ) ), upd_aplctn_undrwrt_reqmt_ins.EDW_STRT_DTTM1) as out_EDW_STRT_DTTM1,
upd_aplctn_undrwrt_reqmt_ins.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_aplctn_undrwrt_reqmt_ins.uwissueblockingpoint_Typecode11 as uwissueblockingpoint_Typecode11,
upd_aplctn_undrwrt_reqmt_ins.UpdateTime1 as UpdateTime1,
upd_aplctn_undrwrt_reqmt_ins.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID1 as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID1,
upd_aplctn_undrwrt_reqmt_ins.source_record_id
FROM
upd_aplctn_undrwrt_reqmt_ins
);


-- Component tgt_APLCTN_UNDRWRT_REQMT_ins, Type TARGET 
INSERT INTO db_t_prod_core.APLCTN_UNDRWRT_REQMT
(
APLCTN_ID,
UNDRWRTG_REQMT_TYPE_CD,
APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
APLCTN_PRTY_STRT_DTTM,
PRTY_APLCTN_ROLE_CD,
PRTY_ID,
REQMT_CRT_DTTM,
REQMT_UPDT_DTTM,
PRCS_ID,
APLCTN_UNDRWRT_REQMT_STRT_DTTM,
APLCTN_UNDRWRT_REQMT_END_DTTM,
APLCTN_BLCKG_TYPE_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins.APLCTN_ID as APLCTN_ID,
exp_pass_to_tgt_ins.UNDRWRTG_REQMT_TYPE_CD as UNDRWRTG_REQMT_TYPE_CD,
exp_pass_to_tgt_ins.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID1 as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
exp_pass_to_tgt_ins.busn_start_dt1 as APLCTN_PRTY_STRT_DTTM,
exp_pass_to_tgt_ins.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
exp_pass_to_tgt_ins.INDIV_PRTY_ID1 as PRTY_ID,
exp_pass_to_tgt_ins.REQMT_CRT_DTTM as REQMT_CRT_DTTM,
exp_pass_to_tgt_ins.Reqmt_Updt_Dttm as REQMT_UPDT_DTTM,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.busn_start_dt1 as APLCTN_UNDRWRT_REQMT_STRT_DTTM,
exp_pass_to_tgt_ins.busn_end_dt1 as APLCTN_UNDRWRT_REQMT_END_DTTM,
exp_pass_to_tgt_ins.uwissueblockingpoint_Typecode11 as APLCTN_BLCKG_TYPE_CD,
exp_pass_to_tgt_ins.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.UpdateTime1 as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component tgt_APLCTN_UNDRWRT_REQMT_ins, Type Post SQL 
UPDATE  db_t_prod_core.APLCTN_UNDRWRT_REQMT    
set TRANS_END_DTTM=  A.lead, 
EDW_END_DTTM=A.lead1
FROM
(

SELECT	distinct APLCTN_ID, UNDRWRTG_REQMT_TYPE_CD, APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,EDW_STRT_DTTM, APLCTN_UNDRWRT_REQMT_STRT_DTTM, 

max(EDW_STRT_DTTM) over (partition by APLCTN_ID, UNDRWRTG_REQMT_TYPE_CD,APLCTN_UNDRWRT_REQ_ISSU_KEY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by APLCTN_ID, UNDRWRTG_REQMT_TYPE_CD,APLCTN_UNDRWRT_REQ_ISSU_KEY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM db_t_prod_core.APLCTN_UNDRWRT_REQMT 

)  A

where  APLCTN_UNDRWRT_REQMT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and APLCTN_UNDRWRT_REQMT.APLCTN_ID=A.APLCTN_ID

and APLCTN_UNDRWRT_REQMT.UNDRWRTG_REQMT_TYPE_CD=A.UNDRWRTG_REQMT_TYPE_CD

and 

APLCTN_UNDRWRT_REQMT.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID=A.APLCTN_UNDRWRT_REQ_ISSU_KEY_ID

AND APLCTN_UNDRWRT_REQMT.APLCTN_UNDRWRT_REQMT_STRT_DTTM=A.APLCTN_UNDRWRT_REQMT_STRT_DTTM

and APLCTN_UNDRWRT_REQMT.TRANS_STRT_DTTM <>APLCTN_UNDRWRT_REQMT.TRANS_END_DTTM

and lead is not null



/*

UPDATE  APLCTN_UNDRWRT_REQMT  FROM  

(

SELECT	distinct APLCTN_ID,UNDRWRTG_REQMT_TYPE_CD,EDW_STRT_DTTM,PRTY_ID, TRANS_STRT_DTTM

FROM	APLCTN_UNDRWRT_REQMT 

WHERE EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.NS'')

QUALIFY ROW_NUMBER() OVER(PARTITION BY APLCTN_ID,UNDRWRTG_REQMT_TYPE_CD  ORDER BY APLCTN_UNDRWRT_REQMT_STRT_DTTM DESC) >1

)  A

SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1'' SECOND,

TRANS_END_DTTM= A.TRANS_STRT_DTTM+ INTERVAL ''1'' SECOND

WHERE  APLCTN_UNDRWRT_REQMT.APLCTN_ID=A.APLCTN_ID

AND  APLCTN_UNDRWRT_REQMT.EDW_STRT_DTTM=A.EDW_STRT_DTTM

AND  APLCTN_UNDRWRT_REQMT.TRANS_STRT_DTTM=A.TRANS_STRT_DTTM

AND APLCTN_UNDRWRT_REQMT.UNDRWRTG_REQMT_TYPE_CD=A.UNDRWRTG_REQMT_TYPE_CD

AND APLCTN_UNDRWRT_REQMT.PRTY_ID=A.PRTY_ID

*/;


END; 
';