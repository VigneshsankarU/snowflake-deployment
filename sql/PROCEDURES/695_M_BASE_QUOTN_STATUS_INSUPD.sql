-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_STATUS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '  
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''QUOTN_STS_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_policyperiodstatus.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_pc_job, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_job AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JobNumber,
$2 as BranchNumber,
$3 as TYPECODE,
$4 as UpdateTime,
$5 as EditEffectiveDate,
$6 as Rank,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	

distinct pc_job.JobNumber_stg as JobNumber,

CAST(pc_policyperiod.branchnumber_stg AS varchar(255)) as BranchNumber ,

pctl_policyperiodstatus.TYPECODE_stg as TYPECODE_policyperiodstatus,

pc_policyperiod.UpdateTime_stg as UpdateTime, 

pc_policyperiod.EditEffectivedate_stg as EditEffectivedate, 

rank() over (partition by JobNumber,BranchNumber order by UpdateTime, EditEffectivedate) rk



from DB_T_PROD_STAG.pc_job inner join DB_T_PROD_STAG.pctl_job pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg

LEFT  OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields on pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg

left outer join DB_T_PROD_STAG.pcx_holineratingfactor_alfa on pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg



WHERE  

pc_policyperiod.UpdateTime_stg > (:start_dttm) 

and pc_policyperiod.UpdateTime_stg <= (:end_dttm) 

and pctl_policyperiodstatus.typecode_stg <> ''Temporary'' 

and pc_effectivedatedfields.expirationdate_stg is null

and pcx_holineratingfactor_alfa.ExpirationDate_stg is  null

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') 

and pc_policyperiod.PolicyNumber_stg is not null



QUALIFY	ROW_NUMBER() over (partition by JobNumber, BranchNumber,TYPECODE_policyperiodstatus order by UpdateTime desc, EditEffectivedate desc)=1

order by UpdateTime
) SRC
)
);


-- Component exp_pass_through_expression, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_expression AS
(
SELECT
sq_pc_job.JobNumber as JobNumber,
''9999/12/31 23:59:59.999999'' as v_end_dt,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as v_lkp_xlat,
sq_pc_job.UpdateTime as UpdateTime,
TO_DATE ( v_end_dt , ''yyyy/mm/dd HH24:MI:SS.FF6'' ) as o_end_dt,
v_lkp_xlat as o_tgt_qtn_sts,
sq_pc_job.BranchNumber as BranchNumber,
sq_pc_job.EditEffectiveDate as EditEffectiveDate,
sq_pc_job.Rank as Rank,
sq_pc_job.source_record_id,
row_number() over (partition by sq_pc_job.source_record_id order by sq_pc_job.source_record_id) as RNK
FROM
sq_pc_job
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_job.TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_job.TYPECODE
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_through_expression.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through_expression.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_through_expression
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_through_expression.JobNumber AND LKP.VERS_NBR = exp_pass_through_expression.BranchNumber
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_STS AS
(
SELECT
LKP.QUOTN_ID,
LKP.QUOTN_STS_STRT_DTTM,
LKP.QUOTN_STS_TYPE_CD,
LKP.QUOTN_STS_END_DTTM,
LKP.NK_JOB_NBR,
LKP.EDW_STRT_DTTM,
LKP_INSRNC_QUOTN.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_INSRNC_QUOTN.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.QUOTN_STS_STRT_DTTM asc,LKP.QUOTN_STS_TYPE_CD asc,LKP.QUOTN_STS_END_DTTM asc,LKP.NK_JOB_NBR asc,LKP.EDW_STRT_DTTM asc) RNK
FROM
LKP_INSRNC_QUOTN
LEFT JOIN (
SELECT QUOTN_STS.QUOTN_STS_STRT_DTTM as QUOTN_STS_STRT_DTTM, 
QUOTN_STS.QUOTN_STS_TYPE_CD as QUOTN_STS_TYPE_CD, QUOTN_STS.QUOTN_STS_END_DTTM as QUOTN_STS_END_DTTM, 
QUOTN_STS.NK_JOB_NBR as NK_JOB_NBR, QUOTN_STS.EDW_STRT_DTTM as EDW_STRT_DTTM,  
QUOTN_STS.QUOTN_ID as QUOTN_ID FROM DB_T_PROD_CORE.QUOTN_STS
QUALIFY ROW_NUMBER () OVER (PARTITION BY QUOTN_ID ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.QUOTN_ID = LKP_INSRNC_QUOTN.QUOTN_ID
QUALIFY RNK = 1
);


-- Component fil_quotn_sts, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_quotn_sts AS
(
SELECT
LKP_INSRNC_QUOTN.QUOTN_ID as in_QUOTN_ID,
exp_pass_through_expression.EditEffectiveDate as in_QUOTN_STS_STRT_DTTM,
exp_pass_through_expression.o_tgt_qtn_sts as in_QUOTN_STS_TYPE_CD,
exp_pass_through_expression.o_end_dt as in_QUOTN_STS_END_DTTM,
exp_pass_through_expression.UpdateTime as in_UpdateTime,
exp_pass_through_expression.JobNumber as in_NK_JOB_NBR,
LKP_QUOTN_STS.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_STS.QUOTN_STS_STRT_DTTM as lkp_QUOTN_STS_STRT_DTTM,
LKP_QUOTN_STS.QUOTN_STS_TYPE_CD as lkp_QUOTN_STS_TYPE_CD,
LKP_QUOTN_STS.QUOTN_STS_END_DTTM as lkp_QUOTN_STS_END_DTTM,
LKP_QUOTN_STS.NK_JOB_NBR as lkp_NK_JOB_NBR,
LKP_QUOTN_STS.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as NewLookupRow,
NULL as in_QUOTN_STS_STRT_DTTM1,
exp_pass_through_expression.Rank as Rank,
exp_pass_through_expression.source_record_id
FROM
exp_pass_through_expression
LEFT JOIN LKP_INSRNC_QUOTN ON exp_pass_through_expression.source_record_id = LKP_INSRNC_QUOTN.source_record_id
LEFT JOIN LKP_QUOTN_STS ON LKP_INSRNC_QUOTN.source_record_id = LKP_QUOTN_STS.source_record_id
WHERE CASE WHEN DATE_TRUNC(DAY, exp_pass_through_expression.EditEffectiveDate) IS NULL THEN TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE DATE_TRUNC(DAY, exp_pass_through_expression.EditEffectiveDate) END > CASE WHEN LKP_QUOTN_STS.QUOTN_STS_STRT_DTTM IS NULL THEN TO_timestamp ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE LKP_QUOTN_STS.QUOTN_STS_STRT_DTTM END
);


-- Component exp_check_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_flag AS
(
SELECT
fil_quotn_sts.in_QUOTN_ID as in_QUOTN_ID,
fil_quotn_sts.in_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM,
fil_quotn_sts.in_QUOTN_STS_TYPE_CD as in_QUOTN_STS_TYPE_CD,
fil_quotn_sts.in_QUOTN_STS_END_DTTM as in_QUOTN_STS_END_DTTM,
fil_quotn_sts.in_UpdateTime as in_UpdateTime,
fil_quotn_sts.in_NK_JOB_NBR as in_NK_JOB_NBR,
:PRCS_ID as in_PRCS_ID,
fil_quotn_sts.lkp_QUOTN_ID as lkp_QUOTN_ID,
fil_quotn_sts.lkp_QUOTN_STS_STRT_DTTM as lkp_QUOTN_STS_STRT_DTTM,
fil_quotn_sts.lkp_QUOTN_STS_TYPE_CD as lkp_QUOTN_STS_TYPE_CD,
fil_quotn_sts.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
MD5 ( ltrim ( rtrim ( fil_quotn_sts.in_QUOTN_STS_STRT_DTTM ) ) || ltrim ( rtrim ( fil_quotn_sts.in_QUOTN_STS_TYPE_CD ) ) || ltrim ( rtrim ( fil_quotn_sts.in_QUOTN_STS_END_DTTM ) ) || ltrim ( rtrim ( fil_quotn_sts.in_NK_JOB_NBR ) ) ) as v_Src_MD5,
MD5 ( ltrim ( rtrim ( fil_quotn_sts.lkp_QUOTN_STS_STRT_DTTM ) ) || ltrim ( rtrim ( fil_quotn_sts.lkp_QUOTN_STS_TYPE_CD ) ) || ltrim ( rtrim ( fil_quotn_sts.lkp_QUOTN_STS_END_DTTM ) ) || ltrim ( rtrim ( fil_quotn_sts.lkp_NK_JOB_NBR ) ) ) as v_Tgt_MD5,
CASE WHEN v_Tgt_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_Src_MD5 = v_Tgt_MD5 THEN ''X'' ELSE ''U'' END END as o_Src_Tgt,
CURRENT_TIMESTAMP as StartDate,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EndDate,
fil_quotn_sts.NewLookupRow as NewLookupRow,
fil_quotn_sts.Rank as Rank,
fil_quotn_sts.source_record_id
FROM
fil_quotn_sts
);


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
(SELECT
exp_check_flag.in_QUOTN_ID as in_QUOTN_ID,
exp_check_flag.in_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM,
exp_check_flag.in_QUOTN_STS_TYPE_CD as in_QUOTN_STS_TYPE_CD,
exp_check_flag.in_QUOTN_STS_END_DTTM as in_QUOTN_STS_END_DTTM,
exp_check_flag.in_UpdateTime as in_UpdateTime,
exp_check_flag.in_NK_JOB_NBR as in_NK_JOB_NBR,
exp_check_flag.in_PRCS_ID as in_PRCS_ID,
exp_check_flag.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.o_Src_Tgt as o_Src_Tgt,
exp_check_flag.StartDate as StartDate,
exp_check_flag.EndDate as EndDate,
exp_check_flag.NewLookupRow as NewLookupRow,
exp_check_flag.lkp_QUOTN_STS_STRT_DTTM as lkp_QUOTN_STS_STRT_DTTM,
exp_check_flag.lkp_QUOTN_STS_TYPE_CD as lkp_QUOTN_STS_TYPE_CD,
exp_check_flag.Rank as Rank,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE ( exp_check_flag.o_Src_Tgt = ''I'' OR exp_check_flag.o_Src_Tgt = ''U'' ) and exp_check_flag.in_QUOTN_ID IS NOT NULL -- exp_check_flag.NewLookupRow = 1
);


-- Component rtr_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_UPDATE AS
(SELECT
exp_check_flag.in_QUOTN_ID as in_QUOTN_ID,
exp_check_flag.in_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM,
exp_check_flag.in_QUOTN_STS_TYPE_CD as in_QUOTN_STS_TYPE_CD,
exp_check_flag.in_QUOTN_STS_END_DTTM as in_QUOTN_STS_END_DTTM,
exp_check_flag.in_UpdateTime as in_UpdateTime,
exp_check_flag.in_NK_JOB_NBR as in_NK_JOB_NBR,
exp_check_flag.in_PRCS_ID as in_PRCS_ID,
exp_check_flag.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.o_Src_Tgt as o_Src_Tgt,
exp_check_flag.StartDate as StartDate,
exp_check_flag.EndDate as EndDate,
exp_check_flag.NewLookupRow as NewLookupRow,
exp_check_flag.lkp_QUOTN_STS_STRT_DTTM as lkp_QUOTN_STS_STRT_DTTM,
exp_check_flag.lkp_QUOTN_STS_TYPE_CD as lkp_QUOTN_STS_TYPE_CD,
exp_check_flag.Rank as Rank,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE 1 = 2 
-- exp_check_flag.o_Src_Tgt = ''U'' 
-- exp_check_flag.NewLookupRow = 2
);


-- Component quotn_sts_Upd_Ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_STS
(
QUOTN_ID,
QUOTN_STS_STRT_DTTM,
QUOTN_STS_TYPE_CD,
QUOTN_STS_END_DTTM,
NK_JOB_NBR,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
rtr_ins_upd_UPDATE.lkp_QUOTN_ID as QUOTN_ID,
rtr_ins_upd_UPDATE.in_QUOTN_STS_STRT_DTTM as QUOTN_STS_STRT_DTTM,
rtr_ins_upd_UPDATE.in_QUOTN_STS_TYPE_CD as QUOTN_STS_TYPE_CD,
rtr_ins_upd_UPDATE.in_QUOTN_STS_END_DTTM as QUOTN_STS_END_DTTM,
rtr_ins_upd_UPDATE.in_NK_JOB_NBR as NK_JOB_NBR,
rtr_ins_upd_UPDATE.in_PRCS_ID as PRCS_ID,
rtr_ins_upd_UPDATE.StartDate as EDW_STRT_DTTM,
rtr_ins_upd_UPDATE.EndDate as EDW_END_DTTM,
rtr_ins_upd_UPDATE.in_UpdateTime as TRANS_STRT_DTTM
FROM
rtr_ins_upd_UPDATE;


-- Component exp_quotn_sts_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_quotn_sts_insert AS
(
SELECT
rtr_ins_upd_INSERT.in_QUOTN_ID as in_QUOTN_ID1,
rtr_ins_upd_INSERT.in_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM1,
rtr_ins_upd_INSERT.in_QUOTN_STS_TYPE_CD as in_QUOTN_STS_TYPE_CD1,
rtr_ins_upd_INSERT.in_QUOTN_STS_END_DTTM as in_QUOTN_STS_END_DTTM1,
rtr_ins_upd_INSERT.in_UpdateTime as in_UpdateTime1,
rtr_ins_upd_INSERT.in_NK_JOB_NBR as in_NK_JOB_NBR1,
rtr_ins_upd_INSERT.in_PRCS_ID as in_PRCS_ID1,
DATEADD(
  SECOND,
  (2 * (rtr_ins_upd_INSERT.Rank - 1)),
  rtr_ins_upd_INSERT.StartDate
) as EDW_STRT_DTTM,
rtr_ins_upd_INSERT.EndDate as EndDate1,
rtr_ins_upd_INSERT.source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component upd_Update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_UPDATE.lkp_QUOTN_ID as lkp_QUOTN_ID3,
rtr_ins_upd_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_UPDATE.lkp_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM3,
rtr_ins_upd_UPDATE.lkp_QUOTN_STS_TYPE_CD as lkp_QUOTN_STS_TYPE_CD3,
rtr_ins_upd_UPDATE.in_QUOTN_STS_STRT_DTTM as in_QUOTN_STS_STRT_DTTM31,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_UPDATE.source_record_id
FROM
rtr_ins_upd_UPDATE
);


-- Component quotn_sts_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_STS
(
QUOTN_ID,
QUOTN_STS_STRT_DTTM,
QUOTN_STS_TYPE_CD,
QUOTN_STS_END_DTTM,
NK_JOB_NBR,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_quotn_sts_insert.in_QUOTN_ID1 as QUOTN_ID,
exp_quotn_sts_insert.in_QUOTN_STS_STRT_DTTM1 as QUOTN_STS_STRT_DTTM,
exp_quotn_sts_insert.in_QUOTN_STS_TYPE_CD1 as QUOTN_STS_TYPE_CD,
exp_quotn_sts_insert.in_QUOTN_STS_END_DTTM1 as QUOTN_STS_END_DTTM,
exp_quotn_sts_insert.in_NK_JOB_NBR1 as NK_JOB_NBR,
exp_quotn_sts_insert.in_PRCS_ID1 as PRCS_ID,
exp_quotn_sts_insert.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_quotn_sts_insert.EndDate1 as EDW_END_DTTM,
exp_quotn_sts_insert.in_UpdateTime1 as TRANS_STRT_DTTM
FROM
exp_quotn_sts_insert;


-- Component quotn_sts_NewInsert, Type Post SQL 
UPDATE DB_T_PROD_CORE.QUOTN_STS FROM  

(

SELECT	distinct QUOTN_ID, EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM DB_T_PROD_CORE.QUOTN_STS 

)  A

set TRANS_END_DTTM=  A.lead, 

EDW_END_DTTM=A.lead1

where  QUOTN_STS.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_STS.QUOTN_ID=A.QUOTN_ID

and QUOTN_STS.TRANS_STRT_DTTM <>QUOTN_STS.TRANS_END_DTTM

and lead is not null;


-- Component exp_Expire, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Expire AS
(
SELECT
upd_Update.lkp_QUOTN_ID3 as lkp_QUOTN_ID3,
upd_Update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
DATEADD (''second'', -1, CURRENT_TIMESTAMP()) as o_EndDate,
upd_Update.in_QUOTN_STS_STRT_DTTM3 as in_QUOTN_STS_STRT_DTTM3,
upd_Update.lkp_QUOTN_STS_TYPE_CD3 as lkp_QUOTN_STS_TYPE_CD3,
DATEADD (
  ''second'',
  -1,
  upd_Update.in_QUOTN_STS_STRT_DTTM31
) as o_trasaction_end_dt,
upd_Update.source_record_id
FROM
upd_Update
);


-- Component quotn_sts_Upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_STS
USING exp_Expire ON (QUOTN_STS.QUOTN_ID = exp_Expire.lkp_QUOTN_ID3 AND QUOTN_STS.QUOTN_STS_STRT_DTTM = exp_Expire.in_QUOTN_STS_STRT_DTTM3 AND QUOTN_STS.QUOTN_STS_TYPE_CD = exp_Expire.lkp_QUOTN_STS_TYPE_CD3 AND QUOTN_STS.EDW_STRT_DTTM = exp_Expire.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_Expire.lkp_QUOTN_ID3,
QUOTN_STS_STRT_DTTM = exp_Expire.in_QUOTN_STS_STRT_DTTM3,
QUOTN_STS_TYPE_CD = exp_Expire.lkp_QUOTN_STS_TYPE_CD3,
EDW_STRT_DTTM = exp_Expire.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_Expire.o_EndDate,
TRANS_END_DTTM = exp_Expire.o_trasaction_end_dt;


END; 
';