-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_SCR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_CREDIT_SCR_LKUP, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CREDIT_SCR_LKUP AS
(
SELECT 
	CREDIT_SCR_LKUP.LEVEL_CD as LEVEL_CD, 
	CREDIT_SCR_LKUP.ST_CD as ST_CD, 
	CREDIT_SCR_LKUP.CREDIT_SCR_LIM1 as CREDIT_SCR_LIM1, 
	CREDIT_SCR_LKUP.CREDIT_SCR_LIM2 as CREDIT_SCR_LIM2 
FROM 
	DB_T_SHRD_PROD.CREDIT_SCR_LKUP
WHERE
	CREDIT_SCR_LKUP.EXP_DT =''9999-12-31''
);


-- Component SQ_pc_modl_run_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_modl_run_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ModelName,
$2 as ModelRunDttm,
$3 as Score,
$4 as PartyAgreement,
$5 as State_Cd,
$6 as UpdateTime,
$7 as Rank,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT 

pc_modl_run_x.MODEL_NAME_stg as ModelName, 

pc_modl_run_x.MODEL_RUN_DTTM_stg as ModelRunDttm, 

pc_modl_run_x.Score_stg as Score, 

pc_modl_run_x.PARTY_AGREEMENT_stg as PartyAgreement,

pc_modl_run_x.State_stg as  State_Cd, 

pc_modl_run_x.UpdateTime_stg as UpdateTime, 

Rank() Over (PARTITION BY MODEL_NAME_stg, MODEL_RUN_DTTM_stg, PARTY_AGREEMENT_stg ORDER BY pc_modl_run_x.UpdateTime_stg  ,pc_modl_run_x.Score_stg ) rk

FROM (

SELECT  DISTINCT 

	''LEXIS NEXIS'' AS MODEL_NAME_stg,

	InsuranceScoreDate_stg AS MODEL_RUN_DTTM_stg,

	Coalesce(Cast(insurancescore_stg AS DECIMAL(19,2)),0.00) AS SCORE_stg

	,CAST(AddressBookUID_stg AS VARCHAR(100))PARTY_AGREEMENT_stg

	,pctl_jurisdiction.TYPECODE_stg AS state_stg

	,pctl_policycontactrole.TYPECODE_stg AS PRTY_ROLE_CD

	,pcx_InsuranceReport_alfa.UpdateTime_stg

FROM 

	DB_T_PROD_STAG.pc_job 

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

        JOIN DB_T_PROD_STAG.pcx_InsuranceReport_alfa ON pc_policyperiod.id_stg=pcx_InsuranceReport_alfa.BranchID_stg

         JOIN DB_T_PROD_STAG.pctl_jurisdiction ON pc_policyperiod.BaseState_stg=pctl_jurisdiction.id_stg

         JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policycontactrole.id_stg=pcx_InsuranceReport_alfa.PolicyContactRoleID_stg

        JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.id_stg=pc_policycontactrole.ContactDenorm_stg

        JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pctl_policycontactrole.id_stg=pc_policycontactrole.Subtype_stg

        JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg=pc_contact.Subtype_stg

	JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg

WHERE  

	pc_contact.AddressBookUID_stg IS NOT NULL 

	AND AddressBookUID_stg IS NOT NULL 

	AND InsuranceScoreDate_stg IS NOT NULL

	AND insurancescore_stg IS NOT NULL 

	AND pctl_policyperiodstatus.typecode_stg<>''Temporary''

	AND pctl_job.typecode_stg IN (''Submission'',''Renewal'',''PolicyChange'')    AND pcx_InsuranceReport_alfa.UpdateTime_stg> CAST(:start_dttm as timestamp)

	AND pcx_InsuranceReport_alfa.UpdateTime_stg <= CAST(:end_dttm as timestamp)) AS pc_modl_run_x

    

WHERE

 	pc_modl_run_x.MODEL_NAME_stg=''LEXIS NEXIS''



 QUALIFY Row_Number() Over(PARTITION BY

pc_modl_run_x.MODEL_NAME_stg, 

pc_modl_run_x.MODEL_RUN_DTTM_stg, pc_modl_run_x.PARTY_AGREEMENT_stg, 

pc_modl_run_x.State_stg

ORDER BY pc_modl_run_x.UpdateTime_stg DESC,pc_modl_run_x.Score_stg DESC)=1
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_modl_run_x.ModelName as ModelName,
SQ_pc_modl_run_x.ModelRunDttm as ModelRunDttm,
TO_NUMBER(SQ_pc_modl_run_x.Score) as var_Score,
SQ_pc_modl_run_x.PartyAgreement as PartyAgreement,
CASE WHEN SQ_pc_modl_run_x.Score IS NULL THEN ''0.00'' ELSE SQ_pc_modl_run_x.Score END as out_Score,
LKP_1.LEVEL_CD /* replaced lookup LKP_CREDIT_SCR_LKUP */ as LVL_Cd,
SQ_pc_modl_run_x.Rank as Rank,
SQ_pc_modl_run_x.UpdateTime as UpdateTime,
SQ_pc_modl_run_x.source_record_id,
row_number() over (partition by SQ_pc_modl_run_x.source_record_id order by SQ_pc_modl_run_x.source_record_id) as RNK
FROM
SQ_pc_modl_run_x
LEFT JOIN LKP_CREDIT_SCR_LKUP LKP_1 ON LKP_1.ST_CD = SQ_pc_modl_run_x.State_Cd AND LKP_1.CREDIT_SCR_LIM1 <= var_Score AND LKP_1.CREDIT_SCR_LIM2 >= var_Score
QUALIFY RNK = 1
);


-- Component LKP_INDIV_CNT_MGR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CNT_MGR AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_LINK_ID desc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_LINK_ID as NK_LINK_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NULL
) LKP ON LKP.NK_LINK_ID = exp_pass_from_src.PartyAgreement
QUALIFY RNK = 1
);


-- Component LKP_ANLTCL_MODL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ANLTCL_MODL AS
(
SELECT
LKP.MODL_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_NAME asc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT ANLTCL_MODL.MODL_ID as MODL_ID, ANLTCL_MODL.MODL_NAME as MODL_NAME FROM DB_T_PROD_CORE.ANLTCL_MODL ORDER BY MODL_FROM_DTTM desc
) LKP ON LKP.MODL_NAME = exp_pass_from_src.ModelName
QUALIFY RNK = 1
);


-- Component LKP_MODL_RUN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MODL_RUN AS
(
SELECT
LKP.MODL_ID,
LKP.MODL_RUN_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_RUN_ID asc,LKP.MODL_RUN_DTTM asc) RNK1
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
LEFT JOIN (
SELECT
MODL_ID,
MODL_RUN_ID,
MODL_RUN_DTTM
FROM DB_T_PROD_CORE.MODL_RUN
) LKP ON LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_DTTM = exp_pass_from_src.ModelRunDttm
QUALIFY RNK1 = 1
);


-- Component LKP_PRTY_SCR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_SCR AS
(
SELECT
LKP.MODL_ID,
LKP.MODL_RUN_ID,
LKP.PRTY_ID,
LKP.PRTY_SCR_VAL,
LKP.LVL_NUM,
LKP.EDW_STRT_DTTM,
LKP_INDIV_CNT_MGR.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_INDIV_CNT_MGR.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_RUN_ID asc,LKP.PRTY_ID asc,LKP.PRTY_SCR_VAL asc,LKP.LVL_NUM asc,LKP.EDW_STRT_DTTM asc) RNK1
FROM
LKP_INDIV_CNT_MGR
INNER JOIN LKP_ANLTCL_MODL ON LKP_INDIV_CNT_MGR.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_MODL_RUN ON LKP_ANLTCL_MODL.source_record_id = LKP_MODL_RUN.source_record_id
LEFT JOIN (
SELECT PRTY_SCR.PRTY_SCR_VAL as PRTY_SCR_VAL, 
PRTY_SCR.LVL_NUM as LVL_NUM, 
PRTY_SCR.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_SCR.MODL_ID as MODL_ID, PRTY_SCR.MODL_RUN_ID as MODL_RUN_ID, PRTY_SCR.PRTY_ID as PRTY_ID 
FROM DB_T_PROD_CORE.PRTY_SCR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ID,MODL_RUN_ID, MODL_ID 
ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_ID = LKP_MODL_RUN.MODL_RUN_ID AND LKP.PRTY_ID = LKP_INDIV_CNT_MGR.INDIV_PRTY_ID
QUALIFY RNK1 = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_PRTY_SCR.MODL_ID as lkp_MODL_ID,
LKP_PRTY_SCR.MODL_RUN_ID as lkp_MODL_RUN_ID,
LKP_PRTY_SCR.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_SCR.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_ANLTCL_MODL.MODL_ID as in_MODL_ID,
LKP_MODL_RUN.MODL_RUN_ID as in_MODL_RUN_ID,
exp_pass_from_src.out_Score as in_PRTY_SCR_VAL,
LKP_INDIV_CNT_MGR.INDIV_PRTY_ID as in_PRTY_ID,
exp_pass_from_src.LVL_Cd as in_LVL_num,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
MD5 ( ltrim ( rtrim ( LKP_PRTY_SCR.PRTY_SCR_VAL ) ) || ltrim ( rtrim ( LKP_PRTY_SCR.LVL_NUM ) ) ) as var_orig_chksm,
MD5 ( ltrim ( rtrim ( exp_pass_from_src.out_Score ) ) || ltrim ( rtrim ( exp_pass_from_src.LVL_Cd ) ) ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
CASE WHEN LKP_PRTY_SCR.MODL_ID IS NULL THEN 1 ELSE 0 END as out_InsertFlag,
CASE WHEN LKP_PRTY_SCR.MODL_ID IS NOT NULL AND LKP_PRTY_SCR.PRTY_SCR_VAL <> exp_pass_from_src.out_Score THEN 1 ELSE 0 END as out_UpdateFlag,
:PRCS_ID as out_PRCS_ID,
CASE WHEN LKP_PRTY_SCR.PRTY_ID IS NULL THEN ''I'' ELSE ''R'' END as cdc_flag,
exp_pass_from_src.Rank as Rank,
exp_pass_from_src.UpdateTime as UpdateTime,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_INDIV_CNT_MGR ON exp_pass_from_src.source_record_id = LKP_INDIV_CNT_MGR.source_record_id
INNER JOIN LKP_ANLTCL_MODL ON LKP_INDIV_CNT_MGR.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_MODL_RUN ON LKP_ANLTCL_MODL.source_record_id = LKP_MODL_RUN.source_record_id
INNER JOIN LKP_PRTY_SCR ON LKP_MODL_RUN.source_record_id = LKP_PRTY_SCR.source_record_id
);


-- Component srt_lvl_num, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE srt_lvl_num AS
(
SELECT
exp_data_transformation.lkp_MODL_ID as lkp_MODL_ID,
exp_data_transformation.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
exp_data_transformation.lkp_PRTY_ID as lkp_PRTY_ID,
NULL as lkp_PRTY_SCR_VAL,
NULL as lkp_LVL_NUM,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.in_MODL_ID as in_MODL_ID,
exp_data_transformation.in_MODL_RUN_ID as in_MODL_RUN_ID,
exp_data_transformation.in_PRTY_SCR_VAL as in_PRTY_SCR_VAL,
exp_data_transformation.in_PRTY_ID as in_PRTY_ID,
exp_data_transformation.in_LVL_num as in_LVL_num,
exp_data_transformation.out_ins_upd as out_ins_upd,
exp_data_transformation.out_InsertFlag as out_InsertFlag,
exp_data_transformation.out_UpdateFlag as out_UpdateFlag,
exp_data_transformation.out_PRCS_ID as out_PRCS_ID,
exp_data_transformation.cdc_flag as cdc_flag,
exp_data_transformation.Rank as Rank,
exp_data_transformation.UpdateTime as UpdateTime,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
ORDER BY in_MODL_ID , in_MODL_RUN_ID , in_PRTY_ID , in_LVL_num 
);


-- Component rtr_pty_scr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_pty_scr_INSERT AS
(SELECT
srt_lvl_num.lkp_MODL_ID as lkp_MODL_ID,
srt_lvl_num.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
srt_lvl_num.lkp_PRTY_ID as lkp_PRTY_ID,
srt_lvl_num.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
srt_lvl_num.in_MODL_ID as in_MODL_ID,
srt_lvl_num.in_MODL_RUN_ID as in_MODL_RUN_ID,
srt_lvl_num.in_PRTY_SCR_VAL as in_PRTY_SCR_VAL,
srt_lvl_num.in_PRTY_ID as in_PRTY_ID,
srt_lvl_num.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
srt_lvl_num.in_LVL_num as in_lvl_num,
srt_lvl_num.out_ins_upd as out_ins_upd,
srt_lvl_num.out_InsertFlag as InsertFlag,
srt_lvl_num.out_UpdateFlag as UpdateFlag,
srt_lvl_num.out_PRCS_ID as in_PRCS_ID,
srt_lvl_num.cdc_flag as in_cdc_flag,
srt_lvl_num.Rank as Rank,
srt_lvl_num.UpdateTime as UpdateTime,
srt_lvl_num.source_record_id
FROM
srt_lvl_num
WHERE 
-- srt_lvl_num.cdc_flag = ''I'' AND 
srt_lvl_num.in_MODL_RUN_ID IS NOT NULL AND srt_lvl_num.in_PRTY_ID IS NOT NULL and srt_lvl_num.in_MODL_ID IS NOT NULL and
srt_lvl_num.out_ins_upd = ''I'' and srt_lvl_num.in_MODL_ID <> 9999 and srt_lvl_num.in_MODL_RUN_ID <> 9999 and srt_lvl_num.in_PRTY_ID <> 9999);


-- Component rtr_pty_scr_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_pty_scr_UPDATE AS
(SELECT
srt_lvl_num.lkp_MODL_ID as lkp_MODL_ID,
srt_lvl_num.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
srt_lvl_num.lkp_PRTY_ID as lkp_PRTY_ID,
srt_lvl_num.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
srt_lvl_num.in_MODL_ID as in_MODL_ID,
srt_lvl_num.in_MODL_RUN_ID as in_MODL_RUN_ID,
srt_lvl_num.in_PRTY_SCR_VAL as in_PRTY_SCR_VAL,
srt_lvl_num.in_PRTY_ID as in_PRTY_ID,
srt_lvl_num.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
srt_lvl_num.in_LVL_num as in_lvl_num,
srt_lvl_num.out_ins_upd as out_ins_upd,
srt_lvl_num.out_InsertFlag as InsertFlag,
srt_lvl_num.out_UpdateFlag as UpdateFlag,
srt_lvl_num.out_PRCS_ID as in_PRCS_ID,
srt_lvl_num.cdc_flag as in_cdc_flag,
srt_lvl_num.Rank as Rank,
srt_lvl_num.UpdateTime as UpdateTime,
srt_lvl_num.source_record_id
FROM
srt_lvl_num
WHERE srt_lvl_num.out_ins_upd = ''U'');


-- Component upd_prty_scr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_scr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_INSERT.in_MODL_ID as MODL_ID,
rtr_pty_scr_INSERT.in_MODL_RUN_ID as MODL_RUN_ID,
rtr_pty_scr_INSERT.in_PRTY_ID as PRTY_ID,
rtr_pty_scr_INSERT.in_PRTY_SCR_VAL as PRTY_SCR_VAL,
rtr_pty_scr_INSERT.in_PRCS_ID as PRCS_ID,
rtr_pty_scr_INSERT.in_lvl_num as in_lvl_num1,
rtr_pty_scr_INSERT.Rank as Rank1,
rtr_pty_scr_INSERT.UpdateTime as UpdateTime1,
0 as UPDATE_STRATEGY_ACTION,
rtr_pty_scr_INSERT.source_record_id
FROM
rtr_pty_scr_INSERT
);


-- Component upd_prty_scr_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_scr_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_UPDATE.in_MODL_ID as in_MODL_ID3,
rtr_pty_scr_UPDATE.in_MODL_RUN_ID as in_MODL_RUN_ID3,
rtr_pty_scr_UPDATE.in_PRTY_SCR_VAL as in_PRTY_SCR_VAL3,
rtr_pty_scr_UPDATE.in_PRTY_ID as in_PRTY_ID3,
rtr_pty_scr_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_pty_scr_UPDATE.in_lvl_num as in_lvl_num3,
rtr_pty_scr_UPDATE.UpdateTime as UpdateTime3,
rtr_pty_scr_UPDATE.in_PRCS_ID as in_PRCS_ID3,
0 as UPDATE_STRATEGY_ACTION,
rtr_pty_scr_UPDATE.source_record_id
FROM
rtr_pty_scr_UPDATE
);


-- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
(
SELECT
upd_prty_scr_upd_ins.in_MODL_ID3 as MODL_ID,
upd_prty_scr_upd_ins.in_MODL_RUN_ID3 as MODL_RUN_ID,
upd_prty_scr_upd_ins.in_PRTY_ID3 as PRTY_ID,
upd_prty_scr_upd_ins.in_PRTY_SCR_VAL3 as PRTY_SCR_VAL,
upd_prty_scr_upd_ins.in_lvl_num3 as LVL_NUM,
upd_prty_scr_upd_ins.in_PRCS_ID3 as in_PRCS_ID3,
upd_prty_scr_upd_ins.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as o_EDW_END_DTTM,
upd_prty_scr_upd_ins.UpdateTime3 as UpdateTime3,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_TRANS_END_DTTM,
upd_prty_scr_upd_ins.source_record_id
FROM
upd_prty_scr_upd_ins
);


-- Component upd_prty_scr_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_scr_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_UPDATE.in_MODL_ID as in_MODL_ID3,
rtr_pty_scr_UPDATE.in_MODL_RUN_ID as in_MODL_RUN_ID3,
rtr_pty_scr_UPDATE.in_PRTY_ID as in_PRTY_ID3,
rtr_pty_scr_UPDATE.in_PRCS_ID as in_PRCS_ID3,
rtr_pty_scr_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_pty_scr_UPDATE.UpdateTime as UpdateTime3,
rtr_pty_scr_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
1 as UPDATE_STRATEGY_ACTION,
rtr_pty_scr_UPDATE.SOURCE_RECORD_ID
FROM
rtr_pty_scr_UPDATE
);


-- Component tgt_prty_scr_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_SCR
(
MODL_ID,
MODL_RUN_ID,
PRTY_ID,
PRTY_SCR_VAL,
LVL_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_upd_ins.MODL_ID as MODL_ID,
exp_pass_to_tgt_upd_ins.MODL_RUN_ID as MODL_RUN_ID,
exp_pass_to_tgt_upd_ins.PRTY_ID as PRTY_ID,
exp_pass_to_tgt_upd_ins.PRTY_SCR_VAL as PRTY_SCR_VAL,
exp_pass_to_tgt_upd_ins.LVL_NUM as LVL_NUM,
exp_pass_to_tgt_upd_ins.in_PRCS_ID3 as PRCS_ID,
exp_pass_to_tgt_upd_ins.in_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_pass_to_tgt_upd_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_upd_ins.UpdateTime3 as TRANS_STRT_DTTM,
exp_pass_to_tgt_upd_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_upd_ins;


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_prty_scr_ins.MODL_ID as MODL_ID,
upd_prty_scr_ins.MODL_RUN_ID as MODL_RUN_ID,
upd_prty_scr_ins.PRTY_ID as PRTY_ID,
upd_prty_scr_ins.PRTY_SCR_VAL as PRTY_SCR_VAL,
upd_prty_scr_ins.PRCS_ID as PRCS_ID,
upd_prty_scr_ins.in_lvl_num1 as in_lvl_num1,
DATEADD (
  SECOND,
  (2 * (upd_prty_scr_ins.Rank1 - 1)),
  CURRENT_TIMESTAMP()
) AS Out_EDW_STRT_DATE,
to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as o_EDW_END_DTTM,
upd_prty_scr_ins.UpdateTime1 as UpdateTime1,
to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_TRANS_END_DTTM,
upd_prty_scr_ins.source_record_id
FROM
upd_prty_scr_ins
);


-- Component tgt_PRTY_SCR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_SCR
(
MODL_ID,
MODL_RUN_ID,
PRTY_ID,
PRTY_SCR_VAL,
LVL_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_ins.MODL_ID as MODL_ID,
exp_pass_to_tgt_ins.MODL_RUN_ID as MODL_RUN_ID,
exp_pass_to_tgt_ins.PRTY_ID as PRTY_ID,
exp_pass_to_tgt_ins.PRTY_SCR_VAL as PRTY_SCR_VAL,
exp_pass_to_tgt_ins.in_lvl_num1 as LVL_NUM,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.Out_EDW_STRT_DATE as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_ins.UpdateTime1 as TRANS_STRT_DTTM,
exp_pass_to_tgt_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
upd_prty_scr_upd.in_MODL_ID3 as MODL_ID,
upd_prty_scr_upd.in_MODL_RUN_ID3 as MODL_RUN_ID,
upd_prty_scr_upd.in_PRTY_ID3 as PRTY_ID,
upd_prty_scr_upd.lkp_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
DATEADD (SECOND, -1, upd_prty_scr_upd.in_EDW_STRT_DTTM1) as EDW_END_DTTM,
DATEADD (SECOND, -1, upd_prty_scr_upd.UpdateTime3) as o_TRANS_END_DTTM,
upd_prty_scr_upd.source_record_id
FROM
upd_prty_scr_upd
);


-- Component tgt_prty_scr_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_SCR
USING exp_pass_to_tgt_upd ON (PRTY_SCR.MODL_ID = exp_pass_to_tgt_upd.MODL_ID AND PRTY_SCR.MODL_RUN_ID = exp_pass_to_tgt_upd.MODL_RUN_ID AND PRTY_SCR.PRTY_ID = exp_pass_to_tgt_upd.PRTY_ID AND PRTY_SCR.EDW_STRT_DTTM = exp_pass_to_tgt_upd.EDW_STRT_DTTM)
WHEN MATCHED THEN UPDATE
SET
MODL_ID = exp_pass_to_tgt_upd.MODL_ID,
MODL_RUN_ID = exp_pass_to_tgt_upd.MODL_RUN_ID,
PRTY_ID = exp_pass_to_tgt_upd.PRTY_ID,
EDW_STRT_DTTM = exp_pass_to_tgt_upd.EDW_STRT_DTTM,
EDW_END_DTTM = exp_pass_to_tgt_upd.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_tgt_upd.o_TRANS_END_DTTM;


END; ';