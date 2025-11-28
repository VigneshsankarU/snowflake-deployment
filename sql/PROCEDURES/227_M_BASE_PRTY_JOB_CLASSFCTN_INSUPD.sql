-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_JOB_CLASSFCTN_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;   

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''JOB_CLASFCN'' 

				AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_prty_job_classfctn_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_prty_job_classfctn_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as TypeCode,
$3 as UpdateTime,
$4 as CreateTime,
$5 as Retired,
$6 as Job_clasfcn_cd,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select pc_contact.PublicID_stg as Prty_ID_SK,

pctl_contact.NAME_stg as contacttype,

''9999-12-31 00:00:00.0000000'' as prty_job_clasfcn_end_dttm,

pc_role.CreateTime_stg as Prty_job_clasfcn_start_dttm,

case when pc_contact.Retired_stg=0 and pc_user.retired_stg=0  and pc_role.Retired_stg=0 

then 0 else 1 end,

pc_role.Name_stg as JOB_CLASFCN_CD

from DB_T_PROD_STAG.pc_user

join DB_T_PROD_STAG.pc_contact on pc_contact.ID_stg=pc_user.ContactID_stg

join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

join DB_T_PROD_STAG.pc_userrole on pc_user.id_stg=pc_userrole.UserID_stg

join DB_T_PROD_STAG.pc_role on pc_role.id_stg=pc_userrole.RoleID_stg

Where  ((pc_contact.updatetime_stg >( :START_DTTM)

AND pc_contact.updatetime_stg <= (:end_dttm ))

or (pc_role.updatetime_stg >( :START_DTTM)

AND pc_role.updatetime_stg <= (:end_dttm )))
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_prty_job_classfctn_x.PublicID as PublicID,
SQ_pc_prty_job_classfctn_x.UpdateTime as UpdateTime,
SQ_pc_prty_job_classfctn_x.CreateTime as CreateTime,
SQ_pc_prty_job_classfctn_x.Retired as Retired,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as o_JOB_CLASFCN_CD,
SQ_pc_prty_job_classfctn_x.source_record_id,
row_number() over (partition by SQ_pc_prty_job_classfctn_x.source_record_id order by SQ_pc_prty_job_classfctn_x.source_record_id) as RNK
FROM
SQ_pc_prty_job_classfctn_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_prty_job_classfctn_x.Job_clasfcn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_prty_job_classfctn_x.Job_clasfcn_cd
QUALIFY RNK = 1
);


-- Component LKP_INDIV_CLM_CTR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_PUBLC_ID desc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
) LKP ON LKP.NK_PUBLC_ID = exp_pass_from_src.PublicID
QUALIFY RNK = 1
);


-- Component LKP_PRTY_JOB_CLASSFCTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_JOB_CLASSFCTN AS
(
SELECT
LKP.JOB_CLASFCN_CD,
LKP.PRTY_JOB_CLASFCN_STRT_DT,
LKP.PRTY_ID,
LKP.PRTY_JOB_CLASFCN_END_DT,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.JOB_CLASFCN_CD asc,LKP.PRTY_JOB_CLASFCN_STRT_DT asc,LKP.PRTY_ID asc,LKP.PRTY_JOB_CLASFCN_END_DT asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK1
FROM
exp_pass_from_src
INNER JOIN LKP_INDIV_CLM_CTR ON exp_pass_from_src.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
LEFT JOIN (
SELECT PRTY_JOB_CLASFCN.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT, PRTY_JOB_CLASFCN.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT, PRTY_JOB_CLASFCN.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_JOB_CLASFCN.EDW_END_DTTM as EDW_END_DTTM, PRTY_JOB_CLASFCN.JOB_CLASFCN_CD as JOB_CLASFCN_CD, PRTY_JOB_CLASFCN.PRTY_ID as PRTY_ID FROM DB_T_PROD_CORE.PRTY_JOB_CLASFCN
 QUALIFY ROW_NUMBER() OVER(PARTITION BY JOB_CLASFCN_CD,PRTY_ID   ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.JOB_CLASFCN_CD = exp_pass_from_src.o_JOB_CLASFCN_CD AND LKP.PRTY_ID = LKP_INDIV_CLM_CTR.INDIV_PRTY_ID
QUALIFY RNK1 = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE
OR REPLACE TEMPORARY TABLE exp_data_transformation AS (
  SELECT
    LKP_PRTY_JOB_CLASSFCTN.PRTY_ID AS lkp_PRTY_ID,
    LKP_PRTY_JOB_CLASSFCTN.EDW_STRT_DTTM AS lkp_EDW_STRT_DTTM,
    LKP_PRTY_JOB_CLASSFCTN.JOB_CLASFCN_CD AS lkp_JOB_CLASFCN_CD,
MD5(
  CONCAT(
    DATE_TRUNC(''DAY'', TO_TIMESTAMP(LKP_PRTY_JOB_CLASSFCTN.PRTY_JOB_CLASFCN_STRT_DT)),
    DATE_TRUNC(''DAY'', TO_TIMESTAMP(LKP_PRTY_JOB_CLASSFCTN.PRTY_JOB_CLASFCN_END_DT))
  )
) AS lkp_checksum,

    exp_pass_from_src.o_JOB_CLASFCN_CD AS JOB_CLASFCN_CD,
    exp_pass_from_src.CreateTime AS PRTY_JOB_CLASFCN_STRT_DT,
    LKP_INDIV_CLM_CTR.INDIV_PRTY_ID AS PRTY_ID,
    exp_pass_from_src.UpdateTime AS PRTY_JOB_CLASFCN_END_DT,
    exp_pass_from_src.Retired AS Retired,
    MD5(
  CONCAT(
    DATE_TRUNC(''DAY'', TO_TIMESTAMP(exp_pass_from_src.CreateTime)),
    DATE_TRUNC(''DAY'', TO_TIMESTAMP(exp_pass_from_src.UpdateTime))
  )
) AS in_checksum,

    CURRENT_TIMESTAMP() AS EDW_STRT_DTTM,
    CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP) AS EDW_END_DTTM,
    :PRCS_ID AS PRCS_ID,
    CASE
      WHEN LKP_PRTY_JOB_CLASSFCTN.PRTY_ID IS NULL THEN ''I''
      WHEN lkp_checksum <> in_checksum THEN ''U''
      ELSE ''R''
    END AS Flag_insupd,
    LKP_PRTY_JOB_CLASSFCTN.EDW_END_DTTM AS lkp_EDW_END_DTTM,
    exp_pass_from_src.source_record_id
  FROM
    exp_pass_from_src
    INNER JOIN LKP_INDIV_CLM_CTR ON exp_pass_from_src.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
    INNER JOIN LKP_PRTY_JOB_CLASSFCTN ON LKP_INDIV_CLM_CTR.source_record_id = LKP_PRTY_JOB_CLASSFCTN.source_record_id
);

-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
(SELECT
exp_data_transformation.lkp_PRTY_ID as lkp_PRTY_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.JOB_CLASFCN_CD as JOB_CLASFCN_CD,
exp_data_transformation.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT,
exp_data_transformation.PRTY_ID as PRTY_ID,
exp_data_transformation.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.Flag_insupd as Flag_insupd,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_JOB_CLASFCN_CD as lkp_JOB_CLASFCN_CD,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Flag_insupd = ''I'' OR ( exp_data_transformation.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_data_transformation.Retired = 0 ));


-- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_RETIRE AS
(SELECT
exp_data_transformation.lkp_PRTY_ID as lkp_PRTY_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.JOB_CLASFCN_CD as JOB_CLASFCN_CD,
exp_data_transformation.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT,
exp_data_transformation.PRTY_ID as PRTY_ID,
exp_data_transformation.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.Flag_insupd as Flag_insupd,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_JOB_CLASFCN_CD as lkp_JOB_CLASFCN_CD,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Flag_insupd = ''R'' and exp_data_transformation.Retired != 0 and exp_data_transformation.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_UPDATE AS
(SELECT
exp_data_transformation.lkp_PRTY_ID as lkp_PRTY_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.JOB_CLASFCN_CD as JOB_CLASFCN_CD,
exp_data_transformation.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT,
exp_data_transformation.PRTY_ID as PRTY_ID,
exp_data_transformation.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.Flag_insupd as Flag_insupd,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_JOB_CLASFCN_CD as lkp_JOB_CLASFCN_CD,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Flag_insupd = ''U'' AND exp_data_transformation.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
rtr_ins_upd_INSERT.JOB_CLASFCN_CD as JOB_CLASFCN_CD1,
rtr_ins_upd_INSERT.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT1,
rtr_ins_upd_INSERT.PRTY_ID as PRTY_ID1,
rtr_ins_upd_INSERT.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT1,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID1,
rtr_ins_upd_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_ins_upd_INSERT.source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component UPDTRANS, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPDTRANS AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_pass_to_tgt_ins.JOB_CLASFCN_CD1 as JOB_CLASFCN_CD1,
exp_pass_to_tgt_ins.PRTY_JOB_CLASFCN_STRT_DT1 as PRTY_JOB_CLASFCN_STRT_DT1,
exp_pass_to_tgt_ins.PRTY_ID1 as PRTY_ID1,
exp_pass_to_tgt_ins.PRTY_JOB_CLASFCN_END_DT1 as PRTY_JOB_CLASFCN_END_DT1,
exp_pass_to_tgt_ins.PRCS_ID1 as PRCS_ID1,
exp_pass_to_tgt_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
exp_pass_to_tgt_ins.EDW_END_DTTM1 as EDW_END_DTTM1
FROM
exp_pass_to_tgt_ins
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
rtr_ins_upd_UPDATE.JOB_CLASFCN_CD as JOB_CLASFCN_CD3,
rtr_ins_upd_UPDATE.PRTY_JOB_CLASFCN_STRT_DT as PRTY_JOB_CLASFCN_STRT_DT3,
rtr_ins_upd_UPDATE.PRTY_ID as PRTY_ID3,
rtr_ins_upd_UPDATE.PRTY_JOB_CLASFCN_END_DT as PRTY_JOB_CLASFCN_END_DT3,
rtr_ins_upd_UPDATE.PRCS_ID as PRCS_ID3,
rtr_ins_upd_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_UPDATE.EDW_END_DTTM as EDW_END_DTTM3,
rtr_ins_upd_UPDATE.Retired as Retired3,
rtr_ins_upd_UPDATE.source_record_id
FROM
rtr_ins_upd_UPDATE
WHERE rtr_ins_upd_UPDATE.Retired = 0
);


-- Component PRTY_JOB_CLASFCN_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_JOB_CLASFCN
(
JOB_CLASFCN_CD,
PRTY_JOB_CLASFCN_STRT_DT,
PRTY_ID,
PRTY_JOB_CLASFCN_END_DT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
UPDTRANS.JOB_CLASFCN_CD1 as JOB_CLASFCN_CD,
UPDTRANS.PRTY_JOB_CLASFCN_STRT_DT1 as PRTY_JOB_CLASFCN_STRT_DT,
UPDTRANS.PRTY_ID1 as PRTY_ID,
UPDTRANS.PRTY_JOB_CLASFCN_END_DT1 as PRTY_JOB_CLASFCN_END_DT,
UPDTRANS.PRCS_ID1 as PRCS_ID,
UPDTRANS.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
UPDTRANS.EDW_END_DTTM1 as EDW_END_DTTM
FROM
UPDTRANS;


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
rtr_ins_upd_UPDATE.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_ins_upd_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_UPDATE.lkp_JOB_CLASFCN_CD as lkp_JOB_CLASFCN_CD4,
DATEADD (SECOND, -1, rtr_ins_upd_UPDATE.EDW_STRT_DTTM) as EDW_END_DTTM1,
rtr_ins_upd_UPDATE.source_record_id
FROM
rtr_ins_upd_UPDATE
);


-- Component exp_pass_to_tgt_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_retired AS
(
SELECT
rtr_ins_upd_RETIRE.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_ins_upd_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_RETIRE.lkp_JOB_CLASFCN_CD as lkp_JOB_CLASFCN_CD4,
DATEADD(SECOND, -1, rtr_ins_upd_RETIRE.EDW_STRT_DTTM) as EDW_END_DTTM1,
rtr_ins_upd_RETIRE.source_record_id
FROM
rtr_ins_upd_RETIRE
);


-- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
(
SELECT
FILTRANS.JOB_CLASFCN_CD3 as JOB_CLASFCN_CD1,
FILTRANS.PRTY_JOB_CLASFCN_STRT_DT3 as PRTY_JOB_CLASFCN_STRT_DT1,
FILTRANS.PRTY_ID3 as PRTY_ID1,
FILTRANS.PRTY_JOB_CLASFCN_END_DT3 as PRTY_JOB_CLASFCN_END_DT1,
FILTRANS.PRCS_ID3 as PRCS_ID1,
FILTRANS.EDW_STRT_DTTM3 as EDW_STRT_DTTM1,
FILTRANS.EDW_END_DTTM3 as EDW_END_DTTM1,
FILTRANS.source_record_id
FROM
FILTRANS
);


-- Component upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_pass_to_tgt_retired.EDW_END_DTTM1 as EDW_END_DTTM1,
exp_pass_to_tgt_retired.lkp_PRTY_ID3 as lkp_PRTY_ID3,
exp_pass_to_tgt_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
exp_pass_to_tgt_retired.lkp_JOB_CLASFCN_CD4 as lkp_JOB_CLASFCN_CD4,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_pass_to_tgt_retired
);


-- Component upd_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_pass_to_tgt_upd.EDW_END_DTTM1 as EDW_END_DTTM1,
exp_pass_to_tgt_upd.lkp_PRTY_ID3 as lkp_PRTY_ID3,
exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
exp_pass_to_tgt_upd.lkp_JOB_CLASFCN_CD4 as lkp_JOB_CLASFCN_CD4,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_pass_to_tgt_upd
);


-- Component PRTY_JOB_CLASFCN_retired, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.PRTY_JOB_CLASFCN
USING upd_retired ON (UPDATE_STRATEGY_ACTION = 1 AND PRTY_JOB_CLASFCN.JOB_CLASFCN_CD = upd_retired.lkp_JOB_CLASFCN_CD4 AND PRTY_JOB_CLASFCN.PRTY_ID = upd_retired.lkp_PRTY_ID3 AND PRTY_JOB_CLASFCN.EDW_STRT_DTTM = upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_retired.EDW_END_DTTM1
;


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_pass_to_tgt_upd_ins.JOB_CLASFCN_CD1 as JOB_CLASFCN_CD1,
exp_pass_to_tgt_upd_ins.PRTY_JOB_CLASFCN_STRT_DT1 as PRTY_JOB_CLASFCN_STRT_DT1,
exp_pass_to_tgt_upd_ins.PRTY_ID1 as PRTY_ID1,
exp_pass_to_tgt_upd_ins.PRTY_JOB_CLASFCN_END_DT1 as PRTY_JOB_CLASFCN_END_DT1,
exp_pass_to_tgt_upd_ins.PRCS_ID1 as PRCS_ID1,
exp_pass_to_tgt_upd_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
exp_pass_to_tgt_upd_ins.EDW_END_DTTM1 as EDW_END_DTTM1
FROM
exp_pass_to_tgt_upd_ins
);


-- Component PRTY_JOB_CLASFCN_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_JOB_CLASFCN
(
JOB_CLASFCN_CD,
PRTY_JOB_CLASFCN_STRT_DT,
PRTY_ID,
PRTY_JOB_CLASFCN_END_DT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
upd_ins.JOB_CLASFCN_CD1 as JOB_CLASFCN_CD,
upd_ins.PRTY_JOB_CLASFCN_STRT_DT1 as PRTY_JOB_CLASFCN_STRT_DT,
upd_ins.PRTY_ID1 as PRTY_ID,
upd_ins.PRTY_JOB_CLASFCN_END_DT1 as PRTY_JOB_CLASFCN_END_DT,
upd_ins.PRCS_ID1 as PRCS_ID,
upd_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
upd_ins.EDW_END_DTTM1 as EDW_END_DTTM
FROM
upd_ins;


-- Component PRTY_JOB_CLASFCN_upd_upd, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.PRTY_JOB_CLASFCN
USING upd_upd ON (UPDATE_STRATEGY_ACTION = 1 AND PRTY_JOB_CLASFCN.JOB_CLASFCN_CD = upd_upd.lkp_JOB_CLASFCN_CD4 AND PRTY_JOB_CLASFCN.PRTY_ID = upd_upd.lkp_PRTY_ID3 AND PRTY_JOB_CLASFCN.EDW_STRT_DTTM = upd_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_upd.EDW_END_DTTM1
;


END; ';