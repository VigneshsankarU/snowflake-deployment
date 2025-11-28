-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_APLCTN_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL , row_number() over (order by 1) AS source_record_id 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''APLCTN_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_job.Typecode''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL , row_number() over (order by 1) AS source_record_id 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_uwissuehistory, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_uwissuehistory AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UpdateTime,
$2 as JobNumber,
$3 as TYPECODE_job,
$4 as PublicID_contact,
$5 as SYS_SRC_CD,
$6 as busn_start_dt,
$7 as busn_end_dt,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

distinct updatetime,

jobnumber, 

typecode,

publicid, 

sys_src_cd, 

busn_start_dt, 

busn_end_dt 

from

(

SELECT 

uwiss.UpdateTime_stg as  updatetime,

pj.JobNumber_stg as  jobnumber,

pjl.TYPECODE_stg as TYPECODE ,

pc.PublicID_stg as publicid,

''SRC_SYS4'' as sys_src_cd,

case when uwiss.retired_stg=0 then uwiss.updatetime_stg end as busn_start_dt,

case when uwiss.retired_stg<>0 then uwiss.updatetime_stg end as busn_end_dt

FROM  DB_T_PROD_STAG.pc_uwissuehistory uwiss

    LEFT JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = uwiss.PolicyPeriodID_stg

    LEFT JOIN DB_T_PROD_STAG.pc_job pj ON pj.id_stg= pp.JobID_stg

    LEFT JOIN DB_T_PROD_STAG.pctl_job pjl ON pj.Subtype_stg = pjl.id_stg

    LEFT JOIN DB_T_PROD_STAG.pc_user pu ON pu.id_stg = uwiss.ResponsibleUser_stg

    LEFT JOIN DB_T_PROD_STAG.pc_contact pc ON pc.id_stg = pu.ContactID_stg

    LEFT JOIN DB_T_PROD_STAG.pctl_uwissuehistorystatus puwiss ON puwiss.id_stg = uwiss.Status_stg

    WHERE uwiss.updatetime_stg > (:start_dttm) and uwiss.updatetime_stg <= (:end_dttm)

	and pjl.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

) a

QUALIFY ROW_NUMBER () OVER (partition by jobnumber,typecode,publicid order by updatetime desc) =1
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_uwissuehistory.JobNumber as JobNumber,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE */ IS NULL , ''UNK'' , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE */ ) as o_APLCTN_TYPE_CD,
SQ_pc_uwissuehistory.PublicID_contact as PublicID_contact,
''PRTY_APLCTN_ROLE1'' as in_PRTY_APLCTN_ROLE_CD,
:PRCS_ID as PRCS_ID,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */ as out_SYS_SRC_CD,
CASE WHEN SQ_pc_uwissuehistory.busn_start_dt IS NOT NULL THEN SQ_pc_uwissuehistory.busn_start_dt ELSE to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) END as out_busn_start_dt,
CASE WHEN SQ_pc_uwissuehistory.busn_end_dt IS NOT NULL THEN SQ_pc_uwissuehistory.busn_end_dt ELSE TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) END as out_busn_end_dt,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
SQ_pc_uwissuehistory.UpdateTime as UpdateTime,
SQ_pc_uwissuehistory.source_record_id,
row_number() over (partition by SQ_pc_uwissuehistory.source_record_id order by SQ_pc_uwissuehistory.source_record_id) as RNK
FROM
SQ_pc_uwissuehistory
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_uwissuehistory.TYPECODE_job
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_uwissuehistory.TYPECODE_job
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pc_uwissuehistory.SYS_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_APLCTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_APLCTN AS
(
SELECT
LKP.APLCTN_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.APLCTN_ID asc,LKP.APLCTN_TYPE_CD asc,LKP.HOST_APLCTN_ID asc,LKP.APLCTN_CMPLTD_DTTM asc,LKP.APLCTN_RECVD_DTTM asc,LKP.SRC_SYS_CD asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.APLCTN_QUOT_TYPE_CD asc,LKP.PROD_GRP_ID asc,LKP.PROD_ID asc,LKP.CHNL_TYPE_CD asc,LKP.HOST_APLCTN_NUM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.TRANS_END_DTTM asc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT APLCTN.APLCTN_ID as APLCTN_ID, 
APLCTN.APLCTN_CMPLTD_DTTM as APLCTN_CMPLTD_DTTM, 
APLCTN.APLCTN_RECVD_DTTM as APLCTN_RECVD_DTTM,
APLCTN.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD,
APLCTN.APLCTN_QUOT_TYPE_CD as APLCTN_QUOT_TYPE_CD, 
APLCTN.PROD_GRP_ID as PROD_GRP_ID,
APLCTN.PROD_ID as PROD_ID, 
APLCTN.CHNL_TYPE_CD as CHNL_TYPE_CD, 
APLCTN.HOST_APLCTN_NUM as HOST_APLCTN_NUM, 
APLCTN.EDW_STRT_DTTM as EDW_STRT_DTTM, 
APLCTN.EDW_END_DTTM as EDW_END_DTTM, 
APLCTN.HOST_APLCTN_ID as HOST_APLCTN_ID, 
APLCTN.SRC_SYS_CD as SRC_SYS_CD, 
APLCTN.APLCTN_TYPE_CD as APLCTN_TYPE_CD,
APLCTN.TRANS_STRT_DTTM as TRANS_STRT_DTTM, 
APLCTN.TRANS_END_DTTM as TRANS_END_DTTM
FROM DB_T_PROD_CORE.APLCTN
QUALIFY ROW_NUMBER () OVER (partition by HOST_APLCTN_ID,SRC_SYS_CD order by EDW_END_DTTM desc)=1
) LKP ON LKP.HOST_APLCTN_ID = exp_pass_from_src.JobNumber AND LKP.SRC_SYS_CD = exp_pass_from_src.out_SYS_SRC_CD AND LKP.APLCTN_TYPE_CD = exp_pass_from_src.o_APLCTN_TYPE_CD
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
) LKP ON LKP.NK_PUBLC_ID = exp_pass_from_src.PublicID_contact
QUALIFY RNK = 1
);


-- Component LKP_PRTY_APLCTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_APLCTN AS
(
SELECT
LKP.APLCTN_ID,
LKP.PRTY_APLCTN_ROLE_CD,
LKP.APLCTN_PRTY_STRT_DTTM,
LKP.PRTY_ID,
LKP.APLCTN_PRTY_END_DTTM,
exp_pass_from_src.out_busn_start_dt as in_busn_start_dt,
exp_pass_from_src.out_busn_end_dt as in_busn_end_dt,
exp_pass_from_src.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_from_src.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.APLCTN_ID desc,LKP.PRTY_APLCTN_ROLE_CD desc,LKP.APLCTN_PRTY_STRT_DTTM desc,LKP.PRTY_ID desc,LKP.APLCTN_PRTY_END_DTTM desc) RNK1
FROM
exp_pass_from_src
INNER JOIN LKP_APLCTN ON exp_pass_from_src.source_record_id = LKP_APLCTN.source_record_id
INNER JOIN LKP_INDIV_CLM_CTR ON LKP_APLCTN.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
--INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON LKP_INDIV_CLM_CTR.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
LEFT JOIN (
SELECT	PRTY_APLCTN.APLCTN_PRTY_STRT_DTTM as APLCTN_PRTY_STRT_DTTM,
		PRTY_APLCTN.APLCTN_PRTY_END_DTTM as APLCTN_PRTY_END_DTTM, 
		PRTY_APLCTN.APLCTN_ID as APLCTN_ID,
		PRTY_APLCTN.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD, 
		PRTY_APLCTN.PRTY_ID as PRTY_ID 
FROM	DB_T_PROD_CORE.PRTY_APLCTN
QUALIFY	ROW_NUMBER () OVER (
PARTITION BY  PRTY_ID,
		APLCTN_ID,PRTY_APLCTN_ROLE_CD  
ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.APLCTN_ID = LKP_APLCTN.APLCTN_ID AND
--LKP.PRTY_APLCTN_ROLE_CD = LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AND 
LKP.PRTY_ID = LKP_INDIV_CLM_CTR.INDIV_PRTY_ID
QUALIFY RNK1 = 1
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
LKP_PRTY_APLCTN.APLCTN_ID as lkp_APLCTN_ID,
LKP_PRTY_APLCTN.PRTY_APLCTN_ROLE_CD as lkp_PRTY_APLCTN_ROLE_CD,
LKP_PRTY_APLCTN.APLCTN_PRTY_STRT_DTTM as lkp_APLCTN_PRTY_STRT_DT,
LKP_PRTY_APLCTN.PRTY_ID as lkp_PRTY_ID,
exp_pass_from_src.out_SYS_SRC_CD as in_PRTY_APLCTN_ROLE_CD,
LKP_APLCTN.APLCTN_ID as in_APLCTN_ID,
LKP_INDIV_CLM_CTR.INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp_pass_from_src.PRCS_ID as PRCS_ID,
LKP_PRTY_APLCTN.in_busn_start_dt as busn_start_dt,
LKP_PRTY_APLCTN.in_busn_end_dt as busn_end_dt,
LKP_PRTY_APLCTN.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
LKP_PRTY_APLCTN.in_EDW_END_DTTM as EDW_END_DTTM,
md5 ( to_char ( LKP_PRTY_APLCTN.in_busn_start_dt , ''yyyy-mm-dd'' ) || to_char ( LKP_PRTY_APLCTN.in_busn_end_dt , ''yyyy-mm-dd'' ) ) as chksum_inp,
md5 ( to_char ( LKP_PRTY_APLCTN.APLCTN_PRTY_STRT_DTTM , ''yyyy-mm-dd'' ) || to_char ( LKP_PRTY_APLCTN.APLCTN_PRTY_END_DTTM , ''yyyy-mm-dd'' ) ) as chksum_lkp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN ( ( chksum_inp != chksum_lkp ) and ( LKP_PRTY_APLCTN.in_busn_start_dt > LKP_PRTY_APLCTN.APLCTN_PRTY_STRT_DTTM ) ) THEN ''U'' ELSE ''R'' END END as flag,
exp_pass_from_src.UpdateTime as TRANS_STRT_DTTM,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_APLCTN ON exp_pass_from_src.source_record_id = LKP_APLCTN.source_record_id
INNER JOIN LKP_INDIV_CLM_CTR ON LKP_APLCTN.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
--INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON LKP_INDIV_CLM_CTR.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
INNER JOIN LKP_PRTY_APLCTN ON LKP_APLCTN.source_record_id = LKP_PRTY_APLCTN.source_record_id
);


-- Component rtr_prty_aplctn_insupd_INS_UPD, Type ROUTER Output Group INS_UPD
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_aplctn_insupd_INS_UPD AS
(SELECT
EXPTRANS.in_APLCTN_ID as in_APLCTN_ID,
EXPTRANS.in_PRTY_APLCTN_ROLE_CD as in_PRTY_APLCTN_ROLE_CD,
EXPTRANS.in_INDIV_PRTY_ID as in_INDIV_PRTY_ID,
EXPTRANS.lkp_APLCTN_ID as lkp_APLCTN_ID,
EXPTRANS.lkp_PRTY_APLCTN_ROLE_CD as lkp_PRTY_APLCTN_ROLE_CD,
EXPTRANS.lkp_APLCTN_PRTY_STRT_DT as lkp_APLCTN_PRTY_STRT_DT,
EXPTRANS.lkp_PRTY_ID as lkp_PRTY_ID,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.busn_start_dt as busn_start_dt,
EXPTRANS.busn_end_dt as busn_end_dt,
EXPTRANS.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
EXPTRANS.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
EXPTRANS.EDW_END_DTTM as in_EDW_END_DTTM,
EXPTRANS.flag as flag,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE ( EXPTRANS.flag = ''I'' or EXPTRANS.flag = ''U'' ) and EXPTRANS.in_APLCTN_ID IS NOT NULL and EXPTRANS.in_PRTY_APLCTN_ROLE_CD IS NOT NULL and EXPTRANS.in_INDIV_PRTY_ID IS NOT NULL);


-- Component upd_prty_aplctn_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_aplctn_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_aplctn_insupd_INS_UPD.in_APLCTN_ID as APLCTN_ID,
rtr_prty_aplctn_insupd_INS_UPD.in_PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
rtr_prty_aplctn_insupd_INS_UPD.in_INDIV_PRTY_ID as PRTY_ID,
rtr_prty_aplctn_insupd_INS_UPD.PRCS_ID as PRCS_ID,
rtr_prty_aplctn_insupd_INS_UPD.busn_start_dt as busn_start_dt1,
rtr_prty_aplctn_insupd_INS_UPD.busn_end_dt as busn_end_dt1,
rtr_prty_aplctn_insupd_INS_UPD.in_EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_prty_aplctn_insupd_INS_UPD.in_EDW_END_DTTM as EDW_END_DTTM1,
rtr_prty_aplctn_insupd_INS_UPD.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_prty_aplctn_insupd_INS_UPD.source_record_id
FROM
rtr_prty_aplctn_insupd_INS_UPD
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_prty_aplctn_ins.APLCTN_ID as APLCTN_ID,
upd_prty_aplctn_ins.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
upd_prty_aplctn_ins.PRTY_ID as PRTY_ID,
upd_prty_aplctn_ins.PRCS_ID as PRCS_ID,
upd_prty_aplctn_ins.busn_start_dt1 as busn_start_dt1,
upd_prty_aplctn_ins.busn_end_dt1 as busn_end_dt1,
upd_prty_aplctn_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_prty_aplctn_ins.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_prty_aplctn_ins.in_TRANS_STRT_DTTM1 as in_TRANS_STRT_DTTM1,
upd_prty_aplctn_ins.source_record_id
FROM
upd_prty_aplctn_ins
);


-- Component tgt_PRTY_APLCTN_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_APLCTN
(
APLCTN_ID,
PRTY_APLCTN_ROLE_CD,
APLCTN_PRTY_STRT_DTTM,
PRTY_ID,
APLCTN_PRTY_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins.APLCTN_ID as APLCTN_ID,
exp_pass_to_tgt_ins.PRTY_APLCTN_ROLE_CD as PRTY_APLCTN_ROLE_CD,
exp_pass_to_tgt_ins.busn_start_dt1 as APLCTN_PRTY_STRT_DTTM,
exp_pass_to_tgt_ins.PRTY_ID as PRTY_ID,
exp_pass_to_tgt_ins.busn_end_dt1 as APLCTN_PRTY_END_DTTM,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component tgt_PRTY_APLCTN_ins, Type Post SQL 
UPDATE  DB_T_PROD_CORE.PRTY_APLCTN  FROM

(SELECT distinct  APLCTN_ID,PRTY_APLCTN_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by  APLCTN_ID,PRTY_APLCTN_ROLE_CD,PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1

,max(TRANS_STRT_DTTM) over (partition by  APLCTN_ID,PRTY_APLCTN_ROLE_CD,PRTY_ID ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''  as lead

,EDW_END_DTTM

FROM    DB_T_PROD_CORE.PRTY_APLCTN

) a

set EDW_END_DTTM=A.lead1

,TRANS_END_DTTM=a.lead

where  PRTY_APLCTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and PRTY_APLCTN.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

and PRTY_APLCTN.APLCTN_ID=A.APLCTN_ID 

and PRTY_APLCTN.PRTY_APLCTN_ROLE_CD=A.PRTY_APLCTN_ROLE_CD

and PRTY_APLCTN.PRTY_ID=A.PRTY_ID

and  CAST(PRTY_APLCTN.EDW_END_DTTM AS DATE)=''9999-12-31''

and CAST(PRTY_APLCTN.TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null and lead is not null;


END; ';