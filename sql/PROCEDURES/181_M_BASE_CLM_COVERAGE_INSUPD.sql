-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_COVERAGE_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	PRCS_ID int;

BEGIN 
start_dttm := current_timestamp();
end_dttm := current_timestamp();
PRCS_ID := 1;

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_CVGE_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pc_etlclausepattern.coveragesubtype'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


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


-- Component SQ_cc_coverage_feature, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_coverage_feature AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClaimNumber,
$2 as Coveragesubtype,
$3 as Clusename,
$4 as Clausetype,
$5 as CREATETIME,
$6 as UPDATETIME,
$7 as SRC_CD,
$8 as Retired,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	SRC.ClaimNumber,

		SRC.Coveragesubtype, 

		SRC.Clusename,

		SRC.Clausetype, 

		SRC.CREATETIME,

		SRC.UPDATETIME,  

''SRC_SYS6'' as SRC_CD,

SRC.Retired as Retired

FROM

/* cc_coverage_feature  */
 (

 select 

distinct

claimnumber_stg as ClaimNumber,

SUBSTR(cc_coverage.PolicySystemId_stg, POSITION(''.'' IN cc_coverage.PolicySystemId_stg)+1,POSITION('':'' IN cc_coverage.PolicySystemId_stg)- POSITION(''.'' IN cc_coverage.PolicySystemId_stg)-1) as CoverageSubtype, 

cast(cctl_coveragetype.typecode_stg as varchar(40)) as Clusename,

''COVERAGE'' as clausetype, 

cc_claim.CreateTime_stg as CREATETIME,

cc_coverage.UpdateTime_stg as UPDATETIME,

/* CASE WHEN cc_claim.Retired_stg=0 and cc_coverage.Retired_stg=0 and cc_exposure.Retired_stg=0 THEN 0 ELSE 1 END Retired */
CASE WHEN cc_claim.Retired_stg=0 and cc_coverage.Retired_stg=0 and cc_exposure.Retired_stg=0 THEN 0 ELSE 1 END as Retired

from (select cc_claim.* from DB_T_PROD_STAG.cc_claim cc_claim  inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim join DB_T_PROD_STAG.cc_exposure 

on cc_claim.id_stg=cc_exposure.claimid_stg

join DB_T_PROD_STAG.cc_coverage on cc_exposure.CoverageID_stg=cc_coverage.ID_stg

join DB_T_PROD_STAG.cctl_coveragetype on cc_coverage.Type_stg=cctl_coveragetype.ID_stg

where 

cc_exposure.UpdateTime_stg > (:start_dttm)

and cc_exposure.UpdateTime_stg <= (:end_dttm)

 ) SRC

  

 QUALIFY	ROW_NUMBER() OVER(

PARTITION BY  SRC.ClaimNumber,SRC.Coveragesubtype,

		SRC.Clusename, SRC.Clausetype 

ORDER BY SRC.UPDATETIME desc) = 1
) SRC
)
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
SQ_cc_coverage_feature.ClaimNumber as ClaimNumber,
SQ_cc_coverage_feature.Clusename as Clusename,
''CL'' as out_feat_sbtype,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_SRC_CD,
SQ_cc_coverage_feature.Retired as Retired,
SQ_cc_coverage_feature.source_record_id,
row_number() over (partition by SQ_cc_coverage_feature.source_record_id order by SQ_cc_coverage_feature.source_record_id) as RNK
FROM
SQ_cc_coverage_feature
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_coverage_feature.SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM db_t_prod_core.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp.ClaimNumber AND LKP.SRC_SYS_CD = exp.out_SRC_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) 
= 1
);


-- Component LKP_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT AS
(
SELECT
LKP.FEAT_ID,
exp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc) RNK
FROM
exp
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.FEAT
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.FEAT_SBTYPE_CD = exp.out_feat_sbtype AND LKP.NK_SRC_KEY = exp.Clusename
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc)  
= 1
);


-- Component exp1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp1 AS
(
SELECT
LKP_CLM.CLM_ID as CLM_ID,
LKP_FEAT.FEAT_ID as FEAT_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
SQ_cc_coverage_feature.CREATETIME as CREATETIME,
SQ_cc_coverage_feature.UPDATETIME as UPDATETIME,
ltrim ( rtrim ( to_char ( SQ_cc_coverage_feature.UPDATETIME ) ) ) as v_UpdateTime,
exp.Retired as Retired,
SQ_cc_coverage_feature.source_record_id
FROM
SQ_cc_coverage_feature
INNER JOIN exp ON SQ_cc_coverage_feature.source_record_id = exp.source_record_id
INNER JOIN LKP_CLM ON exp.source_record_id = LKP_CLM.source_record_id
INNER JOIN LKP_FEAT ON LKP_CLM.source_record_id = LKP_FEAT.source_record_id
);


-- Component LKP_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TGT AS
(
SELECT
LKP.CLM_ID,
LKP.CVGE_FEAT_ID,
LKP.CLM_CVGE_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.EDW_STRT_DTTM,
exp1.CLM_ID as CLM_ID1,
exp1.FEAT_ID as FEAT_ID,
exp1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp1.source_record_id ORDER BY LKP.CLM_ID asc,LKP.CVGE_FEAT_ID asc,LKP.CLM_CVGE_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.EDW_STRT_DTTM asc) RNK
FROM
exp1
LEFT JOIN (
SELECT CLM_CVGE.CLM_CVGE_STRT_DTTM as CLM_CVGE_STRT_DTTM, CLM_CVGE.EDW_END_DTTM as EDW_END_DTTM, CLM_CVGE.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_CVGE.CLM_ID as CLM_ID, CLM_CVGE.CVGE_FEAT_ID as CVGE_FEAT_ID FROM db_t_prod_core.CLM_CVGE as CLM_CVGE
) LKP ON LKP.CLM_ID = exp1.CLM_ID AND LKP.CVGE_FEAT_ID = exp1.FEAT_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp1.source_record_id ORDER BY LKP.CLM_ID asc,LKP.CVGE_FEAT_ID asc,LKP.CLM_CVGE_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.EDW_STRT_DTTM asc) 
= 1
);


-- Component exp2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp2 AS
(
SELECT
LKP_TGT.CLM_ID as LKP_CLM_ID,
LKP_TGT.CVGE_FEAT_ID as LKP_CVGE_FEAT_ID,
CASE WHEN LKP_TGT.CLM_ID IS NULL THEN ''I'' ELSE ( CASE WHEN LKP_TGT.CLM_CVGE_STRT_DTTM <> exp1.CREATETIME THEN ''U'' ELSE ''R'' END ) END as o_flag,
LKP_TGT.CLM_ID1 as CLM_ID,
LKP_TGT.FEAT_ID as FEAT_ID,
:PRCS_ID as prcs_id,
exp1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp1.EDW_END_DTTM as EDW_END_DTTM,
CURRENT_TIMESTAMP as out_start_dt,
exp1.CREATETIME as CREATETIME,
exp1.UPDATETIME as UPDATETIME,
exp1.Retired as Retired,
LKP_TGT.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_TGT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
exp1.source_record_id
FROM
exp1
INNER JOIN LKP_TGT ON exp1.source_record_id = LKP_TGT.source_record_id
);


-- Component rtr_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_Retired AS
SELECT
exp2.CLM_ID as CLM_ID,
exp2.FEAT_ID as FEAT_ID,
exp2.o_flag as o_flag,
exp2.prcs_id as process_id,
exp2.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp2.EDW_END_DTTM as EDW_END_DTTM,
exp2.out_start_dt as out_start_dt,
exp2.CREATETIME as CREATETIME,
exp2.UPDATETIME as UPDATETIME,
exp2.Retired as Retired,
exp2.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp2.LKP_CLM_ID as LKP_CLM_ID,
exp2.LKP_CVGE_FEAT_ID as LKP_CVGE_FEAT_ID,
exp2.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp2.source_record_id
FROM
exp2
WHERE exp2.o_flag = ''R'' and exp2.Retired != 0 and exp2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component rtr_insert, Type ROUTER Output Group insert
create or replace temporary table rtr_insert as
SELECT
exp2.CLM_ID as CLM_ID,
exp2.FEAT_ID as FEAT_ID,
exp2.o_flag as o_flag,
exp2.prcs_id as process_id,
exp2.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp2.EDW_END_DTTM as EDW_END_DTTM,
exp2.out_start_dt as out_start_dt,
exp2.CREATETIME as CREATETIME,
exp2.UPDATETIME as UPDATETIME,
exp2.Retired as Retired,
exp2.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp2.LKP_CLM_ID as LKP_CLM_ID,
exp2.LKP_CVGE_FEAT_ID as LKP_CVGE_FEAT_ID,
exp2.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp2.source_record_id
FROM
exp2
WHERE exp2.o_flag = ''I'' AND exp2.CLM_ID IS NOT NULL AND exp2.FEAT_ID IS NOT NULL OR ( exp2.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp2.Retired = 0 ) /*- - exp2.o_flag = 1 and exp2.FEAT_ID IS NOT NULL and exp2.CLM_ID IS NOT NULL*/
;


-- Component rtr_update, Type ROUTER Output Group update
create or replace temporary table rtr_update as
SELECT
exp2.CLM_ID as CLM_ID,
exp2.FEAT_ID as FEAT_ID,
exp2.o_flag as o_flag,
exp2.prcs_id as process_id,
exp2.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp2.EDW_END_DTTM as EDW_END_DTTM,
exp2.out_start_dt as out_start_dt,
exp2.CREATETIME as CREATETIME,
exp2.UPDATETIME as UPDATETIME,
exp2.Retired as Retired,
exp2.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp2.LKP_CLM_ID as LKP_CLM_ID,
exp2.LKP_CVGE_FEAT_ID as LKP_CVGE_FEAT_ID,
exp2.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp2.source_record_id
FROM
exp2
WHERE exp2.o_flag = ''U'' AND exp2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) /*- - exp2.o_flag = 0 and exp2.FEAT_ID IS NOT NULL and exp2.CLM_ID IS NOT NULL*/
;

-- Component upd_cdc, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_cdc AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_update.LKP_CLM_ID as CLM_ID3,
rtr_update.LKP_CVGE_FEAT_ID as FEAT_ID3,
rtr_update.process_id as process_id3,
rtr_update.lkp_EDW_STRT_DTTM1 as EDW_STRT_DTTM3,
rtr_update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_update.UPDATETIME as UPDATETIME3,
1 as UPDATE_STRATEGY_ACTION,
rtr_update.source_record_id
FROM
rtr_update
);


-- Component exp_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert AS
(
SELECT
rtr_insert.CLM_ID as CLM_ID1,
rtr_insert.FEAT_ID as FEAT_ID1,
rtr_insert.process_id as process_id1,
rtr_insert.CREATETIME as CREATETIME1,
rtr_insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_insert.UPDATETIME as UPDATETIME1,
CASE WHEN rtr_insert.Retired = 0 THEN rtr_insert.EDW_END_DTTM ELSE CURRENT_TIMESTAMP END as o_EDW_END_DTTM11,
rtr_insert.source_record_id
FROM
rtr_insert
);


-- Component upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Retired.LKP_CLM_ID as CLM_ID3,
rtr_Retired.LKP_CVGE_FEAT_ID as FEAT_ID3,
rtr_Retired.process_id as process_id3,
rtr_Retired.lkp_EDW_STRT_DTTM1 as EDW_STRT_DTTM3,
rtr_Retired.UPDATETIME as UPDATETIME4,
1 as UPDATE_STRATEGY_ACTION,
rtr_Retired.source_record_id
FROM
rtr_Retired
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
rtr_update.Retired as Retired3,
rtr_update.CLM_ID as CLM_ID3,
rtr_update.FEAT_ID as FEAT_ID3,
rtr_update.process_id as process_id3,
rtr_update.CREATETIME as CREATETIME3,
rtr_update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_update.UPDATETIME as UPDATETIME3,
rtr_update.source_record_id
FROM
rtr_update
WHERE rtr_update.Retired = 0
);


-- Component CLM_CVGE_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_CVGE
(
CLM_ID,
CVGE_FEAT_ID,
PRCS_ID,
CLM_CVGE_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_insert.CLM_ID1 as CLM_ID,
exp_insert.FEAT_ID1 as CVGE_FEAT_ID,
exp_insert.process_id1 as PRCS_ID,
exp_insert.CREATETIME1 as CLM_CVGE_STRT_DTTM,
exp_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_insert.o_EDW_END_DTTM11 as EDW_END_DTTM,
exp_insert.UPDATETIME1 as TRANS_STRT_DTTM
FROM
exp_insert;


-- Component exp_upd_cdc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_upd_cdc AS
(
SELECT
upd_cdc.CLM_ID3 as CLM_ID3,
upd_cdc.FEAT_ID3 as FEAT_ID3,
upd_cdc.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
dateadd ( second, -1, CURRENT_TIMESTAMP  ) as O_end_dt,
dateadd ( second, -1, upd_cdc.UPDATETIME3 ) as UPDATETIME31,
upd_cdc.source_record_id
FROM
upd_cdc
);


-- Component CLM_CVGE_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_CVGE
USING exp_upd_cdc ON (CLM_CVGE.CLM_ID = exp_upd_cdc.CLM_ID3 AND CLM_CVGE.CVGE_FEAT_ID = exp_upd_cdc.FEAT_ID3 AND CLM_CVGE.EDW_END_DTTM = exp_upd_cdc.O_end_dt)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_upd_cdc.CLM_ID3,
CVGE_FEAT_ID = exp_upd_cdc.FEAT_ID3,
EDW_STRT_DTTM = exp_upd_cdc.EDW_STRT_DTTM3,
EDW_END_DTTM = exp_upd_cdc.O_end_dt,
TRANS_END_DTTM = exp_upd_cdc.UPDATETIME31;


-- Component exp_retire_id_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_retire_id_upd AS
(
SELECT
upd_retired.CLM_ID3 as CLM_ID3,
upd_retired.FEAT_ID3 as FEAT_ID3,
upd_retired.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
dateadd ( second, -1, CURRENT_TIMESTAMP ) as EDW_END_DTTM,
dateadd ( second, -1, upd_retired.UPDATETIME4 ) as UPDATETIME41,
upd_retired.source_record_id
FROM
upd_retired
);


-- Component exp_insert_CDC, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert_CDC AS
(
SELECT
FILTRANS.CLM_ID3 as CLM_ID1,
FILTRANS.FEAT_ID3 as FEAT_ID1,
FILTRANS.process_id3 as process_id1,
FILTRANS.CREATETIME3 as CREATETIME1,
FILTRANS.EDW_STRT_DTTM3 as EDW_STRT_DTTM1,
FILTRANS.UPDATETIME3 as UPDATETIME1,
FILTRANS.source_record_id
FROM
FILTRANS
);


-- Component tgt_CLM_CVGE_upd_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_CVGE
USING exp_retire_id_upd ON (CLM_CVGE.CLM_ID = exp_retire_id_upd.CLM_ID3 AND CLM_CVGE.CVGE_FEAT_ID = exp_retire_id_upd.FEAT_ID3 AND CLM_CVGE.EDW_END_DTTM = exp_retire_id_upd.EDW_END_DTTM)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_retire_id_upd.CLM_ID3,
CVGE_FEAT_ID = exp_retire_id_upd.FEAT_ID3,
EDW_STRT_DTTM = exp_retire_id_upd.EDW_STRT_DTTM3,
EDW_END_DTTM = exp_retire_id_upd.EDW_END_DTTM,
TRANS_END_DTTM = exp_retire_id_upd.UPDATETIME41;


-- Component CLM_CVGE_ins1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_CVGE
(
CLM_ID,
CVGE_FEAT_ID,
PRCS_ID,
CLM_CVGE_STRT_DTTM,
EDW_STRT_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_insert_CDC.CLM_ID1 as CLM_ID,
exp_insert_CDC.FEAT_ID1 as CVGE_FEAT_ID,
exp_insert_CDC.process_id1 as PRCS_ID,
exp_insert_CDC.CREATETIME1 as CLM_CVGE_STRT_DTTM,
exp_insert_CDC.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_insert_CDC.UPDATETIME1 as TRANS_STRT_DTTM
FROM
exp_insert_CDC;


END; ';