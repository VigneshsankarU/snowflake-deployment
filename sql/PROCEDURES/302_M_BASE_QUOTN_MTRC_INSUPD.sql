-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_MTRC_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;

BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1; 

-- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_quotn_mtrc_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_quotn_mtrc_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as jobnumber,
$2 as branchnumber,
$3 as AutoLatePayCount,
$4 as EditEffectiveDate,
$5 as Amount,
$6 as Rate,
$7 as Quotn_Mtrc_Type,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	distinct pc_job.jobnumber_stg, 

pc_policyperiod.branchnumber_stg,

/* ''SRC_SYS4'' AS SYS_SRC_CD,  */
pcx_holineratingfactor_alfa.AutoLatePayCount_stg, 

pc_policyperiod.EditEffectiveDate_stg,

NULL as Amount, 

NULL as Rate,

''INSRNC_MTRC_TYPE17'' quotn_mtrc_type

/* (:start_dttm) as start_dttm, */
/* (:end_dttm) as end_dttm */
FROM	DB_T_PROD_STAG.pc_job 

inner join DB_T_PROD_STAG.pc_policyperiod on pc_job.ID_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pcx_holineratingfactor_alfa on pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg

INNER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg 

LEFT JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

inner join  DB_T_PROD_STAG.pc_policy on pc_policyperiod.PolicyID_stg=pc_policy.ID_stg 

left join DB_T_PROD_STAG.pc_policyline on pc_policyperiod.id_stg = pc_policyline.BranchID_stg

left join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pc_policyline.HOPolicyType_stg = pctl_hopolicytype_hoe.id_stg 

left join DB_T_PROD_STAG.pctl_papolicytype_alfa on pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.id_stg

where	  pc_policy.ProductCode_stg in (''Homeowners'') 

  and pctl_hopolicytype_hoe.NAME_stg=''Homeowners (HO3)''

	AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	AND pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary'' 

	and policynumber_stg is not null

	and pcx_holineratingfactor_alfa.AutoLatePayCount_stg is not null 

	and pc_job.UpdateTime_stg > (:start_dttm)

	and pc_job.UpdateTime_stg <= (:end_dttm)
) SRC
)
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
SQ_pc_quotn_mtrc_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_pc_quotn_mtrc_x.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
SQ_pc_quotn_mtrc_x
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = SQ_pc_quotn_mtrc_x.jobnumber AND LKP.VERS_NBR = SQ_pc_quotn_mtrc_x.branchnumber
QUALIFY RNK = 1
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
LKP_INSRNC_QUOTN.QUOTN_ID as Quotn_ID,
SQ_pc_quotn_mtrc_x.AutoLatePayCount as AutoLatePayCount,
SQ_pc_quotn_mtrc_x.EditEffectiveDate as EditEffectiveDate,
SQ_pc_quotn_mtrc_x.Amount as Amount,
SQ_pc_quotn_mtrc_x.Rate as Rate,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE */ END as out_Quotn_Mtrc_Type,
SQ_pc_quotn_mtrc_x.source_record_id,
row_number() over (partition by SQ_pc_quotn_mtrc_x.source_record_id order by SQ_pc_quotn_mtrc_x.source_record_id) as RNK
FROM
SQ_pc_quotn_mtrc_x
INNER JOIN LKP_INSRNC_QUOTN ON SQ_pc_quotn_mtrc_x.source_record_id = LKP_INSRNC_QUOTN.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_quotn_mtrc_x.Quotn_Mtrc_Type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_quotn_mtrc_x.Quotn_Mtrc_Type
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_MTRC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_MTRC AS
(
SELECT
LKP.QUOTN_ID,
LKP.QUOTN_MTRC_TYPE_CD,
LKP.QUOTN_MTRC_STRT_DTTM,
LKP.QUOTN_MTRC_CNT,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.QUOTN_MTRC_TYPE_CD asc,LKP.QUOTN_MTRC_STRT_DTTM asc,LKP.QUOTN_MTRC_CNT asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT QUOTN_MTRC.QUOTN_ID as QUOTN_ID,
QUOTN_MTRC.QUOTN_MTRC_TYPE_CD as QUOTN_MTRC_TYPE_CD, QUOTN_MTRC.QUOTN_MTRC_STRT_DTTM as QUOTN_MTRC_STRT_DTTM, QUOTN_MTRC.QUOTN_MTRC_CNT as QUOTN_MTRC_CNT,  QUOTN_MTRC.EDW_STRT_DTTM as EDW_STRT_DTTM, QUOTN_MTRC.EDW_END_DTTM as EDW_END_DTTM 
FROM DB_T_PROD_CORE.QUOTN_MTRC
QUALIFY ROW_NUMBER() OVER(PARTITION BY QUOTN_ID, QUOTN_MTRC_TYPE_CD, QUOTN_MTRC_STRT_DTTM  ORDER BY EDW_STRT_DTTM DESC)=1
) LKP ON LKP.QUOTN_ID = exp_all_source.Quotn_ID AND LKP.QUOTN_MTRC_TYPE_CD = exp_all_source.out_Quotn_Mtrc_Type AND LKP.QUOTN_MTRC_STRT_DTTM = exp_all_source.EditEffectiveDate
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_all_source.Quotn_ID as Quotn_ID,
exp_all_source.Quotn_ID as Quotn_ID1,
exp_all_source.AutoLatePayCount as AutoLatePayCount,
exp_all_source.EditEffectiveDate as EditEffectiveDate,
exp_all_source.Amount as Amount,
exp_all_source.Rate as Rate,
exp_all_source.out_Quotn_Mtrc_Type as Quotn_Mtrc_Type,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
TO_DATE ( ''01/01/1900'' , ''MM/DD/YYYY'' ) as TRANS_STRT_DTTM,
LKP_QUOTN_MTRC.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_MTRC.QUOTN_MTRC_TYPE_CD as lkp_QUOTN_MTRC_TYPE_CD,
LKP_QUOTN_MTRC.QUOTN_MTRC_STRT_DTTM as lkp_QUOTN_MTRC_STRT_DTTM,
LKP_QUOTN_MTRC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_QUOTN_MTRC.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_QUOTN_MTRC.QUOTN_MTRC_CNT as lkp_QUOTN_MTRC_CNT,
MD5 ( rtrim ( ltrim ( exp_all_source.out_Quotn_Mtrc_Type ) ) || ltrim ( rtrim ( to_char ( exp_all_source.EditEffectiveDate , ''MM/DD/YYYY'' ) ) ) || ltrim ( rtrim ( exp_all_source.AutoLatePayCount ) ) ) as var_calc_chksm,
MD5 ( rtrim ( ltrim ( LKP_QUOTN_MTRC.QUOTN_MTRC_TYPE_CD ) ) || ltrim ( rtrim ( to_char ( LKP_QUOTN_MTRC.QUOTN_MTRC_STRT_DTTM , ''MM/DD/YYYY'' ) ) ) || ltrim ( rtrim ( LKP_QUOTN_MTRC.QUOTN_MTRC_CNT ) ) ) as var_orig_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_QUOTN_MTRC ON exp_all_source.source_record_id = LKP_QUOTN_MTRC.source_record_id
);


-- Component rtr_quotn_mtrc_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_quotn_mtrc_INSERT AS
(SELECT
exp_data_transformation.Quotn_ID1 as Quotn_ID,
exp_data_transformation.AutoLatePayCount as AutoLatePayCount,
exp_data_transformation.EditEffectiveDate as EditEffectiveDate,
exp_data_transformation.Amount as Amount,
exp_data_transformation.Rate as Rate,
exp_data_transformation.Quotn_Mtrc_Type as Quotn_Mtrc_Type,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_data_transformation.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_data_transformation.lkp_QUOTN_MTRC_TYPE_CD as lkp_QUOTN_MTRC_TYPE_CD,
exp_data_transformation.lkp_QUOTN_MTRC_STRT_DTTM as lkp_QUOTN_MTRC_STRT_DTTM,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.lkp_QUOTN_MTRC_CNT as lkp_QUOTN_MTRC_CNT,
exp_data_transformation.out_ins_upd as out_ins_upd,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.lkp_QUOTN_ID IS NULL and exp_data_transformation.Quotn_ID1 IS NOT NULL);


-- Component rtr_quotn_mtrc_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_quotn_mtrc_UPDATE AS
(SELECT
exp_data_transformation.Quotn_ID1 as Quotn_ID,
exp_data_transformation.AutoLatePayCount as AutoLatePayCount,
exp_data_transformation.EditEffectiveDate as EditEffectiveDate,
exp_data_transformation.Amount as Amount,
exp_data_transformation.Rate as Rate,
exp_data_transformation.Quotn_Mtrc_Type as Quotn_Mtrc_Type,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_data_transformation.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_data_transformation.lkp_QUOTN_MTRC_TYPE_CD as lkp_QUOTN_MTRC_TYPE_CD,
exp_data_transformation.lkp_QUOTN_MTRC_STRT_DTTM as lkp_QUOTN_MTRC_STRT_DTTM,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.lkp_QUOTN_MTRC_CNT as lkp_QUOTN_MTRC_CNT,
exp_data_transformation.out_ins_upd as out_ins_upd,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.lkp_QUOTN_ID IS NOT NULL and exp_data_transformation.AutoLatePayCount <> exp_data_transformation.lkp_QUOTN_MTRC_CNT);


-- Component upd_quotn_mtrc_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_mtrc_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_mtrc_UPDATE.Quotn_ID as Quotn_ID1,
rtr_quotn_mtrc_UPDATE.AutoLatePayCount as AutoLatePayCount1,
rtr_quotn_mtrc_UPDATE.EditEffectiveDate as EditEffectiveDate1,
rtr_quotn_mtrc_UPDATE.Amount as Amount1,
rtr_quotn_mtrc_UPDATE.Rate as Rate1,
rtr_quotn_mtrc_UPDATE.Quotn_Mtrc_Type as Quotn_Mtrc_Type1,
rtr_quotn_mtrc_UPDATE.PRCS_ID as PRCS_ID1,
rtr_quotn_mtrc_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_quotn_mtrc_UPDATE.EDW_END_DTTM as EDW_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_quotn_mtrc_UPDATE.source_record_id
FROM
rtr_quotn_mtrc_UPDATE
);


-- Component upd_quotn_mtrc_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_mtrc_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_mtrc_INSERT.Quotn_ID as Quotn_ID1,
rtr_quotn_mtrc_INSERT.AutoLatePayCount as AutoLatePayCount1,
rtr_quotn_mtrc_INSERT.EditEffectiveDate as EditEffectiveDate1,
rtr_quotn_mtrc_INSERT.Amount as Amount1,
rtr_quotn_mtrc_INSERT.Rate as Rate1,
rtr_quotn_mtrc_INSERT.Quotn_Mtrc_Type as Quotn_Mtrc_Type1,
rtr_quotn_mtrc_INSERT.PRCS_ID as PRCS_ID1,
rtr_quotn_mtrc_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_quotn_mtrc_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_quotn_mtrc_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_quotn_mtrc_INSERT.source_record_id
FROM
rtr_quotn_mtrc_INSERT
);


-- Component upd_quotn_mtrc_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_mtrc_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_mtrc_UPDATE.lkp_QUOTN_ID as lkp_QUOTN_ID3,
rtr_quotn_mtrc_UPDATE.lkp_QUOTN_MTRC_TYPE_CD as lkp_QUOTN_MTRC_TYPE_CD3,
rtr_quotn_mtrc_UPDATE.lkp_QUOTN_MTRC_STRT_DTTM as lkp_QUOTN_MTRC_STRT_DTTM3,
rtr_quotn_mtrc_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_quotn_mtrc_UPDATE.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_quotn_mtrc_UPDATE.lkp_QUOTN_MTRC_CNT as lkp_QUOTN_MTRC_CNT3,
1 as UPDATE_STRATEGY_ACTION,
rtr_quotn_mtrc_UPDATE.source_record_id
FROM
rtr_quotn_mtrc_UPDATE
);


-- Component exp_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert AS
(
SELECT
upd_quotn_mtrc_insert.Quotn_ID1 as Quotn_ID1,
upd_quotn_mtrc_insert.AutoLatePayCount1 as AutoLatePayCount1,
upd_quotn_mtrc_insert.EditEffectiveDate1 as EditEffectiveDate1,
upd_quotn_mtrc_insert.Quotn_Mtrc_Type1 as Quotn_Mtrc_Type1,
upd_quotn_mtrc_insert.PRCS_ID1 as PRCS_ID1,
upd_quotn_mtrc_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_quotn_mtrc_insert.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_quotn_mtrc_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_quotn_mtrc_insert.source_record_id
FROM
upd_quotn_mtrc_insert
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
upd_quotn_mtrc_ins_upd.Quotn_ID1 as Quotn_ID1,
upd_quotn_mtrc_ins_upd.AutoLatePayCount1 as AutoLatePayCount1,
upd_quotn_mtrc_ins_upd.EditEffectiveDate1 as EditEffectiveDate1,
upd_quotn_mtrc_ins_upd.Quotn_Mtrc_Type1 as Quotn_Mtrc_Type1,
upd_quotn_mtrc_ins_upd.PRCS_ID1 as PRCS_ID1,
upd_quotn_mtrc_ins_upd.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_quotn_mtrc_ins_upd.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_quotn_mtrc_ins_upd.source_record_id
FROM
upd_quotn_mtrc_ins_upd
);


-- Component tgt_QUOTN_MTRC_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_MTRC
(
QUOTN_ID,
QUOTN_MTRC_TYPE_CD,
QUOTN_MTRC_STRT_DTTM,
QUOTN_MTRC_END_DTTM,
QUOTN_MTRC_CNT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_insert.Quotn_ID1 as QUOTN_ID,
exp_insert.Quotn_Mtrc_Type1 as QUOTN_MTRC_TYPE_CD,
exp_insert.EditEffectiveDate1 as QUOTN_MTRC_STRT_DTTM,
exp_insert.EDW_END_DTTM1 as QUOTN_MTRC_END_DTTM,
exp_insert.AutoLatePayCount1 as QUOTN_MTRC_CNT,
exp_insert.PRCS_ID1 as PRCS_ID,
exp_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_insert.EDW_END_DTTM1 as EDW_END_DTTM,
exp_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM
FROM
exp_insert;


-- Component tgt_QUOTN_MTRC_ins_upd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_MTRC
(
QUOTN_ID,
QUOTN_MTRC_TYPE_CD,
QUOTN_MTRC_STRT_DTTM,
QUOTN_MTRC_CNT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_ins_upd.Quotn_ID1 as QUOTN_ID,
exp_ins_upd.Quotn_Mtrc_Type1 as QUOTN_MTRC_TYPE_CD,
exp_ins_upd.EditEffectiveDate1 as QUOTN_MTRC_STRT_DTTM,
exp_ins_upd.AutoLatePayCount1 as QUOTN_MTRC_CNT,
exp_ins_upd.PRCS_ID1 as PRCS_ID,
exp_ins_upd.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM1 as EDW_END_DTTM,
exp_ins_upd.EDW_STRT_DTTM1 as TRANS_STRT_DTTM
FROM
exp_ins_upd;


-- Component exp_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_update AS
(
SELECT
upd_quotn_mtrc_update.lkp_QUOTN_ID3 as lkp_QUOTN_ID3,
upd_quotn_mtrc_update.lkp_QUOTN_MTRC_TYPE_CD3 as lkp_QUOTN_MTRC_TYPE_CD3,
upd_quotn_mtrc_update.lkp_QUOTN_MTRC_STRT_DTTM3 as lkp_QUOTN_MTRC_STRT_DTTM3,
upd_quotn_mtrc_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as EDW_END_DTTM1,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as TRANS_END_DTTM,
upd_quotn_mtrc_update.source_record_id
FROM
upd_quotn_mtrc_update
);


-- Component tgt_QUOTN_MTRC_update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_MTRC
USING exp_update ON (QUOTN_MTRC.QUOTN_ID = exp_update.lkp_QUOTN_ID3 AND QUOTN_MTRC.QUOTN_MTRC_TYPE_CD = exp_update.lkp_QUOTN_MTRC_TYPE_CD3 AND QUOTN_MTRC.QUOTN_MTRC_STRT_DTTM = exp_update.lkp_QUOTN_MTRC_STRT_DTTM3 AND QUOTN_MTRC.EDW_STRT_DTTM = exp_update.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_update.lkp_QUOTN_ID3,
QUOTN_MTRC_TYPE_CD = exp_update.lkp_QUOTN_MTRC_TYPE_CD3,
QUOTN_MTRC_STRT_DTTM = exp_update.lkp_QUOTN_MTRC_STRT_DTTM3,
EDW_STRT_DTTM = exp_update.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_update.EDW_END_DTTM1,
TRANS_END_DTTM = exp_update.TRANS_END_DTTM;


END; ';