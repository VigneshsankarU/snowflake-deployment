-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PREFILLIDCHK_ALFA_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_pcx_prefillidcheck_alfa, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pcx_prefillidcheck_alfa AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LoadCommandID,
$2 as PublicID,
$3 as HRI5Description,
$4 as HRI3Description,
$5 as CreateTime,
$6 as HRI1Description,
$7 as UpdateTime,
$8 as ID,
$9 as HRI5Score,
$10 as HRI3Score,
$11 as HRI1Score,
$12 as CreateUserID,
$13 as HRI6Description,
$14 as CVIDescription,
$15 as HRI4Description,
$16 as BeanVersion,
$17 as HRI2Description,
$18 as Retired,
$19 as UpdateUserID,
$20 as Subtype,
$21 as HRI6Score,
$22 as CVIScore,
$23 as HRI4Score,
$24 as HRI2Score,
$25 as CTL_ID,
$26 as LOAD_USER,
$27 as LOAD_DTTM,
$28 as TYPECODE,
$29 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT pcx_prefillidcheck_alfa.LoadCommandID_stg,

 pcx_prefillidcheck_alfa.PublicID_stg, 

 pcx_prefillidcheck_alfa.HRI5Description_stg,

  pcx_prefillidcheck_alfa.HRI3Description_stg, 

  pcx_prefillidcheck_alfa.CreateTime_stg, 

pcx_prefillidcheck_alfa.HRI1Description_stg,

 pcx_prefillidcheck_alfa.UpdateTime_stg,

  pcx_prefillidcheck_alfa.ID_stg,

   pcx_prefillidcheck_alfa.HRI5Score_stg, 

   pcx_prefillidcheck_alfa.HRI3Score_stg,

    pcx_prefillidcheck_alfa.HRI1Score_stg, 

pcx_prefillidcheck_alfa.CreateUserID_stg, 

pcx_prefillidcheck_alfa.HRI6Description_stg, 

pcx_prefillidcheck_alfa.CVIDescription_stg,

 pcx_prefillidcheck_alfa.HRI4Description_stg,

  pcx_prefillidcheck_alfa.BeanVersion_stg, 

  pcx_prefillidcheck_alfa.HRI2Description_stg,

 pcx_prefillidcheck_alfa.Retired_stg, 

 pcx_prefillidcheck_alfa.UpdateUserID_stg, 

 pcx_prefillidcheck_alfa.Subtype_stg, 

 pcx_prefillidcheck_alfa.HRI6Score_stg,

  pcx_prefillidcheck_alfa.CVIScore_stg, 

  pcx_prefillidcheck_alfa.HRI4Score_stg, 

  pcx_prefillidcheck_alfa.HRI2Score_stg, 

1 as CTL_ID_stg, 

  null as LOAD_USER_stg, 

cast(  null as timestamp)as LOAD_DTTM_stg,

   pctl_prefillidcheck_alfa.TYPECODE_stg

FROM

 db_t_prod_stag.pcx_prefillidcheck_alfa pcx_prefillidcheck_alfa left join db_t_prod_stag.pctl_prefillidcheck_alfa pctl_prefillidcheck_alfa

 on pcx_prefillidcheck_alfa.Subtype_stg = pctl_prefillidcheck_alfa.ID_stg

 where pcx_prefillidcheck_alfa.UpdateTime_stg > (:start_dttm) AND pcx_prefillidcheck_alfa.UpdateTime_stg  <= (:end_dttm)
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_pcx_prefillidcheck_alfa.LoadCommandID as LoadCommandID,
SQ_pcx_prefillidcheck_alfa.PublicID as PublicID,
SQ_pcx_prefillidcheck_alfa.HRI5Description as HRI5Description,
SQ_pcx_prefillidcheck_alfa.HRI3Description as HRI3Description,
SQ_pcx_prefillidcheck_alfa.CreateTime as CreateTime,
SQ_pcx_prefillidcheck_alfa.HRI1Description as HRI1Description,
SQ_pcx_prefillidcheck_alfa.UpdateTime as UpdateTime,
SQ_pcx_prefillidcheck_alfa.ID as ID,
SQ_pcx_prefillidcheck_alfa.HRI5Score as HRI5Score,
SQ_pcx_prefillidcheck_alfa.HRI3Score as HRI3Score,
SQ_pcx_prefillidcheck_alfa.HRI1Score as HRI1Score,
SQ_pcx_prefillidcheck_alfa.CreateUserID as CreateUserID,
SQ_pcx_prefillidcheck_alfa.HRI6Description as HRI6Description,
SQ_pcx_prefillidcheck_alfa.CVIDescription as CVIDescription,
SQ_pcx_prefillidcheck_alfa.HRI4Description as HRI4Description,
SQ_pcx_prefillidcheck_alfa.BeanVersion as BeanVersion,
SQ_pcx_prefillidcheck_alfa.HRI2Description as HRI2Description,
SQ_pcx_prefillidcheck_alfa.Retired as Retired,
SQ_pcx_prefillidcheck_alfa.UpdateUserID as UpdateUserID,
SQ_pcx_prefillidcheck_alfa.Subtype as Subtype,
SQ_pcx_prefillidcheck_alfa.HRI6Score as HRI6Score,
SQ_pcx_prefillidcheck_alfa.CVIScore as CVIScore,
SQ_pcx_prefillidcheck_alfa.HRI4Score as HRI4Score,
SQ_pcx_prefillidcheck_alfa.HRI2Score as HRI2Score,
SQ_pcx_prefillidcheck_alfa.CTL_ID as CTL_ID,
:PRCS_ID as PROCESS_ID,
SQ_pcx_prefillidcheck_alfa.LOAD_USER as LOAD_USER,
SQ_pcx_prefillidcheck_alfa.LOAD_DTTM as LOAD_DTTM,
SQ_pcx_prefillidcheck_alfa.TYPECODE as TYPECODE,
SQ_pcx_prefillidcheck_alfa.source_record_id
FROM
SQ_pcx_prefillidcheck_alfa
);


-- Component LKP_TERADATA_ETL_REF_XLAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PREFILL_IDCHK_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_prefillidcheck_alfa.typecode'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_through.TYPECODE
QUALIFY RNK = 1
);


-- Component exp_all_sources, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sources AS
(
SELECT
exp_pass_through.LoadCommandID as LoadCommandID,
exp_pass_through.PublicID as PublicID,
exp_pass_through.HRI5Description as HRI5Description,
exp_pass_through.HRI3Description as HRI3Description,
exp_pass_through.CreateTime as CreateTime,
exp_pass_through.HRI1Description as HRI1Description,
exp_pass_through.UpdateTime as UpdateTime,
exp_pass_through.ID as ID,
exp_pass_through.HRI5Score as HRI5Score,
exp_pass_through.HRI3Score as HRI3Score,
exp_pass_through.HRI1Score as HRI1Score,
exp_pass_through.CreateUserID as CreateUserID,
exp_pass_through.HRI6Description as HRI6Description,
exp_pass_through.CVIDescription as CVIDescription,
exp_pass_through.HRI4Description as HRI4Description,
exp_pass_through.BeanVersion as BeanVersion,
exp_pass_through.HRI2Description as HRI2Description,
LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as lkp_TYPECODE,
exp_pass_through.Retired as Retired,
exp_pass_through.UpdateUserID as UpdateUserID,
exp_pass_through.Subtype as Subtype,
exp_pass_through.HRI6Score as HRI6Score,
exp_pass_through.CVIScore as CVIScore,
exp_pass_through.HRI4Score as HRI4Score,
exp_pass_through.HRI2Score as HRI2Score,
exp_pass_through.CTL_ID as CTL_ID,
exp_pass_through.PROCESS_ID as PROCESS_ID,
exp_pass_through.LOAD_USER as LOAD_USER,
exp_pass_through.LOAD_DTTM as LOAD_DTTM,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON exp_pass_through.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
);


-- Component LKP_PREFILL_IDCHK, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PREFILL_IDCHK AS
(
SELECT
LKP.PREFILL_IDCHK_ID,
exp_all_sources.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sources.source_record_id ORDER BY LKP.PREFILL_IDCHK_ID asc) RNK
FROM
exp_all_sources
LEFT JOIN (
SELECT PREFILL_IDCHK.PREFILL_IDCHK_ID as PREFILL_IDCHK_ID FROM db_t_prod_core.PREFILL_IDCHK
) LKP ON LKP.PREFILL_IDCHK_ID = exp_all_sources.ID
QUALIFY RNK = 1
);


-- Component exp_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins AS
(
SELECT
exp_all_sources.LoadCommandID as in_LoadCommandID,
exp_all_sources.PublicID as in_PublicID,
exp_all_sources.HRI5Description as in_HRI5Description,
exp_all_sources.HRI3Description as in_HRI3Description,
exp_all_sources.CreateTime as in_CreateTime,
exp_all_sources.HRI1Description as in_HRI1Description,
exp_all_sources.UpdateTime as in_UpdateTime,
exp_all_sources.ID as in_ID,
exp_all_sources.HRI5Score as in_HRI5Score,
exp_all_sources.HRI3Score as in_HRI3Score,
exp_all_sources.HRI1Score as in_HRI1Score,
exp_all_sources.CreateUserID as in_CreateUserID,
exp_all_sources.HRI6Description as in_HRI6Description,
exp_all_sources.CVIDescription as in_CVIDescription,
exp_all_sources.HRI4Description as in_HRI4Description,
exp_all_sources.BeanVersion as in_BeanVersion,
exp_all_sources.HRI2Description as in_HRI2Description,
exp_all_sources.lkp_TYPECODE as lkp_TYPECODE,
exp_all_sources.Retired as in_Retired,
exp_all_sources.UpdateUserID as in_UpdateUserID,
exp_all_sources.Subtype as in_Subtype,
exp_all_sources.HRI6Score as in_HRI6Score,
exp_all_sources.CVIScore as in_CVIScore,
exp_all_sources.HRI4Score as in_HRI4Score,
exp_all_sources.HRI2Score as in_HRI2Score,
exp_all_sources.CTL_ID as in_CTL_ID,
exp_all_sources.PROCESS_ID as in_PROCESS_ID,
exp_all_sources.LOAD_USER as in_LOAD_USER,
exp_all_sources.LOAD_DTTM as in_LOAD_DTTM,
CASE WHEN LKP_PREFILL_IDCHK.PREFILL_IDCHK_ID IS NULL THEN ''I'' ELSE ''R'' END as flag,
exp_all_sources.source_record_id
FROM
exp_all_sources
INNER JOIN LKP_PREFILL_IDCHK ON exp_all_sources.source_record_id = LKP_PREFILL_IDCHK.source_record_id
);


-- Component fil_prefill_idchk, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prefill_idchk AS
(
SELECT
exp_ins.in_LoadCommandID as in_LoadCommandID,
exp_ins.in_PublicID as in_PublicID,
exp_ins.in_HRI5Description as in_HRI5Description,
exp_ins.in_HRI3Description as in_HRI3Description,
exp_ins.in_CreateTime as in_CreateTime,
exp_ins.in_HRI1Description as in_HRI1Description,
exp_ins.in_UpdateTime as in_UpdateTime,
exp_ins.in_ID as in_ID,
exp_ins.in_HRI5Score as in_HRI5Score,
exp_ins.in_HRI3Score as in_HRI3Score,
exp_ins.in_HRI1Score as in_HRI1Score,
exp_ins.in_CreateUserID as in_CreateUserID,
exp_ins.in_HRI6Description as in_HRI6Description,
exp_ins.in_CVIDescription as in_CVIDescription,
exp_ins.in_HRI4Description as in_HRI4Description,
exp_ins.in_BeanVersion as in_BeanVersion,
exp_ins.in_HRI2Description as in_HRI2Description,
exp_ins.lkp_TYPECODE as lkp_TYPECODE,
exp_ins.in_Retired as in_Retired,
exp_ins.in_UpdateUserID as in_UpdateUserID,
exp_ins.in_Subtype as in_Subtype,
exp_ins.in_HRI6Score as in_HRI6Score,
exp_ins.in_CVIScore as in_CVIScore,
exp_ins.in_HRI4Score as in_HRI4Score,
exp_ins.in_HRI2Score as in_HRI2Score,
exp_ins.in_CTL_ID as in_CTL_ID,
exp_ins.in_PROCESS_ID as in_PROCESS_ID,
exp_ins.in_LOAD_USER as in_LOAD_USER,
exp_ins.in_LOAD_DTTM as in_LOAD_DTTM,
exp_ins.flag as flag,
exp_ins.source_record_id
FROM
exp_ins
WHERE exp_ins.in_ID IS NOT NULL AND exp_ins.flag = ''I''
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
fil_prefill_idchk.in_PublicID as in_PublicID,
fil_prefill_idchk.in_HRI5Description as in_HRI5Description,
fil_prefill_idchk.in_HRI3Description as in_HRI3Description,
fil_prefill_idchk.in_HRI1Description as in_HRI1Description,
fil_prefill_idchk.in_ID as in_ID,
fil_prefill_idchk.in_HRI5Score as in_HRI5Score,
fil_prefill_idchk.in_HRI3Score as in_HRI3Score,
fil_prefill_idchk.in_HRI1Score as in_HRI1Score,
fil_prefill_idchk.in_HRI6Description as in_HRI6Description,
fil_prefill_idchk.in_CVIDescription as in_CVIDescription,
fil_prefill_idchk.in_HRI4Description as in_HRI4Description,
fil_prefill_idchk.in_HRI2Description as in_HRI2Description,
fil_prefill_idchk.lkp_TYPECODE as lkp_TYPECODE,
fil_prefill_idchk.in_HRI6Score as in_HRI6Score,
fil_prefill_idchk.in_CVIScore as in_CVIScore,
fil_prefill_idchk.in_HRI4Score as in_HRI4Score,
fil_prefill_idchk.in_HRI2Score as in_HRI2Score,
fil_prefill_idchk.in_PROCESS_ID as in_PROCESS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
fil_prefill_idchk.in_UpdateTime as TRANS_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
fil_prefill_idchk.source_record_id
FROM
fil_prefill_idchk
);


-- Component PREFILL_IDCHK, Type TARGET 
INSERT INTO db_t_prod_core.PREFILL_IDCHK
(
PREFILL_IDCHK_ID,
HRI5_DESC,
HRI3_DESC,
HRI1_DESC,
HRI5_SCR_VAL,
HRI3_SCR_VAL,
HRI1_SCR_VAL,
NK_SRC_KEY,
HRI6_DESC,
CVI_DESC,
HRI4_DESC,
HRI2_DESC,
PREFILL_IDCHK_TYPE_CD,
HRI6_SCR_VAL,
CVI_SCR_VAL,
HRI4_SCR_VAL,
HRI2_SCR_VAL,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt.in_ID as PREFILL_IDCHK_ID,
exp_pass_to_tgt.in_HRI5Description as HRI5_DESC,
exp_pass_to_tgt.in_HRI3Description as HRI3_DESC,
exp_pass_to_tgt.in_HRI1Description as HRI1_DESC,
exp_pass_to_tgt.in_HRI5Score as HRI5_SCR_VAL,
exp_pass_to_tgt.in_HRI3Score as HRI3_SCR_VAL,
exp_pass_to_tgt.in_HRI1Score as HRI1_SCR_VAL,
exp_pass_to_tgt.in_PublicID as NK_SRC_KEY,
exp_pass_to_tgt.in_HRI6Description as HRI6_DESC,
exp_pass_to_tgt.in_CVIDescription as CVI_DESC,
exp_pass_to_tgt.in_HRI4Description as HRI4_DESC,
exp_pass_to_tgt.in_HRI2Description as HRI2_DESC,
exp_pass_to_tgt.lkp_TYPECODE as PREFILL_IDCHK_TYPE_CD,
exp_pass_to_tgt.in_HRI6Score as HRI6_SCR_VAL,
exp_pass_to_tgt.in_CVIScore as CVI_SCR_VAL,
exp_pass_to_tgt.in_HRI4Score as HRI4_SCR_VAL,
exp_pass_to_tgt.in_HRI2Score as HRI2_SCR_VAL,
exp_pass_to_tgt.in_PROCESS_ID as PRCS_ID,
exp_pass_to_tgt.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_tgt.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt;


END; ';