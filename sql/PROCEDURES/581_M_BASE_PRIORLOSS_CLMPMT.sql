-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRIORLOSS_CLMPMT("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_priorloss_clmpmt, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_priorloss_clmpmt AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as clm_pmt_id,
$2 as prior_loss_id,
$3 as claimtype,
$4 as claimdisposition,
$5 as claimamount,
$6 as UpdateTime,
$7 as CreateTime,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

pcx_claimpaymentext.id_stg as Clm_pmt_ID,

pcx_priorlossext.id_stg as Prior_loss_ID,

pcx_claimpaymentext.ClaimType_stg as ClaimType,

pcx_claimpaymentext.ClaimDisposition_stg as ClaimDisposition,

pcx_claimpaymentext.ClaimAmount_stg as ClaimAmount,

pcx_priorlossext.CreateTime_stg as UpdateTime,

pcx_priorlossext.UpdateTime_stg as CreateTime

from db_t_prod_stag.pcx_priorlossext

join db_t_prod_stag.pcx_claimpaymentext on pcx_claimpaymentext.PriorLossExtID_stg = pcx_priorlossext.id_stg

where pcx_priorlossext.UpdateTime_stg > (:start_dttm)

and pcx_priorlossext.UpdateTime_stg <= (:end_dttm)
) SRC
)
);


-- Component LKP_XLAT_CLM_PMT_STS_TYP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_XLAT_CLM_PMT_STS_TYP AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
SQ_priorloss_clmpmt.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_priorloss_clmpmt.source_record_id ORDER BY LKP.SRC_IDNTFTN_SYS asc,LKP.SRC_IDNTFTN_NM asc,LKP.SRC_IDNTFTN_VAL asc,LKP.TGT_IDNTFTN_NM asc,LKP.TGT_IDNTFTN_VAL asc,LKP.EXPN_DT asc,LKP.EFF_DT asc) RNK
FROM
SQ_priorloss_clmpmt
LEFT JOIN (
select *
FROM 
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_PMT_STS_TYPE'' 
         AND lower(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM) =  ''pcx_claimpaymentext.claimdisposition''
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS = ''GW''
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = SQ_priorloss_clmpmt.claimdisposition
QUALIFY RNK = 1
);


-- Component LKP_XLAT_INSRNC_CVGE_TYP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_XLAT_INSRNC_CVGE_TYP AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
SQ_priorloss_clmpmt.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_priorloss_clmpmt.source_record_id ORDER BY LKP.SRC_IDNTFTN_SYS asc,LKP.SRC_IDNTFTN_NM asc,LKP.SRC_IDNTFTN_VAL asc,LKP.TGT_IDNTFTN_NM asc,LKP.TGT_IDNTFTN_VAL asc,LKP.EXPN_DT asc,LKP.EFF_DT asc) RNK
FROM
SQ_priorloss_clmpmt
LEFT JOIN (
select * 
FROM 
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_CVGE_TYPE'' 
         AND lower(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM) in ( ''cctl_coveragetype.typecode'',''derived'',''pc_etlclausepattern.coveragesubtype'', ''pc_etlclausepattern.name'', ''pc_etlcovtermpattern.patternid'', ''pcx_claimpaymentext.claimtype'')
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'')
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = SQ_priorloss_clmpmt.claimtype
QUALIFY RNK = 1
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_priorloss_clmpmt.clm_pmt_id as clm_pmt_id,
SQ_priorloss_clmpmt.prior_loss_id as prior_loss_id,
LKP_XLAT_CLM_PMT_STS_TYP.TGT_IDNTFTN_VAL as claimdisposition,
LKP_XLAT_INSRNC_CVGE_TYP.TGT_IDNTFTN_VAL as claimtype,
IFNULL(TRY_TO_DECIMAL(SQ_priorloss_clmpmt.claimamount), 0) as claimamount_out,
CURRENT_TIMESTAMP as START_DTTM,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as END_DTTM,
SQ_priorloss_clmpmt.UpdateTime as UpdateTime,
SQ_priorloss_clmpmt.CreateTime as CreateTime,
:PRCS_ID as PRCS_ID,
SQ_priorloss_clmpmt.source_record_id
FROM
SQ_priorloss_clmpmt
INNER JOIN LKP_XLAT_CLM_PMT_STS_TYP ON SQ_priorloss_clmpmt.source_record_id = LKP_XLAT_CLM_PMT_STS_TYP.source_record_id
INNER JOIN LKP_XLAT_INSRNC_CVGE_TYP ON LKP_XLAT_CLM_PMT_STS_TYP.source_record_id = LKP_XLAT_INSRNC_CVGE_TYP.source_record_id
);


-- Component LKP_PRIORLOSS_CLMPMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRIORLOSS_CLMPMT AS
(
SELECT
LKP.PRIOR_LOSS_CLM_PMT_ID,
EXPTRANS.clm_pmt_id as clm_pmt_id,
EXPTRANS.source_record_id,
ROW_NUMBER() OVER(PARTITION BY EXPTRANS.source_record_id ORDER BY LKP.PRIOR_LOSS_CLM_PMT_ID asc) RNK
FROM
EXPTRANS
LEFT JOIN (
SELECT
PRIOR_LOSS_CLM_PMT_ID
FROM db_t_prod_core.PRIOR_LOSS_CLM_PMT
) LKP ON LKP.PRIOR_LOSS_CLM_PMT_ID = EXPTRANS.clm_pmt_id
QUALIFY RNK = 1
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
EXPTRANS.clm_pmt_id as in_clm_pmt_id,
EXPTRANS.prior_loss_id as in_prior_loss_id,
EXPTRANS.claimdisposition as in_claimdisposition,
EXPTRANS.claimtype as in_claimtype,
EXPTRANS.claimamount_out as in_claimamount,
EXPTRANS.START_DTTM as in_START_DTTM,
EXPTRANS.END_DTTM as in_END_DTTM,
EXPTRANS.UpdateTime as in_UpdateTime,
EXPTRANS.CreateTime as CreateTime,
CASE WHEN LKP_PRIORLOSS_CLMPMT.PRIOR_LOSS_CLM_PMT_ID IS NULL THEN ''I'' ELSE ''R'' END as calc_ins_upd,
''GWPC'' as src_sys_cd,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.source_record_id
FROM
EXPTRANS
INNER JOIN LKP_PRIORLOSS_CLMPMT ON EXPTRANS.source_record_id = LKP_PRIORLOSS_CLMPMT.source_record_id
);


-- Component fil, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil AS
(
SELECT
EXPTRANS1.in_clm_pmt_id as in_clm_pmt_id,
EXPTRANS1.in_prior_loss_id as in_prior_loss_id,
EXPTRANS1.in_claimdisposition as in_claimdisposition,
EXPTRANS1.in_claimtype as in_claimtype,
EXPTRANS1.in_claimamount as in_claimamount,
EXPTRANS1.in_START_DTTM as in_START_DTTM,
EXPTRANS1.in_END_DTTM as in_END_DTTM,
EXPTRANS1.in_UpdateTime as in_UpdateTime,
EXPTRANS1.CreateTime as CreateTime,
EXPTRANS1.calc_ins_upd as calc_ins_upd,
EXPTRANS1.src_sys_cd as src_sys_cd,
EXPTRANS1.PRCS_ID as PRCS_ID,
EXPTRANS1.source_record_id
FROM
EXPTRANS1
WHERE EXPTRANS1.calc_ins_upd = ''I''
);


-- Component PRIOR_LOSS_CLM_PMT, Type TARGET 
INSERT INTO db_t_prod_core.PRIOR_LOSS_CLM_PMT
(
PRIOR_LOSS_CLM_PMT_ID,
PRIOR_LOSS_SUMRY_ID,
CLM_PMT_STS_TYPE_CD,
INSRNC_CVGE_TYPE_CD,
CLM_PMT_AMT,
SRC_SYS_CD,
CRTD_DTTM,
UPDT_DTTM,
PRCS_ID
)
SELECT
fil.in_clm_pmt_id as PRIOR_LOSS_CLM_PMT_ID,
fil.in_prior_loss_id as PRIOR_LOSS_SUMRY_ID,
fil.in_claimdisposition as CLM_PMT_STS_TYPE_CD,
fil.in_claimtype as INSRNC_CVGE_TYPE_CD,
fil.in_claimamount as CLM_PMT_AMT,
fil.src_sys_cd as SRC_SYS_CD,
fil.CreateTime as CRTD_DTTM,
fil.in_UpdateTime as UPDT_DTTM,
fil.PRCS_ID as PRCS_ID
FROM
fil;


END; ';