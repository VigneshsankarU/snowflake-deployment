-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PREFILLADDDATA_ALFA_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '

 DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
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



-- Component SQ_pcx_prefilladddata_alfa1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pcx_prefilladddata_alfa1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CarrierName,
$2 as PublicID,
$3 as CoverageLapse,
$4 as LastCancellationDate,
$5 as RiskType,
$6 as PriorPolicyIncDate,
$7 as StateIndicator,
$8 as UpdateTime,
$9 as Occurances,
$10 as ID,
$11 as LastCancellationReason,
$12 as PriorPolicyInd,
$13 as ExisitngCustomerRelationInd,
$14 as RelationshipCode,
$15 as Status,
$16 as PolicyNumber,
$17 as PolicyLapse,
$18 as PriorNumberOfPolicies,
$19 as pctl_typecode,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT

        SRC.CarrierName_stg,

		SRC.PublicID_stg, 

		CASE WHEN SRC.CoverageLapse_stg=1 THEN ''Y'' WHEN SRC.CoverageLapse_stg=0 THEN ''N'' END as CoverageLapse_stg,

		SRC.LastCancellationDate_stg, 

		SRC.RiskType_stg,

		SRC.PriorPolicyIncDate_stg,

		pctl_state.typecode_stg as StateIndicator, 

		SRC.UpdateTime_stg,

		SRC.Occurances_stg,

		 SRC.ID_stg,

		SRC.LastCancellationReason_stg,

		CASE WHEN SRC.PriorPolicyInd_stg=1 THEN ''Y'' WHEN SRC.PriorPolicyInd_stg=0 THEN ''N'' END as PriorPolicyInd_stg,

		 SRC.ExisitngCustomerRelationInd_stg,

		SRC.RelationshipCode_stg,

		SRC.Status_stg, 

		SRC.PolicyNumber_stg,

		CASE WHEN SRC.PolicyLapse_stg=1 THEN ''Y'' WHEN SRC.PolicyLapse_stg=0 THEN ''N''  END as PolicyLapse_stg,

		SRC.PriorNumberOfPolicies_stg,

		ppa.typecode_stg 

FROM	  

/* pcx_prefilladddata_alfa */


(

SELECT 

pcx_prefilladddata_alfa.CarrierName_stg, 

pcx_prefilladddata_alfa.PublicID_stg, 

pcx_prefilladddata_alfa.CoverageLapse_stg,

pcx_prefilladddata_alfa.LastCancellationDate_stg, 

pcx_prefilladddata_alfa.RiskType_stg, 

pcx_prefilladddata_alfa.PriorPolicyIncDate_stg, 

pcx_prefilladddata_alfa.State_stg, 

pcx_prefilladddata_alfa.UpdateTime_stg, 

pcx_prefilladddata_alfa.Occurances_stg, 

pcx_prefilladddata_alfa.ID_stg, 

pcx_prefilladddata_alfa.LastCancellationReason_stg, 

pcx_prefilladddata_alfa.PriorPolicyInd_stg, 

pcx_prefilladddata_alfa.ExisitngCustomerRelationInd_stg, 

pcx_prefilladddata_alfa.RelationshipCode_stg, 

pcx_prefilladddata_alfa.Status_stg, 

pcx_prefilladddata_alfa.Subtype_stg, 

pcx_prefilladddata_alfa.PolicyNumber_stg, 

pcx_prefilladddata_alfa.PolicyLapse_stg, 

pcx_prefilladddata_alfa.PriorNumberOfPolicies_stg 



FROM

 db_t_prod_stag.pcx_prefilladddata_alfa

where pcx_prefilladddata_alfa.UpdateTime_stg > (:start_dttm) AND pcx_prefilladddata_alfa.UpdateTime_stg  <= (:end_dttm)

)SRC

LEFT JOIN  db_t_prod_stag.pctl_state pctl_state on pctl_state.id_stg = SRC.State_stg

LEFT JOIN db_t_prod_stag.pctl_prefilladddata_alfa ppa on SRC.Subtype_stg=ppa.id_stg
) SRC
)
);


-- Component exp_prefill_adddata_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prefill_adddata_transformation AS
(
SELECT
SQ_pcx_prefilladddata_alfa1.CarrierName as CarrierName,
SQ_pcx_prefilladddata_alfa1.PublicID as PublicID,
SQ_pcx_prefilladddata_alfa1.CoverageLapse as CoverageLapse,
SQ_pcx_prefilladddata_alfa1.LastCancellationDate as LastCancellationDate,
SQ_pcx_prefilladddata_alfa1.RiskType as RiskType,
SQ_pcx_prefilladddata_alfa1.PriorPolicyIncDate as PriorPolicyIncDate,
SQ_pcx_prefilladddata_alfa1.StateIndicator as StateIndicator,
SQ_pcx_prefilladddata_alfa1.UpdateTime as UpdateTime,
SQ_pcx_prefilladddata_alfa1.Occurances as Occurances,
SQ_pcx_prefilladddata_alfa1.ID as ID,
SQ_pcx_prefilladddata_alfa1.LastCancellationReason as LastCancellationReason,
SQ_pcx_prefilladddata_alfa1.PriorPolicyInd as PriorPolicyInd,
SQ_pcx_prefilladddata_alfa1.ExisitngCustomerRelationInd as ExisitngCustomerRelationInd,
SQ_pcx_prefilladddata_alfa1.RelationshipCode as RelationshipCode,
SQ_pcx_prefilladddata_alfa1.Status as Status,
SQ_pcx_prefilladddata_alfa1.PolicyNumber as PolicyNumber,
SQ_pcx_prefilladddata_alfa1.PolicyLapse as PolicyLapse,
SQ_pcx_prefilladddata_alfa1.PriorNumberOfPolicies as PriorNumberOfPolicies,
SQ_pcx_prefilladddata_alfa1.pctl_typecode as pctl_typecode,
SQ_pcx_prefilladddata_alfa1.source_record_id
FROM
SQ_pcx_prefilladddata_alfa1
);


-- Component LKP_PREFILL_ADDDATA_LKP_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PREFILL_ADDDATA_LKP_TGT AS
(
SELECT
LKP.PREFILL_ADDLDATA_ID,
exp_prefill_adddata_transformation.ID as ID,
exp_prefill_adddata_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_prefill_adddata_transformation.source_record_id ORDER BY LKP.PREFILL_ADDLDATA_ID asc) RNK
FROM
exp_prefill_adddata_transformation
LEFT JOIN (
SELECT
PREFILL_ADDLDATA_ID
FROM db_t_prod_core.PREFILL_ADDLDATA
) LKP ON LKP.PREFILL_ADDLDATA_ID = exp_prefill_adddata_transformation.ID
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PREFILL_ADDDATA, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PREFILL_ADDDATA AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_prefill_adddata_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_prefill_adddata_transformation.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_prefill_adddata_transformation
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PREFILL_ADDLDATA_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_prefilladddata_alfa.typecode'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_prefill_adddata_transformation.pctl_typecode
QUALIFY RNK = 1
);


-- Component exp_prefill_adddata_lkp_typecode, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prefill_adddata_lkp_typecode AS
(
SELECT
exp_prefill_adddata_transformation.CarrierName as in_CarrierName,
exp_prefill_adddata_transformation.PublicID as in_PublicID,
exp_prefill_adddata_transformation.CoverageLapse as in_CoverageLapse,
exp_prefill_adddata_transformation.LastCancellationDate as in_LastCancellationDate,
exp_prefill_adddata_transformation.RiskType as in_RiskType,
exp_prefill_adddata_transformation.PriorPolicyIncDate as in_PriorPolicyIncDate,
exp_prefill_adddata_transformation.StateIndicator as in_StateIndicator,
exp_prefill_adddata_transformation.Occurances as in_Occurances,
exp_prefill_adddata_transformation.ID as in_ID,
exp_prefill_adddata_transformation.LastCancellationReason as in_LastCancellationReason,
exp_prefill_adddata_transformation.PriorPolicyInd as in_PriorPolicyInd,
exp_prefill_adddata_transformation.ExisitngCustomerRelationInd as in_ExisitngCustomerRelationInd,
exp_prefill_adddata_transformation.RelationshipCode as in_RelationshipCode,
exp_prefill_adddata_transformation.Status as in_Status,
exp_prefill_adddata_transformation.PolicyNumber as in_PolicyNumber,
exp_prefill_adddata_transformation.PolicyLapse as in_PolicyLapse,
exp_prefill_adddata_transformation.PriorNumberOfPolicies as in_PriorNumberOfPolicies,
:PRCS_ID as PRCS_ID,
LKP_TERADATA_ETL_REF_XLAT_PREFILL_ADDDATA.TGT_IDNTFTN_VAL as out_prefill_adddata_typecode,
CASE WHEN LKP_PREFILL_ADDDATA_LKP_TGT.PREFILL_ADDLDATA_ID IS NULL THEN ''I'' ELSE ''R'' END as cdc_chk,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
exp_prefill_adddata_transformation.UpdateTime as out_TRANS_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
exp_prefill_adddata_transformation.source_record_id
FROM
exp_prefill_adddata_transformation
INNER JOIN LKP_PREFILL_ADDDATA_LKP_TGT ON exp_prefill_adddata_transformation.source_record_id = LKP_PREFILL_ADDDATA_LKP_TGT.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_PREFILL_ADDDATA ON LKP_PREFILL_ADDDATA_LKP_TGT.source_record_id = LKP_TERADATA_ETL_REF_XLAT_PREFILL_ADDDATA.source_record_id
);


-- Component flt_prefill_adddata_cdc_check, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_prefill_adddata_cdc_check AS
(
SELECT
exp_prefill_adddata_lkp_typecode.in_CarrierName as CarrierName,
exp_prefill_adddata_lkp_typecode.in_PublicID as PublicID,
exp_prefill_adddata_lkp_typecode.in_CoverageLapse as CoverageLapse,
exp_prefill_adddata_lkp_typecode.in_LastCancellationDate as LastCancellationDate,
exp_prefill_adddata_lkp_typecode.in_RiskType as RiskType,
exp_prefill_adddata_lkp_typecode.in_PriorPolicyIncDate as PriorPolicyIncDate,
exp_prefill_adddata_lkp_typecode.in_StateIndicator as StateIndicator,
exp_prefill_adddata_lkp_typecode.in_Occurances as Occurances,
exp_prefill_adddata_lkp_typecode.in_ID as ID,
exp_prefill_adddata_lkp_typecode.in_LastCancellationReason as LastCancellationReason,
exp_prefill_adddata_lkp_typecode.in_PriorPolicyInd as PriorPolicyInd,
exp_prefill_adddata_lkp_typecode.in_ExisitngCustomerRelationInd as ExisitngCustomerRelationInd,
exp_prefill_adddata_lkp_typecode.in_RelationshipCode as RelationshipCode,
exp_prefill_adddata_lkp_typecode.in_Status as Status,
exp_prefill_adddata_lkp_typecode.in_PolicyNumber as PolicyNumber,
exp_prefill_adddata_lkp_typecode.in_PolicyLapse as PolicyLapse,
exp_prefill_adddata_lkp_typecode.in_PriorNumberOfPolicies as PriorNumberOfPolicies,
exp_prefill_adddata_lkp_typecode.PRCS_ID as PRCS_ID,
exp_prefill_adddata_lkp_typecode.out_prefill_adddata_typecode as out_prefill_adddata_typecode,
exp_prefill_adddata_lkp_typecode.cdc_chk as cdc_chk,
exp_prefill_adddata_lkp_typecode.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_prefill_adddata_lkp_typecode.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_prefill_adddata_lkp_typecode.out_TRANS_STRT_DTTM as out_TRANS_STRT_DTTM,
exp_prefill_adddata_lkp_typecode.out_TRANS_END_DTTM as out_TRANS_END_DTTM,
exp_prefill_adddata_lkp_typecode.source_record_id
FROM
exp_prefill_adddata_lkp_typecode
WHERE exp_prefill_adddata_lkp_typecode.in_ID IS NOT NULL AND exp_prefill_adddata_lkp_typecode.cdc_chk = ''I''
);


-- Component PREFILL_ADDLDATA_ins, Type TARGET 
INSERT INTO db_t_prod_core.PREFILL_ADDLDATA
(
PREFILL_ADDLDATA_ID,
CARIER_NAME,
NK_SRC_KEY,
CVGE_LAPS_IND,
LAST_CNCLTN_DTTM,
RISK_TYPE_CD,
PRIOR_PLCY_INCPTN_DTTM,
PREFILL_ST_NAME,
OCCRNC_CD,
LAST_CNCLTN_RSN_CD,
PRIOR_PLCY_IND,
EXSTG_CUST_RLTNSHP_IND,
RLTNSHP_CD,
STS_CD,
PREFILL_ADDLDATA_TYPE_CD,
PRIOR_NUM_POLS_CNT,
PARNT_PLCY_NUM,
PLCY_LAPS_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
flt_prefill_adddata_cdc_check.ID as PREFILL_ADDLDATA_ID,
flt_prefill_adddata_cdc_check.CarrierName as CARIER_NAME,
flt_prefill_adddata_cdc_check.PublicID as NK_SRC_KEY,
flt_prefill_adddata_cdc_check.CoverageLapse as CVGE_LAPS_IND,
flt_prefill_adddata_cdc_check.LastCancellationDate as LAST_CNCLTN_DTTM,
flt_prefill_adddata_cdc_check.RiskType as RISK_TYPE_CD,
flt_prefill_adddata_cdc_check.PriorPolicyIncDate as PRIOR_PLCY_INCPTN_DTTM,
flt_prefill_adddata_cdc_check.StateIndicator as PREFILL_ST_NAME,
flt_prefill_adddata_cdc_check.Occurances as OCCRNC_CD,
flt_prefill_adddata_cdc_check.LastCancellationReason as LAST_CNCLTN_RSN_CD,
flt_prefill_adddata_cdc_check.PriorPolicyInd as PRIOR_PLCY_IND,
flt_prefill_adddata_cdc_check.ExisitngCustomerRelationInd as EXSTG_CUST_RLTNSHP_IND,
flt_prefill_adddata_cdc_check.RelationshipCode as RLTNSHP_CD,
flt_prefill_adddata_cdc_check.Status as STS_CD,
flt_prefill_adddata_cdc_check.out_prefill_adddata_typecode as PREFILL_ADDLDATA_TYPE_CD,
flt_prefill_adddata_cdc_check.PriorNumberOfPolicies as PRIOR_NUM_POLS_CNT,
flt_prefill_adddata_cdc_check.PolicyNumber as PARNT_PLCY_NUM,
flt_prefill_adddata_cdc_check.PolicyLapse as PLCY_LAPS_IND,
flt_prefill_adddata_cdc_check.PRCS_ID as PRCS_ID,
flt_prefill_adddata_cdc_check.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
flt_prefill_adddata_cdc_check.out_EDW_END_DTTM as EDW_END_DTTM,
flt_prefill_adddata_cdc_check.out_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
flt_prefill_adddata_cdc_check.out_TRANS_END_DTTM as TRANS_END_DTTM
FROM
flt_prefill_adddata_cdc_check;


END; 
';