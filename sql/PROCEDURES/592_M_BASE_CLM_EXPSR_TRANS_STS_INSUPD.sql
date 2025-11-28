-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_TRANS_STS_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component LKP_TERADATA_ETL_REF_XLAT_STATUS, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_STATUS AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_EXPSR_TRANS_STS_TYPE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_cc_transaction, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_transaction AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as statustype,
$3 as updatetime,
$4 as busn_strt_date,
$5 as RNK,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select 

cc_transaction.id as transactionid,

cctl_transactionstatus.typecode as trnssts,

cc_transaction.updatetime as stschangedatetime,

cc_transaction.createtime as busn_strt_date,

Rank()  OVER(PARTITION BY cc_transaction.id ORDER BY cc_transaction.updatetime ) AS RNK

from (

SELECT cc.CreateTime, cc.UpdateTime, cc.ID, cc.ExposureID, cc.Status, cc.Subtype 

FROM ( SELECT X.*

,case when X.TYPECODE = ''voided'' and X.payload=''voided_11'' then ''N'' 

when X.TYPECODE = ''voided'' and X.payload= ''voided_15'' then ''N'' 

when X.TYPECODE = ''transferred'' and X.payload= ''transferred_11''then ''N'' 

when X.TYPECODE = ''transferred'' and X.payload= ''transferred_13'' then ''N'' 

when X.TYPECODE ='' transferred'' and X.payload=''cleared_13'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload=''recoded_11'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload = ''recoded_14'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload=''issued_14'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload= ''cleared_14'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload= ''requested_14'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload= ''voided_14'' then ''N'' 

when X.TYPECODE = ''recoded'' and X.payload= ''transferred_14'' then ''N'' /* EIM-41121 */
else ''Y'' end as eligible

FROM ( SELECT SRC.CreateTime, SRC.UpdateTime, SRC.ID, SRC.ExposureID, SRC.Status, SRC.Subtype, SRC.PublicID, SRC.typecode, GL.PAYLOAD FROM (

SELECT a.CreateTime_stg as CreateTime, a.UpdateTime_stg as UpdateTime, a.ID_stg as ID,

       a.ExposureID_stg as ExposureID, a.Status_stg as Status, a.Subtype_stg as Subtype, a.publicID_stg as PublicID

	   ,cctl_transactionstatus.typecode_stg as typecode

FROM db_t_prod_stag.cc_transaction a

join  db_t_prod_stag.cctl_transactionstatus on a.status_stg= cctl_transactionstatus.ID_stg

LEFT OUTER JOIN db_t_prod_stag.cc_check ON cc_check.id_stg = a.CheckID_stg

WHERE ((a.UpdateTime_stg >(:Start_dttm) and a.UpdateTime_stg <= (:End_dttm))

or (cc_check.UpdateTime_stg >(:Start_dttm) and cc_check.UpdateTime_stg <= (:End_dttm)))) SRC

LEFT OUTER JOIN (SELECT GL_EventStaging_CC.publicID_stg as PublicID, GL_EventStaging_CC.PAYLOAD_NEW_stg as PAYLOAD FROM db_t_prod_stag.GL_EventStaging_CC) GL on GL.PublicId = SRC.PublicID

) X where eligible=''Y'')cc

)cc_transaction

join ( SELECT ID_stg as ID FROM db_t_prod_stag.cctl_transaction

)cctl_transaction on cc_transaction.Subtype = cctl_transaction.id 

join ( SELECT ID_stg as ID, TYPECODE_stg as TYPECODE FROM db_t_prod_stag.cctl_transactionstatus

)cctl_transactionstatus on cc_transaction.Status = cctl_transactionstatus.id

where

cc_transaction.exposureid is not null 

/* and  ((subtype = 2  and (cc_transaction.Comments <> ''Automatic reserves'' or cc_transaction.Comments is null)) or subtype in (1,3)) */
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
SQ_cc_transaction.ID as ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STATUS */ as out_status,
SQ_cc_transaction.updatetime as UpdateTime,
--SQ_cc_transaction.RNK as RNK,
SQ_cc_transaction.busn_strt_date as busn_strt_date,
SQ_cc_transaction.source_record_id,
row_number() over (partition by SQ_cc_transaction.source_record_id order by SQ_cc_transaction.source_record_id) as RNK
FROM
SQ_cc_transaction
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_STATUS LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_transaction.statustype
QUALIFY row_number() over (partition by SQ_cc_transaction.source_record_id order by SQ_cc_transaction.source_record_id) = 1
);


-- Component LKP_CLM_EXPSR_TRANS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS AS
(
SELECT
LKP.CLM_EXPSR_TRANS_ID,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT	CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR_TRANS.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
		CLM_EXPSR_TRANS.EXPSR_COST_CTGY_TYPE_CD as EXPSR_COST_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.PMT_TYPE_CD as PMT_TYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT, CLM_EXPSR_TRANS.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
		CLM_EXPSR_TRANS.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID, 
		CLM_EXPSR_TRANS.GL_MTH_NUM as GL_MTH_NUM, CLM_EXPSR_TRANS.GL_YR_NUM as GL_YR_NUM,
		CLM_EXPSR_TRANS.TRTY_CD as TRTY_CD, CLM_EXPSR_TRANS.PRCS_ID as PRCS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_STRT_DTTM as CLM_EXPSR_TRANS_STRT_DTTM,
		CLM_EXPSR_TRANS.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR_TRANS.EDW_END_DTTM as EDW_END_DTTM,
		CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID 
FROM	db_t_prod_core.CLM_EXPSR_TRANS
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID  
ORDER BY CLM_EXPSR_TRANS.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_CLM_EXPSR_TRANS_ID = exp_all_source.ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as in_CLM_EXPSR_TRANS_ID,
exp_all_source.out_status as in_CLM_EXPSR_TRANS_STS_CD,
exp_all_source.busn_strt_date as in_CLM_EXPSR_TRANS_STS_STRT_DTTM,
to_timestamp_ltz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as in_CLM_EXPSR_TRANS_STS_END_DTTM,
exp_all_source.UpdateTime as in_TRANS_STS_STRT_DTTM,
:PRCS_ID as in_PRCS_ID,
exp_all_source.RNK as RNK,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_CLM_EXPSR_TRANS ON exp_all_source.source_record_id = LKP_CLM_EXPSR_TRANS.source_record_id
);


-- Component LKP_CLM_EXPSR_TRANS_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS_STS AS
(
SELECT
LKP.CLM_EXPSR_TRANS_ID,
LKP.CLM_EXPSR_TRANS_STS_CD,
LKP.CLM_EXPSR_TRANS_STS_STRT_DTTM,
exp_SrcFields.in_CLM_EXPSR_TRANS_ID as in_CLM_EXPSR_TRANS_ID,
exp_SrcFields.in_CLM_EXPSR_TRANS_STS_CD as in_CLM_EXPSR_TRANS_STS_CD,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID asc,LKP.CLM_EXPSR_TRANS_STS_CD asc,LKP.CLM_EXPSR_TRANS_STS_STRT_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_STS_CD as CLM_EXPSR_TRANS_STS_CD, CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_STS_STRT_DTTM as CLM_EXPSR_TRANS_STS_STRT_DTTM, CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID 
FROM db_t_prod_core.CLM_EXPSR_TRANS_STS 
QUALIFY	ROW_NUMBER () OVER (
PARTITION BY CLM_EXPSR_TRANS_ID 
ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.CLM_EXPSR_TRANS_ID = exp_SrcFields.in_CLM_EXPSR_TRANS_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID asc,LKP.CLM_EXPSR_TRANS_STS_CD asc,LKP.CLM_EXPSR_TRANS_STS_STRT_DTTM asc) = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_CLM_EXPSR_TRANS_ID as in_CLM_EXPSR_TRANS_ID,
exp_SrcFields.in_CLM_EXPSR_TRANS_STS_CD as in_CLM_EXPSR_TRANS_STS_CD,
exp_SrcFields.in_CLM_EXPSR_TRANS_STS_STRT_DTTM as in_CLM_EXPSR_TRANS_STS_STRT_DTTM,
exp_SrcFields.in_CLM_EXPSR_TRANS_STS_END_DTTM as in_CLM_EXPSR_TRANS_STS_END_DTTM,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_timestamp_ltz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
LKP_CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
LKP_CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_STS_CD as lkp_CLM_EXPSR_TRANS_STS_CD,
md5 ( to_char ( ltrim ( rtrim ( exp_SrcFields.in_CLM_EXPSR_TRANS_STS_STRT_DTTM ) ) ) || ltrim ( rtrim ( exp_SrcFields.in_CLM_EXPSR_TRANS_STS_CD ) ) ) as v_Src_MD5,
md5 ( to_char ( ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_STS_STRT_DTTM ) ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_STS_CD ) ) ) as v_Tgt_MD5,
CASE WHEN v_Tgt_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_Src_MD5 = v_Tgt_MD5 THEN ''R'' ELSE ''U'' END END as o_CDC_Check,
exp_SrcFields.RNK as RNK,
exp_SrcFields.in_TRANS_STS_STRT_DTTM as in_TRANS_STS_STRT_DTTM,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_CLM_EXPSR_TRANS_STS ON exp_SrcFields.source_record_id = LKP_CLM_EXPSR_TRANS_STS.source_record_id
);


-- Component rtr_clm_expsr_trans_sts_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_clm_expsr_trans_sts_INSERT AS
(	
SELECT
exp_CDC_Check.in_CLM_EXPSR_TRANS_ID as in_CLM_EXPSR_TRANS_ID,
exp_CDC_Check.in_CLM_EXPSR_TRANS_STS_CD as in_CLM_EXPSR_TRANS_STS_CD,
exp_CDC_Check.in_CLM_EXPSR_TRANS_STS_STRT_DTTM as in_CLM_EXPSR_TRANS_STS_STRT_DTTM,
exp_CDC_Check.in_CLM_EXPSR_TRANS_STS_END_DTTM as in_CLM_EXPSR_TRANS_STS_END_DTTM,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.lkp_CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
exp_CDC_Check.lkp_CLM_EXPSR_TRANS_STS_CD as lkp_CLM_EXPSR_TRANS_STS_CD,
exp_CDC_Check.in_TRANS_STS_STRT_DTTM as in_TRANS_STRT_DTTM3,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
exp_CDC_Check.RNK as RNK,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE ( ( exp_CDC_Check.o_CDC_Check = ''I'' ) AND exp_CDC_Check.in_CLM_EXPSR_TRANS_ID IS NOT NULL OR ( exp_CDC_Check.o_CDC_Check = ''U'' ) )
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
rtr_clm_expsr_trans_sts_INSERT.in_CLM_EXPSR_TRANS_ID as in_CLM_EXPSR_TRANS_ID1,
rtr_clm_expsr_trans_sts_INSERT.in_CLM_EXPSR_TRANS_STS_CD as in_CLM_EXPSR_TRANS_STS_CD1,
rtr_clm_expsr_trans_sts_INSERT.in_CLM_EXPSR_TRANS_STS_STRT_DTTM as in_CLM_EXPSR_TRANS_STS_STRT_DTTM1,
rtr_clm_expsr_trans_sts_INSERT.in_CLM_EXPSR_TRANS_STS_END_DTTM as in_CLM_EXPSR_TRANS_STS_END_DTTM1,
rtr_clm_expsr_trans_sts_INSERT.in_PRCS_ID as in_PRCS_ID1,
DATEADD(''SECOND'', ( 2 * ( rtr_clm_expsr_trans_sts_INSERT.RNK - 1 ) ), CURRENT_TIMESTAMP) as out_EDW_STRT_DTTM1,
rtr_clm_expsr_trans_sts_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_clm_expsr_trans_sts_INSERT.in_TRANS_STRT_DTTM3 as in_TRANS_STRT_DTTM31,
rtr_clm_expsr_trans_sts_INSERT.source_record_id
FROM
rtr_clm_expsr_trans_sts_INSERT
);


-- Component CLM_EXPSR_TRANS_STS_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS_STS
(
CLM_EXPSR_TRANS_ID,
CLM_EXPSR_TRANS_STS_CD,
CLM_EXPSR_TRANS_STS_STRT_DTTM,
CLM_EXPSR_TRANS_STS_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt.in_CLM_EXPSR_TRANS_ID1 as CLM_EXPSR_TRANS_ID,
exp_pass_to_tgt.in_CLM_EXPSR_TRANS_STS_CD1 as CLM_EXPSR_TRANS_STS_CD,
exp_pass_to_tgt.in_CLM_EXPSR_TRANS_STS_STRT_DTTM1 as CLM_EXPSR_TRANS_STS_STRT_DTTM,
exp_pass_to_tgt.in_CLM_EXPSR_TRANS_STS_END_DTTM1 as CLM_EXPSR_TRANS_STS_END_DTTM,
exp_pass_to_tgt.in_PRCS_ID1 as PRCS_ID,
exp_pass_to_tgt.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt.in_TRANS_STRT_DTTM31 as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt;


-- Component CLM_EXPSR_TRANS_STS_INS, Type Post SQL 
UPDATE  db_t_prod_core.CLM_EXPSR_TRANS_STS  
set TRANS_END_DTTM =  A.lead, 
EDW_END_DTTM = A.lead1
FROM  

(

SELECT	distinct CLM_EXPSR_TRANS_ID, EDW_STRT_DTTM, 

max(EDW_STRT_DTTM) over (partition by CLM_EXPSR_TRANS_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1, 

max(TRANS_STRT_DTTM) over (partition by CLM_EXPSR_TRANS_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead

FROM db_t_prod_core.CLM_EXPSR_TRANS_STS   

)  A



where CLM_EXPSR_TRANS_STS.CLM_EXPSR_TRANS_ID = A.CLM_EXPSR_TRANS_ID 

and CLM_EXPSR_TRANS_STS.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

and CLM_EXPSR_TRANS_STS.TRANS_STRT_DTTM <> CLM_EXPSR_TRANS_STS.TRANS_END_DTTM

and A.lead is not null ;


END; ';