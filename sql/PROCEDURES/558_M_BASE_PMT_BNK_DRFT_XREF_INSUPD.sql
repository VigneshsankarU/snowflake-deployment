-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PMT_BNK_DRFT_XREF_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component sq_cc_check, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_check AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as DOC_ID,
$2 as EV_ID,
$3 as updatetime,
$4 as out_id,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH BNK_DRFT_INTRM AS

(

SELECT  EV_KEY,

    DOC_KEY,

    EV_TYPE,

    DOC_TYPE,

    DOC_CATEGORY,

    EV_ACT_TYPE_CODE,

    UPDATETIME

    FROM

  ( SELECT  Cast(BC_OUTGOINGPAYMENT.ID AS VARCHAR(100)) AS EV_KEY,

        Cast(BC_OUTGOINGPAYMENT.ID AS VARCHAR(64)) AS  DOC_KEY,

        ''EV_SBTYPE2'' AS EV_TYPE,

        Cast(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,

        Cast(''DOC_CTGY_TYPE4''  AS VARCHAR(50))AS DOC_CATEGORY,

Cast(''EV_ACTVY_TYPE31''  AS VARCHAR(50) ) AS EV_ACT_TYPE_CODE, /* RFNDPMT */
        BC_OUTGOINGPAYMENT.UPDATETIME

        FROM  (

		Select A.UpdateTime_stg as UpdateTime, A.ID_stg as ID

from db_t_prod_stag.bc_outgoingpayment A

where A.UpdateTime_stg > (:start_dttm)

and A.UpdateTime_stg <= (:end_dttm)

)BC_OUTGOINGPAYMENT 

 UNION

    SELECT  Cast(T.ID AS VARCHAR(100)) AS EV_KEY,

        T.DOC_KEY AS DOC_KEY,

        Cast(''EV_SBTYPE2'' AS VARCHAR(50)) AS EV_TYPE,

        Cast(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,

        Cast(''DOC_CTGY_TYPE1''  AS VARCHAR(50))AS DOC_CATEGORY,

        Cast(''EV_ACTVY_TYPE24'' AS VARCHAR(50)) AS EV_ACT_TYPE_CD,

        T.UPDATETIME

        FROM (

		SELECT a.ID_stg as ID, a.Subtype_stg as Subtype, a.UpdateTime_stg as UpdateTime, cc_check.Publicid_stg as doc_Key

FROM db_t_prod_stag.cc_transaction a LEFT OUTER JOIN db_t_prod_stag.cc_check ON cc_check.id_stg = a.CheckID_stg

WHERE ((a.UpdateTime_stg >(:start_dttm)

and a.UpdateTime_stg <= (:end_dttm))

or (cc_check.UpdateTime_stg >(:start_dttm)

and cc_check.UpdateTime_stg <= (:end_dttm)))

) T 

 JOIN ( SELECT id_stg as id, typecode_stg as TYPECODE FROM db_t_prod_stag.cctl_transaction ) TL ON TL.ID=T.SUBTYPE AND TL.TYPECODE=''Payment''

        WHERE T.DOC_KEY IS NOT NULL 

   UNION

    SELECT

 Cast(T.ID AS VARCHAR(100)) AS EV_KEY,

        T.COMBINEDCHECKNUMBER_ALFA  AS DOC_KEY,

        Cast(''EV_SBTYPE2'' AS VARCHAR(50)) AS EV_TYPE,

        Cast(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,

        Cast(''DOC_CTGY_TYPE1''  AS VARCHAR(50))AS DOC_CATEGORY,

        Cast(''EV_ACTVY_TYPE24'' AS VARCHAR(50)) AS EV_ACT_TYPE_CD,

        T.UPDATETIME

        FROM ( SELECT a.ID_stg as ID, a.Subtype_stg as Subtype, a.UpdateTime_stg as UpdateTime,

cc_check.CombinedCheckNumber_alfa_stg as combinedchecknumber_alfa

FROM db_t_prod_stag.cc_transaction a

LEFT OUTER JOIN db_t_prod_stag.cc_check ON cc_check.id_stg = a.CheckID_stg

WHERE ((a.UpdateTime_stg >(:start_dttm)

and a.UpdateTime_stg <= (:end_dttm))

or (cc_check.UpdateTime_stg >(:start_dttm)

and cc_check.UpdateTime_stg <= (:end_dttm)))

) T

 JOIN( SELECT id_stg as id, typecode_stg as TYPECODE FROM db_t_prod_stag.cctl_transaction) TL ON TL.ID=T.SUBTYPE AND TL.TYPECODE=''Payment''

        WHERE T.COMBINEDCHECKNUMBER_ALFA IS NOT NULL

 ) X 

 QUALIFY Row_Number() Over(PARTITION BY EV_KEY,EV_TYPE, EV_ACT_TYPE_CODE, DOC_KEY, DOC_TYPE, DOC_CATEGORY ORDER BY UPDATETIME DESC) = 1

 )



SELECT

SRC_XLAT.DOC_ID ,

SRC_XLAT.EV_ID ,

SRC_XLAT.UPDATETIME ,CASE WHEN PMT_EV_ID IS NULL AND BNK_DRFT_DOC_ID IS NULL and EV_ID is NOT NULL AND DOC_ID IS NOT NULL THEN ''INSERT'' END AS OUT_ID 





FROM

(SELECT DC.DOC_ID,

EV.EV_ID,

SRC.UPDATETIME

FROM BNK_DRFT_INTRM SRC

LEFT OUTER JOIN --EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
db_t_prod_core.TERADATA_ETL_REF_XLAT AS XLAT_EV_SBTYPE_CD

 ON XLAT_EV_SBTYPE_CD.SRC_IDNTFTN_VAL=SRC.EV_TYPE 

 AND XLAT_EV_SBTYPE_CD.TGT_IDNTFTN_NM= ''EV_SBTYPE''

 AND XLAT_EV_SBTYPE_CD.SRC_IDNTFTN_NM= ''derived'' 

 AND XLAT_EV_SBTYPE_CD.SRC_IDNTFTN_SYS=''DS'' 

 AND XLAT_EV_SBTYPE_CD.EXPN_DT=''9999-12-31''

		

LEFT OUTER JOIN --EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
db_t_prod_core.TERADATA_ETL_REF_XLAT AS XLAT_DOC_CTGY_TYPE

 ON XLAT_DOC_CTGY_TYPE.SRC_IDNTFTN_VAL=SRC.DOC_CATEGORY 

 AND XLAT_DOC_CTGY_TYPE.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE''

 AND XLAT_DOC_CTGY_TYPE.EXPN_DT=''9999-12-31''



LEFT OUTER JOIN --EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
db_t_prod_core.TERADATA_ETL_REF_XLAT AS XLAT_DOC_TYPE

 ON XLAT_DOC_TYPE.SRC_IDNTFTN_VAL=SRC.DOC_TYPE 

 AND XLAT_DOC_TYPE.TGT_IDNTFTN_NM= ''DOC_TYPE''

 AND XLAT_DOC_TYPE.EXPN_DT=''9999-12-31''



LEFT OUTER JOIN --EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
db_t_prod_core.TERADATA_ETL_REF_XLAT AS XLAT_EV_ACTVY_TYPE

 ON XLAT_EV_ACTVY_TYPE.SRC_IDNTFTN_VAL=SRC.EV_ACT_TYPE_CODE

 AND XLAT_EV_ACTVY_TYPE.TGT_IDNTFTN_NM= ''EV_ACTVY_TYPE'' 

 AND XLAT_EV_ACTVY_TYPE.SRC_IDNTFTN_SYS in (''GW'',''DS'' )

 AND XLAT_EV_ACTVY_TYPE.EXPN_DT=''9999-12-31''



 left JOIN --EVIEWDB_EDW.DOC 
 db_t_prod_core.DOC AS DC

 ON DC.DOC_ISSUR_NUM = SRC.DOC_KEY 

 AND DC.DOC_TYPE_CD = XLAT_DOC_TYPE.TGT_IDNTFTN_VAL 

 AND DC.DOC_CTGY_TYPE_CD = XLAT_DOC_CTGY_TYPE.TGT_IDNTFTN_VAL

 AND DC.EDW_END_DTTM=''9999-12-31 23:59:59.999999''



left JOIN --EVIEWDB_EDW.EV 
db_t_prod_core.EV AS EV

 ON EV.SRC_TRANS_ID = EV_KEY

 AND EV.EV_SBTYPE_CD = COALESCE(XLAT_EV_SBTYPE_CD.TGT_IDNTFTN_VAL,''UNK'')

 AND EV.EV_ACTVY_TYPE_CD = COALESCE(XLAT_EV_ACTVY_TYPE.TGT_IDNTFTN_VAL,''UNK'')

 AND EV.EV_SBTYPE_CD=''FINANCL'' 

 AND EV.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

 ) SRC_XLAT

 LEFT JOIN --EVIEWDB_EDW.PMT_BNK_DRFT_XREF 
 db_t_prod_core.PMT_BNK_DRFT_XREF AS TGT_LKP

 ON TGT_lkp.PMT_EV_ID = SRC_XLAT.EV_ID AND TGT_lkp.BNK_DRFT_DOC_ID = SRC_XLAT.DOC_ID
) SRC
)
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
sq_cc_check.EV_ID as EV_ID,
sq_cc_check.DOC_ID as DOC_ID,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM1,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM1,
sq_cc_check.updatetime as updatetime1,
sq_cc_check.out_id as out_id,
sq_cc_check.source_record_id
FROM
sq_cc_check
);


-- Component FTR_data, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FTR_data AS
(
SELECT
exp_pass_to_target.EV_ID as EV_ID,
exp_pass_to_target.DOC_ID as DOC_ID,
exp_pass_to_target.out_PRCS_ID as out_PRCS_ID,
exp_pass_to_target.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
exp_pass_to_target.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
exp_pass_to_target.updatetime1 as updatetime1,
exp_pass_to_target.out_id as out_id,
exp_pass_to_target.source_record_id
FROM
exp_pass_to_target
WHERE exp_pass_to_target.out_id = ''INSERT''
);


-- Component tgt_PMT_BNK_DRFT_XREF_ins, Type TARGET 
INSERT INTO db_t_prod_core.PMT_BNK_DRFT_XREF
(
PMT_EV_ID,
BNK_DRFT_DOC_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
FTR_data.EV_ID as PMT_EV_ID,
FTR_data.DOC_ID as BNK_DRFT_DOC_ID,
FTR_data.out_PRCS_ID as PRCS_ID,
FTR_data.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
FTR_data.in_EDW_END_DTTM1 as EDW_END_DTTM,
FTR_data.updatetime1 as TRANS_STRT_DTTM
FROM
FTR_data;


END; ';