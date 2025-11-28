-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_SCR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  P_AGMT_TYPE_CD_POLICY_VERSION STRING;
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
  P_AGMT_TYPE_CD_POLICY_VERSION := public.func_get_scoped_param(:run_id, ''p_agmt_type_cd_policy_version'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as UpdateTime,
$3 as FixedID,
$4 as Modl_Name,
$5 as VehHistoryScore_alfa,
$6 as ExpirationDate,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  pc_policyperiod.PublicID,

pc_policyperiod.UpdateTime,

pc_personalvehicle.FixedID,

''REDMTN'' as Modl_Name,

pc_personalvehicle.VehHistoryScore_alfa, 

pc_personalvehicle.ExpirationDate

/* , pc_personalvehicle.VIN */
FROM (

SELECT distinct pc_policyperiod.UpdateTime_stg as UpdateTime, 

 pc_policyperiod.ID_stg as ID, 

 pc_policyperiod.PublicID_stg as PublicID,

 pc_policyperiod.Status_stg as Status

from DB_T_PROD_STAG.pc_policyperiod 

WHERE pc_policyperiod.UpdateTime_stg > (:Start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:End_dttm)

) pc_policyperiod INNER JOIN

(

SELECT pc_personalvehicle.FixedID_Stg as FixedID, 

pc_personalvehicle.ExpirationDate_stg as ExpirationDate,

pc_personalvehicle.BranchID_stg as BranchID

,pc_personalvehicle.VehHistoryScore_alfa_stg as VehHistoryScore_alfa /* EIM-17740 - NAP - Added New field */
FROM DB_T_PROD_STAG.pc_personalvehicle

where pc_personalvehicle.UpdateTime_stg> (:Start_dttm)

and pc_personalvehicle.UpdateTime_stg <= (:End_dttm)

and  (pc_personalvehicle.ExpirationDate_stg is null or pc_personalvehicle.ExpirationDate_stg >:Start_dttm) /* EIM-15097 Added Expiration_date filter */
) pc_personalvehicle 

ON pc_personalvehicle.BranchID = pc_policyperiod.ID 

INNER JOIN

(

SELECT ID_stg as ID, Typecode_stg as TYPECODE from DB_T_PROD_STAG.pctl_policyperiodstatus

) pctl_policyperiodstatus

ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

WHERE  

(pctl_policyperiodstatus.TYPECODE = ''Bound'')

AND (pc_personalvehicle.ExpirationDate IS NULL  OR pc_personalvehicle.ExpirationDate >  CURRENT_TIMESTAMP )
   QUALIFY	ROW_NUMBER () OVER ( partition by pc_policyperiod.PublicID,pc_personalvehicle.FixedID 

  order by pc_policyperiod.UpdateTime desc,pc_personalvehicle.ExpirationDate asc)=1

) SRC
)
);


-- Component exp_data_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_from_src AS
(
SELECT
SQ_pc_policyperiod.PublicID as PublicID,
:p_agmt_type_cd_policy_version as out_AGMT_TYPE_CD,
SQ_pc_policyperiod.UpdateTime as UpdateTime,
SQ_pc_policyperiod.FixedID as FixedID,
''MVEH'' as out_PRTY_ASSET_SBTYPE_CD,
SQ_pc_policyperiod.Modl_Name as Modl_Name,
SQ_pc_policyperiod.VehHistoryScore_alfa as VehHistoryScore_alfa,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component LKP_DIR_PRTY_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DIR_PRTY_ASSET AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_data_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_from_src.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc) RNK
FROM
exp_data_from_src
LEFT JOIN (
SELECT DIR_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, 
DIR_PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, 
DIR_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD 
FROM db_t_prod_core.DIR_PRTY_ASSET DIR_PRTY_ASSET
WHERE PRTY_ASSET_SBTYPE_CD = ''MVEH''  /*  */
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_data_from_src.FixedID AND LKP.PRTY_ASSET_SBTYPE_CD = exp_data_from_src.out_PRTY_ASSET_SBTYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_DIR_AGMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DIR_AGMT AS
(
SELECT
LKP.AGMT_ID,
exp_data_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_from_src.source_record_id ORDER BY LKP.AGMT_ID asc) RNK
FROM
exp_data_from_src
LEFT JOIN (
SELECT DIR_AGMT.AGMT_ID as AGMT_ID, 
DIR_AGMT.NK_SRC_KEY as NK_SRC_KEY, 
DIR_AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD 
FROM db_t_prod_core.DIR_AGMT DIR_AGMT
WHERE AGMT_TYPE_CD = ''PPV''  /*  */
) LKP ON LKP.NK_SRC_KEY = exp_data_from_src.PublicID AND LKP.AGMT_TYPE_CD = exp_data_from_src.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_ANLTCL_MODL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ANLTCL_MODL AS
(
SELECT
LKP.MODL_ID,
exp_data_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_from_src.source_record_id ORDER BY LKP.MODL_ID asc) RNK
FROM
exp_data_from_src
LEFT JOIN (
SELECT ANLTCL_MODL.MODL_ID as MODL_ID, ANLTCL_MODL.MODL_NAME as MODL_NAME FROM db_t_prod_core.ANLTCL_MODL ANLTCL_MODL
) LKP ON LKP.MODL_NAME = exp_data_from_src.Modl_Name
QUALIFY RNK = 1
);


-- Component LKP_AGMT_ASSET_SCR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_ASSET_SCR AS
(
SELECT
LKP.AGMT_ID,
LKP.AGMT_ASSET_SCR_VAL,
LKP_DIR_AGMT.AGMT_ID as in_AGMT_ID,
LKP_DIR_PRTY_ASSET.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
LKP_ANLTCL_MODL.MODL_ID as in_MODL_ID,
LKP_DIR_PRTY_ASSET.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_DIR_PRTY_ASSET.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.AGMT_ASSET_SCR_VAL asc) RNK
FROM
LKP_DIR_PRTY_ASSET
INNER JOIN LKP_DIR_AGMT ON LKP_DIR_PRTY_ASSET.source_record_id = LKP_DIR_AGMT.source_record_id
INNER JOIN LKP_ANLTCL_MODL ON LKP_DIR_AGMT.source_record_id = LKP_ANLTCL_MODL.source_record_id
LEFT JOIN (
SELECT	AGMT_ASSET_SCR.AGMT_ASSET_SCR_VAL as AGMT_ASSET_SCR_VAL,
		AGMT_ASSET_SCR.AGMT_ID as AGMT_ID, 
		AGMT_ASSET_SCR.PRTY_ASSET_ID as PRTY_ASSET_ID,
		AGMT_ASSET_SCR.MODL_ID as MODL_ID 
FROM	db_t_prod_core.AGMT_ASSET_SCR
where	EDW_END_DTTM=cast(''9999-12-31'' as date) /*  */
) LKP ON LKP.AGMT_ID = LKP_DIR_AGMT.AGMT_ID AND LKP.PRTY_ASSET_ID = LKP_DIR_PRTY_ASSET.PRTY_ASSET_ID AND LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_DIR_PRTY_ASSET.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.AGMT_ASSET_SCR_VAL asc) = 1
);


-- Component exp_data_compare, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_compare AS
(
SELECT
LKP_AGMT_ASSET_SCR.in_AGMT_ID as in_AGMT_ID,
LKP_AGMT_ASSET_SCR.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
LKP_AGMT_ASSET_SCR.in_MODL_ID as in_MODL_ID,
cast(TO_CHAR ( LKP_AGMT_ASSET_SCR.in_MODL_ID ) || TO_CHAR ( exp_data_from_src.UpdateTime , ''YYYYMMDD'' ) as BIGINT) as MODL_RUN_ID,
exp_data_from_src.VehHistoryScore_alfa as VehHistoryScore_alfa,
exp_data_from_src.UpdateTime as UpdateTime,
MD5 ( ltrim ( Rtrim ( CASE WHEN LKP_AGMT_ASSET_SCR.AGMT_ASSET_SCR_VAL IS NULL THEN ''*'' ELSE LKP_AGMT_ASSET_SCR.AGMT_ASSET_SCR_VAL END ) ) || LKP_AGMT_ASSET_SCR.AGMT_ID ) as Checksum_lkp,
MD5 ( ltrim ( Rtrim ( CASE WHEN exp_data_from_src.VehHistoryScore_alfa IS NULL THEN ''*'' ELSE exp_data_from_src.VehHistoryScore_alfa END ) ) || LKP_AGMT_ASSET_SCR.in_AGMT_ID ) as Checksum_in,
NULL as out_LVL_NUM,
NULL as out_SCR_FCTR_RATE,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
CASE WHEN Checksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
exp_data_from_src.source_record_id
FROM
exp_data_from_src
INNER JOIN LKP_AGMT_ASSET_SCR ON exp_data_from_src.source_record_id = LKP_AGMT_ASSET_SCR.source_record_id
);


-- Component rtr_agmt_asset_scr_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_agmt_asset_scr_INSERT as
SELECT
exp_data_compare.in_AGMT_ID as in_AGMT_ID,
exp_data_compare.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_data_compare.in_MODL_ID as in_MODL_ID,
exp_data_compare.MODL_RUN_ID as MODL_RUN_ID,
exp_data_compare.VehHistoryScore_alfa as VehHistoryScore_alfa,
exp_data_compare.UpdateTime as UpdateTime,
exp_data_compare.out_LVL_NUM as out_LVL_NUM,
exp_data_compare.out_SCR_FCTR_RATE as out_SCR_FCTR_RATE,
exp_data_compare.out_PRCS_ID as out_PRCS_ID,
exp_data_compare.out_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_compare.out_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_compare.CDC_Flag as CDC_Flag,
exp_data_compare.source_record_id
FROM
exp_data_compare
WHERE ( exp_data_compare.CDC_Flag = ''I'' or exp_data_compare.CDC_Flag = ''U'' ) AND exp_data_compare.in_AGMT_ID IS NOT NULL AND exp_data_compare.in_PRTY_ASSET_ID IS NOT NULL AND exp_data_compare.in_MODL_ID IS NOT NULL;


-- Component upd_agmt_asset_scr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_scr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_asset_scr_INSERT.in_AGMT_ID as in_AGMT_ID1,
rtr_agmt_asset_scr_INSERT.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_agmt_asset_scr_INSERT.in_MODL_ID as in_MODL_ID1,
rtr_agmt_asset_scr_INSERT.MODL_RUN_ID as MODL_RUN_ID1,
rtr_agmt_asset_scr_INSERT.VehHistoryScore_alfa as VehHistoryScore_alfa1,
rtr_agmt_asset_scr_INSERT.UpdateTime as UpdateTime1,
rtr_agmt_asset_scr_INSERT.out_LVL_NUM as out_LVL_NUM1,
rtr_agmt_asset_scr_INSERT.out_SCR_FCTR_RATE as out_SCR_FCTR_RATE1,
rtr_agmt_asset_scr_INSERT.out_PRCS_ID as out_PRCS_ID1,
rtr_agmt_asset_scr_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_agmt_asset_scr_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_agmt_asset_scr_INSERT.source_record_id
FROM
rtr_agmt_asset_scr_INSERT
);


-- Component exp_agmt_asser_scr_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_agmt_asser_scr_ins AS
(
SELECT
upd_agmt_asset_scr_ins.in_AGMT_ID1 as in_AGMT_ID1,
upd_agmt_asset_scr_ins.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_agmt_asset_scr_ins.in_MODL_ID1 as in_MODL_ID1,
upd_agmt_asset_scr_ins.MODL_RUN_ID1 as MODL_RUN_ID1,
upd_agmt_asset_scr_ins.VehHistoryScore_alfa1 as VehHistoryScore_alfa1,
upd_agmt_asset_scr_ins.out_PRCS_ID1 as out_PRCS_ID1,
upd_agmt_asset_scr_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_agmt_asset_scr_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_agmt_asset_scr_ins.UpdateTime1 as UpdateTime1,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
upd_agmt_asset_scr_ins.source_record_id
FROM
upd_agmt_asset_scr_ins
);


-- Component AGMT_ASSET_SCR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_SCR
(
AGMT_ID,
PRTY_ASSET_ID,
MODL_ID,
MODL_RUN_ID,
AGMT_ASSET_SCR_VAL,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_agmt_asser_scr_ins.in_AGMT_ID1 as AGMT_ID,
exp_agmt_asser_scr_ins.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_agmt_asser_scr_ins.in_MODL_ID1 as MODL_ID,
exp_agmt_asser_scr_ins.MODL_RUN_ID1 as MODL_RUN_ID,
exp_agmt_asser_scr_ins.VehHistoryScore_alfa1 as AGMT_ASSET_SCR_VAL,
exp_agmt_asser_scr_ins.out_PRCS_ID1 as PRCS_ID,
exp_agmt_asser_scr_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_agmt_asser_scr_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_agmt_asser_scr_ins.UpdateTime1 as TRANS_STRT_DTTM,
exp_agmt_asser_scr_ins.out_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_agmt_asser_scr_ins;


END; ';