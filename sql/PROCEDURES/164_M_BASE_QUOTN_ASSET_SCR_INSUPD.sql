-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_ASSET_SCR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id integer;


BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;


-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JobNumber,
$2 as BranchNumber,
$3 as FixedID,
$4 as UpdateTime,
$5 as Modl_Name,
$6 as VehHistoryScore_alfa,
$7 as ExpirationDate,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	 JobNumber, BranchNumber, FixedID, UpdateTime, Modl_Name,

		VehHistoryScore_alfa, ExpirationDate

        FROM	

		

(

SELECT   distinct pc_job.JobNumber_stg as Jobnumber,

pc_policyperiod.BranchNumber_stg  as BranchNumber,

pc_personalvehicle.FixedID_stg as FixedID,

pc_policyperiod.UpdateTime_stg as Updatetime,

''REDMTN'' as Modl_Name,

pc_personalvehicle.VehHistoryScore_alfa_stg as VehHistoryScore_alfa , 

pc_personalvehicle.ExpirationDate_stg as ExpirationDate

FROM     DB_T_PROD_STAG.pc_policyperiod 

 INNER JOIN

 DB_T_PROD_STAG.pc_job on pc_job.ID_stg = pc_policyperiod.JobID_stg 

 INNER JOIN DB_T_PROD_STAG.pc_personalvehicle ON pc_personalvehicle.BranchID_stg = pc_policyperiod.ID_stg 

 INNER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

 WHERE  

pctl_policyperiodstatus.TYPECODE_stg NOT IN ( ''Bound'', ''Quoting'')

AND (pc_personalvehicle.ExpirationDate_stg IS NULL  OR pc_personalvehicle.ExpirationDate_stg > :start_dttm )

AND pc_personalvehicle.UpdateTime_stg > :start_dttm and   pc_personalvehicle.UpdateTime_stg <= :end_dttm

)a



QUALIFY Row_Number () Over (PARTITION BY JobNumber,BranchNumber,FixedID 

				  ORDER BY UpdateTime DESC,ExpirationDate ASC)=1
) SRC
)
);


-- Component exp_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src AS
(
SELECT
SQ_pc_policyperiod.JobNumber as JobNumber,
SQ_pc_policyperiod.BranchNumber as BranchNumber,
SQ_pc_policyperiod.FixedID as FixedID,
SQ_pc_policyperiod.UpdateTime as UpdateTime,
SQ_pc_policyperiod.Modl_Name as Modl_Name,
SQ_pc_policyperiod.VehHistoryScore_alfa as VehHistoryScore_alfa,
''QUOTN'' as out_DIR_TYPE_VAL,
''MVEH'' as out_PRTY_ASSET_SBTYPE_CD,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component LKP_DIR_APLCTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DIR_APLCTN AS
(
SELECT
LKP.APLCTN_ID,
exp_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src.source_record_id ORDER BY LKP.APLCTN_ID asc) RNK
FROM
exp_src
LEFT JOIN (
SELECT	DIR_APLCTN.APLCTN_ID as APLCTN_ID, DIR_APLCTN.HOST_APLCTN_ID as HOST_APLCTN_ID,
		DIR_APLCTN.VERS_NBR as VERS_NBR, DIR_APLCTN.DIR_TYPE_VAL as DIR_TYPE_VAL 
FROM	db_t_prod_core.DIR_APLCTN
WHERE DIR_APLCTN.DIR_TYPE_VAL = ''QUOTN''  /*  */
) LKP ON LKP.HOST_APLCTN_ID = exp_src.JobNumber AND LKP.VERS_NBR = exp_src.BranchNumber AND LKP.DIR_TYPE_VAL = exp_src.out_DIR_TYPE_VAL
QUALIFY RNK = 1
);


-- Component LKP_ANLTCL_MODL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ANLTCL_MODL AS
(
SELECT
LKP.MODL_ID,
exp_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src.source_record_id ORDER BY LKP.MODL_ID asc) RNK
FROM
exp_src
LEFT JOIN (
SELECT
MODL_ID,
MODL_NAME
FROM db_t_prod_core.ANLTCL_MODL
) LKP ON LKP.MODL_NAME = exp_src.Modl_Name
QUALIFY RNK = 1
);


-- Component LKP_DIR_PRTY_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DIR_PRTY_ASSET AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc) RNK
FROM
exp_src
LEFT JOIN (
SELECT	DIR_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID,
		DIR_PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, DIR_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD 
FROM	db_t_prod_core.DIR_PRTY_ASSET
WHERE DIR_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD = ''MVEH'' /*  */
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_src.FixedID AND LKP.PRTY_ASSET_SBTYPE_CD = exp_src.out_PRTY_ASSET_SBTYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_ASSET_SCR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_ASSET_SCR AS
(
SELECT
LKP.QUOTN_ID,
LKP.PRTY_ASSET_ID,
LKP.MODL_ID,
LKP.QUOTN_ASSET_SCR_VAL,
LKP.EDW_END_DTTM,
LKP_DIR_APLCTN.APLCTN_ID as in_APLCTN_ID,
LKP_DIR_PRTY_ASSET.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
LKP_ANLTCL_MODL.MODL_ID as in_MODL_ID,
LKP_DIR_APLCTN.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_DIR_APLCTN.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.PRTY_ASSET_ID asc,LKP.MODL_ID asc,LKP.QUOTN_ASSET_SCR_VAL asc,LKP.EDW_END_DTTM asc) RNK
FROM
LKP_DIR_APLCTN
INNER JOIN LKP_ANLTCL_MODL ON LKP_DIR_APLCTN.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_DIR_PRTY_ASSET ON LKP_ANLTCL_MODL.source_record_id = LKP_DIR_PRTY_ASSET.source_record_id
LEFT JOIN (
SELECT	QUOTN_ASSET_SCR.QUOTN_ASSET_SCR_VAL as QUOTN_ASSET_SCR_VAL,
		QUOTN_ASSET_SCR.EDW_END_DTTM as EDW_END_DTTM,
		QUOTN_ASSET_SCR.QUOTN_ID as QUOTN_ID,
		QUOTN_ASSET_SCR.PRTY_ASSET_ID as PRTY_ASSET_ID, 
		QUOTN_ASSET_SCR.MODL_ID as MODL_ID 
FROM	db_t_prod_core.QUOTN_ASSET_SCR
WHERE	EDW_END_DTTM=cast(''9999-12-31'' as date)
) LKP ON LKP.QUOTN_ID = LKP_DIR_APLCTN.APLCTN_ID AND LKP.PRTY_ASSET_ID = LKP_DIR_PRTY_ASSET.PRTY_ASSET_ID AND LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_DIR_APLCTN.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.PRTY_ASSET_ID asc,LKP.MODL_ID asc,LKP.QUOTN_ASSET_SCR_VAL asc,LKP.EDW_END_DTTM asc) 
= 1
);


-- Component exp_compare, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare AS
(
SELECT
LKP_QUOTN_ASSET_SCR.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_ASSET_SCR.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_QUOTN_ASSET_SCR.MODL_ID as lkp_MODL_ID,
LKP_QUOTN_ASSET_SCR.QUOTN_ASSET_SCR_VAL as lkp_QUOTN_ASSET_SCR_VAL,
LKP_QUOTN_ASSET_SCR.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_QUOTN_ASSET_SCR.in_APLCTN_ID as in_APLCTN_ID,
LKP_QUOTN_ASSET_SCR.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
LKP_QUOTN_ASSET_SCR.in_MODL_ID as in_MODL_ID,
cast(TO_CHAR ( LKP_QUOTN_ASSET_SCR.in_MODL_ID ) || TO_CHAR ( exp_src.UpdateTime , ''YYYYMMDD'' ) as BIGINT) as out_MODL_RUN_ID,
exp_src.VehHistoryScore_alfa as VehHistoryScore_alfa,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_src.UpdateTime as PP_UpdateTime,
MD5 ( ltrim ( Rtrim ( CASE WHEN LKP_QUOTN_ASSET_SCR.QUOTN_ASSET_SCR_VAL IS NULL THEN ''*'' ELSE LKP_QUOTN_ASSET_SCR.QUOTN_ASSET_SCR_VAL END ) ) || LKP_QUOTN_ASSET_SCR.QUOTN_ID ) as Checksum_lkp,
MD5 ( ltrim ( Rtrim ( CASE WHEN exp_src.VehHistoryScore_alfa IS NULL THEN ''*'' ELSE exp_src.VehHistoryScore_alfa END ) ) || LKP_QUOTN_ASSET_SCR.in_APLCTN_ID ) as Checksum_in,
CASE WHEN Checksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
exp_src.source_record_id
FROM
exp_src
INNER JOIN LKP_QUOTN_ASSET_SCR ON exp_src.source_record_id = LKP_QUOTN_ASSET_SCR.source_record_id
);


-- Component rtr_quotn_asset_scr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_quotn_asset_scr_INSERT AS
SELECT
exp_compare.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_compare.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_compare.lkp_MODL_ID as lkp_MODL_ID,
exp_compare.lkp_QUOTN_ASSET_SCR_VAL as lkp_QUOTN_ASSET_SCR_VAL,
exp_compare.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare.in_APLCTN_ID as in_APLCTN_ID,
exp_compare.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_compare.in_MODL_ID as in_MODL_ID,
exp_compare.out_MODL_RUN_ID as MODL_RUN_ID,
exp_compare.VehHistoryScore_alfa as VehHistoryScore_alfa,
exp_compare.out_PRCS_ID as out_PRCS_ID,
exp_compare.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare.PP_UpdateTime as PP_UpdateTime,
exp_compare.CDC_Flag as CDC_Flag,
exp_compare.source_record_id
FROM
exp_compare
WHERE ( exp_compare.CDC_Flag = ''I'' or exp_compare.CDC_Flag = ''U'' ) AND exp_compare.in_APLCTN_ID IS NOT NULL AND exp_compare.in_PRTY_ASSET_ID IS NOT NULL AND exp_compare.in_MODL_ID IS NOT NULL;


-- Component upd_quotn_asset_scr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_asset_scr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_asset_scr_INSERT.in_APLCTN_ID as in_QUOTN_ID,
rtr_quotn_asset_scr_INSERT.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_quotn_asset_scr_INSERT.in_MODL_ID as in_MODL_ID1,
rtr_quotn_asset_scr_INSERT.MODL_RUN_ID as MODL_RUN_ID1,
rtr_quotn_asset_scr_INSERT.VehHistoryScore_alfa as VehHistoryScore_alfa1,
rtr_quotn_asset_scr_INSERT.out_PRCS_ID as out_PRCS_ID1,
rtr_quotn_asset_scr_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_quotn_asset_scr_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_quotn_asset_scr_INSERT.PP_UpdateTime as PP_UpdateTime1,
0 as UPDATE_STRATEGY_ACTION,
rtr_quotn_asset_scr_INSERT.source_record_id
FROM
rtr_quotn_asset_scr_INSERT
);


-- Component exp_quotn_asset_scr_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_quotn_asset_scr_ins AS
(
SELECT
upd_quotn_asset_scr_ins.in_QUOTN_ID as in_QUOTN_ID,
upd_quotn_asset_scr_ins.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_quotn_asset_scr_ins.in_MODL_ID1 as in_MODL_ID1,
upd_quotn_asset_scr_ins.MODL_RUN_ID1 as MODL_RUN_ID1,
upd_quotn_asset_scr_ins.VehHistoryScore_alfa1 as VehHistoryScore_alfa1,
upd_quotn_asset_scr_ins.out_PRCS_ID1 as out_PRCS_ID1,
upd_quotn_asset_scr_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_quotn_asset_scr_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_quotn_asset_scr_ins.PP_UpdateTime1 as PP_UpdateTime1,
upd_quotn_asset_scr_ins.source_record_id
FROM
upd_quotn_asset_scr_ins
);


-- Component QUOTN_ASSET_SCR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET_SCR
(
QUOTN_ID,
PRTY_ASSET_ID,
MODL_ID,
MODL_RUN_ID,
QUOTN_ASSET_SCR_VAL,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_quotn_asset_scr_ins.in_QUOTN_ID as QUOTN_ID,
exp_quotn_asset_scr_ins.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_quotn_asset_scr_ins.in_MODL_ID1 as MODL_ID,
exp_quotn_asset_scr_ins.MODL_RUN_ID1 as MODL_RUN_ID,
exp_quotn_asset_scr_ins.VehHistoryScore_alfa1 as QUOTN_ASSET_SCR_VAL,
exp_quotn_asset_scr_ins.out_PRCS_ID1 as PRCS_ID,
exp_quotn_asset_scr_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_quotn_asset_scr_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_quotn_asset_scr_ins.PP_UpdateTime1 as TRANS_STRT_DTTM
FROM
exp_quotn_asset_scr_ins;


END; ';