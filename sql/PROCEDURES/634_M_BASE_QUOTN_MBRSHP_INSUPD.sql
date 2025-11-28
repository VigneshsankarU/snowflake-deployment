-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_MBRSHP_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE 
run_id STRING;
workflow_name STRING;
session_name STRING;
start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
BEGIN 
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

-- Component SQ_pc_Quotn_Mbrship_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_Quotn_Mbrship_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Jobnumber,
$2 as BranchNumber,
$3 as ClientId_alfa,
$4 as TYPECODE,
$5 as Updatetime,
$6 as Editeffectivedate,
$7 as Retired,
$8 as Rank,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	Jobnumber, BranchNumber, ClientId_alfa, TYPECODE, Updatetime,

		Editeffectivedate, Retired, Rank()  OVER (

PARTITION BY Jobnumber, BranchNumber, ClientId_alfa, TYPECODE  

ORDER BY updatetime )  as rnk 

from	(

	SELECT	DISTINCT pc_Quotn_Mbrship_x.Jobnumber, pc_Quotn_Mbrship_x.BranchNumber,

			pc_Quotn_Mbrship_x.ClientId_alfa, pc_Quotn_Mbrship_x.TYPECODE,

			pc_Quotn_Mbrship_x.Updatetime, pc_Quotn_Mbrship_x.Editeffectivedate,

			pc_Quotn_Mbrship_x.Retired 

	FROM

	 (	 SELECT	  pc_job.jobnumber_stg as Jobnumber,

pc_policyperiod.branchnumber_stg as BranchNumber,

pc_effectivedatedfields.ClientId_alfa_stg as ClientId_alfa,

pctl_clientidtype_alfa.TYPECODE_stg  as TYPECODE,

(

case 

    when right(cast(extract(second from  pc_policyperiod.updatetime_stg ) as varchar(24)),4) between 1000 and 1499 then cast(cast( pc_policyperiod.updatetime_stg  as varchar(22))||''0000'' as timestamp(6)) 

    when right(cast(extract(second from pc_policyperiod.updatetime_stg ) as varchar(24)),4) between 1500 and 4499 then cast(cast( pc_policyperiod.updatetime_stg  as varchar(22))||''3000'' as timestamp(6))

    when right(cast(extract(second from  pc_policyperiod.updatetime_stg ) as varchar(24)),4) between 4500 and 8499 then cast(cast( pc_policyperiod.updatetime_stg  as varchar(22))||''7000'' as timestamp(6)) 

    when right(cast(extract(second from  pc_policyperiod.updatetime_stg ) as varchar(24)),4) between 8500 and 9999 then cast(cast( pc_policyperiod.updatetime_stg  as varchar(22))||''0000'' as timestamp(6)) + INTERVAL ''0.010 SECOND''

    else  pc_policyperiod.updatetime_stg 

    end) as Updatetime,

pc_policyperiod.Editeffectivedate_stg as Editeffectivedate,

pc_policyperiod.retired_stg as Retired

from	DB_T_PROD_STAG.pc_effectivedatedfields

inner join DB_T_PROD_STAG.pctl_clientidtype_alfa 	on pc_effectivedatedfields.CLientIDType_alfa_stg=pctl_clientidtype_alfa.id_stg

inner join DB_T_PROD_STAG.pc_policyperiod 	on pc_policyperiod.id_stg=pc_effectivedatedfields.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 	on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 	on pc_job.id_stg=pc_policyperiod.JobID_stg 

inner join DB_T_PROD_STAG.pctl_job 	on pctl_job.id_stg=pc_job.Subtype_stg 

where	pctl_policyperiodstatus.typecode_stg <> ''Temporary'' 

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	and pc_effectivedatedfields.ClientId_alfa_stg is not null 

	and pc_effectivedatedfields.expirationdate_stg is null 

	and pc_effectivedatedfields.updatetime_stg > (:start_dttm)

	and pc_effectivedatedfields.updatetime_stg <= (:end_dttm)

	) as pc_Quotn_Mbrship_x

) x
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_Quotn_Mbrship_x.Jobnumber as Jobnumber,
SQ_pc_Quotn_Mbrship_x.BranchNumber as BranchNumber,
SQ_pc_Quotn_Mbrship_x.ClientId_alfa as ClientId_alfa,
SQ_pc_Quotn_Mbrship_x.TYPECODE as TYPECODE,
SQ_pc_Quotn_Mbrship_x.Updatetime as Updatetime,
SQ_pc_Quotn_Mbrship_x.Editeffectivedate as Editeffectivedate,
SQ_pc_Quotn_Mbrship_x.Retired as Retired,
CASE WHEN SQ_pc_Quotn_Mbrship_x.ClientId_alfa IS NULL THEN SQ_pc_Quotn_Mbrship_x.Updatetime ELSE TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) END as o_QUOTN_MBRSHP_END_DTTM,
SQ_pc_Quotn_Mbrship_x.Rank as Rank,
SQ_pc_Quotn_Mbrship_x.source_record_id
FROM
SQ_pc_Quotn_Mbrship_x
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_from_src.Jobnumber AND LKP.VERS_NBR = exp_pass_from_src.BranchNumber
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_MBRSHP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_MBRSHP AS
(
SELECT
LKP.QUOTN_ID,
LKP.MBRSHP_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP_INSRNC_QUOTN.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_INSRNC_QUOTN.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.MBRSHP_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
LKP_INSRNC_QUOTN
LEFT JOIN (
SELECT  QUOTN_MBRSHP.QUOTN_ID as QUOTN_ID, QUOTN_MBRSHP.MBRSHP_ID as MBRSHP_ID, QUOTN_MBRSHP.EDW_STRT_DTTM as EDW_STRT_DTTM, QUOTN_MBRSHP.EDW_END_DTTM as EDW_END_DTTM FROM DB_T_PROD_CORE.QUOTN_MBRSHP
QUALIFY ROW_NUMBER() OVER(PARTITION BY  QUOTN_ID ORDER BY EDW_STRT_DTTM DESC) = 1
) LKP ON LKP.QUOTN_ID = LKP_INSRNC_QUOTN.QUOTN_ID
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''MBRSHP_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_clientidtype_alfa.TYPECODE'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_from_src.TYPECODE
QUALIFY RNK = 1
);


-- Component LKP_MBRSHP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MBRSHP AS
(
SELECT
LKP.MBRSHP_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MBRSHP_ID asc) RNK
FROM
exp_pass_from_src
INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON exp_pass_from_src.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
LEFT JOIN (
SELECT
MBRSHP_ID,
MBRSHP_NUM,
MBRSHP_TYPE_CD
FROM DB_T_PROD_CORE.MBRSHP
) LKP ON LKP.MBRSHP_NUM = exp_pass_from_src.ClientId_alfa AND LKP.MBRSHP_TYPE_CD = LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_INSRNC_QUOTN.QUOTN_ID as in_QUOTN_ID,
LKP_MBRSHP.MBRSHP_ID as in_MBRSHP_ID,
exp_pass_from_src.Editeffectivedate as in_QUOTN_MBRSHP_STRT_DTTM,
exp_pass_from_src.Updatetime as in_TRANS_STRT_DTTM,
exp_pass_from_src.o_QUOTN_MBRSHP_END_DTTM as in_QUOTN_MBRSHP_END_DTTM,
MD5 ( TO_CHAR ( LKP_MBRSHP.MBRSHP_ID ) ) as in_checksum,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
LKP_QUOTN_MBRSHP.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_MBRSHP.MBRSHP_ID as lkp_MBRSHP_ID,
LKP_QUOTN_MBRSHP.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
MD5 ( TO_CHAR ( LKP_QUOTN_MBRSHP.MBRSHP_ID ) ) as lkp_checksum,
CASE WHEN LKP_QUOTN_MBRSHP.QUOTN_ID IS NULL THEN ''I'' ELSE ( CASE WHEN in_checksum <> lkp_checksum THEN ''U'' ELSE ''R'' END ) END as Ins_upd_flag,
LKP_QUOTN_MBRSHP.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_pass_from_src.Retired as Retired,
exp_pass_from_src.Rank as Rank,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_from_src.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_QUOTN_MBRSHP ON LKP_INSRNC_QUOTN.source_record_id = LKP_QUOTN_MBRSHP.source_record_id
INNER JOIN LKP_MBRSHP ON LKP_QUOTN_MBRSHP.source_record_id = LKP_MBRSHP.source_record_id
);


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
(SELECT
exp_data_transformation.in_QUOTN_ID as in_QUOTN_ID,
exp_data_transformation.in_MBRSHP_ID as in_MBRSHP_ID,
exp_data_transformation.in_QUOTN_MBRSHP_STRT_DTTM as in_QUOTN_MBRSHP_STRT_DTTM,
exp_data_transformation.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.in_QUOTN_MBRSHP_END_DTTM as in_QUOTN_MBRSHP_END_DTTM,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.Ins_upd_flag as Ins_upd_flag,
exp_data_transformation.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_data_transformation.lkp_MBRSHP_ID as lkp_MBRSHP_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.Retired as Retired,
exp_data_transformation.Rank as Rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.in_QUOTN_ID IS NOT NULL and exp_data_transformation.in_MBRSHP_ID IS NOT NULL and ( exp_data_transformation.Ins_upd_flag = ''I'' OR ( exp_data_transformation.lkp_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) or exp_data_transformation.Ins_upd_flag = ''U'' ));


-- Component rtr_ins_upd_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_RETIRED AS
(SELECT
exp_data_transformation.in_QUOTN_ID as in_QUOTN_ID,
exp_data_transformation.in_MBRSHP_ID as in_MBRSHP_ID,
exp_data_transformation.in_QUOTN_MBRSHP_STRT_DTTM as in_QUOTN_MBRSHP_STRT_DTTM,
exp_data_transformation.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.in_QUOTN_MBRSHP_END_DTTM as in_QUOTN_MBRSHP_END_DTTM,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.Ins_upd_flag as Ins_upd_flag,
exp_data_transformation.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_data_transformation.lkp_MBRSHP_ID as lkp_MBRSHP_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.Retired as Retired,
exp_data_transformation.Rank as Rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Ins_upd_flag = ''R'' and exp_data_transformation.Retired != 0 and exp_data_transformation.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component exp_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_retired AS
(
SELECT
CURRENT_TIMESTAMP as EDW_END_DTTM,
rtr_ins_upd_RETIRED.lkp_QUOTN_ID as lkp_QUOTN_ID3,
rtr_ins_upd_RETIRED.lkp_MBRSHP_ID as lkp_MBRSHP_ID3,
rtr_ins_upd_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_RETIRED.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
rtr_ins_upd_RETIRED.source_record_id
FROM
rtr_ins_upd_RETIRED
);


-- Component upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_retired.lkp_QUOTN_ID3 as QUOTN_ID,
exp_retired.lkp_MBRSHP_ID3 as MBRSHP_ID,
exp_retired.lkp_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_retired.EDW_END_DTTM as EDW_END_DTTM,
exp_retired.in_TRANS_STRT_DTTM3 as TRANS_END_DTTM,
1 as UPDATE_STRATEGY_ACTION,
exp_retired.source_record_id
FROM
exp_retired
);


-- Component upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_INSERT.in_QUOTN_ID as QUOTN_ID,
rtr_ins_upd_INSERT.in_MBRSHP_ID as MBRSHP_ID,
rtr_ins_upd_INSERT.in_QUOTN_MBRSHP_STRT_DTTM as QUOTN_MBRSHP_STRT_DTTM,
rtr_ins_upd_INSERT.in_QUOTN_MBRSHP_END_DTTM as QUOTN_MBRSHP_END_DTTM,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_ins_upd_INSERT.EDW_END_DTTM as EDW_END_DTTM,
rtr_ins_upd_INSERT.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
rtr_ins_upd_INSERT.EDW_END_DTTM as TRANS_END_DTTM,
rtr_ins_upd_INSERT.Rank as Rank1,
rtr_ins_upd_INSERT.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_INSERT.source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component QUOTN_MBRSHP_RETIRED, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.QUOTN_MBRSHP
USING upd_retired ON (UPDATE_STRATEGY_ACTION = 1 AND QUOTN_MBRSHP.QUOTN_ID = upd_retired.QUOTN_ID AND QUOTN_MBRSHP.MBRSHP_ID = upd_retired.MBRSHP_ID AND QUOTN_MBRSHP.EDW_STRT_DTTM = upd_retired.EDW_STRT_DTTM)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_retired.EDW_END_DTTM,
TRANS_END_DTTM = upd_retired.TRANS_END_DTTM
;


-- Component EXPTRANS2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS2 AS
(
SELECT
upd_insert.QUOTN_ID as QUOTN_ID,
upd_insert.MBRSHP_ID as MBRSHP_ID,
upd_insert.QUOTN_MBRSHP_STRT_DTTM as QUOTN_MBRSHP_STRT_DTTM,
upd_insert.QUOTN_MBRSHP_END_DTTM as QUOTN_MBRSHP_END_DTTM,
upd_insert.PRCS_ID as PRCS_ID,
upd_insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
CASE WHEN upd_insert.Retired1 != 0 THEN upd_insert.TRANS_STRT_DTTM ELSE TO_TIMESTAMP ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
DATEADD (
  SECOND,
  (2 * (upd_insert.Rank1 - 1)),
  CURRENT_TIMESTAMP()
) as out_edw_strt_dttm,
CASE
  WHEN upd_insert.Retired1 = 0 THEN upd_insert.EDW_END_DTTM
  ELSE DATEADD (
    SECOND,
    (2 * (upd_insert.Rank1 - 1)),
    CURRENT_TIMESTAMP()
  )
END as out_EDW_END_DTTM,
upd_insert.source_record_id
FROM
upd_insert
);


-- Component QUOTN_MBRSHP_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_MBRSHP
(
QUOTN_ID,
MBRSHP_ID,
QUOTN_MBRSHP_STRT_DTTM,
QUOTN_MBRSHP_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
EXPTRANS2.QUOTN_ID as QUOTN_ID,
EXPTRANS2.MBRSHP_ID as MBRSHP_ID,
EXPTRANS2.QUOTN_MBRSHP_STRT_DTTM as QUOTN_MBRSHP_STRT_DTTM,
EXPTRANS2.QUOTN_MBRSHP_END_DTTM as QUOTN_MBRSHP_END_DTTM,
EXPTRANS2.PRCS_ID as PRCS_ID,
EXPTRANS2.out_edw_strt_dttm as EDW_STRT_DTTM,
EXPTRANS2.out_EDW_END_DTTM as EDW_END_DTTM,
EXPTRANS2.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
EXPTRANS2.TRANS_END_DTTM as TRANS_END_DTTM
FROM
EXPTRANS2;


-- Component QUOTN_MBRSHP_INS, Type Post SQL 
UPDATE DB_T_PROD_CORE.QUOTN_MBRSHP FROM

(SELECT	distinct QUOTN_ID, EDW_STRT_DTTM ,TRANS_STRT_DTTM,

 max(EDW_STRT_DTTM) over (partition by QUOTN_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1,

 max(TRANS_STRT_DTTM) over (partition by QUOTN_ID ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead2

FROM	DB_T_PROD_CORE.QUOTN_MBRSHP

 ) a

set EDW_END_DTTM=A.lead1

, TRANS_END_DTTM=A.lead2

WHERE  QUOTN_MBRSHP.QUOTN_ID=A.QUOTN_ID 

AND  QUOTN_MBRSHP.EDW_STRT_DTTM=A.EDW_STRT_DTTM

AND  QUOTN_MBRSHP.TRANS_STRT_DTTM=A.TRANS_STRT_DTTM

and CAST(QUOTN_MBRSHP.EDW_END_DTTM AS DATE)=''9999-12-31''

and CAST(QUOTN_MBRSHP.TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null and lead2 is not null;


END; ';