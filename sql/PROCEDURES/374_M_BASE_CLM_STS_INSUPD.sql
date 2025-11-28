-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_STS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id STRING;
  start_dttm TIMESTAMP;
  end_dttm TIMESTAMP;
  PRCS_ID STRING;
BEGIN
  run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
  end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
  PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);


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


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL,
row_number() over (order by 1) AS source_record_id

FROM db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_STS_RSN_TYPE'' AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_cc_claim, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_claim AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Rank,
$2 as ClaimNumber,
$3 as dt,
$4 as Status,
$5 as TYPECODE,
$6 as src_cd,
$7 as UpdateTime,
$8 as Start_date,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct rank() over (partition by claimnumber order by dt ,updatetime,status,reason) rk,aa.* from

(

select distinct claimnumber,dt, status,

cctl_claimreopenedreason.TYPECODE_stg as reason,

src_cd,updatetime ,start_date

from(

SELECT  

cast(cc_claim_hist.ClaimNumber_stg as varchar(60))as claimnumber,

cc_claim_hist.EventTimestamp_stg as dt,

cast(''REOPND'' as varchar(60))as status,

cast(''SRC_SYS6'' as varchar(60)) as src_cd,

cc_claim_hist.EventTimestamp_stg as updatetime,

cast(cc_claim_hist.ReopenedReason_stg as varchar(60))  as ReopenedReason,

 CURRENT_TIMESTAMP as start_date



FROM    

(

select  cc_claim.* ,c.EventTimestamp_stg

from     DB_T_PROD_STAG.cc_claim  inner join  DB_T_PROD_STAG.cctl_claimstate 

    on  cc_claim.State_stg= cctl_claimstate.id_stg 

    inner join DB_T_PROD_STAG.cc_history c on c.claimid_stg = cc_claim.id_stg

inner join DB_T_PROD_STAG.cctl_historytype d on d.id_stg = c.type_stg

where   cctl_claimstate.name_stg <> ''Draft''

and c.ExposureID_stg is null and SubrogationID_Stg is null and matterid_stg is null

and d.name_stg =''Reopened''

) cc_claim_hist

left outer join  DB_T_PROD_STAG.cc_propertyfiredamage 

    on  cc_claim_hist.ID_stg=cc_propertyfiredamage.ClaimID_stg 

    and cc_propertyfiredamage.retired_stg=''0''

left join  DB_T_PROD_STAG.cc_catastrophe 

    on  cc_claim_hist.CatastropheID_stg=cc_catastrophe.ID_stg

left join  DB_T_PROD_STAG.cc_policy 

    on  cc_policy.ID_stg=cc_claim_hist.PolicyID_stg

where cc_claim_hist.EventTimestamp_stg>(:start_dttm) 

    AND cc_claim_hist.EventTimestamp_stg <= (:end_dttm)) a 

left outer join  DB_T_PROD_STAG.cctl_claimreopenedreason on a.ReopenedReason=cctl_claimreopenedreason.id_stg

where a.dt is not null 



union 



select claimnumber,dt, status, cctl_claimclosedoutcometype.TYPECODE_stg as reason, src_cd,updatetime ,start_date

from (

SELECT

cc_claim_hist.ClaimNumber_stg as claimnumber,

cc_claim_hist.EventTimestamp_stg as dt,

cast(''CLOSED'' as varchar(60))as status,

cast(''SRC_SYS6'' as varchar(60)) as src_cd,

cc_claim_hist.EventTimestamp_stg as updatetime,

cc_claim_hist.ClosedOutcome_stg as ClosedOutcome,

 CURRENT_TIMESTAMP as start_date

FROM    

(

select  cc_claim.* ,c.EventTimestamp_stg

from     DB_T_PROD_STAG.cc_claim  inner join  DB_T_PROD_STAG.cctl_claimstate 

    on  cc_claim.State_stg= cctl_claimstate.id_stg 

    inner join DB_T_PROD_STAG.cc_history c on c.claimid_stg = cc_claim.id_stg

inner join DB_T_PROD_STAG.cctl_historytype d on d.id_stg = c.type_stg

where   cctl_claimstate.name_stg <> ''Draft''

and c.ExposureID_stg is null and SubrogationID_Stg is null and matterid_stg is null

and d.name_stg =''Closed''

) cc_claim_hist

left outer join  DB_T_PROD_STAG.cc_propertyfiredamage 

    on  cc_claim_hist.ID_stg=cc_propertyfiredamage.ClaimID_stg 

    and cc_propertyfiredamage.retired_stg=''0''

left join  DB_T_PROD_STAG.cc_catastrophe 

    on  cc_claim_hist.CatastropheID_stg=cc_catastrophe.ID_stg

left join  DB_T_PROD_STAG.cc_policy 

    on  cc_policy.ID_stg=cc_claim_hist.PolicyID_stg

where cc_claim_hist.EventTimestamp_stg>(:start_dttm) 

    AND cc_claim_hist.EventTimestamp_stg <= (:end_dttm)

)b 

left outer join  DB_T_PROD_STAG.cctl_claimclosedoutcometype on b.ClosedOutcome=cctl_claimclosedoutcometype.ID_stg

where b.dt is not null 



union



select ClaimNumber,dt, status, reason,src_cd,CreateTime ,start_date

from (

SELECT  

cc_claim_hist.ClaimNumber_stg as claimnumber,

cc_claim_hist.EventTimestamp_stg as dt,

cast(''OPEN'' as varchar(60))as status,

cast(''NULL''  as varchar(60)) as reason,

cast(''SRC_SYS6'' as varchar(60)) as src_cd,

cc_claim_hist.EventTimestamp_stg as CreateTime,

 CURRENT_TIMESTAMP as start_date   

FROM    

(

select  cc_claim.* ,c.EventTimestamp_stg

from     DB_T_PROD_STAG.cc_claim  inner join  DB_T_PROD_STAG.cctl_claimstate 

    on  cc_claim.State_stg= cctl_claimstate.id_stg 

    inner join DB_T_PROD_STAG.cc_history c on c.claimid_stg = cc_claim.id_stg

inner join DB_T_PROD_STAG.cctl_historytype d on d.id_stg = c.type_stg

where   cctl_claimstate.name_stg <> ''Draft''

and c.ExposureID_stg is null and SubrogationID_Stg is null and matterid_stg is null

and d.name_stg =''Opened''

) cc_claim_hist

left outer join  DB_T_PROD_STAG.cc_propertyfiredamage 

    on  cc_claim_hist.ID_stg=cc_propertyfiredamage.ClaimID_stg 

    and cc_propertyfiredamage.retired_stg=''0''

left join  DB_T_PROD_STAG.cc_catastrophe 

    on  cc_claim_hist.CatastropheID_stg=cc_catastrophe.ID_stg

left join  DB_T_PROD_STAG.cc_policy 

    on  cc_policy.ID_stg=cc_claim_hist.PolicyID_stg

where cc_claim_hist.EventTimestamp_stg>(:start_dttm) 

    AND cc_claim_hist.EventTimestamp_stg <= (:end_dttm)



)c

)aa
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_cc_claim.ClaimNumber as ClaimNumber,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_src_cd,
SQ_cc_claim.dt as dt,
SQ_cc_claim.Status as Status,
SQ_cc_claim.TYPECODE as TYPECODE,
CASE WHEN SQ_cc_claim.UpdateTime IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE SQ_cc_claim.UpdateTime END as in_CLM_STS_STRT_DT,
SQ_cc_claim.Rank as Rank,
SQ_cc_claim.Start_date as Start_date,
SQ_cc_claim.source_record_id,
row_number() over (partition by SQ_cc_claim.source_record_id order by SQ_cc_claim.source_record_id) as RNK
FROM
SQ_cc_claim
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_claim.src_cd
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD 
FROM db_t_prod_core.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_pass_through.ClaimNumber AND LKP.SRC_SYS_CD = exp_pass_through.o_src_cd
QUALIFY RNK = 1
);


-- Component exp_pass_through_lkp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_lkp AS
(
SELECT
LKP_CLM.CLM_ID as CLM_ID,
exp_pass_through.dt as dt,
DATEADD(day, - 1, exp_pass_through.dt) as o_previuos_date,
exp_pass_through.Status as Status,
exp_pass_through.TYPECODE as TYPECODE,
exp_pass_through.in_CLM_STS_STRT_DT as in_CLM_STS_STRT_DT,
exp_pass_through.Rank as Rank,
exp_pass_through.Start_date as Start_date,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN LKP_CLM ON exp_pass_through.source_record_id = LKP_CLM.source_record_id
);


-- Component LKP_CLM_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_STS AS
(
SELECT
LKP.CLM_ID,
LKP.CLM_STS_STRT_DTTM,
LKP.CLM_STS_RSN_TYPE_CD,
LKP.CLM_STS_TYPE_CD,
LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as in_CLM_STS_RSN_TYPE_CD,
exp_pass_through_lkp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through_lkp.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_STS_STRT_DTTM desc,LKP.CLM_STS_RSN_TYPE_CD desc,LKP.CLM_STS_TYPE_CD desc) RNK
FROM
exp_pass_through_lkp
INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON exp_pass_through_lkp.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
LEFT JOIN (
SELECT CLM_STS.CLM_STS_STRT_DTTM as CLM_STS_STRT_DTTM, CLM_STS.CLM_STS_RSN_TYPE_CD as CLM_STS_RSN_TYPE_CD, CLM_STS.CLM_STS_TYPE_CD as CLM_STS_TYPE_CD, CLM_STS.CLM_ID as CLM_ID 
FROM db_t_prod_core.CLM_STS
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_STS.CLM_ID  ORDER BY CLM_STS.EDW_END_DTTM DESC) = 1
) LKP ON LKP.CLM_ID = exp_pass_through_lkp.CLM_ID
QUALIFY RNK = 1
);


-- Component exp_check_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_flag AS
(
SELECT
exp_pass_through_lkp.CLM_ID as CLM_ID1,
exp_pass_through_lkp.dt as dt,
exp_pass_through_lkp.Status as Status,
LKP_CLM_STS.in_CLM_STS_RSN_TYPE_CD as in_CLM_STS_RSN_TYPE_CD,
exp_pass_through_lkp.in_CLM_STS_STRT_DT as in_CLM_STS_STRT_DTTM,
exp_pass_through_lkp.Rank as Rank,
exp_pass_through_lkp.Start_date as Start_date,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_end_date,
:PRCS_ID as prcs_id,
exp_pass_through_lkp.source_record_id
FROM
exp_pass_through_lkp
INNER JOIN LKP_CLM_STS ON exp_pass_through_lkp.source_record_id = LKP_CLM_STS.source_record_id
);


-- Component exp_CDC_check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_check AS
(
SELECT
exp_check_flag.CLM_ID1 as in_CLM_ID,
exp_check_flag.dt as in_CLM_STS_STRT_DTTM,
exp_check_flag.o_end_date as in_CLM_STS_END_DTTM,
exp_check_flag.in_CLM_STS_RSN_TYPE_CD as in_CLM_STS_RSN_TYPE_CD,
exp_check_flag.Status as in_CLM_STS_TYPE_CD,
exp_check_flag.prcs_id as in_PRCS_ID,
LKP_CLM_STS.CLM_ID as lkp_CLM_ID,
LKP_CLM_STS.CLM_STS_STRT_DTTM as lkp_CLM_STS_STRT_DTTM,
NULL as lkp_CLM_STS_END_DTTM,
LKP_CLM_STS.CLM_STS_RSN_TYPE_CD as lkp_CLM_STS_RSN_TYPE_CD,
LKP_CLM_STS.CLM_STS_TYPE_CD as lkp_CLM_STS_TYPE_CD,
NULL as lkp_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EndDate,
exp_check_flag.Start_date as Startdate,
exp_check_flag.in_CLM_STS_STRT_DTTM as in_CLM_STS_STRT_DTTM1,
NULL as lkp_CLM_STS_STRT_DT,
exp_check_flag.Rank as Rank,
md5 ( ltrim ( rtrim ( to_char ( exp_check_flag.dt ) ) ) || ltrim ( rtrim ( to_char ( exp_check_flag.in_CLM_STS_RSN_TYPE_CD ) ) ) || ltrim ( rtrim ( exp_check_flag.Status ) ) ) as v_MD5_src,
md5 ( ltrim ( rtrim ( to_char ( LKP_CLM_STS.CLM_STS_STRT_DTTM ) ) ) || ltrim ( rtrim ( to_char ( LKP_CLM_STS.CLM_STS_RSN_TYPE_CD ) ) ) || ltrim ( rtrim ( LKP_CLM_STS.CLM_STS_TYPE_CD ) ) ) as v_MD5_tgt,
CASE WHEN LKP_CLM_STS.CLM_ID IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_src = v_MD5_tgt THEN ''X'' ELSE CASE WHEN exp_check_flag.dt > LKP_CLM_STS.CLM_STS_STRT_DTTM THEN ''U'' ELSE NULL END END END as o_Ins_Upd,
LKP_CLM_STS.source_record_id
FROM
LKP_CLM_STS
INNER JOIN exp_check_flag ON LKP_CLM_STS.source_record_id = exp_check_flag.source_record_id
);


-- Component rtr_CDC_Update, Type ROUTER Output Group Update
create or replace temporary table rtr_CDC_Update AS
SELECT
exp_CDC_check.in_CLM_ID as in_CLM_ID,
exp_CDC_check.in_CLM_STS_STRT_DTTM as in_CLM_STS_STRT_DTTM,
exp_CDC_check.in_CLM_STS_END_DTTM as in_CLM_STS_END_DTTM,
exp_CDC_check.in_CLM_STS_RSN_TYPE_CD as in_CLM_STS_RSN_TYPE_CD,
exp_CDC_check.in_CLM_STS_TYPE_CD as in_CLM_STS_TYPE_CD,
exp_CDC_check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_check.lkp_CLM_ID as lkp_CLM_ID,
exp_CDC_check.lkp_CLM_STS_STRT_DTTM as lkp_CLM_STS_STRT_DTTM,
exp_CDC_check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_check.o_Ins_Upd as o_Ins_Upd,
exp_CDC_check.Startdate as Startdate,
exp_CDC_check.EndDate as EndDate,
exp_CDC_check.lkp_CLM_STS_TYPE_CD as lkp_CLM_STS_TYPE_CD,
exp_CDC_check.lkp_CLM_STS_END_DTTM as lkp_CLM_STS_END_DTTM,
exp_CDC_check.lkp_CLM_STS_RSN_TYPE_CD as lkp_CLM_STS_RSN_TYPE_CD,
exp_CDC_check.in_CLM_STS_STRT_DTTM1 as in_CLM_STS_STRT_DT,
exp_CDC_check.lkp_CLM_STS_STRT_DT as lkp_CLM_STS_STRT_DT,
exp_CDC_check.Rank as Rank,
exp_CDC_check.source_record_id
FROM
exp_CDC_check
WHERE exp_CDC_check.o_Ins_Upd = ''U'' or exp_CDC_check.o_Ins_Upd = ''I'';


-- Component clm_sts_updins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE clm_sts_updins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_Update.in_CLM_ID as in_CLM_ID3,
rtr_CDC_Update.in_CLM_STS_STRT_DTTM as in_CLM_STS_STRT_DTTM3,
rtr_CDC_Update.in_CLM_STS_END_DTTM as in_CLM_STS_END_DTTM3,
rtr_CDC_Update.in_CLM_STS_RSN_TYPE_CD as in_CLM_STS_RSN_TYPE_CD3,
rtr_CDC_Update.in_CLM_STS_TYPE_CD as in_CLM_STS_TYPE_CD3,
rtr_CDC_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_CDC_Update.in_CLM_STS_STRT_DT as in_CLM_STS_STRT_DT3,
rtr_CDC_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_CDC_Update.EndDate as EndDate3,
rtr_CDC_Update.Rank as Rank3,
rtr_CDC_Update.Startdate as Startdate2,
0 as UPDATE_STRATEGY_ACTION,
rtr_CDC_Update.source_record_id
FROM
rtr_CDC_Update
);


-- Component exp_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tgt AS
(
SELECT
clm_sts_updins.in_CLM_ID3 as in_CLM_ID3,
clm_sts_updins.in_CLM_STS_STRT_DTTM3 as lkp_CLM_STS_STRT_DT3,
clm_sts_updins.in_CLM_STS_END_DTTM3 as in_CLM_STS_END_DTTM3,
clm_sts_updins.in_CLM_STS_RSN_TYPE_CD3 as in_CLM_STS_RSN_TYPE_CD3,
clm_sts_updins.in_CLM_STS_TYPE_CD3 as in_CLM_STS_TYPE_CD3,
clm_sts_updins.in_PRCS_ID3 as in_PRCS_ID3,
clm_sts_updins.in_CLM_STS_STRT_DT3 as in_CLM_STS_STRT_DT3,
clm_sts_updins.EndDate3 as EndDate3,
DATEADD(second, ( 2 * ( clm_sts_updins.Rank3 - 1 ) ), clm_sts_updins.Startdate2) as out_lkp_EDW_STRT_DTTM3,
clm_sts_updins.source_record_id
FROM
clm_sts_updins
);


-- Component clm_sts_Upd_Insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_STS
(
CLM_ID,
CLM_STS_STRT_DTTM,
CLM_STS_END_DTTM,
CLM_STS_RSN_TYPE_CD,
CLM_STS_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_tgt.in_CLM_ID3 as CLM_ID,
exp_tgt.lkp_CLM_STS_STRT_DT3 as CLM_STS_STRT_DTTM,
exp_tgt.in_CLM_STS_END_DTTM3 as CLM_STS_END_DTTM,
exp_tgt.in_CLM_STS_RSN_TYPE_CD3 as CLM_STS_RSN_TYPE_CD,
exp_tgt.in_CLM_STS_TYPE_CD3 as CLM_STS_TYPE_CD,
exp_tgt.in_PRCS_ID3 as PRCS_ID,
exp_tgt.out_lkp_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_tgt.EndDate3 as EDW_END_DTTM,
exp_tgt.in_CLM_STS_STRT_DT3 as TRANS_STRT_DTTM
FROM
exp_tgt;


-- Component clm_sts_Upd_Insert, Type Post SQL 
UPDATE  db_t_prod_core.CLM_STS 
set TRANS_END_DTTM=  A.lead,

EDW_END_DTTM=A.lead1
FROM
(SELECT	distinct CLM_ID,EDW_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by CLM_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''0.01 SECOND''

 as lead,

max(EDW_STRT_DTTM) over (partition by CLM_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''0.01 SECOND''

 as lead1

FROM	db_t_prod_core.CLM_STS 

 ) a

where  CLM_STS.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and CLM_STS.CLM_ID=A.CLM_ID 

and CLM_STS.TRANS_STRT_DTTM <>CLM_STS.TRANS_END_DTTM

and lead is not null;


END; ';