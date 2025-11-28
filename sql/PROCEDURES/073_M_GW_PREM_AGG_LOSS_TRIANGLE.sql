-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_GW_PREM_AGG_LOSS_TRIANGLE("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  MO_ID STRING;
  PRCS_ID STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):MO_ID::STRING,
    TRY_PARSE_JSON(:param_json):PRCS_ID::STRING
  INTO
    MO_ID,
    PRCS_ID;

-- PIPELINE START FOR 1

-- Component SQ_GW_PREM_TRANS_GL_INFO_MO_ID, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GW_PREM_TRANS_GL_INFO_MO_ID AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as GL_EXTC_YR,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct MO_ID from 

DB_T_PROD_COMN.AGG_GW_PREMIUM_INFO

where 

MO_ID in (:MO_ID)
) SRC
)
);


-- Component UPD_DELETE, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPD_DELETE AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
SQ_GW_PREM_TRANS_GL_INFO_MO_ID.GL_EXTC_YR as GL_EXTC_YR,
2 as UPDATE_STRATEGY_ACTION
FROM
SQ_GW_PREM_TRANS_GL_INFO_MO_ID
);


-- Component AGG_GW_PREMIUM_INFO_DELETE, Type TARGET 

/* Perform Deletes */
DELETE FROM DB_T_PROD_COMN.AGG_GW_PREMIUM_INFO
WHERE EXISTS (SELECT 1 FROM UPD_DELETE WHERE UPDATE_STRATEGY_ACTION = 2 AND AGG_GW_PREMIUM_INFO.MO_ID = UPD_DELETE.GL_EXTC_YR)
;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component sq_gw_prem_trans_plcy_info, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_gw_prem_trans_plcy_info AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MO_ID,
$2 as CO_CD,
$3 as ST_CD,
$4 as SRVC_CTR_NUM,
$5 as AGNT_NUM,
$6 as RSRVG_GRP_CD,
$7 as LOB_CD,
$8 as PROD_CD,
$9 as PROD_DESC,
$10 as PLCY_NUM,
$11 as PLCY_EFECT_DT,
$12 as PLCY_EXPN_DT,
$13 as ACCNTG_MO,
$14 as ACCNTG_YR,
$15 as CAT,
$16 as POOL_CD,
$17 as POOL_DESC,
$18 as MNTARY_AMT,
$19 as LEDGR,
$20 as GL_ACCT_NUM,
$21 as GL_ACCT_DESC,
$22 as GL_ACCT_GRP,
$23 as PLCY_TERM,
$24 as TOT_PREM_AMT,
$25 as DEPT_ID,
$26 as PLCY_TYPE,
$27 as PROD_PLAN,
$28 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT (GL_EXTC_YR * 100 + GL_EXTC_MTH) MO_ID

,COALESCE(COMP_CD, ''UNK'') COMP_CD /* ,UNDERWRITING_CMPY_CD */
,PS_ST

,SRVC_CTR AS  SVC_NBR

,AGENT_NUM /* ,CAST(CAST(AGENT_NBR AS INTEGER) AS VARCHAR(20)) AGNT_NBR */
,  COALESCE(CASE WHEN PROD=''F00001''and  ( cast(PROD_NAME as varchar(50)) like ''SF%'') AND COMP_CD IN (''AIC'',''AMF'') THEN ''Fire'' 

WHEN PROD=''F00001''and (cast(PROD_NAME as varchar(50)) like ''SF%'') AND COMP_CD IN (''AAI'') THEN ''No_Triangle'' 

WHEN PROD=''F00001''and ( cast(PROD_NAME as varchar(50))  like ''SF%'') AND COMP_CD IN (''AMI'') THEN ''No_Triangle'' 

WHEN PROD=''F00001''and ( cast(PROD_NAME as varchar(50)) not like ''SF%'') AND COMP_CD IN (''AMI'') THEN ''Fire''

WHEN PROD=''GL0001''and (cast(PROD_NAME as varchar(50))  like ''PersonalUmbrella%'') AND COMP_CD IN (''AIC'',''AMI'') THEN ''Limited_Umbrella'' 

WHEN PROD=''GL0001''and ( cast(PROD_NAME as varchar(50)) NOT  like ''PersonalUmbrella%'') AND COMP_CD IN (''AMI'') THEN ''OL'' 

WHEN PROD=''GL0001''and ( cast(PROD_NAME as varchar(50)) NOT  like ''PersonalUmbrella%'')AND COMP_CD IN (''AIC'') THEN ''No_Triangle'' ELSE

RSV_GRP END, ''UNK'') RSV_GRP

/* ,COALESCE(INSRNC_LOB_TYPE_DESC, ''UNKNOWN'') AS LOB_CD--,POLICY_TYPE_DESC AS LOB */
,COALESCE(CASE WHEN PROD =''F00001'' AND ( cast(PROD_NAME as varchar(50)) like ''SF%'') THEN ''Fire''

		WHEN PROD =''F00001'' AND  ( cast(PROD_NAME as varchar(50)) not like ''SF%'') THEN ''Farmowners''

		WHEN PROD =''GL0001'' AND  ( cast(PROD_NAME as varchar(50))  like ''PersonalUmbrella%'') THEN ''Umbrella''

		WHEN PROD =''GL0001'' AND  (cast(PROD_NAME as varchar(50)) not  like ''PersonalUmbrella%'') THEN ''Other''

		else 		lob_cd end, ''UNKNOWN'') AS LOB_CD

,PROD

,CASE WHEN PROD =''F00001'' AND ( cast(PROD_NAME as varchar(50)) like ''SF%'') THEN ''Fire''

		WHEN PROD =''F00001'' AND  ( cast(PROD_NAME as varchar(50)) not like ''SF%'') THEN ''Farm Fire''

		WHEN PROD =''GL0001'' AND  ( cast(PROD_NAME as varchar(50))  like ''PersonalUmbrella%'') THEN ''GW Personal Umbrella''

		WHEN PROD =''GL0001'' AND  (cast(PROD_NAME as varchar(50)) not  like ''PersonalUmbrella%'') THEN ''FARM UMBR PERS''

		else 		COV_DESC end AS PRODUCT_DESC

,PLCY_NUM

,PLCY_EFF_DT

,PLCY_EXPN_DT

,GL_EXTC_MTH

,GL_EXTC_YR

,CASE  WHEN SUBSTR(GL.PS_POOL_CD, 1, 3) = ''CAT''     THEN ''CAT''      ELSE ''NonCAT'' END CAT

,GL.PS_POOL_CD

,POOL_DESC

,SUM(CASE   WHEN GL.BUS_ACT_TRNS_ACT IN (''WRT'',''NEF'')   THEN TRANS_PREM_AMT  ELSE TRANS_UNEARND_PREM    END) MONETARY_AMT

,LEDGR

,GL.GL_ACCOUNT_NBR

,ACT.ACCT_DESC

,METRIC_LONG_NM ACCT_GRP

,PLCY_TERM_NUM

,CAST(NULL AS INTEGER) AS TOTAL_PREM

,GL.DEPTID, 

PROD_NAME,

case

    when cast(PROD_NAME as varchar(50))=''BUSINESSOWNERS'' then ''BOP''

    when cast(PROD_NAME as varchar(50)) like ''HO%'' then (case when AGMT_PROD_CVGE_LVL_DESC=''HOME INNOVATION'' then ''HOINN'' else ''HO'' end)

    when cast(PROD_NAME as varchar(50)) like ''MH%'' then ''MH''

    when cast(PROD_NAME as varchar(50)) like ''SF%'' then ''SF''

    when cast(PROD_NAME as varchar(50))=''WATERCRAFT'' then ''WTC''

    when cast(PROD_NAME as varchar(50))=''PersonalUmbrella'' then ''UMB''

	when cast(PROD_NAME as varchar(50))=''FarmUmbrella'' then ''UMB''

    else cast(PROD_NAME as varchar(50))

end as PROD_PLAN

FROM DB_T_PROD_COMN.GW_PREM_TRANS_PLCY_INFO GP

JOIN DB_T_PROD_COMN.GW_PREM_TRANS_GL_INFO GL

ON GP.PLCY_INFO_ID = GL.PLCY_INFO_ID

AND (  ( GL.BUS_ACT_TRNS_ACT IN (''WRT'',''NEF'')  AND GL.LEDGR = ''ACTUALS''    )    OR ( GL.BUS_ACT_TRNS_ACT = ''UER''  AND GL.LEDGR = ''STATUTORY''  )  )

LEFT OUTER JOIN ( SELECT DISTINCT POOL_CD     ,POOL_DESC FROM DB_T_SHRD_FIN_PROD.FIN_POOL_CD_LKUP      ) PL ON GL.PS_POOL_CD = PL.POOL_CD

LEFT OUTER JOIN DB_T_CORE_FIN_PROD.FIN_PRODUCT_HIER PH ON PH.PRODUCT_CD = GL.PROD and PH.PRODUCT_CD not in (''F00001'',''GL0001'')

LEFT OUTER JOIN DB_T_STAG_FIN_PROD.FIN_ACCT_NBR_GRP_LKUP  ACCT ON    GL.GL_ACCOUNT_NBR=ACCT.ACCT_NBR  AND METRIC_SHORT_NM IN (''DWP'',''UEP'') 

LEFT OUTER JOIN  (SELECT ACCT_NBR,ACCT_DESC FROM DB_T_SHRD_FIN_PROD.FIN_ACCT_NBR_LKUP  

QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_NBR ORDER BY  EFF_DT DESC)=1)ACT ON GL.GL_ACCOUNT_NBR=ACT.ACCT_NBR

LEFT OUTER JOIN 

(SELECT HOST_AGMT_NUM,COALESCE(MODL_NUM,0) as MODL_NUM,TERM_NUM,INSRNC_LOB_TYPE_DESC,MODL_EFF_DTTM,PROD_NAME,AGMT_PROD_CVGE_LVL_DESC,

MAX(CASE WHEN INTRNL_ORG_SBTYPE_CD=''PRDA''THEN LTRIM(INTRNL_ORG_NUM, ''0'') END) AS AGENT_NUM,

MAX(CASE WHEN PRTY_AGMT_ROLE_CD=''CMP'' AND  INTRNL_ORG_SBTYPE_CD=''CO'' THEN INTRNL_ORG_NUM  END) AS COMP_CD

FROM (

SELECT HOST_AGMT_NUM,MODL_EFF_DTTM,MODL_NUM,TERM_NUM,A.AGMT_ID,PROD_NAME,INSRNC_LOB_TYPE_DESC,AGMT_PROD_CVGE_LVL_DESC FROM DB_T_PROD_CORE.AGMT A 

LEFT JOIN DB_T_PROD_CORE.AGMT_PROD AP ON AP.AGMT_ID=A.AGMT_ID AND  AP.AGMT_PROD_ROLE_CD=''plcytype'' and AP.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

LEFT JOIN DB_T_PROD_CORE.PROD PROD ON PROD.PROD_ID=AP.PROD_ID AND PROD.EDW_END_DTTM =''9999-12-31 23:59:59.999999''

LEFT JOIN DB_T_PROD_CORE.INSRNC_LOB_TYPE  ILT ON PROD.INSRNC_LOB_TYPE_CD=ILT.INSRNC_LOB_TYPE_CD AND ILT.EDW_END_DTTM =''9999-12-31 23:59:59.999999''



WHERE A.AGMT_TYPE_CD=''PPV'' 

QUALIFY RANK() OVER(PARTITION BY HOST_AGMT_NUM,TERM_NUM,MODL_NUM ORDER BY A.MODL_EFF_DTTM DESC, A.TRANS_STRT_DTTM DESC, AP.TRANS_STRT_DTTM DESC)=1) A

LEFT OUTER JOIN

(SELECT AGMT_ID,PA.PRTY_ID,PRTY_AGMT_ROLE_CD,INTRNL_ORG_NUM,INTRNL_ORG_SBTYPE_CD,INTRNL_ORG_STRT_DTTM,PRTY_AGMT_STRT_DTTM

FROM DB_T_PROD_CORE.PRTY_AGMT PA JOIN DB_T_PROD_CORE.INTRNL_ORG IO ON 

PA.PRTY_ID=IO.INTRNL_ORG_PRTY_ID WHERE PRTY_AGMT_ROLE_CD IN (''PRDA'',''CMP''))Q ON 

A.AGMT_ID=Q.AGMT_ID  AND PRTY_AGMT_STRT_DTTM<=MODL_EFF_DTTM

GROUP BY 1,2,3,4,5,6,7

)PLCY ON GP.PLCY_NUM=PLCY.HOST_AGMT_NUM 

AND GP.PLCY_TERM_NUM=PLCY.TERM_NUM

AND GP.PLCY_MODL_NUM=PLCY.MODL_NUM

LEFT OUTER JOIN (select  distinct  source, product_cd , CMPY_CD ,  RSV_GRP,LOB_CD

FROM   DB_T_SHRD_FIN_PROD.RSV_GRP_LKUP 

where product_cd NOT in (''GL0001'',''F00001'')

QUALIFY ROW_NUMBER() OVER( PARTITION BY  product_cd , CMPY_CD   ORDER BY source ) = 1) RSV ON GL.PROD = RSV.PRODUCT_CD

AND RSV.CMPY_CD = COMP_CD

WHERE (GL_EXTC_YR * 100 + GL_EXTC_MTH) in (:MO_ID)

GROUP BY 1 ,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27
) SRC
)
);


-- Component exp_data_cleanse, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_cleanse AS
(
SELECT
sq_gw_prem_trans_plcy_info.MO_ID as mo_id,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.CO_CD ) ) as out_co_cd,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.ST_CD ) ) as out_st_cd,
sq_gw_prem_trans_plcy_info.SRVC_CTR_NUM as srvc_ctr_nm,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.AGNT_NUM ) ) as out_agnt_nm,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.RSRVG_GRP_CD ) ) as out_rsrvg_grp_cd,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.LOB_CD ) ) as out_lob_cd,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.PROD_CD ) ) as out_prod_cd,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.PROD_DESC ) ) as out_prod_desc,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.PLCY_NUM ) ) as out_plcy_num,
sq_gw_prem_trans_plcy_info.PLCY_EFECT_DT as plcy_eff_dt,
sq_gw_prem_trans_plcy_info.PLCY_EXPN_DT as plcy_expn_dt,
sq_gw_prem_trans_plcy_info.ACCNTG_MO as acctng_mo,
sq_gw_prem_trans_plcy_info.ACCNTG_YR as acctng_yr,
sq_gw_prem_trans_plcy_info.CAT as cat,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.POOL_CD ) ) as out_pool_cd,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.POOL_DESC ) ) as out_pool_desc,
sq_gw_prem_trans_plcy_info.MNTARY_AMT as mntary_amt,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.LEDGR ) ) as out_ledger,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.GL_ACCT_NUM ) ) as out_gl_acct_num,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.GL_ACCT_DESC ) ) as out_gl_acct_desc,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.GL_ACCT_GRP ) ) as out_gl_acct_grp,
sq_gw_prem_trans_plcy_info.PLCY_TERM as plcy_term,
sq_gw_prem_trans_plcy_info.TOT_PREM_AMT as tot_prem_amt,
LTRIM ( RTRIM ( sq_gw_prem_trans_plcy_info.DEPT_ID ) ) as out_dept_id,
:PRCS_ID as prcs_id,
CURRENT_TIMESTAMP () as edw_start_dttm,
TO_DATE ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.NS'' ) as edw_end_dttm,
sq_gw_prem_trans_plcy_info.PLCY_TYPE as PLCY_TYPE,
sq_gw_prem_trans_plcy_info.PROD_PLAN as PROD_PLAN,
sq_gw_prem_trans_plcy_info.source_record_id
FROM
sq_gw_prem_trans_plcy_info
);


-- Component UPD_INSERT, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPD_INSERT AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_data_cleanse.mo_id as MO_ID,
exp_data_cleanse.out_co_cd as CO_CD,
exp_data_cleanse.out_st_cd as ST_CD,
exp_data_cleanse.srvc_ctr_nm as SRVC_CTR_NUM,
exp_data_cleanse.out_agnt_nm as AGNT_NUM,
exp_data_cleanse.out_rsrvg_grp_cd as RSRVG_GRP_CD,
exp_data_cleanse.out_lob_cd as LOB_CD,
exp_data_cleanse.out_prod_cd as PROD_CD,
exp_data_cleanse.out_prod_desc as PROD_DESC,
exp_data_cleanse.out_plcy_num as PLCY_NUM,
exp_data_cleanse.plcy_eff_dt as PLCY_EFF_DT,
exp_data_cleanse.plcy_expn_dt as PLCY_EXPN_DT,
exp_data_cleanse.acctng_mo as ACCNTG_MO,
exp_data_cleanse.acctng_yr as ACCNTG_YR,
exp_data_cleanse.cat as CAT,
exp_data_cleanse.out_pool_cd as POOL_CD,
exp_data_cleanse.out_pool_desc as POOL_DESC,
exp_data_cleanse.mntary_amt as MNTARY_AMT,
exp_data_cleanse.out_ledger as LEDGR,
exp_data_cleanse.out_gl_acct_num as GL_ACCT_NUM,
exp_data_cleanse.out_gl_acct_desc as GL_ACCT_DESC,
exp_data_cleanse.out_gl_acct_grp as GL_ACCT_GRP,
exp_data_cleanse.plcy_term as PLCY_TERM,
exp_data_cleanse.tot_prem_amt as TOT_PREM_AMT,
exp_data_cleanse.out_dept_id as DEPT_ID,
exp_data_cleanse.prcs_id as PRCS_ID,
exp_data_cleanse.edw_start_dttm as EDW_STRT_DTTM,
exp_data_cleanse.edw_end_dttm as EDW_END_DTTM,
exp_data_cleanse.PLCY_TYPE as PLCY_TYPE,
exp_data_cleanse.PROD_PLAN as PROD_PLAN,
0 as UPDATE_STRATEGY_ACTION
FROM
exp_data_cleanse
);


-- Component agg_gw_premium_info, Type TARGET 
INSERT INTO DB_T_PROD_COMN.AGG_GW_PREMIUM_INFO
(
MO_ID,
CO_CD,
ST_CD,
SRVC_CTR_NUM,
AGNT_NUM,
RSRVG_GRP_CD,
LOB_CD,
PROD_CD,
PLCY_TYPE,
PROD_DESC,
PLCY_NUM,
PLCY_EFF_DT,
PLCY_EXPN_DT,
ACCNTG_MO,
ACCNTG_YR,
CAT,
POOL_CD,
POOL_DESC,
MNTARY_AMT,
LEDGR,
GL_ACCT_NUM,
GL_ACCT_DESC,
GL_ACCT_GRP,
PLCY_TERM,
TOT_PREM_AMT,
DEPT_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
PROD_PLAN
)
SELECT
UPD_INSERT.MO_ID as MO_ID,
UPD_INSERT.CO_CD as CO_CD,
UPD_INSERT.ST_CD as ST_CD,
UPD_INSERT.SRVC_CTR_NUM as SRVC_CTR_NUM,
UPD_INSERT.AGNT_NUM as AGNT_NUM,
UPD_INSERT.RSRVG_GRP_CD as RSRVG_GRP_CD,
UPD_INSERT.LOB_CD as LOB_CD,
UPD_INSERT.PROD_CD as PROD_CD,
UPD_INSERT.PLCY_TYPE as PLCY_TYPE,
UPD_INSERT.PROD_DESC as PROD_DESC,
UPD_INSERT.PLCY_NUM as PLCY_NUM,
UPD_INSERT.PLCY_EFF_DT as PLCY_EFF_DT,
UPD_INSERT.PLCY_EXPN_DT as PLCY_EXPN_DT,
UPD_INSERT.ACCNTG_MO as ACCNTG_MO,
UPD_INSERT.ACCNTG_YR as ACCNTG_YR,
UPD_INSERT.CAT as CAT,
UPD_INSERT.POOL_CD as POOL_CD,
UPD_INSERT.POOL_DESC as POOL_DESC,
UPD_INSERT.MNTARY_AMT as MNTARY_AMT,
UPD_INSERT.LEDGR as LEDGR,
UPD_INSERT.GL_ACCT_NUM as GL_ACCT_NUM,
UPD_INSERT.GL_ACCT_DESC as GL_ACCT_DESC,
UPD_INSERT.GL_ACCT_GRP as GL_ACCT_GRP,
UPD_INSERT.PLCY_TERM as PLCY_TERM,
UPD_INSERT.TOT_PREM_AMT as TOT_PREM_AMT,
UPD_INSERT.DEPT_ID as DEPT_ID,
UPD_INSERT.PRCS_ID as PRCS_ID,
UPD_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
UPD_INSERT.EDW_END_DTTM as EDW_END_DTTM,
UPD_INSERT.PROD_PLAN as PROD_PLAN
FROM
UPD_INSERT;


-- PIPELINE END FOR 2

END; ';