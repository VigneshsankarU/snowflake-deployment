-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_FEAT_PERIL_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as FixedID,
$3 as Asset_sbtype,
$4 as Asset_Classification_code,
$5 as ASSET_CNTRCT_ROLE_SBTYPE_CD,
$6 as Feat_NKsrckey,
$7 as Inscrn_Mtrc_Type_CD,
$8 as Peril_type,
$9 as Amount,
$10 as Section_type,
$11 as Earnings_as_of_dt,
$12 as TRANS_STRT_DTTM,
$13 as TRANS_END_DTTM,
$14 as cury_cd,
$15 as in_AGMT_ID,
$16 as in_PRTY_ASSET_ID,
$17 as in_FEAT_ID,
$18 as LKP_AGMT_ID,
$19 as LKP_FEAT_ID,
$20 as LKP_PRTY_ASSET_ID,
$21 as LKP_RTG_PERIL_TYPE_CD,
$22 as LKP_AGMT_ASSET_FEAT_STRT_DTTM,
$23 as LKP_PLCY_SECTN_TYPE_CD,
$24 as LKP_LKP_INSRNC_MTRC_TYPE_CD,
$25 as LKP_AGMT_ASSET_FEAT_PERIL_AMT,
$26 as LKP_EDW_STRT_DTTM,
$27 as LKP_EDW_END_DTTM,
$28 as SOURCE_DATA,
$29 as TARGET_DATA,
$30 as FLAG,
$31 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  DISTINCT

main.PublicID_stg

,main.asset_host_id

,main.PRTY_ASSET_SBTYPE_CD

,main.PRTY_ASSET_CLASFCN_CD

,main.ASSET_CNTRCT_ROLE_SBTYPE_CD

, NULL AS Cov_Type_CD

,main.INSRNC_MTRC_TYPE_cd

,main.RTG_PERIL_TYPE_CD

,main.AGMT_ASSET_FEAT_PERIL_AMT

,''UNK'' AS PLCY_SECTN_TYPE_CD

,main.AGMT_ASSET_FEAT_STRT_DTTM

,main.TRANS_STRT_DTTM

,CAST(NULL AS TIMESTAMP) AS TRANS_END_DTTM

,main.CURY_CD

,main.AGMT_ID

,main.PRTY_ASSET_ID

,main.FEAT_ID

,TGT_lkp.AGMT_ID AS LKP_AGMT_ID

,TGT_lkp.FEAT_ID AS LKP_FEAT_ID

,TGT_lkp.PRTY_ASSET_ID AS LKP_PRTY_ASSET_ID ,

TGT_lkp.RTG_PERIL_TYPE_CD AS LKP_RTG_PERIL_TYPE_CD,

TGT_lkp.AGMT_ASSET_FEAT_STRT_DTTM AS LKP_AGMT_ASSET_FEAT_STRT_DTTM,

TGT_lkp.PLCY_SECTN_TYPE_CD AS LKP_PLCY_SECTN_TYPE_CD,

TGT_lkp.INSRNC_MTRC_TYPE_CD AS LKP_INSRNC_MTRC_TYPE_CD,

TGT_lkp.AGMT_ASSET_FEAT_PERIL_AMT AS LKP_AGMT_ASSET_FEAT_PERIL_AMT,

TGT_lkp.EDW_STRT_DTTM AS LKP_EDW_STRT_DTTM,

TGT_lkp.EDW_END_DTTM AS LKP_EDW_END_DTTM ,

Cast(Trim(Cast(main.AGMT_ASSET_FEAT_PERIL_AMT AS VARCHAR(100)))|| Trim(To_Char(main.AGMT_ASSET_FEAT_STRT_DTTM))  AS VARCHAR(100)) AS SOURCEDATA,

/* TARGETMD5DATA */
Cast(Trim(Cast(Cast(TGT_lkp.AGMT_ASSET_FEAT_PERIL_AMT AS DECIMAL(18,2)) AS VARCHAR(100)))||Trim(To_Char(TGT_lkp.AGMT_ASSET_FEAT_STRT_DTTM)) AS VARCHAR(100)) AS TARGETDATA,



/* FLAG */


CASE WHEN TARGETDATA IS NULL  THEN ''I'' WHEN SOURCEDATA <> TARGETDATA  THEN ''U'' ELSE ''R'' END AS INS_UPD_FLAG 



FROM

(

SELECT  DISTINCT

src.PublicID_stg

,src.asset_host_id

,XLAT_ASSET_SBTYPE_CD.TGT_IDNTFTN_VAL AS PRTY_ASSET_SBTYPE_CD

,XLAT_ASSET_CLASFCN_CD.TGT_IDNTFTN_VAL AS  PRTY_ASSET_CLASFCN_CD

,CNTRCT_ROLE.TGT_IDNTFTN_VAL  AS ASSET_CNTRCT_ROLE_SBTYPE_CD

, NULL AS Cov_Type_CD

,INSRNC_MTRC_TYPE.TGT_IDNTFTN_VAL AS INSRNC_MTRC_TYPE_cd

,RTG_PERIL.TGT_IDNTFTN_VAL AS RTG_PERIL_TYPE_CD

,src.Amount_stg AS AGMT_ASSET_FEAT_PERIL_AMT

,''UNK'' AS PLCY_SECTN_TYPE_CD

,src.EditEffectiveDate_stg AS AGMT_ASSET_FEAT_STRT_DTTM

,SRC.updateTime_stg AS TRANS_STRT_DTTM

,CAST(NULL AS TIMESTAMP) AS TRANS_END_DTTM

,XLAT_CURY_CD.TGT_IDNTFTN_VAL AS CURY_CD

,LKP_AGMT_PPV.AGMT_ID

,PRTY_ASSET.PRTY_ASSET_ID

,LKP_FEAT.FEAT_ID

FROM

(

SELECT DISTINCT plcy.PublicID_stg

, clause.PatternID_stg

,Cast(dhoe.FixedID_stg AS VARCHAR(50) ) asset_host_id

,''PRTY_ASSET_SBTYPE5'' AS PRTY_ASSET_SBTYPE_CD

,''PRTY_ASSET_CLASFCN1'' AS PRTY_ASSET_CLASFCN_CD

, peril.TypeCode_stg

,plcy.EditEffectiveDate_stg

,Sum(trans.Amount_stg) AS Amount_stg

,plcy.updateTime_stg

FROM DB_T_PROD_STAG.pc_policyperiod  plcy

JOIN DB_T_PROD_STAG.pcx_hotransaction_hoe trans ON trans.BranchID_stg = plcy.ID_stg

AND (trans.ExpirationDate_stg IS NULL OR trans.ExpirationDate_stg > plcy.EditEffectiveDate_stg)

 JOIN DB_T_PROD_STAG.pctl_policyperiodstatus status ON plcy.Status_stg=status.id_stg

/*  */
 INNER  JOIN DB_T_PROD_STAG.pc_job jobi ON plcy.JobID_stg=jobi.ID_stg

 INNER JOIN  DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=jobi.Subtype_stg

/*  */
JOIN DB_T_PROD_STAG.pcx_homeownerscost_hoe hocost ON hocost.ID_stg = trans.HomeownersCost_stg

JOIN DB_T_PROD_STAG.pctl_periltype_alfa peril ON hocost.PerilType_alfa_stg = peril.ID_stg

LEFT JOIN DB_T_PROD_STAG.pcx_dwellingcov_hoe dwel ON dwel.ID_stg =  hocost.DwellingCov_stg

AND (dwel.ExpirationDate_stg IS NULL OR dwel.ExpirationDate_stg > plcy.EditEffectiveDate_stg) 

LEFT JOIN DB_T_PROD_STAG.pcx_dwelling_hoe dhoe ON dhoe.ID_stg = dwel.Dwelling_stg

LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern clause ON clause.PatternID_stg = dwel.PatternCode_stg

LEFT JOIN  DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa itemcov ON itemcov.ID_stg =  hocost.SchedItemCov_stg

AND (itemcov.ExpirationDate_stg IS NULL OR itemcov.ExpirationDate_stg > plcy.EditEffectiveDate_stg)

/*  */
LEFT  JOIN  DB_T_PROD_STAG.pc_effectivedatedfields ON pc_effectivedatedfields.branchid_stg=plcy.id_stg

WHERE hocost.DwellingCov_stg IS NOT NULL

AND status.typecode_stg=''Bound''

AND pc_effectivedatedfields.ExpirationDate_stg IS NULL

AND plcy.PolicyNumber_stg IS NOT NULL 

and dhoe.FixedID_stg is not null

AND plcy.UpdateTime_stg > (:start_dttm)

	AND  plcy.UpdateTime_stg <= (:end_dttm)

	GROUP BY plcy.PublicID_stg,

clause.PatternID_stg,

asset_host_id,

PRTY_ASSET_SBTYPE_CD,

PRTY_ASSET_CLASFCN_CD,

peril.TypeCode_stg,

plcy.EditEffectiveDate_stg,

plcy.updateTime_stg



UNION



SELECT DISTINCT plcy.PublicID_stg

, clause.PatternID_stg

, Cast(covitem.FixedID_stg AS VARCHAR(50) )AS asset_host_id

,CASE WHEN (clause.PatternID_stg LIKE ''%ScheduledProperty%'' OR clause.PatternID_stg LIKE ''HOLI%'') THEN  ''PRTY_ASSET_SBTYPE7''

WHEN (clause.PatternID_stg LIKE ''%SpecificOtherStructure%'' OR clause.PatternID_stg LIKE ''HODW%'' OR clause.PatternID_stg LIKE ''HOSI%'') THEN ''PRTY_ASSET_SBTYPE5''

ELSE NULL END AS PRTY_ASSET_SBTYPE_CD

,CASE WHEN PRTY_ASSET_SBTYPE_CD IS NULL THEN ''UNK'' WHEN PRTY_ASSET_SBTYPE_CD=''PRTY_ASSET_SBTYPE5'' THEN ''PRTY_ASSET_CLASFCN1'' ELSE itemcov.ChoiceTerm1_stg end AS PRTY_ASSET_CLASFCN_CD

, peril.TypeCode_stg

,plcy.EditEffectiveDate_stg

,Sum(trans.Amount_stg) AS Amount_stg

,plcy.updateTime_stg

FROM DB_T_PROD_STAG.pc_policyperiod  plcy

JOIN DB_T_PROD_STAG.pcx_hotransaction_hoe trans ON trans.BranchID_stg = plcy.ID_stg

AND (trans.ExpirationDate_stg IS NULL OR trans.ExpirationDate_stg > plcy.EditEffectiveDate_stg)

 JOIN DB_T_PROD_STAG.pctl_policyperiodstatus status ON plcy.Status_stg=status.id_stg

JOIN DB_T_PROD_STAG.pcx_homeownerscost_hoe hocost ON hocost.ID_stg = trans.HomeownersCost_stg

JOIN DB_T_PROD_STAG.pctl_periltype_alfa peril ON hocost.PerilType_alfa_stg = peril.ID_stg

LEFT JOIN  DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa itemcov ON itemcov.ID_stg =  hocost.SchedItemCov_stg

AND (itemcov.ExpirationDate_stg IS NULL OR itemcov.ExpirationDate_stg> plcy.EditEffectiveDate_stg)

LEFT JOIN DB_T_PROD_STAG.pcx_holineschedcovitem_alfa covitem ON covitem.ID_stg = itemcov.HOLineSchCovItem_stg

LEFT JOIN DB_T_PROD_STAG.pc_etlclausepattern clause ON clause.PatternID_stg = itemcov.PatternCode_stg

WHERE hocost.SchedItemCov_stg IS NOT NULL

AND status.typecode_stg=''Bound''

AND covitem.FixedID_stg IS NOT NULL 

AND plcy.UpdateTime_stg > (:start_dttm)

	AND  plcy.UpdateTime_stg <= (:end_dttm)

		GROUP BY plcy.PublicID_stg,

clause.PatternID_stg,

asset_host_id,

PRTY_ASSET_SBTYPE_CD,

PRTY_ASSET_CLASFCN_CD,

peril.TypeCode_stg,

plcy.EditEffectiveDate_stg,

plcy.updateTime_stg

/* ---------- */
)SRC



/* PRTY_ASSET_SBTYPE_CD-- */
LEFT JOIN 

(

SELECT TGT_IDNTFTN_VAL,SRC_IDNTFTN_VAL

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TGT_IDNTFTN_NM = ''PRTY_ASSET_SBTYPE'' 

)XLAT_ASSET_SBTYPE_CD

ON

XLAT_ASSET_SBTYPE_CD.SRC_IDNTFTN_VAL = SRC.PRTY_ASSET_SBTYPE_CD



/* PRTY_ASSET_CLASFCN_CD-- */
LEFT JOIN 

(

SELECT TGT_IDNTFTN_VAL,SRC_IDNTFTN_VAL

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TGT_IDNTFTN_NM = ''PRTY_ASSET_CLASFCN''

)XLAT_ASSET_CLASFCN_CD

ON XLAT_ASSET_CLASFCN_CD.SRC_IDNTFTN_VAL = SRC.PRTY_ASSET_CLASFCN_CD



/* -DB_T_PROD_CORE.AGMT JOIN-- */
LEFT OUTER JOIN(

SELECT	AGMT.AGMT_ID AS AGMT_ID,AGMT.NK_SRC_KEY AS NK_SRC_KEY, AGMT.AGMT_TYPE_CD AS AGMT_TYPE_CD 

FROM	DB_T_PROD_CORE.AGMT 

QUALIFY	Row_Number() Over(

PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  

ORDER BY AGMT.EDW_END_DTTM DESC) = 1)LKP_AGMT_PPV

ON LKP_AGMT_PPV.NK_SRC_KEY= SRC.PublicID_stg

AND LKP_AGMT_PPV.AGMT_TYPE_CD=''PPV''





/* --feat_id--- */
LEFT OUTER JOIN(

	SELECT	FEAT.FEAT_ID AS FEAT_ID,

 		FEAT.FEAT_SBTYPE_CD AS FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY AS NK_SRC_KEY 

FROM	DB_T_PROD_CORE.FEAT

QUALIFY	Row_Number () Over (

PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  

ORDER BY edw_end_dttm DESC)=1) LKP_FEAT

ON LKP_FEAT.NK_SRC_KEY  = SRC.PatternID_stg 



/* ---PRTY_aSSET--- */
LEFT JOIN (

SELECT PRTY_ASSET_ID,ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD

FROM DB_T_PROD_CORE.PRTY_ASSET

WHERE EDW_END_DTTM = ''9999-12-31 23:59:59.999999''

) PRTY_ASSET

ON PRTY_ASSET.PRTY_ASSET_SBTYPE_CD = XLAT_ASSET_SBTYPE_CD.TGT_IDNTFTN_VAL 

AND PRTY_ASSET.PRTY_ASSET_CLASFCN_CD = XLAT_ASSET_CLASFCN_CD.TGT_IDNTFTN_VAL

AND  PRTY_ASSET.ASSET_HOST_ID_VAL = src.asset_host_id







/* --CNTRCT_ROLE--- */
LEFT JOIN (

SELECT TGT_IDNTFTN_VAL,TGT_IDNTFTN_NM

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''

)CNTRCT_ROLE

ON CNTRCT_ROLE.TGT_IDNTFTN_NM = ''ASSET_CNTRCT_ROLE_SBTYPE''



/* -RTG_PERIL_TYPE--- */
LEFT JOIN (

SELECT TGT_IDNTFTN_VAL,SRC_IDNTFTN_VAL

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE TGT_IDNTFTN_NM = ''RTG_PERIL_TYPE''

)RTG_PERIL

ON RTG_PERIL.SRC_IDNTFTN_VAL = src.TypeCode_stg



/**************   XLAT_CURY_CD ********************/



LEFT OUTER JOIN

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''ISO_CURY_TYPE''

         		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_CURY_CD

ON XLAT_CURY_CD.SRC_IDNTFTN_VAL=''ISO_CURY_TYPE1'' 





/* -Inscrn_Mtrc_Type_CD-- */


LEFT OUTER JOIN

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 



	WHERE TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''INSRNC_MTRC_TYPE''

         		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

) INSRNC_MTRC_TYPE

ON INSRNC_MTRC_TYPE.SRC_IDNTFTN_VAL=''INSRNC_MTRC_TYPE16''



)main

/* ------------------  TARGET LOOKUP----------------------------- */
LEFT OUTER JOIN(

SELECT	 AGMT_ASSET_FEAT_PERIL_MTRC.AGMT_ASSET_FEAT_STRT_DTTM AS AGMT_ASSET_FEAT_STRT_DTTM,

		AGMT_ASSET_FEAT_PERIL_MTRC.PLCY_SECTN_TYPE_CD AS PLCY_SECTN_TYPE_CD,

		AGMT_ASSET_FEAT_PERIL_MTRC.AGMT_ASSET_FEAT_PERIL_AMT AS AGMT_ASSET_FEAT_PERIL_AMT,

		  AGMT_ASSET_FEAT_PERIL_MTRC.EDW_STRT_DTTM AS EDW_STRT_DTTM,

		AGMT_ASSET_FEAT_PERIL_MTRC.EDW_END_DTTM AS EDW_END_DTTM, AGMT_ASSET_FEAT_PERIL_MTRC.INSRNC_MTRC_TYPE_CD AS INSRNC_MTRC_TYPE_CD,

		AGMT_ASSET_FEAT_PERIL_MTRC.RTG_PERIL_TYPE_CD AS RTG_PERIL_TYPE_CD,

		AGMT_ASSET_FEAT_PERIL_MTRC.FEAT_ID AS FEAT_ID, AGMT_ASSET_FEAT_PERIL_MTRC.AGMT_ID AS AGMT_ID,

		AGMT_ASSET_FEAT_PERIL_MTRC.PRTY_ASSET_ID AS PRTY_ASSET_ID 

FROM	DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL_MTRC 

/* QUALIFY	ROW_NUMBER() OVER(PARTITION BY   INSRNC_MTRC_TYPE_CD,RTG_PERIL_TYPE_CD,FEAT_ID,AGMT_ID,		PRTY_ASSET_ID ORDER BY AGMT_ASSET_FEAT_PERIL_MTRC.EDW_END_DTTM DESC) = 1 */
	WHERE Cast(EDW_END_DTTM AS DATE)=''9999-12-31''



) TGT_LKP

ON TGT_lkp.INSRNC_MTRC_TYPE_CD=main.INSRNC_MTRC_TYPE_cd

AND TGT_lkp.RTG_PERIL_TYPE_CD=main.RTG_PERIL_TYPE_CD

AND TGT_lkp.FEAT_ID=main.FEAT_ID

AND TGT_lkp.AGMT_ID=main.AGMT_ID

AND TGT_lkp.PRTY_ASSET_ID=main.PRTY_ASSET_ID
) SRC
)
);


-- Component exp_pass_frm_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_frm_src AS
(
SELECT
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Earnings_as_of_dt as Earnings_as_of_dt1,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Section_type as Section_type,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Inscrn_Mtrc_Type_CD as Inscrn_Mtrc_Type_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Amount as Amount,
CASE WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_STRT_DTTM END as TRANS_STRT_DTTM1,
CASE WHEN SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM IS NULL THEN to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd hh24:mi:ss.ff6'' ) ELSE SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.TRANS_END_DTTM END as TRANS_END_DTTM1,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Peril_type as in_RTG_PERIL_TYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.Earnings_as_of_dt as Earnings_as_of_dt,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.cury_cd as cury_cd,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.in_AGMT_ID as in_AGMT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.in_FEAT_ID as in_FEAT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_AGMT_ID as LKP_AGMT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_FEAT_ID as LKP_FEAT_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_PRTY_ASSET_ID as LKP_PRTY_ASSET_ID,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_RTG_PERIL_TYPE_CD as LKP_RTG_PERIL_TYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_AGMT_ASSET_FEAT_STRT_DTTM as LKP_AGMT_ASSET_FEAT_STRT_DTTM,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_PLCY_SECTN_TYPE_CD as LKP_PLCY_SECTN_TYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_LKP_INSRNC_MTRC_TYPE_CD as LKP_LKP_INSRNC_MTRC_TYPE_CD,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_AGMT_ASSET_FEAT_PERIL_AMT as LKP_AGMT_ASSET_FEAT_PERIL_AMT,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.FLAG as FLAG,
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x.source_record_id
FROM
SQ_pc_AgmtQuotn_ast_ft_prl_mtr_x
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
exp_pass_frm_src.LKP_AGMT_ID as lkp_AGMT_ID,
exp_pass_frm_src.LKP_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_pass_frm_src.LKP_FEAT_ID as lkp_FEAT_ID,
exp_pass_frm_src.LKP_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_pass_frm_src.LKP_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_pass_frm_src.LKP_LKP_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
exp_pass_frm_src.LKP_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
MD5 ( exp_pass_frm_src.LKP_PLCY_SECTN_TYPE_CD || exp_pass_frm_src.LKP_AGMT_ASSET_FEAT_PERIL_AMT || TO_CHAR ( exp_pass_frm_src.LKP_AGMT_ASSET_FEAT_STRT_DTTM ) ) as lkp_checksum,
exp_pass_frm_src.in_FEAT_ID as in_FEAT_ID,
exp_pass_frm_src.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_pass_frm_src.in_AGMT_ID as in_AGMT_ID,
exp_pass_frm_src.Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_pass_frm_src.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_pass_frm_src.ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_frm_src.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_pass_frm_src.Section_type as Section_type,
exp_pass_frm_src.Earnings_as_of_dt as Earnings_as_of_dt,
exp_pass_frm_src.Amount as Amount,
MD5 ( exp_pass_frm_src.Section_type || exp_pass_frm_src.Amount || TO_CHAR ( exp_pass_frm_src.Earnings_as_of_dt ) ) as in_checksum,
exp_pass_frm_src.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_frm_src.TRANS_END_DTTM1 as TRANS_END_DTTM,
exp_pass_frm_src.FLAG as ins_upd_flag,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) as AGMT_ASSET_STRT_DTTM,
:PRCS_ID as PRCS_ID,
exp_pass_frm_src.cury_cd as TGT_IDNTFTN_VAL,
exp_pass_frm_src.source_record_id
FROM
exp_pass_frm_src
);


-- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table RTRTRANS_INSERT as
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''I'';


-- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
create or replace temporary table RTRTRANS_UPDATE as
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''U'';


-- Component exp_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_upd AS
(
SELECT
RTRTRANS_UPDATE.lkp_AGMT_ID as lkp_AGMT_ID3,
RTRTRANS_UPDATE.lkp_FEAT_ID as lkp_FEAT_ID3,
RTRTRANS_UPDATE.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID3,
RTRTRANS_UPDATE.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD3,
RTRTRANS_UPDATE.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD3,
RTRTRANS_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
DATEADD(''second'', - 1, RTRTRANS_UPDATE.EDW_STRT_DTTM) as EDW_END_DTTM,
DATEADD(''second'', - 1, RTRTRANS_UPDATE.TRANS_STRT_DTTM) as TRANS_END_DTTM,
RTRTRANS_UPDATE.lkp_AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DTTM3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component AGMT_ASSET_FEAT_PERIL_MTRC_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL_MTRC
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
RTG_PERIL_TYPE_CD,
AGMT_ASSET_FEAT_STRT_DTTM,
AAF_PERIL_STRT_DTTM,
PLCY_SECTN_TYPE_CD,
INSRNC_MTRC_TYPE_CD,
AAF_PERIL_MTRC_DTTM,
AGMT_ASSET_FEAT_PERIL_AMT,
CURY_CD,
AAF_PERIL_MTRC_STRT_DTTM,
AAF_PERIL_MTRC_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
RTRTRANS_INSERT.in_AGMT_ID as AGMT_ID,
RTRTRANS_INSERT.in_FEAT_ID as FEAT_ID,
RTRTRANS_INSERT.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_INSERT.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
RTRTRANS_INSERT.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_INSERT.Earnings_as_of_dt4 as AGMT_ASSET_FEAT_STRT_DTTM,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as AAF_PERIL_STRT_DTTM,
RTRTRANS_INSERT.Section_type as PLCY_SECTN_TYPE_CD,
RTRTRANS_INSERT.in_Inscrn_Mtrc_Type_CD as INSRNC_MTRC_TYPE_CD,
RTRTRANS_INSERT.EDW_END_DTTM as AAF_PERIL_MTRC_DTTM,
RTRTRANS_INSERT.Amount as AGMT_ASSET_FEAT_PERIL_AMT,
RTRTRANS_INSERT.TGT_IDNTFTN_VAL as CURY_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as AAF_PERIL_MTRC_STRT_DTTM,
RTRTRANS_INSERT.EDW_END_DTTM as AAF_PERIL_MTRC_END_DTTM,
RTRTRANS_INSERT.PRCS_ID as PRCS_ID,
RTRTRANS_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_INSERT.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
RTRTRANS_INSERT.TRANS_END_DTTM as TRANS_END_DTTM
FROM
RTRTRANS_INSERT;


-- Component exp_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins AS
(
SELECT
RTRTRANS_UPDATE.in_AGMT_ID as AGMT_ID,
RTRTRANS_UPDATE.in_FEAT_ID as FEAT_ID,
RTRTRANS_UPDATE.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_UPDATE.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt,
RTRTRANS_UPDATE.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt1,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt2,
RTRTRANS_UPDATE.Section_type as AGMT_SECTN_CD,
RTRTRANS_UPDATE.in_Inscrn_Mtrc_Type_CD as INSRNC_MTRC_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt as Earnings_as_of_dt3,
RTRTRANS_UPDATE.Amount as AGMT_ASSET_FEAT_PERIL_AMT,
RTRTRANS_UPDATE.PRCS_ID as PRCS_ID,
RTRTRANS_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_UPDATE.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_UPDATE.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
RTRTRANS_UPDATE.TRANS_END_DTTM as TRANS_END_DTTM,
RTRTRANS_UPDATE.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM3,
RTRTRANS_UPDATE.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_ins.AGMT_ID as AGMT_ID,
exp_ins.FEAT_ID as FEAT_ID,
exp_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
exp_ins.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_ins.Earnings_as_of_dt2 as Earnings_as_of_dt2,
exp_ins.AGMT_SECTN_CD as AGMT_SECTN_CD,
exp_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins.Earnings_as_of_dt3 as Earnings_as_of_dt3,
exp_ins.AGMT_ASSET_FEAT_PERIL_AMT as AGMT_ASSET_FEAT_PERIL_AMT,
exp_ins.PRCS_ID as PRCS_ID,
exp_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins.AGMT_ASSET_STRT_DTTM3 as AGMT_ASSET_STRT_DTTM3,
exp_ins.TGT_IDNTFTN_VAL3 as TGT_IDNTFTN_VAL3,
0 as UPDATE_STRATEGY_ACTION
FROM
exp_ins
);


-- Component update_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE update_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_upd.lkp_AGMT_ID3 as lkp_AGMT_ID3,
exp_upd.lkp_FEAT_ID3 as lkp_FEAT_ID3,
exp_upd.lkp_PRTY_ASSET_ID3 as lkp_PRTY_ASSET_ID3,
exp_upd.lkp_RTG_PERIL_TYPE_CD3 as lkp_RTG_PERIL_TYPE_CD3,
exp_upd.lkp_INSRNC_MTRC_TYPE_CD3 as lkp_INSRNC_MTRC_TYPE_CD3,
exp_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
exp_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_upd.lkp_AGMT_ASSET_FEAT_STRT_DTTM3 as lkp_AGMT_ASSET_FEAT_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_upd
);


-- Component AGMT_ASSET_FEAT_PERIL_MTRC_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL_MTRC
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
RTG_PERIL_TYPE_CD,
AGMT_ASSET_FEAT_STRT_DTTM,
AAF_PERIL_STRT_DTTM,
PLCY_SECTN_TYPE_CD,
INSRNC_MTRC_TYPE_CD,
AAF_PERIL_MTRC_DTTM,
AGMT_ASSET_FEAT_PERIL_AMT,
CURY_CD,
AAF_PERIL_MTRC_STRT_DTTM,
AAF_PERIL_MTRC_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
upd_ins.AGMT_ID as AGMT_ID,
upd_ins.FEAT_ID as FEAT_ID,
upd_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as AGMT_ASSET_STRT_DTTM,
upd_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
upd_ins.Earnings_as_of_dt1 as AGMT_ASSET_FEAT_STRT_DTTM,
upd_ins.AGMT_ASSET_STRT_DTTM3 as AAF_PERIL_STRT_DTTM,
upd_ins.AGMT_SECTN_CD as PLCY_SECTN_TYPE_CD,
upd_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
upd_ins.EDW_END_DTTM as AAF_PERIL_MTRC_DTTM,
upd_ins.AGMT_ASSET_FEAT_PERIL_AMT as AGMT_ASSET_FEAT_PERIL_AMT,
upd_ins.TGT_IDNTFTN_VAL3 as CURY_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as AAF_PERIL_MTRC_STRT_DTTM,
upd_ins.EDW_END_DTTM as AAF_PERIL_MTRC_END_DTTM,
upd_ins.PRCS_ID as PRCS_ID,
upd_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins.EDW_END_DTTM as EDW_END_DTTM,
upd_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
upd_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
upd_ins;


-- Component AGMT_ASSET_FEAT_PERIL_MTRC_upd_upd, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.AGMT_ASSET_FEAT_PERIL_MTRC
USING update_upd ON (UPDATE_STRATEGY_ACTION = 1 AND AGMT_ASSET_FEAT_PERIL_MTRC.AGMT_ID = update_upd.lkp_AGMT_ID3 AND AGMT_ASSET_FEAT_PERIL_MTRC.FEAT_ID = update_upd.lkp_FEAT_ID3 AND AGMT_ASSET_FEAT_PERIL_MTRC.PRTY_ASSET_ID = update_upd.lkp_PRTY_ASSET_ID3 AND AGMT_ASSET_FEAT_PERIL_MTRC.RTG_PERIL_TYPE_CD = update_upd.lkp_RTG_PERIL_TYPE_CD3 AND AGMT_ASSET_FEAT_PERIL_MTRC.INSRNC_MTRC_TYPE_CD = update_upd.lkp_INSRNC_MTRC_TYPE_CD3 AND AGMT_ASSET_FEAT_PERIL_MTRC.EDW_STRT_DTTM = update_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = update_upd.EDW_END_DTTM,
TRANS_END_DTTM = update_upd.TRANS_END_DTTM
;


END; ';