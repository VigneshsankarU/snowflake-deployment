-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_SCR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;
  P_DEFAULT_STR_CD STRING;

BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);


-- Component LKP_INSRNC_QUOTN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
);


-- Component LKP_RATING_SCR_CD_LKUP, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_RATING_SCR_CD_LKUP AS
(
SELECT
STATE,
COMPANY,
LOB,
SCORE_TYPE,
LEVEL_NAME,
LEVEL_MIN,
LEVEL_MAX
FROM DB_T_SHRD_PROD.RATING_SCR_CD_LKUP
);


-- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_agmt_quotn_scr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_agmt_quotn_scr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ModelName,
$2 as ModelRunDttm,
$3 as Score,
$4 as JobNumber,
$5 as branchnumber,
$6 as state,
$7 as UWCompany,
$8 as QUOTN_SRC_CD,
$9 as updatetime_pcx_palineratingfactor_alfa,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

	pc_agmt_quotn_scr_x.ModelName, 

	pc_agmt_quotn_scr_x.ModelRunDttm, 

	pc_agmt_quotn_scr_x.Score, 

	pc_agmt_quotn_scr_x.JobNumber, 

	pc_agmt_quotn_scr_x.branchnumber, 

	state, 

	pc_agmt_quotn_scr_x.UWCompany, 

	''SRC_SYS4'' as Agmt_src_cd ,

	updatetime_pcx_palineratingfactor_alfa

FROM

 	(

SELECT 

	 CAST(''ARS''as varchar(25)) MODELNAME

	,COALESCE(ARSScoreDateOutputPoli_stg,CAST(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6))) AS MODELRUNDTTM

/* ,CAST(COALESCE(ActualARSCalcValue_stg,0.0000) as varchar(10))AS SCORE */
	,CAST(COALESCE(ARSRatedScorePoli_stg,0.0000) as varchar(10))AS SCORE 

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,DB_T_PROD_STAG.pctl_jurisdictiON .TYPECODE_stg AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,pcx_palineratingfactor_alfa.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA ON pc_policyperiod.ID_stg=pcx_palineratingfactor_alfa.BranchID_stg

	JOIN DB_T_PROD_STAG.pctl_jurisdictiON ON pc_policyperiod.BaseState_stg=DB_T_PROD_STAG.pctl_jurisdictiON .ID_stg

	left outer join DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

/* pcx_palineratingfactor_alfa.ARSScoreDateOutputPoli_stg IS NOT NULL */
	PCX_PALINERATINGFACTOR_ALFA.ARSRATEDSCOREPOLI_STG IS NOT NULL 

	AND pctl_policyperiodstatus.typecode_stg <>''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	AND pcx_palineratingfactor_alfa.UpdateTime_stg> (:start_dttm) 

	and pcx_palineratingfactor_alfa.UpdateTime_stg <= (:end_dttm)



UNION 



SELECT

	CAST(''LVP'' as varchar(25)) MODELNAME

	,COALESCE(LVPScoreDateOutputPoli_stg,CAST(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6))) AS MODELRUNDTTM

	,CAST(COALESCE(LVPCalcscore_stg,0.0000) as varchar(10))AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,DB_T_PROD_STAG.pctl_jurisdictiON .TYPECODE_stg AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,pcx_palineratingfactor_alfa.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA ON pc_policyperiod.ID_stg=pcx_palineratingfactor_alfa.BranchID_stg

	JOIN DB_T_PROD_STAG.pctl_jurisdictiON ON pc_policyperiod.BaseState_stg=DB_T_PROD_STAG.pctl_jurisdictiON .ID_stg

		left outer join DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

/* pcx_palineratingfactor_alfa.LVPScoreDateOutputPoli_stg IS NOT NULL */
	PCX_PALINERATINGFACTOR_ALFA.LVPCALCSCORE_STG IS NOT NULL

	AND pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	AND pcx_palineratingfactor_alfa.UpdateTime_stg > (:start_dttm) 

	and pcx_palineratingfactor_alfa.UpdateTime_stg <= (:end_dttm)



UNION 



SELECT 

	CAST(''ISE'' as varchar(25)) as MODELNAME

	,COALESCE(pc_pamodifier.createtime_stg,cast (''1900-01-01 00:00:00.000000'' as TIMESTAMP(6))) AS MODELRUNDTTM

	,CAST(COALESCE(pc_pamodifier.ratemodifier_stg,0.0000) as varchar(10)) AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,pc_pamodifier.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_pamodifier on branchID_stg = pc_policyperiod.ID_stg /* and  PatternCode=''PAInsRatingScore_alfa'' */
	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg IN (''Submission'',''PolicyChange'',''Renewal'')

	AND pc_pamodifier.UpdateTime_stg > (:start_dttm) 

	AND pc_pamodifier.UpdateTime_stg <= (:end_dttm)

	

/*  Added below unions as part of EIM-19339 */
	

	UNION

	

	SELECT

	CAST(''PDT'' as varchar(25)) MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.PropertyDamageTier_stg,0.0000) as varchar(10)) AS SCORE

	,pc_job.JobNumber_stg as JobNumber

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

	AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

	

	UNION

	

SELECT

	CAST(''UNMT'' as varchar(25)) as MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.UninsuredMotoristTier_stg,0.0000) as varchar(10))AS SCORE

	,pc_job.JobNumber_stg as JobNumber

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

	AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

	

	UNION

	

	SELECT

	CAST(''COLLT'' as varchar(25)) as MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.CollisionTier_stg,0.0000) as VARCHAR(10)) AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    , r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

	AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

	UNION

	

	SELECT

	CAST(''SLT'' as varchar(25)) as MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.SingleLimitTier_stg,0.0000) as varchar(10))AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50))AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

    AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

	

	

	UNION

	

	SELECT

	CAST(''COMPT'' as varchar(25)) as MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,cast(COALESCE(r.ComprehensiveTier_stg,0.0000) as varchar(10)) AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

    AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

	

	UNION 

	

	SELECT

	CAST(''BIT'' as varchar(25))MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.BodilyInjuryTier_stg,0.0000) as varchar(10))AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg IN (''Submission'',''PolicyChange'',''Renewal'')

    AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	

UNION



	SELECT

	CAST(''MPT'' as varchar(25))MODELNAME

	,cast(NULL as TIMESTAMP(6))AS MODELRUNDTTM

	,CAST(COALESCE(r.MedicalPaymentsTier_stg,0.0000) as varchar(10))AS SCORE

	,pc_job.JobNumber_stg as JobNumber 

	,pc_policyperiod.branchnumber_stg as branchnumber

	,cast(NULL as varchar(50)) AS state

	,pctl_uwcompanycode.TYPECODE_stg AS UWCompany

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa

FROM  

	DB_T_PROD_STAG.pc_job

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.ID_stg = pc_policyperiod.JobID_stg

	LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff on eff.BranchID_stg = pc_policyperiod.ID_stg

	INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r on r.ID_stg=eff.RatingTierPPV2_alfa_stg	

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job on pctl_job.ID_stg=pc_job.Subtype_stg

WHERE

	pctl_policyperiodstatus.typecode_stg <> ''Temporary''

	and pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')

    AND r.UpdateTime_stg > (:start_dttm) 

	AND r.UpdateTime_stg <= (:end_dttm)

	)pc_agmt_quotn_scr_x

WHERE

	ModelName <> ''ISE''

/* and jobnumber like ''%7982'' */
	QUALIFY ROW_NUMBER() OVER(PARTITION BY 

	pc_agmt_quotn_scr_x.JobNumber, 

	pc_agmt_quotn_scr_x.branchnumber, 

	pc_agmt_quotn_scr_x.ModelName, 

	pc_agmt_quotn_scr_x.ModelRunDttm, 

	state, 

	pc_agmt_quotn_scr_x.UWCompany  ORDER BY updatetime_pcx_palineratingfactor_alfa desc, pc_agmt_quotn_scr_x.Score desc) = 1
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_agmt_quotn_scr_x.ModelName as ModelName,
SQ_pc_agmt_quotn_scr_x.ModelRunDttm as ModelRunDttm,
CASE WHEN SQ_pc_agmt_quotn_scr_x.Score IS NULL THEN ''NOSCORE'' ELSE SQ_pc_agmt_quotn_scr_x.Score END as out_Score,
TO_NUMBER(SQ_pc_agmt_quotn_scr_x.Score) as var_Score,
LKP_1.QUOTN_ID /* replaced lookup LKP_INSRNC_QUOTN */ as QUOTN_ID,
''APV'' as LOB,
LKP_2.LEVEL_NAME /* replaced lookup LKP_RATING_SCR_CD_LKUP */ as LVL_NUM,
SQ_pc_agmt_quotn_scr_x.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
SQ_pc_agmt_quotn_scr_x.source_record_id,
row_number() over (partition by SQ_pc_agmt_quotn_scr_x.source_record_id order by SQ_pc_agmt_quotn_scr_x.source_record_id) as RNK
FROM
SQ_pc_agmt_quotn_scr_x
LEFT JOIN LKP_INSRNC_QUOTN LKP_1 ON LKP_1.NK_JOB_NBR = SQ_pc_agmt_quotn_scr_x.JobNumber AND LKP_1.VERS_NBR = SQ_pc_agmt_quotn_scr_x.branchnumber
LEFT JOIN LKP_RATING_SCR_CD_LKUP LKP_2 ON LKP_2.STATE = SQ_pc_agmt_quotn_scr_x.state AND LKP_2.COMPANY = SQ_pc_agmt_quotn_scr_x.UWCompany AND LKP_2.LOB = LOB AND LKP_2.SCORE_TYPE = SQ_pc_agmt_quotn_scr_x.ModelName AND LKP_2.LEVEL_MIN <= var_Score AND LKP_2.LEVEL_MAX >= var_Score
QUALIFY RNK = 1
);


-- Component LKP_ANLTCL_MODL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ANLTCL_MODL AS
(
SELECT
LKP.MODL_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_NAME asc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT ANLTCL_MODL.MODL_ID as MODL_ID, ANLTCL_MODL.MODL_NAME as MODL_NAME FROM DB_T_PROD_CORE.ANLTCL_MODL ORDER BY MODL_FROM_DTTM desc/*  */
) LKP ON LKP.MODL_NAME = exp_pass_from_src.ModelName
QUALIFY RNK = 1
);


-- Component LKP_MODL_RUN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MODL_RUN AS
(
SELECT
LKP.MODL_ID,
LKP.MODL_RUN_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_RUN_ID asc,LKP.MODL_RUN_DTTM asc) RNK1
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
LEFT JOIN (
SELECT
MODL_ID,
MODL_RUN_ID,
MODL_RUN_DTTM
FROM DB_T_PROD_CORE.MODL_RUN
) LKP ON LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_DTTM = exp_pass_from_src.ModelRunDttm
QUALIFY RNK1 = 1
);


-- Component LKP_QUOTN_SCR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_SCR AS
(
SELECT
LKP.QUOTN_ID,
LKP.MODL_ID,
LKP.MODL_RUN_ID,
LKP.QUOTN_SCR_VAL,
LKP.LVL_NUM,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.QUOTN_ID desc,LKP.MODL_ID desc,LKP.MODL_RUN_ID desc,LKP.QUOTN_SCR_VAL desc,LKP.LVL_NUM desc) RNK1
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_MODL_RUN ON LKP_ANLTCL_MODL.source_record_id = LKP_MODL_RUN.source_record_id
LEFT JOIN (
SELECT QUOTN_SCR.QUOTN_SCR_VAL as QUOTN_SCR_VAL, QUOTN_SCR.LVL_NUM as LVL_NUM, QUOTN_SCR.QUOTN_ID as QUOTN_ID, QUOTN_SCR.MODL_ID as MODL_ID, QUOTN_SCR.MODL_RUN_ID as MODL_RUN_ID FROM DB_T_PROD_CORE.QUOTN_SCR
QUALIFY ROW_NUMBER() OVER(PARTITION BY  QUOTN_ID,MODL_ID,MODL_RUN_ID ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.QUOTN_ID = exp_pass_from_src.QUOTN_ID AND LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_ID = LKP_MODL_RUN.MODL_RUN_ID
QUALIFY RNK1 = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_QUOTN_SCR.MODL_ID as lkp_MODL_ID,
LKP_QUOTN_SCR.MODL_RUN_ID as lkp_MODL_RUN_ID,
LKP_QUOTN_SCR.QUOTN_ID as lkp_QUOTN_ID,
LKP_ANLTCL_MODL.MODL_ID as in_MODL_ID,
LKP_MODL_RUN.MODL_RUN_ID as in_MODL_RUN_ID,
exp_pass_from_src.QUOTN_ID as in_QUOTN_ID,
exp_pass_from_src.out_Score as in_QUOTN_SCR_VAL,
:PRCS_ID as out_PRCS_ID,
MD5 ( ltrim ( Rtrim ( LKP_QUOTN_SCR.QUOTN_SCR_VAL ) ) || ltrim ( Rtrim ( LKP_QUOTN_SCR.LVL_NUM ) ) ) as Checksum_lkp,
MD5 ( ltrim ( Rtrim ( exp_pass_from_src.out_Score ) ) || ltrim ( Rtrim ( exp_pass_from_src.LVL_NUM ) ) ) as Checksum_in,
CASE WHEN Checksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_pass_from_src.LVL_NUM as LVL_NUM,
exp_pass_from_src.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_MODL_RUN ON LKP_ANLTCL_MODL.source_record_id = LKP_MODL_RUN.source_record_id
INNER JOIN LKP_QUOTN_SCR ON LKP_MODL_RUN.source_record_id = LKP_QUOTN_SCR.source_record_id
);


-- Component rtr_quotn_scr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_quotn_scr_INSERT AS
(SELECT
exp_data_transformation.lkp_MODL_ID as lkp_MODL_ID,
exp_data_transformation.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
exp_data_transformation.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_data_transformation.in_MODL_ID as in_MODL_ID,
exp_data_transformation.in_MODL_RUN_ID as in_MODL_RUN_ID,
exp_data_transformation.in_QUOTN_ID as in_QUOTN_ID,
exp_data_transformation.in_QUOTN_SCR_VAL as in_QUOTN_SCR_VAL,
exp_data_transformation.out_PRCS_ID as in_PRCS_ID,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.CDC_Flag as CDC_Flag,
exp_data_transformation.LVL_NUM as LVL_NUM,
exp_data_transformation.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE ( exp_data_transformation.CDC_Flag = ''I'' or exp_data_transformation.CDC_Flag = ''U'' ) and exp_data_transformation.in_QUOTN_ID <> 9999 and exp_data_transformation.in_MODL_ID <> 9999 and exp_data_transformation.in_MODL_RUN_ID <> 9999);


-- Component upd_agmt_scr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_scr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_scr_INSERT.in_MODL_ID as MODL_ID,
rtr_quotn_scr_INSERT.in_MODL_RUN_ID as MODL_RUN_ID,
rtr_quotn_scr_INSERT.in_QUOTN_ID as QUOTN_ID,
rtr_quotn_scr_INSERT.in_QUOTN_SCR_VAL as QUOTN_SCR_VAL,
rtr_quotn_scr_INSERT.in_PRCS_ID as PRCS_ID,
rtr_quotn_scr_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_quotn_scr_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_quotn_scr_INSERT.LVL_NUM as LVL_NUM1,
rtr_quotn_scr_INSERT.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa1,
0 as UPDATE_STRATEGY_ACTION,
rtr_quotn_scr_INSERT.source_record_id
FROM
rtr_quotn_scr_INSERT
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_agmt_scr_ins.MODL_ID as MODL_ID,
upd_agmt_scr_ins.MODL_RUN_ID as MODL_RUN_ID,
upd_agmt_scr_ins.QUOTN_ID as QUOTN_ID,
upd_agmt_scr_ins.QUOTN_SCR_VAL as QUOTN_SCR_VAL,
upd_agmt_scr_ins.PRCS_ID as PRCS_ID,
upd_agmt_scr_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_agmt_scr_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
upd_agmt_scr_ins.LVL_NUM1 as LVL_NUM1,
upd_agmt_scr_ins.updatetime_pcx_palineratingfactor_alfa1 as updatetime_pcx_palineratingfactor_alfa1,
upd_agmt_scr_ins.source_record_id
FROM
upd_agmt_scr_ins
);


-- Component tgt_quotn_scr_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_SCR
(
QUOTN_ID,
MODL_ID,
MODL_RUN_ID,
QUOTN_SCR_VAL,
LVL_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_tgt_ins.MODL_ID as MODL_ID,
exp_pass_to_tgt_ins.MODL_RUN_ID as MODL_RUN_ID,
exp_pass_to_tgt_ins.QUOTN_SCR_VAL as QUOTN_SCR_VAL,
exp_pass_to_tgt_ins.LVL_NUM1 as LVL_NUM,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.updatetime_pcx_palineratingfactor_alfa1 as TRANS_STRT_DTTM,
exp_pass_to_tgt_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component tgt_quotn_scr_ins, Type Post SQL 
UPDATE DB_T_PROD_CORE.QUOTN_SCR FROM

(SELECT	distinct QUOTN_ID,MODL_ID,MODL_RUN_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID,MODL_ID,MODL_RUN_ID ORDER by TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID,MODL_ID,MODL_RUN_ID ORDER by TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM	DB_T_PROD_CORE.QUOTN_SCR

 ) a

set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

where  QUOTN_SCR.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_SCR.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_SCR.MODL_ID=A.MODL_ID

AND QUOTN_SCR.MODL_RUN_ID=A.MODL_RUN_ID

and lead1 is not null

and lead2 is not null;


END; 
';