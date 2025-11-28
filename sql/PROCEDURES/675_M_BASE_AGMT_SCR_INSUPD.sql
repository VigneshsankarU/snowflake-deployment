-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_SCR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare

p_agmt_type_cd_policy_version varchar;
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

 p_agmt_type_cd_policy_version:= (SELECT param_value FROM control_params where run_id = :run_id and lower(param_name)=''p_agmt_type_cd_policy_version'' order by insert_ts desc limit 1);


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
FROM db_t_shrd_prod.RATING_SCR_CD_LKUP
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
$4 as PartyAgreement,
$5 as state,
$6 as UWCompany,
$7 as updatetime_pcx_palineratingfactor_alfa,
$8 as AGMT_SRC_CD,
$9 as SCR_FCTR_RATE,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 	

    pc_agmt_quotn_scr_x.MODEL_NAME, 	

    pc_agmt_quotn_scr_x.MODEL_RUN_DTTM, 	

    CASE WHEN MODEL_NAME = ''ISE'' THEN NULL ELSE  pc_agmt_quotn_scr_x.Score  END AS Score ,	

    pc_agmt_quotn_scr_x.PARTY_AGREEMENT, 	

    state, 	

    pc_agmt_quotn_scr_x.UWCompany,	

      pc_agmt_quotn_scr_x.updatetime_pcx_palineratingfactor_alfa, 	

    ''SRC_SYS4'' AS Agmt_src_cd,	

    CASE WHEN MODEL_NAME = ''ISE'' THEN Score	

    ELSE  NULL 	

    END AS SCR_FCTR_RATE	

FROM	

    (	

    SELECT 	

    Cast(''ARS'' AS VARCHAR(10)) MODEL_NAME	

    ,Coalesce(Cast(ARSScoreDateOutputPoli_stg AS VARCHAR(30)),''1900-01-01 00:00:00.000000'') AS MODEL_RUN_DTTM	

/* Cast(Coalesce(ActualARSCalcValue_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
/* ActualARSCalcValue_stg AS SCORE	 */
     ,ARSRatedScorePoli_stg AS SCORE	

    ,cast(pc_policyperiod.PublicID_stg as varchar(100)) PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,pctl_jurisdictiON .TYPECODE_stg AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode,	

      pcx_palineratingfactor_alfa.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

( :start_dttm) AS start_dttm,	

( :end_dttm) AS end_dttm	, pcx_palineratingfactor_alfa.expirationdate_stg

FROM  	

     DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA ON pc_policyperiod.id_stg=pcx_palineratingfactor_alfa.BranchID_stg	

    JOIN DB_T_PROD_STAG.pctl_jurisdictiON ON pc_policyperiod.BaseState_stg=pctl_jurisdictiON .id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE  	

/* pcx_palineratingfactor_alfa.ARSScoreDateOutputPoli_stg IS NOT NULL */
	pcx_palineratingfactor_alfa.ARSRatedScorePoli_stg is not null

    AND pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	and (pcx_palineratingfactor_alfa.expirationdate_stg is null or pcx_palineratingfactor_alfa.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    AND pcx_palineratingfactor_alfa.UpdateTime_stg>  :start_dttm AND pcx_palineratingfactor_alfa.UpdateTime_stg <=  :end_dttm	

/* AND ActualARSCalcValue_stg is not null	 */
	

UNION 	

	

SELECT	

    Cast(''LVP'' AS VARCHAR(10)) MODEL_NAME	

    ,Coalesce(Cast(LVPScoreDateOutputPoli_stg AS VARCHAR(30)),''1900-01-01 00:00:00.000000'') AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(LVPCalcscore_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    LVPCalcscore_stg AS SCORE	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,pctl_jurisdictiON .TYPECODE_stg AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode,	

      pcx_palineratingfactor_alfa.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

( :start_dttm) AS start_dttm,	

( :end_dttm) AS end_dttm	, pcx_palineratingfactor_alfa.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA ON pc_policyperiod.id_stg=pcx_palineratingfactor_alfa.BranchID_stg	

    JOIN DB_T_PROD_STAG.pctl_jurisdictiON ON pc_policyperiod.BaseState_stg=pctl_jurisdictiON .id_stg	

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

/* pcx_palineratingfactor_alfa.LVPScoreDateOutputPoli_stg IS NOT NULL	 */
	PCX_PALINERATINGFACTOR_ALFA.LVPCALCSCORE_STG IS NOT NULL 

    AND pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	 and (pcx_palineratingfactor_alfa.expirationdate_stg is null or pcx_palineratingfactor_alfa.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    AND pcx_palineratingfactor_alfa.UpdateTime_stg > ( :start_dttm) 	

    AND pcx_palineratingfactor_alfa.UpdateTime_stg <= ( :end_dttm)	

   

UNION 	

	

SELECT	

    Cast(''ISE'' AS VARCHAR(10)) MODEL_NAME	

    ,Coalesce(Cast(pc_pamodifier.createtime_stg AS VARCHAR(30)),''1900-01-01 00:00:00.000000'') AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(pc_pamodifier.ratemodifier_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    pc_pamodifier.ratemodifier_stg as Score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode,	

      pc_pamodifier.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, pc_pamodifier.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_pamodifier ON branchID_stg = pc_policyperiod.ID_stg 

AND pc_pamodifier.PatternCode_stg=''PAInsRatingScore_alfa''	/* added		 */
    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND pc_pamodifier.UpdateTime_stg > ( :start_dttm) 	

    AND pc_pamodifier.UpdateTime_stg <= ( :end_dttm)	

    AND pc_pamodifier.ratemodifier_stg is not null	

/*  Added below unions as part of EIM-19339	 */
AND (pc_pamodifier.ExpirationDate_stg is NULL or pc_pamodifier.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)/* added */
    	

    UNION	

    	

    SELECT	

    Cast(''PDT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.PropertyDamageTier_stg,0.0000)  AS VARCHAR(10)) AS SCORE	 */
    r.PropertyDamageTier_stg as score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    , r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    AND r.PropertyDamageTier_stg is not Null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

    	

    UNION	

    	

    SELECT	

    Cast(''UNMT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.UninsuredMotoristTier_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    r.UninsuredMotoristTier_stg as score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg<>''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    and r.UninsuredMotoristTier_stg is not null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

    	

    UNION	

    	

    SELECT	

    Cast(''COLLT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.CollisionTier_stg,0.0000) AS VARCHAR(10))  AS SCORE	 */
    r.CollisionTier_stg as Score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    , r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <>''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    AND r.CollisionTier_stg is not null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

    	

    	

    UNION	

    	

    SELECT	

    Cast(''SLT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.SingleLimitTier_stg,0.0000)  AS VARCHAR(10))AS SCORE	 */
    r.SingleLimitTier_stg as Score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    AND r.SingleLimitTier_stg is not null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

    	

    	

    UNION	

    	

    SELECT	

    Cast(''COMPT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20))  AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.ComprehensiveTier_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    r.ComprehensiveTier_stg as Score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

     ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    and r.ComprehensiveTier_stg is not null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

    	

    UNION 	

    	

    SELECT	

    Cast(''BIT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.BodilyInjuryTier_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    r.BodilyInjuryTier_stg as Score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    AND r.BodilyInjuryTier_stg is not null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    	

UNION	

	

    SELECT	

    Cast(''MPT'' AS VARCHAR(10)) MODEL_NAME	

    ,Cast(NULL AS VARCHAR(20)) AS MODEL_RUN_DTTM	

,/* Cast(Coalesce(r.MedicalPaymentsTier_stg,0.0000) AS VARCHAR(10)) AS SCORE	 */
    r.MedicalPaymentsTier_stg as score	

    ,pc_policyperiod.PublicID_stg PARTY_AGREEMENT	

    ,pc_job.JobNumber_stg ,pc_policyperiod.branchnumber_stg 	

    ,Cast(NULL AS VARCHAR(20)) AS state	

    ,pctl_uwcompanycode.TYPECODE_stg AS UWCompany	

    ,pctl_policyperiodstatus.typecode_stg AS policyperiodstatus_typecode	

    ,r.UpdateTime_stg AS updatetime_pcx_palineratingfactor_alfa,	

    ( :start_dttm) AS start_dttm,	

    ( :end_dttm) AS end_dttm	, eff.expirationdate_stg

FROM  	

    DB_T_PROD_STAG.pc_job	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg	

    LEFT OUTER JOIN  DB_T_PROD_STAG.pc_effectivedatedfields eff ON eff.BranchID_stg = pc_policyperiod.ID_stg	

    INNER JOIN  DB_T_PROD_STAG.pcx_ratingtierppv2_alfa r ON r.ID_stg=eff.RatingTierPPV2_alfa_stg   	

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_uwcompany ON pc_policyperiod.UWCompany_stg = pc_uwcompany.id_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_uwcompanycode ON pc_uwcompany.Code_stg = pctl_uwcompanycode.ID_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg	

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg=pc_job.Subtype_stg	

WHERE	

    pctl_policyperiodstatus.typecode_stg <> ''Temporary''	

    AND pctl_policyperiodstatus.typecode_stg = ''Bound'' 	

    AND pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'')	

    AND r.UpdateTime_stg > ( :start_dttm) 	

    AND r.UpdateTime_stg <= ( :end_dttm)	

    and r.MedicalPaymentsTier_stg is not  null	

	 and (eff.expirationdate_stg is null or eff.expirationdate_stg > pc_policyperiod.EditEffectiveDate_stg)

    )pc_agmt_quotn_scr_x	

 

	

    QUALIFY row_number() Over  (PARTITION BY    

            pc_agmt_quotn_scr_x.MODEL_NAME,     

    pc_agmt_quotn_scr_x.PARTY_AGREEMENT,    

    state,  

    pc_agmt_quotn_scr_x.UWCompany ORDER BY updatetime_pcx_palineratingfactor_alfa  DESC, MODEL_RUN_DTTM  DESC 

    ,coalesce( EXPIRATIONDATE_STG ,cast(''9999-12-31 23:59:59.999999'' as timestamp))  DESC )=1
) SRC
)
);


-- Component exp_pass_from_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_src AS
(
SELECT
SQ_pc_agmt_quotn_scr_x.ModelName as ModelName,
SQ_pc_agmt_quotn_scr_x.ModelRunDttm as ModelRunDttm,
TO_NUMBER(SQ_pc_agmt_quotn_scr_x.Score) as var_Score,
SQ_pc_agmt_quotn_scr_x.PartyAgreement as PartyAgreement,
CASE WHEN SQ_pc_agmt_quotn_scr_x.Score IS NULL THEN ''NOSCORE'' ELSE SQ_pc_agmt_quotn_scr_x.Score END as out_Score,
:P_AGMT_TYPE_CD_POLICY_VERSION as out_AGMT_TYPE_CD,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD */ as out_AGMT_SRC_CD,
''APV'' as LOB,
SQ_pc_agmt_quotn_scr_x.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
LKP_2.LEVEL_NAME /* replaced lookup LKP_RATING_SCR_CD_LKUP */ as LVL_NUM,
SQ_pc_agmt_quotn_scr_x.SCR_FCTR_RATE as SCR_FCTR_RATE,
SQ_pc_agmt_quotn_scr_x.source_record_id,
row_number() over (partition by SQ_pc_agmt_quotn_scr_x.source_record_id order by SQ_pc_agmt_quotn_scr_x.source_record_id) as RNK
FROM
SQ_pc_agmt_quotn_scr_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_agmt_quotn_scr_x.AGMT_SRC_CD
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
SELECT ANLTCL_MODL.MODL_ID as MODL_ID, ANLTCL_MODL.MODL_NAME as MODL_NAME FROM db_t_prod_core.ANLTCL_MODL ORDER BY MODL_FROM_DTTM desc/*  */
) LKP ON LKP.MODL_NAME = exp_pass_from_src.ModelName
QUALIFY RNK = 1
);


-- Component LKP_AGMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT AS
(
SELECT
LKP.AGMT_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_pass_from_src
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_pass_from_src.PartyAgreement AND LKP.AGMT_TYPE_CD = exp_pass_from_src.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_MODL_RUN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MODL_RUN AS
(
SELECT
LKP.MODL_ID,
LKP.MODL_RUN_ID,
exp_pass_from_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_RUN_ID asc,LKP.MODL_RUN_DTTM asc) RNK
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
LEFT JOIN (
SELECT
MODL_ID,
MODL_RUN_ID,
MODL_RUN_DTTM
FROM db_t_prod_core.MODL_RUN
) LKP ON LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_DTTM = exp_pass_from_src.ModelRunDttm
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_pass_from_src.source_record_id ORDER BY LKP.MODL_ID asc,LKP.MODL_RUN_ID asc,LKP.MODL_RUN_DTTM asc) 
= 1
);


-- Component Exp_modl_run_id, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_modl_run_id AS
(
SELECT
CASE WHEN ( LKP_ANLTCL_MODL.MODL_ID = 2 OR LKP_ANLTCL_MODL.MODL_ID = 3 OR LKP_ANLTCL_MODL.MODL_ID = 4 ) THEN LKP_MODL_RUN.MODL_RUN_ID ELSE CASE WHEN ( LKP_ANLTCL_MODL.MODL_ID = 6 OR LKP_ANLTCL_MODL.MODL_ID = 7 OR LKP_ANLTCL_MODL.MODL_ID = 8 OR LKP_ANLTCL_MODL.MODL_ID = 9 OR LKP_ANLTCL_MODL.MODL_ID = 10 OR LKP_ANLTCL_MODL.MODL_ID = 11 OR LKP_ANLTCL_MODL.MODL_ID = 12 ) THEN cast(TO_CHAR ( LKP_ANLTCL_MODL.MODL_ID ) || TO_CHAR ( exp_pass_from_src.updatetime_pcx_palineratingfactor_alfa , ''YYYYMMDD'' ) as BIGINT) ELSE 0 END END as out_MODL_RUN_ID,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_MODL_RUN ON LKP_ANLTCL_MODL.source_record_id = LKP_MODL_RUN.source_record_id
);


-- Component LKP_AGMT_SCR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_SCR AS
(
SELECT
LKP.AGMT_ID,
LKP.MODL_ID,
LKP.MODL_RUN_ID,
LKP.AGMT_SCR_VAL,
LKP.LVL_NUM,
LKP.SCR_FCTR_RATE,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP_ANLTCL_MODL.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_ANLTCL_MODL.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.MODL_ID desc,LKP.MODL_RUN_ID desc,LKP.AGMT_SCR_VAL desc,LKP.LVL_NUM desc,LKP.SCR_FCTR_RATE desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
LKP_ANLTCL_MODL
INNER JOIN LKP_AGMT ON LKP_ANLTCL_MODL.source_record_id = LKP_AGMT.source_record_id
INNER JOIN Exp_modl_run_id ON LKP_AGMT.source_record_id = Exp_modl_run_id.source_record_id
LEFT JOIN (
SELECT AGMT_SCR.SCR_FCTR_RATE as SCR_FCTR_RATE, AGMT_SCR.AGMT_SCR_VAL as AGMT_SCR_VAL,AGMT_SCR.LVL_NUM as LVL_NUM, AGMT_SCR.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_SCR.EDW_END_DTTM as EDW_END_DTTM, AGMT_SCR.AGMT_ID as AGMT_ID, AGMT_SCR.MODL_ID as MODL_ID, AGMT_SCR.MODL_RUN_ID as MODL_RUN_ID FROM db_t_prod_core.AGMT_SCR where EDW_END_DTTM=cast(''9999-12-31'' as date)
) LKP ON LKP.AGMT_ID = LKP_AGMT.AGMT_ID AND LKP.MODL_ID = LKP_ANLTCL_MODL.MODL_ID AND LKP.MODL_RUN_ID = Exp_modl_run_id.out_MODL_RUN_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_ANLTCL_MODL.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.MODL_ID desc,LKP.MODL_RUN_ID desc,LKP.AGMT_SCR_VAL desc,LKP.LVL_NUM desc,LKP.SCR_FCTR_RATE desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) 
= 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_AGMT_SCR.MODL_ID as lkp_MODL_ID,
LKP_AGMT_SCR.MODL_RUN_ID as lkp_MODL_RUN_ID,
LKP_AGMT_SCR.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_SCR.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_ANLTCL_MODL.MODL_ID as in_MODL_ID,
Exp_modl_run_id.out_MODL_RUN_ID as in_MODL_RUN_ID,
LKP_AGMT.AGMT_ID as in_AGMT_ID,
exp_pass_from_src.out_Score as in_AGMT_SCR_VAL,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
MD5 ( ltrim ( Rtrim ( LKP_AGMT_SCR.MODL_RUN_ID ) ) || ltrim ( Rtrim ( LKP_AGMT_SCR.AGMT_SCR_VAL ) ) || ltrim ( Rtrim ( LKP_AGMT_SCR.LVL_NUM ) ) || ltrim ( Rtrim ( LKP_AGMT_SCR.SCR_FCTR_RATE ) ) ) as Checksum_lkp,
MD5 ( ltrim ( Rtrim ( Exp_modl_run_id.out_MODL_RUN_ID ) ) || ltrim ( Rtrim ( exp_pass_from_src.out_Score ) ) || ltrim ( Rtrim ( exp_pass_from_src.LVL_NUM ) ) || ltrim ( Rtrim ( exp_pass_from_src.SCR_FCTR_RATE ) ) ) as Checksum_in,
CASE WHEN Checksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
exp_pass_from_src.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
exp_pass_from_src.LVL_NUM as LVL_NUM,
exp_pass_from_src.SCR_FCTR_RATE as SCR_FCTR_RATE,
exp_pass_from_src.source_record_id
FROM
exp_pass_from_src
INNER JOIN LKP_ANLTCL_MODL ON exp_pass_from_src.source_record_id = LKP_ANLTCL_MODL.source_record_id
INNER JOIN LKP_AGMT ON LKP_ANLTCL_MODL.source_record_id = LKP_AGMT.source_record_id
INNER JOIN Exp_modl_run_id ON LKP_AGMT.source_record_id = Exp_modl_run_id.source_record_id
INNER JOIN LKP_AGMT_SCR ON Exp_modl_run_id.source_record_id = LKP_AGMT_SCR.source_record_id
);


-- Component rtr_pty_scr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_pty_scr_INSERT AS
SELECT
exp_data_transformation.lkp_MODL_ID as lkp_MODL_ID,
exp_data_transformation.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
NULL as lkp_AGMT_SCR_VAL,
NULL as lkp_LVL_NUM,
exp_data_transformation.lkp_AGMT_ID as lkp_AGMT_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.in_MODL_ID as in_MODL_ID,
exp_data_transformation.in_MODL_RUN_ID as in_MODL_RUN_ID,
exp_data_transformation.in_AGMT_ID as in_AGMT_ID,
exp_data_transformation.in_AGMT_SCR_VAL as in_AGMT_SCR_VAL,
exp_data_transformation.out_PRCS_ID as in_PRCS_ID,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.CDC_Flag as CDC_Flag,
exp_data_transformation.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
exp_data_transformation.LVL_NUM as LVL_NUM,
exp_data_transformation.SCR_FCTR_RATE as SCR_FCTR_RATE,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.CDC_Flag = ''I'' and exp_data_transformation.in_AGMT_ID IS NOT NULL and exp_data_transformation.in_MODL_RUN_ID IS NOT NULL;


-- Component rtr_pty_scr_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_pty_scr_UPDATE AS
SELECT
exp_data_transformation.lkp_MODL_ID as lkp_MODL_ID,
exp_data_transformation.lkp_MODL_RUN_ID as lkp_MODL_RUN_ID,
NULL as lkp_AGMT_SCR_VAL,
NULL as lkp_LVL_NUM,
exp_data_transformation.lkp_AGMT_ID as lkp_AGMT_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.in_MODL_ID as in_MODL_ID,
exp_data_transformation.in_MODL_RUN_ID as in_MODL_RUN_ID,
exp_data_transformation.in_AGMT_ID as in_AGMT_ID,
exp_data_transformation.in_AGMT_SCR_VAL as in_AGMT_SCR_VAL,
exp_data_transformation.out_PRCS_ID as in_PRCS_ID,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.CDC_Flag as CDC_Flag,
exp_data_transformation.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
exp_data_transformation.LVL_NUM as LVL_NUM,
exp_data_transformation.SCR_FCTR_RATE as SCR_FCTR_RATE,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.CDC_Flag = ''U'' and exp_data_transformation.in_AGMT_ID IS NOT NULL and exp_data_transformation.in_MODL_RUN_ID IS NOT NULL;


-- Component upd_agmt_scr_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_scr_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_UPDATE.lkp_MODL_ID as MODL_ID,
rtr_pty_scr_UPDATE.lkp_MODL_RUN_ID as MODL_RUN_ID,
rtr_pty_scr_UPDATE.lkp_AGMT_ID as AGMT_ID,
rtr_pty_scr_UPDATE.in_AGMT_SCR_VAL as AGMT_SCR_VAL,
rtr_pty_scr_UPDATE.in_PRCS_ID as PRCS_ID,
rtr_pty_scr_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_pty_scr_UPDATE.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
rtr_pty_scr_UPDATE.LVL_NUM as LVL_NUM3,
rtr_pty_scr_UPDATE.SCR_FCTR_RATE as SCR_FCTR_RATE3,
rtr_pty_scr_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_pty_scr_UPDATE
);


-- Component upd_agmt_scr_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_scr_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_UPDATE.in_MODL_ID as MODL_ID,
rtr_pty_scr_UPDATE.in_MODL_RUN_ID as MODL_RUN_ID,
rtr_pty_scr_UPDATE.in_AGMT_ID as AGMT_ID,
rtr_pty_scr_UPDATE.in_AGMT_SCR_VAL as AGMT_SCR_VAL,
rtr_pty_scr_UPDATE.in_PRCS_ID as PRCS_ID,
rtr_pty_scr_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_pty_scr_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_pty_scr_UPDATE.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
rtr_pty_scr_UPDATE.LVL_NUM as LVL_NUM3,
rtr_pty_scr_UPDATE.SCR_FCTR_RATE as SCR_FCTR_RATE,
0 as UPDATE_STRATEGY_ACTION,
rtr_pty_scr_UPDATE.source_record_id
FROM
rtr_pty_scr_UPDATE
);


-- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
(
SELECT
upd_agmt_scr_upd_ins.MODL_ID as MODL_ID,
upd_agmt_scr_upd_ins.MODL_RUN_ID as MODL_RUN_ID,
upd_agmt_scr_upd_ins.AGMT_ID as AGMT_ID,
upd_agmt_scr_upd_ins.AGMT_SCR_VAL as AGMT_SCR_VAL,
upd_agmt_scr_upd_ins.PRCS_ID as PRCS_ID,
upd_agmt_scr_upd_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_agmt_scr_upd_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
upd_agmt_scr_upd_ins.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
upd_agmt_scr_upd_ins.LVL_NUM3 as LVL_NUM3,
upd_agmt_scr_upd_ins.SCR_FCTR_RATE as SCR_FCTR_RATE,
upd_agmt_scr_upd_ins.source_record_id
FROM
upd_agmt_scr_upd_ins
);


-- Component upd_agmt_scr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_scr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pty_scr_INSERT.in_MODL_ID as MODL_ID,
rtr_pty_scr_INSERT.in_MODL_RUN_ID as MODL_RUN_ID,
rtr_pty_scr_INSERT.in_AGMT_ID as AGMT_ID,
rtr_pty_scr_INSERT.in_AGMT_SCR_VAL as AGMT_SCR_VAL,
rtr_pty_scr_INSERT.in_PRCS_ID as PRCS_ID,
rtr_pty_scr_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_pty_scr_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_pty_scr_INSERT.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
rtr_pty_scr_INSERT.LVL_NUM as LVL_NUM1,
rtr_pty_scr_INSERT.SCR_FCTR_RATE as SCR_FCTR_RATE1,
0 as UPDATE_STRATEGY_ACTION,
rtr_pty_scr_INSERT.source_record_id
FROM
rtr_pty_scr_INSERT
);


-- Component tgt_AGMT_SCR_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_SCR
(
AGMT_ID,
MODL_ID,
MODL_RUN_ID,
AGMT_SCR_VAL,
LVL_NUM,
SCR_FCTR_RATE,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_upd_ins.AGMT_ID as AGMT_ID,
exp_pass_to_tgt_upd_ins.MODL_ID as MODL_ID,
exp_pass_to_tgt_upd_ins.MODL_RUN_ID as MODL_RUN_ID,
exp_pass_to_tgt_upd_ins.AGMT_SCR_VAL as AGMT_SCR_VAL,
exp_pass_to_tgt_upd_ins.LVL_NUM3 as LVL_NUM,
exp_pass_to_tgt_upd_ins.SCR_FCTR_RATE as SCR_FCTR_RATE,
exp_pass_to_tgt_upd_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_upd_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_upd_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_upd_ins.updatetime_pcx_palineratingfactor_alfa as TRANS_STRT_DTTM,
exp_pass_to_tgt_upd_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_upd_ins;


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
upd_agmt_scr_upd.MODL_ID as MODL_ID,
upd_agmt_scr_upd.MODL_RUN_ID as MODL_RUN_ID,
upd_agmt_scr_upd.AGMT_ID as AGMT_ID,
upd_agmt_scr_upd.lkp_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
dateadd (second,-1,  upd_agmt_scr_upd.in_EDW_STRT_DTTM3 ) as EDW_END_DTTM,
dateadd (second,-1,  upd_agmt_scr_upd.updatetime_pcx_palineratingfactor_alfa ) as TRANS_END_DTTM,
upd_agmt_scr_upd.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
upd_agmt_scr_upd.source_record_id
FROM
upd_agmt_scr_upd
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_agmt_scr_ins.MODL_ID as MODL_ID,
upd_agmt_scr_ins.MODL_RUN_ID as MODL_RUN_ID,
upd_agmt_scr_ins.AGMT_ID as AGMT_ID,
upd_agmt_scr_ins.AGMT_SCR_VAL as AGMT_SCR_VAL,
upd_agmt_scr_ins.PRCS_ID as PRCS_ID,
upd_agmt_scr_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_agmt_scr_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
upd_agmt_scr_ins.updatetime_pcx_palineratingfactor_alfa as updatetime_pcx_palineratingfactor_alfa,
upd_agmt_scr_ins.LVL_NUM1 as LVL_NUM1,
upd_agmt_scr_ins.SCR_FCTR_RATE1 as SCR_FCTR_RATE1,
upd_agmt_scr_ins.source_record_id
FROM
upd_agmt_scr_ins
);


-- Component tgt_AGMT_SCR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_SCR
(
AGMT_ID,
MODL_ID,
MODL_RUN_ID,
AGMT_SCR_VAL,
LVL_NUM,
SCR_FCTR_RATE,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_ins.AGMT_ID as AGMT_ID,
exp_pass_to_tgt_ins.MODL_ID as MODL_ID,
exp_pass_to_tgt_ins.MODL_RUN_ID as MODL_RUN_ID,
exp_pass_to_tgt_ins.AGMT_SCR_VAL as AGMT_SCR_VAL,
exp_pass_to_tgt_ins.LVL_NUM1 as LVL_NUM,
exp_pass_to_tgt_ins.SCR_FCTR_RATE1 as SCR_FCTR_RATE,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.updatetime_pcx_palineratingfactor_alfa as TRANS_STRT_DTTM,
exp_pass_to_tgt_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component tgt_AGMT_SCR_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_SCR
USING exp_pass_to_tgt_upd ON (AGMT_SCR.AGMT_ID = exp_pass_to_tgt_upd.AGMT_ID AND AGMT_SCR.MODL_ID = exp_pass_to_tgt_upd.MODL_ID AND AGMT_SCR.MODL_RUN_ID = exp_pass_to_tgt_upd.MODL_RUN_ID AND AGMT_SCR.EDW_STRT_DTTM = exp_pass_to_tgt_upd.EDW_STRT_DTTM)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_tgt_upd.AGMT_ID,
MODL_ID = exp_pass_to_tgt_upd.MODL_ID,
MODL_RUN_ID = exp_pass_to_tgt_upd.MODL_RUN_ID,
EDW_STRT_DTTM = exp_pass_to_tgt_upd.EDW_STRT_DTTM,
EDW_END_DTTM = exp_pass_to_tgt_upd.EDW_END_DTTM,
TRANS_STRT_DTTM = exp_pass_to_tgt_upd.updatetime_pcx_palineratingfactor_alfa,
TRANS_END_DTTM = exp_pass_to_tgt_upd.TRANS_END_DTTM;


END; 
';