-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_SPEC_INSUPD("RUN_ID" VARCHAR)
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

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'', ''pctl_bp7classificationproperty.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SPEC_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'',''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''UOM_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


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


-- PIPELINE START FOR 1

-- Component SQ_pctl_dwellinglocationtype_hoe, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pctl_dwellinglocationtype_hoe AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Rank,
$2 as FixedID,
$3 as PRTY_ASSET_SB_TYPE_CD,
$4 as CLASS_CD,
$5 as PRTY_ASSET_SPEC_TYPE_CD,
$6 as Start_Date,
$7 as PRTY_ASSET_SPEC_VAL,
$8 as Ind,
$9 as Measure,
$10 as Count,
$11 as meas_typ_cd,
$12 as SRC_SYS_CD,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT   Rank() Over (PARTITION BY  id,typecode,classification_code,spec_type_cd ORDER BY strt_dt,Meas,Cnt,ind,Val ) rk,  a.id,a.typecode,a.classification_code,a.spec_type_cd,a.strt_dt,Cast(a.Val AS VARCHAR(100)) AS val,a.ind,a.Meas																						

	,a.Cnt,a.meas_typ_cd,a.SRC_SYS_CD FROM																						

																							

	  (																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE5'' as varchar(50))  AS typecode ,																						

	cast(''PRTY_ASSET_CLASFCN1'' as varchar(50)) AS classification_code,																						

	cast(''PRTY_ASSET_SPEC_TYPE9'' as varchar(50)) spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ( ApproxSquareFootage_stg AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	''UOM_TYPE9''  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE ApproxSquareFootage_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ApproxSquareFootage_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE21'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast(CASE WHEN MitigationZone_alfa_stg = ''0'' THEN ''NONE''																						

	ELSE Cast (MitigationZone_alfa_stg AS VARCHAR(100))																						

	end  AS VARCHAR(100))Val,/*EIM-30257*/																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE MitigationZone_alfa_stg IS NOT NULL																						

	AND MitigationZone_alfa_stg <> '' ''																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,MitigationZone_alfa_stg																						

																							

	UNION																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE10'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast(RoomerBoardersNumber_stg AS VARCHAR(50)) Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE RoomerBoardersNumber_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,RoomerBoardersNumber_stg																						

																							

	UNION																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE11'' spec_type_cd,																						

	(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

Cast ( pctl_numberofstories_hoe.TYPECODE_stg AS VARCHAR(100)) Val,/* EIM-50235																						 */
	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

Cast ('''' AS VARCHAR(100))  Cnt, /* EIM-50235																					 */
	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe INNER JOIN DB_T_PROD_STAG.pctl_numberofstories_hoe ON pctl_numberofstories_hoe.id_stg=pcx_dwelling_hoe.StoriesNumber_stg																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

WHERE (pcx_dwelling_hoe.ExpirationDate_stg IS NULL or pcx_dwelling_hoe.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)/* EIM-50235																						 */
	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

qualify Row_Number() over(partition by id order by Coalesce(pcx_dwelling_hoe.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_dwelling_hoe.UpdateTime_stg desc, pcx_dwelling_hoe.createtime_stg desc)=1/* EIM-50235																					 */
																							

																							

	UNION																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE12'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	cast( Garage_alfa_stg  as varchar(100)) ind, /*EIM-30257*/																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE Garage_alfa_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,Garage_alfa_stg																						

																							

/* -xx1--																						 */
	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE13'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast( ScreenPoolEnclosure_alfa_stg AS VARCHAR(100)) ind,/*EIM-30257*/																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE ScreenPoolEnclosure_alfa_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ScreenPoolEnclosure_alfa_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE14'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(HydrantDistanceOverThreshold_stg AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE HydrantDistanceOverThreshold_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,HydrantDistanceOverThreshold_stg																						

																							

/* -test from here--																						 */
	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE15'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(FireDeptDistanceOverThreshold_stg AS VARCHAR(100)) ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE FireDeptDistanceOverThreshold_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,FireDeptDistanceOverThreshold_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE8'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast (dwellinganswer.BooleanAnswer_stg AS VARCHAR(10))  ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	LEFT OUTER JOIN 																						

	( 																						

	SELECT 																						

	Dwelling_HOE_stg,																						

	pcx_dwellinganswer_alf.QuestionCode_stg,																						

	 pcx_dwellinganswer_alf.BooleanAnswer_stg  ,																						

	Max(pcx_dwellinganswer_alf.UpdateTime_stg) AS updatetime FROM DB_T_PROD_STAG.pcx_dwellinganswer_alf 																						

	WHERE pcx_dwellinganswer_alf.BooleanAnswer_stg IS NOT NULL GROUP BY Dwelling_HOE_stg,pcx_dwellinganswer_alf.QuestionCode_stg,pcx_dwellinganswer_alf.BooleanAnswer_stg 																						

	) dwellinganswer ON dwellinganswer.dwelling_hoe_stg=pcx_dwelling_hoe.id_stg																						

	WHERE questioncode_stg = ''HOGenDangerousAnimalsExist_alfa'' AND booleananswer_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ind																						

																							

	UNION																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE18'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast (dwellinganswer.BooleanAnswer_stg AS VARCHAR(10)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	LEFT OUTER JOIN 																						

	( 																						

	SELECT 																						

	Dwelling_HOE_stg,																						

	pcx_dwellinganswer_alf.QuestionCode_stg,																						

	 pcx_dwellinganswer_alf.BooleanAnswer_stg  ,																						

	Max(pcx_dwellinganswer_alf.UpdateTime_stg) AS updatetime FROM DB_T_PROD_STAG.pcx_dwellinganswer_alf 																						

	WHERE pcx_dwellinganswer_alf.BooleanAnswer_stg IS NOT NULL GROUP BY Dwelling_HOE_stg,pcx_dwellinganswer_alf.QuestionCode_stg,pcx_dwellinganswer_alf.BooleanAnswer_stg 																						

	) dwellinganswer ON dwellinganswer.dwelling_hoe_stg=pcx_dwelling_hoe.id_stg																						

	WHERE questioncode_stg = ''HODwellingConstructionUnderpinningQualifies'' AND booleananswer_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ind																						

																							

	UNION																						

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE17'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast (dwellinganswer.BooleanAnswer_stg AS VARCHAR(10)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	LEFT OUTER JOIN 																						

	( 																						

	SELECT 																						

	Dwelling_HOE_stg,																						

	pcx_dwellinganswer_alf.QuestionCode_stg,																						

	 pcx_dwellinganswer_alf.BooleanAnswer_stg  ,																						

	Max(pcx_dwellinganswer_alf.UpdateTime_stg) AS updatetime FROM DB_T_PROD_STAG.pcx_dwellinganswer_alf 																						

	WHERE pcx_dwellinganswer_alf.BooleanAnswer_stg IS NOT NULL GROUP BY Dwelling_HOE_stg,pcx_dwellinganswer_alf.QuestionCode_stg,pcx_dwellinganswer_alf.BooleanAnswer_stg 																						

	) dwellinganswer ON dwellinganswer.dwelling_hoe_stg=pcx_dwelling_hoe.id_stg																						

	WHERE questioncode_stg = ''HOGenSwimmingPoolExists_alfa'' AND booleananswer_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ind																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE19'' spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast (dwellinganswer.BooleanAnswer_stg AS VARCHAR(10)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	LEFT OUTER JOIN 																						

	( 																						

	SELECT 																						

	Dwelling_HOE_stg,																						

	pcx_dwellinganswer_alf.QuestionCode_stg,																						

	 pcx_dwellinganswer_alf.BooleanAnswer_stg  ,																						

	Max(pcx_dwellinganswer_alf.UpdateTime_stg) AS updatetime FROM DB_T_PROD_STAG.pcx_dwellinganswer_alf 																						

	WHERE pcx_dwellinganswer_alf.BooleanAnswer_stg IS NOT NULL GROUP BY Dwelling_HOE_stg,pcx_dwellinganswer_alf.QuestionCode_stg,pcx_dwellinganswer_alf.BooleanAnswer_stg 																						

	) dwellinganswer ON dwellinganswer.dwelling_hoe_stg=pcx_dwelling_hoe.id_stg																						

	WHERE questioncode_stg IN (''HOGenDangerousBreedExists_alfa'',																						

	''HOGenDangerousAnimalsExist_alfa'',																						

	''HOGenStepsAndRails_alfa'') 																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,ind																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	Cast(''PRTY_ASSET_SBTYPE5''  AS VARCHAR(50)) AS typecode ,																						

	Cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR (50)) AS classification_code,																						

	Cast(''PRTY_ASSET_SPEC_TYPE20''  AS VARCHAR(50)) spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast(MHMake_alfa_stg AS VARCHAR(100))Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE MHMake_alfa_stg IS NOT NULL																						

	AND pcx_dwelling_hoe.ExpirationDate_stg IS NULL																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,MHMake_alfa_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	A.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE5''  AS typecode ,''PRTY_ASSET_CLASFCN1'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE22'' spec_type_cd,																						

	Max(Coalesce(a.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast(pcx_holocation_hoe.RatingHex_alfa_stg AS VARCHAR(100)) Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe A																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = a.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON a.holocation_stg = pcx_holocation_hoe.ID_stg																						

	WHERE pcx_holocation_hoe.RatingHex_alfa_stg IS NOT NULL																						

	AND a.ExpirationDate_stg IS NULL																						

	AND a.UPDATETIME_stg>(:start_dttm) AND a.UPDATETIME_stg<= (:end_dttm)																						

	AND pcx_holocation_hoe.UPDATETIME_stg>(:start_dttm) AND pcx_holocation_hoe.UPDATETIME_stg<= (:end_dttm)																						

	AND pc_policyperiod.UPDATETIME_stg>(:start_dttm) AND pc_policyperiod.UPDATETIME_stg<= (:end_dttm)																						

	GROUP BY A.fixedid_stg,pcx_holocation_hoe.RatingHex_alfa_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE9'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

	    Cast('''' AS VARCHAR(100)) AS Val,																						

	    Cast('''' AS VARCHAR(100)) AS ind,																						

	    Cast(b.BP7SquareFootage_alfa_stg AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast(''UOM_TYPE9'' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM 																						

	DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bCD ON c.bp7classdescription_stg = bCD.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg 																						

	WHERE b.BP7SquareFootage_alfa_stg IS NOT NULL																						

	AND b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	    GROUP BY c.fixedid_stg, cp.TYPECODE_stg, b.BP7SquareFootage_alfa_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE21'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

	    Cast(CASE 																						

	    WHEN b.BP7MitigationZone_alfa_stg = ''0'' THEN ''NONE''																						

	    ELSE b.BP7MitigationZone_alfa_stg																						

	    end  AS VARCHAR(100)) AS Val,																						

	    Cast('''' AS VARCHAR(100)) AS ind,																						

	    Cast('''' AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM 																						

	DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bcd ON c.bp7classdescription_stg = bcd.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg																						

	WHERE b.BP7MitigationZone_alfa_stg IS NOT NULL																						

	AND BP7MitigationZone_alfa_stg <> '' ''																						

	AND b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	GROUP BY c.FixedID_stg, cp.TYPECODE_stg, b.BP7MitigationZone_alfa_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE11'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

cast(pctl_numberofstories_hoe.TYPECODE_stg AS VARCHAR(100)) Val,/* EIM-50235																						 */
	    '''' AS ind,																						

	    '''' AS Meas,																						

Cast ('''' AS VARCHAR(100))  Cnt,	/* EIM-50235																					 */
	    '''' AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM 																						

	DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bcd ON c.bp7classdescription_stg = bcd.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pctl_numberofstories_hoe 																						

	ON pctl_numberofstories_hoe.id_stg = b.Bp7NumOfStories_alfa_stg																						

	WHERE  b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	GROUP BY c.FixedID_stg, cp.TYPECODE_stg, pctl_numberofstories_hoe.TYPECODE_stg																						

																							

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE14'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

	    Cast('''' AS VARCHAR(100)) AS Val,																						

	    Cast(l.FireHydrantDistance_alfa_stg AS VARCHAR(100)) AS ind,/*EIM-30257*/																						

	    Cast('''' AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt, 																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bcd ON c.bp7classdescription_stg = bcd.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg																						

	WHERE l.FireHydrantDistance_alfa_stg IS NOT NULL																						

	AND b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	GROUP BY c.fixedid_stg, cp.TYPECODE_stg, l.FireHydrantDistance_alfa_stg																						

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE17'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

	    Cast('''' AS VARCHAR(100)) AS Val,																						

	    Cast(ba.booleananswer_stg AS VARCHAR(100)) AS ind,																						

	    Cast('''' AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	    FROM 																						

	DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bcd ON c.bp7classdescription_stg = bcd.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg																						

	LEFT JOIN (   SELECT ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg,Max( ba.updatetime_stg) AS updatetime																						

	                     FROM DB_T_PROD_STAG.pcx_bp7buildinganswer_alfa ba																						

	                     WHERE ba.BooleanAnswer_stg IS NOT NULL 																						

	                     GROUP BY ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg																						

	)ba ON ba.BP7Building_stg = b.ID_stg																						

	WHERE ba.QuestionCode_stg = ''BP7SwimmingPool_alfa'' 																						

	AND ba.booleananswer_stg IS NOT NULL																						

	AND b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	GROUP BY c.fixedid_stg, cp.TYPECODE_stg, ba.booleananswer_stg																						

																							

																							

/* EIM-15433 - Adding BCEG Information																						 */
																							

	UNION																						

																							

	SELECT DISTINCT																						

	    c.FixedID_stg AS id,																						

	    ''PRTY_ASSET_SBTYPE13'' AS typecode,																						

	    cp.TYPECODE_stg AS classification_code,																						

	    ''PRTY_ASSET_SPEC_TYPE23'' AS spec_type_cd,																						

	    Max(c.EffectiveDate_stg) AS strt_dt,																						

	    Cast(eg.typecode_stg AS VARCHAR(100)) AS Val,																						

	    Cast('''' AS VARCHAR(100)) AS ind,																						

	    Cast('''' AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM 																						

	DB_T_PROD_STAG.pcx_bp7classification c																						

	INNER JOIN (SELECT b.*, Rank() Over (PARTITION BY b.FixedId_stg ORDER BY b.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7building b) b 																						

	        ON c.Building_stg = b.FixedId_stg																						

	        AND c.branchid_stg=b.branchid_stg																						

	        AND b.r = 1 																						

	/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/      																						

	INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  ON building.id_stg = b.Building_stg    																						

	INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp ON cp.ID_stg = c.bp7classpropertytype_stg																						

	JOIN DB_T_PROD_STAG.pctl_bp7classdescription bcd ON c.bp7classdescription_stg = bcd.ID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg = b.BranchID_stg																						

	INNER JOIN DB_T_PROD_STAG.pc_policy p ON p.id_stg = pp.PolicyID_stg																						

	LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt ON b.BP7RoofType_alfa_stg = rt.ID_stg																						

	INNER JOIN (SELECT l.*, Rank() Over (PARTITION BY l.FixedId_stg ORDER BY l.UPDATETIME_stg DESC) r FROM DB_T_PROD_STAG.pcx_bp7location l) l 																						

	        ON b.Location_stg = l.FixedId_stg																						

	        AND l.r = 1																						

	INNER JOIN DB_T_PROD_STAG.pc_policyline pol ON pol.BranchID_stg = pp.ID_stg																						

	LEFT JOIN (   SELECT ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg,Max(ba.updatetime_stg) AS updatetime																						

	                     FROM DB_T_PROD_STAG.pcx_bp7buildinganswer_alfa ba																						

	                     WHERE ba.BooleanAnswer_stg IS NOT NULL 																						

	                     GROUP BY ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg																						

	)ba ON ba.BP7Building_stg = b.ID_stg																						

	  JOIN DB_T_PROD_STAG.pctl_bp7bldgcodeeffgradeclass egc ON egc.ID_stg = b.bp7bldgcodeeffgradeclass_stg																						

	 JOIN DB_T_PROD_STAG.pctl_bp7bldgcodeeffgrade eg ON eg.ID_stg = b.bp7bldgcodeeffgrade_stg																						

	 WHERE  b.ExpirationDate_stg IS NULL																						

	AND c.ExpirationDate_stg IS NULL																						

	AND l.ExpirationDate_stg IS NULL																						

	AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))																						

	    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= (:end_dttm))																						

	    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))																						

	GROUP BY c.fixedid_stg, cp.TYPECODE_stg, eg.typecode_stg																						

																							

/* EIM-17836 - Adding Motor DB_T_CORE_PROD.vehicle Information																						 */
																							

	UNION																						

																							

	SELECT DISTINCT																						

	    pv.fixedid_stg AS id,																						

	Cast(''PRTY_ASSET_SBTYPE4''  AS VARCHAR(50)) AS typecode ,																						

	Cast(''PRTY_ASSET_CLASFCN3'' AS VARCHAR (50)) AS classification_code,																						

	Cast(''PRTY_ASSET_SPEC_TYPE24''  AS VARCHAR(50)) spec_type_cd,																						

	    pv.EffectiveDate_stg AS strt_dt,																						

	   Cast(CASE WHEN pv.ActualCashValue_alfa_amt_stg=''0'' THEN Cast(''0.00'' AS VARCHAR(100)) ELSE Cast( pv.ActualCashValue_alfa_amt_stg AS VARCHAR(100)) end AS VARCHAR(100)) AS Val,																						

	    Cast('''' AS VARCHAR(100)) AS ind,																						

	    Cast('''' AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pc_personalvehicle pv																						

	 WHERE (pv.ActualCashValue_alfa_amt_stg is not null ) 																						

	 AND pv.UPDATETIME_stg> (:start_dttm)																						

	AND pv.UPDATETIME_stg <= (:end_dttm)																						

	AND  (pv.ExpirationDate_stg IS NULL OR pv.ExpirationDate_stg >:start_dttm)																						

	 QUALIFY Row_Number() Over(PARTITION BY pv.fixedid_stg ORDER BY pv.updatetime_stg DESC,pv.EffectiveDate_stg desc) =1																						

	 																						

	 union																						

																							

	SELECT DISTINCT																						

	pcx_dwelling_hoe.fixedid_stg AS id,																						

	Cast(''PRTY_ASSET_SBTYPE5'' AS VARCHAR(50)) AS typecode ,																						

	Cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR (50)) AS classification_code,																						

	Cast(''PRTY_ASSET_SPEC_TYPE25''AS VARCHAR(50)) spec_type_cd,																						

	Max(Coalesce(pcx_dwelling_hoe.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast(pcx_holocation_hoe.DisableProtectClassCalc_alfa_stg AS VARCHAR(100)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_dwelling_hoe																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg																						

	JOIN DB_T_PROD_STAG.pcx_holocation_hoe ON pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.fixedid_stg																						

	and pcx_dwelling_hoe.branchid_stg = pcx_holocation_hoe.branchid_stg 																						

	WHERE pcx_holocation_hoe.DisableProtectClassCalc_alfa_stg IS NOT NULL																						

	AND (pcx_dwelling_hoe.ExpirationDate_stg IS NULL or pcx_dwelling_hoe.ExpirationDate_stg > pc_policyperiod.modeldate_stg)																						

	AND pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) AND pcx_dwelling_hoe.UpdateTime_stg <= (:end_dttm)																						

	GROUP BY pcx_dwelling_hoe.fixedid_stg,pcx_holocation_hoe.DisableProtectClassCalc_alfa_stg																						

																							

/* EIM-41182 and EIM-81184																						 */
																							

	union 																						

																							

	SELECT DISTINCT																						

	    pv.fixedid_stg AS id,																						

	    Cast(''PRTY_ASSET_SBTYPE4''  AS VARCHAR(50)) AS typecode ,																						

	    Cast(''PRTY_ASSET_CLASFCN3'' AS VARCHAR (50)) AS classification_code,																						

	    Cast(''PRTY_ASSET_SPEC_TYPE26''  AS VARCHAR(50)) spec_type_cd,																						

	    pv.OdometerAsOfDate_alfa_stg AS strt_dt,																						

	    Cast(CASE WHEN pv.ActualCashValue_alfa_amt_stg=''0'' THEN Cast(''0.00'' AS VARCHAR(100)) ELSE Cast( pv.ActualCashValue_alfa_amt_stg AS VARCHAR(100)) end AS VARCHAR(100)) AS Val,																						

	    Cast('''' AS VARCHAR(100)) AS ind,																						

	    Cast(pv.Odometer_alfa_stg AS VARCHAR(100)) AS Meas,																						

	    Cast('''' AS VARCHAR(100)) AS Cnt,																						

	    Cast('''' AS VARCHAR(100)) AS meas_typ_cd,																						

	    ''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pc_personalvehicle pv																						

	 WHERE (pv.OdometerAsOfDate_alfa_stg IS not NULL																						

	  OR pv.Odometer_alfa_stg Is not NULL) 																						

	  AND pv.UPDATETIME_stg> (:start_dttm) 																						

	  AND pv.UPDATETIME_stg <= (:end_dttm)																						

	  AND pv.OdometerAsOfDate_alfa_stg > (:start_dttm)																						

QUALIFY Row_Number() Over(PARTITION BY pv.fixedid_stg ORDER BY pv.updatetime_stg DESC ,pv.OdometerAsOfDate_alfa_stg DESC,coalesce(Expirationdate_stg,cast(:end_dttm as timestamp)) DESC , Odometer_alfa_stg DESC ) =1/* EIM-43801 																						 */
																							

	union																						

																							

	select distinct 																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE37'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN15'' as varchar(50)) as classification_code,																						

	Cast(''PRTY_ASSET_SPEC_TYPE25''AS VARCHAR(50)) spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast(pcx_fopdwelling.FOPDwellingProtecClassDisabled_stg AS VARCHAR(100)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	from DB_T_PROD_STAG.pcx_fopdwelling 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE pcx_fopdwelling.FOPDwellingProtecClassDisabled_stg IS NOT NULL																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopoutbuilding.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE15'' spec_type_cd,																						

	(Coalesce(pcx_fopoutbuilding.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(FireDeptDistOverThreshold_alfa_stg AS VARCHAR(100)) ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	from DB_T_PROD_STAG.pcx_fopoutbuilding 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopoutbuilding.branchid_stg																						

	WHERE FireDeptDistOverThreshold_alfa_stg IS NOT NULL																						

	AND (pcx_fopoutbuilding.ExpirationDate_stg IS NULL or pcx_fopoutbuilding.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopoutbuilding.UpdateTime_stg>(:start_dttm) AND  pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopoutbuilding.UpdateTime_stg desc, pcx_fopoutbuilding.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE37'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN15'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE14'' spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(IsDistToFireHydMoreThn1000fts_stg AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	from DB_T_PROD_STAG.pcx_fopdwelling 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE IsDistToFireHydMoreThn1000fts_stg IS NOT NULL																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopoutbuilding.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE14'' spec_type_cd,																						

	(Coalesce(pcx_fopoutbuilding.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(FireHydrDistOverThreshold_alfa_stg AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	from DB_T_PROD_STAG.pcx_fopoutbuilding 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopoutbuilding.branchid_stg																						

	WHERE FireHydrDistOverThreshold_alfa_stg IS NOT NULL																						

	AND (pcx_fopoutbuilding.ExpirationDate_stg IS NULL or pcx_fopoutbuilding.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopoutbuilding.UpdateTime_stg>(:start_dttm) AND  pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopoutbuilding.UpdateTime_stg desc, pcx_fopoutbuilding.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopoutbuilding.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE11'' spec_type_cd,																						

	(Coalesce(pcx_fopoutbuilding.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

Cast (pctl_FOPNumOfStories.TYPECODE_stg AS VARCHAR(100)) Val,	/* EIM-50235																					 */
	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

Cast ('''' AS VARCHAR(100))  Cnt,	/* EIM-50235																					 */
	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopoutbuilding 																						

	INNER JOIN DB_T_PROD_STAG.pctl_FOPNumOfStories ON pctl_FOPNumOfStories.id_stg=pcx_fopoutbuilding.NumOfStories_stg																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopoutbuilding.branchid_stg																						

	WHERE (pcx_fopoutbuilding.ExpirationDate_stg IS NULL or pcx_fopoutbuilding.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopoutbuilding.UpdateTime_stg>(:start_dttm) AND  pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopoutbuilding.UpdateTime_stg desc, pcx_fopoutbuilding.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	Cast(''PRTY_ASSET_SBTYPE37''  AS VARCHAR(50)) AS typecode ,																						

	Cast(''PRTY_ASSET_CLASFCN15'' AS VARCHAR (50)) AS classification_code,																						

	Cast(''PRTY_ASSET_SPEC_TYPE20''  AS VARCHAR(50)) spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast(Make_stg AS VARCHAR(100))Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopdwelling																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE Make_stg IS NOT NULL																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE37''  AS typecode ,																						

	''PRTY_ASSET_CLASFCN15'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE21'' spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast(CASE WHEN MitigationZone_stg = ''0'' THEN ''NONE''																						

	ELSE Cast (MitigationZone_stg AS VARCHAR(100))																						

	end  AS VARCHAR(100))Val,																						

	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopdwelling 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE MitigationZone_stg IS NOT NULL																						

	AND MitigationZone_stg <> '' ''																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	''PRTY_ASSET_SBTYPE37''  AS typecode ,																						

	''PRTY_ASSET_CLASFCN15'' AS classification_code,																						

	''PRTY_ASSET_SPEC_TYPE17'' spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100)) Val,																						

	Cast (IsThereASwimmPoolOnThePremises_stg AS VARCHAR(10)) ind,																						

	Cast ('''' AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) Cnt,																						

	Cast ('''' AS VARCHAR(100)) meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopdwelling																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE IsThereASwimmPoolOnThePremises_stg = 1																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

/* Adding Square footage																						 */
																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE37'' as varchar(50))  AS typecode ,																						

	cast(''PRTY_ASSET_CLASFCN15'' as varchar(50)) AS classification_code,																						

	cast(''PRTY_ASSET_SPEC_TYPE9'' as varchar(50)) AS spec_type_cd,																						

	Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg) AS strt_dt,																						

	Cast ('''' AS VARCHAR(100)) AS Val,																						

	Cast ('''' AS VARCHAR(100)) AS  ind,																						

	Cast (SquareFootage_stg AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) AS Cnt,																						

	''UOM_TYPE9'' AS meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopdwelling 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE SquareFootage_stg IS NOT NULL																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	union																						

																							

/* Adding Square footage																						 */
																							

	SELECT DISTINCT																						

	pcx_fopoutbuilding.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE36'' as varchar(50))  AS typecode ,																						

	cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) AS classification_code,																						

	cast(''PRTY_ASSET_SPEC_TYPE9'' as varchar(50)) AS spec_type_cd,																						

	Coalesce(pcx_fopoutbuilding.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg) AS strt_dt,																						

	Cast ('''' AS VARCHAR(100)) AS Val,																						

	Cast ('''' AS VARCHAR(100)) AS  ind,																						

	Cast (SqFootage_stg AS VARCHAR(100)) Meas,																						

	Cast ('''' AS VARCHAR(100)) AS Cnt,																						

	''UOM_TYPE9'' AS meas_typ_cd,																						

	''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopoutbuilding 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopoutbuilding.branchid_stg																						

	WHERE SqFootage_stg IS NOT NULL																						

	AND (pcx_fopoutbuilding.ExpirationDate_stg IS NULL or pcx_fopoutbuilding.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopoutbuilding.UpdateTime_stg>(:start_dttm) AND  pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopoutbuilding.UpdateTime_stg desc, pcx_fopoutbuilding.createtime_stg desc)=1																						

																							

	Union																						

																							

/* EIM-50208 Number of Farm Dwelling stories																						 */
																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE37'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN15'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE11'' spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

Cast (pctl_FOPNumOfStories.TYPECODE_stg AS VARCHAR(100)) Val,/* EIM-50235																						 */
	Cast ('''' AS VARCHAR(100))  ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

Cast ('''' AS VARCHAR(100))  Cnt,	/* EIM-50235																					 */
	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	FROM DB_T_PROD_STAG.pcx_fopdwelling 																						

	INNER JOIN DB_T_PROD_STAG.pctl_FOPNumOfStories ON pctl_FOPNumOfStories.id_stg=pcx_fopdwelling.NumOfStories_stg																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	Union																						

																							

	SELECT DISTINCT																						

	pcx_fopdwelling.fixedid_stg AS id,																						

	cast(''PRTY_ASSET_SBTYPE37'' as varchar(50)) as assettype ,																						

	cast(''PRTY_ASSET_CLASFCN15'' as varchar(50)) as classification_code,																						

	''PRTY_ASSET_SPEC_TYPE15'' as spec_type_cd,																						

	(Coalesce(pcx_fopdwelling.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg)) strt_dt,																						

	Cast ('''' AS VARCHAR(100))  Val,																						

	Cast(FireDeptDistanceOverThreshold_stg AS VARCHAR(100)) ind,																						

	Cast ('''' AS VARCHAR(100))  Meas,																						

	Cast ('''' AS VARCHAR(100))  Cnt,																						

	Cast ('''' AS VARCHAR(100))  meas_typ_cd																						

	,''SRC_SYS4'' AS SRC_SYS_CD																						

	from DB_T_PROD_STAG.pcx_fopdwelling 																						

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg																						

	WHERE FireDeptDistanceOverThreshold_stg IS NOT NULL																						

	AND (pcx_fopdwelling.ExpirationDate_stg IS NULL or pcx_fopdwelling.ExpirationDate_stg > pc_policyperiod.EditeffectiveDate_stg)																						

	AND pcx_fopdwelling.UpdateTime_stg>(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)																						

	qualify Row_Number() over(partition by id order by Coalesce(pcx_fopdwelling.ExpirationDate_stg, Cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, pcx_fopdwelling.UpdateTime_stg desc, pcx_fopdwelling.createtime_stg desc)=1																						

																							

	) A
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
TO_CHAR ( SQ_pctl_dwellinglocationtype_hoe.FixedID ) as var_fixedid,
var_fixedid as out_fixedid,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_prty_asset_sbtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_CLASS_CD,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE */ END as prty_asset_spec_type_cd,
SQ_pctl_dwellinglocationtype_hoe.Measure as Measure,
SQ_pctl_dwellinglocationtype_hoe.Count as Count,
CASE WHEN SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SPEC_VAL IS NULL THEN '' '' ELSE SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SPEC_VAL END as PRTY_ASSET_SPEC_VAL1,
CASE WHEN SQ_pctl_dwellinglocationtype_hoe.Start_Date IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pctl_dwellinglocationtype_hoe.Start_Date END as o_Start_Date,
CASE WHEN LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ IS NULL THEN ''UNK'' ELSE LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ END as o_SRC_CD,
CASE WHEN SQ_pctl_dwellinglocationtype_hoe.Ind IS NULL THEN '' '' ELSE SQ_pctl_dwellinglocationtype_hoe.Ind END as PRTY_ASSET_IND,
CASE WHEN LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM */ IS NULL THEN ''UNK'' ELSE LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM */ END as out_meas_typ_cd,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
SQ_pctl_dwellinglocationtype_hoe.Rank as Rank,
SQ_pctl_dwellinglocationtype_hoe.source_record_id,
row_number() over (partition by SQ_pctl_dwellinglocationtype_hoe.source_record_id order by SQ_pctl_dwellinglocationtype_hoe.source_record_id) as RNK
FROM
SQ_pctl_dwellinglocationtype_hoe
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SB_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SB_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.CLASS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.CLASS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SPEC_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SPEC_TYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.PRTY_ASSET_SPEC_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.SRC_SYS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.SRC_SYS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.meas_typ_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_UOM LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = SQ_pctl_dwellinglocationtype_hoe.meas_typ_cd
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.EDW_END_DTTM,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_all_source.out_fixedid AND LKP.PRTY_ASSET_SBTYPE_CD = exp_all_source.out_prty_asset_sbtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_all_source.out_CLASS_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc)  
= 1
);


-- Component LKP_PRTY_ASSET_SPEC_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_SPEC_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_SPEC_VAL,
LKP.PRTY_ASSET_SPEC_MEAS,
LKP.PRTY_ASSET_SPEC_CNT,
LKP.PRTY_ASSET_SPEC_STRT_DTTM,
LKP.PRTY_ASSET_IND,
LKP.PRTY_ASSET_SPEC_UOM_CD,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as in_prty_asset_id,
exp_all_source.prty_asset_spec_type_cd as in_prty_asset_spec_type_cd1,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_SPEC_VAL desc,LKP.PRTY_ASSET_SPEC_MEAS desc,LKP.PRTY_ASSET_SPEC_CNT desc,LKP.PRTY_ASSET_SPEC_STRT_DTTM desc,LKP.PRTY_ASSET_IND desc,LKP.PRTY_ASSET_SPEC_UOM_CD desc) RNK
FROM
exp_all_source
INNER JOIN LKP_PRTY_ASSET_ID ON exp_all_source.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN (
SELECT PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_VAL as PRTY_ASSET_SPEC_VAL, PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_MEAS as PRTY_ASSET_SPEC_MEAS, PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_CNT as PRTY_ASSET_SPEC_CNT, PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_STRT_DTTM as PRTY_ASSET_SPEC_STRT_DTTM, PRTY_ASSET_SPEC.PRTY_ASSET_IND as PRTY_ASSET_IND, PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_UOM_CD as PRTY_ASSET_SPEC_UOM_CD, PRTY_ASSET_SPEC.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_TYPE_CD as PRTY_ASSET_SPEC_TYPE_CD FROM db_t_prod_core.PRTY_ASSET_SPEC QUALIFY	ROW_NUMBER() OVER(
PARTITION BY PRTY_ASSET_ID,PRTY_ASSET_SPEC_TYPE_CD 
ORDER BY EDW_END_DTTM desc) = 1/*  */
) LKP ON LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID AND LKP.PRTY_ASSET_SPEC_TYPE_CD = exp_all_source.prty_asset_spec_type_cd
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_SPEC_VAL desc,LKP.PRTY_ASSET_SPEC_MEAS desc,LKP.PRTY_ASSET_SPEC_CNT desc,LKP.PRTY_ASSET_SPEC_STRT_DTTM desc,LKP.PRTY_ASSET_IND desc,LKP.PRTY_ASSET_SPEC_UOM_CD desc)  
= 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_PRTY_ASSET_SPEC_ASSET_ID.in_prty_asset_spec_type_cd1 as src_prty_asset_spec_type_cd,
LKP_PRTY_ASSET_SPEC_ASSET_ID.in_prty_asset_id as src__prty_asset_id,
:PRCS_ID as process_id,
exp_all_source.o_Start_Date as src_effective_date,
exp_all_source.Count as Count1,
exp_all_source.PRTY_ASSET_IND as Src_PRTY_ASSET_IND,
exp_all_source.out_meas_typ_cd as in_meas_typ_cd,
exp_all_source.PRTY_ASSET_SPEC_VAL1 as PRTY_ASSET_SPEC_VAL1,
exp_all_source.EDW_END_DTTM as EDW_END_DTTM,
md5 ( ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_SPEC_VAL ) ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_SPEC_STRT_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_SPEC_MEAS ) ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_SPEC_CNT ) ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_IND ) ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SPEC_ASSET_ID.PRTY_ASSET_SPEC_UOM_CD ) ) ) as chksum_lkp,
md5 ( ltrim ( rtrim ( exp_all_source.PRTY_ASSET_SPEC_VAL1 ) ) || ltrim ( rtrim ( exp_all_source.o_Start_Date ) ) || ltrim ( rtrim ( exp_all_source.Measure ) ) || ltrim ( rtrim ( exp_all_source.Count ) ) || ltrim ( rtrim ( exp_all_source.PRTY_ASSET_IND ) ) || ltrim ( rtrim ( exp_all_source.out_meas_typ_cd ) ) ) as chksum_inp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_inp != chksum_lkp THEN ''U'' ELSE ''R'' END END as o_flag,
exp_all_source.Measure as Measure,
exp_all_source.Rank as Rank,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_PRTY_ASSET_SPEC_ASSET_ID ON exp_all_source.source_record_id = LKP_PRTY_ASSET_SPEC_ASSET_ID.source_record_id
);


-- Component rtr_party_asset_spec_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_party_asset_spec_INSERT as
SELECT
exp_data_transformation.src_prty_asset_spec_type_cd as prty_asset_spec_type_cd,
exp_data_transformation.src__prty_asset_id as prty_asset_id,
exp_data_transformation.Count1 as prty_count,
exp_data_transformation.Measure as Measure,
exp_data_transformation.process_id as process_id,
exp_data_transformation.src_effective_date as src_effective_date,
exp_data_transformation.PRTY_ASSET_SPEC_VAL1 as PRTY_ASSET_SPEC_VAL12,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.Src_PRTY_ASSET_IND as PRTY_ASSET_IND,
exp_data_transformation.in_meas_typ_cd as in_meas_typ_cd,
exp_data_transformation.Rank as Rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE ( exp_data_transformation.o_flag = ''I'' or exp_data_transformation.o_flag = ''U'' ) and exp_data_transformation.src__prty_asset_id IS NOT NULL 
-- CASE WHEN lkp_prty_asset_id IS NULL THEN TRUE ELSE FALSE END 
-- and 
-- CASE WHEN exp_data_transformation.src__prty_asset_id IS NOT NULL THEN TRUE ELSE FALSE END
;


-- Component upd_party_asset_spec_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_party_asset_spec_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_party_asset_spec_INSERT.prty_asset_spec_type_cd as prty_asset_spec_type_cd,
rtr_party_asset_spec_INSERT.prty_asset_id as prty_asset_id,
rtr_party_asset_spec_INSERT.prty_count as prty_count,
rtr_party_asset_spec_INSERT.Measure as Measure,
rtr_party_asset_spec_INSERT.process_id as process_id,
rtr_party_asset_spec_INSERT.PRTY_ASSET_SPEC_VAL12 as PRTY_ASSET_SPEC_VAL11,
rtr_party_asset_spec_INSERT.src_effective_date as lkp_prty_asset_spec_strt_dttm1,
rtr_party_asset_spec_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_party_asset_spec_INSERT.PRTY_ASSET_IND as PRTY_ASSET_IND,
rtr_party_asset_spec_INSERT.in_meas_typ_cd as in_meas_typ_cd1,
rtr_party_asset_spec_INSERT.Rank as Rank1,
0 as UPDATE_STRATEGY_ACTION,
rtr_party_asset_spec_INSERT.source_record_id as source_record_id
FROM
rtr_party_asset_spec_INSERT
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_party_asset_spec_ins.prty_asset_id as prty_asset_id,
upd_party_asset_spec_ins.prty_asset_spec_type_cd as prty_asset_spec_type_cd,
upd_party_asset_spec_ins.lkp_prty_asset_spec_strt_dttm1 as out_prty_asset_strt_dttm,
upd_party_asset_spec_ins.process_id as process_id,
upd_party_asset_spec_ins.prty_count as prty_count,
upd_party_asset_spec_ins.Measure as Measure,
upd_party_asset_spec_ins.PRTY_ASSET_SPEC_VAL11 as PRTY_ASSET_SPEC_VAL11,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as PRTY_ASSET_SPEC_END_DTTM,
dateadd ( second, ( 2 * ( upd_party_asset_spec_ins.Rank1 - 1 ) ) ,CURRENT_TIMESTAMP ) as EDW_STRT_DTTM1,
upd_party_asset_spec_ins.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_party_asset_spec_ins.PRTY_ASSET_IND as PRTY_ASSET_IND,
upd_party_asset_spec_ins.in_meas_typ_cd1 as in_meas_typ_cd1,
upd_party_asset_spec_ins.source_record_id
FROM
upd_party_asset_spec_ins
);


-- Component PRTY_ASSET_SPEC_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET_SPEC
(
PRTY_ASSET_ID,
PRTY_ASSET_SPEC_TYPE_CD,
PRTY_ASSET_SPEC_STRT_DTTM,
PRTY_ASSET_SPEC_END_DTTM,
PRTY_ASSET_SPEC_VAL,
PRTY_ASSET_SPEC_MEAS,
PRTY_ASSET_SPEC_CNT,
PRTY_ASSET_SPEC_UOM_CD,
PRTY_ASSET_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_ins.prty_asset_id as PRTY_ASSET_ID,
exp_pass_to_target_ins.prty_asset_spec_type_cd as PRTY_ASSET_SPEC_TYPE_CD,
exp_pass_to_target_ins.out_prty_asset_strt_dttm as PRTY_ASSET_SPEC_STRT_DTTM,
exp_pass_to_target_ins.PRTY_ASSET_SPEC_END_DTTM as PRTY_ASSET_SPEC_END_DTTM,
exp_pass_to_target_ins.PRTY_ASSET_SPEC_VAL11 as PRTY_ASSET_SPEC_VAL,
exp_pass_to_target_ins.Measure as PRTY_ASSET_SPEC_MEAS,
exp_pass_to_target_ins.prty_count as PRTY_ASSET_SPEC_CNT,
exp_pass_to_target_ins.in_meas_typ_cd1 as PRTY_ASSET_SPEC_UOM_CD,
exp_pass_to_target_ins.PRTY_ASSET_IND as PRTY_ASSET_IND,
exp_pass_to_target_ins.process_id as PRCS_ID,
exp_pass_to_target_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_target_ins;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_pctl_dwellinglocationtype_hoe1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pctl_dwellinglocationtype_hoe1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Rank,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT pctl_dwellinglocationtype_hoe.PRIORITY_stg FROM DB_T_PROD_STAG.pctl_dwellinglocationtype_hoe WHERE 1=2 ORDER BY pctl_dwellinglocationtype_hoe.PRIORITY_stg
) SRC
)
);


-- Component PRTY_ASSET_SPEC_ins1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET_SPEC
(
PRTY_ASSET_ID
)
SELECT
SQ_pctl_dwellinglocationtype_hoe1.Rank as PRTY_ASSET_ID
FROM
SQ_pctl_dwellinglocationtype_hoe1;


-- PIPELINE END FOR 2
-- Component PRTY_ASSET_SPEC_ins1, Type Post SQL 
/*



UPDATE  PRTY_ASSET_SPEC  FROM  

(

SELECT	distinct PRTY_ASSET_ID,PRTY_ASSET_SPEC_TYPE_CD,EDW_STRT_DTTM

FROM	PRTY_ASSET_SPEC 

WHERE EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID,PRTY_ASSET_SPEC_TYPE_CD  ORDER BY EDW_STRT_DTTM DESC) >1

)  A

SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1 SECOND''

WHERE  PRTY_ASSET_SPEC.PRTY_ASSET_ID=A.PRTY_ASSET_ID

AND PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_TYPE_CD=A.PRTY_ASSET_SPEC_TYPE_CD

AND  PRTY_ASSET_SPEC.EDW_STRT_DTTM=A.EDW_STRT_DTTM



*/





UPDATE  db_t_prod_core.PRTY_ASSET_SPEC  
SET EDW_END_DTTM= A.LEAD1
FROM  

(

SELECT	distinct PRTY_ASSET_ID,PRTY_ASSET_SPEC_TYPE_CD,EDW_STRT_DTTM,

max( EDW_STRT_DTTM) over(partition by PRTY_ASSET_ID,PRTY_ASSET_SPEC_TYPE_CD ORDER BY EDW_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND  1 FOLLOWING)- INTERVAL ''1 SECOND''

AS LEAD1

FROM	db_t_prod_core.PRTY_ASSET_SPEC 

)  A


WHERE  PRTY_ASSET_SPEC.PRTY_ASSET_ID=A.PRTY_ASSET_ID

AND PRTY_ASSET_SPEC.PRTY_ASSET_SPEC_TYPE_CD=A.PRTY_ASSET_SPEC_TYPE_CD

AND  PRTY_ASSET_SPEC.EDW_STRT_DTTM=A.EDW_STRT_DTTM

AND LEAD1 IS NOT NULL;


END; ';