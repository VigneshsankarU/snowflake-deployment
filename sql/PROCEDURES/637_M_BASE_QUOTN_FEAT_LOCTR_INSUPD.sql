-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_FEAT_LOCTR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  PRCS_ID STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
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

-- Component SQ_pc_quotn_feat_loctr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_quotn_feat_loctr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
TGT_QUOTN_ID,
TGT_FEAT_ID,
TGT_QUOTN_FEAT_LOCTR_ROLE_CD,
TGT_QUOTN_FEAT_LOCTR_STRT_DTTM,
TGT_LOC_ID,
TGT_QUOTN_FEAT_LOCTR_AMT,
TGT_QUOTN_FEAT_LOCTR_END_DTTM,
TGT_EDW_END_DTTM,
SRC_QUOTN_ID,
 SRC_FEAT_ID,
 SRC_LOC_ID,
 SRC_QUOTN_FEAT_LOCTR_AMT,
 SRC_QUOTN_FEAT_LOCTR_STRT_DTTM,
 SRC_QUOTN_FEAT_LOCTR_END_DTTM,
 INS_UPD_FLAG,
 SRC_UPDATETIME,
 SRC_RETIRED,
 TGT_EDW_STRT_DTTM,
 SRC_QUOTN_FEAT_LOCTR_ROLE_CD,
 SRC_EDW_STRT_DTTM,
 SRC_EDW_END_DTTM,
 SOURCE_DATA,
 TARGET_DATA,
 source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT LKP_TGT.QUOTN_ID AS TGT_QUOTN_ID,

LKP_TGT.FEAT_ID AS TGT_FEAT_ID,

LKP_TGT.QUOTN_FEAT_LOCTR_ROLE_CD AS TGT_QUOTN_FEAT_LOCTR_ROLE_CD,

LKP_TGT.QUOTN_FEAT_LOCTR_STRT_DTTM AS TGT_QUOTN_FEAT_LOCTR_STRT_DTTM,

LKP_TGT.LOC_ID AS TGT_LOC_ID,

LKP_TGT.QUOTN_FEAT_LOCTR_AMT AS TGT_QUOTN_FEAT_LOCTR_AMT,

LKP_TGT.QUOTN_FEAT_LOCTR_END_DTTM AS TGT_QUOTN_FEAT_LOCTR_END_DTTM,

LKP_TGT.EDW_END_DTTM AS TGT_EDW_END_DTTM,

LKP_IQ.QUOTN_ID AS SRC_QUOTN_ID,

LKP_F.FEAT_ID AS SRC_FEAT_ID,

LKP_ST_ADD.STREET_ADDR_ID AS SRC_LOC_ID,

CASE WHEN TO_NUMBER(SRC.FEAT_AMT_STG) is not null then cast(SRC.FEAT_AMT_STG as decimal(18, 4)) else cast(''0.0000'' as decimal(18, 4))END as SRC_QUOTN_FEAT_LOCTR_AMT,

SRC.QUOTN_FEAT_LOCTR_STRT_DT_stg AS SRC_QUOTN_FEAT_LOCTR_STRT_DTTM,

SRC.QUOTN_FEAT_LOCTR_END_DT_stg AS SRC_QUOTN_FEAT_LOCTR_END_DTTM,

/*FLAG*/

/*CASE

WHEN (TGT_QUOTN_ID IS NULL OR TGT_FEAT_ID IS NULL OR TGT_LOC_ID IS NULL)  THEN ''I'' 

WHEN SOURCE_DATA <> TARGET_DATA THEN ''U'' 

ELSE ''R'' END AS INS_UPD_FLAG,*/

CASE WHEN SRC.UPDATETIME_stg IS null THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp)ELSE SRC.UPDATETIME_STG END as SRC_UPDATETIME ,

SRC.RETIRED_stg AS SRC_RETIRED,

LKP_TGT.EDW_STRT_DTTM AS TGT_EDW_STRT_DTTM,

COALESCE(SRC.QUOTN_FEAT_LOCTR_ROLE_TYPE_CD_STG,''UNK'') AS SRC_QUOTN_FEAT_LOCTR_ROLE_CD,

SRC.START_DTTM_stg AS SRC_EDW_STRT_DTTM,

SRC.END_DTTM_stg AS SRC_EDW_END_DTTM,

/*SOURCE DATA*/

CAST(TRIM(COALESCE(SRC_QUOTN_FEAT_LOCTR_ROLE_CD,0))||TRIM(CAST(SRC_QUOTN_FEAT_LOCTR_STRT_DTTM AS DATE ))||

TRIM(CAST(SRC_QUOTN_FEAT_LOCTR_END_DTTM AS DATE ))||TRIM

(CAST(COALESCE(SRC_QUOTN_FEAT_LOCTR_AMT,0) AS DECIMAL(18,4)))AS VARCHAR(100)) AS SOURCE_DATA,

/*TARGET DATA*/

CAST(TRIM(COALESCE(TGT_QUOTN_FEAT_LOCTR_ROLE_CD,0))||TRIM(CAST(TGT_QUOTN_FEAT_LOCTR_STRT_DTTM AS DATE ))||

TRIM(CAST(TGT_QUOTN_FEAT_LOCTR_END_DTTM AS DATE ))||TRIM

(CAST(COALESCE(TGT_QUOTN_FEAT_LOCTR_AMT,0) AS DECIMAL(18,4)))AS VARCHAR(100)) AS TARGET_DATA,

/*FLAG*/

CASE

WHEN (TGT_QUOTN_ID IS NULL OR TGT_FEAT_ID IS NULL OR TGT_LOC_ID IS NULL)  THEN ''I'' 

WHEN SOURCE_DATA <> TARGET_DATA THEN ''U'' 

ELSE ''R'' END AS INS_UPD_FLAG

FROM(

/* --------------------Source Query Starts-------------------------------- */
SELECT	DISTINCT POLICYNUMBER_stg POLICYNUMBER_stg, JOBNUMBER_stg JOBNUMBER_stg,BRANCHNUMBER_stg BRANCHNUMBER_stg,

		cast(FEAT_SBTYPE_CD_stg as varchar(50)) FEAT_SBTYPE_CD_stg,cast(FEAT_AMT_stg as  varchar(255)) FEAT_AMT_stg,

		ADDRESSBOOKUID_stg ADDRESSBOOKUID_stg, ADDRESSLINE1_stg ADDRESSLINE1_stg, ADDRESSLINE2_stg ADDRESSLINE2_stg, ADDRESSLINE3_stg ADDRESSLINE3_stg,

		COUNTY_stg COUNTY_stg , CITY_stg CITY_stg, tax_state_stg tax_state_stg, ctry_TYPECODE_stg ctry_TYPECODE_stg, POSTALCODE_stg POSTALCODE_stg ,

		PL_ADDRESSLINE1_stg PL_ADDRESSLINE1_stg, PL_ADDRESSLINE2_stg PL_ADDRESSLINE2_stg, PL_ADDRESSLINE3_stg PL_ADDRESSLINE3_stg ,

		PL_COUNTY_stg PL_COUNTY_stg ,PL_CITY_stg PL_CITY_stg, PCTL_STATE.TYPECODE_stg PL_STATE,

		PCTL_COUNTRY.TYPECODE_stg PL_COUNTRY, PL_POSTALCODE_stg PL_POSTALCODE_stg, TAX_CITY_stg TAX_CITY_stg,

		LOC_PUBLIC_ID_stg LOC_PUBLIC_ID_stg, NK_PUBLIC_ID_stg  NK_PUBLIC_ID_stg ,code_stg QUOTN_FEAT_LOCTR_ROLE_TYPE_CD_stg,

		startdate_stg QUOTN_FEAT_LOCTR_STRT_DT_stg, enddate_stg QUOTN_FEAT_LOCTR_END_DT_stg,

		cast(PC_QUOTN_FEAT_LOCTR_X.RETIRED_stg as varchar(10)) RETIRED_stg, CREATETIME_stg CREATETIME_stg ,

		CAST(1 AS INTEGER) CTL_ID_stg, ''EDW_ETL'' LOAD_USER_stg, CAST( START_DTTM_stg AS TIMESTAMP) START_DTTM_stg ,

		current_timestamp LOAD_DTTM_stg,UPDATETIME_stg UPDATETIME_stg ,CAST(END_DTTM_stg AS TIMESTAMP) END_DTTM_stg,

		''SRC_SYS4'' AS SRC_CD_stg,NULL AS VAL_TYP_CD_stg 


FROM	( 

	select	distinct pc_job.JobNumber_stg,pc_policyperiod.BranchNumber_stg,

			pc_policyperiod.PolicyNumber_stg, 

			case 

				when covterm.CovTermType_stg=''package'' then cast (''PACKAGE'' as varchar (50)) 

				when covterm.CovTermType_stg=''Option'' 

		and polcov.val_stg is not null then cast (''OPTIONS'' as varchar(50)) 

				when covterm.CovTermType_stg=''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast ( ''COVTERM'' as varchar (50)) 

			end as FEAT_SBTYPE_CD_stg, 

			case 

				when covterm.CovTermType_stg=''Option'' 

		and optn.ValueType_stg=''money'' then optn.Value_stg 

				when covterm.CovTermType_stg <>''option'' then polcov.val_stg 

			end as feat_amt_stg, 

			case 

				when optn.ValueType_stg=''count'' then optn.Value_stg 

			end as feat_qty_stg, 

			case 

				when optn.ValueType_stg in (''days'',''hours'',''other'') then optn.value_stg 

			end as feat_num_stg, 

			case 

				when optn.ValueType_stg=''percent'' then optn.Value_stg 

			end as feat_rate_stg, cast (null as varchar (50)) feat_effect_type_cd_stg,

			polcov.val_stg as feat_val_stg,covterm.CovTermType_stg as feat_CovTermType_stg ,

			covterm.clausetype_stg as FEAT_INSRNC_SBTYPE_CD_stg ,covterm.covname_stg FEAT_CLASFCN_CD_stg ,

			covterm.clausename_stg COMN_FEAT_NAME_stg ,(:start_dttm) as start_dttm_stg,

			(:end_dttm) as end_dttm_stg ,NULL as Eligible_stg ,polcov.patterncode_stg AS LOC_PUBLIC_ID_stg ,

			pc_address.AddressBookUID_stg ,pc_address.county_stg ,pc_address.postalcode_stg ,

			pc_address.city_stg ,pc_address.addressline1_stg ,pc_address.addressline2_stg ,

			pc_address.addressline3_stg ,pctl_state.typecode_stg AS state_TYPECODE_stg ,

			pctl_country.typecode_stg AS ctry_TYPECODE_stg ,pc_taxlocation.city_stg AS tax_city_stg ,

			''UNK'' as code_stg ,PCTL_JURISDICTION.TYPECODE_stg AS tax_state_stg ,

			pc_policylocation.countyinternal_stg PL_county_stg ,pc_policylocation.postalcodeinternal_stg PL_postalcode_stg ,

			pc_policylocation.cityinternal_stg PL_city_stg ,pc_policylocation.addressline1internal_stg PL_addressline1_stg ,

			pc_policylocation.addressline2internal_stg PL_addressline2_stg ,

			pc_policylocation.addressline3internal_stg PL_addressline3_stg ,

			pc_policylocation.stateinternal_stg PL_state_stg ,pc_policylocation.countryinternal_stg PL_country_stg ,

			case 

				when polcov.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polcov.EffectiveDate_stg 

			end as startdate_stg, 

			case 

				when polcov.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polcov.ExpirationDate_stg 

			end as enddate_stg, 

			case 

				when covterm.CovTermType_stg =''package'' then package.packagePatternID_stg 

				when covterm.CovTermType_stg=''Option'' 

		and polcov.val_stg is not null then optn.optionPatternID_stg 

				when covterm.CovTermType_stg=''Clause'' then covterm.clausePatternID_stg 

				else covterm.covtermPatternID_stg 

			end as nk_public_id_stg ,pc_policyperiod.PublicID_stg ,pc_policyperiod.Createtime_stg ,

			polcov.updatetime_stg ,pc_policyperiod.Retired_stg AS Retired_stg ,

			polcov.columnname_stg 

	from	( /*pcx_bp7locationcov*/ 

	SELECT	''ChoiceTerm1'' AS columnname_stg ,ChoiceTerm1_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS choiceterm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	ChoiceTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm2'' AS columnname_stg ,ChoiceTerm2_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	ChoiceTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm3'' AS columnname_stg ,ChoiceTerm3_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	ChoiceTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm4'' AS columnname_stg ,ChoiceTerm4_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	ChoiceTerm4Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm5'' AS columnname_stg ,ChoiceTerm5_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	ChoiceTerm5Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm1'' AS columnname_stg ,cast(DirectTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	DirectTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm2'' AS columnname_stg ,cast(DirectTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	DirectTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm3'' AS columnname_stg ,cast(DirectTerm3_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	DirectTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm1'' AS columnname_stg ,cast(BooleanTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	BooleanTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm2'' AS columnname_stg ,cast(BooleanTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	BooleanTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm3'' AS columnname_stg ,cast(BooleanTerm3_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	BooleanTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm1'' AS columnname_stg ,cast(StringTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	StringTerm1Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm2'' AS columnname_stg ,cast(StringTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	StringTerm2Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm3'' AS columnname_stg ,cast(StringTerm3_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	StringTerm3Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''PositiveIntTerm1'' AS columnname_stg ,cast(PositiveIntTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	PositiveIntTerm1Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''PositiveIntTerm2'' AS columnname_stg ,cast(PositiveIntTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	PositiveIntTerm2Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm1'' AS columnname_stg ,cast(DateTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	DateTerm1Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm2'' AS columnname_stg ,cast(DateTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	WHERE	DateTerm2Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''Clause'' AS columnname_stg ,cast(NULL AS VARCHAR(255)) val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcov 

	UNION 

	SELECT	''ChoiceTerm1'' AS columnname_stg ,ChoiceTerm1_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS choiceterm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	ChoiceTerm1Avl_stg=1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm2'' AS columnname_stg ,ChoiceTerm2_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	ChoiceTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm3'' AS columnname_stg ,ChoiceTerm3_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	ChoiceTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm4'' AS columnname_stg ,ChoiceTerm4_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype_stg ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,NULL AS patternid_stg ,

			updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	ChoiceTerm4Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm5'' AS columnname_stg ,ChoiceTerm5_stg AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,NULL AS patternid_stg ,

			updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	ChoiceTerm5Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm1'' AS columnname_stg ,cast(DirectTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	DirectTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm2'' AS columnname_stg ,cast(DirectTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	DirectTerm2Avl_stg= 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm3'' AS columnname_stg ,cast(DirectTerm3_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	DirectTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm1'' AS columnname_stg ,cast(BooleanTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	BooleanTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm2'' AS columnname_stg ,cast(BooleanTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	BooleanTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm3'' AS columnname_stg ,cast(BooleanTerm3_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype_stg ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,NULL AS patternid_stg ,

			updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	BooleanTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm1'' AS columnname_stg ,cast(DateTerm1_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	DateTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm2'' AS columnname_stg ,cast(DateTerm2_stg AS VARCHAR(255)) AS val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	WHERE	DateTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''Clause'' AS columnname_stg ,cast(NULL AS VARCHAR(255)) val_stg ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId_stg ,

			cast(Location_stg AS INTEGER) AS assetkey_stg ,''bp7location'' AS assettype_stg ,

			createtime_stg ,EffectiveDate_stg ,ExpirationDate_stg ,NULL AS ChoiceTerm1_stg ,

			NULL AS patternid_stg ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationcond 

	UNION 

	SELECT	''ChoiceTerm1'' AS columnname ,ChoiceTerm1_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS choiceterm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	ChoiceTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm2'' AS columnname ,ChoiceTerm2_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	ChoiceTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm3'' AS columnname ,ChoiceTerm3_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	ChoiceTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm4'' AS columnname ,ChoiceTerm4_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	ChoiceTerm4Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''ChoiceTerm5'' AS columnname ,ChoiceTerm5_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	ChoiceTerm5Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm1'' AS columnname ,cast(DirectTerm1_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	DirectTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm2'' AS columnname ,cast(DirectTerm2_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	DirectTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DirectTerm3'' AS columnname ,cast(DirectTerm3_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	DirectTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm1'' AS columnname ,cast(BooleanTerm1_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	BooleanTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm2'' AS columnname ,cast(BooleanTerm2_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	BooleanTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''BooleanTerm3'' AS columnname ,cast(BooleanTerm3_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	BooleanTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm1'' AS columnname ,StringTerm1_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	StringTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm2'' AS columnname ,StringTerm2_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	StringTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''StringTerm3'' AS columnname ,StringTerm3_stg AS val ,patterncode_stg ,

			cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	StringTerm3Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm1'' AS columnname ,cast(DateTerm1_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	DateTerm1Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''DateTerm2'' AS columnname ,cast(DateTerm2_stg AS VARCHAR(255)) AS val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 

	WHERE	DateTerm2Avl_stg = 1 

		AND ExpirationDate_stg IS NULL 

	UNION 

	SELECT	''Clause'' AS columnname ,cast(NULL AS VARCHAR(255)) val ,

			patterncode_stg ,cast(BranchID_stg AS INTEGER) AS BranchId ,cast(Location_stg AS INTEGER) AS assetkey ,

			''bp7location'' AS assettype ,createtime_stg ,EffectiveDate_stg ,

			ExpirationDate_stg ,NULL AS ChoiceTerm1 ,NULL AS patternid ,updatetime_stg 

	FROM	DB_T_PROD_STAG.pcx_bp7locationexcl 



	) polcov 

	INNER JOIN ( 

		SELECT	cast(id_stg AS INTEGER) AS id_stg ,PolicyNumber_stg ,BranchNumber_stg ,

				PeriodStart_stg ,PeriodEnd_stg ,MostRecentModel_stg ,STATUS_stg ,

				JobID_stg ,PublicID_stg ,Createtime_stg ,updatetime_stg ,Retired_stg ,

				PolicyID_stg 

		FROM	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		ON pc_policyperiod.id_stg = polcov.BranchID_stg 

	LEFT JOIN ( 

		SELECT	pc_etlclausepattern.PatternID_stg clausePatternID_stg ,

				pc_etlcovtermpattern.PatternID_stg covtermPatternID_stg ,pc_etlcovtermpattern.ColumnName_stg ,

				pc_etlcovtermpattern.name_stg as covname_stg ,pc_etlcovtermpattern.CovTermType_stg ,

				pc_etlclausepattern.NAME_stg clausename_stg ,pc_etlclausepattern.clausetype_stg clausetype_stg 

		FROM	DB_T_PROD_STAG.pc_etlclausepattern 

		INNER JOIN DB_T_PROD_STAG.pc_etlcovtermpattern 

			ON pc_etlclausepattern.id_stg = pc_etlcovtermpattern.ClausePatternID_stg 

		UNION 

		SELECT	pc_etlclausepattern.PatternID_stg clausePatternID_stg ,

				pc_etlcovtermpattern.PatternID_stg covtermPatternID_stg ,coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') columnname_stg ,pc_etlcovtermpattern.name_stg as covname_stg ,

				coalesce(pc_etlcovtermpattern.CovTermType_stg, ''Clause'') covtermtype_stg ,

				pc_etlclausepattern.NAME_stg clausename_stg ,pc_etlclausepattern.clausetype_stg clausetype_stg 

		FROM	DB_T_PROD_STAG.pc_etlclausepattern 

		LEFT JOIN ( 

			SELECT	* 

			FROM	DB_T_PROD_STAG.pc_etlcovtermpattern 

			WHERE	NAME_stg NOT LIKE ''ZZ%'' ) pc_etlcovtermpattern 

			ON pc_etlcovtermpattern.ClausePatternID_stg = pc_etlclausepattern.ID_stg 

		WHERE	pc_etlclausepattern.NAME_stg NOT LIKE ''ZZ%'' 

			AND pc_etlcovtermpattern.NAME_stg IS NULL 

			AND pc_etlclausepattern.OwningEntityType_stg =''BP7Location'' ) covterm 

		ON covterm.clausePatternID_stg = polcov.PatternCode_stg 

		AND covterm.ColumnName_stg = polcov.columnname_stg 

	LEFT JOIN ( 

		SELECT	pc_etlcovtermpackage.PatternID_stg packagePatternID_stg ,

				pc_etlcovtermpackage.PackageCode_stg cov_id_stg ,pc_etlcovtermpackage.PackageCode_stg NAME_stg 

		FROM	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		ON package.packagePatternID_stg = polcov.val_stg 

	LEFT JOIN ( 

		SELECT	pc_etlcovtermoption.PatternID_stg optionPatternID_stg ,

				pc_etlcovtermoption.optioncode_stg NAME_stg ,cast(pc_etlcovtermoption.value_stg AS VARCHAR(255)) AS value_stg ,

				pc_etlcovtermpattern.ValueType_stg 

		FROM	DB_T_PROD_STAG.pc_etlcovtermpattern 

		INNER JOIN DB_T_PROD_STAG.pc_etlcovtermoption 

			ON pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		ON optn.optionPatternID_stg = polcov.val_stg 

	inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg 

	inner join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg=pc_policyperiod.JobID_stg 

	inner join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg=pc_job.Subtype_stg 

	INNER JOIN DB_T_PROD_STAG.pcx_bp7location 

		ON pcx_bp7location.BranchID_stg=pc_policyperiod.ID_stg 

		and pcx_bp7location.fixedID_stg=polcov.assetkey_stg 

	INNER JOIN DB_T_PROD_STAG.pc_policylocation 

		ON pc_policylocation.id_stg = pcx_bp7location.Location_stg 

	INNER JOIN DB_T_PROD_STAG.pc_address 

		ON pc_policylocation.AccountLocation_stg= pc_address.Id_stg 

	LEFT JOIN DB_T_PROD_STAG.PCTL_STATE 

		ON pc_address.STATE_stg = pctl_state.id_stg 

	LEFT JOIN DB_T_PROD_STAG.PCTL_COUNTRY 

		ON pctl_country.id_stg = pc_address.country_stg 

	LEFT JOIN ( 

		SELECT	DISTINCT pc_contact.primaryaddressid_stg ,pctl_contact.typecode_stg 

		FROM	DB_T_PROD_STAG.pc_contact 

		INNER JOIN DB_T_PROD_STAG.pctl_contact 

			ON pc_contact.subtype_stg = pctl_contact.id_stg 

			AND pctl_contact.typecode_stg = ''LegalVenue'' ) contact 

		ON contact.primaryaddressid_stg = pc_address.id_stg 

	LEFT JOIN DB_T_PROD_STAG.pc_taxlocation 

		ON pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg 

	LEFT JOIN DB_T_PROD_STAG.PCTL_JURISDICTION 

		ON PCTL_JURISDICTION.ID_stg = pc_taxlocation.STATE_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'') 

		and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary'' 

		and pc_policyperiod.updatetime_stg> (:start_dttm) 

		and pc_policyperiod.updatetime_stg <= (:end_dttm))PC_QUOTN_FEAT_LOCTR_X 

INNER JOIN DB_T_PROD_STAG.PCTL_STATE 

	ON PC_QUOTN_FEAT_LOCTR_X.PL_STATE_stg=PCTL_STATE.ID_stg 

INNER JOIN DB_T_PROD_STAG.PCTL_COUNTRY 

	ON PC_QUOTN_FEAT_LOCTR_X.PL_COUNTRY_stg=PCTL_COUNTRY.ID_stg

	union 





SELECT  DISTINCT POLICYNUMBER, jobnumber_stg,

		branchnumber_stg, FEAT_SBTYPE_CD,cast(FEAT_AMT as  varchar(255)) FEAT_AMT,ADDRESSBOOKUID, ADDRESSLINE1, ADDRESSLINE2, ADDRESSLINE3,

        COUNTY, CITY,STATE , COUNTRY, POSTALCODE, PL_ADDRESSLINE1, PL_ADDRESSLINE2,

        PL_ADDRESSLINE3, PL_COUNTY, PL_CITY, PL_STATE, PL_COUNTRY, PL_POSTALCODE,

        TAX_CITY, LOC_PUBLICID, NK_PUBLICID, QUOTN_FEAT_LOCTR_ROLE_TYPE_CD,

        QUOTN_FEAT_LOCTR_STRT_DT, QUOTN_FEAT_LOCTR_END_DT, cast(RETIRED as VARCHAR(60))RETIRED, CREATETIME,

        cast(1 as INTEGER)CTL_ID, ''EDW_ETL'' LOAD_USER, cast(START_DTTM as TIMESTAMP(6)) START_DTTM, current_timestamp LOAD_DTTM, UPDATETIME,

        cast(END_DTTM as TIMESTAMP(6))END_DTTM,

''SRC_SYS4'' AS  SRC_CD, NULL  as VAL_TYP_CD



FROM    



(SELECT DISTINCT 

        pc_policyperiod.PolicyNumber,jobnumber_stg,

		branchnumber_stg,

''FEAT_SBTYPE15'' FEAT_SBTYPE_CD,      

null feat_amt

    ,(:start_dttm)as START_DTTM

    ,(:end_dttm)as  END_DTTM

    ,assetkey AS LOC_PUBLICID

    ,pc_address.AddressBookUID_stg as ADDRESSBOOKUID

    ,pc_address.county_stg as COUNTY

    ,pc_address.postalcode_stg as POSTALCODE

    ,pc_address.city_stg as CITY

    ,pc_address.addressline1_stg as ADDRESSLINE1

    ,pc_address.addressline2_stg as ADDRESSLINE2

    ,pc_address.addressline3_stg as ADDRESSLINE3

    ,pctl_country.typecode_stg AS COUNTRY

    ,pc_taxlocation.city_stg AS tax_city

    ,cast(''UNK'' as varchar(50))as QUOTN_FEAT_LOCTR_ROLE_TYPE_CD

    ,PCTL_JURISDICTION.TYPECODE_stg AS STATE 

    ,pc_policylocation.countyinternal_stg as PL_county

    ,pc_policylocation.postalcodeinternal_stg as PL_postalcode

    ,pc_policylocation.cityinternal_stg as PL_city

    ,pc_policylocation.addressline1internal_stg as PL_addressline1

    ,pc_policylocation.addressline2internal_stg as PL_addressline2

    ,pc_policylocation.addressline3internal_stg as PL_addressline3

    ,pc_policylocation.stateinternal_stg as PL_state1

    ,pc_policylocation.countryinternal_stg as PL_country1

    ,PCTL_STATE.TYPECODE_stg as PL_STATE

    ,PCTL_COUNTRY.TYPECODE_stg as PL_COUNTRY,

CASE     

    WHEN    polcov.EffectiveDate IS NULL

            THEN pc_policyperiod.PeriodStart

        ELSE polcov.EffectiveDate

        END QUOTN_FEAT_LOCTR_STRT_DT,

CASE    WHEN polcov.ExpirationDate IS NULL

            THEN pc_policyperiod.PeriodEnd

        ELSE polcov.ExpirationDate

        END QUOTN_FEAT_LOCTR_END_DT,

patterncode NK_PUBLICID 

    ,pc_policyperiod.PublicID

    ,pc_policyperiod.Createtime 

    ,polcov.updatetime

    ,pc_policyperiod.Retired AS Retired

    ,cast(''1'' as varchar(10)) as CTL_ID

    ,(''$P_LOAD_USER'') as LOAD_USER

    ,cast(current_timestamp as TIMESTAMP(6))as LOAD_DTTM

FROM    (

    select NULL as columnname,

NULL val,

formpatterncode_stg as patterncode

,cast(a.BranchID_stg  AS VARCHAR(255)) AS BranchId

,cast(a.Location_stg AS VARCHAR(255)) AS assetkey

,a.createtime_stg as createtime

,a.EffectiveDate_stg as EffectiveDate

,a.ExpirationDate_stg as ExpirationDate

,a.updatetime_stg as updatetime

from DB_T_PROD_STAG.pcx_bp7locationcov a

join DB_T_PROD_STAG.pc_policyperiod b on b.id_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_formpattern c on c.clausepatterncode_stg = a.patterncode_stg

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg

    inner join DB_T_PROD_STAG.pctl_documenttype pd 

        on pd.id_stg = c.DocumentType_stg  

    and     pd.typecode_stg = ''endorsement_alfa''

where ( (a.EffectiveDate_stg is null) or( a.EffectiveDate_stg > b.ModelDate_stg and coalesce( a.EffectiveDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))

<> coalesce(a.ExpirationDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))))

and d.RemovedorSuperseded_stg is null

)polcov

INNER JOIN (

    SELECT cast(id_stg AS VARCHAR(255)) AS id

        ,PolicyNumber_stg as PolicyNumber

        ,PeriodStart_stg as PeriodStart

        ,PeriodEnd_stg as PeriodEnd,

		branchnumber_stg

        ,STATUS_stg as  STATUS

        ,JobID_stg as JobID

        ,PublicID_stg as PublicID

        ,Createtime_stg as Createtime

        ,updatetime_stg as updatetime

        ,Retired_stg as Retired

        ,PolicyID_stg as PolicyID

    FROM DB_T_PROD_STAG.pc_policyperiod

    ) pc_policyperiod 

    ON  pc_policyperiod.id = polcov.BranchID

INNER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus 

    ON  pctl_policyperiodstatus.id_stg = pc_policyperiod.STATUS

	and STATUS<>2

INNER JOIN DB_T_PROD_STAG.pc_job 

    ON  pc_job.id_stg = pc_policyperiod.JobID

INNER JOIN DB_T_PROD_STAG.pctl_job 

    ON  pctl_job.id_stg = pc_job.Subtype_stg

INNER JOIN (select cast(BranchID_stg as varchar(100))as BranchID_stg,cast(FixedID_stg as varchar(100))as FixedID_stg,Location_stg from DB_T_PROD_STAG.pcx_bp7location ) pcx_bp7location  

    ON  pcx_bp7location.BranchID_stg =pc_policyperiod.ID 

    and pcx_bp7location.FixedID_stg =polcov.assetkey

INNER JOIN DB_T_PROD_STAG.pc_policylocation  

    ON  pc_policylocation.id_stg = pcx_bp7location.Location_stg

INNER JOIN DB_T_PROD_STAG.pc_address  

    ON  pc_policylocation.AccountLocation_stg= pc_address.Id_stg

LEFT JOIN DB_T_PROD_STAG.PCTL_STATE 

    ON  pc_address.STATE_stg = pctl_state.id_stg

LEFT JOIN DB_T_PROD_STAG.PCTL_COUNTRY 

    ON  pctl_country.id_stg = pc_address.country_stg

LEFT JOIN (

    SELECT DISTINCT pc_contact.primaryaddressid_stg

        ,pctl_contact.typecode_stg

    FROM DB_T_PROD_STAG.pc_contact

    INNER JOIN DB_T_PROD_STAG.pctl_contact 

    ON  pc_contact.subtype_stg = pctl_contact.id_stg

        AND pctl_contact.typecode_stg = ''LegalVenue''

    ) contact 

    ON  contact.primaryaddressid_stg = pc_address.id_stg

LEFT JOIN DB_T_PROD_STAG.pc_taxlocation 

    ON  pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg

LEFT JOIN DB_T_PROD_STAG.PCTL_JURISDICTION 

    ON  PCTL_JURISDICTION.ID_stg = pc_taxlocation.STATE_stg

    where pc_policyperiod.updatetime > (:start_dttm)

AND pc_policyperiod.updatetime <= (:end_dttm)

    )PC_QUOTN_FEAT_LOCTR_X

INNER JOIN (select cast(ID_stg as varchar(100))as ID from DB_T_PROD_STAG.PCTL_STATE ) PCTL_STATE ON PC_QUOTN_FEAT_LOCTR_X.PL_STATE1=PCTL_STATE.ID

INNER JOIN (select cast(ID_stg as varchar(100))as ID from  db_t_prod_stag.PCTL_COUNTRY) PCTL_COUNTRY ON PC_QUOTN_FEAT_LOCTR_X.PL_COUNTRY1=PCTL_COUNTRY.ID

QUALIFY ROW_NUMBER() OVER (PARTITION BY PUBLICID,NK_PUBLICID,FEAT_SBTYPE_CD,PL_ADDRESSLINE1,PL_ADDRESSLINE2,PL_ADDRESSLINE3,PL_COUNTY,PL_CITY,PL_STATE,PL_POSTALCODE,PL_COUNTRY

ORDER BY UPDATETIME DESC)=1

)SRC



/* -LKP XLAT SRC_CD */
LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_SRC_CD 

ON XLAT_SRC_CD.SRC_IDNTFTN_VAL=SRC.SRC_CD_STG

AND XLAT_SRC_CD.TGT_IDNTFTN_NM= ''SRC_SYS'' 

AND XLAT_SRC_CD.SRC_IDNTFTN_NM= ''derived'' 

AND XLAT_SRC_CD.SRC_IDNTFTN_SYS=''DS'' 

AND XLAT_SRC_CD.EXPN_DT=''9999-12-31''

/* LKP XLAT FEAT_SBTYPE_CD */
LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_FEAT_SBTYPE_CD

ON XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_VAL=(Case 

                    When SRC.FEAT_SBTYPE_CD_STG = ''MODIFIER'' THEN ''FEAT_SBTYPE11'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''OPTIONS'' THEN ''FEAT_SBTYPE8'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''COVTERM'' THEN ''FEAT_SBTYPE6'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''CLAUSE'' THEN ''FEAT_SBTYPE7'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''PACKAGE'' THEN ''FEAT_SBTYPE9'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''CL'' THEN ''FEAT_SBTYPE7'' 

                    When SRC.FEAT_SBTYPE_CD_STG = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15'' else ''UNK'' End)

AND XLAT_FEAT_SBTYPE_CD.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

AND XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_NM= ''derived'' 

AND XLAT_FEAT_SBTYPE_CD.SRC_IDNTFTN_SYS=''DS'' 

AND XLAT_FEAT_SBTYPE_CD.EXPN_DT=''9999-12-31''

/* -LKP XLAT VAL_TYP_CD */
LEFT OUTER JOIN DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT AS XLAT_VAL_TYP_CD

ON COALESCE(XLAT_VAL_TYP_CD.SRC_IDNTFTN_VAL,''UNK'')=COALESCE(SRC.VAL_TYP_CD_STG,''UNK'')

AND XLAT_VAL_TYP_CD.TGT_IDNTFTN_NM= ''VAL_TYPE'' 

AND XLAT_VAL_TYP_CD.SRC_IDNTFTN_NM= ''cctl_coveragebasis.name'' 

AND XLAT_VAL_TYP_CD.SRC_IDNTFTN_SYS=''GW'' 

AND XLAT_VAL_TYP_CD.EXPN_DT=''9999-12-31''

/* LKP_CTRY */
LEFT OUTER JOIN (select ctry_id,geogrcl_area_shrt_name from DB_T_PROD_CORE.CTRY  CTRY

QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY geogrcl_area_shrt_name

            ORDER BY edw_end_dttm DESC)=1) LKP_CTRY

ON LKP_CTRY.geogrcl_area_shrt_name=SRC.PL_COUNTRY

/* LKP DB_T_PROD_CORE.TERR */
LEFT OUTER JOIN(select terr_id,ctry_id,geogrcl_area_shrt_name from DB_T_PROD_CORE.TERR TERR

WHERE EDW_END_DTTM=''9999-12-31 23:59:59.999999''

QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY ctry_id,geogrcl_area_shrt_name

            ORDER BY edw_end_dttm DESC)=1) LKP_TERR

ON LKP_TERR.geogrcl_area_shrt_name= SRC.PL_STATE and lkp_terr.ctry_id=lkp_ctry.ctry_id

/* LKP_CNTY */
LEFT OUTER JOIN (select cnty_id,terr_id,geogrcl_area_shrt_name from DB_T_PROD_CORE.CNTY CNTY

WHERE EDW_END_DTTM=''9999-12-31 23:59:59.999999''

QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY terr_id,geogrcl_area_shrt_name

            ORDER BY edw_end_dttm DESC)=1) lkp_cnty

ON LKP_CNTY.geogrcl_area_shrt_name=src.pl_county_stg and lkp_cnty.terr_id=lkp_terr.terr_id

/* LKP DB_T_PROD_CORE.POSTL_CD */
LEFT OUTER JOIN (SELECT PC.POSTL_CD_ID,PC.CTRY_ID,pc.POSTL_CD_NUM

FROM DB_T_PROD_CORE.POSTL_CD PC 

WHERE EDW_END_DTTM=''9999-12-31 23:59:59.999999''

QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY POSTL_CD_NUM,CTRY_ID 

            ORDER BY edw_end_dttm DESC)=1)LKP_POSTL_CD

ON LKP_POSTL_CD.POSTL_CD_NUM=SRC.PL_POSTALCODE_STG and LKP_POSTL_CD.CTRY_ID=LKP_CTRY.CTRY_ID

/* LKP_CITY */
LEFT OUTER JOIN (SELECT c.city_id,c.terr_id,c.geogrcl_area_shrt_name from  DB_T_PROD_CORE.CITY C

WHERE EDW_END_DTTM=''9999-12-31 23:59:59.999999''

QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY c.terr_id,c.geogrcl_area_shrt_name

            ORDER BY edw_end_dttm DESC)=1

) LKP_CITY

ON UPPER(LKP_CITY.geogrcl_area_shrt_name)=UPPER(SRC.PL_city_stg) and lkp_city.TERR_ID=LKP_terr.TERR_ID

/* -LKP STREET_ADDRESSS */
LEFT OUTER JOIN

(SELECT distinct STREET_ADDR.STREET_ADDR_ID as STREET_ADDR_ID, STREET_ADDR.DWLNG_TYPE_CD as DWLNG_TYPE_CD,

        STREET_ADDR.CARIER_RTE_TXT as CARIER_RTE_TXT, STREET_ADDR.SPTL_PNT as SPTL_PNT,

        STREET_ADDR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, STREET_ADDR.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD,

        STREET_ADDR.GEOCODE_STS_TYPE_CD as GEOCODE_STS_TYPE_CD, STREET_ADDR.ADDR_STDZN_TYPE_CD as ADDR_STDZN_TYPE_CD,

        STREET_ADDR.PRCS_ID as PRCS_ID, STREET_ADDR.EDW_STRT_DTTM as EDW_STRT_DTTM,

        STREET_ADDR.EDW_END_DTTM as EDW_END_DTTM, STREET_ADDR.ADDR_LN_1_TXT as ADDR_LN_1_TXT,

        STREET_ADDR.ADDR_LN_2_TXT as ADDR_LN_2_TXT, STREET_ADDR.ADDR_LN_3_TXT as ADDR_LN_3_TXT,

        STREET_ADDR.CITY_ID as CITY_ID, STREET_ADDR.TERR_ID as TERR_ID,

        STREET_ADDR.POSTL_CD_ID as POSTL_CD_ID, STREET_ADDR.CTRY_ID as CTRY_ID,

        STREET_ADDR.CNTY_ID as CNTY_ID 

FROM    DB_T_PROD_CORE.STREET_ADDR 

WHERE STREET_ADDR.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

AND STREET_ADDR_ID IS NOT NULL /* IS NOT NULL FILTER CONDITION FOR LOC_ID */
qualify row_number () over (

partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT, CITY_ID ,

        TERR_ID,POSTL_CD_ID,CTRY_ID ,CNTY_ID 

order by EDW_END_DTTM desc)=1)LKP_ST_ADD 

ON UPPER(LKP_ST_ADD.ADDR_LN_1_TXT)=UPPER(SRC.PL_ADDRESSLINE1_STG)

AND COALESCE(UPPER(LKP_ST_ADD.ADDR_LN_2_TXT),''UNK'')=COALESCE(UPPER(SRC.PL_ADDRESSLINE2_STG),''UNK'')

AND COALESCE(UPPER(LKP_ST_ADD.ADDR_LN_3_TXT),''UNK'')=COALESCE(UPPER(SRC.PL_ADDRESSLINE3_STG),''UNK'')

AND LKP_ST_ADD.CITY_ID=LKP_CITY.CITY_ID

AND LKP_ST_ADD.TERR_ID=LKP_TERR.TERR_ID

AND LKP_ST_ADD.POSTL_CD_ID=LKP_POSTL_CD.POSTL_CD_ID

AND LKP_ST_ADD.CTRY_ID=LKP_CTRY.CTRY_ID

AND COALESCE(LKP_ST_ADD.CNTY_ID,''UNK'')=COALESCE(LKP_CNTY.CNTY_ID,''UNK'')

/* -LKP DB_T_PROD_CORE.INSRNC_QUOTN */
LEFT OUTER JOIN(SELECT  INSRNC_QUOTN.QUOTN_ID as QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR as NK_JOB_NBR,

                    INSRNC_QUOTN.VERS_NBR as VERS_NBR, INSRNC_QUOTN.SRC_SYS_CD as SRC_SYS_CD 

            FROM    DB_T_PROD_CORE.INSRNC_QUOTN INSRNC_QUOTN

            QUALIFY ROW_NUMBER () OVER ( 

            PARTITION BY NK_JOB_NBR,VERS_NBR 

            ORDER BY edw_end_dttm DESC)=1) LKP_IQ

ON LKP_IQ.NK_JOB_NBR=SRC.JOBNUMBER_STG AND LKP_IQ.VERS_NBR=SRC.BRANCHNUMBER_STG  AND  LKP_IQ.SRC_SYS_CD=''GWPC''

/* -LKP DB_T_PROD_CORE.FEAT */
LEFT OUTER JOIN

(SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,

        FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC,

        FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME,

        FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,

        FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID,

        FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY 

FROM    DB_T_PROD_CORE.FEAT

QUALIFY ROW_NUMBER () OVER (

PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  

ORDER BY edw_end_dttm DESC)=1)LKP_F ON LKP_F.FEAT_SBTYPE_CD=XLAT_FEAT_SBTYPE_CD.TGT_IDNTFTN_VAL AND LKP_F.NK_SRC_KEY=SRC.NK_PUBLIC_ID_STG

/* ---------------------- /*Source Query Ends*/ ------------------------------------------------- */
/*Target Query*/

/* -LKP TGT */
LEFT OUTER JOIN

(

SELECT  QUOTN_FEAT_LOCTR.QUOTN_FEAT_LOCTR_ROLE_CD as QUOTN_FEAT_LOCTR_ROLE_CD,

        QUOTN_FEAT_LOCTR.QUOTN_FEAT_LOCTR_STRT_DTTM as QUOTN_FEAT_LOCTR_STRT_DTTM,

        QUOTN_FEAT_LOCTR.QUOTN_FEAT_LOCTR_AMT as QUOTN_FEAT_LOCTR_AMT,

        QUOTN_FEAT_LOCTR.QUOTN_FEAT_LOCTR_END_DTTM as QUOTN_FEAT_LOCTR_END_DTTM,

        QUOTN_FEAT_LOCTR.EDW_STRT_DTTM as EDW_STRT_DTTM, QUOTN_FEAT_LOCTR.EDW_END_DTTM as EDW_END_DTTM,

        QUOTN_FEAT_LOCTR.QUOTN_ID as QUOTN_ID, QUOTN_FEAT_LOCTR.FEAT_ID as FEAT_ID,

        QUOTN_FEAT_LOCTR.LOC_ID as LOC_ID 

FROM    DB_T_PROD_CORE.QUOTN_FEAT_LOCTR QUOTN_FEAT_LOCTR

) LKP_TGT ON LKP_TGT.QUOTN_ID=LKP_IQ.QUOTN_ID AND LKP_TGT.FEAT_ID=LKP_F.FEAT_ID

AND  LKP_TGT.LOC_ID=LKP_ST_ADD.STREET_ADDR_ID
) SRC
)
/*QUALIFY	ROW_NUMBER() OVER (

PARTITION BY JOBNUMBER_stg, BRANCHNUMBER_stg,FEAT_SBTYPE_CD_stg,

		NK_PUBLIC_ID_stg,PL_ADDRESSLINE1_stg,PL_ADDRESSLINE2_stg, PL_ADDRESSLINE3_stg,

		PL_COUNTY_stg,PL_CITY_stg,PL_STATE_stg,PL_POSTALCODE_stg,PL_COUNTRY_stg 

ORDER BY UPDATETIME_stg DESC)=1 */

);


-- Component exp_pass_from_source_imp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source_imp AS
(
SELECT
SQ_pc_quotn_feat_loctr_x.TGT_QUOTN_ID as TGT_QUOTN_ID,
SQ_pc_quotn_feat_loctr_x.TGT_FEAT_ID as TGT_FEAT_ID,
SQ_pc_quotn_feat_loctr_x.TGT_QUOTN_FEAT_LOCTR_ROLE_CD as TGT_QUOTN_FEAT_LOCTR_ROLE_CD,
SQ_pc_quotn_feat_loctr_x.TGT_QUOTN_FEAT_LOCTR_STRT_DTTM as TGT_QUOTN_FEAT_LOCTR_STRT_DTTM,
SQ_pc_quotn_feat_loctr_x.TGT_LOC_ID as TGT_LOC_ID,
SQ_pc_quotn_feat_loctr_x.TGT_QUOTN_FEAT_LOCTR_AMT as TGT_QUOTN_FEAT_LOCTR_AMT,
SQ_pc_quotn_feat_loctr_x.TGT_QUOTN_FEAT_LOCTR_END_DTTM as TGT_QUOTN_FEAT_LOCTR_END_DTTM,
SQ_pc_quotn_feat_loctr_x.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_pc_quotn_feat_loctr_x.SRC_QUOTN_ID as SRC_QUOTN_ID,
SQ_pc_quotn_feat_loctr_x.SRC_FEAT_ID as SRC_FEAT_ID,
SQ_pc_quotn_feat_loctr_x.SRC_LOC_ID as SRC_LOC_ID,
SQ_pc_quotn_feat_loctr_x.SRC_QUOTN_FEAT_LOCTR_AMT as SRC_QUOTN_FEAT_LOCTR_AMT,
SQ_pc_quotn_feat_loctr_x.SRC_QUOTN_FEAT_LOCTR_STRT_DTTM as SRC_QUOTN_FEAT_LOCTR_STRT_DTTM,
SQ_pc_quotn_feat_loctr_x.SRC_QUOTN_FEAT_LOCTR_END_DTTM as SRC_QUOTN_FEAT_LOCTR_END_DTTM,
SQ_pc_quotn_feat_loctr_x.INS_UPD_FLAG as INS_UPD_FLAG,
SQ_pc_quotn_feat_loctr_x.SRC_UPDATETIME as SRC_UPDATETIME,
SQ_pc_quotn_feat_loctr_x.SRC_RETIRED as SRC_RETIRED,
SQ_pc_quotn_feat_loctr_x.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_pc_quotn_feat_loctr_x.SRC_QUOTN_FEAT_LOCTR_ROLE_CD as SRC_QUOTN_FEAT_LOCTR_ROLE_CD,
SQ_pc_quotn_feat_loctr_x.SRC_EDW_STRT_DTTM as SRC_EDW_STRT_DTTM,
SQ_pc_quotn_feat_loctr_x.SRC_EDW_END_DTTM as SRC_EDW_END_DTTM,
:PRCS_ID as PRCS_ID,
SQ_pc_quotn_feat_loctr_x.source_record_id
FROM
SQ_pc_quotn_feat_loctr_x
);


-- Component rtr_agmt_feat_loctr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_agmt_feat_loctr_INSERT AS (
SELECT
exp_pass_from_source_imp.TGT_QUOTN_ID as lkp_QUOTN_ID,
exp_pass_from_source_imp.TGT_FEAT_ID as lkp_FEAT_ID,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_ROLE_CD as lkp_QUOTN_FEAT_LOCTR_ROLE_CD,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_STRT_DTTM as lkp_QUOTN_FEAT_LOCTR_STRT_DTTM,
exp_pass_from_source_imp.TGT_LOC_ID as lkp_LOC_ID,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_AMT as lkp_QUOTN_FEAT_LOCTR_AMT,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_END_DTTM as lkp_QUOTN_FEAT_LOCTR_END_DTTM,
exp_pass_from_source_imp.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_ID as in_QUOTN_ID,
exp_pass_from_source_imp.SRC_FEAT_ID as in_FEAT_ID,
exp_pass_from_source_imp.SRC_LOC_ID as in_LOC_ID,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_AMT as in_QUOTN_FEAT_LOCTR_AMT,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_STRT_DTTM as in_QUOTN_FEAT_LOCTR_STRT_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_END_DTTM as in_QUOTN_FEAT_LOCTR_END_DTTM,
exp_pass_from_source_imp.INS_UPD_FLAG as o_FLAG,
exp_pass_from_source_imp.SRC_UPDATETIME as in_UPDATETIME,
exp_pass_from_source_imp.SRC_RETIRED as in_RETIRED,
exp_pass_from_source_imp.TGT_EDW_STRT_DTTM as lkp_EDW_START_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_ROLE_CD as in_QUOTN_FEAT_LOCTR_ROLE_CD,
exp_pass_from_source_imp.SRC_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_from_source_imp.SRC_EDW_END_DTTM as in_EDW_END_DTTM,
exp_pass_from_source_imp.PRCS_ID as PRCS_ID,
exp_pass_from_source_imp.source_record_id
FROM
exp_pass_from_source_imp
WHERE exp_pass_from_source_imp.INS_UPD_FLAG = ''I'' AND ( exp_pass_from_source_imp.SRC_QUOTN_ID IS NOT NULL AND exp_pass_from_source_imp.SRC_FEAT_ID IS NOT NULL AND exp_pass_from_source_imp.SRC_LOC_ID IS NOT NULL ) OR ( exp_pass_from_source_imp.INS_UPD_FLAG = ''U'' ) or ( exp_pass_from_source_imp.SRC_RETIRED = 0 AND exp_pass_from_source_imp.TGT_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ) ;
-- > first insert -- > insert incase of Change -- > retired earlier and now restored;
-- Component rtr_agmt_feat_loctr_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_agmt_feat_loctr_RETIRED AS (
SELECT
exp_pass_from_source_imp.TGT_QUOTN_ID as lkp_QUOTN_ID,
exp_pass_from_source_imp.TGT_FEAT_ID as lkp_FEAT_ID,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_ROLE_CD as lkp_QUOTN_FEAT_LOCTR_ROLE_CD,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_STRT_DTTM as lkp_QUOTN_FEAT_LOCTR_STRT_DTTM,
exp_pass_from_source_imp.TGT_LOC_ID as lkp_LOC_ID,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_AMT as lkp_QUOTN_FEAT_LOCTR_AMT,
exp_pass_from_source_imp.TGT_QUOTN_FEAT_LOCTR_END_DTTM as lkp_QUOTN_FEAT_LOCTR_END_DTTM,
exp_pass_from_source_imp.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_ID as in_QUOTN_ID,
exp_pass_from_source_imp.SRC_FEAT_ID as in_FEAT_ID,
exp_pass_from_source_imp.SRC_LOC_ID as in_LOC_ID,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_AMT as in_QUOTN_FEAT_LOCTR_AMT,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_STRT_DTTM as in_QUOTN_FEAT_LOCTR_STRT_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_END_DTTM as in_QUOTN_FEAT_LOCTR_END_DTTM,
exp_pass_from_source_imp.INS_UPD_FLAG as o_FLAG,
exp_pass_from_source_imp.SRC_UPDATETIME as in_UPDATETIME,
exp_pass_from_source_imp.SRC_RETIRED as in_RETIRED,
exp_pass_from_source_imp.TGT_EDW_STRT_DTTM as lkp_EDW_START_DTTM,
exp_pass_from_source_imp.SRC_QUOTN_FEAT_LOCTR_ROLE_CD as in_QUOTN_FEAT_LOCTR_ROLE_CD,
exp_pass_from_source_imp.SRC_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_from_source_imp.SRC_EDW_END_DTTM as in_EDW_END_DTTM,
exp_pass_from_source_imp.PRCS_ID as PRCS_ID,
exp_pass_from_source_imp.source_record_id
FROM
exp_pass_from_source_imp
WHERE exp_pass_from_source_imp.INS_UPD_FLAG = ''R'' and exp_pass_from_source_imp.SRC_RETIRED != 0 and exp_pass_from_source_imp.TGT_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
-- > not insert or update , no change in values 
-- > but data is retired 
-- > update these records with CURRENT_TIMESTAMP;
-- Component upd_stg_upd_retire_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_retire_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_feat_loctr_RETIRED.lkp_QUOTN_ID as lkp_QUOTN_ID3,
rtr_agmt_feat_loctr_RETIRED.lkp_FEAT_ID as lkp_FEAT_ID3,
rtr_agmt_feat_loctr_RETIRED.lkp_LOC_ID as lkp_LOC_ID3,
rtr_agmt_feat_loctr_RETIRED.lkp_QUOTN_FEAT_LOCTR_AMT as lkp_QUOTN_FEAT_LOCTR_AMT3,
rtr_agmt_feat_loctr_RETIRED.lkp_QUOTN_FEAT_LOCTR_ROLE_CD as lkp_QUOTN_FEAT_LOCTR_ROLE_CD3,
rtr_agmt_feat_loctr_RETIRED.lkp_EDW_START_DTTM as lkp_EDW_START_DTTM3,
rtr_agmt_feat_loctr_RETIRED.PRCS_ID as o_PRCS_ID3,
rtr_agmt_feat_loctr_RETIRED.in_QUOTN_FEAT_LOCTR_STRT_DTTM as in_QUOTN_FEAT_LOCTR_STRT_DTTM3,
rtr_agmt_feat_loctr_RETIRED.in_UPDATETIME as in_UPDATETIME3,
NULL as TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_agmt_feat_loctr_RETIRED.source_record_id
FROM
rtr_agmt_feat_loctr_RETIRED
);


-- Component upd_stg_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_feat_loctr_INSERT.in_QUOTN_ID as in_QUOTN_ID1,
rtr_agmt_feat_loctr_INSERT.in_FEAT_ID as in_FEAT_ID1,
rtr_agmt_feat_loctr_INSERT.in_LOC_ID as in_LOC_ID1,
rtr_agmt_feat_loctr_INSERT.in_QUOTN_FEAT_LOCTR_AMT as in_QUOTN_FEAT_LOCTR_AMT1,
rtr_agmt_feat_loctr_INSERT.in_QUOTN_FEAT_LOCTR_STRT_DTTM as in_QUOTN_FEAT_LOCTR_STRT_DTTM1,
rtr_agmt_feat_loctr_INSERT.in_QUOTN_FEAT_LOCTR_END_DTTM as in_QUOTN_FEAT_LOCTR_END_DTTM1,
rtr_agmt_feat_loctr_INSERT.PRCS_ID as o_PRCS_ID1,
rtr_agmt_feat_loctr_INSERT.in_UPDATETIME as in_UPDATETIME1,
NULL as in_RETIRED1,
rtr_agmt_feat_loctr_INSERT.in_QUOTN_FEAT_LOCTR_ROLE_CD as in_QUOTN_FEAT_LOCTR_ROLE_CD1,
0 as UPDATE_STRATEGY_ACTION,
rtr_agmt_feat_loctr_INSERT.source_record_id,
FROM
rtr_agmt_feat_loctr_INSERT
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_stg_ins_upd.in_QUOTN_ID1 as QUOTN_ID,
upd_stg_ins_upd.in_FEAT_ID1 as FEAT_ID,
upd_stg_ins_upd.in_LOC_ID1 as LOC_ID,
upd_stg_ins_upd.in_QUOTN_FEAT_LOCTR_AMT1 as QUOTN_FEAT_LOCTR_AMT,
upd_stg_ins_upd.in_QUOTN_FEAT_LOCTR_ROLE_CD1 as QUOTN_FEAT_LOCTR_ROLE_CD,
upd_stg_ins_upd.in_QUOTN_FEAT_LOCTR_STRT_DTTM1 as QUOTN_FEAT_LOCTR_STRT_DT,
upd_stg_ins_upd.in_QUOTN_FEAT_LOCTR_END_DTTM1 as QUOTN_FEAT_LOCTR_END_DT,
upd_stg_ins_upd.o_PRCS_ID1 as PRCS_ID,
upd_stg_ins_upd.in_UPDATETIME1 as in_UPDATETIME1,
CURRENT_TIMESTAMP as v_EDW_STRT_DTTM,
v_EDW_STRT_DTTM as o_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as v_EDW_END_DTTM,
v_EDW_END_DTTM as o_EDW_END_DTTM,
upd_stg_ins_upd.in_UPDATETIME1 as TRANS_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as v_TRANS_END_DTTM,
v_TRANS_END_DTTM as o_TRANS_END_DTTM,
upd_stg_ins_upd.source_record_id
FROM
upd_stg_ins_upd
);


-- Component exp_pass_to_target_upd__retire_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd__retire_rejected AS
(
SELECT
upd_stg_upd_retire_rejected.lkp_QUOTN_ID3 as QUOTN_ID,
upd_stg_upd_retire_rejected.lkp_FEAT_ID3 as FEAT_ID,
upd_stg_upd_retire_rejected.lkp_LOC_ID3 as LOC_ID,
upd_stg_upd_retire_rejected.lkp_QUOTN_FEAT_LOCTR_AMT3 as QUOTN_FEAT_LOCTR_AMT,
upd_stg_upd_retire_rejected.lkp_QUOTN_FEAT_LOCTR_ROLE_CD3 as QUOTN_FEAT_LOCTR_ROLE_CD,
upd_stg_upd_retire_rejected.o_PRCS_ID3 as PRCS_ID,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_stg_upd_retire_rejected.lkp_EDW_START_DTTM3 as EDW_STRT_DTTM_upd3,
upd_stg_upd_retire_rejected.TRANS_STRT_DTTM4 as o_TRANS_END_DTTM,
upd_stg_upd_retire_rejected.source_record_id
FROM
upd_stg_upd_retire_rejected
);


-- Component tgt_QUOTN_FEAT_LOCTR_ins_upd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_FEAT_LOCTR
(
QUOTN_ID,
FEAT_ID,
QUOTN_FEAT_LOCTR_ROLE_CD,
QUOTN_FEAT_LOCTR_STRT_DTTM,
LOC_ID,
QUOTN_FEAT_LOCTR_AMT,
QUOTN_FEAT_LOCTR_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_target_ins.FEAT_ID as FEAT_ID,
exp_pass_to_target_ins.QUOTN_FEAT_LOCTR_ROLE_CD as QUOTN_FEAT_LOCTR_ROLE_CD,
exp_pass_to_target_ins.QUOTN_FEAT_LOCTR_STRT_DT as QUOTN_FEAT_LOCTR_STRT_DTTM,
exp_pass_to_target_ins.LOC_ID as LOC_ID,
exp_pass_to_target_ins.QUOTN_FEAT_LOCTR_AMT as QUOTN_FEAT_LOCTR_AMT,
exp_pass_to_target_ins.QUOTN_FEAT_LOCTR_END_DT as QUOTN_FEAT_LOCTR_END_DTTM,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.o_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_target_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component tgt_QUOTN_FEAT_LOCTR_ins_upd, Type Post SQL 
UPDATE  db_t_prod_core.QUOTN_FEAT_LOCTR 
set TRANS_END_DTTM=  A.lead,

EDW_END_DTTM=A.EDW_lead

FROM

(SELECT	distinct QUOTN_ID,FEAT_ID,LOC_ID,QUOTN_FEAT_LOCTR_ROLE_CD ,EDW_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,LOC_ID,QUOTN_FEAT_LOCTR_ROLE_CD  ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead,max(EDW_STRT_DTTM) over (partition by QUOTN_ID,FEAT_ID,LOC_ID,QUOTN_FEAT_LOCTR_ROLE_CD   ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as EDW_lead

FROM db_t_prod_core.QUOTN_FEAT_LOCTR 

 ) A


where  QUOTN_FEAT_LOCTR.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_FEAT_LOCTR.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_FEAT_LOCTR.FEAT_ID=A.FEAT_ID

AND QUOTN_FEAT_LOCTR.LOC_ID=A.LOC_ID

AND QUOTN_FEAT_LOCTR.QUOTN_FEAT_LOCTR_ROLE_CD =A.QUOTN_FEAT_LOCTR_ROLE_CD 

and QUOTN_FEAT_LOCTR.TRANS_STRT_DTTM <>QUOTN_FEAT_LOCTR.TRANS_END_DTTM

and lead is not null;


-- Component tgt_QUOTN_FEAT_LOCTR_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_FEAT_LOCTR
USING exp_pass_to_target_upd__retire_rejected ON (QUOTN_FEAT_LOCTR.QUOTN_ID=exp_pass_to_target_upd__retire_rejected.QUOTN_ID)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_pass_to_target_upd__retire_rejected.QUOTN_ID,
FEAT_ID = exp_pass_to_target_upd__retire_rejected.FEAT_ID,
QUOTN_FEAT_LOCTR_ROLE_CD = exp_pass_to_target_upd__retire_rejected.QUOTN_FEAT_LOCTR_ROLE_CD,
LOC_ID = exp_pass_to_target_upd__retire_rejected.LOC_ID,
QUOTN_FEAT_LOCTR_AMT = exp_pass_to_target_upd__retire_rejected.QUOTN_FEAT_LOCTR_AMT,
PRCS_ID = exp_pass_to_target_upd__retire_rejected.PRCS_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd__retire_rejected.EDW_STRT_DTTM_upd3,
EDW_END_DTTM = exp_pass_to_target_upd__retire_rejected.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_target_upd__retire_rejected.o_TRANS_END_DTTM;


END; ';