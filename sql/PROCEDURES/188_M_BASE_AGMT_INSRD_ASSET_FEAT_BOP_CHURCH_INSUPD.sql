-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_INSRD_ASSET_FEAT_BOP_CHURCH_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id integer;
p_agmt_type_cd_policy_version varchar;

BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;
p_agmt_type_cd_policy_version := ''TEST'';




-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.NK_BUSN_CD as NK_BUSN_CD FROM db_t_prod_core.BUSN
Qualify Row_number() over(partition by busn_prty_id order by edw_end_Dttm desc)=1
);


-- Component LKP_INDIV, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV AS
(
SELECT INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, INDIV.NK_link_ID as NK_link_ID FROM db_t_prod_core.INDIV Qualify Row_number() over(partition by  INDIV_PRTY_ID order by edw_end_dttm desc)=1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,upper(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL) as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'')

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


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_CNTRCT_ROLE_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_EFECT_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_discountsurcharge_alfa.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

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

-- Component SQ_pc_agmt_insrd_asset_feat_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_agmt_insrd_asset_feat_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as pol_strt_dt,
$3 as feature_strt_dt,
$4 as feature_end_dt,
$5 as Cntrct_role,
$6 as nk_public_id,
$7 as feat_sbtype_cd,
$8 as typecode,
$9 as classification_code,
$10 as fixed_id,
$11 as asset_strt_dt,
$12 as UPDTAETIME,
$13 as SRC_CD,
$14 as RateSymbolCollision_alfa,
$15 as Ratesymbol_alfa,
$16 as Retired,
$17 as FEAT_VAL,
$18 as FEAT_COVTERMTYPE,
$19 as polcov_RateModifier,
$20 as polcov_Eligible,
$21 as DiscountSurcharge_alfa_typecd,
$22 as Addressbookuid,
$23 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	* 

FROM	( 

	SELECT	pcx_bp7_agmt_insrd_asset_feat_x.PublicID, pcx_bp7_agmt_insrd_asset_feat_x.pol_strt_dt,

			pcx_bp7_agmt_insrd_asset_feat_x.feature_strt_dt, pcx_bp7_agmt_insrd_asset_feat_x.feature_end_dt,

			pcx_bp7_agmt_insrd_asset_feat_x.Cntrct_role,pcx_bp7_agmt_insrd_asset_feat_x.nk_public_id,

			pcx_bp7_agmt_insrd_asset_feat_x.feat_sbtype_cd, pcx_bp7_agmt_insrd_asset_feat_x.typecode,

			pcx_bp7_agmt_insrd_asset_feat_x.classification_code, pcx_bp7_agmt_insrd_asset_feat_x.fixed_id,

			pcx_bp7_agmt_insrd_asset_feat_x.asset_strt_dt, pcx_bp7_agmt_insrd_asset_feat_x.updatetime,

			pcx_bp7_agmt_insrd_asset_feat_x.SRC_CD, RateSymbolCollision_alfa,

			RateSymbol_alfa, Retired, pcx_bp7_agmt_insrd_asset_feat_x.FEAT_VAL,

			pcx_bp7_agmt_insrd_asset_feat_x.FEAT_COVTERMTYPE, pcx_bp7_agmt_insrd_asset_feat_x.polcov_RateModifier,

			substr (pcx_bp7_agmt_insrd_asset_feat_x.polcov_Eligible,1,1) as eligible,

			DiscountSurcharge_alfa_typecd, addressbookuid, ROW_NUMBER() OVER( 

	PARTITION BY PublicID,nk_public_id,feat_sbtype_cd,typecode,classification_code,

			fixed_id,Cntrct_role 

	ORDER BY pol_strt_dt DESC) AS rankid 

	FROM	( /*3 FLOW PIPE LINE IN MAPPING*/ /*coverage*/ 

	select	distinct pc_policyperiod.PUBLICID_stg as PublicID, pc_policyperiod.PeriodStart_stg as pol_strt_dt,

			case 

				when polcov.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polcov.EffectiveDate_stg 

			end as feature_strt_dt, 

			case 

				when polcov.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polcov.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100)) =''package'' then cast(package.packagePatternID_stg as varchar(100)) 

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polcov.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60)) =''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''option'' 

		and polcov.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,  /* polcov.assettype_stg,policynumber,*/ 

			case 

				when polcov.assettype_stg in ( ''bp7classification'' , ''BP7Building'') then ''PRTY_ASSET_SBTYPE13'' 

				when polcov.assettype_stg in ( ''BP7BldgSchedCovItem'') then ''PRTY_ASSET_SBTYPE23'' 

				when polcov.assettype_stg in ( ''BP7LineSchedCovItem'') then ''PRTY_ASSET_SBTYPE29'' 

				when polcov.assettype_stg in ( ''BP7LocSchedCovItem'') then ''PRTY_ASSET_SBTYPE20'' 

				when polcov.assettype_stg in ( ''BP7ClassSchedCovItem'') then ''PRTY_ASSET_SBTYPE26'' 

			end as typecode, 

			case 

				when polcov.assettype_stg in (''BP7Building'') then pctl_bp7classificationproperty.TYPECODE_stg 

				when polcov.assettype_stg in ( ''bp7classification'' ) then pctl_bp7classificationproperty.TYPECODE_stg 

				when polcov.assettype_stg in ( ''BP7BldgSchedCovItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcov.assettype_stg in ( ''BP7LineSchedCovItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcov.assettype_stg in ( ''BP7LocSchedCovItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcov.assettype_stg in ( ''BP7ClassSchedCovItem'') then ''PRTY_ASSET_CLASFCN8'' 

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polcov.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255)) as RateSymbolCollision_alfa,

			 cast(null as varchar(255)) as RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polcov.val_stg as FEAT_VAL, null as polcov_RateModifier, null as polcov_Eligible,

			covterm.CovTermType_stg as FEAT_COVTERMTYPE,  cast(null as varchar(255)) as DiscountSurcharge_alfa_typecd,

			coalesce(pc_con_ins.addressbookuid_ins_stg, pc_con.addressbookuid_int_stg)addressbookuid  

	from	( /*pcx_bp7classificationcov*/ 

	select	cast(''ChoiceTerm1'' as varchar(100)) as columnname_stg, cast( ChoiceTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast( DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast( DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast( DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union  

	select	''BooleanTerm1'' as columnname_stg, cast( BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast( BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast( BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null  

	union 

	select	''StringTerm1'' as columnname_stg, cast( StringTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, cast( StringTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, cast( StringTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm4'' as columnname_stg, cast( StringTerm4_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm5'' as columnname_stg, cast( StringTerm5_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast( DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast( DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcov a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	a.ChoiceTerm1Avl_stg is null 

		and a.ChoiceTerm2Avl_stg is null 

		and a.ChoiceTerm3Avl_stg is null 

		and a.ChoiceTerm4Avl_stg is null 

		and a.ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and StringTerm4Avl_stg is null 

		and StringTerm5Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null  

	union /*pcx_bp7bldgschedcovitemcov*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, cast(''bp7bldgschedcovitem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7bldgschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	union /*pcx_bp7buildingcov*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, cast(''bp7building'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm6'' as columnname_stg, ChoiceTerm6_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm6Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm4'' as columnname_stg, StringTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm5'' as columnname_stg, StringTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and ChoiceTerm6Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and StringTerm4Avl_stg is null 

		and StringTerm5Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null 

	union /*pcx_bp7classschedcovitemcov*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7ClassSchedCovItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCovItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCovItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, null as ChoiceTerm1_stg, null as patternid_stg,

			updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedcovitemcov 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	UNION /*pcx_bp7lineschedcovitemcov*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, cast(''bp7lineschedcovitem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm4'' as columnname_stg, cast(DirectTerm4_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm5'' as columnname_stg, cast(DirectTerm5_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm6'' as columnname_stg, cast(DirectTerm6_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm6Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm7'' as columnname_stg, cast(DirectTerm7_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DirectTerm7Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm4'' as columnname_stg, cast(BooleanTerm4_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm5'' as columnname_stg, cast(BooleanTerm5_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm6'' as columnname_stg, cast(BooleanTerm6_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm6Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm7'' as columnname_stg, cast(BooleanTerm7_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm7Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm8'' as columnname_stg, cast(BooleanTerm8_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm8Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm9'' as columnname_stg, cast(BooleanTerm9_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm9Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm10'' as columnname_stg, cast(BooleanTerm10_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm10Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm11'' as columnname_stg, cast(BooleanTerm11_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	BooleanTerm11Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm4'' as columnname_stg, StringTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm5'' as columnname_stg, StringTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm6'' as columnname_stg, StringTerm6_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm6Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm7'' as columnname_stg, StringTerm7_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm7Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm8'' as columnname_stg, StringTerm8_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm8Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm9'' as columnname_stg, StringTerm9_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm9Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm10'' as columnname_stg, StringTerm10_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm10Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm11'' as columnname_stg, StringTerm11_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm11Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm12'' as columnname_stg, StringTerm12_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	StringTerm12Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7lineschedcovitem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedcovitemcov 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and DirectTerm4Avl_stg is null 

		and DirectTerm5Avl_stg is null 

		and DirectTerm6Avl_stg is null 

		and DirectTerm7Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and BooleanTerm4Avl_stg is null 

		and BooleanTerm5Avl_stg is null 

		and BooleanTerm6Avl_stg is null 

		and BooleanTerm7Avl_stg is null 

		and BooleanTerm8Avl_stg is null 

		and BooleanTerm9Avl_stg is null 

		and BooleanTerm10Avl_stg is null 

		and BooleanTerm11Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and StringTerm4Avl_stg is null 

		and StringTerm5Avl_stg is null 

		and StringTerm6Avl_stg is null 

		and StringTerm7Avl_stg is null 

		and StringTerm8Avl_stg is null 

		and StringTerm9Avl_stg is null 

		and StringTerm10Avl_stg is null 

		and StringTerm11Avl_stg is null 

		and StringTerm12Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null /*pcx_bp7locschedcovitemcov*/ 

	union 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, cast(''bp7LocSchedCovItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCovItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCovItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, null as ChoiceTerm1_stg,

			null as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedcovitemcov 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null )polcov  

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'',

				''BP7BusinessOwnersLine'',''BP7Classification'', ''BP7LineSchedCovItem'',

				''BP7LocSchedCovItem'',''BP7ClassSchedCovItem'') ) covterm 

		on covterm.clausePatternID_stg = polcov.patterncode_stg 

		and covterm.ColumnName_stg = polcov.ColumnName_stg   

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polcov.val_stg  

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name_stg, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polcov.val_stg  

	left outer join ( 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7classification'' as varchar(255)) assettype_stg , EffectiveDate_stg,

				ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))as bp7classpropertytype_stg,

				cast(null as varchar(255))additionalinterest_stg, cast(null as varchar(255))additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_bp7classification 

		where	ExpirationDate_stg is null 

		union 

		select	cast(b.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255)) as bp7classpropertytype_stg,

				cast(null as varchar(255))additionalinterest_stg, cast(null as varchar(255))additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a join DB_T_PROD_STAG.pcx_bp7classification b 

			on a.fixedid_stg =b.building_stg 

			and a.BranchID_stg=b.BranchID_stg 

		where	a.ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7BldgSchedCovItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255))bp7classpropertytype_stg,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7BldgSchedCovItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7ClassSchedCovItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))bp7classpropertytype_stg,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				 cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg ,branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7ClassSchedCovItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LOCSchedCovItem'' as varchar(255)) assettype_stg , EffectiveDate_stg,

				ExpirationDate_stg, cast(null as varchar(255))bp7classpropertytype_stg,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				 cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LocSchedCovItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LineSchedCovItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg,cast(null as varchar(255))bp7classpropertytype_stg,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				 cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LineSchedCovItem 

		where	ExpirationDate_stg is null ) polveh 

		on polcov.assetkey_stg =polveh.fixedid_stg 

		and polcov.BranchID_stg=polveh.BranchID_stg 

		and polcov.assettype_stg=polveh.assettype_stg 

		and r=1  

	left outer join DB_T_PROD_STAG.pctl_bp7classificationproperty 

		on pctl_bp7classificationproperty.id_stg = polveh.bp7classpropertytype_stg 

	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg )pc_con 

		on cast(addlinter_id as integer) =cast(polveh.additionalinterest_stg as integer)   

	left join ( 

		select	 distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) =cast(polveh.additionalinsured_stg as integer)  

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polcov.BranchID_stg , polveh.BranchID_stg) join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 

		and pc_policyperiod.updatetime_stg > (:start_dttm) 

		and pc_policyperiod.updatetime_stg <= (:end_dttm)   

	union  /*2 FLOW PIPE LINE IN MAPPING*/  /*Exclusion*/ 

	select	distinct pc_policyperiod.PUBLICID_stg as PublicID,  pc_policyperiod.PeriodStart_stg as pol_strt_dt,

			case 

				when polexcl.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polexcl.EffectiveDate_stg 

			end as feature_strt_dt,  

			case 

				when polexcl.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polexcl.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100))=''package'' then cast(package.packagePatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polexcl.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60))=''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60))=''option'' 

		and polexcl.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,  

			case 

				when polexcl.assettype_stg in ( ''bp7classification'' , ''BP7Building'') then ''PRTY_ASSET_SBTYPE13'' 

				when polexcl.assettype_stg in ( ''BP7BldgSchedexclItem'') then ''PRTY_ASSET_SBTYPE25'' 

				when polexcl.assettype_stg in ( ''BP7LineSchedexclItem'') then ''PRTY_ASSET_SBTYPE31'' 

				when polexcl.assettype_stg in ( ''BP7LocSchedexclItem'') then ''PRTY_ASSET_SBTYPE22'' 

				when polexcl.assettype_stg in ( ''BP7ClassSchedexclItem'') then ''PRTY_ASSET_SBTYPE28'' 

			end as typecode,  

			case 

				when polexcl.assettype_stg in (''BP7Building'') then pctl_bp7classificationproperty.TYPECODE_stg 

				when polexcl.assettype_stg in ( ''bp7classification'' ) then pctl_bp7classificationproperty.TYPECODE_stg 

				when polexcl.assettype_stg in ( ''BP7BldgSchedexclItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polexcl.assettype_stg in ( ''BP7LineSchedexclItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polexcl.assettype_stg in ( ''BP7LocSchedexclItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polexcl.assettype_stg in ( ''BP7ClassSchedexclItem'') then ''PRTY_ASSET_CLASFCN8'' 

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polexcl.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255)) as RateSymbolCollision_alfa,

			 cast(null as varchar(255)) as RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polexcl.val_stg as FEAT_VAL, null as polcov_RateModifier,  null as polcov_Eligible,

			covterm.CovTermType_stg as FEAT_COVTERMTYPE,  cast(null as varchar(255)) as DiscountSurcharge_alfa_typecd,

			coalesce( pc_con.addressbookUID_INT_stg, pc_con_ins.addressbookUID_INS_stg) addressbookuid 

	from	( /*pcx_bp7classificationexcl*/ 

	select	cast(''ChoiceTerm1'' as varchar(100)) as columnname_stg, cast( ChoiceTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast( Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationexcl a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null 

	union /*pcx_bp7bldgschedexclitemexcl*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7BldgSchedExclItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedexclitemexcl 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	union /*pcx_bp7buildingexcl*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null 

	union /*pcx_bp7classschedexclitemexcl*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7ClassSchedExclItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedExclItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedExclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedexclitemexcl 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	UNION /*pcx_bp7lineschedexclitemexcl*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7LineSchedEXclItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedEXclItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedEXclItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedexclitemexcl 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null /*pcx_bp7locschedexclitemexcl*/ 

	union 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, cast(''bp7LocSchedExclItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedExclItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedExclItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as ChoiceTerm1_stg,

			cast(null as varchar(255)) as patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedexclitemexcl 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null ) polexcl 

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7BldgSchedexclItem'', ''BP7Building'',

				''bp7ClassSchedexclItem'',''BP7Classification'', ''BP7LineSchedexclItem'',

				''BP7LocSchedexclItem'') ) covterm 

		on covterm.clausePatternID_stg = polexcl.patterncode_stg 

		and covterm.ColumnName_stg = polexcl.ColumnName_stg 

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id_stg, pc_etlcovtermpackage.PackageCode_stg as name_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polexcl.val_stg 

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name_stg, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polexcl.val_stg 

	left outer join ( 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7classification'' as varchar(255)) assettype_stg , EffectiveDate_stg,

				ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))  as bp7classpropertytype_stg,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_bp7classification 

		where	ExpirationDate_stg is null 

		union 

		select	cast(b.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))  as bp7classpropertytype_stg,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a join DB_T_PROD_STAG.pcx_bp7classification b 

			on a.fixedid_stg=b.building_stg 

			and a.BranchID_stg=b.BranchID_stg 

		where	a.ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7BldgSchedexclItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as  additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7BldgSchedexclItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7ClassSchedexclItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as  additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7ClassSchedexclItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LOCSchedexclItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(null as varchar(255)) as	additionalinterest_stg, cast(null as varchar(255)) as  additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LocSchedexclItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LineSchedexclItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as  additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LineSchedexclItem 

		where	ExpirationDate_stg is null ) polveh 

		on polexcl.assetkey_stg =polveh.fixedid_stg 

		and polexcl.BranchID_stg=polveh.BranchID_stg 

		and polexcl.assettype_stg=polveh.assettype_stg 

		and r=1 

	left outer join DB_T_PROD_STAG.pctl_bp7classificationproperty 

		on pctl_bp7classificationproperty.id_stg = polveh.bp7classpropertytype_stg 

	left outer join ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con 

		on cast(addlinter_id as integer) = cast(polveh.additionalinterest_stg as integer) 

	left join ( 

		select	  distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				 pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) = cast( polveh.additionalinsured_stg as integer) 

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polexcl.BranchID_stg , polveh.BranchID_stg) join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 

		and pc_policyperiod.updatetime_stg > (:start_dttm) 

		and pc_policyperiod.updatetime_stg <= (:end_dttm)  

	UNION  /*1 FLOW PIPE LINE IN MAPPING*/ /*Condition*/ 

	select	distinct pc_policyperiod.PUBLICID_stg as PublicID, pc_policyperiod.PeriodStart_stg as pol_start_dt,

			case 

				when polcond.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polcond.EffectiveDate_stg 

			end as feature_strt_dt,  

			case 

				when polcond.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polcond.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100)) =''package'' then cast(package.packagePatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polcond.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60)) =''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''option'' 

		and polcond.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,  

			case 

				when polcond.assettype_stg in ( ''bp7classification'' , ''BP7Building'') then ''PRTY_ASSET_SBTYPE13'' 

				when polcond.assettype_stg in ( ''BP7BldgSchedcondItem'') then ''PRTY_ASSET_SBTYPE24'' 

				when polcond.assettype_stg in ( ''BP7LineSchedcondItem'') then ''PRTY_ASSET_SBTYPE30'' 

				when polcond.assettype_stg in ( ''BP7LocSchedcondItem'') then ''PRTY_ASSET_SBTYPE21'' 

				when polcond.assettype_stg in ( ''BP7ClassSchedcondItem'') then ''PRTY_ASSET_SBTYPE27'' 

			end as typecode,  

			case 

				when polcond.assettype_stg in (''BP7Building'') then pctl_bp7classificationproperty.TYPECODE_stg 

				when polcond.assettype_stg in ( ''bp7classification'' ) then pctl_bp7classificationproperty.TYPECODE_stg 

				when polcond.assettype_stg in ( ''BP7BldgSchedcondItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcond.assettype_stg in ( ''BP7LineSchedcondItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcond.assettype_stg in ( ''BP7LocSchedcondItem'') then ''PRTY_ASSET_CLASFCN8'' 

				when polcond.assettype_stg in ( ''BP7ClassSchedcondItem'') then ''PRTY_ASSET_CLASFCN8'' 

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polcond.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255))  as  RateSymbolCollision_alfa,

			 cast(null as varchar(255))  as  RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polcond.val_stg as FEAT_VAL, null  as  polcov_RateModifier,

			 null  as  polcov_Eligible, covterm.CovTermType_stg as FEAT_COVTERMTYPE,

			 cast(null as varchar(255))  as  DiscountSurcharge_alfa_typecd ,

			coalesce(pc_con_ins.addressbookuid_ins_stg,	pc_con.addressbookuid_int_stg) addressbookuid  

	from	( /*pcx_bp7classificationcond*/ 

	select	cast(''ChoiceTerm1'' as varchar(100)) as columnname_stg, cast( ChoiceTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, cast(''bp7classification'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(Classification_stg as varchar(255)) as assetkey_stg, ''bp7classification'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classificationcond a join DB_T_PROD_STAG.pcx_bp7classification 

		on a.Classification_stg = pcx_bp7classification.id_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null 

	union /*pcx_bp7bldgschedconditemcond*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7BldgSchedCondItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(BldgSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7BldgSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7bldgschedconditemcond 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	union /*pcx_bp7buildingcond*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, cast(''bp7building'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcond a join DB_T_PROD_STAG.pcx_bp7classification b 

		on a.building_stg =b.building_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null 

	union /*pcx_bp7classschedconditemcond*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7ClassSchedCondItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(ClassSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7ClassSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7classschedconditemcond 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null 

	UNION /*pcx_bp7lineschedconditemcond*/ 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			cast(''bp7LineSchedCondItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LineSchedCondItem_stg as varchar(255)) as assetkey_stg,

			''bp7LineSchedCondItem'' as assettype_stg, createtime_stg, EffectiveDate_stg,

			ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7lineschedconditemcond 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null /*pcx_bp7locschedconditemcond*/ 

	union 

	select	''ChoiceTerm1'' as columnname_stg, ChoiceTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, cast(''bp7LocSchedCondItem'' as varchar(255)) as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm4Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm5Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	DirectTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	DirectTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	DirectTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	BooleanTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	BooleanTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	BooleanTerm3Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	StringTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	StringTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	DateTerm1Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	DateTerm2Avl_stg = 1 

		and ExpirationDate_stg  is null 

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(BranchID_stg as varchar(255)) as BranchId_stg,

			cast(LocSchedCondItem_stg as varchar(255)) as assetkey_stg, ''bp7LocSchedCondItem'' as assettype_stg,

			createtime_stg, EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255))  as  ChoiceTerm1_stg,

			cast(null as varchar(255))  as  patternid_stg, updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7locschedconditemcond 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and ExpirationDate_stg  is null ) polcond 

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7BldgSchedCondItem'', ''BP7Building'',

				''bp7ClassSchedCondItem'',''BP7Classification'', ''BP7LineSchedCondItem'',

				''BP7LocSchedCondItem'') ) covterm 

		on covterm.clausePatternID_stg = polcond.patterncode_stg 

		and covterm.ColumnName_stg = polcond.ColumnName_stg 

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polcond.val_stg 

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polcond.val_stg 

	left outer join ( 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7classification'' as varchar(255)) assettype_stg , EffectiveDate_stg,

				ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))  as bp7classpropertytype_stg,

				cast(null as varchar(255))  as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_bp7classification 

		where	ExpirationDate_stg is null 

		union 

		select	cast(b.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))  as bp7classpropertytype_stg,

				cast(null as varchar(255))  as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a join DB_T_PROD_STAG.pcx_bp7classification b 

			on a.fixedid_stg =b.building_stg 

			and a.BranchID_stg=b.BranchID_stg 

		where	a.ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7BldgSchedcondItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				 cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				 cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg ,branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7BldgSchedcondItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''bp7ClassSchedcondItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7ClassSchedcondItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LOCSchedcondItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LocSchedcondItem 

		where	ExpirationDate_stg is null 

		union 

		select	cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7LineSchedcondItem'' as varchar(255)) assettype_stg ,

				EffectiveDate_stg, ExpirationDate_stg, cast(null as varchar(255)) as bp7classpropertytype_stg ,

				cast(additionalinterest_stg as varchar(255)) as additionalinterest_stg,

				cast(additionalinsured_stg as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7LineSchedcondItem 

		where	ExpirationDate_stg is null ) polveh 

		on polcond.assetkey_stg =polveh.fixedid_stg 

		and polcond.BranchID_stg=polveh.BranchID_stg 

		and polcond.assettype_stg=polveh.assettype_stg 

		and r=1 

	left outer join DB_T_PROD_STAG.pctl_bp7classificationproperty 

		on pctl_bp7classificationproperty.id_stg = polveh.bp7classpropertytype_stg 

	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con 

		on cast(addlinter_id as integer) =cast(polveh.additionalinterest_stg as integer) 

	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) = cast(polveh.additionalinsured_stg as integer) 

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polcond.BranchID_stg , polveh.BranchID_stg) join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 

		and pc_policyperiod.updatetime_stg > (:start_dttm) 

		and pc_policyperiod.updatetime_stg <= (:end_dttm) )pcx_bp7_agmt_insrd_asset_feat_x ) tmp 

WHERE	rankid=1  

	and fixed_id is not null
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
SQ_pc_agmt_insrd_asset_feat_x.PublicID as PUBLICID,
SQ_pc_agmt_insrd_asset_feat_x.pol_strt_dt as pol_start_dt,
SQ_pc_agmt_insrd_asset_feat_x.feature_strt_dt as feature_start_dt,
to_char ( SQ_pc_agmt_insrd_asset_feat_x.feature_strt_dt , ''YYYY-MM-DD'' ) as v_feature_start_dt,
SQ_pc_agmt_insrd_asset_feat_x.feature_end_dt as feature_end_dt,
to_char ( SQ_pc_agmt_insrd_asset_feat_x.feature_end_dt , ''YYYY-MM-DD'' ) as v_feature_end_dt,
SQ_pc_agmt_insrd_asset_feat_x.Cntrct_role as cntrct_role,
SQ_pc_agmt_insrd_asset_feat_x.nk_public_id as nk_public_id,
SQ_pc_agmt_insrd_asset_feat_x.feat_sbtype_cd as feat_sbtype_cd,
SQ_pc_agmt_insrd_asset_feat_x.typecode as typecode,
SQ_pc_agmt_insrd_asset_feat_x.classification_code as classification_code,
SQ_pc_agmt_insrd_asset_feat_x.fixed_id as fixedid,
SQ_pc_agmt_insrd_asset_feat_x.asset_strt_dt as asset_start_dt,
SQ_pc_agmt_insrd_asset_feat_x.SRC_CD as src_cd,
CASE WHEN SQ_pc_agmt_insrd_asset_feat_x.UPDTAETIME IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE SQ_pc_agmt_insrd_asset_feat_x.UPDTAETIME END as o_UpdateTime,
SQ_pc_agmt_insrd_asset_feat_x.RateSymbolCollision_alfa as RateSymbolCollision_alfa,
SQ_pc_agmt_insrd_asset_feat_x.Retired as Retired,
SQ_pc_agmt_insrd_asset_feat_x.Ratesymbol_alfa as Ratesymbol_alfa,
SQ_pc_agmt_insrd_asset_feat_x.FEAT_VAL as FEAT_VAL,
SQ_pc_agmt_insrd_asset_feat_x.FEAT_COVTERMTYPE as FEAT_COVTERMTYPE,
SQ_pc_agmt_insrd_asset_feat_x.polcov_RateModifier as polcov_RateModifier,
SQ_pc_agmt_insrd_asset_feat_x.polcov_Eligible as polcov_Eligible,
SQ_pc_agmt_insrd_asset_feat_x.DiscountSurcharge_alfa_typecd as DiscountSurcharge_alfa_typecd,
substr ( SQ_pc_agmt_insrd_asset_feat_x.Addressbookuid , 1 , POSITION(''-'',SQ_pc_agmt_insrd_asset_feat_x.Addressbookuid) - 1 ) as o_Addressbookuid,
substr ( SQ_pc_agmt_insrd_asset_feat_x.Addressbookuid , POSITION(''-'',SQ_pc_agmt_insrd_asset_feat_x.Addressbookuid) + 1 , 10 ) as Prty_TypeCode,
SQ_pc_agmt_insrd_asset_feat_x.source_record_id
FROM
SQ_pc_agmt_insrd_asset_feat_x
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source.PUBLICID as PUBLICID,
exp_pass_from_source.feature_start_dt as feature_start_dt,
exp_pass_from_source.feature_end_dt as feature_end_dt,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE */ END as out_cntrct_role,
exp_pass_from_source.nk_public_id as nk_public_id,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as o_typecode,
CASE WHEN LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as o_classification_code,
exp_pass_from_source.fixedid as fixedid,
exp_pass_from_source.asset_start_dt as asset_start_dt,
CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''MODIFIER'' THEN ''FEAT_SBTYPE11'' ELSE CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''OPTIONS'' THEN ''FEAT_SBTYPE8'' ELSE CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''COVTERM'' THEN ''FEAT_SBTYPE6'' ELSE CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''CLAUSE'' THEN ''FEAT_SBTYPE7'' ELSE CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''PACKAGE'' THEN ''FEAT_SBTYPE9'' ELSE CASE WHEN exp_pass_from_source.feat_sbtype_cd = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15'' ELSE $3 END END END END END END as var_Feat_sbtype_val,
CASE WHEN LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ END as OUT_FEAT_SBTYPE_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
:p_agmt_type_cd_policy_version as out_AGMT_TYPE_CD_policy,
:PRCS_ID as out_PRCS_ID,
LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_src_cd,
exp_pass_from_source.o_UpdateTime as o_UpdateTime,
exp_pass_from_source.Retired as Retired,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_COVTERMTYPE ) ) ) = ''direct'' THEN IFNULL(TRY_TO_DECIMAL(exp_pass_from_source.FEAT_VAL), 0) ELSE NULL END as AGMT_ASSET_FEAT_AMT,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_COVTERMTYPE ) ) ) = ''datetime'' THEN TO_DATE ( substr ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_VAL ) ) , 0 , 10 ) , ''YYYY-mm-dd'' ) ELSE NULL END as AGMT_ASSET_FEAT_DT,
CASE WHEN ( LOWER ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_COVTERMTYPE ) ) ) = ''shorttext'' ) or ( LOWER ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_COVTERMTYPE ) ) ) = ''typekey'' ) THEN exp_pass_from_source.FEAT_VAL ELSE $3 END as AGMT_ASSET_FEAT_TXT,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source.FEAT_COVTERMTYPE ) ) ) = ''bit'' THEN LTRIM ( RTRIM ( exp_pass_from_source.FEAT_VAL ) ) ELSE NULL END as AGMT_ASSET_FEAT_IND,
exp_pass_from_source.polcov_RateModifier as polcov_RateModifier,
exp_pass_from_source.polcov_Eligible as polcov_Eligible,
CASE WHEN LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD */ END as o_DiscountSurcharge_alfa_typecd,
CASE WHEN exp_pass_from_source.Prty_TypeCode = ''Person'' THEN ( LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV */ ) ELSE ( CASE WHEN exp_pass_from_source.Prty_TypeCode = ''Company'' THEN ( LKP_12.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ ) ELSE null END ) END as V_PRTY_ID,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK
FROM
exp_pass_from_source
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.cntrct_role
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source.cntrct_role
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_pass_from_source.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = upper ( rtrim ( ltrim ( exp_pass_from_source.classification_code ) ) )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = upper ( rtrim ( ltrim ( exp_pass_from_source.classification_code ) ) )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = var_Feat_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = var_Feat_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = exp_pass_from_source.src_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = exp_pass_from_source.DiscountSurcharge_alfa_typecd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = exp_pass_from_source.DiscountSurcharge_alfa_typecd
LEFT JOIN LKP_INDIV LKP_11 ON LKP_11.NK_link_ID = exp_pass_from_source.o_Addressbookuid
LEFT JOIN LKP_BUSN LKP_12 ON LKP_12.NK_BUSN_CD = exp_pass_from_source.o_Addressbookuid
QUALIFY RNK = 1
);


-- Component LKP_AGMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT AS
(
SELECT
LKP.AGMT_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM (SELECT	A.AGMT_ID as AGMT_ID, A.HOST_AGMT_NUM as HOST_AGMT_NUM,
		A.AGMT_NAME as AGMT_NAME, A.AGMT_OPN_DTTM as AGMT_OPN_DTTM,
		A.AGMT_CLS_DTTM as AGMT_CLS_DTTM, A.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM,
		A.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, A.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND,
		A.AGMT_SRC_CD as AGMT_SRC_CD, A.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD,
		A.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, A.AGMT_OBTND_CD as AGMT_OBTND_CD,
		A.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, A.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM,
		A.ALT_AGMT_NAME as ALT_AGMT_NAME, A.ASSET_LIABTY_CD as ASSET_LIABTY_CD,
		A.BAL_SHET_CD as BAL_SHET_CD, A.STMT_CYCL_CD as STMT_CYCL_CD,
		A.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, A.PRPOSL_ID as PRPOSL_ID,
		A.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, A.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD,
		A.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, A.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT,
		A.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD,
		A.BNK_TRD_BK_CD as BNK_TRD_BK_CD, A.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD,
		A.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, A.DY_CNT_BSS_CD as DY_CNT_BSS_CD,
		A.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, A.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD,
		A.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, A.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD,
		A.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, A.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM,
		A.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, A.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM,
		A.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM,
		A.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, A.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT,
		A.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, A.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT,
		A.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, A.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE,
		A.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, A.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND,
		A.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND,
		A.LGCY_DSCNT_IND as LGCY_DSCNT_IND, A.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD,
		A.TRMTN_TYPE_CD as TRMTN_TYPE_CD, A.INT_PMT_METH_CD as INT_PMT_METH_CD,
		A.LBR_AGMT_DESC as LBR_AGMT_DESC, A.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT,
		A.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, A.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT,
		A.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, A.BUSN_PRTY_ID as BUSN_PRTY_ID,
		A.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, A.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD,
		A.MODL_CRTN_DTTM as MODL_CRTN_DTTM, A.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM,
		A.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, A.SRC_SYS_CD as SRC_SYS_CD,
		A.AGMT_EFF_DTTM as AGMT_EFF_DTTM, A.MODL_EFF_DTTM as MODL_EFF_DTTM,
		A.PRCS_ID as PRCS_ID, A.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM,
		A.TIER_TYPE_CD as TIER_TYPE_CD, A.EDW_STRT_DTTM as EDW_STRT_DTTM,
		A.EDW_END_DTTM as EDW_END_DTTM, A.VFYD_PLCY_IND as VFYD_PLCY_IND,
		A.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, A.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD,
		A.LGCY_PLCY_IND as LGCY_PLCY_IND, A.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
		A.NK_SRC_KEY as NK_SRC_KEY, A.AGMT_TYPE_CD as AGMT_TYPE_CD 
FROM	db_t_prod_core.AGMT A  JOIN db_t_prod_core.AGMT_PROD AP
	ON	A.AGMT_ID=AP.AGMT_ID
  JOIN db_t_prod_core.PROD P 
	ON	AP.PROD_ID=P.PROD_ID 
 WHERE P.INSRNC_LOB_TYPE_CD=''BO'' 
	AND	CAST(A.EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY A.NK_SRC_KEY,A.HOST_AGMT_NUM  
ORDER	BY A.EDW_END_DTTM desc) = 1
)agmt
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation.PUBLICID AND LKP.AGMT_TYPE_CD = exp_data_transformation.out_AGMT_TYPE_CD_policy
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc)  
= 1
);


-- Component LKP_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT AS
(
SELECT
LKP.FEAT_ID,
LKP.INSRNC_CVGE_TYPE_CD,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.FEAT
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.FEAT_SBTYPE_CD = exp_data_transformation.OUT_FEAT_SBTYPE_CD AND LKP.NK_SRC_KEY = exp_data_transformation.nk_public_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc)  
= 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT	PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD,
		PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME,
		PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM,
		PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM,
		PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,
		PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM	db_t_prod_core.PRTY_ASSET 
WHERE	PRTY_ASSET_SBTYPE_CD NOT IN (''REALSP'',''OTH'',''REALDW'',''MVEH'') 
	AND	CAST(EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,
		PRTY_ASSET_CLASFCN_CD 
ORDER	BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_data_transformation.fixedid AND LKP.PRTY_ASSET_SBTYPE_CD = exp_data_transformation.o_typecode AND LKP.PRTY_ASSET_CLASFCN_CD = exp_data_transformation.o_classification_code
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc)  
= 1
);


-- Component LKP_AGMT_INSRD_ASSET_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_INSRD_ASSET_FEAT AS
(
SELECT
LKP.AGMT_ID,
LKP.FEAT_ID,
LKP.PRTY_ASSET_ID,
LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD,
LKP.AGMT_ASSET_STRT_DTTM,
LKP.AGMT_ASSET_FEAT_STRT_DTTM,
LKP.AGMT_ASSET_FEAT_END_DTTM,
LKP.AGMT_ASSET_FEAT_AMT,
LKP.AGMT_ASSET_FEAT_DT,
LKP.FEAT_EFECT_TYPE_CD,
LKP.AGMT_ASSET_FEAT_TXT,
LKP.AGMT_ASSET_FEAT_IND,
LKP.PRTY_CNTCT_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.RATE_SYMB_CD,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.FEAT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_END_DTTM desc,LKP.AGMT_ASSET_FEAT_AMT desc,LKP.AGMT_ASSET_FEAT_DT desc,LKP.FEAT_EFECT_TYPE_CD desc,LKP.AGMT_ASSET_FEAT_TXT desc,LKP.AGMT_ASSET_FEAT_IND desc,LKP.PRTY_CNTCT_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.RATE_SYMB_CD desc) RNK
FROM
exp_data_transformation
INNER JOIN LKP_AGMT ON exp_data_transformation.source_record_id = LKP_AGMT.source_record_id
INNER JOIN LKP_FEAT ON LKP_AGMT.source_record_id = LKP_FEAT.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID ON LKP_FEAT.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN (
SELECT 
AGMT_INSRD_ASSET_FEAT.AGMT_ID as AGMT_ID, AGMT_INSRD_ASSET_FEAT.FEAT_ID as FEAT_ID, AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID as PRTY_ASSET_ID, 
AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_STRT_DTTM as AGMT_ASSET_FEAT_STRT_DTTM, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_END_DTTM as AGMT_ASSET_FEAT_END_DTTM,
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT, 
AGMT_INSRD_ASSET_FEAT.FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD, AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT, AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND, AGMT_INSRD_ASSET_FEAT.PRTY_CNTCT_ID as PRTY_CNTCT_ID, 
AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM as EDW_END_DTTM,  AGMT_INSRD_ASSET_FEAT.RATE_SYMB_CD as RATE_SYMB_CD 
FROM	db_t_prod_core.AGMT_INSRD_ASSET_FEAT   JOIN db_t_prod_core.AGMT_PROD 
	ON	AGMT_INSRD_ASSET_FEAT.AGMT_ID=AGMT_PROD.AGMT_ID
  JOIN db_t_prod_core.PROD 
	ON	AGMT_PROD.PROD_ID=PROD.PROD_ID 
 WHERE PROD.INSRNC_LOB_TYPE_CD=''BO''  AND	CAST(AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY  AGMT_INSRD_ASSET_FEAT.AGMT_ID,
  AGMT_INSRD_ASSET_FEAT.FEAT_ID,AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID,
  AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD 
  ORDER BY AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM DESC) = 1
/*  */
) LKP ON LKP.AGMT_ID = LKP_AGMT.AGMT_ID AND LKP.FEAT_ID = LKP_FEAT.FEAT_ID AND LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID AND LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_data_transformation.out_cntrct_role
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.FEAT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_END_DTTM desc,LKP.AGMT_ASSET_FEAT_AMT desc,LKP.AGMT_ASSET_FEAT_DT desc,LKP.FEAT_EFECT_TYPE_CD desc,LKP.AGMT_ASSET_FEAT_TXT desc,LKP.AGMT_ASSET_FEAT_IND desc,LKP.PRTY_CNTCT_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.RATE_SYMB_CD desc)  
= 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_INSRD_ASSET_FEAT.FEAT_ID as lkp_FEAT_ID,
LKP_AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DT,
LKP_AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_STRT_DTTM as lkp_AGMT_ASSET_STRT_DT,
LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_END_DTTM as lkp_AGMT_ASSET_FEAT_END_DT,
LKP_AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_AGMT_INSRD_ASSET_FEAT.FEAT_EFECT_TYPE_CD as lkp_FEAT_EFECT_TYPE_CD,
MD5 ( to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_STRT_DTTM) ) || to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_STRT_DTTM) ) || to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_END_DTTM) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT.RATE_SYMB_CD ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_AMT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_DT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_TXT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_IND ) ) || RTRIM ( LTRIM ( LKP_AGMT_INSRD_ASSET_FEAT.FEAT_EFECT_TYPE_CD ) ) || LKP_AGMT_INSRD_ASSET_FEAT.PRTY_CNTCT_ID ) as ORIG_CHKSM,
LKP_AGMT.AGMT_ID as AGMT_ID,
LKP_FEAT.FEAT_ID as FEAT_ID,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_data_transformation.out_cntrct_role as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_data_transformation.asset_start_dt as AGMT_ASSET_STRT_DT,
exp_data_transformation.feature_start_dt as AGMT_ASSET_FEAT_STRT_DT,
exp_data_transformation.feature_end_dt as AGMT_ASSET_FEAT_END_DT,
exp_data_transformation.o_UpdateTime as in_TRANS_STRT_DTTM,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
Decode ( LKP_FEAT.INSRNC_CVGE_TYPE_CD , ''COMP'' , exp_pass_from_source.Ratesymbol_alfa , ''COLL'' , exp_pass_from_source.RateSymbolCollision_alfa , '''' ) as v_RATE_SYMB_CD,
v_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_data_transformation.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_data_transformation.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_data_transformation.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_data_transformation.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
MD5 ( to_char ( DATE_TRUNC(DAY, exp_data_transformation.feature_start_dt) ) || to_char ( DATE_TRUNC(DAY, exp_data_transformation.asset_start_dt) ) || TO_CHAR ( DATE_TRUNC(DAY, exp_data_transformation.feature_end_dt) ) || rtrim ( ltrim ( v_RATE_SYMB_CD ) ) || rtrim ( ltrim ( exp_data_transformation.AGMT_ASSET_FEAT_AMT ) ) || rtrim ( ltrim ( exp_data_transformation.AGMT_ASSET_FEAT_DT ) ) || rtrim ( ltrim ( exp_data_transformation.AGMT_ASSET_FEAT_TXT ) ) || rtrim ( ltrim ( exp_data_transformation.AGMT_ASSET_FEAT_IND ) ) || RTRIM ( LTRIM ( exp_data_transformation.o_DiscountSurcharge_alfa_typecd ) ) || exp_data_transformation.V_PRTY_ID ) as CALC_CHKSM,
CASE WHEN ORIG_CHKSM IS NULL THEN ''I'' ELSE CASE WHEN ORIG_CHKSM != CALC_CHKSM THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_Default_EndDate,
exp_data_transformation.Retired as Retired,
exp_data_transformation.polcov_RateModifier as polcov_RateModifier,
exp_data_transformation.polcov_Eligible as polcov_Eligible,
exp_data_transformation.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_data_transformation.V_PRTY_ID as V_PRTY_ID,
exp_pass_from_source.source_record_id
FROM
exp_pass_from_source
INNER JOIN exp_data_transformation ON exp_pass_from_source.source_record_id = exp_data_transformation.source_record_id
INNER JOIN LKP_AGMT ON exp_data_transformation.source_record_id = LKP_AGMT.source_record_id
INNER JOIN LKP_FEAT ON LKP_AGMT.source_record_id = LKP_FEAT.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID ON LKP_FEAT.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_AGMT_INSRD_ASSET_FEAT ON LKP_PRTY_ASSET_ID.source_record_id = LKP_AGMT_INSRD_ASSET_FEAT.source_record_id
);


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_INSERT AS
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd.lkp_FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD,
exp_ins_upd.AGMT_ID as AGMT_ID,
exp_ins_upd.FEAT_ID as FEAT_ID,
exp_ins_upd.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_END_DT as lkp_AGMT_ASSET_FEAT_END_DT,
exp_ins_upd.in_TRANS_STRT_DTTM as o_Default_Date,
exp_ins_upd.o_Default_EndDate as o_Default_EndDate,
exp_ins_upd.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_ins_upd.Retired as Retired,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_ins_upd.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_ins_upd.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_ins_upd.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
exp_ins_upd.polcov_RateModifier as polcov_RateModifier,
exp_ins_upd.polcov_Eligible as polcov_Eligible,
exp_ins_upd.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_ins_upd.V_PRTY_ID as V_PRTY_ID,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.AGMT_ID IS NOT NULL AND exp_ins_upd.FEAT_ID IS NOT NULL AND exp_ins_upd.PRTY_ASSET_ID IS NOT NULL AND ( exp_ins_upd.out_ins_upd = ''I'' ) OR ( exp_ins_upd.Retired = 0 AND exp_ins_upd.lkp_EDW_END_DTTM != to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ) 
OR 
-- exp_ins_upd.AGMT_ID IS NOT NULL AND exp_ins_upd.out_ins_upd = ''U'' AND exp_ins_upd.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) 
exp_ins_upd.AGMT_ID IS NOT NULL AND exp_ins_upd.FEAT_ID IS NOT NULL AND exp_ins_upd.PRTY_ASSET_ID IS NOT NULL AND ( exp_ins_upd.out_ins_upd = ''U'' ) AND exp_ins_upd.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_RETIRE AS
(
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd.lkp_FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD,
exp_ins_upd.AGMT_ID as AGMT_ID,
exp_ins_upd.FEAT_ID as FEAT_ID,
exp_ins_upd.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_AGMT_ASSET_FEAT_END_DT as lkp_AGMT_ASSET_FEAT_END_DT,
exp_ins_upd.in_TRANS_STRT_DTTM as o_Default_Date,
exp_ins_upd.o_Default_EndDate as o_Default_EndDate,
exp_ins_upd.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_ins_upd.Retired as Retired,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_ins_upd.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_ins_upd.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_ins_upd.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_ins_upd.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
exp_ins_upd.polcov_RateModifier as polcov_RateModifier,
exp_ins_upd.polcov_Eligible as polcov_Eligible,
exp_ins_upd.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_ins_upd.V_PRTY_ID as V_PRTY_ID,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.AGMT_ID IS NOT NULL AND exp_ins_upd.FEAT_ID IS NOT NULL AND exp_ins_upd.PRTY_ASSET_ID IS NOT NULL AND exp_ins_upd.out_ins_upd = ''R'' and exp_ins_upd.Retired != 0 and exp_ins_upd.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ));


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_RETIRE.lkp_AGMT_ID as lkp_AGMT_ID3,
rtr_ins_upd_RETIRE.lkp_FEAT_ID as lkp_FEAT_ID3,
rtr_ins_upd_RETIRE.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
rtr_ins_upd_RETIRE.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID3,
rtr_ins_upd_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_RETIRE.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT3,
rtr_ins_upd_RETIRE.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT3,
rtr_ins_upd_RETIRE.PRCS_ID as PRCS_ID3,
rtr_ins_upd_RETIRE.o_Default_Date as o_Default_Date4,
rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT4,
rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT4,
rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT4,
rtr_ins_upd_RETIRE.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND4,
rtr_ins_upd_RETIRE.polcov_RateModifier as polcov_RateModifier,
rtr_ins_upd_RETIRE.polcov_Eligible as polcov_Eligible,
rtr_ins_upd_RETIRE.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd4,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_RETIRE.source_record_id as source_record_id
FROM
rtr_ins_upd_RETIRE
);


-- Component upd_ins_new, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_new AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_INSERT.AGMT_ID as AGMT_ID,
rtr_ins_upd_INSERT.FEAT_ID as FEAT_ID,
rtr_ins_upd_INSERT.PRTY_ASSET_ID as PRTY_ASSET_ID,
rtr_ins_upd_INSERT.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
rtr_ins_upd_INSERT.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_ins_upd_INSERT.o_Default_Date as o_Default_Date3,
rtr_ins_upd_INSERT.o_Default_EndDate as o_Default_EndDate1,
rtr_ins_upd_INSERT.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
rtr_ins_upd_INSERT.Retired as Retired1,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT1,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT1,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT1,
rtr_ins_upd_INSERT.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND1,
rtr_ins_upd_INSERT.polcov_RateModifier as polcov_RateModifier,
rtr_ins_upd_INSERT.polcov_Eligible as polcov_Eligible,
rtr_ins_upd_INSERT.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd1,
rtr_ins_upd_INSERT.V_PRTY_ID as V_PRTY_ID2,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_INSERT.source_record_id as source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
upd_update.lkp_AGMT_ID3 as lkp_AGMT_ID3,
upd_update.lkp_FEAT_ID3 as lkp_FEAT_ID3,
upd_update.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3 as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
upd_update.lkp_PRTY_ASSET_ID3 as lkp_PRTY_ASSET_ID3,
upd_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_update.o_Default_Date4 as o_Default_Date4,
upd_update.o_Default_Date4 as out_trans_end_dttm4,
upd_update.AGMT_ASSET_FEAT_AMT4 as AGMT_ASSET_FEAT_AMT4,
upd_update.AGMT_ASSET_FEAT_DT4 as AGMT_ASSET_FEAT_DT4,
upd_update.AGMT_ASSET_FEAT_TXT4 as AGMT_ASSET_FEAT_TXT4,
upd_update.AGMT_ASSET_FEAT_IND4 as AGMT_ASSET_FEAT_IND4,
upd_update.polcov_RateModifier as polcov_RateModifier,
upd_update.polcov_Eligible as polcov_Eligible,
upd_update.o_DiscountSurcharge_alfa_typecd4 as o_DiscountSurcharge_alfa_typecd4,
upd_update.source_record_id
FROM
upd_update
);


-- Component exp_pass_src_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_src_to_tgt_ins AS
(
SELECT
upd_ins_new.AGMT_ID as AGMT_ID,
upd_ins_new.FEAT_ID as FEAT_ID,
upd_ins_new.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_ins_new.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_ins_new.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
upd_ins_new.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
upd_ins_new.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
upd_ins_new.PRCS_ID as PRCS_ID,
upd_ins_new.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN upd_ins_new.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_ins_new.EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_ins_new.o_Default_Date3 as o_Default_Date3,
upd_ins_new.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
upd_ins_new.AGMT_ASSET_FEAT_AMT1 as AGMT_ASSET_FEAT_AMT1,
upd_ins_new.AGMT_ASSET_FEAT_DT1 as AGMT_ASSET_FEAT_DT1,
upd_ins_new.AGMT_ASSET_FEAT_TXT1 as AGMT_ASSET_FEAT_TXT1,
upd_ins_new.AGMT_ASSET_FEAT_IND1 as AGMT_ASSET_FEAT_IND1,
CASE WHEN upd_ins_new.Retired1 = 0 THEN to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ELSE upd_ins_new.o_Default_Date3 END as TRNS_END_DTTM,
upd_ins_new.polcov_RateModifier as polcov_RateModifier,
upd_ins_new.polcov_Eligible as polcov_Eligible,
upd_ins_new.o_DiscountSurcharge_alfa_typecd1 as o_DiscountSurcharge_alfa_typecd1,
upd_ins_new.V_PRTY_ID2 as V_PRTY_ID2,
upd_ins_new.source_record_id
FROM
upd_ins_new
);


-- Component AGMT_INSRD_ASSET_FEAT_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
USING exp_pass_to_tgt_upd ON (AGMT_INSRD_ASSET_FEAT.AGMT_ID = exp_pass_to_tgt_upd.lkp_AGMT_ID3 AND AGMT_INSRD_ASSET_FEAT.FEAT_ID = exp_pass_to_tgt_upd.lkp_FEAT_ID3 AND AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID = exp_pass_to_tgt_upd.lkp_PRTY_ASSET_ID3 AND AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3 AND AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_tgt_upd.lkp_AGMT_ID3,
FEAT_ID = exp_pass_to_tgt_upd.lkp_FEAT_ID3,
PRTY_ASSET_ID = exp_pass_to_tgt_upd.lkp_PRTY_ASSET_ID3,
ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
AGMT_ASSET_FEAT_AMT = exp_pass_to_tgt_upd.AGMT_ASSET_FEAT_AMT4,
AGMT_ASSET_FEAT_RATE = exp_pass_to_tgt_upd.polcov_RateModifier,
AGMT_ASSET_FEAT_DT = exp_pass_to_tgt_upd.AGMT_ASSET_FEAT_DT4,
FEAT_EFECT_TYPE_CD = exp_pass_to_tgt_upd.o_DiscountSurcharge_alfa_typecd4,
AGMT_ASSET_FEAT_TXT = exp_pass_to_tgt_upd.AGMT_ASSET_FEAT_TXT4,
AGMT_ASSET_FEAT_IND = exp_pass_to_tgt_upd.AGMT_ASSET_FEAT_IND4,
FEAT_ELGBL_IND = exp_pass_to_tgt_upd.polcov_Eligible,
EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_upd.EDW_END_DTTM,
TRANS_STRT_DTTM = exp_pass_to_tgt_upd.o_Default_Date4,
TRANS_END_DTTM = exp_pass_to_tgt_upd.out_trans_end_dttm4;


-- Component AGMT_INSRD_ASSET_FEAT_insert_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_FEAT_STRT_DTTM,
AGMT_ASSET_FEAT_END_DTTM,
RATE_SYMB_CD,
AGMT_ASSET_FEAT_AMT,
AGMT_ASSET_FEAT_RATE,
AGMT_ASSET_FEAT_DT,
FEAT_EFECT_TYPE_CD,
AGMT_ASSET_FEAT_TXT,
AGMT_ASSET_FEAT_IND,
FEAT_ELGBL_IND,
PRTY_CNTCT_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_src_to_tgt_ins.AGMT_ID as AGMT_ID,
exp_pass_src_to_tgt_ins.FEAT_ID as FEAT_ID,
exp_pass_src_to_tgt_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_pass_src_to_tgt_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_src_to_tgt_ins.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DTTM,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DTTM,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DTTM,
exp_pass_src_to_tgt_ins.out_RATE_SYMB_CD as RATE_SYMB_CD,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_AMT1 as AGMT_ASSET_FEAT_AMT,
exp_pass_src_to_tgt_ins.polcov_RateModifier as AGMT_ASSET_FEAT_RATE,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_DT1 as AGMT_ASSET_FEAT_DT,
exp_pass_src_to_tgt_ins.o_DiscountSurcharge_alfa_typecd1 as FEAT_EFECT_TYPE_CD,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_TXT1 as AGMT_ASSET_FEAT_TXT,
exp_pass_src_to_tgt_ins.AGMT_ASSET_FEAT_IND1 as AGMT_ASSET_FEAT_IND,
exp_pass_src_to_tgt_ins.polcov_Eligible as FEAT_ELGBL_IND,
exp_pass_src_to_tgt_ins.V_PRTY_ID2 as PRTY_CNTCT_ID,
exp_pass_src_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_src_to_tgt_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_src_to_tgt_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_src_to_tgt_ins.o_Default_Date3 as TRANS_STRT_DTTM,
exp_pass_src_to_tgt_ins.TRNS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_src_to_tgt_ins;


-- PIPELINE END FOR 1
-- Component AGMT_INSRD_ASSET_FEAT_upd_retired, Type Post SQL 
UPDATE db_t_prod_core.AGMT_INSRD_ASSET_FEAT
   set EDW_END_DTTM=A.lead1,

  TRANS_END_DTTM=A.lead2

FROM

            (SELECT   distinct  AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,

                        max(EDW_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD

                        ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

                        as lead1

                       ,max(TRANS_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD

                       ORDER BY TRANS_STRT_DTTM  ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''

                        as lead2

                       FROM             db_t_prod_core.AGMT_INSRD_ASSET_FEAT  

                      group by   AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM

                          ) A


  where  AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

                                                    and AGMT_INSRD_ASSET_FEAT.AGMT_ID=A.AGMT_ID

                          and AGMT_INSRD_ASSET_FEAT.FEAT_ID=A.FEAT_ID

                          and AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID=A.PRTY_ASSET_ID

                          and AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD

                          and CAST(AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''

                                                   AND lead1 IS NOT NULL

                         AND lead2 IS NOT NULL;


-- PIPELINE START FOR 2

-- Component SQ_pc_agmt_insrd_asset_feat_x1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_agmt_insrd_asset_feat_x1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as pol_strt_dt,
$3 as feature_strt_dt,
$4 as feature_end_dt,
$5 as Cntrct_role,
$6 as nk_public_id,
$7 as feat_sbtype_cd,
$8 as typecode,
$9 as classification_code,
$10 as fixed_id,
$11 as asset_strt_dt,
$12 as UPDTAETIME,
$13 as SRC_CD,
$14 as RateSymbolCollision_alfa,
$15 as Ratesymbol_alfa,
$16 as Retired,
$17 as FEAT_VAL,
$18 as FEAT_COVTERMTYPE,
$19 as polcov_RateModifier,
$20 as polcov_Eligible,
$21 as DiscountSurcharge_alfa_typecd,
$22 as Addressbookuid,
$23 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* EIM-35923 Bring in Building to DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT for BOP\\CH */


/* Union DB_T_PROD_STAG.pcx_bp7buildingcov, DB_T_PROD_STAG.pcx_bp7buildingexcl and DB_T_PROD_STAG.pcx_bp7buildingcov */


/* BUILDING COV WITH BUILDING--- */


SELECT	* 

FROM	( 

	SELECT	pcx_bp7_agmt_insrd_asset_feat_x.PublicID, pcx_bp7_agmt_insrd_asset_feat_x.pol_strt_dt,

			pcx_bp7_agmt_insrd_asset_feat_x.feature_strt_dt, pcx_bp7_agmt_insrd_asset_feat_x.feature_end_dt,

			pcx_bp7_agmt_insrd_asset_feat_x.Cntrct_role,pcx_bp7_agmt_insrd_asset_feat_x.nk_public_id,

			pcx_bp7_agmt_insrd_asset_feat_x.feat_sbtype_cd, pcx_bp7_agmt_insrd_asset_feat_x.typecode,

			pcx_bp7_agmt_insrd_asset_feat_x.classification_code, pcx_bp7_agmt_insrd_asset_feat_x.fixed_id,

			pcx_bp7_agmt_insrd_asset_feat_x.asset_strt_dt, pcx_bp7_agmt_insrd_asset_feat_x.updatetime,

			pcx_bp7_agmt_insrd_asset_feat_x.SRC_CD, RateSymbolCollision_alfa,

			RateSymbol_alfa, Retired, pcx_bp7_agmt_insrd_asset_feat_x.FEAT_VAL,

			pcx_bp7_agmt_insrd_asset_feat_x.FEAT_COVTERMTYPE, pcx_bp7_agmt_insrd_asset_feat_x.polcov_RateModifier,

			substr (pcx_bp7_agmt_insrd_asset_feat_x.polcov_Eligible,1,1) as eligible,

			DiscountSurcharge_alfa_typecd, addressbookuid, ROW_NUMBER() OVER( 

	PARTITION BY PublicID,nk_public_id,feat_sbtype_cd,typecode,classification_code,

			fixed_id,Cntrct_role 

	ORDER BY pol_strt_dt DESC) AS rankid 

	FROM	( /*3 FLOW PIPE LINE IN MAPPING*/ /*DB_T_CORE_DM_PROD.Coverage BUILDING COV WITH BUILDING*/

	select	distinct pc_policyperiod.PUBLICID_stg as PublicID, pc_policyperiod.PeriodStart_stg as pol_strt_dt,

			case 

				when polcov.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polcov.EffectiveDate_stg 

			end as feature_strt_dt, 

			case 

				when polcov.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polcov.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100)) =''package'' then cast(package.packagePatternID_stg as varchar(100)) 

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polcov.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60)) =''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''option'' 

		and polcov.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,

			case 

				when polcov.assettype_stg in ( ''BP7Building'') then ''PRTY_ASSET_SBTYPE32''  

			end as typecode, 

			case 

				when polcov.assettype_stg in (''BP7Building'') then ''PRTY_ASSET_CLASFCN10''

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polcov.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255)) as RateSymbolCollision_alfa,

			 cast(null as varchar(255)) as RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polcov.val_stg as FEAT_VAL, null as polcov_RateModifier, null as polcov_Eligible,

			covterm.CovTermType_stg as FEAT_COVTERMTYPE,  cast(null as varchar(255)) as DiscountSurcharge_alfa_typecd,

			coalesce(pc_con_ins.addressbookuid_ins_stg, pc_con.addressbookuid_int_stg)addressbookuid  

	from	( 

	/*pcx_bp7buildingcov*/ 

select	cast(''ChoiceTerm1'' as varchar(50)) as columnname_stg, cast(ChoiceTerm1_stg as varchar(255)) as val_stg,

			cast(patterncode_stg as varchar(255)) as patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, cast(''bp7building'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building  b 

		on a.building_stg =b.fixedid_stg

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building  b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm6'' as columnname_stg, ChoiceTerm6_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm6Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

						and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

			and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

					and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

						and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join  DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

			and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm4'' as columnname_stg, StringTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	StringTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

			and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm5'' as columnname_stg, StringTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	StringTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

					and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255))  as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			null as ChoiceTerm1_stg, null as patternid_stg, a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingcov a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and ChoiceTerm6Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and StringTerm4Avl_stg is null 

		and StringTerm5Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null and b.expirationdate_stg is null

			and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	 )polcov  

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7Building'')

				) covterm 

		on covterm.clausePatternID_stg = polcov.patterncode_stg 

		and covterm.ColumnName_stg = polcov.ColumnName_stg   

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polcov.val_stg  

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name_stg, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polcov.val_stg  

	left outer join ( 

		select	cast(a.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg,

				cast(null as varchar(255))additionalinterest_stg, cast(null as varchar(255))additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a join DB_T_PROD_STAG.pc_building b 

			on b.fixedid_stg =a.building_stg 

			and a.BranchID_stg=b.BranchID_stg 

		where	a.ExpirationDate_stg is null 

	 ) polveh 

		on polcov.assetkey_stg =polveh.fixedid_stg 

		and polcov.BranchID_stg=polveh.BranchID_stg 

		and polcov.assettype_stg=polveh.assettype_stg 

		and r=1  

	

	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg )pc_con 

		on cast(addlinter_id as integer) =cast(polveh.additionalinterest_stg as integer)   

	left join ( 

		select	 distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) =cast(polveh.additionalinsured_stg as integer)  

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod  ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polcov.BranchID_stg , polveh.BranchID_stg)

		join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 



		

	union

	



/* DB_T_PROD_STAG.pcx_bp7buildingexcl with DB_T_PROD_STAG.pcx_BP7Building qry */


select	distinct pc_policyperiod.PUBLICID_stg as PublicID,  pc_policyperiod.PeriodStart_stg as pol_strt_dt,

			case 

				when polexcl.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polexcl.EffectiveDate_stg 

			end as feature_strt_dt,  

			case 

				when polexcl.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polexcl.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100))=''package'' then cast(package.packagePatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polexcl.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60))=''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60))=''option'' 

		and polexcl.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,  

			case 

				when polexcl.assettype_stg in (''BP7Building'') then ''PRTY_ASSET_SBTYPE32'' 

			end as typecode,  

			case 

				when polexcl.assettype_stg in ( ''BP7Building'') then ''PRTY_ASSET_CLASFCN10'' 

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polexcl.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255)) as RateSymbolCollision_alfa,

			 cast(null as varchar(255)) as RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polexcl.val_stg as FEAT_VAL, null as polcov_RateModifier,  null as polcov_Eligible,

			covterm.CovTermType_stg as FEAT_COVTERMTYPE,  cast(null as varchar(255)) as DiscountSurcharge_alfa_typecd,

			coalesce( pc_con.addressbookUID_INT_stg, pc_con_ins.addressbookUID_INS_stg) addressbookuid 

	from	( 

	/*pcx_bp7buildingexcl*/ 

select	cast(''ChoiceTerm1'' as varchar(50)) as columnname_stg, cast(ChoiceTerm1_stg as varchar(255)) as val_stg,

			cast(patterncode_stg as varchar(255)) as patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) as ChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.Fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null 

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null 

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null 

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

		inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null 

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and  b.ExpirationDate_stg is null 

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg 

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''Clause'' as columnname_stg, cast(null as varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255)) asChoiceTerm1_stg, cast(null as varchar(255)) as patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.pcx_bp7buildingexcl a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null

				and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	 ) polexcl 

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7Building'') ) covterm 

		on covterm.clausePatternID_stg = polexcl.patterncode_stg 

		and covterm.ColumnName_stg = polexcl.ColumnName_stg 

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id_stg, pc_etlcovtermpackage.PackageCode_stg as name_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polexcl.val_stg 

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name_stg, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polexcl.val_stg 

	left outer join ( 

		select	cast(b.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg,

				cast(null as varchar(255)) as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a 

		join DB_T_PROD_STAG.pc_building b 

			on b.fixedid_stg =a.building_stg 

			and a.BranchID_stg=b.BranchID_stg

		where	a.ExpirationDate_stg is null 

		 ) polveh 

		on polexcl.assetkey_stg =polveh.fixedid_stg 

		and polexcl.BranchID_stg=polveh.BranchID_stg 

		and polexcl.assettype_stg=polveh.assettype_stg 

		and r=1 

	left outer join ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con 

		on cast(addlinter_id as integer) = cast(polveh.additionalinterest_stg as integer) 

	left join ( 

		select	  distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				 pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) = cast( polveh.additionalinsured_stg as integer) 

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polexcl.BranchID_stg , polveh.BranchID_stg) join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 





union

		

		

/* DB_T_PROD_STAG.PCX_BP7BUILDINGCOND with DB_T_PROD_STAG.pcx_BP7Building Query */


select	distinct pc_policyperiod.PUBLICID_stg as PublicID, pc_policyperiod.PeriodStart_stg as pol_start_dt,

			case 

				when polcond.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg 

				else polcond.EffectiveDate_stg 

			end as feature_strt_dt,  

			case 

				when polcond.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg 

				else polcond.ExpirationDate_stg 

			end as feature_end_dt,  cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 case 

				when cast(covterm.CovTermType_stg as varchar(100)) =''package'' then cast(package.packagePatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''option'' 

		and polcond.val_stg is not null then cast(optn.optionPatternID_stg as varchar(100))

				when cast(covterm.CovTermType_stg as varchar(100)) =''Clause'' then cast(covterm.clausePatternID_stg as varchar(100))

				else cast(covterm.covtermPatternID_stg as varchar(100))

			end as nk_public_id,  

			case 

				when cast(covterm.CovTermType_stg as varchar(60)) =''package'' then cast(''PACKAGE'' as varchar (50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''option'' 

		and polcond.val_stg is not null then cast(''OPTIONS'' as varchar(50)) 

				when cast(covterm.CovTermType_stg as varchar(60)) =''Clause'' then cast(''CLAUSE'' as varchar(50)) 

				else cast(''COVTERM'' as varchar (50)) 

			end as feat_sbtype_cd,  

			case 

				when polcond.assettype_stg in (''BP7Building'') then ''PRTY_ASSET_SBTYPE32'' 

			end as typecode,  

			case 

				when polcond.assettype_stg in (''BP7Building'') then ''PRTY_ASSET_CLASFCN10''  

			end as classification_code,  polveh.fixedid_stg as fixed_id, COALESCE(polveh.EffectiveDate_stg,

			pc_policyperiod.PeriodStart_stg) as asset_strt_dt, polcond.updatetime_stg as updatetime,

			 ''SRC_SYS4'' as SRC_CD, cast(null as varchar(255))  as  RateSymbolCollision_alfa,

			 cast(null as varchar(255))  as  RateSymbol_alfa, pc_policyperiod.Retired_stg AS Retired,

			 polcond.val_stg as FEAT_VAL, null  as  polcov_RateModifier,

			 null  as  polcov_Eligible, covterm.CovTermType_stg as FEAT_COVTERMTYPE,

			 cast(null as varchar(255))  as  DiscountSurcharge_alfa_typecd ,

			coalesce(pc_con_ins.addressbookuid_ins_stg,	pc_con.addressbookuid_int_stg) addressbookuid  

	from	(  /*pcx_bp7buildingcond*/ 

		select	cast(''ChoiceTerm1'' as varchar(50)) as columnname_stg, cast(ChoiceTerm1_stg as varchar(255)) as val_stg,

			cast(patterncode_stg as varchar(255)) as patterncode_stg  , cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, cast(''bp7building'' as varchar(255)) as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm2'' as columnname_stg, ChoiceTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm3'' as columnname_stg, ChoiceTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm4'' as columnname_stg, ChoiceTerm4_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm4Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''ChoiceTerm5_stg'' as columnname_stg, ChoiceTerm5_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm5Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm1'' as columnname_stg, cast(DirectTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DirectTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm2'' as columnname_stg, cast(DirectTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DirectTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DirectTerm3'' as columnname_stg, cast(DirectTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DirectTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm1'' as columnname_stg, cast(BooleanTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm2'' as columnname_stg, cast(BooleanTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null  and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''BooleanTerm3'' as columnname_stg, cast(BooleanTerm3_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	BooleanTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm1'' as columnname_stg, StringTerm1_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm2'' as columnname_stg, StringTerm2_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''StringTerm3'' as columnname_stg, StringTerm3_stg as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	StringTerm3Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm1'' as columnname_stg, cast(DateTerm1_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DateTerm1Avl_stg = 1 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''DateTerm2'' as columnname_stg, cast(DateTerm2_stg as varchar(255)) as val_stg,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	DateTerm2Avl_stg = 1 

		and a.ExpirationDate_stg is null  and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	union 

	select	''Clause'' as columnname_stg, cast(cast(null as varchar(255))  as  varchar(255)) val,

			patterncode_stg, cast(a.BranchID_stg as varchar(255)) as BranchId_stg,

			cast(b.fixedid_stg as varchar(255)) as assetkey_stg, ''bp7building'' as assettype_stg,

			a.createtime_stg, a.EffectiveDate_stg, a.ExpirationDate_stg,

			cast(null as varchar(255))  as  ChoiceTerm1_stg, cast(null as varchar(255))  as  patternid_stg,

			a.updatetime_stg 

	from	DB_T_PROD_STAG.PCX_BP7BUILDINGCOND a join DB_T_PROD_STAG.pcx_BP7Building b 

		on a.building_stg =b.fixedid_stg 

	inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=a.branchid_stg

	where	ChoiceTerm1Avl_stg is null 

		and ChoiceTerm2Avl_stg is null 

		and ChoiceTerm3Avl_stg is null 

		and ChoiceTerm4Avl_stg is null 

		and ChoiceTerm5Avl_stg is null 

		and DirectTerm1Avl_stg is null 

		and DirectTerm2Avl_stg is null 

		and DirectTerm3Avl_stg is null 

		and BooleanTerm1Avl_stg is null 

		and BooleanTerm2Avl_stg is null 

		and BooleanTerm3Avl_stg is null 

		and StringTerm1Avl_stg is null 

		and StringTerm2Avl_stg is null 

		and StringTerm3Avl_stg is null 

		and DateTerm1Avl_stg is null 

		and DateTerm2Avl_stg is null 

		and a.ExpirationDate_stg is null and b.ExpirationDate_stg is null 

		and pp.UpdateTime_stg > (:start_dttm)

        and pp.UpdateTime_stg <= (:end_dttm)

	 ) polcond 

	left join ( 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, pc_etlcovtermpattern.ColumnName_stg,

				pc_etlcovtermpattern.CovTermType_stg, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern join DB_T_PROD_STAG.pc_etlcovtermpattern 

			on pc_etlclausepattern.id_stg = pc_etlcovtermpattern.clausePatternID_stg 

		union 

		select	pc_etlclausepattern.PatternID_stg as clausePatternID_stg,

				pc_etlcovtermpattern.PatternID_stg as covtermPatternID_stg, coalesce(pc_etlcovtermpattern.ColumnName_stg,

				''Clause'') as columnname_stg, coalesce(pc_etlcovtermpattern.CovTermType_stg,

				''Clause'') as covtermtype, pc_etlclausepattern.name_stg as clausename_stg 

		from	DB_T_PROD_STAG.pc_etlclausepattern 

		left join ( 

			select	* 

			from	DB_T_PROD_STAG.pc_etlcovtermpattern 

			where	Name_stg not like ''ZZ%'' ) pc_etlcovtermpattern 

			on pc_etlcovtermpattern.clausePatternID_stg = pc_etlclausepattern.id_stg 

		where	pc_etlclausepattern.Name_stg not like ''ZZ%'' 

			and pc_etlcovtermpattern.Name_stg is null 

			and OwningEntityType_stg in (''BP7Building'')

			) covterm 

		on covterm.clausePatternID_stg = polcond.patterncode_stg 

		and covterm.ColumnName_stg = polcond.ColumnName_stg 

	left outer join ( 

		select	pc_etlcovtermpackage.PatternID_stg as packagePatternID_stg,

				pc_etlcovtermpackage.PackageCode_stg as cov_id, pc_etlcovtermpackage.PackageCode_stg as name 

		from	DB_T_PROD_STAG.pc_etlcovtermpackage ) package 

		on package.packagePatternID_stg = polcond.val_stg 

	left outer join ( 

		select	pc_etlcovtermoption.PatternID_stg as optionPatternID_stg,

				pc_etlcovtermoption.optioncode_stg as name, pc_etlcovtermoption.value_stg,

				pc_etlcovtermpattern.valueType_stg 

		from	DB_T_PROD_STAG.pc_etlcovtermpattern 

		inner join DB_T_PROD_STAG.pc_etlcovtermoption 

			on pc_etlcovtermpattern.id_stg = pc_etlcovtermoption.CoverageTermPatternID_stg ) optn 

		on optn.optionPatternID_stg = polcond.val_stg 

	left outer join ( 

		select	cast(b.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg,

				cast(null as varchar(255))  as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a

		join DB_T_PROD_STAG.pc_building b 

			on b.fixedid_stg =a.building_stg 

			and a.BranchID_stg=b.BranchID_stg

		where	a.ExpirationDate_stg is null 

		 ) polveh 

		on polcond.assetkey_stg =polveh.fixedid_stg 

		and polcond.BranchID_stg=polveh.BranchID_stg 

		and polcond.assettype_stg=polveh.assettype_stg 

		and r=1 



	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con 

		on cast(addlinter_id as integer) =cast(polveh.additionalinterest_stg as integer) 

	LEFT OUTER JOIN ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins 

		on cast(addinsuredid as integer) = cast(polveh.additionalinsured_stg as integer) 

	inner join ( 

		select	cast(id_stg as varchar(255)) as id_stg, PolicyNumber_stg,

				PeriodStart_stg, PNIContactDenorm_stg, PeriodEnd_stg, MostRecentModel_stg,

				Status_stg, JOBID_stg, PUBLICID_stg,  updatetime_stg, Retired_stg 

		from	DB_T_PROD_STAG.pc_policyperiod ) pc_policyperiod 

		on pc_policyperiod.id_stg = coalesce(polcond.BranchID_stg , polveh.BranchID_stg) join DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg join DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg = pc_policyperiod.JobID_stg   join DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg = pc_job.Subtype_stg 

	where	covterm.clausename_stg not like''%ZZ%'' 

		and pctl_policyperiodstatus.TYPECODE_stg = ''Bound'' 

	

/* EIM-36113 BOP & CHURCH DB_T_CORE_DM_PROD.Endorsement */
	UNION

	

	select	distinct pc_policyperiod.PUBLICID as PublicID, pc_policyperiod.PeriodStart as pol_strt_dt,

			case 

				when polcov.EffectiveDate is null then pc_policyperiod.PeriodStart 

				else polcov.EffectiveDate 

			end as feature_strt_dt, 

			case 

				when polcov.ExpirationDate is null then pc_policyperiod.PeriodEnd

				else polcov.ExpirationDate 

			end as feature_end_dt, 

			cast(''ASSET_CNTRCT_ROLE_SBTYPE1'' as varchar(50)) as Cntrct_role,

			 nk_public_id,  

		    cast(''FEAT_SBTYPE15'' AS Varchar(50)) as FEAT_SBTYPE_CD,

			case 

				when polcov.assettype_stg in ( ''BP7Classification'') then ''PRTY_ASSET_SBTYPE13'' 

				when polcov.assettype_stg in ( ''BP7Building'') then ''PRTY_ASSET_SBTYPE32''

			end as typecode, 

			case 

			  when polcov.assettype_stg in ( ''BP7Classification'' ) then pctl_bp7classificationproperty.TYPECODE_stg 

			  when polcov.assettype_stg in (''BP7Building'') then ''PRTY_ASSET_CLASFCN10''

			end as classification_code,  

			pol.fixedid_stg as fixed_id, 

			COALESCE(pol.EffectiveDate_stg,pc_policyperiod.PeriodStart) as asset_strt_dt, 

			polcov.updatetime as updatetime,

			 ''SRC_SYS4'' as SRC_CD, 

			 cast(NULL as varchar(255)) as RateSymbolCollision_alfa,

			 cast(NULL as varchar(255)) as RateSymbol_alfa,

			 pc_policyperiod.Retired as Retired,

			 cast(NULL as varchar(255)) as  feat_val, 

			 NULL as polcov_RateModifier, 

			 NULL AS polcov_Eligible,

			 cast(NULL as varchar(255)) as FEAT_COVTERMTYPE,  

			 cast(NULL as varchar(255)) as DiscountSurcharge_alfa_typecd,

			 coalesce(pc_con_ins.addressbookuid_ins_stg, pc_con.addressbookuid_int_stg)addressbookuid

		

	FROM

(/*  BP7Classification DB_T_CORE_DM_PROD.Coverage */
select distinct

d.formpatterncode_stg as nk_public_id,

cast(a.BranchID_stg  AS VARCHAR(255)) AS BranchId,

cast(a.Classification_stg AS VARCHAR(255)) AS assetkey,

cast( ''BP7Classification'' as varchar(255)) assettype_stg

,a.createtime_stg as createtime

,a.EffectiveDate_stg as EffectiveDate

,a.ExpirationDate_stg as ExpirationDate

,a.updatetime_stg as updatetime,

a.patterncode_stg, e.CoverageSubtype_stg

from DB_T_PROD_STAG.pcx_bp7classificationcov a 

join DB_T_PROD_STAG.pc_policyperiod b on b.id_stg = a.branchid_stg 

join DB_T_PROD_STAG.pc_formpattern c on c.clausepatterncode_stg = a.patterncode_stg

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg

join DB_T_PROD_STAG.pctl_documenttype pd on pd.id_stg = c.DocumentType_stg  

and  pd.typecode_stg = ''endorsement_alfa''

where ( (a.EffectiveDate_stg is null) or( a.EffectiveDate_stg > b.ModelDate_stg and coalesce( a.EffectiveDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))

<> coalesce(a.ExpirationDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))))

and c.Retired_stg = 0

and d.RemovedorSuperseded_stg is null 

and b.updatetime_stg > (:start_dttm)

and b.updatetime_stg<= (:end_dttm)



Union



/* BP7Building DB_T_CORE_DM_PROD.Coverage */
select distinct

d.formpatterncode_stg as nk_public_id,

cast(a.BranchID_stg  AS VARCHAR(255)) AS BranchId,

cast(a.Building_stg AS VARCHAR(255)) AS assetkey,

cast( ''BP7Building'' as varchar(255)) assettype_stg

,a.createtime_stg as createtime

,a.EffectiveDate_stg as EffectiveDate

,a.ExpirationDate_stg as ExpirationDate

,a.updatetime_stg as updatetime,

a.patterncode_stg, e.CoverageSubtype_stg

from DB_T_PROD_STAG.pcx_bp7buildingcov a  

join DB_T_PROD_STAG.pc_policyperiod b on b.id_stg = a.branchid_stg 

join DB_T_PROD_STAG.pc_formpattern c on c.clausepatterncode_stg = a.patterncode_stg

join DB_T_PROD_STAG.pc_form d on d.formpatterncode_stg = c.code_stg and d.branchid_stg = a.branchid_stg

join DB_T_PROD_STAG.pc_etlclausepattern e on e.patternid_stg = a.patterncode_stg

join DB_T_PROD_STAG.pctl_documenttype pd on pd.id_stg = c.DocumentType_stg  

and pd.typecode_stg = ''endorsement_alfa''

where ( (a.EffectiveDate_stg is null) or( a.EffectiveDate_stg > b.ModelDate_stg and coalesce( a.EffectiveDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))

<> coalesce(a.ExpirationDate_stg,cast(''1900-01-01 00:00:00.000000'' as timestamp))))

and c.Retired_stg = 0

and d.RemovedorSuperseded_stg is null 

and b.updatetime_stg > (:start_dttm)

and b.updatetime_stg<= (:end_dttm)



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



LEFT OUTER JOIN ( 

		select	distinct cast(fixedid_stg as varchar(50)) fixedid_stg, cast(BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Classification'' as varchar(255)) assettype_stg , EffectiveDate_stg,

				ExpirationDate_stg, cast(bp7classpropertytype_stg as varchar(255))  as bp7classpropertytype_stg,

				cast(null as varchar(255))  as additionalinterest_stg, cast(null as varchar(255)) as additionalinsured_stg,

				rank() over ( 

		partition by FixedID_stg , branchid_stg 

		order by UpdateTime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_bp7classification 

		where	ExpirationDate_stg is null 

		

	  union

	  

	    select	distinct cast(a.FixedID_stg as varchar(50)) fixedid_stg, cast(a.BranchID_stg as varchar(250)) branchid_stg,

				cast( ''BP7Building'' as varchar(255)) assettype_stg , a.EffectiveDate_stg,

				a.ExpirationDate_stg,cast(null as varchar(255)) as bp7classpropertytype_stg,

				cast(null as varchar(255))additionalinterest_stg, cast(null as varchar(255))additionalinsured_stg ,

				rank() over ( 

		partition by a.FixedID_stg , a.BranchID_stg 

		order by a.updatetime_stg desc) r 

		from	DB_T_PROD_STAG.pcx_BP7Building a join DB_T_PROD_STAG.pc_building b 

			on b.fixedid_stg =a.building_stg 

			and a.BranchID_stg=b.BranchID_stg 

		where	a.ExpirationDate_stg is null 

	   

		) pol 

		on polcov.assetkey =pol.fixedid_stg 

		and polcov.BranchID=pol.BranchID_stg 

		and polcov.assettype_stg=pol.assettype_stg 

		and r=1 

left outer join DB_T_PROD_STAG.pctl_bp7classificationproperty on pctl_bp7classificationproperty.id_stg = pol.bp7classpropertytype_stg 

		join DB_T_PROD_STAG.pc_job on pc_job.id_stg = pc_policyperiod.JobID   

		join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg = pc_job.Subtype_stg 

left outer join ( 

		select	distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INT_stg,

				pc_addlinterestdetail.id_stg addlinter_id 

		from	DB_T_PROD_STAG.pc_addlinterestdetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_addlinterestdetail.policyaddlinterest_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg )pc_con 

		on cast(addlinter_id as integer) =cast(pol.additionalinterest_stg as integer)   

	left join ( 

		select	 distinct AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg AS AddressBookUID_INS_stg,

				pc_policyaddlinsureddetail.id_stg addinsuredid 

		from	DB_T_PROD_STAG.pc_policyaddlinsureddetail join DB_T_PROD_STAG.pc_policycontactrole 

			on pc_policyaddlinsureddetail.PolicyAddlInsured_stg = pc_policycontactrole.id_stg join DB_T_PROD_STAG.pc_contact 

			on pc_policycontactrole.contactdenorm_stg =pc_contact.id_stg 

		inner join DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg=pc_contact.Subtype_stg ) pc_con_ins

		on cast(addinsuredid as integer) =cast(pol.additionalinsured_stg as integer)  

	where 

	 pctl_policyperiodstatus.typecode_stg = ''Bound'' and

	pc_policyperiod.updatetime > (:start_dttm)

AND pc_policyperiod.updatetime <= (:end_dttm)

	

	)pcx_bp7_agmt_insrd_asset_feat_x 

	) tmp 

WHERE	rankid=1  

	and fixed_id is not null
) SRC
)
);


-- Component exp_pass_from_source1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source1 AS
(
SELECT
SQ_pc_agmt_insrd_asset_feat_x1.PublicID as PUBLICID,
SQ_pc_agmt_insrd_asset_feat_x1.pol_strt_dt as pol_start_dt,
SQ_pc_agmt_insrd_asset_feat_x1.feature_strt_dt as feature_start_dt,
to_char ( SQ_pc_agmt_insrd_asset_feat_x1.feature_strt_dt , ''YYYY-MM-DD'' ) as v_feature_start_dt,
SQ_pc_agmt_insrd_asset_feat_x1.feature_end_dt as feature_end_dt,
to_char ( SQ_pc_agmt_insrd_asset_feat_x1.feature_end_dt , ''YYYY-MM-DD'' ) as v_feature_end_dt,
SQ_pc_agmt_insrd_asset_feat_x1.Cntrct_role as cntrct_role,
SQ_pc_agmt_insrd_asset_feat_x1.nk_public_id as nk_public_id,
SQ_pc_agmt_insrd_asset_feat_x1.feat_sbtype_cd as feat_sbtype_cd,
SQ_pc_agmt_insrd_asset_feat_x1.typecode as typecode,
SQ_pc_agmt_insrd_asset_feat_x1.classification_code as classification_code,
SQ_pc_agmt_insrd_asset_feat_x1.fixed_id as fixedid,
SQ_pc_agmt_insrd_asset_feat_x1.asset_strt_dt as asset_start_dt,
SQ_pc_agmt_insrd_asset_feat_x1.SRC_CD as src_cd,
CASE WHEN SQ_pc_agmt_insrd_asset_feat_x1.UPDTAETIME IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE SQ_pc_agmt_insrd_asset_feat_x1.UPDTAETIME END as o_UpdateTime,
SQ_pc_agmt_insrd_asset_feat_x1.RateSymbolCollision_alfa as RateSymbolCollision_alfa,
SQ_pc_agmt_insrd_asset_feat_x1.Retired as Retired,
SQ_pc_agmt_insrd_asset_feat_x1.Ratesymbol_alfa as Ratesymbol_alfa,
SQ_pc_agmt_insrd_asset_feat_x1.FEAT_VAL as FEAT_VAL,
SQ_pc_agmt_insrd_asset_feat_x1.FEAT_COVTERMTYPE as FEAT_COVTERMTYPE,
SQ_pc_agmt_insrd_asset_feat_x1.polcov_RateModifier as polcov_RateModifier,
SQ_pc_agmt_insrd_asset_feat_x1.polcov_Eligible as polcov_Eligible,
SQ_pc_agmt_insrd_asset_feat_x1.DiscountSurcharge_alfa_typecd as DiscountSurcharge_alfa_typecd,
substr ( SQ_pc_agmt_insrd_asset_feat_x1.Addressbookuid , 1 , POSITION(''-'',SQ_pc_agmt_insrd_asset_feat_x1.Addressbookuid) - 1 ) as o_Addressbookuid,
substr ( SQ_pc_agmt_insrd_asset_feat_x1.Addressbookuid , POSITION(''-'',SQ_pc_agmt_insrd_asset_feat_x1.Addressbookuid) + 1 , 10 ) as Prty_TypeCode,
SQ_pc_agmt_insrd_asset_feat_x1.source_record_id
FROM
SQ_pc_agmt_insrd_asset_feat_x1
);


-- Component exp_data_transformation1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation1 AS
(
SELECT
exp_pass_from_source1.PUBLICID as PUBLICID,
exp_pass_from_source1.feature_start_dt as feature_start_dt,
exp_pass_from_source1.feature_end_dt as feature_end_dt,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE */ END as out_cntrct_role,
exp_pass_from_source1.nk_public_id as nk_public_id,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as o_typecode,
CASE WHEN LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as o_classification_code,
exp_pass_from_source1.fixedid as fixedid,
exp_pass_from_source1.asset_start_dt as asset_start_dt,
CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''MODIFIER'' THEN ''FEAT_SBTYPE11'' ELSE CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''OPTIONS'' THEN ''FEAT_SBTYPE8'' ELSE CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''COVTERM'' THEN ''FEAT_SBTYPE6'' ELSE CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''CLAUSE'' THEN ''FEAT_SBTYPE7'' ELSE CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''PACKAGE'' THEN ''FEAT_SBTYPE9'' ELSE CASE WHEN exp_pass_from_source1.feat_sbtype_cd = ''FEAT_SBTYPE15'' THEN ''FEAT_SBTYPE15'' ELSE $3 END END END END END END as var_Feat_sbtype_val,
CASE WHEN LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ END as OUT_FEAT_SBTYPE_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
:p_agmt_type_cd_policy_version as out_AGMT_TYPE_CD_policy,
:PRCS_ID as out_PRCS_ID,
LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_src_cd,
exp_pass_from_source1.o_UpdateTime as o_UpdateTime,
exp_pass_from_source1.Retired as Retired,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_COVTERMTYPE ) ) ) = ''direct'' THEN IFNULL(TRY_TO_DECIMAL(exp_pass_from_source1.FEAT_VAL), 0) ELSE NULL END as AGMT_ASSET_FEAT_AMT,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_COVTERMTYPE ) ) ) = ''datetime'' THEN TO_DATE ( substr ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_VAL ) ) , 0 , 10 ) , ''YYYY-mm-dd'' ) ELSE NULL END as AGMT_ASSET_FEAT_DT,
CASE WHEN ( LOWER ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_COVTERMTYPE ) ) ) = ''shorttext'' ) or ( LOWER ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_COVTERMTYPE ) ) ) = ''typekey'' ) THEN exp_pass_from_source1.FEAT_VAL ELSE $3 END as AGMT_ASSET_FEAT_TXT,
CASE WHEN LOWER ( LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_COVTERMTYPE ) ) ) = ''bit'' THEN LTRIM ( RTRIM ( exp_pass_from_source1.FEAT_VAL ) ) ELSE NULL END as AGMT_ASSET_FEAT_IND,
exp_pass_from_source1.polcov_RateModifier as polcov_RateModifier,
exp_pass_from_source1.polcov_Eligible as polcov_Eligible,
CASE WHEN LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD */ END as o_DiscountSurcharge_alfa_typecd,
CASE WHEN exp_pass_from_source1.Prty_TypeCode = ''Person'' THEN ( LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV */ ) ELSE ( CASE WHEN exp_pass_from_source1.Prty_TypeCode = ''Company'' THEN ( LKP_12.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ ) ELSE null END ) END as V_PRTY_ID,
exp_pass_from_source1.source_record_id,
row_number() over (partition by exp_pass_from_source1.source_record_id order by exp_pass_from_source1.source_record_id) as RNK
FROM
exp_pass_from_source1
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source1.cntrct_role
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_CNTRCT_ROLE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source1.cntrct_role
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_pass_from_source1.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = upper ( rtrim ( ltrim ( exp_pass_from_source1.classification_code ) ) )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = upper ( rtrim ( ltrim ( exp_pass_from_source1.classification_code ) ) )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = var_Feat_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = var_Feat_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = exp_pass_from_source1.src_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = exp_pass_from_source1.DiscountSurcharge_alfa_typecd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_EFFECT_TYPE_CD LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = exp_pass_from_source1.DiscountSurcharge_alfa_typecd
LEFT JOIN LKP_INDIV LKP_11 ON LKP_11.NK_link_ID = exp_pass_from_source1.o_Addressbookuid
LEFT JOIN LKP_BUSN LKP_12 ON LKP_12.NK_BUSN_CD = exp_pass_from_source1.o_Addressbookuid
QUALIFY row_number() over (partition by exp_pass_from_source1.source_record_id order by exp_pass_from_source1.source_record_id) 
= 1
);


-- Component LKP_PRTY_ASSET_ID1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID1 AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transformation1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_data_transformation1
LEFT JOIN (
SELECT	PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD,
		PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME,
		PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM,
		PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM,
		PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,
		PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM	db_t_prod_core.PRTY_ASSET 
WHERE	PRTY_ASSET_SBTYPE_CD NOT IN (''REALSP'',''OTH'',''REALDW'',''MVEH'') 
	AND	CAST(EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,
		PRTY_ASSET_CLASFCN_CD 
ORDER	BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_data_transformation1.fixedid AND LKP.PRTY_ASSET_SBTYPE_CD = exp_data_transformation1.o_typecode AND LKP.PRTY_ASSET_CLASFCN_CD = exp_data_transformation1.o_classification_code
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc)  
= 1
);


-- Component LKP_AGMT1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT1 AS
(
SELECT
LKP.AGMT_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transformation1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_transformation1
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM (SELECT	A.AGMT_ID as AGMT_ID, A.HOST_AGMT_NUM as HOST_AGMT_NUM,
		A.AGMT_NAME as AGMT_NAME, A.AGMT_OPN_DTTM as AGMT_OPN_DTTM,
		A.AGMT_CLS_DTTM as AGMT_CLS_DTTM, A.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM,
		A.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, A.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND,
		A.AGMT_SRC_CD as AGMT_SRC_CD, A.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD,
		A.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, A.AGMT_OBTND_CD as AGMT_OBTND_CD,
		A.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, A.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM,
		A.ALT_AGMT_NAME as ALT_AGMT_NAME, A.ASSET_LIABTY_CD as ASSET_LIABTY_CD,
		A.BAL_SHET_CD as BAL_SHET_CD, A.STMT_CYCL_CD as STMT_CYCL_CD,
		A.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, A.PRPOSL_ID as PRPOSL_ID,
		A.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, A.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD,
		A.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, A.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT,
		A.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD,
		A.BNK_TRD_BK_CD as BNK_TRD_BK_CD, A.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD,
		A.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, A.DY_CNT_BSS_CD as DY_CNT_BSS_CD,
		A.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, A.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD,
		A.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, A.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD,
		A.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, A.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM,
		A.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, A.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM,
		A.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM,
		A.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, A.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT,
		A.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, A.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT,
		A.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, A.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE,
		A.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, A.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND,
		A.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND,
		A.LGCY_DSCNT_IND as LGCY_DSCNT_IND, A.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD,
		A.TRMTN_TYPE_CD as TRMTN_TYPE_CD, A.INT_PMT_METH_CD as INT_PMT_METH_CD,
		A.LBR_AGMT_DESC as LBR_AGMT_DESC, A.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT,
		A.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, A.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT,
		A.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, A.BUSN_PRTY_ID as BUSN_PRTY_ID,
		A.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, A.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD,
		A.MODL_CRTN_DTTM as MODL_CRTN_DTTM, A.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM,
		A.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, A.SRC_SYS_CD as SRC_SYS_CD,
		A.AGMT_EFF_DTTM as AGMT_EFF_DTTM, A.MODL_EFF_DTTM as MODL_EFF_DTTM,
		A.PRCS_ID as PRCS_ID, A.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM,
		A.TIER_TYPE_CD as TIER_TYPE_CD, A.EDW_STRT_DTTM as EDW_STRT_DTTM,
		A.EDW_END_DTTM as EDW_END_DTTM, A.VFYD_PLCY_IND as VFYD_PLCY_IND,
		A.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, A.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD,
		A.LGCY_PLCY_IND as LGCY_PLCY_IND, A.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
		A.NK_SRC_KEY as NK_SRC_KEY, A.AGMT_TYPE_CD as AGMT_TYPE_CD 
FROM	db_t_prod_core.AGMT A  JOIN db_t_prod_core.AGMT_PROD AP
	ON	A.AGMT_ID=AP.AGMT_ID
  JOIN db_t_prod_core.PROD P 
	ON	AP.PROD_ID=P.PROD_ID 
 WHERE P.INSRNC_LOB_TYPE_CD=''BO'' 
	AND	CAST(A.EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY A.NK_SRC_KEY,A.HOST_AGMT_NUM  
ORDER	BY A.EDW_END_DTTM desc) = 1
)agmt
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation1.PUBLICID AND LKP.AGMT_TYPE_CD = exp_data_transformation1.out_AGMT_TYPE_CD_policy
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc)  
= 1
);


-- Component LKP_FEAT1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT1 AS
(
SELECT
LKP.FEAT_ID,
LKP.INSRNC_CVGE_TYPE_CD,
exp_data_transformation1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc) RNK
FROM
exp_data_transformation1
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.FEAT
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.FEAT_SBTYPE_CD = exp_data_transformation1.OUT_FEAT_SBTYPE_CD AND LKP.NK_SRC_KEY = exp_data_transformation1.nk_public_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc)  
= 1
);


-- Component LKP_AGMT_INSRD_ASSET_FEAT1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_INSRD_ASSET_FEAT1 AS
(
SELECT
LKP.AGMT_ID,
LKP.FEAT_ID,
LKP.PRTY_ASSET_ID,
LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD,
LKP.AGMT_ASSET_STRT_DTTM,
LKP.AGMT_ASSET_FEAT_STRT_DTTM,
LKP.AGMT_ASSET_FEAT_END_DTTM,
LKP.AGMT_ASSET_FEAT_AMT,
LKP.AGMT_ASSET_FEAT_DT,
LKP.FEAT_EFECT_TYPE_CD,
LKP.AGMT_ASSET_FEAT_TXT,
LKP.AGMT_ASSET_FEAT_IND,
LKP.PRTY_CNTCT_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.RATE_SYMB_CD,
exp_data_transformation1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.FEAT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_END_DTTM desc,LKP.AGMT_ASSET_FEAT_AMT desc,LKP.AGMT_ASSET_FEAT_DT desc,LKP.FEAT_EFECT_TYPE_CD desc,LKP.AGMT_ASSET_FEAT_TXT desc,LKP.AGMT_ASSET_FEAT_IND desc,LKP.PRTY_CNTCT_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.RATE_SYMB_CD desc) RNK
FROM
exp_data_transformation1
INNER JOIN LKP_PRTY_ASSET_ID1 ON exp_data_transformation1.source_record_id = LKP_PRTY_ASSET_ID1.source_record_id
INNER JOIN LKP_AGMT1 ON LKP_PRTY_ASSET_ID1.source_record_id = LKP_AGMT1.source_record_id
INNER JOIN LKP_FEAT1 ON LKP_AGMT1.source_record_id = LKP_FEAT1.source_record_id
LEFT JOIN (
SELECT 
AGMT_INSRD_ASSET_FEAT.AGMT_ID as AGMT_ID, AGMT_INSRD_ASSET_FEAT.FEAT_ID as FEAT_ID, AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID as PRTY_ASSET_ID, 
AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_STRT_DTTM as AGMT_ASSET_FEAT_STRT_DTTM, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_END_DTTM as AGMT_ASSET_FEAT_END_DTTM,
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT, 
AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT, 
AGMT_INSRD_ASSET_FEAT.FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD, AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT, AGMT_INSRD_ASSET_FEAT.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND, AGMT_INSRD_ASSET_FEAT.PRTY_CNTCT_ID as PRTY_CNTCT_ID, 
AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM as EDW_END_DTTM,  AGMT_INSRD_ASSET_FEAT.RATE_SYMB_CD as RATE_SYMB_CD 
FROM	db_t_prod_core.AGMT_INSRD_ASSET_FEAT   JOIN db_t_prod_core.AGMT_PROD 
	ON	AGMT_INSRD_ASSET_FEAT.AGMT_ID=AGMT_PROD.AGMT_ID
  JOIN db_t_prod_core.PROD 
	ON	AGMT_PROD.PROD_ID=PROD.PROD_ID 
 WHERE PROD.INSRNC_LOB_TYPE_CD=''BO''  AND	CAST(AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''
QUALIFY	ROW_NUMBER() OVER(PARTITION BY  AGMT_INSRD_ASSET_FEAT.AGMT_ID,
  AGMT_INSRD_ASSET_FEAT.FEAT_ID,AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID,
  AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD 
  ORDER BY AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM DESC) = 1
/*  */
) LKP ON LKP.AGMT_ID = LKP_AGMT1.AGMT_ID AND LKP.FEAT_ID = LKP_FEAT1.FEAT_ID AND LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID1.PRTY_ASSET_ID AND LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_data_transformation1.out_cntrct_role
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation1.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.FEAT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.ASSET_CNTRCT_ROLE_SBTYPE_CD desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_STRT_DTTM desc,LKP.AGMT_ASSET_FEAT_END_DTTM desc,LKP.AGMT_ASSET_FEAT_AMT desc,LKP.AGMT_ASSET_FEAT_DT desc,LKP.FEAT_EFECT_TYPE_CD desc,LKP.AGMT_ASSET_FEAT_TXT desc,LKP.AGMT_ASSET_FEAT_IND desc,LKP.PRTY_CNTCT_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.RATE_SYMB_CD desc)  
= 1
);


-- Component exp_ins_upd1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd1 AS
(
SELECT
LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_INSRD_ASSET_FEAT1.FEAT_ID as lkp_FEAT_ID,
LKP_AGMT_INSRD_ASSET_FEAT1.ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_STRT_DTTM as lkp_AGMT_ASSET_FEAT_STRT_DT,
LKP_AGMT_INSRD_ASSET_FEAT1.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_STRT_DTTM as lkp_AGMT_ASSET_STRT_DT,
LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_END_DTTM as lkp_AGMT_ASSET_FEAT_END_DT,
LKP_AGMT_INSRD_ASSET_FEAT1.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_AGMT_INSRD_ASSET_FEAT1.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_AGMT_INSRD_ASSET_FEAT1.FEAT_EFECT_TYPE_CD as lkp_FEAT_EFECT_TYPE_CD,
MD5 ( to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_STRT_DTTM) ) || to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_STRT_DTTM) ) || to_char ( DATE_TRUNC(DAY, LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_END_DTTM) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT1.RATE_SYMB_CD ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_AMT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_DT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_TXT ) ) || rtrim ( ltrim ( LKP_AGMT_INSRD_ASSET_FEAT1.AGMT_ASSET_FEAT_IND ) ) || RTRIM ( LTRIM ( LKP_AGMT_INSRD_ASSET_FEAT1.FEAT_EFECT_TYPE_CD ) ) || LKP_AGMT_INSRD_ASSET_FEAT1.PRTY_CNTCT_ID ) as ORIG_CHKSM,
LKP_AGMT1.AGMT_ID as AGMT_ID,
LKP_FEAT1.FEAT_ID as FEAT_ID,
LKP_PRTY_ASSET_ID1.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_data_transformation1.out_cntrct_role as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_data_transformation1.asset_start_dt as AGMT_ASSET_STRT_DT,
exp_data_transformation1.feature_start_dt as AGMT_ASSET_FEAT_STRT_DT,
exp_data_transformation1.feature_end_dt as AGMT_ASSET_FEAT_END_DT,
exp_data_transformation1.o_UpdateTime as in_TRANS_STRT_DTTM,
exp_data_transformation1.out_PRCS_ID as PRCS_ID,
Decode ( LKP_FEAT1.INSRNC_CVGE_TYPE_CD , ''COMP'' , exp_pass_from_source1.Ratesymbol_alfa , ''COLL'' , exp_pass_from_source1.RateSymbolCollision_alfa , '''' ) as v_RATE_SYMB_CD,
v_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_data_transformation1.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_data_transformation1.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_data_transformation1.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_data_transformation1.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
MD5 ( to_char ( DATE_TRUNC(DAY, exp_data_transformation1.feature_start_dt) ) || to_char ( DATE_TRUNC(DAY, exp_data_transformation1.asset_start_dt) ) || TO_CHAR ( DATE_TRUNC(DAY, exp_data_transformation1.feature_end_dt) ) || rtrim ( ltrim ( v_RATE_SYMB_CD ) ) || rtrim ( ltrim ( exp_data_transformation1.AGMT_ASSET_FEAT_AMT ) ) || rtrim ( ltrim ( exp_data_transformation1.AGMT_ASSET_FEAT_DT ) ) || rtrim ( ltrim ( exp_data_transformation1.AGMT_ASSET_FEAT_TXT ) ) || rtrim ( ltrim ( exp_data_transformation1.AGMT_ASSET_FEAT_IND ) ) || RTRIM ( LTRIM ( exp_data_transformation1.o_DiscountSurcharge_alfa_typecd ) ) || exp_data_transformation1.V_PRTY_ID ) as CALC_CHKSM,
CASE WHEN ORIG_CHKSM IS NULL THEN ''I'' ELSE CASE WHEN ORIG_CHKSM != CALC_CHKSM THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_data_transformation1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation1.EDW_END_DTTM as EDW_END_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_Default_EndDate,
exp_data_transformation1.Retired as Retired,
exp_data_transformation1.polcov_RateModifier as polcov_RateModifier,
exp_data_transformation1.polcov_Eligible as polcov_Eligible,
exp_data_transformation1.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_data_transformation1.V_PRTY_ID as V_PRTY_ID,
exp_pass_from_source1.source_record_id
FROM
exp_pass_from_source1
INNER JOIN exp_data_transformation1 ON exp_pass_from_source1.source_record_id = exp_data_transformation1.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID1 ON exp_data_transformation1.source_record_id = LKP_PRTY_ASSET_ID1.source_record_id
INNER JOIN LKP_AGMT1 ON LKP_PRTY_ASSET_ID1.source_record_id = LKP_AGMT1.source_record_id
INNER JOIN LKP_FEAT1 ON LKP_AGMT1.source_record_id = LKP_FEAT1.source_record_id
INNER JOIN LKP_AGMT_INSRD_ASSET_FEAT1 ON LKP_FEAT1.source_record_id = LKP_AGMT_INSRD_ASSET_FEAT1.source_record_id
);


-- Component rtr_ins_upd1_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd1_INSERT AS
SELECT
exp_ins_upd1.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd1.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd1.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd1.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd1.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd1.lkp_FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD,
exp_ins_upd1.AGMT_ID as AGMT_ID,
exp_ins_upd1.FEAT_ID as FEAT_ID,
exp_ins_upd1.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd1.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd1.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
exp_ins_upd1.PRCS_ID as PRCS_ID,
exp_ins_upd1.out_ins_upd as out_ins_upd,
exp_ins_upd1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd1.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd1.lkp_AGMT_ASSET_FEAT_END_DT as lkp_AGMT_ASSET_FEAT_END_DT,
exp_ins_upd1.in_TRANS_STRT_DTTM as o_Default_Date,
exp_ins_upd1.o_Default_EndDate as o_Default_EndDate,
exp_ins_upd1.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_ins_upd1.Retired as Retired,
exp_ins_upd1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_ins_upd1.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_ins_upd1.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_ins_upd1.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
exp_ins_upd1.polcov_RateModifier as polcov_RateModifier,
exp_ins_upd1.polcov_Eligible as polcov_Eligible,
exp_ins_upd1.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_ins_upd1.V_PRTY_ID as V_PRTY_ID,
exp_ins_upd1.source_record_id
FROM
exp_ins_upd1
WHERE exp_ins_upd1.AGMT_ID IS NOT NULL AND exp_ins_upd1.FEAT_ID IS NOT NULL AND exp_ins_upd1.PRTY_ASSET_ID IS NOT NULL AND ( exp_ins_upd1.out_ins_upd = ''I'' ) OR ( exp_ins_upd1.Retired = 0 AND exp_ins_upd1.lkp_EDW_END_DTTM != to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ) OR -- exp_ins_upd1.AGMT_ID IS NOT NULL AND exp_ins_upd1.out_ins_upd = ''U'' AND exp_ins_upd1.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) 
exp_ins_upd1.AGMT_ID IS NOT NULL AND exp_ins_upd1.FEAT_ID IS NOT NULL AND exp_ins_upd1.PRTY_ASSET_ID IS NOT NULL AND ( exp_ins_upd1.out_ins_upd = ''U'' ) AND exp_ins_upd1.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component rtr_ins_upd1_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd1_RETIRE AS
SELECT
exp_ins_upd1.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd1.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd1.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd1.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd1.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd1.lkp_FEAT_EFECT_TYPE_CD as FEAT_EFECT_TYPE_CD,
exp_ins_upd1.AGMT_ID as AGMT_ID,
exp_ins_upd1.FEAT_ID as FEAT_ID,
exp_ins_upd1.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins_upd1.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd1.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
exp_ins_upd1.PRCS_ID as PRCS_ID,
exp_ins_upd1.out_ins_upd as out_ins_upd,
exp_ins_upd1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd1.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd1.lkp_AGMT_ASSET_FEAT_END_DT as lkp_AGMT_ASSET_FEAT_END_DT,
exp_ins_upd1.in_TRANS_STRT_DTTM as o_Default_Date,
exp_ins_upd1.o_Default_EndDate as o_Default_EndDate,
exp_ins_upd1.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
exp_ins_upd1.Retired as Retired,
exp_ins_upd1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_ins_upd1.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT,
exp_ins_upd1.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT,
exp_ins_upd1.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT,
exp_ins_upd1.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND,
exp_ins_upd1.polcov_RateModifier as polcov_RateModifier,
exp_ins_upd1.polcov_Eligible as polcov_Eligible,
exp_ins_upd1.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd,
exp_ins_upd1.V_PRTY_ID as V_PRTY_ID,
exp_ins_upd1.source_record_id
FROM
exp_ins_upd1
WHERE exp_ins_upd1.AGMT_ID IS NOT NULL AND exp_ins_upd1.FEAT_ID IS NOT NULL AND exp_ins_upd1.PRTY_ASSET_ID IS NOT NULL AND exp_ins_upd1.out_ins_upd = ''R'' and exp_ins_upd1.Retired != 0 and exp_ins_upd1.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component upd_update1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd1_RETIRE.lkp_AGMT_ID as lkp_AGMT_ID3,
rtr_ins_upd1_RETIRE.lkp_FEAT_ID as lkp_FEAT_ID3,
rtr_ins_upd1_RETIRE.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
rtr_ins_upd1_RETIRE.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID3,
rtr_ins_upd1_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd1_RETIRE.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT3,
rtr_ins_upd1_RETIRE.lkp_AGMT_ASSET_FEAT_STRT_DT as lkp_AGMT_ASSET_FEAT_STRT_DT3,
rtr_ins_upd1_RETIRE.PRCS_ID as PRCS_ID3,
rtr_ins_upd1_RETIRE.o_Default_Date as o_Default_Date4,
rtr_ins_upd1_RETIRE.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT4,
rtr_ins_upd1_RETIRE.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT4,
rtr_ins_upd1_RETIRE.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT4,
rtr_ins_upd1_RETIRE.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND4,
rtr_ins_upd1_RETIRE.polcov_RateModifier as polcov_RateModifier,
rtr_ins_upd1_RETIRE.polcov_Eligible as polcov_Eligible,
rtr_ins_upd1_RETIRE.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd4,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd1_RETIRE.source_record_id as source_record_id
FROM
rtr_ins_upd1_RETIRE
);


-- Component exp_pass_to_tgt_upd1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd1 AS
(
SELECT
upd_update1.lkp_AGMT_ID3 as lkp_AGMT_ID3,
upd_update1.lkp_FEAT_ID3 as lkp_FEAT_ID3,
upd_update1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3 as lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
upd_update1.lkp_PRTY_ASSET_ID3 as lkp_PRTY_ASSET_ID3,
upd_update1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_update1.o_Default_Date4 as o_Default_Date4,
upd_update1.o_Default_Date4 as out_trans_end_dttm4,
upd_update1.AGMT_ASSET_FEAT_AMT4 as AGMT_ASSET_FEAT_AMT4,
upd_update1.AGMT_ASSET_FEAT_DT4 as AGMT_ASSET_FEAT_DT4,
upd_update1.AGMT_ASSET_FEAT_TXT4 as AGMT_ASSET_FEAT_TXT4,
upd_update1.AGMT_ASSET_FEAT_IND4 as AGMT_ASSET_FEAT_IND4,
upd_update1.polcov_RateModifier as polcov_RateModifier,
upd_update1.polcov_Eligible as polcov_Eligible,
upd_update1.o_DiscountSurcharge_alfa_typecd4 as o_DiscountSurcharge_alfa_typecd4,
upd_update1.source_record_id
FROM
upd_update1
);


-- Component upd_ins_new1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_new1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd1_INSERT.AGMT_ID as AGMT_ID,
rtr_ins_upd1_INSERT.FEAT_ID as FEAT_ID,
rtr_ins_upd1_INSERT.PRTY_ASSET_ID as PRTY_ASSET_ID,
rtr_ins_upd1_INSERT.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
rtr_ins_upd1_INSERT.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
rtr_ins_upd1_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd1_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd1_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_ins_upd1_INSERT.o_Default_Date as o_Default_Date3,
rtr_ins_upd1_INSERT.o_Default_EndDate as o_Default_EndDate1,
rtr_ins_upd1_INSERT.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
rtr_ins_upd1_INSERT.Retired as Retired1,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_AMT as AGMT_ASSET_FEAT_AMT1,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_DT as AGMT_ASSET_FEAT_DT1,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_TXT as AGMT_ASSET_FEAT_TXT1,
rtr_ins_upd1_INSERT.AGMT_ASSET_FEAT_IND as AGMT_ASSET_FEAT_IND1,
rtr_ins_upd1_INSERT.polcov_RateModifier as polcov_RateModifier,
rtr_ins_upd1_INSERT.polcov_Eligible as polcov_Eligible,
rtr_ins_upd1_INSERT.o_DiscountSurcharge_alfa_typecd as o_DiscountSurcharge_alfa_typecd1,
rtr_ins_upd1_INSERT.V_PRTY_ID as V_PRTY_ID2,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd1_INSERT.source_record_id as source_record_id,
FROM
rtr_ins_upd1_INSERT
);


-- Component exp_pass_src_to_tgt_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_src_to_tgt_ins1 AS
(
SELECT
upd_ins_new1.AGMT_ID as AGMT_ID,
upd_ins_new1.FEAT_ID as FEAT_ID,
upd_ins_new1.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_ins_new1.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_ins_new1.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DT,
upd_ins_new1.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DT,
upd_ins_new1.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DT,
upd_ins_new1.PRCS_ID as PRCS_ID,
upd_ins_new1.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN upd_ins_new1.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_ins_new1.EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_ins_new1.o_Default_Date3 as o_Default_Date3,
upd_ins_new1.out_RATE_SYMB_CD as out_RATE_SYMB_CD,
upd_ins_new1.AGMT_ASSET_FEAT_AMT1 as AGMT_ASSET_FEAT_AMT1,
upd_ins_new1.AGMT_ASSET_FEAT_DT1 as AGMT_ASSET_FEAT_DT1,
upd_ins_new1.AGMT_ASSET_FEAT_TXT1 as AGMT_ASSET_FEAT_TXT1,
upd_ins_new1.AGMT_ASSET_FEAT_IND1 as AGMT_ASSET_FEAT_IND1,
CASE WHEN upd_ins_new1.Retired1 = 0 THEN to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ELSE upd_ins_new1.o_Default_Date3 END as TRNS_END_DTTM,
upd_ins_new1.polcov_RateModifier as polcov_RateModifier,
upd_ins_new1.polcov_Eligible as polcov_Eligible,
upd_ins_new1.o_DiscountSurcharge_alfa_typecd1 as o_DiscountSurcharge_alfa_typecd1,
upd_ins_new1.V_PRTY_ID2 as V_PRTY_ID2,
upd_ins_new1.source_record_id
FROM
upd_ins_new1
);


-- Component AGMT_INSRD_ASSET_FEAT_upd_Building_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
USING exp_pass_to_tgt_upd1 ON (AGMT_INSRD_ASSET_FEAT.AGMT_ID = exp_pass_to_tgt_upd1.lkp_AGMT_ID3 AND AGMT_INSRD_ASSET_FEAT.FEAT_ID = exp_pass_to_tgt_upd1.lkp_FEAT_ID3 AND AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID = exp_pass_to_tgt_upd1.lkp_PRTY_ASSET_ID3 AND AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3 AND AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = exp_pass_to_tgt_upd1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_tgt_upd1.lkp_AGMT_ID3,
FEAT_ID = exp_pass_to_tgt_upd1.lkp_FEAT_ID3,
PRTY_ASSET_ID = exp_pass_to_tgt_upd1.lkp_PRTY_ASSET_ID3,
ASSET_CNTRCT_ROLE_SBTYPE_CD = exp_pass_to_tgt_upd1.lkp_ASSET_CNTRCT_ROLE_SBTYPE_CD3,
AGMT_ASSET_FEAT_AMT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_AMT4,
AGMT_ASSET_FEAT_RATE = exp_pass_to_tgt_upd1.polcov_RateModifier,
AGMT_ASSET_FEAT_DT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_DT4,
FEAT_EFECT_TYPE_CD = exp_pass_to_tgt_upd1.o_DiscountSurcharge_alfa_typecd4,
AGMT_ASSET_FEAT_TXT = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_TXT4,
AGMT_ASSET_FEAT_IND = exp_pass_to_tgt_upd1.AGMT_ASSET_FEAT_IND4,
FEAT_ELGBL_IND = exp_pass_to_tgt_upd1.polcov_Eligible,
EDW_STRT_DTTM = exp_pass_to_tgt_upd1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_upd1.EDW_END_DTTM,
TRANS_STRT_DTTM = exp_pass_to_tgt_upd1.o_Default_Date4,
TRANS_END_DTTM = exp_pass_to_tgt_upd1.out_trans_end_dttm4;


-- Component AGMT_INSRD_ASSET_FEAT_insert_Building_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT
(
AGMT_ID,
FEAT_ID,
PRTY_ASSET_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_FEAT_STRT_DTTM,
AGMT_ASSET_FEAT_END_DTTM,
RATE_SYMB_CD,
AGMT_ASSET_FEAT_AMT,
AGMT_ASSET_FEAT_RATE,
AGMT_ASSET_FEAT_DT,
FEAT_EFECT_TYPE_CD,
AGMT_ASSET_FEAT_TXT,
AGMT_ASSET_FEAT_IND,
FEAT_ELGBL_IND,
PRTY_CNTCT_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_src_to_tgt_ins1.AGMT_ID as AGMT_ID,
exp_pass_src_to_tgt_ins1.FEAT_ID as FEAT_ID,
exp_pass_src_to_tgt_ins1.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_pass_src_to_tgt_ins1.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_STRT_DT as AGMT_ASSET_STRT_DTTM,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_STRT_DT as AGMT_ASSET_FEAT_STRT_DTTM,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_END_DT as AGMT_ASSET_FEAT_END_DTTM,
exp_pass_src_to_tgt_ins1.out_RATE_SYMB_CD as RATE_SYMB_CD,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_AMT1 as AGMT_ASSET_FEAT_AMT,
exp_pass_src_to_tgt_ins1.polcov_RateModifier as AGMT_ASSET_FEAT_RATE,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_DT1 as AGMT_ASSET_FEAT_DT,
exp_pass_src_to_tgt_ins1.o_DiscountSurcharge_alfa_typecd1 as FEAT_EFECT_TYPE_CD,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_TXT1 as AGMT_ASSET_FEAT_TXT,
exp_pass_src_to_tgt_ins1.AGMT_ASSET_FEAT_IND1 as AGMT_ASSET_FEAT_IND,
exp_pass_src_to_tgt_ins1.polcov_Eligible as FEAT_ELGBL_IND,
exp_pass_src_to_tgt_ins1.V_PRTY_ID2 as PRTY_CNTCT_ID,
exp_pass_src_to_tgt_ins1.PRCS_ID as PRCS_ID,
exp_pass_src_to_tgt_ins1.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_src_to_tgt_ins1.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_src_to_tgt_ins1.o_Default_Date3 as TRANS_STRT_DTTM,
exp_pass_src_to_tgt_ins1.TRNS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_src_to_tgt_ins1;


-- PIPELINE END FOR 2
-- Component AGMT_INSRD_ASSET_FEAT_upd_Building_retired, Type Post SQL 
UPDATE db_t_prod_core.AGMT_INSRD_ASSET_FEAT
  set EDW_END_DTTM=A.lead1,

  TRANS_END_DTTM=A.lead2
FROM

            (SELECT   distinct  AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,

                        max(EDW_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD

                        ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

                        as lead1

                       ,max(TRANS_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD

                       ORDER BY TRANS_STRT_DTTM  ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''

                        as lead2

                       FROM             db_t_prod_core.AGMT_INSRD_ASSET_FEAT  

                      group by   AGMT_ID,FEAT_ID,PRTY_ASSET_ID,ASSET_CNTRCT_ROLE_SBTYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM

                          ) A

 
  where  AGMT_INSRD_ASSET_FEAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

                                                    and AGMT_INSRD_ASSET_FEAT.AGMT_ID=A.AGMT_ID

                          and AGMT_INSRD_ASSET_FEAT.FEAT_ID=A.FEAT_ID

                          and AGMT_INSRD_ASSET_FEAT.PRTY_ASSET_ID=A.PRTY_ASSET_ID

                          and AGMT_INSRD_ASSET_FEAT.ASSET_CNTRCT_ROLE_SBTYPE_CD=A.ASSET_CNTRCT_ROLE_SBTYPE_CD

                          and CAST(AGMT_INSRD_ASSET_FEAT.EDW_END_DTTM AS DATE)=''9999-12-31''

                                                   AND lead1 IS NOT NULL

                         AND lead2 IS NOT NULL;


END; ';